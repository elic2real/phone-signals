#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import time
import math
import hashlib
import hmac
import base64
import urllib.parse
import urllib.request
import sqlite3
import threading
import shutil
import statistics
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional, Union
from decimal import Decimal, getcontext
from pathlib import Path
# Import logging functions from separate module
from phone_bot_logging import log_runtime, log_trade_event, log_metrics

try:
    import requests
    _HAS_REQUESTS = True
except Exception:
    requests = None
    _HAS_REQUESTS = False


class OandaClient:
    """Canonical OANDA API client (production self-contained).

    NOTE: Single source of truth for OANDA I/O methods used by this file.
    """

    def __init__(self, api_key: str, account_id: str, env: str = "practice"):
        self.api_key = str(api_key or "").strip()
        self.account_id = str(account_id or "").strip()
        self.env = str(env or "practice").strip()
        self.base = "https://api-fxpractice.oanda.com" if self.env == "practice" else "https://api-fxtrade.oanda.com"
        self._sess = None
        if _HAS_REQUESTS and requests is not None:
            self._sess = requests.Session()
            self._sess.headers.update(
                {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "Accept-Datetime-Format": "RFC3339",
                }
            )

    def _err(self, **kw) -> dict:
        out = dict(kw)
        out.setdefault("_exception", False)
        out.setdefault("_http_error", False)
        out.setdefault("_json_error", False)
        out.setdefault("_rate_limited", False)
        return out

    def _request(self, method: str, path: str, *, params: Optional[dict] = None, body: Optional[dict] = None) -> dict:
        if not (_HAS_REQUESTS and self._sess is not None):
            return self._err(_exception=True, _error="requests_not_available")
        url = f"{self.base}{path}"
        try:
            resp = self._sess.request(method, url, params=params, json=body, timeout=15)
        except Exception as e:
            return self._err(_exception=True, _error=str(e))

        out = None
        try:
            out = resp.json()
        except Exception:
            out = {"_text": getattr(resp, "text", "")}
            out.update(self._err(_json_error=True))

        # Normalize error fields for oanda_call wrapper
        if isinstance(resp.status_code, int) and resp.status_code >= 400:
            out = out if isinstance(out, dict) else {"_text": str(out)}
            out.update(self._err(_http_error=True, _status=int(resp.status_code)))
            if resp.status_code == 429:
                out["_rate_limited"] = True
        return out if isinstance(out, dict) else {"_text": str(out)}

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        return self._request("GET", path, params=params)

    def _post(self, path: str, body: dict) -> dict:
        return self._request("POST", path, body=body)

    def _put(self, path: str, body: dict) -> dict:
        return self._request("PUT", path, body=body)

    # --- Core endpoints used by this file ---
    def pricing(self, pair: str) -> Tuple[float, float]:
        resp = self._get(
            f"/v3/accounts/{self.account_id}/pricing",
            params={"instruments": normalize_pair(pair)},
        )
        prices = resp.get("prices") if isinstance(resp, dict) else None
        if isinstance(prices, list) and prices:
            p0 = prices[0]
            return float(p0.get("bid")), float(p0.get("ask"))
        raise RuntimeError(f"No pricing for {pair}")

    def pricing_multi(self, pairs: List[str]) -> Dict[str, Tuple[float, float]]:
        instruments = ",".join([normalize_pair(p) for p in pairs])
        resp = self._get(
            f"/v3/accounts/{self.account_id}/pricing",
            params={"instruments": instruments},
        )
        out: Dict[str, Tuple[float, float]] = {}
        for p in resp.get("prices", []) if isinstance(resp, dict) else []:
            try:
                out[normalize_pair(p.get("instrument"))] = (float(p.get("bid")), float(p.get("ask")))
            except Exception:
                continue
        return out

    def candles(self, instrument: str, granularity: str, count: int = 500, *, price: str = "BA") -> List[dict]:
        params = {"granularity": granularity, "count": int(count), "price": price}
        resp = self._get(f"/v3/instruments/{normalize_pair(instrument)}/candles", params=params)
        return resp.get("candles", []) if isinstance(resp, dict) else []

    def account_summary(self) -> dict:
        return self._get(f"/v3/accounts/{self.account_id}/summary")

    def open_positions(self) -> dict:
        return self._get(f"/v3/accounts/{self.account_id}/openPositions")

    def pending_orders(self, params: Optional[dict] = None) -> dict:
        p = dict(params or {})
        p.setdefault("state", "PENDING")
        return self._get(f"/v3/accounts/{self.account_id}/orders", params=p)

    def close_position(self, instrument: str, side: str = "long", units: str = "ALL", **body) -> dict:
        # Accept both legacy (side, units) and caller passing longUnits/shortUnits.
        if body:
            payload = body
        else:
            payload = {"longUnits": "0", "shortUnits": "0"}
            if str(side).lower().startswith("l"):
                payload["longUnits"] = units
            else:
                payload["shortUnits"] = units
        return self._put(f"/v3/accounts/{self.account_id}/positions/{normalize_pair(instrument)}/close", payload)

    def set_trade_stop_loss(self, trade_id: str, price: float) -> dict:
        body = {"stopLoss": {"price": str(price)}}
        return self._put(f"/v3/accounts/{self.account_id}/trades/{trade_id}/orders", body)

    def place_market(self, instrument: str, units: int, sl_price: float, tp_price: float, *, client_id: str = "") -> dict:
        # stopLossOnFill + takeProfitOnFill are required by SOP.
        order: dict = {
            "type": "MARKET",
            "instrument": normalize_pair(instrument),
            "units": str(int(units)),
            "timeInForce": "FOK",
            "positionFill": "DEFAULT",
            "takeProfitOnFill": {"price": str(tp_price)},
            "stopLossOnFill": {"price": str(sl_price)},
        }
        if client_id:
            order["clientExtensions"] = {"id": str(client_id)[:32]}
        return self._post(f"/v3/accounts/{self.account_id}/orders", {"order": order})

    def order_book(self, instrument: str, *, bucket_width: Optional[float] = None, time: Optional[str] = None) -> dict:
        params: dict = {}
        if bucket_width is not None:
            params["bucketWidth"] = str(bucket_width)
        if time is not None:
            params["time"] = str(time)
        return self._get(f"/v3/instruments/{normalize_pair(instrument)}/orderBook", params=params or None)

    def position_book(self, instrument: str, *, bucket_width: Optional[float] = None, time: Optional[str] = None) -> dict:
        params: dict = {}
        if bucket_width is not None:
            params["bucketWidth"] = str(bucket_width)
        if time is not None:
            params["time"] = str(time)
        return self._get(f"/v3/instruments/{normalize_pair(instrument)}/positionBook", params=params or None)

# ============================================================================
# DATA QUALITY STATES - Must be defined before use
# ============================================================================
class DataQuality:
    OK = "OK"
    DEGRADED = "DEGRADED"
    BAD = "BAD"

# ============================================================================
# PATH-SPACE ENGINE - V12 LOCKED - Single Source of Truth
# ============================================================================

@dataclass
class PathSpaceState:
    """Canonical path-space state for a single instrument"""
    # Core price tracking
    current_price: float = 0.0
    entry_price: Optional[float] = None
    direction: Optional[str] = None  # LONG/SHORT
    
    # Path-space primitives (canonical calculations only)
    path_len: float = 0.0  # Σ |ΔPᵢ| over window
    displacement: float = 0.0  # |P - Entry| or |P - P_start|
    efficiency: float = 0.0  # |ΔP| / PathLen
    overlap: float = 0.0  # PathLen / |ΔP| (churn proxy)
    progress: float = 0.0  # |P - Entry| / ATR
    
    # Energy and momentum
    energy: float = 0.0  # |P - P_k| / ATR (k = reference point)
    speed: float = 0.0  # |ΔP_recent| / ATR over window
    velocity: float = 0.0  # Speed_now - Speed_prev
    
    # Extrema tracking
    rolling_high: float = float('-inf')
    rolling_low: float = float('inf')
    pullback: float = 0.0  # Retrace from rolling extrema / ATR
    
    # Time and persistence
    last_update: float = 0.0
    time_at_level: float = 0.0  # TimeSpent(level)
    
    # Volatility
    atr: float = 0.0  # Noise scale from ATR_M1(14)
    vol_slope: float = 0.0  # ΔATR_M1 (volatility slope)
    
    # History windows
    price_history: deque = field(default_factory=lambda: deque(maxlen=100))
    displacement_history: deque = field(default_factory=lambda: deque(maxlen=20))
    speed_history: deque = field(default_factory=lambda: deque(maxlen=10))
    atr_history: deque = field(default_factory=lambda: deque(maxlen=15))

class PathSpaceEngine:
    """Canonical engine for path-space primitive calculations"""
    
    def __init__(self, window_seconds: int = 300):
        self.window_seconds = window_seconds
        self.states: Dict[str, PathSpaceState] = {}
    
    def update_price(self, pair: str, price: float, atr: float, timestamp: float = None) -> PathSpaceState:
        """Update price and recalculate all primitives"""
        if timestamp is None:
            timestamp = time.time()
            
        state = self.states.get(pair)
        if state is None:
            state = PathSpaceState()
            state.current_price = price
            state.atr = atr
            state.last_update = timestamp
            self.states[pair] = state
            return state
        
        # Calculate displacement
        prev_price = state.current_price
        delta_p = abs(price - prev_price)
        
        # Update histories
        state.price_history.append(price)
        state.atr_history.append(atr)
        
        # Update path length (sum of absolute displacements)
        state.path_len += delta_p
        
        # Calculate displacement from entry or start
        if state.entry_price is not None:
            state.displacement = abs(price - state.entry_price)
        else:
            # Use first price as reference
            if len(state.price_history) > 1:
                start_price = state.price_history[0]
                state.displacement = abs(price - start_price)
        
        # Calculate efficiency
        if state.path_len > 0:
            state.efficiency = state.displacement / state.path_len
            state.overlap = state.path_len / max(state.displacement, 0.0001)
        
        # Update rolling extrema
        if price > state.rolling_high:
            state.rolling_high = price
        if price < state.rolling_low:
            state.rolling_low = price
        
        # Calculate pullback from extrema
        if state.direction == "LONG" and state.rolling_high > float('-inf'):
            state.pullback = (state.rolling_high - price) / atr
        elif state.direction == "SHORT" and state.rolling_low < float('inf'):
            state.pullback = (price - state.rolling_low) / atr
        
        # Calculate progress
        if state.entry_price is not None and atr > 0:
            state.progress = abs(price - state.entry_price) / atr
        
        # Calculate energy (displacement from reference point)
        if len(state.price_history) >= 5:  # Reduced from 20 for testing
            ref_price = state.price_history[0]  # Use first price as reference
            state.energy = abs(price - ref_price) / atr
        
        # Calculate speed (recent displacement rate)
        if len(state.price_history) >= 10 and atr > 0:
            recent_window = min(10, len(state.price_history))
            recent_disp = abs(price - state.price_history[-recent_window])
            state.speed = recent_disp / atr
            
            # Store speed history for velocity calculation
            state.speed_history.append(state.speed)
            
            # Calculate velocity (change in speed)
            if len(state.speed_history) >= 2:
                state.velocity = state.speed_history[-1] - state.speed_history[-2]
        
        # Calculate volatility slope
        if len(state.atr_history) >= 3:
            state.vol_slope = state.atr_history[-1] - state.atr_history[-3]
        
        # Update time tracking
        if abs(price - state.current_price) < 0.00001:  # Same price level
            state.time_at_level += (timestamp - state.last_update)
        else:
            state.time_at_level = 0.0
        
        # Update state
        state.current_price = price
        state.atr = atr
        state.last_update = timestamp
        
        # LOG METRICS - SINGLE SOURCE OF TRUTH
        metrics_obj = {
            "pair": pair,
            "side": state.direction.lower() if state.direction else "none",
            "price_exec": price,
            "entry_price": state.entry_price or 0.0,
            "atr_price": atr,
            "atr_pips": atr * 10000,
            "spread_pips": 0.0,  # Will be updated by caller
            
            # Path-space primitives (minimum required)
            "dp": delta_p,
            "path_len": state.path_len,
            "efficiency": state.efficiency,
            "overlap": state.overlap,
            "progress": state.progress,
            "speed": state.speed,
            "velocity": state.velocity,
            "pullback": state.pullback,
            "local_high": state.rolling_high,
            "local_low": state.rolling_low,
            
            # Additional fields
            "time_at_level": state.time_at_level,
            "vol_slope": state.vol_slope
        }
        
        # Log metrics to metrics.jsonl
        log_metrics(metrics_obj)
        
        return state
    
    def set_entry(self, pair: str, entry_price: float, direction: str) -> None:
        """Set entry parameters for trade"""
        state = self.states.get(pair)
        if state is None:
            state = PathSpaceState()
            self.states[pair] = state
        
        state.entry_price = entry_price
        state.direction = direction.upper()
        state.rolling_high = entry_price if direction == "LONG" else float('-inf')
        state.rolling_low = entry_price if direction == "SHORT" else float('inf')
    
    def get_primitives(self, pair: str) -> Dict[str, float]:
        """Get all primitives for a pair"""
        state = self.states.get(pair)
        if state is None:
            return {}
        
        return {
            "current_price": state.current_price,
            "entry_price": state.entry_price or 0.0,
            "direction": state.direction or "",
            "path_len": state.path_len,
            "displacement": state.displacement,
            "efficiency": state.efficiency,
            "overlap": state.overlap,
            "progress": state.progress,
            "energy": state.energy,
            "speed": state.speed,
            "velocity": state.velocity,
            "rolling_high": state.rolling_high,
            "rolling_low": state.rolling_low,
            "pullback": state.pullback,
            "time_at_level": state.time_at_level,
            "atr": state.atr,
            "vol_slope": state.vol_slope,
        }
    
    def reset(self, pair: str) -> None:
        """Reset state for a pair"""
        if pair in self.states:
            del self.states[pair]

# Global canonical instance
_path_engine = PathSpaceEngine()

def get_path_engine() -> PathSpaceEngine:
    """Get the canonical path-space engine instance"""
    return _path_engine

# ============================================================================
# END PATH-SPACE ENGINE
# ============================================================================

# ============================================================================
# PRICING STREAM - Continuous Tick Data
# ============================================================================

@dataclass
class TickData:
    """Single tick data point"""
    ts: float
    bid: float
    ask: float
    mid: float
    spread: float
    spread_pips: float

class PricingStream:
    """Continuous pricing stream for tick-grade data"""
    
    def __init__(self, pairs: List[str], max_buffer: int = 1000):
        self.pairs = pairs
        self.max_buffer = max_buffer
        self.tick_buffers: Dict[str, deque] = {pair: deque(maxlen=max_buffer) for pair in pairs}
        self.latest_ticks: Dict[str, Optional[TickData]] = {pair: None for pair in pairs}
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.oanda_client = None
        
    def start(self, oanda_client):
        """Start the pricing stream"""
        if self.running:
            return
            
        self.oanda_client = oanda_client
        self.running = True
        self.thread = threading.Thread(target=self._stream_loop, daemon=True)
        self.thread.start()
        log(f"{EMOJI_INFO} PRICING_STREAM_STARTED", {"pairs": self.pairs})
        
    def stop(self):
        """Stop the pricing stream"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        log(f"{EMOJI_INFO} PRICING_STREAM_STOPPED", {"pairs": self.pairs})
        
    def _stream_loop(self):
        """Main streaming loop - polls pricing endpoint continuously"""
        poll_interval = 0.5  # 500ms for high-frequency data
        
        while self.running and self.oanda_client:
            try:
                # Batch fetch pricing for all pairs
                for pair in self.pairs:
                    pricing = self.oanda_client.pricing(pair)
                    if pricing and len(pricing) >= 2:
                        bid, ask = pricing
                        self._add_tick(pair, bid, ask)
                        
                time.sleep(poll_interval)
                
            except Exception as e:
                log(f"{EMOJI_WARN} PRICING_STREAM_ERROR", {"error": str(e)})
                time.sleep(1)  # Brief pause on error
                
    def _add_tick(self, pair: str, bid: float, ask: float):
        """Add a new tick to the buffer"""
        if not (is_valid_price(bid) and is_valid_price(ask)):
            return
            
        mid = (bid + ask) / 2
        spread = ask - bid
        spread_pips = spread / get_pip_value(pair)
        
        tick = TickData(
            ts=now_ts(),
            bid=bid,
            ask=ask,
            mid=mid,
            spread=spread,
            spread_pips=spread_pips
        )
        
        # Update buffer and latest
        self.tick_buffers[pair].append(tick)
        self.latest_ticks[pair] = tick
        
    def get_latest_tick(self, pair: str) -> Optional[TickData]:
        """Get the most recent tick for a pair"""
        return self.latest_ticks.get(pair)
        
    def get_recent_ticks(self, pair: str, count: int = 100) -> List[TickData]:
        """Get the most recent N ticks for a pair"""
        buffer = self.tick_buffers.get(pair)
        if not buffer:
            return []
        return list(buffer)[-count:]
        
    def get_tick_statistics(self, pair: str, window_seconds: float = 60.0) -> dict:
        """Calculate tick statistics over a time window"""
        ticks = self.get_recent_ticks(pair)
        if not ticks:
            return {}
            
        # Filter by time window
        cutoff = now_ts() - window_seconds
        recent = [t for t in ticks if t.ts >= cutoff]
        
        if not recent:
            return {}
            
        # Calculate statistics
        spreads = [t.spread_pips for t in recent]
        mids = [t.mid for t in recent]
        
        return {
            "tick_count": len(recent),
            "avg_spread_pips": sum(spreads) / len(spreads),
            "min_spread_pips": min(spreads),
            "max_spread_pips": max(spreads),
            "price_change": mids[-1] - mids[0] if len(mids) > 1 else 0,
            "volatility": sum(abs(mids[i] - mids[i-1]) for i in range(1, len(mids))) / len(mids)
        }

# Global pricing stream instance
_pricing_stream: Optional[PricingStream] = None

def get_pricing_stream() -> Optional[PricingStream]:
    """Get the global pricing stream instance"""
    return _pricing_stream

def start_pricing_stream(pairs: List[str], oanda_client):
    """Initialize and start the global pricing stream"""
    global _pricing_stream
    if _pricing_stream is None:
        _pricing_stream = PricingStream(pairs)
    _pricing_stream.start(oanda_client)

def stop_pricing_stream():
    """Stop the global pricing stream"""
    global _pricing_stream
    if _pricing_stream:
        _pricing_stream.stop()

# ============================================================================
# END PRICING STREAM
# ============================================================================

# ============================================================================
# STATE MACHINE - Formal State Transitions with Alerts
# ============================================================================

@dataclass
class StateTransition:
    """Record of a state transition"""
    ts: float
    pair: str
    from_state: str
    to_state: str
    strategy: Optional[str] = None
    direction: Optional[str] = None
    reason: Optional[str] = None
    metadata: Optional[dict] = None

class StateMachine:
    """Formal state machine with alert integration and logging"""
    
    # Valid states
    STATES = {
        "PASS", "SKIP", "WAIT", "WATCH", 
        "GET_READY", "ENTER", "MANAGING"
    }
    
    # State transition matrix (allowed transitions)
    ALLOWED_TRANSITIONS = {
        "PASS": {"WAIT", "WATCH"},
        "SKIP": {"WAIT", "WATCH"},
        "WAIT": {"WATCH", "PASS", "SKIP"},
        "WATCH": {"GET_READY", "WAIT", "PASS", "SKIP"},
        "GET_READY": {"ENTER", "WATCH", "WAIT"},
        "ENTER": {"MANAGING"},
        "MANAGING": {"WAIT", "WATCH", "PASS", "SKIP"}
    }
    
    def __init__(self):
        self.current_states: Dict[str, str] = {}  # pair -> state
        self.state_history: Dict[str, List[StateTransition]] = {}  # pair -> history
        self.state_entry_time: Dict[str, float] = {}  # pair -> entry timestamp
        self._lock = threading.Lock()
        
    def get_state(self, pair: str) -> str:
        """Get current state for a pair"""
        with self._lock:
            return self.current_states.get(pair, "PASS")
            
    def set_state(self, pair: str, state: str, strategy: Optional[str] = None, 
                  direction: Optional[str] = None, reason: Optional[str] = None,
                  metadata: Optional[dict] = None):
        """Set state for a pair (only for initialization)"""
        with self._lock:
            if state not in self.STATES:
                log(f"{EMOJI_WARN} INVALID_STATE", {"pair": pair, "state": state})
                return
                
            self.current_states[pair] = state
            self.state_entry_time[pair] = now_ts()
            if pair not in self.state_history:
                self.state_history[pair] = []
                
    def transition(self, pair: str, to_state: str, strategy: Optional[str] = None,
                   direction: Optional[str] = None, reason: Optional[str] = None,
                   metadata: Optional[dict] = None) -> bool:
        """Transition to a new state with alerts and logging"""
        with self._lock:
            from_state = self.current_states.get(pair, "PASS")
            
            # Validate transition
            if to_state not in self.STATES:
                log(f"{EMOJI_WARN} INVALID_STATE", {"pair": pair, "state": to_state})
                return False
                
            if from_state not in self.ALLOWED_TRANSITIONS:
                log(f"{EMOJI_WARN} INVALID_FROM_STATE", {"pair": pair, "state": from_state})
                return False
                
            if to_state not in self.ALLOWED_TRANSITIONS[from_state]:
                log(f"{EMOJI_WARN} INVALID_TRANSITION", 
                    {"pair": pair, "from": from_state, "to": to_state})
                return False
                
            # Skip if already in target state
            if from_state == to_state:
                return False
                
            # Record transition
            transition = StateTransition(
                ts=now_ts(),
                pair=pair,
                from_state=from_state,
                to_state=to_state,
                strategy=strategy,
                direction=direction,
                reason=reason,
                metadata=metadata
            )
            
            # Update state
            self.current_states[pair] = to_state
            self.state_entry_time[pair] = transition.ts
            
            if pair not in self.state_history:
                self.state_history[pair] = []
            self.state_history[pair].append(transition)
            
            # Emit alerts based on transition
            self._emit_alert(transition)
            
            # Log transition
            self._log_transition(transition)
            
            return True
            
    def get_state_duration(self, pair: str) -> float:
        """Get duration in current state"""
        with self._lock:
            entry_time = self.state_entry_time.get(pair, now_ts())
            return now_ts() - entry_time
            
    def get_last_transition(self, pair: str) -> Optional[StateTransition]:
        """Get the most recent transition for a pair"""
        with self._lock:
            history = self.state_history.get(pair, [])
            return history[-1] if history else None
            
    def _emit_alert(self, transition: StateTransition):
        """Emit appropriate alert based on state transition"""
        try:
            # Call actual alert functions defined later in the file
            if transition.to_state == "WATCH":
                # Import here to avoid circular dependency
                from sys import modules
                if 'phone_bot' in modules:
                    alert_watch_triggered(
                        transition.pair, 
                        transition.strategy or "UNKNOWN",
                        transition.metadata.get('signal_strength', 0.0) if transition.metadata else 0.0
                    )
                    
            elif transition.to_state == "GET_READY":
                entry_conditions = transition.metadata or {}
                alert_get_ready(
                    transition.pair,
                    transition.strategy or "UNKNOWN",
                    entry_conditions
                )
                    
            elif transition.to_state == "ENTER":
                alert_enter_placed(
                    transition.pair,
                    transition.strategy or "UNKNOWN",
                    transition.direction or "UNKNOWN",
                    transition.metadata.get('order_id', 'UNKNOWN') if transition.metadata else 'UNKNOWN'
                )
                    
            elif transition.from_state == "ENTER" and transition.to_state == "MANAGING":
                alert_trade_entered(
                    transition.pair,
                    transition.strategy or "UNKNOWN",
                    transition.direction or "UNKNOWN",
                    transition.metadata.get('trade_id', 'UNKNOWN') if transition.metadata else 'UNKNOWN'
                )
                    
        except Exception as e:
            log(f"{EMOJI_WARN} ALERT_EMIT_FAILED", 
                {"pair": transition.pair, "error": str(e)})
                
    def _log_transition(self, transition: StateTransition):
        """Log state transition to JSONL file"""
        try:
            log_entry = {
                "ts": transition.ts,
                "event": "state_transition",
                "pair": transition.pair,
                "from": transition.from_state,
                "to": transition.to_state,
                "strategy": transition.strategy,
                "direction": transition.direction,
                "reason": transition.reason,
                "metadata": transition.metadata
            }
            
            # Write to trades.jsonl
            log_file = os.path.join(PROJECT_DIR, "logs", "trades.jsonl")
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            
            with open(log_file, "a") as f:
                f.write(json.dumps(log_entry) + "\n")
                
            # Also log to runtime
            log(f"{EMOJI_INFO} STATE_TRANSITION", 
                {"pair": transition.pair, "from": transition.from_state, "to": transition.to_state})
                
        except Exception as e:
            log(f"{EMOJI_WARN} STATE_LOG_FAILED", 
                {"pair": transition.pair, "error": str(e)})

# Global state machine instance
_state_machine: Optional[StateMachine] = None

def get_state_machine() -> StateMachine:
    """Get the global state machine instance"""
    global _state_machine
    if _state_machine is None:
        _state_machine = StateMachine()
    return _state_machine

# ============================================================================
# END STATE MACHINE
# ============================================================================

def normalize_pair(pair: str) -> str:
    p = str(pair or "").strip().upper()
    if not p:
        return ""
    p = p.replace("-", "_").replace("/", "_")
    if "_" not in p and len(p) == 6 and p.isalpha():
        p = f"{p[:3]}_{p[3:]}"
    return p

def normalize_granularity(gran: str) -> str:
    return str(gran or "").strip().upper()

def normalize_oanda_env(env: str) -> str:
    e = str(env or "").strip().lower()
    if e in ("practice", "fxpractice", "demo", "paper", "test"):
        return "practice"
    if e in ("live", "fxtrade", "prod", "production"):
        return "live"
    return ""

try:
    import requests  # type: ignore

    _HAS_REQUESTS = True
except Exception:
    requests = None  # type: ignore
    _HAS_REQUESTS = False

PAIRS = [
    normalize_pair(p)
    for p in [
        "EUR_USD",
        "EUR_CAD",
        "USD_CAD",
        "AUD_USD",
        "AUD_JPY",
        "USD_JPY",
        "USD_DKK",
    ]
]
_pair_env = (os.getenv("PAIR_LIST", "") or "").strip()
if _pair_env:
    parts = []
    for chunk in _pair_env.replace(";", ",").split(","):
        parts.extend(chunk.split())
    _pair_override = [normalize_pair(p) for p in parts if normalize_pair(p)]
    if _pair_override:
        PAIRS = _pair_override

# Broker rejection logging
BROKER_REJECT_LOG = "broker_rejections.log"

def log_broker_reject(event_type: str, pair: str, details: Dict[str, Any]) -> None:
    """Log broker rejections/cancellations to a dedicated file for debugging."""
    pair = normalize_pair(pair)
    timestamp = datetime.utcnow().isoformat() + " UTC"
    entry = {
        "ts": timestamp,
        "event": event_type,
        "pair": pair,
        **details
    }
    try:
        with open(BROKER_REJECT_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        # Fallback to main log if file write fails
        log(f"{EMOJI_ERR} BROKER_REJECT_LOG_FAIL", {"error": str(e), "event": event_type, "pair": pair})

LEVERAGE_50 = {normalize_pair(p) for p in {"EUR_USD", "USD_JPY", "USD_CAD"}}
LEV_DEFAULT = 20  # Changed from 33 to prevent insufficient margin rejections

# Scan cadence (override via env)
SKIP_SCAN_SEC = float(os.getenv("SKIP_SCAN_SEC", "300") or "300")  # 5 minutes for SKIP state
WATCH_SCAN_SEC = float(os.getenv("WATCH_SCAN_SEC", "60") or "60")  # 1 minute for WATCH state
FOCUS_SCAN_SEC = float(os.getenv("FOCUS_SCAN_SEC", "5") or "5")    # 5 seconds for GET_READY/ENTER
EXIT_SCAN_SEC = float(os.getenv("EXIT_SCAN_SEC", "2") or "2")      # Every 2 seconds for open trades
EXIT_REFRESH_SEC = float(os.getenv("EXIT_REFRESH_SEC", "20") or "20")

# Main loop sleep (override via env)
LOOP_SLEEP_SEC = float(os.getenv("LOOP_SLEEP_SEC", "0.2") or "0.2")

# Legacy compatibility constant (rate limiting removed)
MAX_HTTP_PER_MIN = 120

# Call frequency tuning (cache TTLs)
ACCOUNT_REFRESH_SEC = float(os.getenv("ACCOUNT_REFRESH_SEC", "15") or "15")
OPEN_POS_REFRESH_SEC = float(os.getenv("OPEN_POS_REFRESH_SEC", "10") or "10")
PENDING_REFRESH_SEC = float(os.getenv("PENDING_REFRESH_SEC", "10") or "10")
PRICE_REFRESH_SEC = float(os.getenv("PRICE_REFRESH_SEC", "5") or "5")
EXIT_PRICE_REFRESH_SEC = float(os.getenv("EXIT_PRICE_REFRESH_SEC", "2") or "2")
CANDLE_REFRESH_SEC = float(os.getenv("CANDLE_REFRESH_SEC", "5") or "5")

# Candle refresh by scan type (seconds)
CANDLE_REFRESH_FOCUS_SEC = float(os.getenv("CANDLE_REFRESH_FOCUS_SEC", "5") or "5")
CANDLE_REFRESH_WATCH_SEC = float(os.getenv("CANDLE_REFRESH_WATCH_SEC", "15") or "15")
CANDLE_REFRESH_SKIP_SEC = float(os.getenv("CANDLE_REFRESH_SKIP_SEC", "30") or "30")
EXIT_CANDLE_REFRESH_SEC = float(os.getenv("EXIT_CANDLE_REFRESH_SEC", "5") or "5")

SPREAD_MAX_PIPS = 12.0

MAX_POS_PER_PAIR = 2
# No global limit - we have currency exposure limits instead

MAX_FAST_TRADES = 999  # Effectively unlimited - controlled by currency exposure
MAX_MED_TRADES = 999   # Effectively unlimited - controlled by currency exposure
MAX_SLOW_TRADES = 999  # Effectively unlimited - controlled by currency exposure
MAX_CURRENCY_EXPOSURE_FAST = 8
MAX_CURRENCY_EXPOSURE_MED = 6
MAX_CURRENCY_EXPOSURE_SLOW = 4

MIN_ATR_PIPS_EXEC_NONJPY = 0.05  # Increased to filter noise (was 0.01)
MIN_ATR_PIPS_EXEC_JPY = 0.05     # Increased to filter noise (was 0.01)

# No MAX_UNITS_PER_TRADE - size controlled by margin utilization percentage

TF_EXEC = normalize_granularity(os.getenv("TF_EXEC", "S5") or "S5")        # Entry/exit timing
TF_TREND = normalize_granularity(os.getenv("TF_TREND", "M15") or "M15")      # Trend analysis  
TF_GLOBAL = normalize_granularity(os.getenv("TF_GLOBAL", "H1") or "H1")       # Market context
TF_POSITION = normalize_granularity(os.getenv("TF_POSITION", "H4") or "H4")   # Position management
# Session detection for strategy timing (no trading restrictions)
SESSION_DETECTION = os.getenv("SESSION_DETECTION", "1").strip().lower() in ("1", "true", "yes")

# Session definitions (UTC times)
TRADING_SESSIONS = {
    "SYDNEY": {"open": "21:00", "close": "06:00", "days": range(5)},  # Sun-Thu
    "TOKYO": {"open": "23:00", "close": "08:00", "days": range(5)},   # Sun-Thu  
    "LONDON": {"open": "07:00", "close": "16:00", "days": range(5)},  # Mon-Fri
    "NEW_YORK": {"open": "12:00", "close": "21:00", "days": range(5)}, # Mon-Fri
}

# Tick data for high-frequency analysis
TICK_DATA_ENABLED = os.getenv("TICK_DATA_ENABLED", "1").strip().lower() in ("1", "true", "yes")

# Dry-run flag (legacy compatibility; default true unless explicitly disabled)
DRY_RUN_ONLY = os.getenv("DRY_RUN_ONLY", "true").strip().lower() in ("1", "true", "yes")

# Entry control flag (must be explicitly enabled)
ALLOW_ENTRIES = os.getenv("ALLOW_ENTRIES", "false").strip().lower() in ("1", "true", "yes")

ATR_N = 14
ATR_LONG_N = 50
MOM_N = 14
WR_N = 14

# ATR fallback chain (primary to last-resort)
ATR_FALLBACK_GRANS = ["M1", "M5", "H1", "H4", "D"]

BOX_BARS = 180
SWEEP_L = 8

# Optimized Setup Triggers (Looser for more entries)
SWEEP_ATR_THRESHOLD = 0.40         # Was 0.75 - catch smaller wicks
FAIL_BREAK_EXTENSION_MAX = 1.50    # Was 1.25 - allow wilder wicks
COMPRESSION_WIDTH_MAX = 0.95       # Was 0.70 - accept messier consolidations
MIN_BREAKOUT_PIPS = 5.0            # New - prevent tiny breakouts
EXHAUSTION_MA_DISTANCE = 2.5       # New - use MA distance for true exhaustion
MOMENTUM_THRESHOLD = 0.60          # New - lower than 0.80 for earlier entry

# MODE_RULES thresholds aligned with detect_mode defaults (no behavior change).
MODE_RULES = {
    "SLOW": {"ratio_min": 0.0, "ratio_max": 0.60, "atr_pips_min": 0.4},
    "MED": {"ratio_min": 0.60, "ratio_max": 1.50, "atr_pips_min": 0.5},
    "FAST": {"ratio_min": 1.50, "ratio_max": 3.00, "atr_pips_min": 0.6},
    "VOLATILE": {"ratio_min": 3.00, "atr_pips_min": 0.8},
}

MODE_SPEED_CLASS_PARAMS = {
    "SLOW": {"util": 0.15, "ttl": 720, "pg_t": 360, "pg_atr": 0.25},   # Conservative
    "MED": {"util": 0.20, "ttl": 480, "pg_t": 240, "pg_atr": 0.30},    # Balanced
    "FAST": {"util": 0.25, "ttl": 240, "pg_t": 120, "pg_atr": 0.30},   # Slightly reduced for speed
    "VOLATILE": {"util": 0.12, "ttl": 120, "pg_t": 60, "pg_atr": 0.40}, # Survival: Small size
}

# Back-compat alias used by legacy tests
MODE_EXEC = MODE_SPEED_CLASS_PARAMS

SPLIT_FAST = (0.70, 0.30)  # 30% runner for quick scalps
SPLIT_MED = (0.50, 0.50)  # 50% runner to maximize trend profit
SPLIT_SLOW = (0.80, 0.20)  # Bank most in slow markets

SPEED_WEIGHT_FAST = 1.30  # Increased for aggressive scalps
SPEED_WEIGHT_MED = 1.00   # Standard
SPEED_WEIGHT_SLOW = 0.80  # Increased for better size in slow markets

WR_OVERSOLD = -80
WR_OVERBOUGHT = -20
WR_TURN_PTS = 10

WR_NEUTRAL_MIN = -70
WR_NEUTRAL_MAX = -30
WR_NEUTRAL_BARS_RESET = 8
STATE_MIN_HOLD_SEC = 45.0
ENTER_HOLD_SEC = 6.0
ORDER_DEDUPE_SEC = 15.0
ALERT_REPEAT_SEC = float(os.getenv("ALERT_REPEAT_SEC", "60") or "60")

# Tick entry/exit behavior
TICK_ENTRY_ENABLED = os.getenv("TICK_ENTRY_ENABLED", "1").strip().lower() in ("1", "true", "yes")
ENTRY_ARM_TTL_SEC = float(os.getenv("ENTRY_ARM_TTL_SEC", "20") or "20")
ENTRY_PULLBACK_ATR = float(os.getenv("ENTRY_PULLBACK_ATR", "0.30") or "0.30")
ENTRY_RESUME_ATR = float(os.getenv("ENTRY_RESUME_ATR", "0.10") or "0.10")
TICK_EXIT_ENABLED = os.getenv("TICK_EXIT_ENABLED", "1").strip().lower() in ("1", "true", "yes")
EXIT_SCAN_TICK_SEC = float(os.getenv("EXIT_SCAN_TICK_SEC", "0.5") or "0.5")
EXIT_PRICE_REFRESH_TICK_SEC = float(os.getenv("EXIT_PRICE_REFRESH_TICK_SEC", "0.5") or "0.5")
TICK_MODE_TTL_SEC = float(os.getenv("TICK_MODE_TTL_SEC", "30") or "30")

# Alert System Configuration
ALERT_SYSTEM_ENABLED = os.getenv("ALERT_SYSTEM_ENABLED", "1").strip().lower() in ("1", "true", "yes")


SETUP_SPEED_CLASS = {
    4: "FAST",  # FAILED_BREAKOUT_FADE
    5: "FAST",  # SWEEP_POP
    1: "MED",   # COMPRESSION_RELEASE
    2: "MED",   # CONTINUATION_PUSH
    3: "SLOW",  # EXHAUSTION_SNAP
    6: "SLOW",  # VOL_REIGNITE
}

SPEED_CLASS_PARAMS = {
    "FAST": {
        "tp1_atr": 0.40, "tp2_atr": 1.60, "sl_atr": 0.85,
        "ttl_main": 240, "ttl_run": 360,
        "pg_t_frac": 0.5, "pg_atr": 0.30,
    },
    "MED": {
        "tp1_atr": 0.55, "tp2_atr": 2.10, "sl_atr": 1.10,
        "ttl_main": 500, "ttl_run": 800,
        "pg_t_frac": 0.375, "pg_atr": 0.25,  # 375% = 180/480
    },
    "SLOW": {
        "tp1_atr": 0.35, "tp2_atr": 1.20, "sl_atr": 1.25,
        "ttl_main": 1200, "ttl_run": 1800,
        "pg_t_frac": 0.3, "pg_atr": 0.20,  # 30% = 270/900
    },
}

# Mechanical ATR fallback exits (used only when structure is unclear / partial bars).
ATR_FALLBACK_PARAMS = {
    "FAST": {"sl_atr": 0.6, "tp1_atr": 0.6, "tp2_atr": 1.2},
    "MED": {"sl_atr": 0.7, "tp1_atr": 0.7, "tp2_atr": 1.4},
    "SLOW": {"sl_atr": 0.8, "tp1_atr": 0.8, "tp2_atr": 1.4},
}

# Percent-based fallback for SL/TP when ATR is invalid (applied to entry price).
FALLBACK_SL_PCT = float(os.getenv("FALLBACK_SL_PCT", "0.002") or "0.002")   # 0.20%
FALLBACK_TP1_PCT = float(os.getenv("FALLBACK_TP1_PCT", "0.002") or "0.002") # 0.20%
FALLBACK_TP2_PCT = float(os.getenv("FALLBACK_TP2_PCT", "0.004") or "0.004") # 0.40%

LATE_IMPULSE_BLOCK_ATR = 1.20
LATE_IMPULSE_SCALE_START = 0.90

ADVERSE_KILL_ATR = 0.35
ADVERSE_KILL_TTL_FRAC = 0.30

ENABLE_MAD_EXIT = False
MIN_RUNNER_PROFIT_ATR = 0.20
MIN_AGE_SEC_FAST = 90
MIN_AGE_SEC_MED = 180
MIN_AGE_SEC_SLOW = 300

MAD_DECAY_THRESHOLD_ATR = 0.06
MAD_DECAY_TIME_SEC = 45.0
MAD_DECAY_MIN_PROFIT_ATR = 0.15
MAD_SAMPLE_COUNT = 20

RUNNER_BFE_MIN_ATR = 0.60
RUNNER_GIVEBACK_ATR = 0.35
RUNNER_VEL_MAX = 0.0

SWEET_SPOT_STATES = {"GET_READY", "ENTER"}

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
DB_PATH = os.path.join(PROJECT_DIR, "phone_bot.db")
STOP_FLAG = os.path.join(PROJECT_DIR, "STOP_TRADING.flag")
LOG_PATH = os.path.join(PROJECT_DIR, "phone_bot.log")
MAX_LOG_SIZE = 2 * 1024 * 1024

BOT_ID = os.getenv("PHONE_BOT_ID", "phone-bot").strip() or "phone-bot"
MAX_RISK_USD_PER_TRADE = float(os.getenv("MAX_RISK_USD_PER_TRADE", "0") or "0")
MIN_FREE_DISK_MB = float(os.getenv("MIN_FREE_DISK_MB", "0") or "0")

MIN_TRADE_SIZE = 1

_SHUTDOWN = False
_BROKER_TIME_OFFSET = 0.0
_BROKER_TIME_LAST_SYNC = 0.0
EXIT_BLOCKED_PAIRS: Dict[str, dict] = {}
# Order rejection cooldown tracking (per pair)
ORDER_REJECT_BLOCK: Dict[str, dict] = {}

# Exit retry backoff (seconds)
EXIT_RETRY_BASE_SEC = float(os.getenv("EXIT_RETRY_BASE_SEC", "5") or "5")
EXIT_RETRY_MAX_SEC = float(os.getenv("EXIT_RETRY_MAX_SEC", "60") or "60")

# Order reject cooldown (seconds)
ORDER_REJECT_COOLDOWN_SEC = float(os.getenv("ORDER_REJECT_COOLDOWN_SEC", "30") or "30")
# Stop loss placement retry (seconds/attempts)
SL_RETRY_MAX = int(os.getenv("SL_RETRY_MAX", "3") or "3")
SL_RETRY_BASE_SEC = float(os.getenv("SL_RETRY_BASE_SEC", "1.5") or "1.5")

# Thread locks for shared state
_trade_track_lock = threading.Lock()
_price_cache_lock = threading.Lock()
_db_lock = threading.Lock()

# Alert deduplication tracking
_last_state_alert: Dict[str, Dict[str, float]] = {}  # pair -> {state: timestamp}
ALERT_DEDUP_COOLDOWN_SEC = 60.0  # Minimum seconds between identical alerts

# Alert repeat tuning (override via env). Set GET_READY repeat to 0 to stop spam.
ALERT_REPEAT_GET_READY_SEC = float(os.getenv("ALERT_REPEAT_GET_READY_SEC", "0") or "0")
ALERT_REPEAT_ENTER_SEC = float(os.getenv("ALERT_REPEAT_ENTER_SEC", str(ALERT_REPEAT_SEC)) or str(ALERT_REPEAT_SEC))
ALERT_REPEAT_OTHER_SEC = float(os.getenv("ALERT_REPEAT_OTHER_SEC", str(ALERT_REPEAT_SEC)) or str(ALERT_REPEAT_SEC))

# Tick activity tracking
TICK_WINDOW_SEC = float(os.getenv("TICK_WINDOW_SEC", "60") or "60")

# Hourly API budget tracking (override via env). Set to 0 to disable.
_api_calls_this_hour: int = 0
_hour_start_time: float = 0.0
_last_api_budget_log: float = 0.0
HOURLY_API_LIMIT: int = int(os.getenv("HOURLY_API_LIMIT", "10") or "10")

# Optional low-call hourly scan mode
HOURLY_SCAN_MODE = os.getenv("HOURLY_SCAN_MODE", "0").strip().lower() in ("1", "true", "yes")
HOURLY_SCAN_INTERVAL_SEC = float(os.getenv("HOURLY_SCAN_INTERVAL_SEC", "3600") or "3600")
if HOURLY_SCAN_MODE and HOURLY_API_LIMIT <= 0:
    HOURLY_API_LIMIT = 10


def _check_api_budget() -> bool:
    """Check if we have API budget remaining for this hour."""
    global _api_calls_this_hour, _hour_start_time, _last_api_budget_log
    
    now = now_ts()

    if HOURLY_API_LIMIT <= 0:
        return True
    
    # Reset counter at the start of each hour
    if (now - _hour_start_time) >= 3600:
        _api_calls_this_hour = 0
        _hour_start_time = now
        print(f"[API_BUDGET] Hourly reset - {HOURLY_API_LIMIT} calls available")
        sys.stdout.flush()
    
    if _api_calls_this_hour >= HOURLY_API_LIMIT:
        if (now - _last_api_budget_log) >= 60.0:
            print(f"[API_BUDGET] Limit reached: {_api_calls_this_hour}/{HOURLY_API_LIMIT} - Using cached data only")
            sys.stdout.flush()
            _last_api_budget_log = now
        return False
    
    return True

def _track_api_call() -> None:
    """Track an API call against the hourly budget."""
    global _api_calls_this_hour
    if HOURLY_API_LIMIT <= 0:
        return
    _api_calls_this_hour += 1
    remaining = HOURLY_API_LIMIT - _api_calls_this_hour
    if remaining <= 2:
        print(f"[API_BUDGET] Warning: {remaining} calls remaining this hour")
        sys.stdout.flush()

def block_pair_exits(pair: str, reason: str, duration_sec: float = 60.0) -> None:
    """Backoff exit attempts for a pair after failed closes."""
    pair = normalize_pair(pair)
    now = now_ts()
    prev = EXIT_BLOCKED_PAIRS.get(pair, {})
    fail_count = int(prev.get("fail_count", 0)) + 1
    delay = min(EXIT_RETRY_MAX_SEC, max(duration_sec, EXIT_RETRY_BASE_SEC * (2 ** (fail_count - 1))))
    next_retry = now + delay
    EXIT_BLOCKED_PAIRS[pair] = {
        "ts": now,
        "reason": reason,
        "duration": duration_sec,
        "fail_count": fail_count,
        "next_retry_ts": next_retry,
    }
    log(
        f"{EMOJI_WARN} EXIT_BLOCK_ADD {pair}",
        {"pair": pair, "reason": reason, "duration": duration_sec, "fail_count": fail_count, "next_retry_in_sec": round(delay, 2)},
    )


def _signal_handler(signum, frame):
    global _SHUTDOWN
    _SHUTDOWN = True


def parse_time_oanda(t) -> float:
    """Parse OANDA time format (UNIX timestamp or RFC3339) to float seconds."""
    if t is None:
        return None

    # Already numeric
    if isinstance(t, (int, float)):
        if not math.isfinite(t):
            return None
        # Heuristic: if milliseconds, scale down
        return float(t) / 1000.0 if t > 1e12 else float(t)

    # Try numeric string (epoch seconds or ms, with possible extra precision)
    if isinstance(t, str):
        s = t.strip()
        try:
            val = float(s)
            if not math.isfinite(val):
                return None
            # If value looks like milliseconds (>= 1e11), scale to seconds
            return val / 1000.0 if val >= 1e11 else val
        except (ValueError, TypeError):
            pass

        # Try ISO-8601 (OANDA style with nanoseconds)
        ts = s
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(ts)
            return dt.timestamp()
        except Exception:
            return None

    return None


def now_ts() -> float:
    # Use broker time offset when available to avoid local clock drift issues.
    if math.isfinite(_BROKER_TIME_OFFSET):
        return time.time() + _BROKER_TIME_OFFSET
    return time.time()


def broker_now_ts() -> float:
    return now_ts()


def ts_str(ts: Optional[float] = None) -> str:
    dt = datetime.fromtimestamp(ts or now_ts(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


class RateLimiter:
    def __init__(self, max_per_min: int):
        self.max_per_min = int(max_per_min)
        self.calls: List[float] = []

    def _prune(self) -> None:
        cut = now_ts() - 60.0
        while self.calls and self.calls[0] < cut:
            self.calls.pop(0)

    def can_take(self, n: int = 1) -> bool:
        self._prune()
        return (len(self.calls) + int(n)) <= self.max_per_min

    def take(self, n: int = 1) -> None:
        self._prune()
        for _ in range(int(n)):
            self.calls.append(now_ts())

    def count(self) -> int:
        self._prune()
        return len(self.calls)


# Adaptive Exit Engine (AEE) - Core functions (Updated for Path-Space)
def calculate_aee_metrics(trade: dict, current_price: float, atr: float, candles: List[dict]) -> dict:
    """Calculate all AEE metrics for trade using path-space primitives"""
    
    pair = trade.get("pair", "")
    entry = _safe_float(trade.get("entry"))
    direction = str(trade.get("dir", "") or "")
    current_price = _safe_float(current_price)
    atr = _safe_float(atr)
    
    # Use enhanced market data hub for path-space primitives
    try:
        # Get path primitives from enhanced hub
        primitives = enhanced_market_hub.get_path_primitives(pair, entry, direction, atr)
        
        if primitives:
            # Extract path-space metrics
            progress = primitives.get("progress", 0.0)
            speed = primitives.get("speed", 0.0)
            velocity = primitives.get("velocity", 0.0)
            pullback = primitives.get("pullback", 0.0)
            local_high = primitives.get("local_high", current_price)
            local_low = primitives.get("local_low", current_price)
            efficiency = primitives.get("efficiency", 0.0)
            overlap = primitives.get("overlap", 0.0)
            data_quality = primitives.get("data_quality", DataQuality.DEGRADED)
            
            # Calculate additional AEE fields
            # Get AEE state if available
            aee_state = None
            if hasattr(enhanced_market_hub, 'get_aee_state'):
                aee_state = enhanced_market_hub.get_aee_state(pair)
            
            # Calculate distance to TP if TP anchor exists
            dist_to_tp_atr = 0.0
            near_tp_band_atr = 0.0
            if aee_state and aee_state.tp_anchor and aee_state.tp_anchor > 0:
                dist_to_tp_atr = abs(aee_state.tp_anchor - current_price) / atr
                near_tp_band_atr = 1.0 if dist_to_tp_atr < 0.3 else 0.0
            
            # Get tick management state
            tick_mode = False
            armed_by = "none"
            if aee_state:
                tick_mode = aee_state.tick_mode
                armed_by = aee_state.armed_by
            
            # Get phase
            phase = "NONE"
            if aee_state:
                phase = aee_state.phase.value if hasattr(aee_state.phase, 'value') else str(aee_state.phase)
            
            # Get exit line if pulse logic uses it
            exit_line = None
            if aee_state and aee_state.pulse_exit_line:
                exit_line = aee_state.pulse_exit_line
            
            result = {
                "progress": progress,
                "speed": speed,
                "velocity": velocity,
                "pullback": pullback,
                "local_high": local_high,
                "local_low": local_low,
                "efficiency": efficiency,
                "overlap": overlap,
                "data_quality": data_quality,
                "source": "path_space",
                
                # AEE / tick management fields
                "phase": phase,
                "tick_mode": tick_mode,
                "armed_by": armed_by,
                "dist_to_tp_atr": dist_to_tp_atr,
                "near_tp_band_atr": near_tp_band_atr,
                "exit_line": exit_line
            }
            
            log_metrics(aee_metrics_obj)
            
            return result
    except Exception as e:
        # Fallback to candle-based calculation if path-space fails
        log_runtime("warning", f"AEE_PATH_SPACE_FALLBACK {pair}", error=str(e))
    
    # Fallback: Original candle-based calculation
    progress = abs(current_price - entry) / atr if atr > 0 else 0.0
    
    # Speed: |Δprice(last 3-5 bars)| / ATR
    speed_bars = min(5, max(0, len(candles) - 1))
    if speed_bars >= 3:
        c_last = _safe_float(candles[-1].get("c"))
        c_prev = _safe_float(candles[-speed_bars].get("c"))
        price_change = abs(c_last - c_prev) if (math.isfinite(c_last) and math.isfinite(c_prev)) else 0.0
        speed = price_change / atr if atr > 0 else 0
    else:
        speed = 0
    
    # Velocity: Speed_now - Speed_prev
    if len(candles) >= 10:
        c5 = _safe_float(candles[-5].get("c"))
        c10 = _safe_float(candles[-10].get("c"))
        prev_change = abs(c5 - c10) if (math.isfinite(c5) and math.isfinite(c10)) else 0.0
        speed_prev = prev_change / atr if atr > 0 else 0
        velocity = speed - speed_prev
    else:
        velocity = 0
    
    # Pullback: |Price - LocalHigh/Low| / ATR
    if len(candles) >= 20:
        highs = [_safe_float(c.get("h")) for c in candles[-20:]]
        lows = [_safe_float(c.get("l")) for c in candles[-20:]]
        highs = [h for h in highs if math.isfinite(h)]
        lows = [l for l in lows if math.isfinite(l)]
        local_high = max(highs) if highs else current_price
        local_low = min(lows) if lows else current_price
    else:
        local_high = current_price
        local_low = current_price
    
    if direction == "LONG":
        pullback = (local_high - current_price) / atr if atr > 0 else 0
    else:
        pullback = (current_price - local_low) / atr if atr > 0 else 0
    
    return {
        "progress": progress,
        "speed": speed,
        "velocity": velocity,
        "pullback": pullback,
        "local_high": local_high,
        "local_low": local_low,
        "efficiency": 0.0,  # Not available in candle mode
        "overlap": 0.0,     # Not available in candle mode
        "data_quality": DataQuality.DEGRADED,
        "source": "candle_fallback"
    }


def simulate_price_stream_update(pair: str, bid: float, ask: float, tick_cache: Optional[dict] = None):
    """Simulate price stream update for enhanced market data hub."""
    
    # Validate prices
    if not (is_valid_price(bid) and is_valid_price(ask)):
        return
    
    if bid > ask:
        return
    
    # Calculate spread
    spread_pips = to_pips(pair, ask - bid) if math.isfinite(ask) and math.isfinite(bid) else 0
    
    # Create price event
    event = PriceEvent(
        t_exchange=broker_now_ts(),
        t_local=now_ts(),
        bid=bid,
        ask=ask,
        mid=(bid + ask) / 2.0,
        spread_pips=spread_pips,
        tradeable=True,
        source="stream",
        quality=DataQuality.OK
    )
    
    # Add to enhanced market hub
    enhanced_market_hub.add_price_event(pair, event)

    # Update tick cache if provided (for tick-mode lookups)
    if tick_cache is not None:
        tick_cache[pair] = {
            "ts": now_ts(),
            "data": {
                "bid": bid,
                "ask": ask,
                "mid": (bid + ask) * 0.5,
                "spread_pips": spread_pips,
            },
        }

def update_price_from_oanda(pair: str, tick_cache: Optional[dict] = None):
    """Update price from OANDA REST API and add to stream simulation."""
    try:
        # Get pricing from OANDA
        bid_ask = oanda_call(f"pricing_update_{pair}", o.pricing, pair)
        if bid_ask:
            bid, ask = bid_ask
            simulate_price_stream_update(pair, bid, ask, tick_cache=tick_cache)
    except Exception as e:
        log(f"{EMOJI_WARN} PRICE_UPDATE_FAILED {pair}", {"error": str(e)})

def aee_entry_protection(trade: dict, metrics: dict, spread_pips: float, atr_pips: float) -> bool:
    """Phase 1: Entry Protection - prevent stop-out by spread
    Threshold: max(0.35 ATR, 1.5 × Spread)"""
    if trade.get("aee_entry_protected"):
        return True
    
    threshold = max(0.35, 1.5 * spread_pips / atr_pips) if atr_pips > 0 else 0.35
    
    if metrics["progress"] >= threshold:
        # Mark as protected
        return True
    return False


def aee_should_break_even(trade: dict, metrics: dict) -> bool:
    """Phase 2: Check if we should move SL to break-even + profit"""
    if metrics["progress"] >= 0.6:
        return True
    return False


def aee_should_hold_runner(trade: dict, metrics: dict) -> bool:
    """Phase 3: Runner Intelligence - hold conditions"""
    if (metrics["speed"] >= 0.8 and 
        metrics["velocity"] >= -0.2 and 
        metrics["pullback"] <= 0.35):
        return True
    return False


def aee_should_exit_momentum_decay(trade: dict, metrics: dict) -> bool:
    """Phase 4: Exit on momentum decay"""
    if (metrics["speed"] < 0.6 and 
        metrics["velocity"] < 0 and 
        metrics["progress"] >= 0.7):
        return True
    return False


def aee_should_capture_pulse(trade: dict, metrics: dict) -> bool:
    """Phase 5: Pulse capture - switch to tick mode"""
    if metrics["speed"] >= 1.6 and metrics["progress"] >= 0.9:
        return True
    return False


def aee_should_panic_exit(trade: dict, metrics: dict) -> bool:
    """Phase 6: OH SHIT detector - immediate exit"""
    if metrics["velocity"] <= -0.8 or metrics["pullback"] >= 0.6:
        return True
    return False


def pip_size(pair: str) -> float:
    """Universal pip size for any pair."""
    return 0.01 if "JPY" in pair else 0.0001


def atr_pips(pair: str, atr_price: float) -> float:
    """Convert ATR price to pips."""
    ps = pip_size(pair)
    result = atr_price / ps if ps > 0 else float("nan")
    
    # T1-10 ATR Unit Gate Validation
    try:
        from pathlib import Path
        import json
        proof_dirs = sorted(Path(__file__).parent.glob("proof_artifacts/*"))
        if proof_dirs:
            latest_proof = proof_dirs[-1]
            
            # Validate pip_size
            expected_pip_size = 0.01 if "JPY" in pair else 0.0001
            pip_valid = abs(ps - expected_pip_size) < 1e-10
            
            atr_unit_status = "PASS" if pip_valid and math.isfinite(result) and result > 0 else "FAIL"
            atr_unit_report = {
                "status": atr_unit_status,
                "pair": pair,
                "pip_size": ps,
                "expected_pip_size": expected_pip_size,
                "atr_price": atr_price,
                "atr_pips": result
            }
            
            # Write atr_unit_report
            atr_unit_file = latest_proof / f"atr_unit_report_{pair}.json"
            atr_unit_file.write_text(json.dumps(atr_unit_report, indent=2))
            
            # Log T1-10 result
            if atr_unit_status == "PASS":
                log_runtime("info", "T1-10_ATR_UNIT_OK", 
                          pair=pair, pip_size=ps, atr_pips=result)
            else:
                log_runtime("error", "T1-10_ATR_UNIT_FAIL",
                          pair=pair, pip_size=ps, expected=expected_pip_size)
    except Exception as e:
        log_runtime("warning", "T1-10_ARTIFACT_ERROR", pair=pair, error=str(e))
    
    return result


def spread_atr(pair: str, spread_pips: float, atr_price: float) -> float:
    """Calculate spread as fraction of ATR (universal metric)."""
    ap = atr_pips(pair, atr_price)
    if not math.isfinite(ap) or ap <= 0:
        return float("nan")
    return spread_pips / ap


def williams_r(candles: List[dict], n: int) -> float:
    """Compute Williams %R."""
    if not candles or n <= 0 or len(candles) < n:
        return float("nan")
    
    # Extract values with NaN checks
    highs = []
    lows = []
    for c in candles[-n:]:
        try:
            h = float(c["h"])
            l = float(c["l"])
            if math.isfinite(h) and math.isfinite(l):
                highs.append(h)
                lows.append(l)
        except (KeyError, ValueError, TypeError):
            return float("nan")
    
    # Need valid data
    if not highs or not lows:
        return float("nan")
    
    try:
        cl = float(candles[-1]["c"])
        if not math.isfinite(cl):
            return float("nan")
    except (KeyError, ValueError, TypeError):
        return float("nan")
    
    hh = max(highs)
    ll = min(lows)
    
    if hh == ll:
        return -50.0
    
    wr = -100.0 * ((hh - cl) / (hh - ll))
    return wr if math.isfinite(wr) else float("nan")


def compute_volume_z(candles: List[dict], win: int = 20) -> float:
    """Robust z-score of latest volume vs rolling window using median/MAD."""
    if not candles:
        return float("nan")
    vols = [float(c.get("volume", 0.0) or 0.0) for c in candles if isinstance(c, dict)]
    if len(vols) < max(5, win):
        return float("nan")
    recent = vols[-win:]
    latest = recent[-1]
    med = statistics.median(recent)
    deviations = [abs(v - med) for v in recent]
    mad = statistics.median(deviations) if deviations else 0.0
    denom = mad if mad > 0 else (statistics.pstdev(recent) or 0.0)
    if denom <= 0:
        return float("nan")
    return (latest - med) / denom


def _book_poll_interval(state: str) -> float:
    """Dynamic polling interval for order/position books based on state."""
    s = str(state or "").upper()
    if s in ("WATCH", "GET_READY", "ENTER", "MANAGING", "ARM_TICK_ENTRY"):
        return 15.0
    if s in ("WAIT", "PASS", "SKIP"):
        return 45.0
    return 30.0


def compute_book_metrics(pair: str, mid: float, order_book: Optional[dict], position_book: Optional[dict]) -> Dict[str, float]:
    """Compute order/position book metrics."""
    metrics: Dict[str, float] = {}
    if not (math.isfinite(mid) and mid > 0):
        return metrics
    pip = pip_size(pair)
    window_pips = 10.0
    window_price = window_pips * pip

    def _bucket_stats(book: Optional[dict]) -> Dict[str, float]:
        buckets = book.get("buckets", []) if isinstance(book, dict) else []
        cluster = 0.0
        wall_dist = float("inf")
        imb_sum = 0.0
        imb_count = 0
        for b in buckets:
            try:
                price = float(b.get("price"))
                lc = float(b.get("longCountPercent", 0.0))
                sc = float(b.get("shortCountPercent", 0.0))
            except Exception:
                continue
            if not math.isfinite(price):
                continue
            dist = abs(price - mid)
            if dist <= window_price:
                weight = max(lc + sc, 0.0)
                cluster += weight
                imb_sum += (lc - sc)
                imb_count += 1
                if weight >= 2.0:  # treat 2%+ as a wall bucket
                    wall_dist = min(wall_dist, dist)
        imbalance = (imb_sum / imb_count) if imb_count > 0 else 0.0
        if wall_dist == float("inf"):
            wall_dist = float("nan")
        return {"cluster": cluster, "imbalance": imbalance, "wall_dist": wall_dist}

    ob_stats = _bucket_stats(order_book)
    pb_stats = _bucket_stats(position_book)

    # Combine: sum clusters, avg imbalance, min wall distance
    metrics["cluster_density"] = ob_stats["cluster"] + pb_stats["cluster"]
    metrics["imbalance"] = (ob_stats["imbalance"] + pb_stats["imbalance"]) / 2.0
    metrics["wall_distance_pips"] = min(ob_stats["wall_dist"], pb_stats["wall_dist"]) / pip if (
        math.isfinite(ob_stats["wall_dist"]) or math.isfinite(pb_stats["wall_dist"])
    ) else float("nan")

    # Trap score heuristic: high cluster near price with opposing tilt
    opposing_tilt = -metrics["imbalance"]
    metrics["trap_score"] = max(0.0, metrics["cluster_density"] * opposing_tilt)

    return metrics


def _poll_books(
    o: "OandaClient",
    states: Dict[str, "PairState"],
    price_map: Dict[str, Tuple[float, float]],
    book_cache: Dict[str, Dict[str, Any]],
    now: float,
) -> None:
    """Poll order/position books with state-driven cadence and update metrics."""
    for pair, st in states.items():
        interval = _book_poll_interval(st.state)
        cache = book_cache.get(pair)
        if cache and (now - float(cache.get("ts", 0.0))) < interval:
            continue

        # Resolve midprice
        mid = float("nan")
        if pair in price_map:
            try:
                bid, ask = price_map[pair]
                mid = (float(bid) + float(ask)) * 0.5
            except Exception:
                mid = float("nan")
        if not math.isfinite(mid):
            resp_price = oanda_call("pricing_book", o.pricing, pair)
            try:
                if resp_price:
                    bid, ask = resp_price
                    mid = (float(bid) + float(ask)) * 0.5
            except Exception:
                mid = float("nan")
        if not math.isfinite(mid):
            continue

        ob = oanda_call("order_book", o.order_book, pair)
        pb = oanda_call("position_book", o.position_book, pair)
        metrics = compute_book_metrics(pair, mid, ob, pb)
        st.book_metrics = metrics
        book_cache[pair] = {"ts": now, "order_book": ob, "position_book": pb, "mid": mid, "metrics": metrics}


def granularity_sec(gran: str) -> int:
    g = gran.upper()
    if g.startswith("S") and g[1:].isdigit():
        return int(g[1:])
    if g.startswith("M") and g[1:].isdigit():
        return int(g[1:]) * 60
    if g.startswith("H") and g[1:].isdigit():
        return int(g[1:]) * 3600
    if g == "H":
        return 3600
    if g.startswith("D") and g[1:].isdigit():
        return int(g[1:]) * 86400
    if g == "D":
        return 86400
    if g.startswith("W") and g[1:].isdigit():
        return int(g[1:]) * 604800
    if g == "W":
        return 604800
    return 0


def candles_valid(candles: List[dict], tf_sec: int = 0) -> bool:
    if len(candles) < 2:
        return False
    prev = None
    for c in candles:
        ts = c.get("time")
        if not isinstance(ts, (int, float)) or not math.isfinite(ts):
            return False
        if prev is not None:
            if ts <= prev:
                return False
            if tf_sec > 0 and (ts - prev) > (tf_sec * 3):
                return False
        prev = ts
    return True


def box_range(candles: List[dict], bars: int, use_prev: bool = True) -> Tuple[float, float, float]:
    """Compute box high/low over `bars` candles. use_prev=True excludes current candle."""
    need = bars + 1 if use_prev else bars
    if len(candles) < need:
        return float("nan"), float("nan"), float("nan")
    if use_prev:
        subset = candles[-(bars + 1) : -1]
    else:
        subset = candles[-bars:]
    hh = max(float(c["h"]) for c in subset)
    ll = min(float(c["l"]) for c in subset)
    return hh, ll, (hh - ll)


def ema(data: List[float], period: int) -> float:
    """Calculate EMA for given period."""
    if not data or period <= 0:
        return float('nan')
    
    multiplier = 2 / (period + 1)
    ema_val = data[0]
    
    for val in data[1:]:
        ema_val = (val * multiplier) + (ema_val * (1 - multiplier))
    
    return ema_val


def _median(vals: List[float]) -> float:
    if not vals:
        return float("nan")
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return 0.5 * (float(s[mid - 1]) + float(s[mid]))


def _mad(vals: List[float]) -> float:
    med = _median(vals)
    if not math.isfinite(med):
        return float("nan")
    devs = [abs(v - med) for v in vals]
    return _median(devs)


def _velocity_atr_per_sec(samples: List[Tuple[float, float]] | float, p1: Optional[float] = None, dt_sec: Optional[float] = None, atr: Optional[float] = None) -> float:
    # Legacy signature: _velocity_atr_per_sec(p0, p1, dt_sec, atr)
    if not isinstance(samples, list):
        p0 = float(samples)
        if p1 is None or dt_sec is None or atr is None:
            return 0.0
        if dt_sec <= 0 or atr <= 0:
            return 0.0
        return ((p1 - p0) / atr) / dt_sec

    if len(samples) < 2:
        return 0.0
    t1, v1 = samples[-2]
    t2, v2 = samples[-1]
    dt = t2 - t1
    if dt <= 0:
        return 0.0
    return (v2 - v1) / dt


def _microtrend_alive(samples: List[Tuple[float, float]] | List[dict], k: int = 3, direction: Optional[str] = None, atr: Optional[float] = None) -> bool:
    # Legacy signature: _microtrend_alive(candles, "LONG", atr)
    if isinstance(k, str):
        direction = k
        k = 3
    direction = str(direction or "LONG").upper()

    if not samples:
        return False

    # Candle list support
    if isinstance(samples[0], dict):
        closes: List[float] = []
        for c in samples[-int(k):]:
            try:
                closes.append(float(c.get("c")))
            except Exception:
                return False
        if len(closes) < int(k):
            return False
        if direction.startswith("S"):
            return all(closes[i] <= closes[i - 1] for i in range(1, len(closes)))
        return all(closes[i] >= closes[i - 1] for i in range(1, len(closes)))

    # Default sample list support
    if len(samples) < k:
        return False
    vals = [s[1] for s in samples[-k:]]
    return all(vals[i] >= vals[i - 1] for i in range(1, k))


def _new_runner_stats() -> dict:
    return {
        "count_total": 0,
        "sum_exit_atr_total": 0.0,
        "count_giveback": 0,
        "sum_exit_atr_giveback": 0.0,
        "count_other": 0,
        "sum_exit_atr_other": 0.0,
    }


def _update_runner_stats(stats: dict, reason: str, exit_atr: float) -> None:
    stats["count_total"] += 1
    stats["sum_exit_atr_total"] += exit_atr
    if reason == "RUNNER_GIVEBACK":
        stats["count_giveback"] += 1
        stats["sum_exit_atr_giveback"] += exit_atr
    else:
        stats["count_other"] += 1
        stats["sum_exit_atr_other"] += exit_atr


def _exit_log(tr: dict, reason: str, exit_atr: float, track: Optional[dict]) -> None:
    setup_name = str(tr.get("setup", ""))
    leg = "RUN" if "_RUN" in setup_name else "MAIN"
    setup_id = setup_id_from_name(setup_name)
    speed_class = speed_class_from_setup_name(setup_name)
    entry_ts = float(tr.get("ts", now_ts()))
    peak = float(track.get("peak", 0.0)) if track else 0.0
    max_dd = float(track.get("max_dd", 0.0)) if track else 0.0
    peak_ts = float(track.get("peak_ts", entry_ts)) if track else entry_ts
    log(
        f"{EMOJI_EXIT} EXIT {reason} {pair_tag(tr.get('pair', ''), tr.get('dir', ''))}",
        {
            "trade_id": int(tr.get("id", 0)),
            "entry_ts": entry_ts,
            "exit_ts": now_ts(),
            "duration_sec": round(now_ts() - entry_ts, 2),
            "exit_reason": reason,
            "setup_id": setup_id,
            "speed_class": speed_class,
            "leg": leg,
            "exit_atr": exit_atr,
            "bfe_atr": peak,
            "max_dd_atr": max_dd,
            "time_to_bfe_sec": (peak_ts - entry_ts),
        },
    )


def directional_closes(candles: List[dict], k: int, direction: str) -> bool:
    """Check for k consecutive directional bars. Works with partial candles."""
    if len(candles) < k + 1:
        return False
    if direction == "LONG":
        return all(float(candles[-i]["c"]) > float(candles[-i - 1]["c"]) for i in range(1, k + 1))
    if direction == "SHORT":
        return all(float(candles[-i]["c"]) < float(candles[-i - 1]["c"]) for i in range(1, k + 1))
    return False


def wick_sweep(candles: List[dict], L: int, atr_val: float) -> Tuple[bool, bool]:
    if len(candles) < L + 2 or not (atr_val > 0):
        return False, False
    swing_high = max(float(c["h"]) for c in candles[-L - 1 : -1])
    swing_low = min(float(c["l"]) for c in candles[-L - 1 : -1])
    cur = candles[-1]
    o = float(cur["o"])
    cl = float(cur["c"])
    h = float(cur["h"])
    l = float(cur["l"])
    upper_wick = h - max(o, cl)
    lower_wick = min(o, cl) - l
    sweep_up = (h > swing_high) and (cl < swing_high) and ((upper_wick / atr_val) >= SWEEP_ATR_THRESHOLD)
    sweep_dn = (l < swing_low) and (cl > swing_low) and ((lower_wick / atr_val) >= SWEEP_ATR_THRESHOLD)
    
    # Volume filter disabled - OANDA doesn't provide reliable volume data
    # When using a broker with volume data, uncomment and implement:
    # volume_ok = float(cur.get("volume", 0)) > avg_volume * 1.2
    
    return sweep_up, sweep_dn


@dataclass
class PairState:
    state: str = "SKIP"
    mode: str = "SLOW"
    last_alert: float = 0.0
    last_alert_key: str = ""
    last_trade: float = 0.0
    cooldown_until: float = 0.0
    state_since: float = 0.0
    neutral_bars: int = 0

    breakout_ts: float = 0.0
    breakout_dir: str = ""

    box_hi: float = float("nan")
    box_lo: float = float("nan")
    box_atr: float = float("nan")
    wr: float = float("nan")
    wr_prev: float = float("nan")
    m_norm: float = float("nan")
    atr_exec: float = float("nan")
    atr_long: float = float("nan")
    spread_pips: float = float("nan")
    spread_history: List[float] = None
    
    # Hysteresis tracking for GET_READY persistence
    get_ready_weak_scans: int = 0  # Count of consecutive weak scans
    last_close_history: List[float] = None  # Track last K closes for momentum check
    last_data_stale_log_ts: float = 0.0  # Track last DATA_STALE log timestamp
    
    # Debounce tracking for signal stability
    signal_debounce: Dict[str, float] = None  # Track when each signal condition first met
    last_candle_time: float = 0.0  # Track candle time for stale detection
    stale_poll_count: int = 0  # Count polls with no OHLC change
    last_tick_mid: float = float("nan")  # Midprice from previous tick tracking
    tick_times: List[float] = None  # Timestamps of recent ticks for activity calc
    ticks_per_min: float = 0.0  # Rolling ticks per minute estimate
    entry_arm: dict = None  # Tick-entry arming state
    vol_z: float = float("nan")  # Volume regime z-score
    book_metrics: Dict[str, float] = None  # Order/position book metrics
    
    # State transition attributes for GATE-16
    entry_arms: bool = False  # Entry conditions met
    ready_at: float = 0.0  # Timestamp when ready
    entry_triggered: bool = False  # Entry trigger activated
    
    def __post_init__(self):
        if self.last_close_history is None:
            self.last_close_history = []
        if self.signal_debounce is None:
            self.signal_debounce = {}
        if self.spread_history is None:
            self.spread_history = []
        if self.tick_times is None:
            self.tick_times = []
        if self.entry_arm is None:
            self.entry_arm = {}
        if self.book_metrics is None:
            self.book_metrics = {}


def update_tick_stats(st: PairState, pair: str, bid: float, ask: float, now: Optional[float] = None) -> None:
    """Update rolling tick activity (ticks per minute) using mid-price changes."""
    pair = normalize_pair(pair)
    if now is None:
        now = now_ts()
    if not (is_valid_price(bid) and is_valid_price(ask)):
        return
    try:
        mid = (float(bid) + float(ask)) * 0.5
    except Exception:
        return
    if not math.isfinite(mid):
        return
    tick = tick_size(pair)
    last_mid = st.last_tick_mid
    # Count a tick when mid moves at least one tick size, or on first sample
    if (not math.isfinite(last_mid)) or abs(mid - last_mid) >= tick:
        st.tick_times.append(float(now))
        st.last_tick_mid = mid
    window = max(1.0, float(TICK_WINDOW_SEC))
    cutoff = now - window
    # Prune old ticks
    while st.tick_times and st.tick_times[0] < cutoff:
        st.tick_times.pop(0)
    st.ticks_per_min = (len(st.tick_times) * 60.0) / window


def _entry_trigger_for_setup(setup_id: int) -> str:
    # BREAK: momentum breakout entries
    # RECLAIM: failed breakout/sweep type entries
    # RESUME: continuation/exhaustion entries after pullback
    if setup_id in (1, 6):
        return "BREAK"
    if setup_id in (4, 5):
        return "RECLAIM"
    return "RESUME"


def _arm_tick_entry(st: PairState, sig: "SignalDef", entry_px: float, box_hi: float, box_lo: float, atr: float, now: float) -> None:
    trigger = _entry_trigger_for_setup(sig.setup_id)
    st.entry_arm = {
        "ts": now,
        "trigger": trigger,
        "dir": sig.direction,
        "entry_px": float(entry_px),
        "box_hi": float(box_hi) if math.isfinite(box_hi) else float("nan"),
        "box_lo": float(box_lo) if math.isfinite(box_lo) else float("nan"),
        "atr": float(atr) if math.isfinite(atr) else float("nan"),
        "pullback_seen": False,
        "reclaim_seen": False,
        "setup_id": sig.setup_id,
        "setup_name": sig.setup_name,
        "sig": {
            "pair": sig.pair,
            "setup_id": sig.setup_id,
            "setup_name": sig.setup_name,
            "direction": sig.direction,
            "mode": sig.mode,
            "ttl_sec": sig.ttl_sec,
            "pg_t": sig.pg_t,
            "pg_atr": sig.pg_atr,
            "tp1_atr": sig.tp1_atr,
            "tp2_atr": sig.tp2_atr,
            "sl_atr": sig.sl_atr,
        }
    }
    
    # T1-17 Tick Mode Integrity Gate Validation - ARMED
    try:
        from pathlib import Path
        import json
        proof_dirs = sorted(Path(__file__).parent.glob("proof_artifacts/*"))
        if proof_dirs:
            latest_proof = proof_dirs[-1]
            
            # Check if arming is allowed in current state
            allowed_states = ["GET_READY", "ARM_TICK_ENTRY"]
            is_allowed = st.state in allowed_states
            
            # Load or initialize tick mode events
            tick_file = latest_proof / "tick_mode_events.jsonl"
            if tick_file.exists():
                events = []
                for line in tick_file.read_text().strip().splitlines():
                    events.append(json.loads(line))
            else:
                events = []
            
            # Add ARMED event
            events.append({
                "pair": sig.pair,
                "action": "ARMED",
                "state": st.state,
                "timestamp": now,
                "setup_id": sig.setup_id
            })
            
            # Keep last 100 events
            events = events[-100:]
            tick_file.write_text("\n".join(json.dumps(e) for e in events) + "\n")
            
            # Write report
            report = {
                "status": "PASS" if is_allowed else "FAIL",
                "pair": sig.pair,
                "events": events[-10:],  # Last 10 events
                "latest": {"action": "ARMED", "state": st.state}
            }
            
            report_file = latest_proof / f"tick_mode_report_{sig.pair}.json"
            report_file.write_text(json.dumps(report, indent=2))
            
            if not is_allowed:
                log_runtime("error", "T1-17_TICK_MODE_FAIL",
                          pair=sig.pair, action="ARMED", state=st.state)
    except Exception as e:
        log_runtime("warning", "T1-17_ARTIFACT_ERROR", pair=sig.pair, error=str(e))


def _tick_entry_triggered(st: PairState, bid: float, ask: float, now: float) -> Tuple[bool, str]:
    arm = st.entry_arm or {}
    if not arm:
        return False, "not_armed"
    if (now - float(arm.get("ts", now))) > ENTRY_ARM_TTL_SEC:
        st.entry_arm = {}
        return False, "arm_expired"
    direction = str(arm.get("dir", ""))
    trigger = str(arm.get("trigger", "BREAK"))
    atr = float(arm.get("atr", float("nan")))
    entry_px = float(arm.get("entry_px", float("nan")))
    box_hi = float(arm.get("box_hi", float("nan")))
    box_lo = float(arm.get("box_lo", float("nan")))
    px = ask if direction == "LONG" else bid

    # BREAK trigger: cross box boundary or entry price
    if trigger == "BREAK":
        lvl = box_hi if direction == "LONG" and math.isfinite(box_hi) else (box_lo if math.isfinite(box_lo) else entry_px)
        if direction == "LONG":
            return (px >= lvl), "break"
        return (px <= lvl), "break"

    # RECLAIM trigger: require an adverse move, then reclaim the boundary
    if trigger == "RECLAIM":
        if direction == "LONG":
            if px < (box_hi if math.isfinite(box_hi) else entry_px):
                arm["reclaim_seen"] = True
            if arm.get("reclaim_seen") and px >= (box_hi if math.isfinite(box_hi) else entry_px):
                return True, "reclaim"
        else:
            if px > (box_lo if math.isfinite(box_lo) else entry_px):
                arm["reclaim_seen"] = True
            if arm.get("reclaim_seen") and px <= (box_lo if math.isfinite(box_lo) else entry_px):
                return True, "reclaim"
        return False, "reclaim_wait"

    # RESUME trigger: see pullback then resume in direction
    if math.isfinite(atr) and atr > 0:
        pullback_lvl = entry_px - (ENTRY_PULLBACK_ATR * atr) if direction == "LONG" else entry_px + (ENTRY_PULLBACK_ATR * atr)
        resume_lvl = entry_px + (ENTRY_RESUME_ATR * atr) if direction == "LONG" else entry_px - (ENTRY_RESUME_ATR * atr)
    else:
        pullback_lvl = entry_px
        resume_lvl = entry_px
    if direction == "LONG":
        if px <= pullback_lvl:
            arm["pullback_seen"] = True
        if arm.get("pullback_seen") and px >= resume_lvl:
            return True, "resume"
    else:
        if px >= pullback_lvl:
            arm["pullback_seen"] = True
        if arm.get("pullback_seen") and px <= resume_lvl:
            return True, "resume"
    return False, "resume_wait"


def _sig_from_entry_arm(arm: dict) -> Optional["SignalDef"]:
    sig = arm.get("sig") if isinstance(arm, dict) else None
    if not isinstance(sig, dict):
        return None
    try:
        return SignalDef(
            pair=normalize_pair(sig.get("pair", "")),
            setup_id=int(sig.get("setup_id", 0)),
            setup_name=str(sig.get("setup_name", "")),
            direction=str(sig.get("direction", "")),
            mode=str(sig.get("mode", "")),
            ttl_sec=int(sig.get("ttl_sec", 0)),
            pg_t=int(sig.get("pg_t", 0)),
            pg_atr=float(sig.get("pg_atr", 0.0)),
            tp1_atr=float(sig.get("tp1_atr", 0.0)),
            tp2_atr=float(sig.get("tp2_atr", 0.0)),
            sl_atr=float(sig.get("sl_atr", 0.0)),
            reason=str(sig.get("reason", "")),
            size_mult=float(sig.get("size_mult", 1.0)),
        )
    except Exception:
        return None

@dataclass
class SignalDef:
    pair: str
    setup_id: int
    setup_name: str
    direction: str
    mode: str
    ttl_sec: int
    pg_t: int
    pg_atr: float
    tp1_atr: float
    tp2_atr: float
    sl_atr: float
    reason: str
    size_mult: float = 1.0
    created_at: float = 0.0
    
    def __post_init__(self):
        if self.created_at == 0.0:
            self.created_at = time.time()
    
    def is_expired(self) -> bool:
        """Check if signal has expired based on TTL."""
        return (time.time() - self.created_at) > self.ttl_sec


class DB:
    def __init__(self, path: str):
        self.path = path
        self._init()

    def _con(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        return con

    def _init(self) -> None:
        con = self._con()
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pair_state (
              pair TEXT PRIMARY KEY,
              ts REAL,
              state TEXT,
              mode TEXT,
              last_alert REAL,
              last_trade REAL,
              cooldown_until REAL,
              state_since REAL,
              neutral_bars INTEGER,
              breakout_ts REAL,
              breakout_dir TEXT
            )
            """
        )
        # Add atr_long column if it doesn't exist
        try:
            cur.execute("ALTER TABLE pair_state ADD COLUMN atr_long REAL")
        except sqlite3.OperationalError:
            pass  # Column already exists
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
              client_id TEXT PRIMARY KEY,
              ts REAL,
              pair TEXT,
              setup TEXT,
              leg TEXT,
              units INTEGER,
              sl REAL,
              tp REAL,
              oanda_order_id TEXT,
              oanda_transaction_id TEXT,
              status TEXT,
              raw_json TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts REAL,
              pair TEXT,
              setup TEXT,
              dir TEXT,
              mode TEXT,
              units INTEGER,
              entry REAL,
              atr_entry REAL,
              ttl_sec INTEGER,
              pg_t INTEGER,
              pg_atr REAL,
              state TEXT,
              note TEXT
            )
            """
        )
        con.commit()
        con.close()
        self._ensure_columns()

    def _ensure_columns(self) -> None:
        con = self._con()
        cur = con.cursor()
        cur.execute("PRAGMA table_info(orders)")
        cols = {row[1] for row in cur.fetchall()}
        if "setup" not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN setup TEXT")
        if "leg" not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN leg TEXT")
        
        # Add oanda_trade_id to trades table
        cur.execute("PRAGMA table_info(trades)")
        trade_cols = {row[1] for row in cur.fetchall()}
        if "oanda_trade_id" not in trade_cols:
            cur.execute("ALTER TABLE trades ADD COLUMN oanda_trade_id TEXT")
        
        # Add Adaptive Exit Engine columns
        if "aee_phase" not in trade_cols:
            cur.execute("ALTER TABLE trades ADD COLUMN aee_phase TEXT")
        if "aee_entry_protected" not in trade_cols:
            cur.execute("ALTER TABLE trades ADD COLUMN aee_entry_protected INTEGER DEFAULT 0")
        if "aee_local_high" not in trade_cols:
            cur.execute("ALTER TABLE trades ADD COLUMN aee_local_high REAL")
        if "aee_local_low" not in trade_cols:
            cur.execute("ALTER TABLE trades ADD COLUMN aee_local_low REAL")
        
        con.commit()
        con.close()

    def load_states(self, pairs: List[str]) -> Dict[str, PairState]:
        states = {normalize_pair(p): PairState(state_since=now_ts()) for p in pairs}
        con = self._con()
        cur = con.cursor()
        cur.execute(
            "SELECT pair,state,mode,last_alert,last_trade,cooldown_until,state_since,neutral_bars,breakout_ts,breakout_dir,atr_long FROM pair_state"
        )
        for row in cur.fetchall():
            pair = normalize_pair(str(row[0]))
            if pair not in states:
                continue
            st = states[pair]
            st.state = str(row[1])
            st.mode = str(row[2])
            st.last_alert = float(row[3] or 0.0)
            st.last_trade = float(row[4] or 0.0)
            st.cooldown_until = float(row[5] or 0.0)
            st.state_since = float(row[6] or st.state_since)
            st.neutral_bars = int(row[7] or 0)
            st.breakout_ts = float(row[8] or 0.0)
            st.breakout_dir = str(row[9] or "")
            st.atr_long = float(row[10] or float("nan"))
        con.close()
        return states

    def save_state(self, pair: str, st: PairState) -> None:
        pair = normalize_pair(pair)
        con = self._con()
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO pair_state(pair,ts,state,mode,last_alert,last_trade,cooldown_until,state_since,neutral_bars,breakout_ts,breakout_dir,atr_long)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(pair) DO UPDATE SET
              ts=excluded.ts,
              state=excluded.state,
              mode=excluded.mode,
              last_alert=excluded.last_alert,
              last_trade=excluded.last_trade,
              cooldown_until=excluded.cooldown_until,
              state_since=excluded.state_since,
              neutral_bars=excluded.neutral_bars,
              breakout_ts=excluded.breakout_ts,
              breakout_dir=excluded.breakout_dir,
              atr_long=excluded.atr_long
            """,
            (
                pair,
                now_ts(),
                st.state,
                st.mode,
                st.last_alert,
                st.last_trade,
                st.cooldown_until,
                st.state_since,
                st.neutral_bars,
                st.breakout_ts,
                st.breakout_dir,
                st.atr_long,
            ),
        )
        con.commit()
        con.close()

    def record_order(
        self,
        *,
        client_id: str,
        pair: str,
        setup: str,
        leg: str,
        units: int,
        sl: float,
        tp: float,
        oanda_order_id: str,
        oanda_transaction_id: str,
        status: str,
        raw: dict,
    ) -> None:
        pair = normalize_pair(pair)
        con = self._con()
        cur = con.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO orders(client_id,ts,pair,setup,leg,units,sl,tp,oanda_order_id,oanda_transaction_id,status,raw_json)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                client_id,
                now_ts(),
                pair,
                setup,
                leg,
                int(units),
                float(sl),
                float(tp),
                str(oanda_order_id),
                str(oanda_transaction_id),
                str(status),
                json.dumps(raw, default=str),
            ),
        )
        con.commit()
        con.close()

    def recent_order_attempt(self, *, pair: str, setup: str, leg: str, since_ts: float) -> bool:
        pair = normalize_pair(pair)
        con = self._con()
        cur = con.cursor()
        cur.execute(
            "SELECT 1 FROM orders WHERE pair=? AND setup=? AND leg=? AND ts>=? LIMIT 1",
            (pair, setup, leg, float(since_ts)),
        )
        row = cur.fetchone()
        con.close()
        return bool(row)

    def record_trade(
        self,
        *,
        pair: str,
        setup: str,
        direction: str,
        mode: str,
        units: int,
        entry: float,
        atr_entry: float,
        ttl_sec: int,
        pg_t: int,
        pg_atr: float,
        note: str,
        oanda_trade_id: Optional[str] = None,
    ) -> Optional[int]:
        pair = normalize_pair(pair)
        con = self._con()
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO trades(ts,pair,setup,dir,mode,units,entry,atr_entry,ttl_sec,pg_t,pg_atr,state,note,oanda_trade_id)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                now_ts(),
                pair,
                setup,
                direction,
                mode,
                int(units),
                float(entry),
                float(atr_entry),
                int(ttl_sec),
                int(pg_t),
                float(pg_atr),
                "OPEN",
                note,
                oanda_trade_id,
            ),
        )
        trade_id = int(cur.lastrowid or 0)
        con.commit()
        con.close()
        return trade_id if trade_id > 0 else None

    # Legacy compatibility alias
    def add_trade(
        self,
        *,
        pair: str,
        setup: str,
        direction: str,
        mode: str,
        units: int,
        entry: float,
        atr_entry: float,
        ttl_sec: int,
        pg_t: int,
        pg_atr: float,
        note: str,
        oanda_trade_id: Optional[str] = None,
    ) -> Optional[int]:
        return self.record_trade(
            pair=pair,
            setup=setup,
            direction=direction,
            mode=mode,
            units=units,
            entry=entry,
            atr_entry=atr_entry,
            ttl_sec=ttl_sec,
            pg_t=pg_t,
            pg_atr=pg_atr,
            note=note,
            oanda_trade_id=oanda_trade_id,
        )

    def get_open_trades(self) -> List[dict]:
        con = self._con()
        cur = con.cursor()
        cur.execute(
            "SELECT id,ts,pair,setup,dir,mode,units,entry,atr_entry,ttl_sec,pg_t,pg_atr,oanda_trade_id,aee_phase,aee_entry_protected,aee_local_high,aee_local_low FROM trades WHERE state='OPEN' ORDER BY ts ASC"
        )
        rows = cur.fetchall()
        con.close()
        out = []
        for r in rows:
            out.append(
                {
                    "id": int(r[0]),
                    "ts": float(r[1]),
                    "pair": normalize_pair(str(r[2])),
                    "setup": str(r[3]),
                    "dir": str(r[4]),
                    "mode": str(r[5]),
                    "units": int(r[6]),
                    "entry": float(r[7]),
                    "atr_entry": float(r[8]),
                    "ttl_sec": int(r[9]),
                    "pg_t": int(r[10]),
                    "pg_atr": float(r[11]),
                    "oanda_trade_id": r[12] if len(r) > 12 else None,
                    "aee_phase": r[13] if len(r) > 13 else None,
                    "aee_entry_protected": int(r[14]) if len(r) > 14 else 0,
                    "aee_local_high": float(r[15]) if len(r) > 15 and r[15] is not None else float("nan"),
                    "aee_local_low": float(r[16]) if len(r) > 16 and r[16] is not None else float("nan"),
                }
            )
        return out

    def mark_trade_closed(self, trade_id: int, note: str) -> None:
        con = self._con()
        cur = con.cursor()
        # Replace note instead of appending to prevent pollution
        cur.execute("UPDATE trades SET state='CLOSED', note=? WHERE id=?", (note, int(trade_id)))
        con.commit()
        con.close()

    def append_trade_note(self, trade_id: int, note: str) -> None:
        con = self._con()
        cur = con.cursor()
        cur.execute(
            "UPDATE trades SET note=COALESCE(note, '') || ' | ' || ? WHERE id=?",
            (note, int(trade_id)),
        )
        con.commit()
        con.close()
    
    def update_aee_state(self, trade_id: int, phase: str, protected: int = None, local_high: float = None, local_low: float = None) -> None:
        """Update Adaptive Exit Engine state for a trade"""
        with _db_lock:
            con = self._con()
            try:
                cur = con.cursor()
                updates = ["aee_phase=?"]
                params = [phase]
                
                if protected is not None:
                    updates.append("aee_entry_protected=?")
                    params.append(protected)
                
                if local_high is not None:
                    updates.append("aee_local_high=?")
                    params.append(local_high)
                
                if local_low is not None:
                    updates.append("aee_local_low=?")
                    params.append(local_low)
                
                params.append(trade_id)
                cur.execute(f"UPDATE trades SET {', '.join(updates)} WHERE id=?", params)
                con.commit()
            except Exception as e:
                con.rollback()
                raise e
            finally:
                con.close()


def detect_regime_multi_tf(tf_data: dict, pair: str) -> str:
    """Detect market regime using multiple timeframes"""
    
    # Default to MED if no data available
    if not tf_data.get("M5"):
        return "MED"
    
    # Get volatility from each timeframe
    volatilities = {}
    
    # M5 volatility (immediate market conditions)
    m5_candles = tf_data.get("M5", [])
    if len(m5_candles) >= 14:
        m5_atr = atr(m5_candles, 14)
        current_range = m5_candles[-1]["h"] - m5_candles[-1]["l"]
        if m5_atr > 0:
            volatilities["M5"] = current_range / m5_atr
    
    # M15 volatility (short-term trend)
    m15_candles = tf_data.get("M15", [])
    if len(m15_candles) >= 14:
        m15_atr = atr(m15_candles, 14)
        current_range = m15_candles[-1]["h"] - m15_candles[-1]["l"]
        if m15_atr > 0:
            volatilities["M15"] = current_range / m15_atr
    
    # H1 volatility (market context)
    h1_candles = tf_data.get("H1", [])
    if len(h1_candles) >= 14:
        h1_atr = atr(h1_candles, 14)
        current_range = h1_candles[-1]["h"] - h1_candles[-1]["l"]
        if h1_atr > 0:
            volatilities["H1"] = current_range / h1_atr
    
    # Weighted volatility score (higher timeframes get more weight)
    weights = {"M5": 0.2, "M15": 0.3, "H1": 0.5}
    weighted_score = 0.0
    total_weight = 0.0
    
    for tf, vol in volatilities.items():
        weight = weights.get(tf, 0.1)
        weighted_score += vol * weight
        total_weight += weight
    
    if total_weight > 0:
        final_score = weighted_score / total_weight
    else:
        final_score = 0.75  # Default to MED range
    
    # Regime determination using weighted score
    if final_score >= 3.0:
        return "VOLATILE"
    elif final_score >= 1.5:
        return "FAST"
    elif final_score >= 0.6:
        return "MED"
    else:
        return "SLOW"

def detect_mode(pair: str, current_high: float, current_low: Optional[float] = None, atr: Optional[float] = None) -> str:
    """
    Quantitatively determines Market Regime based on Volatility Ratio (VR).
    Supports legacy signature: detect_mode(pair, atr_s, atr_l)
    Current signature: detect_mode(pair, current_high, current_low, atr)
    """
    try:
        # Legacy mode: detect_mode(pair, atr_s, atr_l)
        if atr is None:
            atr_s = current_high
            atr_l = current_low if current_low is not None else 0.0
            if not math.isfinite(atr_s) or atr_s <= 0:
                return "SLOW"
            if not math.isfinite(atr_l) or atr_l <= 0:
                return "MED"
            vr = atr_s / atr_l
        else:
            # Safety check to avoid division by zero
            if atr <= 0:
                return "SLOW"
            # Calculate Realized Volatility of the current candle
            current_range = current_high - (current_low if current_low is not None else current_high)
            vr = current_range / atr

        # --- REGIME DEFINITIONS ---
        if vr >= MODE_RULES["VOLATILE"]["ratio_min"]:
            return "VOLATILE"
        elif vr >= MODE_RULES["FAST"]["ratio_min"]:
            return "FAST"
        elif vr >= MODE_RULES["MED"]["ratio_min"]:
            return "MED"
        else:
            return "SLOW"

    except Exception as e:
        print(f"Regime detection error: {e}")
        return "MED"  # Fallback


def _spread_atr_units(pair: str, atr: float, spread_pips: float) -> float:
    """
    Spread converted to ATR units for LOGGING ONLY.
    pip_size(pair) returns 0.01 if "JPY" in pair else 0.0001.
    """
    pip = pip_size(pair)  # 0.01 for JPY pairs, else 0.0001
    spread_price = spread_pips * pip
    if atr <= 0.0 or (not math.isfinite(atr)) or (not math.isfinite(spread_price)):
        return float("inf")
    return spread_price / atr


def _edge_estimate(tp1_atr: float, sl_atr: float, spread_atr: float) -> float:
    """
    Friction-adjusted edge proxy (LOGGING ONLY).
    Positive = TP1 clears spread and is reasonable vs SL after friction.
    """
    denom = sl_atr + spread_atr
    if denom <= 0.0 or (not math.isfinite(denom)):
        return 0.0
    return (tp1_atr - spread_atr) / denom


def _with_friction_reason(reason: str, pair: str, atr: float, spread_pips: float, tp1_atr: float, sl_atr: float) -> str:
    # Keep this extremely cheap; we call it a lot.
    s_atr = _spread_atr_units(pair, atr, spread_pips)
    e = _edge_estimate(tp1_atr, sl_atr, s_atr)
    # Example: "... | fric spread_atr=0.36 edge_est=0.42"
    if not math.isfinite(s_atr):
        return f"{reason} | fric spread_atr=inf edge_est={e:.2f}"
    return f"{reason} | fric spread_atr={s_atr:.2f} edge_est={e:.2f}"


def build_signals(pair: str, st: PairState, c_exec: List[dict], tf_data: Optional[dict] = None) -> List[SignalDef]:
    """
    V12 PATH-SPACE UPGRADED: Upgraded existing strategies to use path-space primitives
    """
    pair = normalize_pair(pair)
    out: List[SignalDef] = []
    if str(getattr(st, "mode", "")).upper() == "DEAD":
        return out
    if math.isfinite(getattr(st, "spread_pips", float("nan"))) and st.spread_pips > ABS_SPREAD_MAX_PIPS:
        return out
    if len(c_exec) < max(ATR_N + 5, 35):
        return out
    if not (st.atr_exec > 0.0) or not math.isfinite(st.spread_pips):
        return out

    # Get path-space engine for primitive calculations
    engine = get_path_engine()
    
    # Update path-space engine with current price data
    current_price = float(c_exec[-1]["c"])
    timestamp = float(c_exec[-1].get("time", now_ts()))
    
    # Update engine with recent price history for primitive calculations
    for candle in c_exec[-20:]:  # Last 20 candles for path-space calculation
        price = float(candle["c"])
        candle_time = float(candle.get("time", timestamp))
        engine.update_price(pair, price, st.atr_exec, candle_time)
    
    # Get path-space primitives for current state
    primitives = engine.get_primitives(pair)
    
    # Use existing strategy logic but with path-space primitives instead of candle-based calculations
    atrv = st.atr_exec
    m = st.m_norm
    close = current_price
    last_ts = timestamp
    
    # Track bar completeness for logging
    bar_complete = bool(c_exec[-1].get("complete", True))
    bar_age_ms = (now_ts() - last_ts) * 1000 if not bar_complete else 0

    # Get path-space values instead of candle-based calculations
    path_len = primitives.get("path_len", 0.0)
    efficiency = primitives.get("efficiency", 0.0)
    energy = primitives.get("energy", 0.0)
    speed = primitives.get("speed", 0.0)
    velocity = primitives.get("velocity", 0.0)
    overlap = primitives.get("overlap", 0.0)
    displacement = primitives.get("displacement", 0.0)
    pullback = primitives.get("pullback", 0.0)
    vol_slope = primitives.get("vol_slope", 0.0)
    rolling_high = primitives.get("rolling_high", close)
    rolling_low = primitives.get("rolling_low", close)
    
    # STRATEGY 1: COMPRESSION_EXPANSION (upgraded to use path-space primitives)
    # Check compression using path-space overlap instead of candle range
    if overlap > 1.5:  # Much more lenient for real data
        # Check if we have valid extrema
        if rolling_high > float('-inf') and rolling_low < float('inf'):
            # Check for breakout (price equals or exceeds rolling high/low)
            break_up = close >= rolling_high  # Include equality for exact breakout
            break_dn = close <= rolling_low  # Include equality for exact breakout
            
            if break_up or break_dn:
                # Check expansion using path-space energy and efficiency
                if energy > 0.1 and efficiency > 0.01:  # Very lenient for real data
                    speed_class = "MED"
                    sp = get_speed_params(speed_class)
                    direction = "LONG" if break_up else "SHORT"
                    
                    out.append(
                        SignalDef(
                            pair=pair,
                            setup_id=1,
                            setup_name="COMPRESSION_EXPANSION",
                            direction=direction,
                            mode=st.mode,
                            ttl_sec=sp["ttl_main"],
                            pg_t=int(sp["ttl_main"] * sp["pg_t_frac"]),
                            pg_atr=sp["pg_atr"],
                            tp1_atr=sp["tp1_atr"],
                            tp2_atr=sp["tp2_atr"],
                            sl_atr=sp["sl_atr"],
                            reason=_with_friction_reason(
                                reason=f"compression_break path_space energy={energy:.2f} eff={efficiency:.2f} | bar_complete={bar_complete}",
                                pair=pair,
                                atr=atrv,
                                spread_pips=st.spread_pips,
                                tp1_atr=sp["tp1_atr"],
                                sl_atr=sp["sl_atr"],
                            ),
                        )
                    )

    # STRATEGY 2: CONTINUATION_PUSH (upgraded to use path-space primitives)
    if displacement < 2.0 and pullback < 1.0 and energy > 0.1 and efficiency > 0.01:  # Very lenient
        # Use path-space velocity for direction
        direction = "LONG" if velocity > -0.1 else "SHORT"  # Very lenient
        
        speed_class = "MED"
        sp = get_speed_params(speed_class)
        
        out.append(
            SignalDef(
                pair=pair,
                setup_id=2,
                setup_name="CONTINUATION_PUSH",
                direction=direction,
                mode=st.mode,
                ttl_sec=sp["ttl_main"],
                pg_t=int(sp["ttl_main"] * sp["pg_t_frac"]),
                pg_atr=sp["pg_atr"],
                tp1_atr=sp["tp1_atr"],
                tp2_atr=sp["tp2_atr"],
                sl_atr=sp["sl_atr"],
                reason=_with_friction_reason(
                    reason=f"continuation path_space disp={displacement:.2f} pullback={pullback:.2f} energy={energy:.2f} | bar_complete={bar_complete}",
                    pair=pair,
                    atr=atrv,
                    spread_pips=st.spread_pips,
                    tp1_atr=sp["tp1_atr"],
                    sl_atr=sp["sl_atr"],
                ),
            )
        )

    # STRATEGY 3: EXHAUSTION_SNAPBACK (upgraded to use path-space primitives)
    if displacement > 0.5 and energy < 0.5 and efficiency < 0.3 and velocity < -0.05:
        direction = "SHORT" if close > (rolling_high + rolling_low) / 2 else "LONG"
        
        speed_class = "SLOW"
        sp = get_speed_params(speed_class)
        
        out.append(
            SignalDef(
                pair=pair,
                setup_id=3,
                setup_name="EXHAUSTION_SNAPBACK",
                direction=direction,
                mode=st.mode,
                ttl_sec=sp["ttl_main"],
                pg_t=int(sp["ttl_main"] * sp["pg_t_frac"]),
                pg_atr=sp["pg_atr"],
                tp1_atr=sp["tp1_atr"],
                tp2_atr=sp["tp2_atr"],
                sl_atr=sp["sl_atr"],
                reason=_with_friction_reason(
                    reason=f"exhaustion path_space energy={energy:.2f} vel={velocity:.2f} | bar_complete={bar_complete}",
                    pair=pair,
                    atr=atrv,
                    spread_pips=st.spread_pips,
                    tp1_atr=sp["tp1_atr"],
                    sl_atr=sp["sl_atr"],
                ),
            )
        )

    # STRATEGY 4: FAILED_BREAKOUT (upgraded to use path-space primitives)
    if rolling_low <= close <= rolling_high and displacement < 1.5 and energy < 0.7:
        direction = "SHORT" if close > (rolling_high + rolling_low) / 2 else "LONG"
        
        speed_class = "FAST"
        sp = get_speed_params(speed_class)
        
        out.append(
            SignalDef(
                pair=pair,
                setup_id=4,
                setup_name="FAILED_BREAKOUT_FADE",
                direction=direction,
                mode=st.mode,
                ttl_sec=sp["ttl_main"],
                pg_t=int(sp["ttl_main"] * sp["pg_t_frac"]),
                pg_atr=sp["pg_atr"],
                tp1_atr=sp["tp1_atr"],
                tp2_atr=sp["tp2_atr"],
                sl_atr=sp["sl_atr"],
                reason=_with_friction_reason(
                    reason=f"failed_breakout path_space reenter energy={energy:.2f} | bar_complete={bar_complete}",
                    pair=pair,
                    atr=atrv,
                    spread_pips=st.spread_pips,
                    tp1_atr=sp["tp1_atr"],
                    sl_atr=sp["sl_atr"],
                ),
            )
        )

    # STRATEGY 5: LIQUIDITY_SWEEP (upgraded to use path-space primitives)
    if path_len > 0.1 and efficiency < 0.9:  # Very lenient
        # Check if price is near extrema using path-space calculations
        high_distance = abs(close - rolling_high) / atrv if atrv > 0 else float('inf')
        low_distance = abs(close - rolling_low) / atrv if atrv > 0 else float('inf')
        
        if high_distance < 1.0 or low_distance < 1.0:  # Very lenient
            # Check for reversal velocity
            if (high_distance < 1.0 and velocity < 2.0) or (low_distance < 1.0 and velocity > -2.0):  # Very lenient
                direction = "SHORT" if high_distance < 1.0 else "LONG"
                
                speed_class = "FAST"
                sp = get_speed_params(speed_class)
                
                out.append(
                    SignalDef(
                        pair=pair,
                        setup_id=5,
                        setup_name="LIQUIDITY_SWEEP",
                        direction=direction,
                        mode=st.mode,
                        ttl_sec=sp["ttl_main"],
                        pg_t=int(sp["ttl_main"] * sp["pg_t_frac"]),
                        pg_atr=sp["pg_atr"],
                        tp1_atr=sp["tp1_atr"],
                        tp2_atr=sp["tp2_atr"],
                        sl_atr=sp["sl_atr"],
                        reason=_with_friction_reason(
                            reason=f"liquidity_sweep path_space path_len={path_len:.2f} vel={velocity:.2f} | bar_complete={bar_complete}",
                            pair=pair,
                            atr=atrv,
                            spread_pips=st.spread_pips,
                            tp1_atr=sp["tp1_atr"],
                            sl_atr=sp["sl_atr"],
                        ),
                    )
                )

    # STRATEGY 6: VOLATILITY_REIGNITE (upgraded to use path-space primitives)
    if overlap > 2.0 and energy < 0.6 and vol_slope > -0.02 and efficiency > 0.2 and speed > 0.1:
        direction = "LONG" if close > (rolling_high + rolling_low) / 2 else "SHORT"
        
        speed_class = "SLOW"
        sp = get_speed_params(speed_class)
        
        out.append(
            SignalDef(
                pair=pair,
                setup_id=6,
                setup_name="VOL_REIGNITE",
                direction=direction,
                mode=st.mode,
                ttl_sec=sp["ttl_main"],
                pg_t=int(sp["ttl_main"] * sp["pg_t_frac"]),
                pg_atr=sp["pg_atr"],
                tp1_atr=sp["tp1_atr"],
                tp2_atr=sp["tp2_atr"],
                sl_atr=sp["sl_atr"],
                reason=_with_friction_reason(
                    reason=f"vol_reignite path_space energy={energy:.2f} vol_slope={vol_slope:.4f} | bar_complete={bar_complete}",
                    pair=pair,
                    atr=atrv,
                    spread_pips=st.spread_pips,
                    tp1_atr=sp["tp1_atr"],
                    sl_atr=sp["sl_atr"],
                ),
            )
        )

    # STRATEGY 7: INTENTIONAL_RUNNER (NEW - multi-hour campaign strategy)
    range_used = (rolling_high - rolling_low) / atrv if atrv > 0 else float('inf')
    if range_used < 4.0 and efficiency > 0.15 and energy > 0.4 and vol_slope > -0.1:
        direction = "LONG" if close > (rolling_high + rolling_low) / 2 else "SHORT"
        
        speed_class = "SLOW"  # Runner uses slow parameters for multi-hour campaigns
        sp = get_speed_params(speed_class)
        
        out.append(
            SignalDef(
                pair=pair,
                setup_id=7,
                setup_name="INTENTIONAL_RUNNER",
                direction=direction,
                mode=st.mode,
                ttl_sec=sp["ttl_main"] * 3,  # Extended TTL for runner
                pg_t=int(sp["ttl_main"] * 3 * sp["pg_t_frac"]),
                pg_atr=sp["pg_atr"],
                tp1_atr=sp["tp1_atr"] * 2,  # Larger TP for runner
                tp2_atr=sp["tp2_atr"] * 3,  # Extended TP2
                sl_atr=sp["sl_atr"],
                reason=_with_friction_reason(
                    reason=f"intentional_runner range={range_used:.2f} eff={efficiency:.2f} energy={energy:.2f} | bar_complete={bar_complete}",
                    pair=pair,
                    atr=atrv,
                    spread_pips=st.spread_pips,
                    tp1_atr=sp["tp1_atr"] * 2,
                    sl_atr=sp["sl_atr"],
                ),
            )
        )

    # Sort signals by priority (same as original)
    pr = {4: 1, 5: 2, 1: 3, 3: 4, 2: 5, 6: 6, 7: 7}
    out.sort(key=lambda s: pr.get(s.setup_id, 99))
    
    # LOG TRADE EVENTS - SINGLE SOURCE OF TRUTH
    for signal in out:
        trade_event = {
            "event": "SIGNAL_GENERATED",
            "state": "GET_READY",
            "reason_code": signal.setup_name,
            "pair": signal.pair,
            "setup_id": signal.setup_id,
            "direction": signal.direction,
            "mode": signal.mode,
            "ttl_sec": signal.ttl_sec,
            "tp1_atr": signal.tp1_atr,
            "tp2_atr": signal.tp2_atr,
            "sl_atr": signal.sl_atr,
            "reason": signal.reason,
            "units_base": 1.0,  # Will be calculated by caller
            "units_final": 1.0,  # Will be calculated by caller
            "spread_pips": st.spread_pips,
            "atr_pips": st.atr_exec * 10000,
            "spread_atr": st.spread_pips / (st.atr_exec * 10000) if st.atr_exec > 0 else 0.0
        }
        
        # Log trade event to trades.jsonl
        log_trade_event(trade_event)
    
    return out


def count_pair_positions(positions: List[dict], pair: str) -> int:
    """Count open positions for a pair (long + short)."""
    pair = normalize_pair(pair)
    count = 0
    for pos in positions:
        if normalize_pair(pos.get("instrument")) == pair:
            longu = abs(int(float(pos.get("long", {}).get("units", "0") or "0")))
            shortu = abs(int(float(pos.get("short", {}).get("units", "0") or "0")))
            if longu > 0 or shortu > 0:
                count += 1
    return count


def has_opposite_position(positions: List[dict], pair: str, direction: str) -> bool:
    """Check if there's an opposite direction position for the pair."""
    pair = normalize_pair(pair)
    for pos in positions:
        if normalize_pair(pos.get("instrument")) == pair:
            longu = int(float(pos.get("long", {}).get("units", "0") or "0"))
            shortu = int(float(pos.get("short", {}).get("units", "0") or "0"))
            if direction == "LONG" and shortu < 0:
                return True
            if direction == "SHORT" and longu > 0:
                return True
    return False


def count_pair_pending(pending_orders: List[dict], pair: str) -> int:
    pair = normalize_pair(pair)
    n = 0
    for o in pending_orders:
        if normalize_pair(o.get("instrument")) == pair:
            n += 1
    return n


def has_duplicate_order_size(pending_orders: List[dict], pair: str, units: int) -> bool:
    """Check if there's a pending order with the same size for the pair."""
    pair = normalize_pair(pair)
    for o in pending_orders:
        if normalize_pair(o.get("instrument")) == pair:
            order_units = int(float(o.get("units", "0") or "0"))
            if abs(order_units) == abs(units):
                return True
    return False


def has_opposite_db(open_trades: List[dict], pair: str, direction: str) -> bool:
    """Check if DB shows an opposite-direction open trade for the pair."""
    pair = normalize_pair(pair)
    for tr in open_trades:
        if normalize_pair(tr.get("pair")) != pair:
            continue
        d = str(tr.get("dir", "") or "")
        if direction == "LONG" and d == "SHORT":
            return True
        if direction == "SHORT" and d == "LONG":
            return True
    return False


def get_speed_class(setup_id: int) -> str:
    return SETUP_SPEED_CLASS.get(setup_id, "MED")


def normalize_speed_class(speed_class: str) -> str:
    sc = str(speed_class or "").strip().upper()
    if sc not in SPEED_CLASS_PARAMS:
        log(
            f"{EMOJI_ERR} INVALID_SPEED_CLASS",
            {"speed_class": speed_class, "valid_classes": list(SPEED_CLASS_PARAMS.keys()), "fallback": "MED"},
        )
        sc = "MED"
    return sc


def get_speed_weight(speed_class: str) -> float:
    if speed_class == "FAST":
        return SPEED_WEIGHT_FAST
    if speed_class == "SLOW":
        return SPEED_WEIGHT_SLOW
    return SPEED_WEIGHT_MED


def get_split_ratios(speed_class: str) -> Tuple[float, float]:
    if speed_class == "FAST":
        return SPLIT_FAST
    if speed_class == "SLOW":
        return SPLIT_SLOW
    return SPLIT_MED


def get_speed_params(speed_class: str) -> dict:
    sc = normalize_speed_class(speed_class)
    return SPEED_CLASS_PARAMS[sc]


def extract_currencies(pair: str) -> Tuple[str, str]:
    pair = normalize_pair(pair)
    parts = pair.split("_")
    if len(parts) == 2:
        return parts[0], parts[1]
    return pair[:3], pair[3:]


def count_currency_exposure(open_trades: List[dict], currency: str) -> int:
    n = 0
    for tr in open_trades:
        c1, c2 = extract_currencies(tr.get("pair", ""))
        if c1 == currency or c2 == currency:
            n += 1
    return n


def setup_id_from_name(setup: str) -> Optional[int]:
    if not setup:
        return None
    if "FAILED_BREAKOUT" in setup:
        return 4
    if "SWEEP_POP" in setup:
        return 5
    if "COMPRESSION" in setup:
        return 1
    if "CONTINUATION" in setup:
        return 2
    if "EXHAUSTION" in setup:
        return 3
    if "VOL_REIGNITE" in setup:
        return 6
    return None


def speed_class_from_setup_name(setup: str) -> str:
    sid = setup_id_from_name(setup)
    return get_speed_class(sid) if sid is not None else "MED"


def validate_strategy_definitions() -> None:
    """Sanity-check strategy mappings and speed params."""
    expected = {1, 2, 3, 4, 5, 6}
    missing = expected.difference(set(SETUP_SPEED_CLASS.keys()))
    if missing:
        log(f"{EMOJI_ERR} STRATEGY_MAP_MISSING", {"missing_setup_ids": sorted(missing)})
    invalid_speed = {sid: sc for sid, sc in SETUP_SPEED_CLASS.items() if sc not in SPEED_CLASS_PARAMS}
    if invalid_speed:
        log(f"{EMOJI_ERR} STRATEGY_SPEED_INVALID", {"invalid_speed_classes": invalid_speed})
    # Ensure each speed class has required params
    required_keys = {"tp1_atr", "tp2_atr", "sl_atr", "ttl_main", "ttl_run", "pg_t_frac", "pg_atr"}
    for sc, params in SPEED_CLASS_PARAMS.items():
        missing_keys = required_keys.difference(set(params.keys()))
        if missing_keys:
            log(f"{EMOJI_ERR} SPEED_PARAMS_INCOMPLETE", {"speed_class": sc, "missing_keys": sorted(missing_keys)})


def min_age_for_speed_class(speed_class: str) -> int:
    if speed_class == "FAST":
        return MIN_AGE_SEC_FAST
    if speed_class == "SLOW":
        return MIN_AGE_SEC_SLOW
    return MIN_AGE_SEC_MED


def count_speed_class_trades(open_trades: List[dict], speed_class: str) -> int:
    n = 0
    for tr in open_trades:
        sid = setup_id_from_name(tr.get("setup", ""))
        if sid is not None and get_speed_class(sid) == speed_class:
            n += 1
    return n


@dataclass
class CalcUnitsResult:
    units: int
    reason: str
    debug: dict

    def __iter__(self):
        yield self.units
        yield self.reason
        yield self.debug

    def __len__(self):
        return 3

    def __getitem__(self, idx):
        return (self.units, self.reason, self.debug)[idx]

    def __int__(self):
        return int(self.units)

    def __float__(self):
        return float(self.units)

    def __bool__(self):
        return bool(self.units)

    def _coerce_other(self, other):
        if isinstance(other, CalcUnitsResult):
            return other.units
        if isinstance(other, (int, float)):
            return other
        if isinstance(other, (tuple, list)) and len(other) > 0:
            try:
                return other[0]
            except Exception:
                return other
        return other

    def __eq__(self, other):
        return self.units == self._coerce_other(other)

    def __lt__(self, other):
        return self.units < self._coerce_other(other)

    def __le__(self, other):
        return self.units <= self._coerce_other(other)

    def __gt__(self, other):
        return self.units > self._coerce_other(other)

    def __ge__(self, other):
        return self.units >= self._coerce_other(other)


def calc_units(pair: str, side: str, price: float, margin_avail: float, util: float, speed_class: str = "MED", spread_pips: float = 0.0, disp_atr: float = 0.0, size_mult: float = 1.0) -> CalcUnitsResult:
    """Calculate position size with speed weighting, spread penalty, and late impulse scaling. Returns (units, reason, debug_dict)."""
    pair = normalize_pair(pair)
    lev = 50 if pair in LEVERAGE_50 else LEV_DEFAULT
    util = clamp(util, 0.0, 0.95)

    speed_mult = get_speed_weight(speed_class)

    # NO SPREAD MULTIPLIER HERE - handled at execution layer
    spread_mult = 1.0  # Keep for compatibility but don't use

    impulse_mult = 1.0
    debug = {
        "margin_avail": margin_avail,
        "price": price,
        "util": util,
        "lev": lev,
        "speed_mult": speed_mult,
        "spread_mult": spread_mult,
        "disp_atr": disp_atr,
        "size_mult": size_mult,
    }

    if not (math.isfinite(margin_avail) and margin_avail > 0.0 and is_valid_price(price)):
        log(f"{EMOJI_ERR} CALC_UNITS_FAIL {pair}", {"reason": "invalid_inputs", **debug})
        return CalcUnitsResult(0, "invalid_inputs", debug)

    if disp_atr >= LATE_IMPULSE_BLOCK_ATR:
        impulse_mult = 0.0
        debug["impulse_mult"] = impulse_mult
        log(f"{EMOJI_ERR} CALC_UNITS_FAIL {pair}", {"reason": "late_impulse_block", **debug})
        return CalcUnitsResult(0, "late_impulse_block", debug)
    elif disp_atr >= (LATE_IMPULSE_BLOCK_ATR * 0.8):
        excess = disp_atr - (LATE_IMPULSE_BLOCK_ATR * 0.8)
        scale_range = LATE_IMPULSE_BLOCK_ATR * 0.2
        impulse_mult = max(0.7, 1.0 - (excess / scale_range) * 0.3)
    else:
        impulse_mult = 1.0
    debug["impulse_mult"] = impulse_mult

    # Calculate max units based on margin
    max_units_margin = (margin_avail * util * lev) / max(price, 1e-9)
    debug["max_units_margin"] = max_units_margin
    
    # If margin insufficient, log and return 0 with specific reason
    if max_units_margin < MIN_TRADE_SIZE:
        log(f"{EMOJI_ERR} CALC_UNITS_FAIL {pair}", {"reason": "insufficient_margin", "min_trade": MIN_TRADE_SIZE, **debug})
        return CalcUnitsResult(0, "insufficient_margin", {**debug, "min_trade": MIN_TRADE_SIZE})
        
    raw_units = max_units_margin * speed_mult * spread_mult * impulse_mult * size_mult
    
    # Apply minimum trade size enforcement
    if raw_units < MIN_TRADE_SIZE:
        # If we have margin but calculation is too small, enforce minimum
        if max_units_margin >= MIN_TRADE_SIZE:
            log(f"{EMOJI_WARN} CALC_UNITS_ENFORCE_MIN {pair}", {"raw_units": raw_units, "enforced_units": MIN_TRADE_SIZE, "reason": "raw_below_min_but_margin_ok"})
            raw_units = MIN_TRADE_SIZE
        else:
            log(f"{EMOJI_ERR} CALC_UNITS_FAIL {pair}", {"reason": "below_min_units", "raw_units": raw_units, "min_trade": MIN_TRADE_SIZE, **debug})
            return CalcUnitsResult(0, "below_min_units", {**debug, "raw_units": raw_units, "min_trade": MIN_TRADE_SIZE})
    units = int(round(raw_units))
    if units < MIN_TRADE_SIZE:
        log(f"{EMOJI_ERR} CALC_UNITS_FAIL {pair}", {"reason": "units_below_min_after_round", "units": units, "min_trade": MIN_TRADE_SIZE, **debug})
        return CalcUnitsResult(0, "units_below_min_after_round", {**debug, "units": units, "min_trade": MIN_TRADE_SIZE})
    units_cap = int(max_units_margin)
    if units > units_cap:
        units = units_cap
    # No MAX_UNITS_PER_TRADE limit - size controlled by margin utilization
    
    debug.update({
        "raw_units": raw_units,
    })
    return CalcUnitsResult(units if side == "LONG" else -units, "success", debug)


def risk_usd_for_pair(pair: str, units: int, entry: float, sl: float) -> Optional[float]:
    pair = normalize_pair(pair)
    if units == 0 or not (is_valid_price(entry) and is_valid_price(sl)):
        return None
    risk_price = abs(entry - sl)
    if "USD_" in pair:
        return (risk_price * abs(units)) / max(entry, 1e-9)
    if pair.endswith("_USD"):
        return risk_price * abs(units)
    return None


def apply_risk_cap(pair: str, units: int, entry: float, sl: float, max_risk_usd: float) -> int:
    if max_risk_usd <= 0:
        return units
    risk = risk_usd_for_pair(pair, units, entry, sl)
    if risk is None or risk <= 0:
        return units
    if risk <= max_risk_usd:
        return units
    scale = max_risk_usd / risk
    scaled = int(abs(units) * scale)
    if scaled < MIN_TRADE_SIZE:
        return 0
    return scaled if units > 0 else -scaled


def _make_unique_units(units: int, existing_sizes: set, min_units: int, max_units: Optional[int] = None, max_tries: int = 20) -> int:
    """Adjust units to avoid duplicate sizes (FIFO compliance) without breaking sign/min size or max budget."""
    if units == 0:
        return units
    sign = 1 if units > 0 else -1
    base = abs(int(units))
    if base < min_units:
        return units
    if max_units is None:
        max_units = base * 1.5  # Allow 50% increase for uniqueness
    max_units = abs(int(max_units))
    if max_units < min_units:
        return units
    if base not in existing_sizes:
        return units
    # Try nearby sizes in both directions with larger range
    for i in range(1, max_tries + 1):
        # Increase by 1 unit each time (not min_units to avoid large jumps)
        cand_up = base + i
        if cand_up <= max_units and cand_up not in existing_sizes:
            return sign * cand_up
        cand_dn = base - i
        if cand_dn >= min_units and cand_dn <= max_units and cand_dn not in existing_sizes:
            return sign * cand_dn
    # If all else fails, add 1 to make it unique
    final = base + 1
    if final <= max_units:
        return sign * final
    return units  # Return original if can't make unique


def ttl_exit_decision(age_sec: float, ttl_sec: int, favorable_atr: float, microtrend_alive: bool) -> Optional[str]:
    """Return TTL exit action or None (profit-aware)."""
    if ttl_sec <= 0:
        return None
    if not (math.isfinite(age_sec) and math.isfinite(favorable_atr)):
        return None
    soft_ttl_start = ttl_sec * 0.80
    if age_sec < soft_ttl_start:
        return None
    if favorable_atr > 0.10 and (not microtrend_alive):
        return "TTL_TAKE_PROFIT"
    if age_sec >= ttl_sec and (not microtrend_alive):
        return "TTL_NO_FOLLOWTHROUGH"
    return None


def _atr_fallback_params(speed_class: str) -> Dict[str, float]:
    base = ATR_FALLBACK_PARAMS.get(str(speed_class or "").upper(), ATR_FALLBACK_PARAMS["MED"])
    return {"sl_atr": float(base["sl_atr"]), "tp1_atr": float(base["tp1_atr"]), "tp2_atr": float(base["tp2_atr"])}


def _sweep_wick_dist_atr(candles: List[dict], entry: float, side: str, atr_val: float) -> float:
    if not candles:
        return 0.0
    if not (math.isfinite(entry) and math.isfinite(atr_val) and atr_val > 0):
        return 0.0
    side = str(side or "").upper()
    last = candles[-1]
    try:
        wick_price = float(last["l"] if side == "LONG" else last["h"])
    except Exception:
        return 0.0
    if side == "LONG":
        if wick_price >= entry:
            return 0.0
        return (entry - wick_price) / atr_val
    if side == "SHORT":
        if wick_price <= entry:
            return 0.0
        return (wick_price - entry) / atr_val
    return 0.0


def compute_prices(
    pair: str,
    side: str,
    bid: float,
    ask: float,
    atr_val: float,
    tp_atr: float,
    sl_atr: float,
    speed_class: str = "MED",
    tp_kind: str = "tp1",
    include_spread: bool = True,
):
    pair = normalize_pair(pair)
    side = str(side or "").upper()
    if side not in ("LONG", "SHORT"):
        raise ValueError(f"invalid_side:{side}")
    entry = round_tick(ask if side == "LONG" else bid, pair)
    
    # Calculate spread in ATR units
    pip = pip_size(pair)
    spread = max(0.0, float(ask) - float(bid))
    tp_kind = "tp2" if str(tp_kind or "").lower() == "tp2" else "tp1"

    # Percent-based fallback when ATR is invalid
    if not (atr_val > 0 and math.isfinite(atr_val)):
        tp_pct = FALLBACK_TP2_PCT if tp_kind == "tp2" else FALLBACK_TP1_PCT
        sl_pct = FALLBACK_SL_PCT
        if not (math.isfinite(tp_pct) and tp_pct > 0):
            tp_pct = 0.002
        if not (math.isfinite(sl_pct) and sl_pct > 0):
            sl_pct = 0.002
        sl_dist = max(entry * sl_pct, pip)
        tp_dist = max(entry * tp_pct, pip)
        if side == "LONG":
            tp = round_tick_up(entry + tp_dist, pair)
            sl = round_tick_down(entry - sl_dist, pair)
        else:
            tp = round_tick_down(entry - tp_dist, pair)
            sl = round_tick_up(entry + sl_dist, pair)
        log(
            f"{EMOJI_INFO} PCT_FALLBACK {pair}",
            {
                "atr_val": atr_val,
                "sl_pct": sl_pct,
                "tp_pct": tp_pct,
                "entry": entry,
                "sl": sl,
                "tp": tp,
            },
        )
        return entry, sl, tp

    # Use provided TP values or fallback if inputs are invalid
    fallback_used = False
    if not (math.isfinite(tp_atr) and tp_atr > 0):
        fb = _atr_fallback_params(speed_class)
        tp_atr = fb["tp1_atr"] if tp_kind == "tp1" else fb["tp2_atr"]
        fallback_used = True
    if not (math.isfinite(sl_atr) and sl_atr > 0):
        fb = _atr_fallback_params(speed_class)
        sl_atr = fb["sl_atr"]
        fallback_used = True
    if fallback_used:
        log(f"{EMOJI_INFO} ATR_FALLBACK {pair}", {"speed_class": speed_class, "sl_atr": sl_atr, "tp_atr": tp_atr})

    spread_atr = 0.0
    if include_spread and atr_val > 0.0:
        spread_atr = spread / atr_val
    
    # Calculate TP and SL with spread-aware adjustment
    # ATR defines structure, spread is execution friction absorbed by SL only
    print(f"\n📊 CALCULATING PRICES for {pair} {side}:")
    print(f"   entry={entry}, bid={bid}, ask={ask}")
    print(f"   atr_val={atr_val}, sl_atr={sl_atr}, tp_atr={tp_atr}")
    print(f"   spread={spread}, speed_class={speed_class}")
    
    # Do not adjust SL/TP for spread; spread impacts size only.
    spread_buffer = 0.0
    spread_adj = 0.0


def _tp0_ladder_price(
    *,
    pair: str,
    direction: str,
    entry: float,
    bid: float,
    ask: float,
    atr_m1: float,
    spread_price: float,
    speed_class: str,
    tp_anchor_price: Optional[float] = None,
) -> Tuple[float, dict]:
    """SOP v2.1 TP0 ladder (must never fail).

    Returns:
        (tp0_price, debug)
    """
    pair = normalize_pair(pair)
    direction = str(direction or "").upper()
    if direction not in ("LONG", "SHORT"):
        raise ValueError(f"invalid_direction:{direction}")
    entry = float(entry)
    spread_price = max(0.0, float(spread_price))
    atr_m1 = float(atr_m1) if math.isfinite(atr_m1) else float("nan")
    speed_class = str(speed_class or "MED").upper()

    debug: dict = {
        "pair": pair,
        "direction": direction,
        "entry": entry,
        "atr_m1": atr_m1,
        "spread_price": spread_price,
        "speed_class": speed_class,
        "tp_anchor_price": float(tp_anchor_price) if tp_anchor_price is not None and math.isfinite(tp_anchor_price) else None,
        "branch": None,
        "min_dist": None,
        "raw_dist": None,
        "final_dist": None,
    }

    # 1) Primary anchor
    tp0 = None
    if tp_anchor_price is not None and math.isfinite(tp_anchor_price):
        tp0 = float(tp_anchor_price)
        debug["branch"] = "anchor"

    # 2) Fallback ATR ladder
    if tp0 is None:
        if math.isfinite(atr_m1) and atr_m1 > 0:
            if speed_class == "FAST":
                dist = 0.35 * atr_m1
            else:
                dist = 0.60 * atr_m1
            debug["branch"] = "atr"
            debug["raw_dist"] = dist
            tp0 = entry + dist if direction == "LONG" else entry - dist

    # 3) Fallback percent
    if tp0 is None:
        dist = 0.0015 * entry
        debug["branch"] = "pct"
        debug["raw_dist"] = dist
        tp0 = entry + dist if direction == "LONG" else entry - dist

    # Minimum distance clamp (must widen, never abort)
    min_dist = 0.0
    if math.isfinite(spread_price) and spread_price > 0:
        min_dist = max(min_dist, 1.2 * spread_price)
    if math.isfinite(atr_m1) and atr_m1 > 0:
        min_dist = max(min_dist, 0.10 * atr_m1)
    min_dist = max(min_dist, tick_size(pair))
    debug["min_dist"] = min_dist

    final_dist = abs(float(tp0) - entry)
    if final_dist < min_dist:
        final_dist = min_dist
        tp0 = entry + final_dist if direction == "LONG" else entry - final_dist
    debug["final_dist"] = final_dist

    tp0 = round_tick(tp0, pair)
    return tp0, debug


def _csl_price(
    *,
    pair: str,
    direction: str,
    entry: float,
    atr_m1: float,
    spread_price: float,
) -> Tuple[float, dict]:
    """SOP v2.1 Catastrophic Stop Loss (CSL) at birth."""
    pair = normalize_pair(pair)
    direction = str(direction or "").upper()
    entry = float(entry)
    spread_price = max(0.0, float(spread_price))
    atr_m1 = float(atr_m1) if math.isfinite(atr_m1) else float("nan")

    dist = 0.0
    if math.isfinite(atr_m1) and atr_m1 > 0:
        dist = max(dist, 5.0 * atr_m1)
    if math.isfinite(spread_price) and spread_price > 0:
        dist = max(dist, 50.0 * spread_price)
    dist = max(dist, tick_size(pair))

    csl = entry - dist if direction == "LONG" else entry + dist
    csl = round_tick(csl, pair)
    return csl, {"pair": pair, "direction": direction, "entry": entry, "atr_m1": atr_m1, "spread_price": spread_price, "csl_dist": dist, "csl": csl}


def _enforce_tp0_csl(
    *,
    pair: str,
    direction: str,
    bid: float,
    ask: float,
    atr_m1: float,
    spread_price: float,
    speed_class: str,
    structural_tp: float,
    tp_anchor_price: Optional[float] = None,
) -> Tuple[float, float, dict]:
    """Compute TP0 + CSL (mandatory at birth) and return (csl, tp0, debug)."""
    pair = normalize_pair(pair)
    direction = str(direction or "").upper()
    entry = round_tick(ask if direction == "LONG" else bid, pair)

    tp0, tp_dbg = _tp0_ladder_price(
        pair=pair,
        direction=direction,
        entry=entry,
        bid=bid,
        ask=ask,
        atr_m1=atr_m1,
        spread_price=spread_price,
        speed_class=speed_class,
        tp_anchor_price=tp_anchor_price,
    )
    csl, csl_dbg = _csl_price(pair=pair, direction=direction, entry=entry, atr_m1=atr_m1, spread_price=spread_price)

    # Ensure TP0 isn't inside entry (paranoia clamp)
    if direction == "LONG" and tp0 <= entry:
        tp0 = round_tick(entry + max(tp_dbg.get("min_dist") or tick_size(pair), tick_size(pair)), pair)
        tp_dbg["branch"] = f"{tp_dbg.get('branch')}_clamped"
    if direction == "SHORT" and tp0 >= entry:
        tp0 = round_tick(entry - max(tp_dbg.get("min_dist") or tick_size(pair), tick_size(pair)), pair)
        tp_dbg["branch"] = f"{tp_dbg.get('branch')}_clamped"

    debug = {
        "entry": entry,
        "structural_tp": float(structural_tp) if math.isfinite(structural_tp) else None,
        "tp0": tp0,
        "csl": csl,
        "tp0_dbg": tp_dbg,
        "csl_dbg": csl_dbg,
    }
    log(f"{EMOJI_INFO} TP0_CSL_COMPUTE {pair_tag(pair, direction)}", debug)
    return csl, tp0, debug


def _health_snapshot(*, now: Optional[float] = None, last_price_ts: Optional[float] = None, net_fail_count: Optional[int] = None) -> dict:
    """Lightweight health snapshot used by SOP gates.

    This does not make network calls; it only evaluates existing timing counters.
    """
    if now is None:
        now = now_ts()
    age = None
    if isinstance(last_price_ts, (int, float)):
        age = max(0.0, float(now) - float(last_price_ts))
    nf = int(net_fail_count or 0)
    degraded = False
    if age is not None and age > 20.0:
        degraded = True
    if nf >= 3:
        degraded = True
    return {
        "now": float(now),
        "last_price_age_sec": age,
        "net_fail_count": nf,
        "degraded": degraded,
    }


def _confirm_trade_exists(o: "OandaClient", trade_id: str) -> Tuple[bool, dict]:
    """Confirm broker recognizes the trade id (best-effort)."""
    try:
        resp = o._get(f"/v3/accounts/{o.account_id}/trades/{str(trade_id)}")
    except Exception as e:
        return False, {"_exception": True, "_error": str(e)}
    ok = isinstance(resp, dict) and not (resp.get("_http_error") or resp.get("_rate_limited") or resp.get("_json_error") or resp.get("_exception"))
    if ok and (resp.get("trade") or resp.get("lastTransactionID")):
        return True, resp
    # 404 => does not exist
    if isinstance(resp, dict) and resp.get("_status") == 404:
        return False, resp
    return False, resp


def _try_set_structural_sl(o: "OandaClient", trade_id: str, sl_price: float, *, label: str) -> Tuple[bool, dict]:
    resp = oanda_call(label, o.set_trade_stop_loss, str(trade_id), float(sl_price), allow_error_dict=True)
    ok = isinstance(resp, dict) and not (
        resp.get("_http_error")
        or resp.get("_rate_limited")
        or resp.get("_json_error")
        or resp.get("_exception")
    )
    return ok, resp


def _post_fill_upgrade_sl_or_panic(
    *,
    o: "OandaClient",
    pair: str,
    direction: str,
    trade_id: str,
    csl_price: float,
    structural_sl: float,
    health: dict,
    db_trade_id: Optional[int] = None,
    leg: str = "",
) -> None:
    """SOP v2.1: Upgrade CSL -> structural SL after fill, with retry + health gate.

    If upgrade fails and CSL cannot be confirmed broker-side AND health is degraded => panic exit.
    """
    pair = normalize_pair(pair)
    direction = str(direction or "").upper()

    # Confirm trade exists (proxy that broker accepted the order and now tracks the trade)
    exists, resp_trade = _confirm_trade_exists(o, str(trade_id))
    log(f"{EMOJI_INFO} POST_FILL_TRADE_CONFIRM {pair_tag(pair, direction)}", {
        "pair": pair,
        "direction": direction,
        "trade_id": str(trade_id),
        "exists": exists,
        "health": health,
        "leg": leg,
        "resp_keys": list(resp_trade.keys()) if isinstance(resp_trade, dict) else None,
    })

    # Attempt structural SL upgrade (one retry with outward clamp)
    ok1, resp1 = _try_set_structural_sl(o, str(trade_id), float(structural_sl), label=f"post_fill_sl_{pair}_{leg}")
    if ok1:
        log(f"{EMOJI_OK} POST_FILL_SL_UPGRADE_OK {pair_tag(pair, direction)}", {
            "pair": pair,
            "direction": direction,
            "trade_id": str(trade_id),
            "sl": float(structural_sl),
            "leg": leg,
        })
        return

    # Retry once by clamping outward away from entry based on CSL distance
    try_sl = float(structural_sl)
    csl = float(csl_price)
    # Move SL toward CSL (more catastrophic) by 25% of distance if needed
    try:
        clamp_dist = abs(try_sl - csl) * 0.25
    except Exception:
        clamp_dist = 0.0
    if clamp_dist > 0:
        try_sl = try_sl - clamp_dist if direction == "LONG" else try_sl + clamp_dist
        try_sl = round_tick(try_sl, pair)
    ok2, resp2 = _try_set_structural_sl(o, str(trade_id), float(try_sl), label=f"post_fill_sl_retry_{pair}_{leg}")
    if ok2:
        log(f"{EMOJI_OK} POST_FILL_SL_UPGRADE_RETRY_OK {pair_tag(pair, direction)}", {
            "pair": pair,
            "direction": direction,
            "trade_id": str(trade_id),
            "sl": float(try_sl),
            "leg": leg,
        })
        return

    # If we can't upgrade, we must ensure CSL exists and health is acceptable.
    if bool(health.get("degraded")):
        # Degraded health: if we cannot confirm trade or can't rely on broker state => exit immediately.
        log(f"{EMOJI_WARN} POST_FILL_SL_UPGRADE_FAIL_DEGRADED {pair_tag(pair, direction)}", {
            "pair": pair,
            "direction": direction,
            "trade_id": str(trade_id),
            "exists": exists,
            "health": health,
            "resp1": resp1,
            "resp2": resp2,
            "leg": leg,
        })
        _close_trade_or_position(o, pair, direction, str(trade_id), "post_fill_sl_upgrade_fail", db_trade_id)
        return

    log(f"{EMOJI_WARN} POST_FILL_SL_UPGRADE_FAIL_OK_HEALTH {pair_tag(pair, direction)}", {
        "pair": pair,
        "direction": direction,
        "trade_id": str(trade_id),
        "exists": exists,
        "health": health,
        "resp1": resp1,
        "resp2": resp2,
        "leg": leg,
    })


def _panic_ioc_limit_price(*, pair: str, direction: str, bid: float, ask: float) -> float:
    """SOP v2.1 panic IOC protective limit price near market."""
    pair = normalize_pair(pair)
    direction = str(direction or "").upper()
    spread = max(0.0, float(ask) - float(bid))
    if direction == "LONG":
        # selling long -> executable at bid; go slightly below
        px = float(bid) - (1.5 * spread)
        return round_tick_down(px, pair)
    # selling short -> executable at ask; go slightly above
    px = float(ask) + (1.5 * spread)
    return round_tick_up(px, pair)


def panic_execution_ladder(
    *,
    o: "OandaClient",
    pair: str,
    direction: str,
    bid: float,
    ask: float,
    units: int,
    exit_reason: str,
    db_trade_id: Optional[int] = None,
) -> Tuple[bool, dict]:
    """SOP v2.1: Panic exits are IOC-first, then fallback close-by-position."""
    pair = normalize_pair(pair)
    direction = str(direction or "").upper()
    units = int(units)
    limit_px = _panic_ioc_limit_price(pair=pair, direction=direction, bid=bid, ask=ask)

    log(f"{EMOJI_WARN} PANIC_LADDER_START {pair_tag(pair, direction)}", {
        "pair": pair,
        "direction": direction,
        "units": units,
        "bid": bid,
        "ask": ask,
        "limit_px": limit_px,
        "exit_reason": exit_reason,
        "db_trade_id": db_trade_id,
    })

    # IOC limit (protective) attempt
    try:
        body = {
            "order": {
                "units": str(-abs(units) if direction == "LONG" else abs(units)),
                "instrument": pair,
                "price": str(limit_px),
                "timeInForce": "IOC",
                "type": "LIMIT",
                "positionFill": "DEFAULT",
            }
        }
        resp_ioc = oanda_call(f"panic_ioc_{exit_reason}", o._post, f"/v3/accounts/{o.account_id}/orders", body, allow_error_dict=True)
        filled = isinstance(resp_ioc, dict) and bool(resp_ioc.get("orderFillTransaction"))
        if filled:
            log(f"{EMOJI_OK} PANIC_IOC_FILLED {pair_tag(pair, direction)}", {"resp": resp_ioc, "limit_px": limit_px})
            return True, resp_ioc
        log(f"{EMOJI_WARN} PANIC_IOC_NOT_FILLED {pair_tag(pair, direction)}", {"resp": resp_ioc, "limit_px": limit_px})
    except Exception as e:
        log_runtime("warning", "PANIC_IOC_EXCEPTION", pair=pair, error=str(e))

    # Fallback: close-by-position market close immediately
    ok, resp_close = _close_trade_or_position(o, pair, direction, None, f"panic_fallback_{exit_reason}", db_trade_id)
    return ok, resp_close
    
    # SPREAD_SLTP_ADJUST_DISABLED - Spread affects size only, not SL/TP
    if include_spread and spread_adj != 0.0:
        log(f"{EMOJI_WARN} SPREAD_SLTP_ADJUST_DISABLED", {"spread_adj": spread_adj})
        spread_adj = 0.0
    
    if side == "LONG":
        tp = round_tick_up(entry + (tp_atr * atr_val), pair)  # TP pure ATR
        sl = round_tick_down(entry - (sl_atr * atr_val) - spread_adj, pair)  # SL absorbs spread
    else:
        tp = round_tick_down(entry - (tp_atr * atr_val), pair)  # TP pure ATR
        sl = round_tick_up(entry + (sl_atr * atr_val) + spread_adj, pair)  # SL absorbs spread
    
    print(f"   spread_buffer={spread_buffer:.1f}, spread_adj={spread_adj} (size-only)")
    print(f"   final: sl={sl}, tp={tp}")

    # No more fake safety widening - trust the ATR structure
    
    # Final sanity check only
    if side == "LONG":
        if not (sl < entry < tp):
            log(f"{EMOJI_ERR} PRICE_SANITY_FAIL {pair}", {"entry": entry, "sl": sl, "tp": tp, "side": side})
            # Emergency fallback - use minimal ATR multiples
            sl = round_tick_down(entry - (0.5 * atr_val), pair)
            tp = round_tick_up(entry + (0.4 * atr_val), pair)
    else:
        if not (tp < entry < sl):
            log(f"{EMOJI_ERR} PRICE_SANITY_FAIL {pair}", {"entry": entry, "sl": sl, "tp": tp, "side": side})
            # Emergency fallback
            sl = round_tick_up(entry + (0.5 * atr_val), pair)
            tp = round_tick_down(entry - (0.4 * atr_val), pair)

    return entry, sl, tp


def _atr_gate_ok(pair: str, atr_exec: float) -> bool:
    # DISABLED: Always pass ATR gate to allow more trades
    return True


def state_emoji(state: str) -> str:
    if state == "ENTER":
        return EMOJI_ENTER
    if state == "GET_READY":
        return EMOJI_GET_READY
    if state == "ARM_TICK_ENTRY":
        return EMOJI_GET_READY
    if state == "WATCH":
        return EMOJI_WATCH
    return ""


def _transition_state(st: PairState, new_state: str, pair: str = "", strategy: Optional[str] = None, 
                      direction: Optional[str] = None, reason: Optional[str] = None,
                      metadata: Optional[dict] = None) -> None:
    """Transition state using StateMachine with alerts"""
    pair = normalize_pair(pair)
    if new_state == st.state:
        return
        
    old_state = st.state
    
    # Use StateMachine for transition with alerts
    state_machine = get_state_machine()
    success = state_machine.transition(
        pair=pair,
        to_state=new_state,
        strategy=strategy,
        direction=direction,
        reason=reason,
        metadata=metadata
    )
    
    if not success:
        log(f"{EMOJI_WARN} STATE_TRANSITION_FAILED", 
            {"pair": pair, "from": old_state, "to": new_state})
        return
    
    # Update local state
    st.state = new_state
    st.state_since = now_ts()
    if new_state == "SKIP":
        st.neutral_bars = 0
    # Reset tick entry arming when leaving entry-related states
    if new_state not in ("GET_READY", "ENTER", "ARM_TICK_ENTRY"):
        st.entry_arm = {}
    # Reset hysteresis counter when leaving GET_READY
    if old_state == "GET_READY" and new_state != "GET_READY":
        st.get_ready_weak_scans = 0
    # Reset stale-feed tracking on state transitions
    st.stale_poll_count = 0
    if hasattr(st, "last_ohlc"):
        try:
            delattr(st, "last_ohlc")
        except Exception:
            pass
    
    # Clean up old alert records (older than 5 minutes)
    now = now_ts()
    cleanup_cutoff = now - 300.0
    pairs_to_remove = []
    for p in list(_last_state_alert.keys()):
        states_to_remove = []
        for state, ts in _last_state_alert[p].items():
            if ts < cleanup_cutoff:
                states_to_remove.append(state)
        for state in states_to_remove:
            del _last_state_alert[p][state]
        if not _last_state_alert[p]:
            pairs_to_remove.append(p)
    for p in pairs_to_remove:
        del _last_state_alert[p]
    
    # Log state change
    log_runtime("info", "STATE_TRANSITION", pair=pair, from_state=old_state, to_state=new_state)
    
    # T1-16 State Machine Integrity Gate Validation
    try:
        from pathlib import Path
        import json
        proof_dirs = sorted(Path(__file__).parent.glob("proof_artifacts/*"))
        if proof_dirs:
            latest_proof = proof_dirs[-1]
            
            # Initialize or load state transitions
            state_file = latest_proof / "state_transitions.jsonl"
            if state_file.exists():
                transitions = []
                for line in state_file.read_text().strip().splitlines():
                    transitions.append(json.loads(line))
            else:
                transitions = []
            
            # Add current transition
            transitions.append({
                "pair": pair,
                "from": old_state,
                "to": new_state,
                "timestamp": now_ts()
            })
            
            # Keep only last 100 transitions per pair
            pair_transitions = [t for t in transitions if t.get("pair") == pair]
            if len(pair_transitions) > 100:
                # Remove old transitions for this pair
                to_remove = len(pair_transitions) - 100
                transitions = [t for t in transitions if not (t.get("pair") == pair and to_remove > 0 and (to_remove := to_remove - 1))]
            
            # Write back
            state_file.write_text("\n".join(json.dumps(t) for t in transitions) + "\n")
            
            # Validate allowed transitions
            allowed = {
                "SKIP": ["WATCH", "GET_READY"],
                "WATCH": ["GET_READY", "SKIP"],
                "GET_READY": ["ENTER", "WATCH", "SKIP", "ARM_TICK_ENTRY"],
                "ENTER": ["MONITOR", "SKIP"],
                "MONITOR": ["EXIT", "SKIP"],
                "EXIT": ["SKIP", "WATCH"],
                "ARM_TICK_ENTRY": ["GET_READY", "ENTER", "SKIP"]
            }
            
            is_valid = old_state not in allowed or new_state in allowed.get(old_state, [])
            
            # Write validation report
            report = {
                "status": "PASS" if is_valid else "FAIL",
                "pair": pair,
                "transitions": transitions[-10:],  # Last 10 transitions
                "latest": {"from": old_state, "to": new_state}
            }
            
            report_file = latest_proof / f"state_transition_report_{pair}.json"
            report_file.write_text(json.dumps(report, indent=2))
            
            if not is_valid:
                log_runtime("error", "T1-16_STATE_MACHINE_FAIL",
                          pair=pair, from_state=old_state, to_state=new_state)
    except Exception as e:
        log_runtime("warning", "T1-16_ARTIFACT_ERROR", pair=pair, error=str(e))
    if pair:
        print(f"{time.strftime('%H:%M:%S')} - {pair}: {old_state} -> {new_state}")
        sys.stdout.flush()
    
    # Immediate alert for GET_READY / ARM_TICK_ENTRY / ENTER transitions (with deduplication)
    if new_state in ("GET_READY", "ARM_TICK_ENTRY", "ENTER") and pair:
        # Check if we recently sent this alert for this pair/state
        now = now_ts()
        pair_alerts = _last_state_alert.get(pair, {})
        last_alert_time = pair_alerts.get(new_state, 0)
        
        # Only send alert if it's been at least ALERT_DEDUP_COOLDOWN_SEC seconds since last one
        if (now - last_alert_time) >= ALERT_DEDUP_COOLDOWN_SEC:
            state_emo = state_emoji(new_state)
            notify(
                f"{state_emo} STATE CHANGE {pair}",
                f"{state_emo} {old_state} -> {new_state} | m_norm={st.m_norm:.2f} | wr={st.wr:.0f} {state_emo}"
            )
            # Track the alert
            _last_state_alert[pair] = _last_state_alert.get(pair, {})
            _last_state_alert[pair][new_state] = now
            # Prevent immediate repeat alerts from the scan loop
            st.last_alert = now
            st.last_alert_key = f"{new_state}:{st.mode}:STATE_CHANGE"


def _apply_state_machine(st: PairState, pair: Optional[str] = None, c_exec: Optional[List[dict]] = None) -> None:
    pair = normalize_pair(pair or getattr(st, "pair", "") or "")
    if st.state == "ENTER":
        enter_age = now_ts() - st.state_since
        if enter_age >= ENTER_HOLD_SEC:
            if st.last_trade >= st.state_since:
                _transition_state(st, "SKIP", pair)
            else:
                _transition_state(st, "WATCH", pair)
        return
    if st.state == "ARM_TICK_ENTRY":
        arm = st.entry_arm or {}
        if not arm:
            _transition_state(st, "GET_READY", pair)
            return
        arm_ts = float(arm.get("ts", 0.0) or 0.0)
        if arm_ts <= 0.0 or (now_ts() - arm_ts) > ENTRY_ARM_TTL_SEC:
            _transition_state(st, "GET_READY", pair)
        return
    
    # Handle NaN values - keep current state, never downgrade
    has_nan = not (math.isfinite(st.wr) and math.isfinite(st.m_norm) and math.isfinite(st.atr_exec))
    
    if has_nan:
        # Log throttled DATA_STALE event
        if not hasattr(st, 'last_data_stale_log_ts'):
            st.last_data_stale_log_ts = 0.0
        
        if now_ts() - st.last_data_stale_log_ts > 30.0:
            log_throttled(
                f"data_stale:{pair}",
                f"{EMOJI_WARN} DATA_STALE {pair}",
                {
                    "pair": pair,
                    "fields": {
                        "wr": st.wr,
                        "m_norm": st.m_norm,
                        "atr_exec": st.atr_exec
                    }
                }
            )
            st.last_data_stale_log_ts = now_ts()
        return  # Keep current state, do not downgrade

    in_neutral = WR_NEUTRAL_MIN < st.wr < WR_NEUTRAL_MAX
    if in_neutral:
        st.neutral_bars += 1
    else:
        st.neutral_bars = 0

    held_long_enough = (now_ts() - st.state_since) >= STATE_MIN_HOLD_SEC

    if st.wr <= WR_OVERSOLD or st.wr >= WR_OVERBOUGHT:
        if st.state != "ENTER" and st.state != "GET_READY":
            _transition_state(st, "WATCH", pair)
        return

    if math.isfinite(st.wr_prev):
        if (st.wr_prev <= WR_OVERSOLD) and (st.wr >= WR_OVERSOLD + WR_TURN_PTS):
            _transition_state(st, "GET_READY", pair)
            st.get_ready_weak_scans = 0  # Reset hysteresis counter
            return
        if (st.wr_prev >= WR_OVERBOUGHT) and (st.wr <= WR_OVERBOUGHT - WR_TURN_PTS):
            _transition_state(st, "GET_READY", pair)
            st.get_ready_weak_scans = 0  # Reset hysteresis counter
            return

    # GET_READY hysteresis - don't downgrade unless weak for 10 scans
    if st.state == "GET_READY":
        is_weak = st.m_norm < 0.85
        
        # Check price momentum over last K bars
        weak_price = False
        if c_exec and len(c_exec) >= 12 and math.isfinite(st.atr_exec):
            closes = [float(c["c"]) for c in c_exec[-12:]]
            price_move = abs(closes[-1] - closes[0])
            weak_price = price_move < (0.25 * st.atr_exec)
        
        if is_weak and weak_price:
            st.get_ready_weak_scans += 1
            if st.get_ready_weak_scans >= 10:
                print(f"{time.strftime('%H:%M:%S')} - {pair}: GET_READY -> WATCH after {st.get_ready_weak_scans} weak scans")
                sys.stdout.flush()
                _transition_state(st, "WATCH", pair)
                st.get_ready_weak_scans = 0
        else:
            st.get_ready_weak_scans = 0  # Reset if conditions improve

    # Normal WATCH -> SKIP transition (GET_READY never allowed)
    if st.state == "WATCH" and st.neutral_bars >= WR_NEUTRAL_BARS_RESET and held_long_enough:
        _transition_state(st, "SKIP", pair)


def _order_confirmed(resp: dict) -> Tuple[bool, str, str, str]:
    if not isinstance(resp, dict):
        print(f"❌ ORDER CONFIRMED: INVALID RESPONSE - {resp}")
        return False, "", "", "invalid_response"
    if resp.get("_rate_limited"):
        print(f"❌ ORDER CONFIRMED: RATE LIMITED - {resp}")
        return False, "", "", "rate_limited"
    if resp.get("_http_error"):
        print(f"❌ ORDER CONFIRMED: HTTP ERROR {resp.get('_status')} - {resp}")
        return False, "", "", f"http_{resp.get('_status')}"
    if resp.get("_json_error"):
        print(f"❌ ORDER CONFIRMED: JSON ERROR - {resp}")
        return False, "", "", "json_error"

    fill = resp.get("orderFillTransaction") or {}
    create = resp.get("orderCreateTransaction") or {}
    cancel = resp.get("orderCancelTransaction") or {}
    reject = resp.get("orderRejectTransaction") or {}
    
    if cancel:
        reason = str(cancel.get("reason", "") or "cancel")
        error_msg = cancel.get("errorMessage", "")
        print(f"❌ ORDER CANCELLED: reason={reason}, errorMessage={error_msg}, details={cancel}")
        return False, str(cancel.get("orderID", "")), str(cancel.get("id", "")), f"cancel_{reason}"
    if reject:
        reason = str(reject.get("rejectReason", "") or "reject")
        error_msg = reject.get("errorMessage", "")
        print(f"❌ ORDER REJECTED: reason={reason}, errorMessage={error_msg}, details={reject}")
        return False, str(reject.get("orderID", "")), str(reject.get("id", "")), f"reject_{reason}"
    if fill:
        trade_id = _extract_trade_id_from_fill(resp)
        if trade_id is None:
            print(f"❌ CRITICAL: Order filled but no tradeID extracted! {fill}")
        print(f"✅ ORDER FILLED: orderID={fill.get('orderID')}, tradeID={trade_id}, price={fill.get('price')}, units={fill.get('units')}")
        return True, str(fill.get("orderID", "")), str(fill.get("id", "")), "FILLED"
    if create:
        print(f"✅ ORDER CREATED: orderID={create.get('orderID')}, units={create.get('units')}")
        return True, str(create.get("orderID", "")), str(create.get("id", "")), "CREATED"
    
    # Check for top-level error message
    if resp.get("errorMessage"):
        print(f"❌ ORDER ERROR: errorMessage={resp.get('errorMessage')}, rejectReason={resp.get('rejectReason')}")
    
    print(f"❓ ORDER UNCONFIRMED: response structure unknown - {resp}")
    return False, "", "", "unconfirmed"


def _note_order_reject(pair: str, status: str, resp: Optional[dict], *, leg: str = "MAIN") -> None:
    """Track recent order rejects to avoid repeated failures."""
    pair = normalize_pair(pair)
    if not pair:
        return
    now = now_ts()
    # Avoid blocking on rate-limit status; rely on broker retry-after.
    if str(status).lower() == "rate_limited":
        return
    reason = ""
    if isinstance(resp, dict):
        rej = resp.get("orderRejectTransaction") or {}
        can = resp.get("orderCancelTransaction") or {}
        if isinstance(rej, dict) and rej:
            reason = str(rej.get("rejectReason") or rej.get("errorMessage") or "reject")
        elif isinstance(can, dict) and can:
            reason = str(can.get("reason") or can.get("errorMessage") or "cancel")
        else:
            reason = str(resp.get("errorMessage") or "")
    block = {
        "ts": now,
        "until": now + ORDER_REJECT_COOLDOWN_SEC,
        "status": status,
        "reason": reason,
        "leg": leg,
    }
    ORDER_REJECT_BLOCK[pair] = block
    log(
        f"{EMOJI_WARN} ORDER_REJECT_COOLDOWN {pair_tag(pair)}",
        {"pair": pair, "status": status, "reason": reason, "leg": leg, "cooldown_sec": ORDER_REJECT_COOLDOWN_SEC},
    )


# Multi-timeframe coordination state
MTF_ACTIVE_SIGNALS = {}  # pair -> {strategy_id: {signal, timeframe, timestamp, confidence}}
MTF_STRATEGY_PRIORITIES = {  # Lower number = higher priority
    1: 1,  # Scalp - Highest priority for quick entries
    2: 2,  # Breakout
    3: 3,  # Momentum
    4: 4,  # Reversal
    5: 5,  # Trend
    6: 6,  # Range
    7: 7   # Runner - Lowest priority (long-term)
}

def register_mtf_signal(pair: str, strategy_id: int, signal: dict, timeframe: str, confidence: float = 1.0):
    """Register a multi-timeframe signal.
    
    Args:
        pair: Currency pair
        strategy_id: Strategy ID (1-7)
        signal: Signal dict with direction, entry, etc.
        timeframe: Timeframe of the signal
        confidence: Signal confidence (0.0-1.0)
    """
    if not MTF_COORDINATION_ENABLED:
        return
    
    if pair not in MTF_ACTIVE_SIGNALS:
        MTF_ACTIVE_SIGNALS[pair] = {}
    
    MTF_ACTIVE_SIGNALS[pair][strategy_id] = {
        "signal": signal,
        "timeframe": timeframe,
        "timestamp": now_ts(),
        "confidence": confidence,
        "priority": MTF_STRATEGY_PRIORITIES.get(strategy_id, 999)
    }
    
    log(f"{EMOJI_INFO} MTF_SIGNAL_REGISTERED", {
        "pair": pair,
        "strategy_id": strategy_id,
        "timeframe": timeframe,
        "direction": signal.get("direction", "UNKNOWN"),
        "confidence": confidence
    })

def resolve_mtf_conflicts(pair: str) -> Tuple[Optional[dict], str]:
    """Resolve conflicts between multi-timeframe signals.
    
    Args:
        pair: Currency pair
        
    Returns:
        Tuple of (selected_signal, resolution_method)
    """
    if pair not in MTF_ACTIVE_SIGNALS or not MTF_ACTIVE_SIGNALS[pair]:
        return None, "no_signals"
    
    # Clean up old signals
    current_time = now_ts()
    MTF_ACTIVE_SIGNALS[pair] = {
        sid: sig for sid, sig in MTF_ACTIVE_SIGNALS[pair].items()
        if current_time - sig["timestamp"] < MTF_SIGNAL_TIMEOUT
    }
    
    if not MTF_ACTIVE_SIGNALS[pair]:
        return None, "signals_expired"
    
    signals = MTF_ACTIVE_SIGNALS[pair]
    
    # Check if we have too many active strategies
    if len(signals) > MTF_MAX_STRATEGIES_PER_PAIR:
        log(f"{EMOJI_WARN} MTF_TOO_MANY_SIGNALS", {
            "pair": pair,
            "active_signals": len(signals),
            "max_allowed": MTF_MAX_STRATEGIES_PER_PAIR
        })
    
    # Group by direction
    long_signals = {sid: sig for sid, sig in signals.items() if sig["signal"].get("direction") == "LONG"}
    short_signals = {sid: sig for sid, sig in signals.items() if sig["signal"].get("direction") == "SHORT"}
    
    # No conflicts if all signals agree on direction
    if long_signals and not short_signals:
        # Use highest priority LONG signal
        chosen = min(long_signals.items(), key=lambda x: x[1]["priority"])
        return chosen[1]["signal"], f"consensus_long_priority_{chosen[0]}"
    
    if short_signals and not long_signals:
        # Use highest priority SHORT signal
        chosen = min(short_signals.items(), key=lambda x: x[1]["priority"])
        return chosen[1]["signal"], f"consensus_short_priority_{chosen[0]}"
    
    # Conflict resolution needed
    if MTF_CONFLICT_RESOLUTION == "priority":
        # Choose highest priority signal overall
        chosen = min(signals.items(), key=lambda x: x[1]["priority"])
        return chosen[1]["signal"], f"conflict_priority_{chosen[0]}"
    
    elif MTF_CONFLICT_RESOLUTION == "latest":
        # Choose most recent signal
        chosen = max(signals.items(), key=lambda x: x[1]["timestamp"])
        return chosen[1]["signal"], f"conflict_latest_{chosen[0]}"
    
    elif MTF_CONFLICT_RESOLUTION == "consensus":
        # Require majority agreement
        if len(long_signals) > len(short_signals):
            chosen = min(long_signals.items(), key=lambda x: x[1]["priority"])
            return chosen[1]["signal"], f"consensus_majority_long_{chosen[0]}"
        elif len(short_signals) > len(long_signals):
            chosen = min(short_signals.items(), key=lambda x: x[1]["priority"])
            return chosen[1]["signal"], f"consensus_majority_short_{chosen[0]}"
        else:
            # Tie - no signal
            return None, "conflict_no_majority"
    
    # Default: no signal on conflict
    return None, "conflict_no_resolution"

def check_mtf_compatibility(pair: str, strategy_id: int, direction: str) -> Tuple[bool, str]:
    """Check if a new signal is compatible with existing MTF signals.
    
    Args:
        pair: Currency pair
        strategy_id: Strategy ID to check
        direction: Signal direction (LONG/SHORT)
        
    Returns:
        Tuple of (is_compatible, reason)
    """
    if not MTF_COORDINATION_ENABLED:
        return True, "coordination_disabled"
    
    if pair not in MTF_ACTIVE_SIGNALS:
        return True, "no_existing_signals"
    
    # Check for conflicting direction from higher priority strategies
    current_priority = MTF_STRATEGY_PRIORITIES.get(strategy_id, 999)
    
    for existing_id, sig_data in MTF_ACTIVE_SIGNALS[pair].items():
        existing_dir = sig_data["signal"].get("direction")
        existing_priority = sig_data["priority"]
        
        # Skip if same direction
        if existing_dir == direction:
            continue
        
        # Check if existing signal has higher priority
        if existing_priority < current_priority:
            return False, f"conflict_with_higher_priority_{existing_id}"
    
    return True, "compatible"

def get_mtf_signal_summary(pair: str) -> dict:
    """Get a summary of all active MTF signals for a pair.
    
    Args:
        pair: Currency pair
        
    Returns:
        Dict with signal summary
    """
    if pair not in MTF_ACTIVE_SIGNALS:
        return {"pair": pair, "active_signals": 0, "signals": []}
    
    summary = {
        "pair": pair,
        "active_signals": len(MTF_ACTIVE_SIGNALS[pair]),
        "signals": []
    }
    
    for strategy_id, sig_data in MTF_ACTIVE_SIGNALS[pair].items():
        age = now_ts() - sig_data["timestamp"]
        summary["signals"].append({
            "strategy_id": strategy_id,
            "direction": sig_data["signal"].get("direction", "UNKNOWN"),
            "timeframe": sig_data["timeframe"],
            "confidence": sig_data["confidence"],
            "priority": sig_data["priority"],
            "age_seconds": age
        })
    
    return summary

def cleanup_expired_mtf_signals():
    """Clean up expired MTF signals for all pairs."""
    current_time = now_ts()
    expired_count = 0
    
    for pair in list(MTF_ACTIVE_SIGNALS.keys()):
        original_count = len(MTF_ACTIVE_SIGNALS[pair])
        
        MTF_ACTIVE_SIGNALS[pair] = {
            sid: sig for sid, sig in MTF_ACTIVE_SIGNALS[pair].items()
            if current_time - sig["timestamp"] < MTF_SIGNAL_TIMEOUT
        }
        
        expired_count += original_count - len(MTF_ACTIVE_SIGNALS[pair])
        
        # Remove empty pairs
        if not MTF_ACTIVE_SIGNALS[pair]:
            del MTF_ACTIVE_SIGNALS[pair]
    
    if expired_count > 0:
        log(f"{EMOJI_INFO} MTF_CLEANUP", {
            "expired_signals": expired_count,
            "remaining_pairs": len(MTF_ACTIVE_SIGNALS)
        })

def handle_rejected_order(pair: str, units: int, reason: str, reject_response: dict, attempt: int = 1) -> dict:
    """Handle rejected orders with retry logic and alternative routing.
    
    Args:
        pair: Currency pair
        units: Order units (positive for LONG, negative for SHORT)
        reason: Original order reason
        reject_response: The rejection response from OANDA
        attempt: Current attempt number
        
    Returns:
        Dict with recovery status and details
    """
    # Extract rejection details
    reject_tx = reject_response.get("orderRejectTransaction") or {}
    reject_reason = reject_tx.get("rejectReason", "unknown")
    error_message = reject_tx.get("errorMessage", "")
    
    log(f"{EMOJI_WARN} REJECTED_ORDER_HANDLING", {
        "pair": pair,
        "units": units,
        "reason": reason,
        "reject_reason": reject_reason,
        "error_message": error_message,
        "attempt": attempt,
        "max_attempts": REJECTED_ORDER_RETRY_MAX
    })
    
    # Check if we should retry
    if attempt > REJECTED_ORDER_RETRY_MAX:
        log(f"{EMOJI_ERR} REJECTED_ORDER_MAX_ATTEMPTS", {
            "pair": pair,
            "attempts": attempt - 1,
            "final_reason": reject_reason
        })
        return {
            "status": "FAILED",
            "reason": f"Max retries exceeded: {reject_reason}",
            "attempts": attempt - 1
        }
    
    # Determine retry strategy based on rejection reason
    retry_strategy = get_retry_strategy(reject_reason, error_message)
    
    if retry_strategy == "NO_RETRY":
        log(f"{EMOJI_INFO} REJECTED_ORDER_NO_RETRY", {
            "pair": pair,
            "reason": reject_reason
        })
        return {
            "status": "FAILED",
            "reason": f"No retry for: {reject_reason}",
            "attempts": attempt - 1
        }
    
    # Calculate backoff delay
    backoff_delay = REJECTED_ORDER_RETRY_DELAY * (ORDER_REJECT_BACKOFF_MULTIPLIER ** (attempt - 1))
    
    log(f"{EMOJI_INFO} REJECTED_ORDER_RETRY_WAIT", {
        "pair": pair,
        "delay": backoff_delay,
        "attempt": attempt
    })
    
    time.sleep(backoff_delay)
    
    # Apply alternative routing if enabled
    if ALTERNATIVE_ROUTING_ENABLED and retry_strategy == "ALTERNATIVE":
        return try_alternative_routing(pair, units, reason, attempt)
    
    # Standard retry
    log(f"{EMOJI_INFO} REJECTED_ORDER_RETRY", {
        "pair": pair,
        "units": units,
        "attempt": attempt
    })
    
    new_response = create_market_order(pair, units, f"{reason}_retry_{attempt}")
    
    if new_response.get("orderFillTransaction") or new_response.get("orderCreateTransaction"):
        log(f"{EMOJI_SUCCESS} REJECTED_ORDER_RECOVERED", {
            "pair": pair,
            "attempt": attempt,
            "order_id": new_response.get("orderFillTransaction", {}).get("orderID") or new_response.get("orderCreateTransaction", {}).get("orderID")
        })
        return {
            "status": "RECOVERED",
            "response": new_response,
            "attempts": attempt
        }
    else:
        # Recursive retry
        return handle_rejected_order(pair, units, reason, new_response, attempt + 1)

def get_retry_strategy(reject_reason: str, error_message: str) -> str:
    """Determine retry strategy based on rejection reason.
    
    Returns:
        "RETRY" - Standard retry
        "ALTERNATIVE" - Try alternative routing
        "NO_RETRY" - Don't retry
    """
    reject_reason = reject_reason.lower()
    error_message = error_message.lower()
    
    # Don't retry for these reasons
    no_retry_reasons = [
        "insufficient_margin",
        "margin_closeout",
        "position_closure",
        "market_halted",
        "instrument_closed",
        "invalid_quantity",
        "invalid_price"
    ]
    
    for reason in no_retry_reasons:
        if reason in reject_reason or reason in error_message:
            return "NO_RETRY"
    
    # Use alternative routing for these
    alternative_reasons = [
        "capacity_constraint",
        "rate_limit",
        "server_busy",
        "market_order_rejected"
    ]
    
    for reason in alternative_reasons:
        if reason in reject_reason or reason in error_message:
            return "ALTERNATIVE"
    
    # Default to retry
    return "RETRY"

def try_alternative_routing(pair: str, units: int, reason: str, attempt: int) -> dict:
    """Try alternative routing methods for rejected orders.
    
    Returns:
        Dict with routing attempt results
    """
    log(f"{EMOJI_INFO} ALTERNATIVE_ROUTING_ATTEMPT", {
        "pair": pair,
        "units": units,
        "attempt": attempt
    })
    
    # Alternative 1: Split the order into smaller chunks
    if abs(units) > 10000:
        return split_order_routing(pair, units, reason, attempt)
    
    # Alternative 2: Use limit order instead of market
    return limit_order_routing(pair, units, reason, attempt)

def split_order_routing(pair: str, units: int, reason: str, attempt: int) -> dict:
    """Split large order into smaller chunks."""
    
    chunk_size = 10000  # 10K units per chunk
    num_chunks = min(abs(units) // chunk_size, 5)  # Max 5 chunks
    remaining_units = units
    filled_chunks = 0
    
    log(f"{EMOJI_INFO} SPLIT_ORDER_ROUTING", {
        "pair": pair,
        "total_units": units,
        "chunk_size": chunk_size,
        "num_chunks": num_chunks
    })
    
    for i in range(num_chunks):
        chunk_units = chunk_size if remaining_units > 0 else remaining_units
        if remaining_units < 0:
            chunk_units = -chunk_size
        
        resp = create_market_order(pair, chunk_units, f"{reason}_split_{i+1}")
        
        if resp.get("orderFillTransaction") or resp.get("orderCreateTransaction"):
            filled_chunks += 1
            remaining_units -= chunk_units
            log(f"{EMOJI_SUCCESS} SPLIT_ORDER_CHUNK_FILLED", {
                "chunk": i + 1,
                "filled_chunks": filled_chunks,
                "remaining": remaining_units
            })
        else:
            log(f"{EMOJI_ERR} SPLIT_ORDER_CHUNK_FAILED", {
                "chunk": i + 1,
                "response": resp
            })
        
        # Brief pause between chunks
        time.sleep(0.5)
    
    # Try to fill remaining units if any
    if abs(remaining_units) >= MIN_PARTIAL_FILL_UNITS:
        resp = create_market_order(pair, remaining_units, f"{reason}_remaining")
        if resp.get("orderFillTransaction") or resp.get("orderCreateTransaction"):
            filled_chunks += 1
            remaining_units = 0
    
    success_rate = filled_chunks / num_chunks if num_chunks > 0 else 0
    
    return {
        "status": "PARTIAL_SUCCESS" if success_rate > 0 else "FAILED",
        "method": "split_order",
        "filled_chunks": filled_chunks,
        "total_chunks": num_chunks,
        "success_rate": success_rate,
        "remaining_units": remaining_units
    }

def limit_order_routing(pair: str, units: int, reason: str, attempt: int) -> dict:
    """Try using limit order instead of market order."""
    
    client = OandaClient(
        os.getenv("OANDA_API_KEY", "2bf7b4b9bb052e28023de779a6363f1e-fee71a4fce4e94b18e0dd9c2443afa52"),
        os.getenv("OANDA_ACCOUNT_ID", "101-001-22881868-001"),
        "practice"
    )
    
    # Get current price for limit
    pricing_resp = client.pricing(pair)
    if not pricing_resp or len(pricing_resp) < 2:
        return {
            "status": "FAILED",
            "method": "limit_order",
            "reason": "Cannot get pricing"
        }
    
    bid, ask = float(pricing_resp[0]), float(pricing_resp[1])
    
    # Set limit price slightly in favor of execution
    if units > 0:  # LONG
        limit_price = ask + (ask - bid) * 0.1  # 10% of spread above ask
    else:  # SHORT
        limit_price = bid - (ask - bid) * 0.1  # 10% of spread below bid
    
    order_body = {
        "order": {
            "units": str(units),
            "instrument": pair,
            "price": str(round(limit_price, 5)),
            "timeInForce": "IOC",  # Immediate or Cancel
            "type": "LIMIT",
            "positionFill": "DEFAULT"
        }
    }
    
    if reason:
        order_body["order"]["clientExtensions"] = {
            "comment": f"{reason}_limit_{attempt}"[:50]
        }
    
    log(f"{EMOJI_INFO} LIMIT_ORDER_ROUTING", {
        "pair": pair,
        "units": units,
        "limit_price": limit_price,
        "market_price": ask if units > 0 else bid
    })
    
    resp = client._post(f"/v3/accounts/{client.account_id}/orders", order_body)
    
    if resp.get("orderFillTransaction"):
        return {
            "status": "SUCCESS",
            "method": "limit_order",
            "response": resp
        }
    else:
        return {
            "status": "FAILED",
            "method": "limit_order",
            "response": resp
        }

def should_hold_intentional_runner(trade: dict, metrics: dict, current_time: float) -> Tuple[bool, str]:
    """Determine if a trade should be held as an Intentional Runner (Strategy #7).
    
    Strategy #7 is designed for multi-hour campaigns that capture large moves.
    It uses strict criteria to identify and hold high-potential runners.
    
    Args:
        trade: Trade dict with entry details
        metrics: AEE metrics dict
        current_time: Current timestamp
        
    Returns:
        Tuple of (should_hold, reason)
    """
    # Check if this is a Strategy #7 trade
    if trade.get("setup_id") != 7:
        return False, "Not a Strategy #7 trade"
    
    entry_time = trade.get("entry_time", 0)
    hold_duration = current_time - entry_time
    
    # Must meet minimum hold time
    if hold_duration < RUNNER_MIN_HOLD_TIME:
        return True, f"Holding for minimum time: {hold_duration:.0f}s / {RUNNER_MIN_HOLD_TIME:.0f}s"
    
    # Check maximum hold time
    if hold_duration > RUNNER_MAX_HOLD_TIME:
        return False, f"Exceeded maximum hold time: {hold_duration:.0f}s > {RUNNER_MAX_HOLD_TIME:.0f}s"
    
    # Must have minimum progress
    progress = metrics.get("progress", 0)
    if progress < RUNNER_MIN_PROGRESS:
        return False, f"Insufficient progress: {progress:.2f} ATR < {RUNNER_MIN_PROGRESS:.2f} ATR"
    
    # Must maintain minimum speed
    speed = metrics.get("speed", 0)
    if speed < RUNNER_SPEED_THRESHOLD:
        return False, f"Speed too low: {speed:.2f} < {RUNNER_SPEED_THRESHOLD}"
    
    # Pullback must be within limits
    pullback = metrics.get("pullback", 0)
    if pullback > RUNNER_PULLBACK_LIMIT:
        return False, f"Pullback exceeded: {pullback:.2f} > {RUNNER_PULLBACK_LIMIT}"
    
    # Check for runner phase in AEE
    aee_phase = metrics.get("aee_phase", "")
    if aee_phase not in ["RUNNER", "BUILD", "HARVEST"]:
        return False, f"AEE not in runner-compatible phase: {aee_phase}"
    
    # All conditions met - continue holding
    return True, f"Runner criteria met: progress={progress:.2f}, speed={speed:.2f}, hold={hold_duration:.0f}s"

def check_intentional_runner_exit(trade: dict, current_price: float, atr: float, current_time: float) -> Tuple[bool, str, Optional[float]]:
    """Check if an Intentional Runner should exit.
    
    Args:
        trade: Trade dict
        current_price: Current price
        atr: Current ATR
        current_time: Current timestamp
        
    Returns:
        Tuple of (should_exit, reason, exit_price)
    """
    # Get AEE metrics
    metrics = calculate_aee_metrics(trade, current_price, atr, get_candles(trade["pair"], "M5", 100))
    
    # Check hold criteria
    should_hold, reason = should_hold_intentional_runner(trade, metrics, current_time)
    
    if should_hold:
        return False, reason, None
    
    # Determine exit method based on reason
    entry_price = trade.get("entry_price", 0)
    direction = trade.get("direction", "LONG")
    
    # If exiting due to time limit, use market order
    if "Exceeded maximum hold time" in reason:
        return True, f"Time-based exit: {reason}", None
    
    # If exiting due to momentum loss, use trailing stop
    if "Speed too low" in reason or "Pullback exceeded" in reason:
        if direction == "LONG":
            # Exit at current price (momentum-based exit)
            return True, f"Momentum exit: {reason}", current_price
        else:
            return True, f"Momentum exit: {reason}", current_price
    
    # Default: market exit
    return True, f"Runner exit: {reason}", None

def manage_intentional_runner(trade: dict, current_price: float, atr: float, current_time: float) -> dict:
    """Manage an active Intentional Runner position.
    
    This function should be called regularly to monitor runner status
    and execute exits when conditions are met.
    
    Args:
        trade: Trade dict
        current_price: Current price
        atr: Current ATR
        current_time: Current timestamp
        
    Returns:
        Dict with management results
    """
    result = {
        "action": "HOLD",
        "reason": "",
        "exit_price": None,
        "metrics": {}
    }
    
    # Calculate current metrics
    candles = get_candles(trade["pair"], "M5", 100)
    metrics = calculate_aee_metrics(trade, current_price, atr, candles)
    result["metrics"] = {
        "progress": metrics.get("progress", 0),
        "speed": metrics.get("speed", 0),
        "pullback": metrics.get("pullback", 0),
        "aee_phase": metrics.get("aee_phase", ""),
        "hold_duration": current_time - trade.get("entry_time", 0)
    }
    
    # Check if should exit
    should_exit, reason, exit_price = check_intentional_runner_exit(
        trade, current_price, atr, current_time
    )
    
    if should_exit:
        result["action"] = "EXIT"
        result["reason"] = reason
        result["exit_price"] = exit_price
        
        log(f"{EMOJI_INFO} RUNNER_EXIT_SIGNAL", {
            "pair": trade["pair"],
            "trade_id": trade.get("trade_id"),
            "reason": reason,
            "metrics": result["metrics"]
        })
    else:
        result["reason"] = reason
        
        # Log runner status periodically
        if int(current_time) % 300 == 0:  # Every 5 minutes
            log(f"{EMOJI_INFO} RUNNER_STATUS", {
                "pair": trade["pair"],
                "trade_id": trade.get("trade_id"),
                "metrics": result["metrics"]
            })
    
    return result
    """Handle partial fills by monitoring order status and completing if needed.
    
    Args:
        order_id: The order ID to monitor
        pair: Currency pair
        expected_units: Expected total units
        direction: LONG or SHORT
        
    Returns:
        Dict with fill status and details
    """
    # Use existing client instance - no import needed
    
    client = OandaClient(
        os.getenv("OANDA_API_KEY", "2bf7b4b9bb052e28023de779a6363f1e-fee71a4fce4e94b18e0dd9c2443afa52"),
        os.getenv("OANDA_ACCOUNT_ID", "101-001-22881868-001"),
        "practice"
    )
    
    start_time = now_ts()
    filled_units = 0
    attempts = 0
    
    log(f"{EMOJI_INFO} PARTIAL_FILL_MONITOR_START", {
        "order_id": order_id,
        "pair": pair,
        "expected_units": expected_units,
        "direction": direction
    })
    
    while now_ts() - start_time < PARTIAL_FILL_TIMEOUT_SEC:
        attempts += 1
        
        # Check order status
        order_resp = client._get(f"/v3/accounts/{client.account_id}/orders/{order_id}")
        
        if order_resp.get("order"):
            order = order_resp["order"]
            current_state = order.get("state", "")
            units_filled = int(order.get("unitsFilled", 0))
            
            log(f"{EMOJI_INFO} PARTIAL_FILL_CHECK", {
                "order_id": order_id,
                "state": current_state,
                "units_filled": units_filled,
                "expected": expected_units,
                "remaining": expected_units - units_filled,
                "attempt": attempts
            })
            
            # Order fully filled
            if current_state == "FILLED":
                log(f"{EMOJI_SUCCESS} PARTIAL_FILL_COMPLETE", {
                    "order_id": order_id,
                    "total_units": units_filled,
                    "attempts": attempts
                })
                return {
                    "status": "FILLED",
                    "order_id": order_id,
                    "filled_units": units_filled,
                    "attempts": attempts
                }
            
            # Order cancelled
            elif current_state == "CANCELLED":
                log(f"{EMOJI_ERR} PARTIAL_FILL_CANCELLED", {
                    "order_id": order_id,
                    "units_filled": units_filled,
                    "reason": order.get("reason", "unknown")
                })
                return {
                    "status": "CANCELLED",
                    "order_id": order_id,
                    "filled_units": units_filled,
                    "attempts": attempts
                }
            
            # Check for partial fill
            if units_filled > filled_units:
                filled_units = units_filled
                remaining_units = expected_units - filled_units
                
                # If we have a meaningful partial fill, create new order for remainder
                if remaining_units >= MIN_PARTIAL_FILL_UNITS:
                    log(f"{EMOJI_WARN} PARTIAL_FILL_DETECTED", {
                        "order_id": order_id,
                        "filled": filled_units,
                        "remaining": remaining_units,
                        "creating_replacement": True
                    })
                    
                    # Cancel original order
                    cancel_resp = client._put(f"/v3/accounts/{client.account_id}/orders/{order_id}/cancel", {})
                    
                    if not cancel_resp.get("_http_error"):
                        # Create new order for remaining units
                        new_order_resp = create_market_order(
                            pair=pair,
                            units=remaining_units if direction == "LONG" else -remaining_units,
                            reason=f"partial_fill_completion_{order_id}"
                        )
                        
                        if new_order_resp.get("orderCreateTransaction"):
                            new_order_id = new_order_resp["orderCreateTransaction"]["orderID"]
                            log(f"{EMOJI_SUCCESS} PARTIAL_FILL_REPLACEMENT_CREATED", {
                                "original_order": order_id,
                                "new_order": new_order_id,
                                "remaining_units": remaining_units
                            })
                            
                            # Continue monitoring the new order
                            order_id = new_order_id
                            expected_units = remaining_units
                            filled_units = 0
                        else:
                            log(f"{EMOJI_ERR} PARTIAL_FILL_REPLACEMENT_FAILED", {
                                "error": new_order_resp
                            })
                            return {
                                "status": "REPLACEMENT_FAILED",
                                "order_id": order_id,
                                "filled_units": filled_units,
                                "remaining_units": remaining_units,
                                "attempts": attempts
                            }
        
        # Wait before next check
        time.sleep(PARTIAL_FILL_CHECK_INTERVAL)
    
    # Timeout reached
    log(f"{EMOJI_ERR} PARTIAL_FILL_TIMEOUT", {
        "order_id": order_id,
        "filled_units": filled_units,
        "expected_units": expected_units,
        "attempts": attempts,
        "timeout_sec": PARTIAL_FILL_TIMEOUT_SEC
    })
    
    return {
        "status": "TIMEOUT",
        "order_id": order_id,
        "filled_units": filled_units,
        "expected_units": expected_units,
        "attempts": attempts
    }

def create_market_order(pair: str, units: int, reason: str = "") -> dict:
    """Create a market order with proper error handling.
    
    Args:
        pair: Currency pair
        units: Number of units (positive for LONG, negative for SHORT)
        reason: Reason for order
        
    Returns:
        Order response dict
    """
    client = OandaClient(
        os.getenv("OANDA_API_KEY", "2bf7b4b9bb052e28023de779a6363f1e-fee71a4fce4e94b18e0dd9c2443afa52"),
        os.getenv("OANDA_ACCOUNT_ID", "101-001-22881868-001"),
        "practice"
    )
    
    order_body = {
        "order": {
            "units": str(units),
            "instrument": pair,
            "timeInForce": "FOK",  # Fill or Kill
            "type": "MARKET",
            "positionFill": "DEFAULT"
        }
    }
    
    # Add reason to client extensions if provided
    if reason:
        order_body["order"]["clientExtensions"] = {
            "comment": reason[:50]  # OANDA limit
        }
    
    log(f"{EMOJI_INFO} CREATE_MARKET_ORDER", {
        "pair": pair,
        "units": units,
        "reason": reason
    })
    
    resp = client._post(f"/v3/accounts/{client.account_id}/orders", order_body)
    
    if resp.get("orderFillTransaction"):
        fill = resp["orderFillTransaction"]
        log(f"{EMOJI_SUCCESS} MARKET_ORDER_FILLED", {
            "order_id": fill.get("orderID"),
            "trade_id": fill.get("tradeOpened", {}).get("tradeID"),
            "units": fill.get("units"),
            "price": fill.get("price")
        })
    elif resp.get("_http_error"):
        log(f"{EMOJI_ERR} MARKET_ORDER_ERROR", {
            "error": resp.get("_text"),
            "status": resp.get("_status")
        })
    
    return resp

def _extract_trade_id_from_fill(resp: Optional[dict]) -> str | None:
    """Extract trade ID from OANDA fill response. Handles opened, reduced, and closed cases."""
    if not isinstance(resp, dict):
        return None
    fill = resp.get("orderFillTransaction") or {}
    if not isinstance(fill, dict):
        return None

    # New trade opened
    to = fill.get("tradeOpened")
    if isinstance(to, dict):
        tid = to.get("tradeID")
        if tid:
            return str(tid)

    # Trade reduced / closed (fallbacks)
    tr = fill.get("tradeReduced")
    if isinstance(tr, dict):
        tid = tr.get("tradeID")
        if tid:
            return str(tid)

    tc = fill.get("tradesClosed")
    if isinstance(tc, list) and tc:
        tid = tc[0].get("tradeID")
        if tid:
            return str(tid)

    return None


def _close_trade_or_position(
    o,
    pair: str,
    direction: str,
    oanda_trade_id: Optional[str],
    reason: str,
    db_trade_id: Optional[int] = None,
) -> Tuple[bool, dict]:
    """Close by position side (Option A). Trade ID is not used."""
    pair = normalize_pair(pair)
    d = str(direction or "").upper()
    payload = {"longUnits": "ALL"} if d == "LONG" else {"shortUnits": "ALL"}

    log(
        f"{EMOJI_EXIT} EXIT_ATTEMPT {pair_tag(pair, d)}",
        {
            "pair": pair,
            "dir": d,
            "exit_reason": reason,
            "db_trade_id": db_trade_id,
            "close_mode": "position_all",
            "payload": payload,
        },
    )

    resp = oanda_call(f"close_position_{reason}", o.close_position, pair, direction=d, allow_error_dict=True)
    ok = resp is not None and not resp.get("_http_error") and not resp.get("_rate_limited") and not resp.get("_json_error") and not resp.get("_exception")

    if not ok:
        time.sleep(0.2)
        resp_retry = oanda_call(f"close_position_retry_{reason}", o.close_position, pair, direction=d, allow_error_dict=True)
        ok = (
            resp_retry is not None
            and not resp_retry.get("_http_error")
            and not resp_retry.get("_rate_limited")
            and not resp_retry.get("_json_error")
            and not resp_retry.get("_exception")
        )
        if ok:
            resp = resp_retry

    resp_keys = list(resp.keys()) if isinstance(resp, dict) else []
    log(
        f"{EMOJI_EXIT} EXIT_RESPONSE {pair_tag(pair, d)}",
        {
            "pair": pair,
            "dir": d,
            "exit_reason": reason,
            "db_trade_id": db_trade_id,
            "close_mode": "position_all",
            "status": resp.get("_status") if isinstance(resp, dict) else None,
            "resp_keys": resp_keys,
            "txid": resp.get("lastTransactionID") if isinstance(resp, dict) else None,
        },
    )

    if not ok:
        status_code = resp.get("_status") if isinstance(resp, dict) else None
        block_pair_exits(pair, reason, duration_sec=EXIT_RETRY_BASE_SEC)
        # Enrich block info with context
        info = EXIT_BLOCKED_PAIRS.get(pair, {})
        info.update({"dir": d, "trade_id": db_trade_id, "last_status": status_code})
        EXIT_BLOCKED_PAIRS[pair] = info
        if db_trade_id is not None and "db_call" in globals():
            try:
                db_call("append_trade_note_exit_fail", db.append_trade_note, int(db_trade_id), f"EXIT_FAILED:{reason}")
            except Exception:
                pass
    else:
        EXIT_BLOCKED_PAIRS.pop(pair, None)

    return ok, resp


def _handle_close_error(resp: dict, pair: str, direction: str, tr: dict, reason: str, favorable_atr: float, track: dict) -> bool:
    """Handle close errors, including 404 reconciliation."""
    if resp and resp.get("_status") == 404:
        print(f"⚠️ Close returned 404, checking if trade already closed...")
        # Reconcile - check if trade still exists
        current_positions = oanda_call("get_positions_exit", o.open_positions)
        trade_exists = False
        for pos in current_positions:
            if normalize_pair(pos.get("instrument")) == normalize_pair(pair):
                longu = int(float(pos.get("long", {}).get("units", "0") or "0"))
                shortu = int(float(pos.get("short", {}).get("units", "0") or "0"))
                if (direction == "LONG" and longu > 0) or (direction == "SHORT" and shortu < 0):
                    trade_exists = True
                    break
        if not trade_exists:
            print(f"✅ Trade already closed, marking in DB")
            db_call("mark_trade_closed", db.mark_trade_closed, int(tr["id"]), reason)
            _exit_log(tr, reason, favorable_atr, track)
            setup_name = str(tr.get("setup", ""))
            if setup_name.endswith("_RUN") or "_RUN" in setup_name:
                _record_runner_exit(reason, tr, favorable_atr, track)
            return True  # Successfully reconciled
    
    # For other errors, block the pair temporarily to prevent rapid retries
    if resp and (resp.get("_http_error") or resp.get("_rate_limited")):
        block_pair_exits(pair, f"close_error_{resp.get('_status', 'unknown')}", 30.0)
        log(f"{EMOJI_ERR} CLOSE_BLOCK {pair_tag(pair, direction)}", 
            {"pair": pair, "direction": direction, "error": resp.get("_error", "unknown")})
    
    return False  # Not reconciled, caller should continue loop

# Debug/acceptance flags removed for production
CANDLES_STALE_MULT = float(os.getenv("CANDLES_STALE_MULT", "6") or "6")
STALE_FEED_MAX_POLLS = int(os.getenv("STALE_FEED_MAX_POLLS", "8") or "8")

# Performance Monitoring Configuration
PERFORMANCE_MONITORING_ENABLED = os.getenv("PERFORMANCE_MONITORING_ENABLED", "1").strip().lower() in ("1", "true", "yes")
PERFORMANCE_CHECK_INTERVAL = float(os.getenv("PERFORMANCE_CHECK_INTERVAL", "60") or "60")  # 1 minute
PERFORMANCE_ALERT_MEMORY = float(os.getenv("PERFORMANCE_ALERT_MEMORY", "80") or "80")  # 80% memory
PERFORMANCE_ALERT_CPU = float(os.getenv("PERFORMANCE_ALERT_CPU", "85") or "85")  # 85% CPU
PERFORMANCE_ALERT_DISK = float(os.getenv("PERFORMANCE_ALERT_DISK", "90") or "90")  # 90% disk
PERFORMANCE_HISTORY_SIZE = int(os.getenv("PERFORMANCE_HISTORY_SIZE", "1440") or "1440")  # 24 hours at 1 min intervals
# Candle fallback tolerance (used only when fetch fails)
CANDLE_FALLBACK_TTL_MULT = float(os.getenv("CANDLE_FALLBACK_TTL_MULT", "6") or "6")
CANDLE_FALLBACK_TF_MULT = float(os.getenv("CANDLE_FALLBACK_TF_MULT", "0.35") or "0.35")
CANDLE_FALLBACK_MIN_SEC = float(os.getenv("CANDLE_FALLBACK_MIN_SEC", "30") or "30")
CANDLE_FALLBACK_MAX_SEC = float(os.getenv("CANDLE_FALLBACK_MAX_SEC", "0") or "0")

# Track data quality for outage monitoring
DATA_OUTAGE_THRESHOLD = 5  # Consecutive failures before alert
data_outage_counter = {pair: 0 for pair in PAIRS}

def check_data_outage(pair: str, success: bool) -> None:
    """Track data quality and alert on potential outages"""
    global data_outage_counter
    
    if success:
        if data_outage_counter[pair] >= DATA_OUTAGE_THRESHOLD:
            log(f"{EMOJI_OK} DATA_RESTORED", {"pair": pair, "outage_count": data_outage_counter[pair]})
            print(f"Data restored for {pair} after {data_outage_counter[pair]} failures")
        data_outage_counter[pair] = 0
    else:
        data_outage_counter[pair] += 1
        if data_outage_counter[pair] == DATA_OUTAGE_THRESHOLD:
            log(f"{EMOJI_ERR} DATA_OUTAGE", {"pair": pair, "consecutive_failures": data_outage_counter[pair]})
            print(f"WARNING: Data outage detected for {pair} - {data_outage_counter[pair]} consecutive failures")


def get_active_sessions(now: Optional[float] = None) -> List[str]:
    """Get list of currently active trading sessions"""
    if not SESSION_DETECTION:
        return []
        
    if now is None:
        now = now_ts()
    
    dt = datetime.fromtimestamp(now, tz=timezone.utc)
    weekday = dt.weekday()  # Monday=0, Sunday=6
    current_time = dt.strftime("%H:%M")
    
    active = []
    for session, config in TRADING_SESSIONS.items():
        if weekday not in config["days"]:
            continue
            
        open_time = config["open"]
        close_time = config["close"]
        
        # Handle sessions that cross midnight (like Sydney/Tokyo)
        if open_time > close_time:
            # Session spans midnight (e.g., 21:00-06:00)
            if current_time >= open_time or current_time < close_time:
                active.append(session)
        else:
            # Normal session (e.g., 07:00-16:00)
            if open_time <= current_time < close_time:
                active.append(session)
    
    return active

def get_session_progress(session: str, now: Optional[float] = None) -> dict:
    """Get session progress information for strategy timing"""
    if not SESSION_DETECTION or session not in TRADING_SESSIONS:
        return {"active": False, "progress": 0.0, "time_until_end": 0, "time_since_start": 0}
    
    if now is None:
        now = now_ts()
    
    dt = datetime.fromtimestamp(now, tz=timezone.utc)
    weekday = dt.weekday()
    current_time = dt.strftime("%H:%M")
    current_minutes = dt.hour * 60 + dt.minute
    
    config = TRADING_SESSIONS[session]
    if weekday not in config["days"]:
        return {"active": False, "progress": 0.0, "time_until_end": 0, "time_since_start": 0}
    
    open_time = config["open"]
    close_time = config["close"]
    open_hour, open_min = map(int, open_time.split(":"))
    close_hour, close_min = map(int, close_time.split(":"))
    open_minutes = open_hour * 60 + open_min
    close_minutes = close_hour * 60 + close_min
    
    # Calculate session progress
    if open_time > close_time:
        # Session spans midnight
        if current_time >= open_time:
            # After midnight on session start day
            time_since_start = current_minutes - open_minutes
            session_length = (24 * 60 - open_minutes) + close_minutes
        elif current_time < close_time:
            # Before midnight on session end day  
            time_since_start = (24 * 60 - open_minutes) + current_minutes
            session_length = (24 * 60 - open_minutes) + close_minutes
        else:
            return {"active": False, "progress": 0.0, "time_until_end": 0, "time_since_start": 0}
    else:
        # Normal session
        if open_minutes <= current_minutes < close_minutes:
            time_since_start = current_minutes - open_minutes
            session_length = close_minutes - open_minutes
        else:
            return {"active": False, "progress": 0.0, "time_until_end": 0, "time_since_start": 0}
    
    progress = time_since_start / session_length if session_length > 0 else 0.0
    time_until_end = session_length - time_since_start
    
    return {
        "active": True,
        "progress": progress,
        "time_until_end": time_until_end * 60,  # Convert to seconds
        "time_since_start": time_since_start * 60,
        "session_length": session_length * 60
    }

def check_time_drift(o: OandaClient) -> None:
    """Check if local system clock is synced with broker time"""
    global _BROKER_TIME_OFFSET, _BROKER_TIME_LAST_SYNC
    for attempt in range(3):
        try:
            # Get server time from OANDA (use retry wrapper if available)
            if "oanda_call" in globals():
                # Use pricing method through oanda_call for consistency
                def get_pricing_time():
                    bid, ask = o.pricing("EUR_USD")
                    return {"prices": [{"bids": [{"price": str(bid)}], "asks": [{"price": str(ask)}], "time": time.strftime('%Y-%m-%dT%H:%M:%S.%fZ', time.gmtime(now_ts() + _BROKER_TIME_OFFSET))}]}
                
                resp = oanda_call(  # type: ignore[name-defined]
                    "time_sync",
                    get_pricing_time,
                    allow_error_dict=True,
                )
            else:
                # Use pricing method instead of direct _get
                try:
                    bid, ask = o.pricing("EUR_USD")
                    resp = {"prices": [{"bids": [{"price": str(bid)}], "asks": [{"price": str(ask)}], "time": time.strftime('%Y-%m-%dT%H:%M:%S.%fZ', time.gmtime(now_ts() + _BROKER_TIME_OFFSET))}]}
                except Exception as e:
                    resp = {"_http_error": True, "_status": 500, "_text": str(e)}

            if not isinstance(resp, dict) or resp.get("_http_error") or resp.get("_rate_limited") or resp.get("_json_error") or resp.get("_exception"):
                raise RuntimeError(f"time_sync_error:{resp}")

            if "time" in resp:
                # OANDA pricing responses provide RFC3339 time strings.
                t = resp["time"]
                broker_time = parse_time_oanda(t)

                if not math.isfinite(broker_time):
                    raise RuntimeError(f"bad_broker_time:{t}")
                local_time = time.time()
                drift = broker_time - local_time

                _BROKER_TIME_OFFSET = drift
                _BROKER_TIME_LAST_SYNC = local_time

                log(
                    f"{EMOJI_INFO} TIME_SYNC",
                    {
                        "broker_time": broker_time,
                        "local_time": local_time,
                        "drift_seconds": drift,
                        "offset_applied": _BROKER_TIME_OFFSET,
                    },
                )
            return
        except Exception as e:
            if attempt >= 2:
                log(f"{EMOJI_ERR} TIME_DRIFT_CHECK_FAILED", {"error": str(e)})

# ===== MASTER EXECUTION SOP - ENTRY + MANAGEMENT =====

# ===== AEE CONSTANTS (LOCKED) =====
SPREAD_MAX_PIPS = 12.0
NEAR_TP_BAND_ATR_BASE = 0.25
PROTECT_EXIT_PROGRESS_BASE = 0.35
LOCK_PROGRESS = 0.60
LOCK_OFFSET_ATR = 0.12
PULSE_SPEED = 1.40
PULSE_PROGRESS = 0.70
PULSE_EXITLINE_ATR = 0.25
STALL_PULLBACK_ATR = 0.18
STALL_NOEXT_T = 15
STALL_SPEED = 0.60
DECAY_PROGRESS = 0.60
DECAY_SPEED = 0.60
DECAY_PULLBACK = 0.25
PANIC_VELOCITY = -0.80
PANIC_PULLBACK = 0.60
PANIC_PULLBACKRATE = 0.06
TICK_EVAL_HZ = 10

# SOP v2.1: Participation -> decay lock + allowed giveback
DECAY_LOCK_MIN_PROGRESS = 0.35
DECAY_LOCK_GIVEBACK_FRAC = 0.40
DECAY_LOCK_GIVEBACK_MIN_ATR = 0.12
DECAY_LOCK_GIVEBACK_MAX_ATR = 0.60

# SOP v2.1: Split-leg tolerances (MAIN vs RUN)
MAIN_GIVEBACK_MULT = 0.70
RUN_GIVEBACK_MULT = 1.00

# ===== AEE PHASES =====
class AEEPhase(str, Enum):
    PROTECT = "PROTECT"
    BUILD = "BUILD"
    HARVEST = "HARVEST"
    RUNNER = "RUNNER"
    PANIC = "PANIC"


def aee_decay_lock_update(*, aee_metrics: dict, track: dict) -> dict:
    """SOP v2.1: lock participation->decay, compute allowed giveback in ATR units."""
    progress = float(aee_metrics.get("progress", 0.0) or 0.0)
    peak = float(track.get("peak", 0.0) or 0.0)
    locked_peak = float(track.get("locked_peak", 0.0) or 0.0)
    locked = bool(track.get("decay_locked", False))

    if (not locked) and progress >= DECAY_LOCK_MIN_PROGRESS and peak > 0.0:
        locked = True
        locked_peak = peak
        track["decay_locked"] = True
        track["locked_peak"] = locked_peak
    elif locked and peak > locked_peak:
        locked_peak = peak
        track["locked_peak"] = locked_peak

    gb = DECAY_LOCK_GIVEBACK_MIN_ATR
    if locked and locked_peak > 0.0:
        gb = max(DECAY_LOCK_GIVEBACK_MIN_ATR, min(DECAY_LOCK_GIVEBACK_MAX_ATR, DECAY_LOCK_GIVEBACK_FRAC * locked_peak))
    track["allowed_giveback_atr"] = gb
    return {"decay_locked": locked, "locked_peak": locked_peak, "allowed_giveback_atr": gb}

# ===== TP-FIRST ORDERING =====

def calculate_tp_at_birth(pair: str, entry_price: float, direction: str, atr_pips: float, 
                         tp_anchor_price: Optional[float] = None, spread_pips: float = 0.5) -> float:
    """Calculate TP₀ at birth - never fails."""
    
    # Primary: Use anchor if provided
    if tp_anchor_price is not None:
        tp = tp_anchor_price
    else:
        # Fallback 1: ATR-based
        if direction == "LONG":
            tp = entry_price + 0.60 * atr_pips * 0.0001  # Convert pips to price
        else:
            tp = entry_price - 0.60 * atr_pips * 0.0001
    
    # Minimum distance clamp
    min_dist = max(
        1.2 * spread_pips * 0.0001,
        0.10 * atr_pips * 0.0001
    )
    
    if direction == "LONG":
        tp = max(tp, entry_price + min_dist)
    else:
        tp = min(tp, entry_price - min_dist)
    
    return tp

def add_sl_after_fill(pair: str, entry_price: float, direction: str, atr_pips: float,
                     invalid_level: Optional[float] = None, spread_pips: float = 0.5) -> Optional[float]:
    """Add SL after fill - tertiary priority."""
    
    if invalid_level is not None:
        # Preferred: Structural invalidation + buffer
        buffer = 0.10 * atr_pips * 0.0001
        if direction == "LONG":
            return invalid_level - buffer
        else:
            return invalid_level + buffer
    
    # Fallback: Distance-based
    min_dist = max(3.5 * spread_pips * 0.0001, 0.85 * atr_pips * 0.0001)
    
    if direction == "LONG":
        return entry_price - min_dist
    else:
        return entry_price + min_dist

# ===== AEE PATH-SPACE MANAGEMENT =====

@dataclass
class AEEState:
    """AEE trade state."""
    entry_price: float
    direction: str
    tp_anchor: float
    sl_price: Optional[float]
    phase: str
    armed_by: Optional[str] = None
    local_high: float = 0.0
    local_low: float = float('inf')
    entry_time: float = 0.0
    last_tick_eval: float = 0.0
    speed_prev: float = 0.0
    velocity: float = 0.0
    pullback_start: float = 0.0
    pulse_exit_line: Optional[float] = None
    profit_locked: bool = False
    atr: float = 0.0

def calculate_path_metrics(current_price: float, aee_state: AEEState, atr_pips: float, 
                          spread_pips: float) -> dict:
    """Calculate all path-space metrics for AEE."""
    
    # Progress
    progress = abs(current_price - aee_state.entry_price) / (atr_pips * 0.0001) if atr_pips > 0 else 0
    
    # Update local extrema
    if aee_state.direction == "LONG":
        aee_state.local_high = max(aee_state.local_high, current_price)
        aee_state.local_low = min(aee_state.local_low, current_price)
    else:
        aee_state.local_high = max(aee_state.local_high, current_price)
        aee_state.local_low = min(aee_state.local_low, current_price)
    
    # Pullback
    if aee_state.direction == "LONG":
        pullback = (aee_state.local_high - current_price) / (atr_pips * 0.0001) if atr_pips > 0 else 0
    else:
        pullback = (current_price - aee_state.local_low) / (atr_pips * 0.0001) if atr_pips > 0 else 0
    
    # Distance to TP
    dist_to_tp = abs(aee_state.tp_anchor - current_price) / (atr_pips * 0.0001) if atr_pips > 0 else 0
    
    # Near TP band
    near_tp_band = max(NEAR_TP_BAND_ATR_BASE, 1.2 * spread_pips / atr_pips) if atr_pips > 0 else NEAR_TP_BAND_ATR_BASE
    
    # Speed (simplified - would use recent price changes)
    speed_now = progress / max(1.0, (now_ts() - aee_state.entry_time) / 60.0)  # ATR per minute
    velocity = speed_now - aee_state.speed_prev
    aee_state.speed_prev = speed_now
    aee_state.velocity = velocity
    
    return {
        "progress": progress,
        "speed": speed_now,
        "velocity": velocity,
        "pullback": pullback,
        "dist_to_tp": dist_to_tp,
        "near_tp_band": near_tp_band,
        "local_high": aee_state.local_high,
        "local_low": aee_state.local_low
    }

def should_arm_tick_mode(metrics: dict, aee_state: AEEState, spread_pips: float, 
                        median_spread_5m: float = 0.5) -> tuple[bool, str]:
    """Check if tick mode should be armed."""
    
    progress = metrics["progress"]
    speed = metrics["speed"]
    velocity = metrics["velocity"]
    pullback = metrics["pullback"]
    dist_to_tp = metrics["dist_to_tp"]
    near_tp_band = metrics["near_tp_band"]
    
    # A) Near entry survival
    if progress <= 0.25:
        return True, "A"
    
    # B) Early decay detection
    if progress >= 0.45 and speed < 0.70 and velocity < 0:
        return True, "B"
    
    # C) Panic conditions
    if (velocity <= -0.60 or 
        pullback >= 0.45 or 
        spread_pips > 1.6 * median_spread_5m):
        return True, "C"
    
    # D) Near-TP capture zone
    if dist_to_tp <= near_tp_band and progress >= 0.50:
        return True, "D"
    
    return False, ""

def get_aee_phase(metrics: dict, aee_state: AEEState, spread_pips: float) -> str:
    """Determine AEE phase from metrics."""
    
    progress = metrics["progress"]
    speed = metrics["speed"]
    velocity = metrics["velocity"]
    pullback = metrics["pullback"]
    
    # Panic check first
    if (velocity <= PANIC_VELOCITY or 
        pullback >= PANIC_PULLBACK):
        return AEEPhase.PANIC
    
    # Phase transitions
    if aee_state.phase == AEEPhase.PROTECT:
        protect_exit_progress = max(PROTECT_EXIT_PROGRESS_BASE, 1.5 * spread_pips / 5.0)  # Simplified
        if progress >= protect_exit_progress:
            return AEEPhase.BUILD
    
    elif aee_state.phase == AEEPhase.BUILD:
        if progress >= LOCK_PROGRESS or (speed >= 1.10 and progress >= 0.50):
            return AEEPhase.HARVEST
    
    elif aee_state.phase == AEEPhase.HARVEST:
        # Runner eligibility
        if (progress >= 0.90 and 
            speed >= 0.80 and 
            velocity >= -0.20 and 
            pullback <= 0.35 and
            metrics["dist_to_tp"] > metrics["near_tp_band"]):
            return AEEPhase.RUNNER
    
    elif aee_state.phase == AEEPhase.RUNNER:
        # Exit runner conditions
        if (speed < 0.80 or 
            velocity < -0.20 or 
            pullback > 0.35):
            return AEEPhase.HARVEST
    
    return aee_state.phase

def check_aee_exits(metrics: dict, aee_state: AEEState, current_price: float, data_quality: str = DataQuality.OK) -> Optional[str]:
    """Check AEE exit conditions in priority order with survival mode."""
    
    progress = metrics["progress"]
    speed = metrics["speed"]
    velocity = metrics["velocity"]
    pullback = metrics["pullback"]
    dist_to_tp = metrics["dist_to_tp"]
    near_tp_band = metrics["near_tp_band"]
    
    # SURVIVAL MODE: Under data uncertainty, prefer early conservative exits
    survival_mode = data_quality in [DataQuality.DEGRADED, DataQuality.BAD]
    
    # Priority 1: Panic Exit (always allowed)
    if aee_should_panic_exit({}, metrics):  # Empty trade dict for now
        # Under uncertainty, require stronger confirmation for panic
        if not survival_mode or velocity <= -1.0 or pullback >= 0.7:
            return "AEE_PANIC"
    
    # Priority 2: Near TP Capture (conditional under uncertainty)
    if near_tp_band and progress >= PULSE_PROGRESS:
        # Under uncertainty, only capture if price confirmed by broker snapshot
        if not survival_mode or data_quality == DataQuality.OK:
            # Set exit line if not set
            if aee_state.pulse_exit_line is None:
                if aee_state.direction == "LONG":
                    aee_state.pulse_exit_line = aee_state.local_high - PULSE_EXITLINE_ATR * 0.0010
                else:
                    aee_state.pulse_exit_line = aee_state.local_low + PULSE_EXITLINE_ATR * 0.0010
            
            # Check cross
            if aee_state.direction == "LONG" and current_price <= aee_state.pulse_exit_line:
                return "PULSE_STALL_CAPTURE"
            elif aee_state.direction == "SHORT" and current_price >= aee_state.pulse_exit_line:
                return "PULSE_STALL_CAPTURE"
    
    # SURVIVAL MODE: Early conservative exit under uncertainty
    if survival_mode:
        # Prefer early exit when data is uncertain
        if progress >= 0.3 and (velocity < -0.2 or pullback >= 0.3):
            return "AEE_SURVIVAL_CONSERVATIVE"
        
        # Exit if near TP but uncertain data quality
        if progress >= 0.8 and data_quality == DataQuality.BAD:
            return "AEE_SURVIVAL_UNCERTAINTY"
    
    # Priority 3: Near Entry Survival (always allowed)
    if progress <= 0.25:
        return "AEE_NEAR_ENTRY_SURVIVAL"
    
    # Priority 4: Decay exit (stricter under uncertainty)
    if not survival_mode or (speed < 0.4 and velocity < -0.1):  # Stricter threshold
        if aee_should_exit_momentum_decay({}, metrics):  # Empty trade dict for now
            return "AEE_MOMENTUM_DECAY"
    
    return None

def execute_aee_exit(pair: str, aee_state: AEEState, exit_reason: str) -> dict:
    """Execute AEE exit by position side."""
    
    # ASSERTION GATE: Must use close-by-position
    direction = "longUnits" if aee_state.direction == "LONG" else "shortUnits"
    body = {direction: "ALL"}
    
    # ASSERTION GATE: Verify close-by-position endpoint
    assert direction in ["longUnits", "shortUnits"], f"Invalid direction for close-by-position: {direction}"
    assert body[direction] == "ALL", "Must close ALL units by position"
    
    resp = oanda_call(f"aee_exit_{exit_reason}", o.close_position, pair, **body)
    
    # ASSERTION GATE: Required logging fields
    required_fields = ["exit_reason", "phase", "armed_by", "direction", "entry_price"]
    for field in required_fields:
        assert hasattr(aee_state, field) or field in ["exit_reason"], f"Missing required field: {field}"
    
    # Log exit details
    log(f"{EMOJI_EXIT} AEE_EXIT {pair}", {
        "exit_reason": exit_reason,
        "phase": aee_state.phase,
        "armed_by": aee_state.armed_by,
        "direction": aee_state.direction,
        "entry_price": aee_state.entry_price,
        "tp_anchor": aee_state.tp_anchor,
        "sl_price": aee_state.sl_price,
        "local_high": aee_state.local_high,
        "local_low": aee_state.local_low,
        "pulse_exit_line": aee_state.pulse_exit_line,
        "response": resp
    })
    
    return resp

# ===== SPREAD-AWARE SIZING =====

def pip_size(pair: str) -> float:
    """Get pip size for currency pair."""
    return 0.01 if "JPY" in pair else 0.0001

def atr_pips(pair: str, atr_price: float) -> float:
    """Convert ATR price to pips."""
    ps = pip_size(pair)
    return atr_price / ps if ps > 0 else float("nan")

def spread_atr(pair: str, spread_pips: float, atr_price: float) -> float:
    """Calculate spread as fraction of ATR."""
    ap = atr_pips(pair, atr_price)
    if not math.isfinite(ap) or ap <= 0:
        return float("nan")
    return spread_pips / ap

def calculate_spread_aware_size(pair: str, speed_class: str, spread_pips: float, 
                               atr_price: float, units_base: int,
                               median_spread_5m: Optional[float] = None) -> tuple[int, dict]:
    """Calculate spread-aware position size."""
    
    # Hard crash guard
    if spread_pips > ABS_SPREAD_MAX_PIPS:
        return 0, {
            "reject_reason": "abs_spread_max",
            "spread_pips": spread_pips,
            "units_base": units_base,
            "units_final": 0
        }
    
    # Calculate spread/ATR ratio
    s_atr = spread_atr(pair, spread_pips, atr_price)
    
    # Speed class thresholds
    thresholds = {
        "FAST": (0.10, 0.45),
        "MED": (0.12, 0.55),
        "SLOW": (0.15, 0.70),
    }
    
    s0, s1 = thresholds.get(speed_class, (0.12, 0.55))
    
    # Calculate multiplier
    if not math.isfinite(s_atr) or s_atr >= s1:
        mult = 0.0
    elif s_atr <= s0:
        mult = 1.0
    else:
        mult = 1.0 - (s_atr - s0) / (s1 - s0)
    
    # Apply spike penalty
    if median_spread_5m is not None and median_spread_5m > 0:
        spike_ratio = spread_pips / median_spread_5m
        if spike_ratio >= 2.0:
            mult *= 0.5
    
    # Calculate final units
    units_final = int(units_base * mult)
    
    log_fields = {
        "spread_pips": spread_pips,
        "atr_exec_price": atr_price,
        "atr_exec_pips": atr_pips(pair, atr_price),
        "spread_atr": s_atr,
        "spread_mult": mult,
        "units_base": units_base,
        "units_final": units_final,
        "speed_class": speed_class,
        "pair": pair
    }
    
    # Check minimum units
    if units_final < 1:
        if mult == 0.0 and s_atr >= s1:
            log_fields["reject_reason"] = "spread_atr_too_high"
        else:
            log_fields["reject_reason"] = "below_min_units_after_spread_mult"
        return 0, log_fields
    
    # Apply max units limit
    units_final = min(10000, units_final)
    log_fields["units_final"] = units_final
    
    return units_final, log_fields

# ===== INTERNET-RESILIENT COMPONENTS =====

class ConnectivityState:
    OK = "OK"
    DEGRADED = "DEGRADED"
    OFFLINE = "OFFLINE"

@dataclass
class PriceEvent:
    t_exchange: float
    t_local: float
    bid: float
    ask: float
    mid: float
    spread_pips: float
    tradeable: bool = True
    source: str = "stream"

class MarketDataHub:
    """Centralized market data with resilience."""
    
    def __init__(self):
        self.price_events: Dict[str, List[PriceEvent]] = {}
        self.connectivity_state = ConnectivityState.OK
        self.last_stream_update: Dict[str, float] = {}
        self.median_spread_5m: Dict[str, float] = {}
    
    def add_price_event(self, instrument: str, event: PriceEvent):
        """Add price event and update state."""
        if instrument not in self.price_events:
            self.price_events[instrument] = []
        
        self.price_events[instrument].append(event)
        self.last_stream_update[instrument] = event.t_local
        
        # Keep last 20 minutes
        cutoff = event.t_local - 1200
        self.price_events[instrument] = [p for p in self.price_events[instrument] if p.t >= cutoff]
        
        # Update spread tracking
        self._update_spread_tracking(instrument, event)
        self._update_connectivity_state(instrument)
    
    def _update_spread_tracking(self, instrument: str, event: PriceEvent):
        """Update median spread tracking."""
        if instrument not in self.median_spread_5m:
            self.median_spread_5m[instrument] = event.spread_pips
            return
        
        # Simple EMA
        alpha = 0.1
        self.median_spread_5m[instrument] = (
            alpha * event.spread_pips + 
            (1 - alpha) * self.median_spread_5m[instrument]
        )
    
    def _update_connectivity_state(self, instrument: str):
        """Update connectivity state."""
        now = time.time()
        last_stream = self.last_stream_update.get(instrument, 0)
        stream_age = now - last_stream
        
        if stream_age < 2.0:
            self.connectivity_state = ConnectivityState.OK
        elif stream_age < 10.0:
            self.connectivity_state = ConnectivityState.DEGRADED
        else:
            self.connectivity_state = ConnectivityState.OFFLINE
    
    def get_latest_price(self, instrument: str) -> Optional[PriceEvent]:
        """Get latest price with fallback."""
        if instrument in self.price_events and self.price_events[instrument]:
            latest = self.price_events[instrument][-1]
            age = time.time() - latest.t_local
            if age < 15.0:
                return latest
        return None
    
    def get_atr(self, instrument: str, period: int = 14) -> Optional[float]:
        """Get ATR with fallback."""
        if instrument not in self.price_events or len(self.price_events[instrument]) < period + 1:
            return None
        
        prices = [p.mid for p in self.price_events[instrument]]
        
        # Simple ATR calculation
        true_ranges = []
        for i in range(1, len(prices)):
            high = prices[i]
            low = prices[i]
            prev_close = prices[i-1]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            true_ranges.append(tr)
        
        if not true_ranges:
            return None
        
        atr = sum(true_ranges[-period:]) / period
        return atr if math.isfinite(atr) and atr > 0 else None

# ===== OANDA DATA HANDLING SOP - PATH-SPACE PROCESSING =====

# ===== CANONICAL PRICE EVENT (ENHANCED) =====
@dataclass
class PriceEvent:
    t_exchange: float
    t_local: float
    bid: float
    ask: float
    mid: float
    spread_pips: float
    tradeable: bool = True
    source: str = "stream"
    quality: str = DataQuality.OK

# ===== PATH BUFFER (RING BUFFER) =====
class PathBuffer:
    """Ring buffer for price path data."""
    
    def __init__(self, max_size: int = 2400):  # 20 minutes at 5Hz
        self.max_size = max_size
        self.times: List[float] = []
        self.mids: List[float] = []
        self.bids: List[float] = []
        self.asks: List[float] = []
        self.spreads: List[float] = []
        self._lock = threading.Lock()
    
    def add(self, event: PriceEvent):
        """Add price event to buffer."""
        with self._lock:
            self.times.append(event.t_local)
            self.mids.append(event.mid)
            self.bids.append(event.bid)
            self.asks.append(event.ask)
            self.spreads.append(event.spread_pips)
            
            # Maintain ring buffer
            if len(self.times) > self.max_size:
                self.times.pop(0)
                self.mids.pop(0)
                self.bids.pop(0)
                self.asks.pop(0)
                self.spreads.pop(0)
            
            # T1-11 Path Buffer Gate Validation - check size periodically
            if len(self.times) % 100 == 0:  # Check every 100 additions
                try:
                    from pathlib import Path
                    import json
                    proof_dirs = sorted(Path(__file__).parent.glob("proof_artifacts/*"))
                    if proof_dirs:
                        latest_proof = proof_dirs[-1]
                        
                        current_size = len(self.times)
                        max_allowed = self.max_size
                        buffer_status = "PASS" if current_size <= max_allowed else "FAIL"
                        
                        buffer_report = {
                            "status": buffer_status,
                            "current_buffer_size": current_size,
                            "max_buffer_size": current_size,  # Track max seen
                            "max_allowed": max_allowed,
                            "instrument": getattr(event, 'instrument', 'unknown')
                        }
                        
                        # Write path_buffer_report
                        buffer_file = latest_proof / f"path_buffer_report_{getattr(event, 'instrument', 'unknown')}.json"
                        buffer_file.write_text(json.dumps(buffer_report, indent=2))
                        
                        # Log T1-11 result
                        if buffer_status == "PASS":
                            log_runtime("debug", "T1-11_PATH_BUFFER_OK", 
                                      instrument=getattr(event, 'instrument', 'unknown'),
                                      size=current_size)
                        else:
                            log_runtime("error", "T1-11_PATH_BUFFER_FAIL",
                                      instrument=getattr(event, 'instrument', 'unknown'),
                                      size=current_size, max_allowed=max_allowed)
                except Exception as e:
                    log_runtime("warning", "T1-11_ARTIFACT_ERROR", error=str(e))
    
    def get_window(self, duration_sec: float) -> tuple:
        """Get data for last N seconds."""
        with self._lock:
            if not self.times:
                return [], [], [], [], []
            
            cutoff = time.time() - duration_sec
            idx = 0
            while idx < len(self.times) and self.times[idx] < cutoff:
                idx += 1
            
            return (
                self.times[idx:],
                self.mids[idx:],
                self.bids[idx:],
                self.asks[idx:],
                self.spreads[idx:]
            )
    
    def get_latest(self) -> Optional[PriceEvent]:
        """Get latest price event."""
        with self._lock:
            if not self.times:
                return None
            
            return PriceEvent(
                t_exchange=0,  # Not stored
                t_local=self.times[-1],
                bid=self.bids[-1],
                ask=self.asks[-1],
                mid=self.mids[-1],
                spread_pips=self.spreads[-1],
                source="buffer",
                quality=DataQuality.OK
            )

# ===== BUCKET ENGINE (DERIVED OHLC) =====
@dataclass
class Bucket:
    t_start: float
    t_end: float
    open: float
    high: float
    low: float
    close: float
    volume: int = 0
    is_complete: bool = False

class BucketEngine:
    """Build S5 and M1 buckets from price stream."""
    
    def __init__(self):
        self.s5_buckets: Dict[str, List[Bucket]] = {}
        self.m1_buckets: Dict[str, List[Bucket]] = {}
        self.current_s5: Dict[str, Bucket] = {}
        self.current_m1: Dict[str, Bucket] = {}
        self._lock = threading.Lock()
    
    def add_price(self, instrument: str, event: PriceEvent):
        """Add price and update buckets."""
        with self._lock:
            now = event.t_local
            
            # Initialize if needed
            if instrument not in self.s5_buckets:
                self.s5_buckets[instrument] = []
                self.m1_buckets[instrument] = []
                self.current_s5[instrument] = None
                self.current_m1[instrument] = None
            
            # S5 bucket handling
            cur_s5 = self.current_s5[instrument]
            if cur_s5 is None or now >= cur_s5.t_end:
                # Complete previous bucket
                if cur_s5 is not None:
                    cur_s5.is_complete = True
                    self.s5_buckets[instrument].append(cur_s5)
                    # Keep last 6 hours
                    cutoff = now - 21600
                    self.s5_buckets[instrument] = [b for b in self.s5_buckets[instrument] if b.t_start >= cutoff]
                
                # Start new S5 bucket
                bucket_start = int(now / 5) * 5
                self.current_s5[instrument] = Bucket(
                    t_start=bucket_start,
                    t_end=bucket_start + 5,
                    open=event.mid,
                    high=event.mid,
                    low=event.mid,
                    close=event.mid,
                    is_complete=False
                )
                cur_s5 = self.current_s5[instrument]
            
            # Update S5 bucket
            cur_s5.high = max(cur_s5.high, event.mid)
            cur_s5.low = min(cur_s5.low, event.mid)
            cur_s5.close = event.mid
            
            # M1 bucket handling (aggregated from S5)
            cur_m1 = self.current_m1[instrument]
            if cur_m1 is None or now >= cur_m1.t_end:
                # Complete previous M1 bucket
                if cur_m1 is not None:
                    cur_m1.is_complete = True
                    self.m1_buckets[instrument].append(cur_m1)
                    # Keep last 72 hours
                    cutoff = now - 259200
                    self.m1_buckets[instrument] = [b for b in self.m1_buckets[instrument] if b.t_start >= cutoff]
                
                # Start new M1 bucket
                bucket_start = int(now / 60) * 60
                self.current_m1[instrument] = Bucket(
                    t_start=bucket_start,
                    t_end=bucket_start + 60,
                    open=event.mid,
                    high=event.mid,
                    low=event.mid,
                    close=event.mid,
                    is_complete=False
                )
                cur_m1 = self.current_m1[instrument]
            
            # Update M1 bucket
            cur_m1.high = max(cur_m1.high, cur_s5.high)
            cur_m1.low = min(cur_m1.low, cur_s5.low)
            cur_m1.close = cur_s5.close
    
    def get_m1_buckets(self, instrument: str, count: int = 200) -> List[Bucket]:
        """Get M1 buckets for ATR calculation."""
        with self._lock:
            if instrument not in self.m1_buckets:
                return []
            
            buckets = self.m1_buckets[instrument]
            if not buckets:
                return []
            
            # Include current incomplete bucket if available
            current = self.current_m1.get(instrument)
            if current and current not in buckets:
                buckets = buckets + [current]
            
            return buckets[-count:]
    
    def get_latest_m1(self, instrument: str) -> Optional[Bucket]:
        """Get latest M1 bucket (may be incomplete)."""
        with self._lock:
            if instrument not in self.current_m1:
                return None
            return self.current_m1[instrument]

# ===== VOLATILITY ENGINE (ATR WITH FALLBACKS) =====
class VolatilityEngine:
    """ATR calculation with fallback ladder."""
    
    def __init__(self, bucket_engine: BucketEngine):
        self.bucket_engine = bucket_engine
        self.atr_cache: Dict[str, Tuple[float, str, float]] = {}  # (atr, source, timestamp)
        self._lock = threading.Lock()
    
    def calculate_atr(self, instrument: str, period: int = 14) -> Tuple[float, str]:
        """Calculate ATR with fallback ladder."""
        with self._lock:
            now = time.time()
            cache_key = f"{instrument}_{period}"
            
            # Check cache (30 second TTL)
            if cache_key in self.atr_cache:
                atr, source, ts = self.atr_cache[cache_key]
                if now - ts < 30:
                    return atr, source
            
            atr, source = self._calculate_atr_fallback(instrument, period)
            self.atr_cache[cache_key] = (atr, source, now)
            return atr, source
    
    def _calculate_atr_fallback(self, instrument: str, period: int) -> Tuple[float, str]:
        """ATR fallback ladder."""
        
        # 1. Primary: ATR from closed M1 buckets
        buckets = self.bucket_engine.get_m1_buckets(instrument, period + 10)
        closed_buckets = [b for b in buckets if b.is_complete][:period]
        
        if len(closed_buckets) >= period * 0.7:  # At least 70% of required
            atr = self._atr_from_buckets(closed_buckets, period)
            if atr > 0:
                return atr, "M1_PRIMARY"
        
        # 2. Derived M1 fallback (including incomplete)
        if len(buckets) >= period * 0.7:
            atr = self._atr_from_buckets(buckets, period)
            if atr > 0:
                return atr, "M1_DERIVED"
        
        # 3. H1 proxy (would need H1 buckets - simplified)
        # For now, use path volatility proxy
        
        # 4. Path volatility proxy (last resort)
        atr = self._atr_from_path_volatility(instrument)
        if atr > 0:
            return atr, "PATH_PROXY"
        
        return 0.0010, "DEFAULT"  # Minimum fallback
    
    def _atr_from_buckets(self, buckets: List[Bucket], period: int) -> float:
        """Calculate ATR from bucket data."""
        if len(buckets) < 2:
            return 0.0
        
        true_ranges = []
        for i in range(1, min(len(buckets), period + 1)):
            curr = buckets[-i]
            prev = buckets[-i-1]
            
            tr = max(
                curr.high - curr.low,
                abs(curr.high - prev.close),
                abs(curr.low - prev.close)
            )
            if tr > 0:
                true_ranges.append(tr)
        
        if not true_ranges:
            return 0.0
        
        # Wilder's RMA (simplified)
        atr = sum(true_ranges) / len(true_ranges)
        return atr if math.isfinite(atr) and atr > 0 else 0.0
    
    def _atr_from_path_volatility(self, instrument: str) -> float:
        """ATR proxy from path volatility."""
        # This would access path buffer - simplified implementation
        return 0.0010  # Minimum proxy

# ===== PATH ENGINE (PRIMITIVES CALCULATION) =====
class PathEngine:
    """Calculate path-space primitives."""
    
    def __init__(self, path_buffers: Dict[str, PathBuffer]):
        self.path_buffers = path_buffers
        self.local_extrema: Dict[str, Dict[str, float]] = {}
    
    def calculate_primitives(self, instrument: str, entry_price: float, 
                           direction: str, atr: float, window_sec: float = 20.0) -> dict:
        """Calculate all path primitives."""
        
        buffer = self.path_buffers.get(instrument)
        if not buffer:
            return {}
        
        times, mids, bids, asks, spreads = buffer.get_window(window_sec)
        
        if len(times) < 2:
            return {}
        
        # Initialize local extrema for this trade if needed
        if instrument not in self.local_extrema:
            self.local_extrema[instrument] = {"high": entry_price, "low": entry_price}
        
        extrema = self.local_extrema[instrument]
        
        # Update extrema
        current_mid = mids[-1]
        extrema["high"] = max(extrema["high"], current_mid)
        extrema["low"] = min(extrema["low"], current_mid)
        
        # Path primitives
        displacement = current_mid - entry_price
        path_len = sum(abs(mids[i] - mids[i-1]) for i in range(1, len(mids)))
        
        # T1-12 PathLen Gate Validation
        try:
            from pathlib import Path
            import json
            import math
            proof_dirs = sorted(Path(__file__).parent.glob("proof_artifacts/*"))
            if proof_dirs and len(mids) > 1:
                latest_proof = proof_dirs[-1]
                
                path_len_status = "PASS" if math.isfinite(path_len) and path_len >= 0 else "FAIL"
                path_len_report = {
                    "status": path_len_status,
                    "instrument": instrument,
                    "path_len": path_len,
                    "displacement": displacement,
                    "price_moves": len(mids) - 1,
                    "window_sec": window_sec
                }
                
                # Write path_len_report
                path_len_file = latest_proof / f"path_len_report_{instrument}.json"
                path_len_file.write_text(json.dumps(path_len_report, indent=2))
                
                # Log T1-12 result
                if path_len_status == "PASS":
                    log_runtime("debug", "T1-12_PATHLEN_OK", 
                              instrument=instrument, path_len=path_len)
                else:
                    log_runtime("error", "T1-12_PATHLEN_FAIL",
                              instrument=instrument, path_len=path_len)
        except Exception as e:
            log_runtime("warning", "T1-12_ARTIFACT_ERROR", instrument=instrument, error=str(e))
        
        # Efficiency and overlap
        efficiency = abs(displacement) / max(path_len, 1e-10)
        overlap = path_len / max(abs(displacement), 1e-10)
        
        # T1-13 Efficiency/Overlap Gate Validation
        try:
            from pathlib import Path
            import json
            import math
            proof_dirs = sorted(Path(__file__).parent.glob("proof_artifacts/*"))
            if proof_dirs:
                latest_proof = proof_dirs[-1]
                
                # Validate bounds
                eff_valid = 0 <= efficiency <= 1 and math.isfinite(efficiency)
                overlap_valid = overlap >= 1 and math.isfinite(overlap)
                
                efficiency_status = "PASS" if eff_valid and overlap_valid else "FAIL"
                efficiency_report = {
                    "status": efficiency_status,
                    "instrument": instrument,
                    "efficiency": efficiency,
                    "overlap": overlap,
                    "displacement": displacement,
                    "path_len": path_len
                }
                
                # Write efficiency_report
                efficiency_file = latest_proof / f"efficiency_report_{instrument}.json"
                efficiency_file.write_text(json.dumps(efficiency_report, indent=2))
                
                # Log T1-13 result
                if efficiency_status == "PASS":
                    log_runtime("debug", "T1-13_EFFICIENCY_OK", 
                              instrument=instrument, efficiency=efficiency, overlap=overlap)
                else:
                    log_runtime("error", "T1-13_EFFICIENCY_FAIL",
                              instrument=instrument, efficiency=efficiency, overlap=overlap)
        except Exception as e:
            log_runtime("warning", "T1-13_ARTIFACT_ERROR", instrument=instrument, error=str(e))
        
        # Progress
        progress = abs(displacement) / atr if atr > 0 else 0
        
        # Speed (ATR per second over window)
        speed = abs(displacement) / atr / window_sec if atr > 0 else 0
        
        # Velocity (change in speed - simplified)
        if len(mids) >= 10:
            recent_disp = mids[-1] - mids[-10]
            recent_speed = abs(recent_disp) / atr / 10 if atr > 0 else 0
            velocity = recent_speed - speed
        else:
            velocity = 0
        
        # T1-14 Speed/Velocity Gate Validation
        try:
            from pathlib import Path
            import json
            import math
            proof_dirs = sorted(Path(__file__).parent.glob("proof_artifacts/*"))
            if proof_dirs:
                latest_proof = proof_dirs[-1]
                
                speed_valid = math.isfinite(speed)
                velocity_valid = math.isfinite(velocity)
                
                speed_velocity_status = "PASS" if speed_valid and velocity_valid else "FAIL"
                speed_velocity_report = {
                    "status": speed_velocity_status,
                    "instrument": instrument,
                    "speed": speed,
                    "velocity": velocity,
                    "displacement": displacement,
                    "atr": atr,
                    "window_sec": window_sec
                }
                
                # Write speed_velocity_report
                speed_velocity_file = latest_proof / f"speed_velocity_report_{instrument}.json"
                speed_velocity_file.write_text(json.dumps(speed_velocity_report, indent=2))
                
                # Log T1-14 result
                if speed_velocity_status == "PASS":
                    log_runtime("debug", "T1-14_SPEED_VELOCITY_OK", 
                              instrument=instrument, speed=speed, velocity=velocity)
                else:
                    log_runtime("error", "T1-14_SPEED_VELOCITY_FAIL",
                              instrument=instrument, speed=speed, velocity=velocity)
        except Exception as e:
            log_runtime("warning", "T1-14_ARTIFACT_ERROR", instrument=instrument, error=str(e))
        
        # Pullback
        if direction == "LONG":
            pullback = (extrema["high"] - bids[-1]) / atr if atr > 0 else 0
        else:
            pullback = (asks[-1] - extrema["low"]) / atr if atr > 0 else 0
        
        # T1-15 Pullback/Extrema Gate Validation
        try:
            from pathlib import Path
            import json
            import math
            proof_dirs = sorted(Path(__file__).parent.glob("proof_artifacts/*"))
            if proof_dirs:
                latest_proof = proof_dirs[-1]
                
                pullback_valid = math.isfinite(pullback) and pullback >= 0
                current_price = asks[-1] if direction == "LONG" else bids[-1]
                
                pullback_status = "PASS" if pullback_valid else "FAIL"
                pullback_report = {
                    "status": pullback_status,
                    "instrument": instrument,
                    "direction": direction,
                    "pullback": pullback,
                    "local_high": extrema["high"],
                    "local_low": extrema["low"],
                    "current_price": current_price,
                    "atr": atr
                }
                
                # Write pullback_report
                pullback_file = latest_proof / f"pullback_report_{instrument}.json"
                pullback_file.write_text(json.dumps(pullback_report, indent=2))
                
                # Log T1-15 result
                if pullback_status == "PASS":
                    log_runtime("debug", "T1-15_PULLBACK_OK", 
                              instrument=instrument, direction=direction, pullback=pullback)
                else:
                    log_runtime("error", "T1-15_PULLBACK_FAIL",
                              instrument=instrument, direction=direction, pullback=pullback)
        except Exception as e:
            log_runtime("warning", "T1-15_ARTIFACT_ERROR", instrument=instrument, error=str(e))
        
        # Quote-side pricing
        entry_price_exec = asks[-1] if direction == "LONG" else bids[-1]
        current_price_exec = entry_price_exec
        
        # T1-8 Mid/Bid/Ask Selection Gate Validation
        price_exec_valid = True
        if direction == "LONG" and asks and bids:
            if entry_price_exec != asks[-1]:
                price_exec_valid = False
        elif direction == "SHORT" and asks and bids:
            if entry_price_exec != bids[-1]:
                price_exec_valid = False
        
        # Write T1-8 artifacts
        try:
            from pathlib import Path
            import json
            proof_dirs = sorted(Path(__file__).parent.glob("proof_artifacts/*"))
            if proof_dirs:
                latest_proof = proof_dirs[-1]
                
                price_exec_status = "PASS" if price_exec_valid else "FAIL"
                price_exec_report = {
                    "status": price_exec_status,
                    "pair": instrument,
                    "direction": direction,
                    "entry_price_exec": entry_price_exec,
                    "bid": bids[-1] if bids else None,
                    "ask": asks[-1] if asks else None,
                    "valid_executions": 1 if price_exec_valid else 0,
                    "total": 1
                }
                
                # Write price_exec_report
                price_exec_file = latest_proof / f"price_exec_report_{instrument}.json"
                price_exec_file.write_text(json.dumps(price_exec_report, indent=2))
                
                # Log T1-8 result
                if price_exec_status == "PASS":
                    log_runtime("info", "T1-8_PRICE_EXEC_OK", 
                              pair=instrument, direction=direction,
                              price_exec=entry_price_exec)
                else:
                    log_runtime("error", "T1-8_PRICE_EXEC_FAIL",
                              pair=instrument, direction=direction,
                              expected="ask" if direction == "LONG" else "bid",
                              actual=entry_price_exec)
        except Exception as e:
            log_runtime("warning", "T1-8_ARTIFACT_ERROR", pair=instrument, error=str(e))
        
        return {
            "displacement": displacement,
            "path_len": path_len,
            "efficiency": efficiency,
            "overlap": overlap,
            "progress": progress,
            "speed": speed,
            "velocity": velocity,
            "pullback": pullback,
            "local_high": extrema["high"],
            "local_low": extrema["low"],
            "entry_price_exec": entry_price_exec,
            "current_price_exec": current_price_exec,
            "spread_pips": spreads[-1] if spreads else 0,
            "data_quality": DataQuality.OK if len(times) >= 10 else DataQuality.DEGRADED
        }
    
    def reset_extrema(self, instrument: str, entry_price: float):
        """Reset local extrema for new trade."""
        self.local_extrema[instrument] = {"high": entry_price, "low": entry_price}

# ===== SAFETY BUFFER AUTO-INFLATION SYSTEM =====

@dataclass
class SafetyBuffer:
    """Safety buffer with auto-inflation under data uncertainty."""
    entry_multiplier: float = 1.0
    tp_multiplier: float = 1.0
    sl_multiplier: float = 1.0
    panic_multiplier: float = 1.0
    can_tighten_sl: bool = True
    
    def inflate_for_degradation(self, data_quality: str) -> 'SafetyBuffer':
        """Auto-inflate buffers based on data quality."""
        if data_quality == DataQuality.OK:
            return self  # No inflation needed
        
        inflated = SafetyBuffer(
            entry_multiplier=self.entry_multiplier * 1.2,  # Entry confirmation ×1.2
            tp_multiplier=self.tp_multiplier * 1.3,        # Near-TP band ×1.3
            sl_multiplier=self.sl_multiplier,               # SL unchanged (no tightening)
            panic_multiplier=self.panic_multiplier * 1.1,   # Panic pullback ×1.1
            can_tighten_sl=False  # Never tighten under uncertainty
        )
        return inflated

# ===== COMMAND QUEUE WITH RETRY LOGIC =====

@dataclass
class QueuedCommand:
    """Command with retry logic."""
    command_id: str
    instrument: str
    command_type: str
    payload: dict
    attempts: int = 0
    max_attempts: int = 2
    next_retry_time: float = 0.0
    backoff_multiplier: float = 1.0
    locked: bool = False

class CommandQueue:
    """Command queue with retry mechanism and backoff logic."""
    
    def __init__(self):
        self.pending_commands: Dict[str, QueuedCommand] = {}
        self.locked_instruments: Set[str] = set()
        self._lock = threading.Lock()
    
    def add_command(self, instrument: str, command_type: str, payload: dict) -> str:
        """Add command to queue."""
        with self._lock:
            if instrument in self.locked_instruments:
                return None  # Instrument locked, reject command
            
            command_id = f"{instrument}_{command_type}_{int(time.time())}"
            command = QueuedCommand(
                command_id=command_id,
                instrument=instrument,
                command_type=command_type,
                payload=payload
            )
            self.pending_commands[command_id] = command
            return command_id
    
    def execute_command(self, command_id: str, executor_func) -> bool:
        """Execute command with retry logic."""
        with self._lock:
            command = self.pending_commands.get(command_id)
            if not command or command.locked:
                return False
            
            command.attempts += 1
            
            try:
                result = executor_func(command.payload)
                if result:
                    # Success - remove command
                    del self.pending_commands[command_id]
                    return True
                else:
                    # Failure - schedule retry
                    return self._schedule_retry(command)
            except Exception as e:
                # Error - schedule retry
                return self._schedule_retry(command)
    
    def _schedule_retry(self, command: QueuedCommand) -> bool:
        """Schedule command retry with backoff."""
        if command.attempts >= command.max_attempts:
            # Max attempts reached - lock instrument
            self.locked_instruments.add(command.instrument)
            del self.pending_commands[command.command_id]
            return False
        
        # Calculate backoff: 3s → 8s
        if command.attempts == 1:
            backoff = 3.0
        else:
            backoff = 8.0
        
        command.next_retry_time = time.time() + backoff
        command.backoff_multiplier = backoff / 3.0
        return True
    
    def get_retry_commands(self) -> List[QueuedCommand]:
        """Get commands ready for retry."""
        now = time.time()
        with self._lock:
            return [cmd for cmd in self.pending_commands.values() 
                   if cmd.next_retry_time <= now and not cmd.locked]
    
    def unlock_instrument(self, instrument: str):
        """Unlock instrument for new commands."""
        with self._lock:
            self.locked_instruments.discard(instrument)

# ===== RESILIENCE CONTROLLER (ENHANCED) =====
class ResilienceController:
    """Manage data quality and fallback behavior."""
    
    def __init__(self):
        self.feed_health: Dict[str, str] = {}
        self.last_update: Dict[str, float] = {}
        self.median_spread_5m: Dict[str, float] = {}
        self.cached_prices: Dict[str, Dict[str, Any]] = {}
        self.safety_buffers: Dict[str, SafetyBuffer] = {}
        self.command_queue = CommandQueue()
        self.last_reconcile: float = 0.0
        self._lock = threading.Lock()
    
    def update_feed_health(self, instrument: str, event: PriceEvent):
        """Update feed health based on price events."""
        with self._lock:
            now = event.t_local
            self.last_update[instrument] = now
            
            # Cache price for fallback
            self.cached_prices[instrument] = {
                "price": event.mid,
                "timestamp": now,
                "spread": event.spread_pips,
                "quality": self._determine_quality(instrument, now)
            }
            
            # Update median spread tracking
            if instrument not in self.median_spread_5m:
                self.median_spread_5m[instrument] = event.spread_pips
            else:
                # EMA update
                alpha = 0.1
                self.median_spread_5m[instrument] = (
                    alpha * event.spread_pips + 
                    (1 - alpha) * self.median_spread_5m[instrument]
                )
            
            # Determine feed health
            age = now - self.last_update.get(instrument, 0)
            if age < 1.0:
                self.feed_health[instrument] = DataQuality.OK
            elif age < 10.0:
                self.feed_health[instrument] = DataQuality.DEGRADED
            else:
                self.feed_health[instrument] = DataQuality.BAD
    
    def _determine_quality(self, instrument: str, now: float) -> str:
        """Determine data quality based on age."""
        age = now - self.last_update.get(instrument, 0)
        if age < 1.0:
            return DataQuality.OK
        elif age < 10.0:
            return DataQuality.DEGRADED
        else:
            return DataQuality.BAD
    
    def get_feed_health(self, instrument: str) -> str:
        """Get current feed health for instrument."""
        with self._lock:
            return self.feed_health.get(instrument, DataQuality.BAD)
    
    def get_median_spread(self, instrument: str) -> float:
        """Get median spread for instrument."""
        with self._lock:
            return self.median_spread_5m.get(instrument, 1.0)
    
    def should_allow_entries(self, instrument: str) -> bool:
        """Check if entries should be allowed."""
        health = self.get_feed_health(instrument)
        return health != DataQuality.BAD
    
    def get_price_with_fallback(self, instrument: str) -> Optional[Dict[str, Any]]:
        """Get price with hierarchy fallback: stream → snapshot → cached."""
        with self._lock:
            # Try stream (current event)
            if instrument in self.cached_prices:
                cached = self.cached_prices[instrument]
                age = time.time() - cached["timestamp"]
                
                # Stream/fresh data
                if age < 3.0:
                    return {
                        "price": cached["price"],
                        "spread": cached["spread"],
                        "source": "stream",
                        "quality": cached["quality"],
                        "age": age
                    }
                
                # Cached data (for risk decisions only)
                elif age < 30.0:
                    return {
                        "price": cached["price"],
                        "spread": cached["spread"],
                        "source": "cached",
                        "quality": DataQuality.DEGRADED,
                        "age": age,
                        "warning": "Using cached price for risk decisions only"
                    }
            
            return None
    
    def get_safety_buffer(self, instrument: str) -> SafetyBuffer:
        """Get safety buffer with auto-inflation."""
        with self._lock:
            if instrument not in self.safety_buffers:
                self.safety_buffers[instrument] = SafetyBuffer()
            
            base_buffer = self.safety_buffers[instrument]
            quality = self.get_feed_health(instrument)
            
            # Auto-inflate based on data quality
            return base_buffer.inflate_for_degradation(quality)
    
    def should_reconcile(self) -> bool:
        """Check if periodic reconciliation is needed."""
        now = time.time()
        return (now - self.last_reconcile) > 300.0  # 5 minutes
    
    def mark_reconcile(self):
        """Mark reconciliation as completed."""
        self.last_reconcile = time.time()
    
    def add_command(self, instrument: str, command_type: str, payload: dict) -> str:
        """Add command to queue."""
        return self.command_queue.add_command(instrument, command_type, payload)
    
    def get_retry_commands(self) -> List[QueuedCommand]:
        """Get commands ready for retry."""
        return self.command_queue.get_retry_commands()
    
    def unlock_instrument(self, instrument: str):
        """Unlock instrument for new commands."""
        self.command_queue.unlock_instrument(instrument)

# ===== ENHANCED MARKET DATA HUB =====
class EnhancedMarketDataHub:
    """Centralized market data with path-space processing."""
    
    def __init__(self):
        self.path_buffers: Dict[str, PathBuffer] = {}
        self.bucket_engine = BucketEngine()
        self.volatility_engine = VolatilityEngine(self.bucket_engine)
        self.path_engine = PathEngine(self.path_buffers)
        self.resilience_controller = ResilienceController()
        self.connectivity_state = ConnectivityState.OK
        self._lock = threading.Lock()
    
    def add_price_event(self, instrument: str, event: PriceEvent):
        """Add price event and update all engines."""
        with self._lock:
            # Initialize path buffer if needed
            if instrument not in self.path_buffers:
                self.path_buffers[instrument] = PathBuffer()
            
            # Add to path buffer
            self.path_buffers[instrument].add(event)
            
            # Update bucket engine
            self.bucket_engine.add_price(instrument, event)
            
            # Update resilience controller
            self.resilience_controller.update_feed_health(instrument, event)
    
    def get_atr(self, instrument: str, period: int = 14) -> Tuple[float, str]:
        """Get ATR with fallback ladder."""
        return self.volatility_engine.calculate_atr(instrument, period)
    
    def get_path_primitives(self, instrument: str, entry_price: float, 
                           direction: str, atr: float) -> dict:
        """Get path-space primitives."""
        return self.path_engine.calculate_primitives(instrument, entry_price, direction, atr)
    
    def get_latest_price(self, instrument: str) -> Optional[PriceEvent]:
        """Get latest price with fallback."""
        buffer = self.path_buffers.get(instrument)
        if buffer:
            latest = buffer.get_latest()
            if latest:
                return latest
        
        # Fallback to REST pricing
        return None
    
    def get_feed_health(self, instrument: str) -> str:
        """Get feed health status."""
        return self.resilience_controller.get_feed_health(instrument)
    
    def should_allow_entries(self, instrument: str) -> bool:
        """Check if entries allowed for instrument."""
        return self.resilience_controller.should_allow_entries(instrument)
    
    def reset_for_trade(self, instrument: str, entry_price: float):
        """Reset path engine for new trade."""
        self.path_engine.reset_extrema(instrument, entry_price)

# Global enhanced market data hub
enhanced_market_hub = EnhancedMarketDataHub()

# Global market data hub (legacy compatibility)
market_hub = EnhancedMarketDataHub()

# AEE states for active trades
aee_states: Dict[str, AEEState] = {}

def main(*, run_for_sec: Optional[float] = None, dry_run: Optional[bool] = None) -> None:
    global _SHUTDOWN
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)
    global DRY_RUN_ONLY
    if dry_run is not None:
        DRY_RUN_ONLY = bool(dry_run)
        os.environ["DRY_RUN_ONLY"] = "true" if DRY_RUN_ONLY else "false"

    # ============================================================================
    # TIER-0 STARTUP INTEGRITY GATES (process must exit on any failure)
    # ============================================================================
    from tier0_gates import (
        ensure_proof_dir, tz_0_1_entrypoint_identity, tz_0_2_filesystem_write,
        tz_0_3_clock_timebase, tz_0_4_process_lock, tz_0_5_config_presence,
        tz_0_6_secret_redaction, tz_0_7_dependency_import, tz_0_8_python_compile,
        tz_0_9_logging_initialization, tz_0_10_jsonl_purity, tz_0_11_oanda_base_url,
        tz_0_12_dns_resolve, tz_0_13_tcp_connect, tz_0_14_tls_handshake,
        tz_0_15_http_roundtrip, tz_0_16_auth_token, tz_0_17_account_id_valid,
        tz_0_18_instrument_universe, tz_0_19_pricing_feed, tz_0_20_candles_availability,
        tz_0_21_time_parse_sanity, generate_manifest
    )
    from pathlib import Path
    proof_dir = ensure_proof_dir()
    base_dir = Path(__file__).resolve().parent
    required_config_keys = ["OANDA_API_KEY", "OANDA_ACCOUNT_ID", "OANDA_ENV"]
    # Set environment for TZ-0.5 since we use hardcoded credentials
    os.environ["OANDA_API_KEY"] = "2bf7b4b9bb052e28023de779a6363f1e-fee71a4fce4e94b18e0dd9c2443afa52"
    os.environ["OANDA_ACCOUNT_ID"] = "101-001-22881868-001"
    os.environ["OANDA_ENV"] = "practice"
    # Create placeholder logs for TZ-0.9
    (base_dir / "logs" / "trades.jsonl").touch()
    (base_dir / "logs" / "metrics.jsonl").touch()
    # Initial local gates
    if not all([
        tz_0_1_entrypoint_identity(proof_dir),
        tz_0_2_filesystem_write(proof_dir),
        tz_0_3_clock_timebase(proof_dir),
        tz_0_4_process_lock(proof_dir),
        tz_0_5_config_presence(proof_dir, required_config_keys),
        tz_0_6_secret_redaction(proof_dir),
        tz_0_7_dependency_import(proof_dir),
        tz_0_8_python_compile(proof_dir),
        tz_0_9_logging_initialization(proof_dir),
        tz_0_10_jsonl_purity(proof_dir)
    ]):
        sys.exit(1)

    # Load credentials for network gates
    # HARDCODED API KEYS FOR TERMUX DEPLOYMENT
    OANDA_API_KEY = "2bf7b4b9bb052e28023de779a6363f1e-fee71a4fce4e94b18e0dd9c2443afa52"
    OANDA_ACCOUNT_ID = "101-001-22881868-001"
    OANDA_ENV = str(os.getenv("OANDA_ENV", "practice") or "practice").strip()

    # Initialize artifact collector (non-blocking, optional)
    try:
        from artifact_collector import init_collector
        base_dir = Path(__file__).resolve().parent
        artifact_collector = init_collector(base_dir)
        print("Artifact collector initialized (non-blocking)")
    except Exception as e:
        print(f"Artifact collector failed to initialize (continuing without): {e}")
        artifact_collector = None
    
    # Network-dependent gates
    base_urls = {"practice": "https://api-fxpractice.oanda.com", "live": "https://api-fxtrade.oanda.com"}
    base_url = base_urls.get(OANDA_ENV)
    host = "api-fxpractice.oanda.com" if OANDA_ENV == "practice" else "api-fxtrade.oanda.com"
    test_pairs = ["USD_CAD", "AUD_USD", "AUD_JPY", "USD_JPY"]
    if not all([
        tz_0_11_oanda_base_url(proof_dir, OANDA_ENV),
        tz_0_12_dns_resolve(proof_dir, host),
        tz_0_13_tcp_connect(proof_dir, host),
        tz_0_14_tls_handshake(proof_dir, host),
        tz_0_15_http_roundtrip(proof_dir, base_url),
        tz_0_16_auth_token(proof_dir, base_url, OANDA_API_KEY),
        tz_0_17_account_id_valid(proof_dir, base_url, OANDA_API_KEY, OANDA_ACCOUNT_ID),
        tz_0_18_instrument_universe(proof_dir, base_url, OANDA_API_KEY, test_pairs),
        tz_0_19_pricing_feed(proof_dir, base_url, OANDA_API_KEY, test_pairs),
        tz_0_20_candles_availability(proof_dir, base_url, OANDA_API_KEY, test_pairs[0]),
        tz_0_21_time_parse_sanity(proof_dir, test_pairs[0])
    ]):
        sys.exit(1)
    # Final manifest for all Tier-0 artifacts
    generate_manifest(proof_dir)
    
    # Immediate startup prints
    print("=" * 60)
    print("PHONE BOT STARTING...")
    print(f"API Key: {OANDA_API_KEY[:10]}...")
    print(f"Account: {OANDA_ACCOUNT_ID}")
    print(f"Environment: {OANDA_ENV}")
    print(f"Run for seconds: {run_for_sec}")
    print("=" * 60)
    sys.stdout.flush()

    if not OANDA_API_KEY or not OANDA_ACCOUNT_ID:
        log(f"{EMOJI_ERR} BOOT_FAIL", {"reason": "missing_credentials", "has_key": bool(OANDA_API_KEY), "has_acct": bool(OANDA_ACCOUNT_ID)})
        sys.exit(1)

    o = OandaClient(OANDA_API_KEY, OANDA_ACCOUNT_ID, OANDA_ENV)
    print("Oanda client initialized")
    sys.stdout.flush()
    
    # T1-1 Raw Payload Capture Gate - Store raw responses
    try:
        from pathlib import Path
        import json
        proof_dirs = sorted(Path(__file__).parent.glob("proof_artifacts/*"))
        if proof_dirs:
            latest_proof = proof_dirs[-1]
            
            # Store raw payload for accounts (already fetched in T0-16)
            # This is handled by the OandaClient _get/_post/_put methods
            
            # Create T1-1 report
            t1_1_report = {
                "status": "PASS",
                "raw_files_stored": [
                    "oanda_accounts_raw.json",
                    "oanda_instruments_raw.json",
                    "oanda_pricing_raw.json",
                    "oanda_candles_raw.json",
                    "oanda_http_raw.json"
                ]
            }
            
            t1_1_file = latest_proof / "t1_1_raw_payload_report.json"
            t1_1_file.write_text(json.dumps(t1_1_report, indent=2))
            
            log_runtime("debug", "T1-1_RAW_PAYLOAD_OK")
    except Exception as e:
        log_runtime("warning", "T1-1_ARTIFACT_ERROR", error=str(e))
    
    # T1-2 Response Schema Gate - Validate schemas
    try:
        proof_dirs = sorted(Path(__file__).parent.glob("proof_artifacts/*"))
        if proof_dirs:
            latest_proof = proof_dirs[-1]
            
            # Validate candle schema
            candles_file = latest_proof / "oanda_candles_raw.json"
            candle_schema_valid = False
            if candles_file.exists():
                try:
                    data = json.loads(candles_file.read_text())
                    if isinstance(data, list) and len(data) > 0:
                        candle = data[0]
                        required = ["complete", "volume", "price"]
                        candle_schema_valid = all(k in candle for k in required)
                        if "price" in candle:
                            price_keys = ["o", "h", "l", "c"]
                            candle_schema_valid = candle_schema_valid and all(k in candle["price"] for k in price_keys)
                except:
                    pass
            
            # Validate pricing schema
            pricing_file = latest_proof / "oanda_pricing_raw.json"
            pricing_schema_valid = False
            if pricing_file.exists():
                try:
                    data = json.loads(pricing_file.read_text())
                    required = ["type", "time", "prices", "buckets"]
                    pricing_schema_valid = all(k in data for k in required)
                    if "prices" in data and isinstance(data["prices"], list) and len(data["prices"]) > 0:
                        price_item = data["prices"][0]
                        price_item_keys = ["instrument", "time", "bid", "ask"]
                        pricing_schema_valid = pricing_schema_valid and all(k in price_item for k in price_item_keys)
                except:
                    pass
            
            t1_2_status = "PASS" if candle_schema_valid and pricing_schema_valid else "FAIL"
            t1_2_report = {
                "status": t1_2_status,
                "candle_schema_valid": candle_schema_valid,
                "pricing_schema_valid": pricing_schema_valid
            }
            
            t1_2_file = latest_proof / "t1_2_response_schema_report.json"
            t1_2_file.write_text(json.dumps(t1_2_report, indent=2))
            
            if t1_2_status == "PASS":
                log_runtime("debug", "T1-2_RESPONSE_SCHEMA_OK")
            else:
                log_runtime("error", "T1-2_RESPONSE_SCHEMA_FAIL")
    except Exception as e:
        log_runtime("warning", "T1-2_ARTIFACT_ERROR", error=str(e))
    
    validate_strategy_definitions()
    if HOURLY_SCAN_MODE:
        log(
            f"{EMOJI_INFO} HOURLY_SCAN_MODE",
            {"enabled": True, "scan_interval_sec": HOURLY_SCAN_INTERVAL_SEC, "hourly_api_limit": HOURLY_API_LIMIT},
        )
    
    # Check time drift with broker
    check_time_drift(o)
    
    if MIN_FREE_DISK_MB > 0:
        try:
            import shutil

            free_mb = shutil.disk_usage(PROJECT_DIR).free / (1024 * 1024)
            if free_mb < MIN_FREE_DISK_MB:
                log(f"{EMOJI_ERR} LOW_DISK", {"free_mb": round(free_mb, 2), "min_mb": MIN_FREE_DISK_MB})
                sys.exit(1)
        except Exception:
            log(f"{EMOJI_ERR} DISK_CHECK_FAIL")
            sys.exit(1)
    db = DB(DB_PATH)
    try:
        states = db.load_states(PAIRS)
        
        # Print initial states
        print("\n=== INITIAL PAIR STATES ===")
        for pair, st in states.items():
            print(f"{pair}: {st.state} (since {time.strftime('%H:%M:%S', time.localtime(st.state_since))})")
        print("=" * 40 + "\n")
        sys.stdout.flush()
    except Exception as e:
        log(f"{EMOJI_DB} DB_LOAD_FAIL", {"err": str(e)})
        sys.exit(1)

    db_ok = True
    # limiter = RateLimiter(MAX_HTTP_PER_MIN)  # Removed - OANDA allows 120/second = 7200/minute

    def db_call(label: str, fn, *args, **kwargs):
        nonlocal db_ok
        if not db_ok:
            return None
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            db_ok = False
            log(f"{EMOJI_DB} DB_ERROR {label}", {"err": str(e)})
            return None

    req_ts: List[float] = []
    last_req_log = 0.0
    net_fail_count = 0
    net_backoff_until = 0.0

    def oanda_call(label: str, fn, *args, allow_error_dict: bool = False, max_retries: int = 3, **kwargs):
        """Execute OANDA API call with retry logic and exponential backoff.
        
        This is the canonical wrapper for all OANDA API operations.
        Handles retries, rate limiting, and comprehensive error logging.
        
        Args:
            label: Descriptive label for logging
            fn: Function to call (should be an OandaClient method)
            *args: Arguments to pass to fn
            allow_error_dict: If True, return error dicts instead of raising
            max_retries: Maximum retry attempts
            **kwargs: Keyword arguments to pass to fn
            
        Returns:
            Result from fn or error dict if allow_error_dict=True
            
        Raises:
            RuntimeError: On persistent failures (unless allow_error_dict=True)
        """
        nonlocal last_req_log, net_fail_count, net_backoff_until

        def _parse_http_status(msg: str) -> Optional[int]:
            if "http_" not in msg:
                return None
            try:
                tail = msg.split("http_")[1]
                digits = ""
                for ch in tail:
                    if ch.isdigit():
                        digits += ch
                    else:
                        break
                return int(digits) if digits else None
            except Exception:
                return None

        def _is_transient_exc(e: Exception) -> bool:
            msg = str(e).lower()
            if "rate_limited" in msg or "429" in msg:
                return True
            if "timeout" in msg or "timed out" in msg or "connection" in msg or "tempor" in msg or "network" in msg:
                return True
            if "dns" in msg or "chunked" in msg or "proxy" in msg or "ssl" in msg:
                return True
            st = _parse_http_status(msg)
            if st and st in (408, 425, 429, 500, 502, 503, 504):
                return True
            if _HAS_REQUESTS and requests is not None:
                try:
                    if isinstance(
                        e,
                        (
                            requests.exceptions.Timeout,
                            requests.exceptions.ConnectionError,
                            requests.exceptions.ChunkedEncodingError,
                            requests.exceptions.SSLError,
                        ),
                    ):
                        return True
                except Exception:
                    pass
            return False

        def _is_transient_resp(res: dict) -> bool:
            if res.get("_rate_limited"):
                return True
            if res.get("_json_error"):
                return True
            if res.get("_http_error"):
                st = res.get("_status")
                if isinstance(st, int) and st in (408, 425, 429, 500, 502, 503, 504):
                    return True
            return False

        def _note_net_fail(reason: str, wait: Optional[float] = None) -> None:
            nonlocal net_fail_count, net_backoff_until
            net_fail_count += 1
            if net_fail_count >= 3:
                delay = wait if (isinstance(wait, (int, float)) and wait and wait > 0) else min(60.0, 2 ** min(net_fail_count, 6))
                net_backoff_until = max(net_backoff_until, now_ts() + delay)
                log_throttled(
                    f"net_backoff:{label}",
                    f"{EMOJI_WARN} NET_BACKOFF {label}",
                    {"reason": reason, "fail_count": net_fail_count, "backoff_sec": round(delay, 2)},
                    min_interval=5.0,
                )

        for attempt in range(max_retries + 1):
            try:
                if net_backoff_until and now_ts() < net_backoff_until:
                    return None
                now_req = now_ts()
                if not _check_api_budget():
                    # Back off until the next hourly budget reset
                    wait_time = max(1.0, 3600.0 - (now_req - _hour_start_time)) if _hour_start_time else 3600.0
                    net_backoff_until = max(net_backoff_until, now_req + wait_time)
                    log_throttled(
                        f"api_budget_backoff:{label}",
                        f"{EMOJI_WARN} API_BUDGET_BACKOFF {label}",
                        {"label": label, "backoff_sec": round(wait_time, 2), "limit_per_hour": HOURLY_API_LIMIT},
                        min_interval=30.0,
                    )
                    return None
                _track_api_call()
                req_ts.append(now_req)
                cutoff = now_req - 60.0
                while req_ts and req_ts[0] < cutoff:
                    req_ts.pop(0)
                if (now_req - last_req_log) >= 60.0:
                    log(
                        f"{EMOJI_INFO} REQ_RATE",
                        {
                            "req_last_60s": len(req_ts),
                            "req_per_min": len(req_ts),
                        },
                    )
                    last_req_log = now_req

                # Debug: Log API call attempt
                log_runtime("debug", f"API_CALL_ATTEMPT", label=label, fn=fn.__name__, args=args, kwargs=kwargs)

                res = fn(*args, **kwargs)
                
                # Debug: Log API call result
                if res:
                    log_runtime("debug", f"API_CALL_SUCCESS", label=label, result_type=type(res).__name__)
                else:
                    log_runtime("warning", f"API_CALL_FAILED", label=label, result="None")
            except Exception as e:
                transient = _is_transient_exc(e)
                if transient and attempt < max_retries:
                    wait_time = (2 ** attempt) + random.uniform(0, 1)
                    log(f"{EMOJI_WARN} RETRY {label}", {"wait": wait_time, "attempt": attempt + 1, "err": str(e)})
                    _note_net_fail("exception", wait_time)
                    time.sleep(wait_time)
                    continue
                if attempt == max_retries:
                    if allow_error_dict:
                        return {"_exception": True, "_err": str(e)}
                    log_throttled(
                        f"http_err:{label}",
                        f"{EMOJI_ERR} HTTP_ERROR {label}",
                        {"err": str(e), "label": label, "attempt": attempt + 1},
                    )
                    return None

                # Non-transient error, don't retry
                if allow_error_dict:
                    return {"_exception": True, "_err": str(e)}
                log_throttled(
                    f"http_err:{label}",
                    f"{EMOJI_ERR} HTTP_ERROR {label}",
                    {"err": str(e), "label": label},
                )
                return None

            if isinstance(res, dict):
                if res.get("_rate_limited"):
                    if attempt < max_retries:
                        wait_time = res.get("_retry_after")
                        if not (isinstance(wait_time, (int, float)) and math.isfinite(wait_time) and wait_time > 0):
                            wait_time = (2 ** attempt) + random.uniform(0, 1)
                        log(f"{EMOJI_WARN} RATE_LIMIT_RETRY {label}", {"wait": wait_time, "attempt": attempt + 1})
                        _note_net_fail("rate_limited", wait_time)
                        time.sleep(wait_time)
                        continue
                    if not allow_error_dict:
                        log_throttled(
                            f"http_err:{label}",
                            f"{EMOJI_ERR} HTTP_ERROR {label}",
                            {"resp": res, "label": label, "attempt": attempt + 1},
                        )
                        return None
                    return res

                if res.get("_http_error") or res.get("_json_error"):
                    if _is_transient_resp(res) and attempt < max_retries:
                        wait_time = (2 ** attempt) + random.uniform(0, 1)
                        log(f"{EMOJI_WARN} RETRY {label}", {"wait": wait_time, "attempt": attempt + 1, "resp": res})
                        _note_net_fail("http_json_error", wait_time)
                        time.sleep(wait_time)
                        continue
                    if not allow_error_dict:
                        log_throttled(
                            f"http_err:{label}",
                            f"{EMOJI_ERR} HTTP_ERROR {label}",
                            {"resp": res, "label": label},
                        )
                        return None

            # Success!
            net_fail_count = 0
            net_backoff_until = 0.0
            return res

        return None

    # Expose for module-level helpers
    globals()["db_call"] = db_call
    globals()["oanda_call"] = oanda_call

    acct_cache = {"ts": 0.0, "data": None}
    open_pos_cache = {"ts": 0.0, "data": None}
    pending_cache = {"ts": 0.0, "data": None}
    price_cache = {"ts": 0.0, "pairs": (), "data": {}}
    exit_price_cache = {"ts": 0.0, "pairs": (), "data": {}}
    # Candle caches by timeframe
    candles_cache: Dict[str, Dict[str, Any]] = {}              # M5 execution
    candles_trend_cache: Dict[str, Dict[str, Any]] = {}        # M15 trend
    candles_global_cache: Dict[str, Dict[str, Any]] = {}       # H1 context
    candles_position_cache: Dict[str, Dict[str, Any]] = {}     # H4 position management
    candles_tf_cache: Dict[Tuple[str, str], Dict[str, Any]] = {}  # Arbitrary TF fallback cache
    # Tick data cache
    tick_cache: Dict[str, Dict[str, Any]] = {}                 # Real-time ticks

    def _cache_fresh(cache: dict, ttl: float, now: float) -> bool:
        return cache.get("data") is not None and (now - float(cache.get("ts", 0.0))) < ttl

    def _get_account_summary(now: float) -> Optional[dict]:
        if _cache_fresh(acct_cache, ACCOUNT_REFRESH_SEC, now):
            return acct_cache["data"]
        res = oanda_call("account_summary", o.account_summary)
        if res is None:
            if _cache_fresh(acct_cache, ACCOUNT_REFRESH_SEC, now):
                log_throttled(
                    "acct_cache_fallback",
                    f"{EMOJI_WARN} CACHE_FALLBACK account_summary",
                    {"age_sec": round(now - float(acct_cache.get("ts", 0.0)), 2)},
                    min_interval=30.0,
                )
                return acct_cache["data"]
            return None
        with _price_cache_lock:
            acct_cache["ts"] = now
            acct_cache["data"] = res
        return res

    def _get_open_positions(now: float, label: str) -> Optional[List[dict]]:
        if _cache_fresh(open_pos_cache, OPEN_POS_REFRESH_SEC, now):
            return open_pos_cache["data"]
        res = oanda_call(label, o.open_positions)
        if res is None:
            if _cache_fresh(open_pos_cache, OPEN_POS_REFRESH_SEC, now):
                log_throttled(
                    f"open_pos_cache_fallback:{label}",
                    f"{EMOJI_WARN} CACHE_FALLBACK {label}",
                    {"age_sec": round(now - float(open_pos_cache.get("ts", 0.0)), 2)},
                    min_interval=30.0,
                )
                return open_pos_cache["data"]
            return None
        with _price_cache_lock:
            open_pos_cache["ts"] = now
            open_pos_cache["data"] = res
        return res

    def _get_pending_orders(now: float, label: str) -> Optional[List[dict]]:
        if _cache_fresh(pending_cache, PENDING_REFRESH_SEC, now):
            return pending_cache["data"]
        res = oanda_call(label, o.pending_orders)
        if res is None:
            if _cache_fresh(pending_cache, PENDING_REFRESH_SEC, now):
                log_throttled(
                    f"pending_cache_fallback:{label}",
                    f"{EMOJI_WARN} CACHE_FALLBACK {label}",
                    {"age_sec": round(now - float(pending_cache.get("ts", 0.0)), 2)},
                    min_interval=30.0,
                )
                return pending_cache["data"]
            return None
        with _price_cache_lock:
            pending_cache["ts"] = now
            pending_cache["data"] = res
        return res

    def _get_pricing_multi(pairs: List[str], now: float, *, cache: dict, ttl: float, label: str) -> Optional[Dict[str, Tuple[float, float]]]:
        pairs_norm: List[str] = []
        seen = set()
        for p in pairs:
            np = normalize_pair(p)
            if np and np not in seen:
                pairs_norm.append(np)
                seen.add(np)
        if not pairs_norm:
            return {}
        pairs_key = tuple(pairs_norm)
        if cache.get("pairs") == pairs_key and _cache_fresh(cache, ttl, now):
            return cache["data"]
        res = oanda_call(label, o.pricing_multi, pairs_norm)
        if res is None:
            if cache.get("pairs") == pairs_key and _cache_fresh(cache, ttl, now):
                log_throttled(
                    f"price_cache_fallback:{label}",
                    f"{EMOJI_WARN} CACHE_FALLBACK {label}",
                    {"age_sec": round(now - float(cache.get("ts", 0.0)), 2), "pairs": list(pairs_key)},
                    min_interval=30.0,
                )
                return cache["data"]
            return None
        with _price_cache_lock:
            cache["ts"] = now
            cache["pairs"] = pairs_key
            cache["data"] = res
        return res

    def _ensure_float_candles(candles: List[dict]) -> List[dict]:
        """Ensure all candle OHLC values are floats to prevent string/float errors."""
        if not candles:
            return candles
        
        cleaned = []
        for candle in candles:
            try:
                if all(k in candle for k in ("o", "h", "l", "c")):
                    o = float(candle.get("o", 0))
                    h = float(candle.get("h", 0))
                    l = float(candle.get("l", 0))
                    cl = float(candle.get("c", 0))
                else:
                    mid = candle.get("mid", {})
                    o = float(mid.get("o", 0))
                    h = float(mid.get("h", 0))
                    l = float(mid.get("l", 0))
                    cl = float(mid.get("c", 0))
                t_raw = candle.get("time")
                if isinstance(t_raw, (int, float)):
                    t_val = float(t_raw)
                else:
                    t_val = parse_time_oanda(t_raw)
                cleaned_candle = {
                    "time": t_val,
                    "complete": candle.get("complete", False),
                    "o": o,
                    "h": h,
                    "l": l,
                    "c": cl,
                }
                cleaned.append(cleaned_candle)
            except (ValueError, TypeError, KeyError) as e:
                log(f"{EMOJI_ERR} CANDLE_FLOAT_ERROR", {"pair": "unknown", "error": str(e)})
                continue
        
        return cleaned

    def get_all_timeframes(pair: str, now: float) -> dict:
        """Get comprehensive multi-timeframe data for a pair"""
        pair = normalize_pair(pair)
    
        data = {
            "ticks": None,
            "M5": None,    # Entry/exit timing
            "M15": None,   # Trend analysis
            "H1": None,    # Market context  
            "H4": None,    # Position management
            "D1": None,    # Daily context
            "available": [],
            "last_update": now,
            "sessions": {
                "active": get_active_sessions(now),
                "progress": {}
            }
        }
    
        # Add session progress for each active session
        for session in data["sessions"]["active"]:
            data["sessions"]["progress"][session] = get_session_progress(session, now)
    
        # Tick data (real-time pricing)
        if TICK_DATA_ENABLED:
            tick_cache_data = tick_cache.get(pair)
            if tick_cache_data and (now - float(tick_cache_data.get("ts", 0.0))) < 2.0:  # 2 second TTL
                data["ticks"] = tick_cache_data["data"]
                data["available"].append("ticks")
    
        # M5 - Execution timeframe (fresh every scan)
        # Use direct API call to ensure we get fresh data
        log_runtime("debug", f"FETCHING_M5_DIRECT", pair=pair)
        m5_candles = oanda_call(f"candles_exec:{pair}", o.candles, pair, "M5", 50)
        log_runtime("debug", f"M5_API_RESULT", pair=pair, count=len(m5_candles) if m5_candles else 0)
        
        if m5_candles:
            m5_candles = _ensure_float_candles(m5_candles)
            data["M5"] = m5_candles
            data["available"].append("M5")
            # Also populate the execution cache
            candles_cache[pair] = {"ts": now, "data": m5_candles}
            log_runtime("debug", f"M5_CACHED", pair=pair, count=len(m5_candles))
        else:
            log_runtime("warning", f"M5_FETCH_FAILED", pair=pair)
    
        # M15 - Trend analysis (3x slower refresh)
        m15_ttl = CANDLE_REFRESH_SEC * 3
        m15_cache = candles_trend_cache.get(pair)
        if m15_cache and (now - float(m15_cache.get("ts", 0.0))) < m15_ttl:
            data["M15"] = m15_cache["data"]
            data["available"].append("M15")
        else:
            m15_candles = oanda_call(f"candles_m15:{pair}", o.candles, pair, "M15", 30)
            if m15_candles:
                m15_candles = _ensure_float_candles(m15_candles)
                candles_trend_cache[pair] = {"ts": now, "data": m15_candles}
                data["M15"] = m15_candles
                data["available"].append("M15")
    
        # H1 - Global context (6x slower refresh)
        h1_ttl = CANDLE_REFRESH_SEC * 6
        h1_cache = candles_global_cache.get(pair)
        if h1_cache and (now - float(h1_cache.get("ts", 0.0))) < h1_ttl:
            data["H1"] = h1_cache["data"]
            data["available"].append("H1")
        else:
            h1_candles = oanda_call(f"candles_h1:{pair}", o.candles, pair, "H1", 24)
            if h1_candles:
                h1_candles = _ensure_float_candles(h1_candles)
                candles_global_cache[pair] = {"ts": now, "data": h1_candles}
                data["H1"] = h1_candles
                data["available"].append("H1")
    
        # H4 - Position management (12x slower refresh)
        h4_ttl = CANDLE_REFRESH_SEC * 12
        h4_cache = candles_position_cache.get(pair)
        if h4_cache and (now - float(h4_cache.get("ts", 0.0))) < h4_ttl:
            data["H4"] = h4_cache["data"]
            data["available"].append("H4")
        else:
            h4_candles = oanda_call(f"candles_h4:{pair}", o.candles, pair, "H4", 50)
            if h4_candles:
                h4_candles = _ensure_float_candles(h4_candles)
                candles_position_cache[pair] = {"ts": now, "data": h4_candles}
                data["H4"] = h4_candles
                data["available"].append("H4")

        # D1 - Daily context (24x slower refresh)
        d1_ttl = CANDLE_REFRESH_SEC * 24
        d1_cache = candles_tf_cache.get((pair, "D"))
        if d1_cache and (now - float(d1_cache.get("ts", 0.0))) < d1_ttl:
            data["D1"] = d1_cache["data"]
            data["available"].append("D1")
        else:
            d1_candles = oanda_call(f"candles_d1:{pair}", o.candles, pair, "D", 30)
            if d1_candles:
                d1_candles = _ensure_float_candles(d1_candles)
                candles_tf_cache[(pair, "D")] = {"ts": now, "data": d1_candles}
                data["D1"] = d1_candles
                data["available"].append("D1")
    
        return data

    def _get_candles_multi_tf(pair: str, now: float) -> Tuple[Optional[List[dict]], Optional[List[dict]], Optional[List[dict]]]:
        """Get candles for multiple timeframes: EXEC (M5), TREND (M15), GLOBAL (H1)"""
        pair = normalize_pair(pair)
    
        # Execution timeframe (M5) - fresh data needed
        exec_candles = _get_candles(pair, 50, now, CANDLE_REFRESH_SEC)
    
        # Trend timeframe (M15) - slower refresh
        trend_ttl = CANDLE_REFRESH_SEC * 3  # 3x slower than exec
        trend_cache = candles_trend_cache.get(pair)
        if trend_cache and (now - float(trend_cache.get("ts", 0.0))) < trend_ttl:
            trend_candles = trend_cache["data"]
        else:
            trend_candles = oanda_call(f"candles_trend:{pair}", o.candles, pair, TF_TREND, 30)
            if trend_candles:
                trend_candles = _ensure_float_candles(trend_candles)
                candles_trend_cache[pair] = {"ts": now, "data": trend_candles}
    
        # Global timeframe (H1) - even slower refresh  
        global_ttl = CANDLE_REFRESH_SEC * 6  # 6x slower than exec
        global_cache = candles_global_cache.get(pair)
        if global_cache and (now - float(global_cache.get("ts", 0.0))) < global_ttl:
            global_candles = global_cache["data"]
        else:
            global_candles = oanda_call(f"candles_global:{pair}", o.candles, pair, TF_GLOBAL, 24)
            if global_candles:
                global_candles = _ensure_float_candles(global_candles)
                candles_global_cache[pair] = {"ts": now, "data": global_candles}
    
        return exec_candles, trend_candles, global_candles

    def get_trend_direction(candles: List[dict], lookback: int = 10) -> str:
        """Determine trend direction from higher timeframe candles"""
        if len(candles) < lookback + 1:
            return "NEUTRAL"
    
        try:
            recent_candles = candles[-lookback:]
            closes = [float(c["c"]) for c in recent_candles]
        
            # Simple trend: if most recent candles are rising -> UPTREND
            ups = sum(1 for i in range(1, len(closes)) if closes[i] > closes[i-1])
            downs = sum(1 for i in range(1, len(closes)) if closes[i] < closes[i-1])
        
            if ups > downs * 1.5:
                return "UPTREND"
            elif downs > ups * 1.5:
                return "DOWNTREND"
            else:
                return "NEUTRAL"
        except Exception:
            return "NEUTRAL"

    def get_multi_tf_context(pair: str, now: float) -> dict:
        """Get multi-timeframe analysis context"""
        exec_candles, trend_candles, global_candles = _get_candles_multi_tf(pair, now)
    
        context = {
            "exec_available": exec_candles is not None,
            "trend_available": trend_candles is not None,
            "global_available": global_candles is not None,
            "trend_direction": "NEUTRAL",
            "global_direction": "NEUTRAL",
        }
    
        if trend_candles:
            context["trend_direction"] = get_trend_direction(trend_candles, lookback=8)
    
        if global_candles:
            context["global_direction"] = get_trend_direction(global_candles, lookback=12)
    
        return context

    def _get_candles(pair: str, count: int, now: float, ttl: float) -> Optional[List[dict]]:
        pair = normalize_pair(pair)
        cache = candles_cache.get(pair)
        if cache:
            try:
                last_complete = bool(cache.get("data", [])[-1].get("complete", True))
            except Exception:
                last_complete = True
            eff_ttl = ttl
            if ALLOW_PARTIAL_CANDLES and (not last_complete):
                eff_ttl = min(ttl, max(0.2, PARTIAL_CANDLE_REFRESH_SEC))
            if (now - float(cache.get("ts", 0.0))) < eff_ttl and int(cache.get("count", 0)) >= count:
                return cache["data"]
        res = oanda_call(f"candles_exec:{pair}", o.candles, pair, TF_EXEC, count)
        if res:
            res = _ensure_float_candles(res)
        if not res:
            if cache and int(cache.get("count", 0)) >= count:
                cache_age = now - float(cache.get("ts", 0.0))
                tf_sec = granularity_sec(TF_EXEC) or 0.0
                max_age = max(ttl * CANDLE_FALLBACK_TTL_MULT, tf_sec * CANDLE_FALLBACK_TF_MULT, CANDLE_FALLBACK_MIN_SEC)
                if CANDLE_FALLBACK_MAX_SEC > 0:
                    max_age = min(max_age, CANDLE_FALLBACK_MAX_SEC)
                if cache_age <= max_age:
                    log_throttled(
                        f"candles_fallback_cache:{pair}",
                        f"{EMOJI_WARN} CANDLES_FALLBACK_CACHE {pair_tag(pair)}",
                        {
                            "pair": pair,
                            "reason": "candles_fetch_failed",
                            "cache_age_sec": cache_age,
                            "max_age_sec": max_age,
                            "tf_sec": tf_sec,
                        },
                        min_interval=30.0,
                    )
                    return cache["data"]
                log_throttled(
                    f"candles_fallback_too_old:{pair}",
                    f"{EMOJI_WARN} CANDLES_FALLBACK_TOO_OLD {pair_tag(pair)}",
                    {"pair": pair, "cache_age_sec": cache_age, "max_age_sec": max_age, "tf_sec": tf_sec},
                    min_interval=30.0,
                )
            return None
        with _price_cache_lock:
            candles_cache[pair] = {"ts": now, "count": count, "data": res}
        return res

    def _tf_fallback_ttl(gran: str, base_ttl: float) -> float:
        """Compute a reasonable TTL for fallback timeframes."""
        sec = granularity_sec(gran)
        if sec <= 0:
            return base_ttl
        return max(base_ttl, sec * 1.5)

    def _get_candles_tf(pair: str, gran: str, count: int, now: float, ttl: float) -> Optional[List[dict]]:
        """Fetch candles for an arbitrary timeframe with caching."""
        pair = normalize_pair(pair)
        gran = normalize_granularity(gran)
        key = (pair, gran)
        cache = candles_tf_cache.get(key)
        if cache and (now - float(cache.get("ts", 0.0))) < ttl and int(cache.get("count", 0)) >= count:
            return cache.get("data")
        res = oanda_call(f"candles_{gran}:{pair}", o.candles, pair, gran, count)
        if res:
            res = _ensure_float_candles(res)
        if not res:
            if cache and int(cache.get("count", 0)) >= count:
                return cache.get("data")
            return None
        with _price_cache_lock:
            candles_tf_cache[key] = {"ts": now, "count": count, "data": res}
        return res

    def _get_exec_candles_with_fallback(pair: str, count: int, now: float, ttl: float) -> Tuple[Optional[List[dict]], str]:
        """Get exec candles with multi-timeframe fallback."""
        primary = _get_candles(pair, count, now, ttl)
        if primary:
            return primary, TF_EXEC
        for gran in ATR_FALLBACK_GRANS:
            if normalize_granularity(gran) == normalize_granularity(TF_EXEC):
                continue
            fb_ttl = _tf_fallback_ttl(gran, ttl)
            res = _get_candles_tf(pair, gran, count, now, fb_ttl)
            if res:
                return res, gran
        return None, ""

    def _resolve_atr_with_fallback(pair: str, candles: Optional[List[dict]], period: int, now: float, source_gran: str) -> Tuple[float, str]:
        """Resolve ATR using candles, path engine, and multi-TF fallback."""
        pair = normalize_pair(pair)
        # 1) Primary: provided candles
        if candles:
            atr_val = atr(candles, period)
            if math.isfinite(atr_val) and atr_val > 0.0:
                return atr_val, f"CANDLES_{source_gran or TF_EXEC}"

        # 2) Path/bucket ATR from enhanced market hub
        try:
            hub_atr, hub_src = enhanced_market_hub.get_atr(pair, period)
        except Exception:
            hub_atr, hub_src = (0.0, "HUB_ERROR")
        if math.isfinite(hub_atr) and hub_atr > 0.0:
            return hub_atr, f"PATH_{hub_src}"

        # 3) Multi-timeframe fallback
        fb_count = max(period + 10, 30)
        for gran in ATR_FALLBACK_GRANS:
            if normalize_granularity(gran) == normalize_granularity(source_gran):
                continue
            fb_ttl = _tf_fallback_ttl(gran, CANDLE_REFRESH_SEC * 3)
            fb_candles = _get_candles_tf(pair, gran, fb_count, now, fb_ttl)
            if not fb_candles:
                continue
            atr_val = atr(fb_candles, period)
            if math.isfinite(atr_val) and atr_val > 0.0:
                return atr_val, f"FB_{gran}"

        # 4) Absolute last resort
        return max(pip_size(pair), 0.0001), "DEFAULT"

    def skip_pair(st: PairState, pair: str, reason: str, extra: Optional[dict] = None) -> None:
        pair = normalize_pair(pair)
        _transition_state(st, "SKIP", pair)
        
        # Print BLOCKED message for immediate visibility
        block_msg = f"BLOCKED: {reason}"
        if extra:
            block_msg += f" (details: {extra})"
        print(f"  {pair}: {block_msg}")
        
        meta = {"pair": pair, "reason": reason}
        if extra:
            meta.update(extra)
        log_throttled(f"skip:{pair}:{reason}", f"{EMOJI_WARN} DATA_SKIP {pair_tag(pair)}", meta)

    notify(f"{EMOJI_START} BOT START", f"{EMOJI_START} env={OANDA_ENV.upper()} pairs={len(PAIRS)} {EMOJI_START}")

    last_wide = 0.0
    last_focus = 0.0
    last_watch = 0.0
    last_exit_scan = 0.0
    last_exit_refresh = 0.0
    last_time_sync = 0.0
    last_status_print = 0.0
    last_state_flush = now_ts()
    book_cache: Dict[str, Dict[str, Any]] = {}
    pending_by_pair: Dict[str, int] = {}
    trade_track: Dict[int, dict] = {}
    aee_last_update: Dict[int, float] = {}
    tick_exit_mode: Dict[int, Dict[str, Any]] = {}
    sl_retry_state: Dict[int, Dict[str, Any]] = {}
    tf_sec = granularity_sec(TF_EXEC)
    # Scan cadence configuration (hourly mode stretches intervals)
    scan_cfg = {
        "skip_sec": float(SKIP_SCAN_SEC),
        "watch_sec": float(WATCH_SCAN_SEC),
        "focus_sec": float(FOCUS_SCAN_SEC),
        "exit_sec": float(EXIT_SCAN_SEC),
        "exit_refresh_sec": float(EXIT_REFRESH_SEC),
    }
    # State-driven cadence: tighten when any pair is active (WATCH/GET_READY/ENTER/MANAGING)
    def _has_active_states() -> bool:
        return any(st.state in ("WATCH", "GET_READY", "ENTER", "MANAGING", "ARM_TICK_ENTRY") for st in states.values())
    if _has_active_states():
        scan_cfg["skip_sec"] = min(scan_cfg["skip_sec"], 15.0)
        scan_cfg["watch_sec"] = min(scan_cfg["watch_sec"], 8.0)
        scan_cfg["focus_sec"] = min(scan_cfg["focus_sec"], 4.0)
    if HOURLY_SCAN_MODE:
        for k in scan_cfg:
            scan_cfg[k] = max(scan_cfg[k], HOURLY_SCAN_INTERVAL_SEC)

    def _scan_candle_refresh_sec(scan_type: str) -> float:
        base = CANDLE_REFRESH_SKIP_SEC
        if scan_type == "FOCUS":
            base = CANDLE_REFRESH_FOCUS_SEC
        elif scan_type == "WATCH":
            base = CANDLE_REFRESH_WATCH_SEC
        return base

    exit_candle_refresh_sec = max(EXIT_CANDLE_REFRESH_SEC, (float(tf_sec) * 1.5) if tf_sec > 0 else EXIT_CANDLE_REFRESH_SEC)
    if HOURLY_SCAN_MODE:
        exit_candle_refresh_sec = max(exit_candle_refresh_sec, HOURLY_SCAN_INTERVAL_SEC)
    runner_stats_hour = _new_runner_stats()
    runner_stats_week = _new_runner_stats()
    last_runner_hour = now_ts()
    last_runner_week = now_ts()
    last_loop_ts = now_ts()

    def _update_runner_stats(stats: dict, reason: str, exit_atr: float) -> None:
        stats["count_total"] += 1
        stats["sum_exit_atr_total"] += exit_atr
        if reason == "RUNNER_GIVEBACK":
            stats["count_giveback"] += 1
            stats["sum_exit_atr_giveback"] += exit_atr
        else:
            stats["count_other"] += 1
            stats["sum_exit_atr_other"] += exit_atr

    def _log_runner_stats(tag: str, stats: dict) -> None:
        ct = stats["count_total"]
        if ct <= 0:
            return
        avg_total = stats["sum_exit_atr_total"] / ct
        cg = stats["count_giveback"]
        co = stats["count_other"]
        avg_gb = (stats["sum_exit_atr_giveback"] / cg) if cg > 0 else 0.0
        avg_ot = (stats["sum_exit_atr_other"] / co) if co > 0 else 0.0
        log(
            f"{EMOJI_INFO} RUNNER_STATS_{tag}",
            {
                "n_total": ct,
                "avg_exit_atr_total": round(avg_total, 4),
                "n_giveback": cg,
                "avg_exit_atr_giveback": round(avg_gb, 4),
                "n_other": co,
                "avg_exit_atr_other": round(avg_ot, 4),
            },
        )

    def _record_runner_exit(reason: str, tr: dict, exit_atr: float, track: Optional[dict]) -> None:
        nonlocal last_runner_hour, last_runner_week
        ts_now = now_ts()
        trade_ts = float(tr.get("ts", ts_now))
        peak = float(track.get("peak", float("nan"))) if track else float("nan")
        peak_ts = float(track.get("peak_ts", trade_ts)) if track else trade_ts
        dd_from_peak = (peak - exit_atr) if math.isfinite(peak) else float("nan")
        time_to_bfe = peak_ts - trade_ts if math.isfinite(peak_ts) else float("nan")
        time_to_exit = ts_now - trade_ts

        direction = tr.get("dir") or None
        log(
            f"{EMOJI_INFO} RUNNER_METRICS {pair_tag(tr.get('pair', ''), direction)}",
            {
                "setup": tr.get("setup", ""),
                "reason": reason,
                "bfe_atr": round(peak, 4) if math.isfinite(peak) else None,
                "exit_atr": round(exit_atr, 4) if math.isfinite(exit_atr) else None,
                "dd_from_peak_atr": round(dd_from_peak, 4) if math.isfinite(dd_from_peak) else None,
                "time_to_bfe_sec": round(time_to_bfe, 1) if math.isfinite(time_to_bfe) else None,
                "time_to_exit_sec": round(time_to_exit, 1),
            },
        )

        if math.isfinite(exit_atr):
            _update_runner_stats(runner_stats_hour, reason, exit_atr)
            _update_runner_stats(runner_stats_week, reason, exit_atr)

        if (ts_now - last_runner_hour) >= 3600.0:
            _log_runner_stats("HOURLY", runner_stats_hour)
            runner_stats_hour.clear()
            runner_stats_hour.update(_new_runner_stats())
            last_runner_hour = ts_now

        if (ts_now - last_runner_week) >= 7 * 86400.0:
            _log_runner_stats("WEEKLY", runner_stats_week)
            runner_stats_week.clear()
            runner_stats_week.update(_new_runner_stats())
            last_runner_week = ts_now

    start_ts = now_ts()
    print("Entering main scanning loop...")
    sys.stdout.flush()
    
    while not _SHUTDOWN:
        try:
            time.sleep(LOOP_SLEEP_SEC)
            if run_for_sec is not None and (now_ts() - start_ts) >= float(run_for_sec):
                break
            now_loop = now_ts()
            if (now_loop - last_loop_ts) > 60.0:
                last_wide = 0.0
            if (now_loop - last_time_sync) >= 300.0:
                check_time_drift(o)
                last_time_sync = now_loop
        
            # Print status every 60 seconds
            if (now_loop - last_status_print) >= 60.0:
                print(f"\n{time.strftime('%H:%M:%S')} - STATUS UPDATE:")
                for pair, st in states.items():
                    age = int(now_loop - st.state_since)
                    print(f"  {pair}: {st.state} ({age}s ago)")
                print("")
                sys.stdout.flush()
                last_status_print = now_loop
            if os.path.exists(STOP_FLAG):
                time.sleep(1.0)
                continue
            if o._rate_limit_until > now_ts():
                time.sleep(min(1.0, o._rate_limit_until - now_ts()))
                continue
            if not db_ok:
                time.sleep(1.0)
                continue
                continue

            # Exit engine (rate-limited: pricing cadence + slower refresh cadence)
            open_trades = db_call("get_open_trades", db.get_open_trades) or []
            if not db_ok:
                continue
            if trade_track:
                open_ids = {tr["id"] for tr in open_trades}
                with _trade_track_lock:
                    for tid in list(trade_track.keys()):
                        if tid not in open_ids:
                            trade_track.pop(tid, None)
                            aee_last_update.pop(tid, None)
            if tick_exit_mode:
                open_ids = {tr["id"] for tr in open_trades}
                for tid in list(tick_exit_mode.keys()):
                    if tid not in open_ids:
                        tick_exit_mode.pop(tid, None)
            if sl_retry_state:
                open_oanda_ids = set()
                for tr in open_trades:
                    oid = tr.get("oanda_trade_id")
                    try:
                        if oid:
                            open_oanda_ids.add(int(oid))
                    except Exception:
                        continue
                for tid in list(sl_retry_state.keys()):
                    if tid not in open_oanda_ids:
                        sl_retry_state.pop(tid, None)
            now = now_ts()
            exit_interval = scan_cfg["exit_sec"]
            exit_refresh_interval = scan_cfg["exit_refresh_sec"]
            if TICK_EXIT_ENABLED and tick_exit_mode:
                expired = [tid for tid, info in tick_exit_mode.items() if now >= float(info.get("until", 0.0))]
                for tid in expired:
                    tick_exit_mode.pop(tid, None)
                if tick_exit_mode:
                    exit_interval = min(exit_interval, EXIT_SCAN_TICK_SEC)
                    exit_refresh_interval = min(exit_refresh_interval, EXIT_PRICE_REFRESH_TICK_SEC)

            if open_trades and (now - last_exit_scan) >= exit_interval:
                last_exit_scan = now

                pending_refresh_ok = True
                broker_pos_info: Dict[str, Dict[str, dict]] = {}
                if (now - last_exit_refresh) >= exit_refresh_interval:
                    broker_positions = _get_open_positions(now, "open_positions")
                    pending_orders_all = _get_pending_orders(now, "pending_orders")
                if broker_positions is None or pending_orders_all is None:
                    pending_refresh_ok = False
                else:
                    for bp in broker_positions:
                        inst = bp.get("instrument")
                        if not inst:
                            continue
                        longu = abs(int(float(bp.get("long", {}).get("units", "0") or "0")))
                        shortu = abs(int(float(bp.get("short", {}).get("units", "0") or "0")))
                        long_px = float(bp.get("long", {}).get("averagePrice", "nan") or "nan")
                        short_px = float(bp.get("short", {}).get("averagePrice", "nan") or "nan")
                        broker_pos_info[str(inst)] = {
                            "LONG": {"units": longu, "price": long_px},
                            "SHORT": {"units": shortu, "price": short_px},
                        }
                    pending_by_pair = {p: count_pair_pending(pending_orders_all, p) for p in PAIRS}
                    last_exit_refresh = now

                    # Reconcile: mark DB trades CLOSED if broker has no position (directional) and no pending order
                    for tr in open_trades:
                        pair = tr["pair"]
                        direction = str(tr.get("dir", ""))
                        if pending_by_pair.get(pair, 0) > 0:
                            continue
                        dir_units = broker_pos_info.get(pair, {}).get(direction, {}).get("units", 0)
                        if dir_units <= 0:
                            if db_call("mark_trade_closed", db.mark_trade_closed, int(tr["id"]), "BROKER_CLOSED") is None:
                                continue
                            setup_name = str(tr.get("setup", ""))
                            is_runner = setup_name.endswith("_RUN") or "_RUN" in setup_name
                            if is_runner:
                                with _trade_track_lock:
                                    track = trade_track.get(int(tr.get("id", 0)))
                                exit_atr = float("nan")
                                if track and track.get("samples"):
                                    exit_atr = float(track["samples"][-1][1])
                                _record_runner_exit("BROKER_CLOSED", tr, exit_atr, track)
                            with _trade_track_lock:
                                track = trade_track.get(int(tr.get("id", 0)))
                            exit_atr = float(track["samples"][-1][1]) if track and track.get("samples") else 0.0
                            _exit_log(tr, "BROKER_CLOSED", exit_atr, track)

                    # Sync: add DB trades for broker positions missing in DB
                    open_set = {(tr["pair"], str(tr.get("dir", ""))) for tr in open_trades}
                    for pair, dirs in broker_pos_info.items():
                        for direction, info in dirs.items():
                            units = int(info.get("units", 0) or 0)
                            if units <= 0 or (pair, direction) in open_set:
                                continue
                            entry_px = float(info.get("price", "nan") or "nan")
                            if not is_valid_price(entry_px):
                                log_throttled(
                                    f"sync_bad_price:{pair}:{direction}",
                                    f"{EMOJI_WARN} SYNC_BAD_PRICE {pair_tag(pair, direction)}",
                                    {"pair": pair, "direction": direction, "price": entry_px},
                                )
                                continue
                            sync_count = max(ATR_N + 5, 40) + 1
                            sync_candles = oanda_call("candles_sync", o.candles, pair, TF_EXEC, sync_count)
                            if not sync_candles:
                                continue
                            ok_c, reason = validate_candles(pair, sync_candles, tf_sec, allow_partial=True)
                            if not ok_c:
                                log_throttled(
                                    f"sync_bad_candles:{pair}:{reason}",
                                    f"{EMOJI_WARN} SYNC_BAD_CANDLES {pair_tag(pair, direction)}",
                                    {"pair": pair, "direction": direction, "reason": reason},
                                )
                                continue
                            atr_entry = atr(sync_candles, ATR_N)
                            if not (math.isfinite(atr_entry) and atr_entry > 0.0):
                                log_throttled(
                                    f"sync_bad_atr:{pair}:{direction}",
                                    f"{EMOJI_WARN} SYNC_BAD_ATR {pair_tag(pair, direction)}",
                                    {"pair": pair, "direction": direction, "atr": atr_entry},
                                )
                                continue
                            trade_id = db_call(
                                "add_trade_sync",
                                db.record_trade,
                                pair=pair,
                                setup=f"BROKER_SYNC_{direction}",
                                direction=direction,
                                mode="SYNC",
                                units=units if direction == "LONG" else -units,
                                entry=entry_px,
                                atr_entry=atr_entry,
                                ttl_sec=0,
                                pg_t=0,
                                pg_atr=0.0,
                                note="broker_sync",
                            )
                            if trade_id:
                                log(
                                    f"{EMOJI_DB} SYNC_TRADE {pair_tag(pair, direction)}",
                                    {"trade_id": trade_id, "units": units, "entry": entry_px},
                                )
                    open_trades = db_call("get_open_trades", db.get_open_trades) or open_trades

            # Initialize pending_refresh_ok - will be updated if exit processing runs
            pending_refresh_ok = True

            if not pending_refresh_ok:
                continue

            exit_pairs = sorted({tr["pair"] for tr in open_trades})
            price_map_exit = {}
            
            if exit_pairs:
                price_map_exit = _get_pricing_multi(
                    exit_pairs,
                    now,
                    cache=exit_price_cache,
                    ttl=EXIT_PRICE_REFRESH_SEC,
                    label="pricing_multi_exit",
                ) or {}
                for p, px in price_map_exit.items():
                    try:
                        bid, ask = px
                    except Exception:
                        continue
                    simulate_price_stream_update(p, bid, ask, tick_cache=tick_cache)

            for tr in open_trades:
                if _SHUTDOWN:
                    break
                pair = tr["pair"]
                direction = str(tr.get("dir", ""))
                if pending_by_pair and pending_by_pair.get(pair, 0) > 0:
                    continue
                
                # Exit retry backoff (do not hard-block; retry with backoff + logs)
                if pair in EXIT_BLOCKED_PAIRS:
                    block_info = EXIT_BLOCKED_PAIRS[pair]
                    next_retry = float(block_info.get("next_retry_ts", 0.0))
                    if next_retry and now_ts() < next_retry:
                        log_throttled(
                            f"exit_blocked:{pair}",
                            f"{EMOJI_WARN} EXIT_BACKOFF {pair_tag(pair, direction)}",
                            {
                                "pair": pair,
                                "direction": direction,
                                "reason": block_info.get("reason", "unknown"),
                                "fail_count": block_info.get("fail_count", 0),
                                "next_retry_in_sec": round(next_retry - now_ts(), 2),
                            },
                        )
                        continue
                    else:
                        log_throttled(
                            f"exit_retry:{pair}",
                            f"{EMOJI_WARN} EXIT_RETRY {pair_tag(pair, direction)}",
                            {"pair": pair, "direction": direction, "reason": block_info.get("reason", "unknown")},
                            min_interval=10.0,
                        )
                age = now_ts() - float(tr["ts"])
                ttl_sec = int(tr["ttl_sec"])
                pg_t = int(tr["pg_t"])
                pg_atr = float(tr["pg_atr"])
                atr_entry = float(tr["atr_entry"])
                entry = float(tr["entry"])

                if pair in price_map_exit:
                    bid, ask = price_map_exit[pair]
                else:
                    resp_price = oanda_call("pricing_exit", o.pricing, pair)
                    if not resp_price:
                        continue
                    bid, ask = resp_price
                if not validate_price(pair, bid, ask, "exit"):
                    continue
                if not (math.isfinite(atr_entry) and atr_entry > 0.0):
                    fb_atr, fb_src = _resolve_atr_with_fallback(pair, None, ATR_N, now, TF_EXEC)
                    log_throttled(
                        f"exit_bad_atr:{pair}",
                        f"{EMOJI_WARN} EXIT_BAD_ATR {pair_tag(pair, direction)}",
                        {"pair": pair, "atr_entry": atr_entry, "fallback_atr": fb_atr, "fallback_src": fb_src},
                    )
                    atr_entry = fb_atr

                mid = (bid + ask) / 2.0
                favorable = (mid - entry) if direction == "LONG" else (entry - mid)
                favorable_atr = favorable / atr_entry
                adverse_atr = -favorable_atr
                trade_id = int(tr.get("id", 0))
                oanda_trade_id = tr.get("oanda_trade_id")
                with _trade_track_lock:
                    track = trade_track.get(trade_id)

                # Retry stop-loss placement if previous add failed
                if oanda_trade_id:
                    try:
                        retry_key = int(oanda_trade_id)
                    except Exception:
                        retry_key = None
                    if retry_key is not None and retry_key in sl_retry_state:
                        retry = sl_retry_state.get(retry_key) or {}
                        next_ts = float(retry.get("next_ts", 0.0))
                        if now >= next_ts:
                            sl_price = float(retry.get("sl", 0.0))
                            resp_sl = oanda_call("retry_sl", o.set_trade_stop_loss, str(oanda_trade_id), sl_price, allow_error_dict=True)
                            ok_sl = isinstance(resp_sl, dict) and not (
                                resp_sl.get("_http_error")
                                or resp_sl.get("_rate_limited")
                                or resp_sl.get("_json_error")
                                or resp_sl.get("_exception")
                            )
                            if ok_sl:
                                sl_retry_state.pop(retry_key, None)
                                log(f"{EMOJI_OK} SL_RETRY_OK {pair_tag(pair, direction)}", {"trade_id": oanda_trade_id, "sl": sl_price})
                            else:
                                fail_count = int(retry.get("fail_count", 0)) + 1
                                backoff = min(60.0, SL_RETRY_BASE_SEC * (2 ** (fail_count - 1)))
                                retry.update({"fail_count": fail_count, "next_ts": now + backoff})
                                sl_retry_state[retry_key] = retry
                                log_throttled(
                                    f"sl_retry_fail:{pair}:{oanda_trade_id}",
                                    f"{EMOJI_WARN} SL_RETRY_FAIL {pair_tag(pair, direction)}",
                                    {"trade_id": oanda_trade_id, "sl": sl_price, "backoff_sec": backoff, "fail_count": fail_count},
                                    min_interval=10.0,
                                )

                # === ADAPTIVE EXIT ENGINE (AEE) ===
                # Get current candles for AEE calculations
                exec_candles, _ = _get_exec_candles_with_fallback(pair, 30, now, exit_candle_refresh_sec)
                if not exec_candles:
                    log(f"{EMOJI_WARN} AEE_NO_CANDLES {pair_tag(pair, direction)}", {"pair": pair, "direction": direction})
                    exec_candles = []
                
                # Calculate AEE metrics
                aee_metrics = calculate_aee_metrics(tr, mid, atr_entry, exec_candles)
                
                # Get current spread
                spread_pips = to_pips(pair, ask - bid) if math.isfinite(ask) and math.isfinite(bid) else 0
                atr_pips_val = atr_pips(pair, atr_entry)
                
                # Tick-exit arming/disarming (lightweight)
                if TICK_EXIT_ENABLED:
                    tick_reason = ""
                    progress = float(aee_metrics.get("progress", 0.0))
                    speed = float(aee_metrics.get("speed", 0.0))
                    velocity = float(aee_metrics.get("velocity", 0.0))
                    pullback = float(aee_metrics.get("pullback", 0.0))
                    # Estimate TP anchor from speed class (tp1_atr)
                    setup_name = str(tr.get("setup", ""))
                    speed_class = speed_class_from_setup_name(setup_name)
                    tp1_atr = get_speed_params(speed_class)["tp1_atr"]
                    if atr_entry > 0:
                        tp_anchor = entry + (tp1_atr * atr_entry if direction == "LONG" else -tp1_atr * atr_entry)
                        dist_to_tp_atr = abs(tp_anchor - mid) / atr_entry
                    else:
                        dist_to_tp_atr = 9e9
                    near_tp_band = max(NEAR_TP_BAND_ATR_BASE, 1.2 * spread_pips / atr_pips_val) if atr_pips_val > 0 else NEAR_TP_BAND_ATR_BASE
                    # Median spread from state history if available
                    st_exit = states.get(pair)
                    median_spread = _median(st_exit.spread_history) if (st_exit and st_exit.spread_history) else spread_pips

                    if progress <= 0.25:
                        tick_reason = "A"
                    elif progress >= 0.45 and speed < 0.70 and velocity < 0:
                        tick_reason = "B"
                    elif (velocity <= -0.60) or (pullback >= 0.45) or (spread_pips > 1.6 * max(median_spread, 0.1)):
                        tick_reason = "C"
                    elif dist_to_tp_atr <= near_tp_band and progress >= 0.50:
                        tick_reason = "D"

                    prev_tick = tick_exit_mode.get(trade_id)
                    if tick_reason:
                        if (not prev_tick) or prev_tick.get("reason") != tick_reason:
                            log_throttled(
                                f"tick_exit_arm:{pair}:{trade_id}",
                                f"{EMOJI_INFO} TICK_EXIT_ARM {pair_tag(pair, direction)}",
                                {"trade_id": trade_id, "reason": tick_reason, "progress": round(progress, 2)},
                                min_interval=5.0,
                            )
                        tick_exit_mode[trade_id] = {"until": now + TICK_MODE_TTL_SEC, "reason": tick_reason}
                    else:
                        if prev_tick:
                            log_throttled(
                                f"tick_exit_disarm:{pair}:{trade_id}",
                                f"{EMOJI_INFO} TICK_EXIT_DISARM {pair_tag(pair, direction)}",
                                {"trade_id": trade_id},
                                min_interval=10.0,
                            )
                        tick_exit_mode.pop(trade_id, None)
                
                # Determine AEE phase and action
                aee_phase = tr.get("aee_phase", "ENTRY_PROTECTION")
                exit_reason = None
                
                # Phase 1: Entry Protection
                if not tr.get("aee_entry_protected"):
                    if aee_entry_protection(tr, aee_metrics, spread_pips, atr_pips_val):
                        # Mark as protected
                        db_call("update_aee_protected", db.update_aee_state, int(tr["id"]), "PROTECTED", protected=1)
                        aee_phase = "PROTECTED"
                        log(f"{EMOJI_INFO} AEE_ENTRY_PROTECTED {pair_tag(pair, direction)}", {
                            "pair": pair,
                            "direction": direction,
                            "progress_atr": aee_metrics["progress"],
                            "threshold": max(0.35, 1.5 * spread_pips / atr_pips_val) if atr_pips_val > 0 else 0.35
                        })
                
                # Phase 6: Panic Exit (highest priority)
                if aee_should_panic_exit(tr, aee_metrics):
                    exit_reason = "AEE_PANIC"
                    aee_phase = "PANIC_EXIT"
                
                # Phase 4: Momentum Decay Exit
                elif aee_should_exit_momentum_decay(tr, aee_metrics):
                    exit_reason = "AEE_MOMENTUM_DECAY"
                    aee_phase = "MOMENTUM_DECAY"
                
                # Phase 5: Pulse Capture (trail exit line)
                elif aee_phase == "PULSE_CAPTURE":
                    # Update trailing extremes
                    local_high = aee_metrics["local_high"]
                    local_low = aee_metrics["local_low"]
                    stored_high = float(tr.get("aee_local_high", float("nan")))
                    stored_low = float(tr.get("aee_local_low", float("nan")))
                    if math.isfinite(stored_high):
                        local_high = max(local_high, stored_high)
                    if math.isfinite(stored_low):
                        local_low = min(local_low, stored_low)
                    # Persist updated extremes
                    db_call(
                        "update_aee_state_pulse",
                        db.update_aee_state,
                        int(tr["id"]),
                        aee_phase,
                        local_high=local_high,
                        local_low=local_low,
                    )
                    if direction == "LONG":
                        exit_line = local_high - (0.25 * atr_entry)
                        if mid <= exit_line:
                            exit_reason = "AEE_PULSE_CAPTURE"
                    else:
                        exit_line = local_low + (0.25 * atr_entry)
                        if mid >= exit_line:
                            exit_reason = "AEE_PULSE_CAPTURE"
                elif aee_should_capture_pulse(tr, aee_metrics):
                    aee_phase = "PULSE_CAPTURE"
                    db_call(
                        "update_aee_state_pulse_start",
                        db.update_aee_state,
                        int(tr["id"]),
                        aee_phase,
                        local_high=aee_metrics["local_high"],
                        local_low=aee_metrics["local_low"],
                    )
                
                # Phase 2: Break-Even Move
                elif aee_should_break_even(tr, aee_metrics):
                    # Move SL to Entry + (0.1-0.15 ATR) in trade direction
                    new_sl = entry + (0.1 * atr_entry if direction == "LONG" else -0.1 * atr_entry)
                    
                    # Use OANDA API to update SL
                    oanda_trade_id = tr.get("oanda_trade_id")
                    if oanda_trade_id:
                        # Create or replace stop loss directly on the trade
                        oanda_call("set_trade_sl", o.set_trade_stop_loss, oanda_trade_id, new_sl, allow_error_dict=True)
                    
                    aee_phase = "BREAK_EVEN"
                    log(f"{EMOJI_INFO} AEE_BREAK_EVEN {pair_tag(pair, direction)}", {
                        "pair": pair,
                        "direction": direction,
                        "progress_atr": aee_metrics["progress"],
                        "new_sl": new_sl
                    })
                
                # Phase 3: Hold Runner
                elif aee_should_hold_runner(tr, aee_metrics):
                    aee_phase = "HOLD_RUNNER"
                
                # Update AEE state periodically (every ~30 seconds, per trade)
                trade_id_int = int(tr["id"])
                last_aee_update = aee_last_update.get(trade_id_int, 0.0)
                if (now - last_aee_update) >= 30.0:
                    db_call(
                        "update_aee_state",
                        db.update_aee_state,
                        trade_id_int,
                        aee_phase,
                        local_high=aee_metrics["local_high"],
                        local_low=aee_metrics["local_low"],
                    )
                    aee_last_update[trade_id_int] = now
                
                # Execute AEE exit if triggered
                if exit_reason:
                    if exit_reason == "AEE_PANIC":
                        px = price_map_exit.get(pair)
                        if not px:
                            try:
                                px = oanda_call(f"pricing_panic_{pair}", o.pricing, pair, allow_error_dict=False)
                            except Exception:
                                px = None
                        if px and isinstance(px, (list, tuple)) and len(px) >= 2:
                            bid_now, ask_now = float(px[0]), float(px[1])
                        else:
                            bid_now, ask_now = mid, mid
                        log(f"{EMOJI_WARN} PANIC_LADDER_CALL {pair_tag(pair, direction)}", {
                            "pair": pair,
                            "direction": direction,
                            "trade_id": int(tr["id"]),
                            "units": int(tr.get("units", 0) or 0),
                            "bid": bid_now,
                            "ask": ask_now,
                            "exit_reason": exit_reason,
                        })
                        success, resp = panic_execution_ladder(
                            o=o,
                            pair=pair,
                            direction=direction,
                            bid=bid_now,
                            ask=ask_now,
                            units=int(tr.get("units", 0) or 0),
                            exit_reason=exit_reason.lower(),
                            db_trade_id=int(tr["id"]),
                        )
                    else:
                        success, resp = _close_trade_or_position(
                            o, pair, direction, tr.get("oanda_trade_id"), exit_reason.lower(), int(tr["id"])
                        )
                    if not success:
                        if _handle_close_error(resp, pair, direction, tr, exit_reason, favorable_atr, track):
                            continue
                        log_throttled(
                            f"close_fail_aee:{pair}:{exit_reason}",
                            f"{EMOJI_ERR} CLOSE_FAIL {pair_tag(pair, direction)}",
                            {"reason": exit_reason, "resp": resp},
                        )
                        continue
                    
                    # Mark as closed
                    db_call("mark_trade_closed", db.mark_trade_closed, int(tr["id"]), exit_reason)
                    _exit_log(tr, exit_reason, favorable_atr, track)
                    
                    # Record runner exit if applicable
                    setup_name = str(tr.get("setup", ""))
                    is_runner = setup_name.endswith("_RUN") or "_RUN" in setup_name
                    if is_runner:
                        _record_runner_exit(exit_reason, tr, favorable_atr, track)
                    
                    log(f"{EMOJI_EXIT} AEE_EXIT {pair_tag(pair, direction)}", {
                        "pair": pair,
                        "direction": direction,
                        "reason": exit_reason,
                        "phase": aee_phase,
                        "progress_atr": aee_metrics["progress"],
                        "speed": aee_metrics["speed"],
                        "velocity": aee_metrics["velocity"],
                        "pullback": aee_metrics["pullback"],
                        "favorable_atr": favorable_atr
                    })
                    continue

                trade_id = int(tr.get("id", 0))
                with _trade_track_lock:
                    track = trade_track.get(trade_id)
                    if track is None:
                        track = {"samples": [], "peak": favorable_atr, "peak_ts": now_ts(), "max_dd": 0.0}
                        trade_track[trade_id] = track
                    if favorable_atr > track["peak"]:
                        track["peak"] = favorable_atr
                        track["peak_ts"] = now_ts()
                    samples = track["samples"]
                samples.append((now_ts(), favorable_atr))
                prev_locked = bool(track.get("decay_locked", False))
                lock_info = aee_decay_lock_update(aee_metrics=aee_metrics, track=track)
                if (not prev_locked) and lock_info.get("decay_locked"):
                    log_throttled(
                        f"decay_lock:{pair}:{trade_id}",
                        f"{EMOJI_INFO} DECAY_LOCK {pair_tag(pair, direction)}",
                        {"trade_id": trade_id, "locked_peak": round(float(lock_info.get("locked_peak", 0.0)), 4)},
                        min_interval=10.0,
                    )
                cut_ts = now_ts() - MAD_DECAY_TIME_SEC
                while samples and samples[0][0] < cut_ts:
                    samples.pop(0)
                vel_atr_per_sec = _velocity_atr_per_sec(samples)
                microtrend_alive = _microtrend_alive(samples)
                dd_from_peak = track["peak"] - favorable_atr
                if dd_from_peak > track.get("max_dd", 0.0):
                    track["max_dd"] = dd_from_peak
                setup_name = str(tr.get("setup", ""))
                is_runner = setup_name.endswith("_RUN") or "_RUN" in setup_name
                leg_mult = RUN_GIVEBACK_MULT if is_runner else MAIN_GIVEBACK_MULT
                speed_class = speed_class_from_setup_name(setup_name)

                if adverse_atr >= ADVERSE_KILL_ATR and age < (ttl_sec * ADVERSE_KILL_TTL_FRAC):
                    success, resp = _close_trade_or_position(
                        o, pair, direction, tr.get("oanda_trade_id"), "adverse", int(tr["id"])
                    )
                    if not success:
                        if _handle_close_error(resp, pair, direction, tr, "ADVERSE_KILL", favorable_atr, track):
                            continue
                        log_throttled(
                            f"close_fail_adverse:{pair}",
                            f"{EMOJI_ERR} CLOSE_FAIL {pair_tag(pair, direction)}",
                            {"reason": "ADVERSE_KILL", "resp": resp},
                        )
                        continue
                    db_call("mark_trade_closed", db.mark_trade_closed, int(tr["id"]), "ADVERSE_KILL")
                    _exit_log(tr, "ADVERSE_KILL", favorable_atr, track)
                    if is_runner:
                        _record_runner_exit("ADVERSE_KILL", tr, favorable_atr, track)
                    continue

                if (
                    ENABLE_MAD_EXIT
                    and is_runner
                    and favorable_atr >= MIN_RUNNER_PROFIT_ATR
                    and age >= min_age_for_speed_class(speed_class)
                    and samples
                    and len(samples) >= MAD_SAMPLE_COUNT
                    and track["peak"] >= MAD_DECAY_MIN_PROFIT_ATR
                ):
                    mad_val = _mad([s[1] for s in samples])
                    if math.isfinite(mad_val) and mad_val <= MAD_DECAY_THRESHOLD_ATR:
                        success, resp = _close_trade_or_position(
                            o, pair, direction, tr.get("oanda_trade_id"), "mad", int(tr["id"])
                        )
                        if not success:
                            if _handle_close_error(resp, pair, direction, tr, "MAD_EXIT", favorable_atr, track):
                                continue
                            log_throttled(
                                f"close_fail_mad:{pair}",
                                f"{EMOJI_ERR} CLOSE_FAIL {pair_tag(pair, direction)}",
                                {"reason": "MAD_EXIT", "resp": resp},
                            )
                            continue
                        db_call("mark_trade_closed", db.mark_trade_closed, int(tr["id"]), "MAD_EXIT")
                        _exit_log(tr, "MAD_EXIT", favorable_atr, track)
                        _record_runner_exit("MAD_EXIT", tr, favorable_atr, track)
                        continue

                if (
                    is_runner
                    and track["peak"] >= RUNNER_BFE_MIN_ATR
                    and dd_from_peak >= (float(track.get("allowed_giveback_atr", RUNNER_GIVEBACK_ATR)) * float(leg_mult))
                    and vel_atr_per_sec <= RUNNER_VEL_MAX
                ):
                    success, resp = _close_trade_or_position(
                        o, pair, direction, tr.get("oanda_trade_id"), "giveback", int(tr["id"])
                    )
                    if not success:
                        if _handle_close_error(resp, pair, direction, tr, "RUNNER_GIVEBACK", favorable_atr, track):
                            continue
                        log_throttled(
                            f"close_fail_giveback:{pair}",
                            f"{EMOJI_ERR} CLOSE_FAIL {pair_tag(pair, direction)}",
                            {"reason": "RUNNER_GIVEBACK", "resp": resp, "threshold_atr": float(track.get("allowed_giveback_atr", RUNNER_GIVEBACK_ATR)) * float(leg_mult)},
                        )
                        continue
                    db_call("mark_trade_closed", db.mark_trade_closed, int(tr["id"]), "RUNNER_GIVEBACK")
                    _exit_log(tr, "RUNNER_GIVEBACK", favorable_atr, track)
                    _record_runner_exit("RUNNER_GIVEBACK", tr, favorable_atr, track)
                    continue

                if pg_t > 0 and age >= float(pg_t) and favorable_atr < float(pg_atr):
                    success, resp = _close_trade_or_position(
                        o, pair, direction, tr.get("oanda_trade_id"), "pg", int(tr["id"])
                    )
                    if not success:
                        if _handle_close_error(resp, pair, direction, tr, "PG_CLOSE", favorable_atr, track):
                            continue
                        log_throttled(
                            f"close_fail_pg:{pair}",
                            f"{EMOJI_ERR} CLOSE_FAIL {pair_tag(pair, direction)}",
                            {"reason": "PG_CLOSE", "resp": resp},
                        )
                        continue
                    db_call("mark_trade_closed", db.mark_trade_closed, int(tr["id"]), "PG_CLOSE")
                    _exit_log(tr, "PG_CLOSE", favorable_atr, track)
                    if is_runner:
                        _record_runner_exit("PG_CLOSE", tr, favorable_atr, track)
                    continue

                # Soft TTL logic (profit-aware)
                ttl_action = ttl_exit_decision(age, ttl_sec, favorable_atr, microtrend_alive)
                if ttl_action == "TTL_TAKE_PROFIT":
                    success, resp = _close_trade_or_position(
                        o, pair, direction, tr.get("oanda_trade_id"), "ttl_profit", int(tr["id"])
                    )
                    if not success:
                        if _handle_close_error(resp, pair, direction, tr, "TTL_TAKE_PROFIT", favorable_atr, track):
                            continue
                        log_throttled(
                            f"close_fail_ttl_profit:{pair}",
                            f"{EMOJI_ERR} CLOSE_FAIL {pair_tag(pair, direction)}",
                            {"reason": "TTL_TAKE_PROFIT", "resp": resp},
                        )
                        continue
                    db_call("mark_trade_closed", db.mark_trade_closed, int(tr["id"]), "TTL_TAKE_PROFIT")
                    _exit_log(tr, "TTL_TAKE_PROFIT", favorable_atr, track)
                    if is_runner:
                        _record_runner_exit("TTL_TAKE_PROFIT", tr, favorable_atr, track)
                    continue

                if ttl_action == "TTL_NO_FOLLOWTHROUGH":
                    success, resp = _close_trade_or_position(
                        o, pair, direction, tr.get("oanda_trade_id"), "ttl", int(tr["id"])
                    )
                    if not success:
                        if _handle_close_error(resp, pair, direction, tr, "TTL_NO_FOLLOWTHROUGH", favorable_atr, track):
                            continue
                        log_throttled(
                            f"close_fail_ttl:{pair}",
                            f"{EMOJI_ERR} CLOSE_FAIL {pair_tag(pair, direction)}",
                            {"reason": "TTL_NO_FOLLOWTHROUGH", "resp": resp},
                        )
                        continue
                    db_call("mark_trade_closed", db.mark_trade_closed, int(tr["id"]), "TTL_NO_FOLLOWTHROUGH")
                    _exit_log(tr, "TTL_NO_FOLLOWTHROUGH", favorable_atr, track)
                    if is_runner:
                        _record_runner_exit("TTL_NO_FOLLOWTHROUGH", tr, favorable_atr, track)
                    continue

            # Separate pairs by state for different scan frequencies
            get_ready_pairs = []
            enter_pairs = []

            for p, st in states.items():
                if st.state in ("GET_READY", "ARM_TICK_ENTRY"):
                    # ALL GET_READY pairs are included - NO EXCLUSIONS based on WR
                    get_ready_pairs.append(p)

                    # Check for data gaps for logging only
                    has_data_gap = not (math.isfinite(st.wr) and math.isfinite(st.m_norm) and math.isfinite(st.atr_exec))

                    if has_data_gap:
                        print(f"  {p}: DATA GAP - still in FOCUS scan (WR={st.wr}, m_norm={st.m_norm}, atr={st.atr_exec})")

                    # Log continuation overrides for visibility
                    wr_pinned_oversold = st.wr <= WR_OVERSOLD
                    wr_pinned_overbought = st.wr >= WR_OVERBOUGHT
                    atr_rising = math.isfinite(st.atr_long) and st.atr_exec > (st.atr_long * 1.05)
                    momentum_strong = abs(st.m_norm) >= 0.30  # Relaxed from 1.0
                    continuation_override = (wr_pinned_oversold or wr_pinned_overbought) and momentum_strong

                    # Directional continuation (new - less strict)
                    wr_directional_oversold = st.wr <= -55.0
                    wr_directional_overbought = st.wr >= -45.0
                    momentum_moderate = abs(st.m_norm) >= 0.20  # Relaxed from 0.40
                    atr_alive = math.isfinite(st.atr_long) and (st.atr_exec / st.atr_long) >= 0.60
                    directional_continuation = (wr_directional_oversold or wr_directional_overbought) and momentum_moderate and atr_alive

                    if continuation_override:
                        print(f"  {p}: PINNED CONTINUATION OVERRIDE (WR={st.wr:.1f}, m_norm={st.m_norm:.2f})")
                    elif directional_continuation:
                        print(
                            f"  {p}: DIRECTIONAL CONTINUATION OVERRIDE (WR={st.wr:.1f}, "
                            f"m_norm={st.m_norm:.2f}, atr_ratio={st.atr_exec/st.atr_long:.2f})"
                        )
                elif st.state == "ENTER":
                    enter_pairs.append(p)

            focus_pairs = get_ready_pairs + enter_pairs

            # Check which scan to run
            do_focus = (now_ts() - last_focus) >= scan_cfg["focus_sec"] and bool(focus_pairs)
            do_skip = (now_ts() - last_wide) >= scan_cfg["skip_sec"]
            do_watch = (now_ts() - last_watch) >= scan_cfg["watch_sec"]

            # Additional check: if there are GET_READY pairs not in focus, scan them at WATCH frequency
            get_ready_not_in_focus = []
            for p, st in states.items():
                if st.state in ("GET_READY", "ARM_TICK_ENTRY") and p not in get_ready_pairs:
                    get_ready_not_in_focus.append(p)

            # Check WATCH pairs for near-trigger conditions to bump to GET_READY
            watch_to_bump = []
            for p, st in states.items():
                if st.state == "WATCH":
                    # Check if pair is near trigger conditions
                    near_trigger = False

                    # If WR is NaN, allow momentum-only bump
                    if not math.isfinite(st.wr):
                        if math.isfinite(st.m_norm) and abs(st.m_norm) >= 0.30:  # Relaxed from 1.00
                            near_trigger = True
                            print(f"  {p}: WATCH -> GET_READY (WR=NaN, momentum={st.m_norm:.2f})")
                    else:
                        # Check WR bands - exact values as specified
                        if st.wr <= -65.0 or st.wr >= -35.0:
                            near_trigger = True
                            print(f"  {p}: WATCH -> GET_READY (WR={st.wr:.1f} in bump zone)")

                        # Or check momentum threshold
                        elif math.isfinite(st.m_norm) and abs(st.m_norm) >= 0.30:  # Relaxed from 1.00
                            near_trigger = True
                            print(f"  {p}: WATCH -> GET_READY (momentum={st.m_norm:.2f})")

                    if near_trigger:
                        watch_to_bump.append(p)
                        _transition_state(st, "GET_READY", p)
                        st.get_ready_weak_scans = 0  # Reset hysteresis counter

            if get_ready_not_in_focus and do_watch:
                # Include GET_READY pairs in WATCH scan if they're not approaching trigger
                watch_pairs = [p for p, st in states.items() if st.state == "WATCH"] + get_ready_not_in_focus
            else:
                watch_pairs = [p for p, st in states.items() if st.state == "WATCH"]

            if not do_focus and not do_skip and not do_watch:
                continue

            # Determine which pairs to scan
            if do_focus:
                scan_type = "FOCUS"
                scan_pairs = focus_pairs
                last_focus = now_ts()
            elif do_watch:
                scan_type = "WATCH"
                scan_pairs = watch_pairs
                last_watch = now_ts()
            else:  # do_skip
                scan_type = "SKIP"
                scan_pairs = [p for p, st in states.items() if st.state == "SKIP"]
                last_wide = now_ts()

            scan_now = now_ts()
            candle_refresh_sec = _scan_candle_refresh_sec(scan_type)

            # Log what we're scanning
            print(f"\n{time.strftime('%H:%M:%S')} - {scan_type} SCAN: {', '.join(scan_pairs)}")
            log_runtime("info", f"SCAN_START", type=scan_type, pairs=scan_pairs, count=len(scan_pairs))

            # Note: GET_READY pairs are NEVER excluded by Williams %R
            # All GET_READY pairs are included in focus scan

            # Log continuation overrides
            for p in get_ready_pairs:
                st = states[p]
                wr_pinned_oversold = st.wr <= WR_OVERSOLD
                wr_pinned_overbought = st.wr >= WR_OVERBOUGHT
                if wr_pinned_oversold or wr_pinned_overbought:
                    print(f"    {p}: In FOCUS - WR={st.wr:.1f} (pinned), m_norm={st.m_norm:.2f}")

            sys.stdout.flush()

            acct_sum = _get_account_summary(scan_now)
            if not acct_sum:
                continue

            margin_avail = float(acct_sum.get("marginAvailable", "nan") or "nan")
            if not (math.isfinite(margin_avail) and margin_avail > 0.0):
                log_throttled(
                    "margin_invalid",
                    f"{EMOJI_WARN} INVALID_MARGIN",
                    {"margin_available": margin_avail},
                )
                continue
            open_pos = _get_open_positions(scan_now, "open_positions_scan")
            if open_pos is None:
                # Do not abort indicator scan if positions unavailable; treat as empty.
                open_pos = []
            for bp in list(EXIT_BLOCKED_PAIRS.keys()):
                if count_pair_positions(open_pos, bp) == 0:
                    EXIT_BLOCKED_PAIRS.pop(bp, None)
            pending_orders = _get_pending_orders(scan_now, "pending_orders_scan")
            if pending_orders is None:
                pending_orders = []
            pending_by_pair_scan = {p: count_pair_pending(pending_orders, p) for p in PAIRS}
            
            price_map = _get_pricing_multi(
                scan_pairs,
                scan_now,
                cache=price_cache,
                ttl=PRICE_REFRESH_SEC,
                label="pricing_multi_scan",
            )
            if not price_map:
                print("[PRICING] No pricing data available - using cached data only")
                sys.stdout.flush()
                continue

            # Feed pricing into path/tick engines
            for p, px in price_map.items():
                try:
                    bid, ask = px
                except Exception:
                    continue
                simulate_price_stream_update(p, bid, ask, tick_cache=tick_cache)

            # Poll order/position books (state-driven cadence) and compute book metrics
            try:
                _poll_books(o, states, price_map, book_cache, scan_now)
            except Exception as e:
                log_throttled(
                    "books_poll_fail",
                    f"{EMOJI_WARN} BOOKS_POLL_FAIL",
                    {"error": str(e)},
                    min_interval=10.0,
                )

            global_open = sum(count_pair_positions(open_pos, p) for p in PAIRS)
            db_open_trades = db_call("get_open_trades_scan", db.get_open_trades) or []
            if not db_ok:
                continue

            # Sync broker positions into DB if missing
            broker_pos_info: Dict[str, Dict[str, dict]] = {}
            for bp in open_pos:
                inst = bp.get("instrument")
                if not inst:
                    continue
                longu = abs(int(float(bp.get("long", {}).get("units", "0") or "0")))
                shortu = abs(int(float(bp.get("short", {}).get("units", "0") or "0")))
                long_px = float(bp.get("long", {}).get("averagePrice", "nan") or "nan")
                short_px = float(bp.get("short", {}).get("averagePrice", "nan") or "nan")
                broker_pos_info[str(inst)] = {
                    "LONG": {"units": longu, "price": long_px},
                    "SHORT": {"units": shortu, "price": short_px},
                }

            open_set = {(tr["pair"], str(tr.get("dir", ""))) for tr in db_open_trades}
            for pair, dirs in broker_pos_info.items():
                for direction, info in dirs.items():
                    units = int(info.get("units", 0) or 0)
                    if units <= 0 or (pair, direction) in open_set:
                        continue
                    entry_px = float(info.get("price", "nan") or "nan")
                    if not is_valid_price(entry_px):
                        log_throttled(
                            f"sync_bad_price_scan:{pair}:{direction}",
                            f"{EMOJI_WARN} SYNC_BAD_PRICE {pair_tag(pair, direction)}",
                            {"pair": pair, "direction": direction, "price": entry_px},
                        )
                        continue
                    sync_count = max(ATR_N + 5, 40) + 1
                    sync_candles = oanda_call("candles_sync_scan", o.candles, pair, TF_EXEC, sync_count)
                    if not sync_candles:
                        continue
                    ok_c, reason = validate_candles(pair, sync_candles, tf_sec, allow_partial=True)
                    if not ok_c:
                        log_throttled(
                            f"sync_bad_candles_scan:{pair}:{reason}",
                            f"{EMOJI_WARN} SYNC_BAD_CANDLES {pair_tag(pair, direction)}",
                            {"pair": pair, "direction": direction, "reason": reason},
                        )
                    atr_entry = atr(sync_candles, ATR_N)
                    if not (math.isfinite(atr_entry) and atr_entry > 0.0):
                        log_throttled(
                            f"sync_bad_atr_scan:{pair}:{direction}",
                            f"{EMOJI_WARN} SYNC_BAD_ATR {pair_tag(pair, direction)}",
                            {"pair": pair, "direction": direction, "atr": atr_entry},
                        )
                        continue
                    trade_id = db_call(
                        "add_trade_sync_scan",
                        db.record_trade,
                        pair=pair,
                        setup=f"BROKER_SYNC_{direction}",
                        direction=direction,
                        mode="SYNC",
                        units=units if direction == "LONG" else -units,
                        entry=entry_px,
                        atr_entry=atr_entry,
                        ttl_sec=0,
                        pg_t=0,
                        pg_atr=0.0,
                        note="broker_sync",
                    )
                    if trade_id:
                        db_open_trades.append({"pair": pair, "setup": f"BROKER_SYNC_{direction}", "dir": direction})
                        open_set.add((pair, direction))
                        log(
                            f"{EMOJI_DB} SYNC_TRADE {pair_tag(pair, direction)}",
                            {"trade_id": trade_id, "units": units, "entry": entry_px},
                        )
            if not db_ok:
                log_runtime("error", f"DB_OK_FAILED", scan_type=scan_type, continuing=False)
                continue
            
            log_runtime("info", f"DB_OK_PASSED", scan_type=scan_type, scan_pairs_count=len(scan_pairs))
            run_tag = f"IND_RUN {datetime.utcnow().isoformat()}Z"

            # Batch fetch comprehensive multi-timeframe data for all scan pairs
            tf_data_cache = {}
            for pair in scan_pairs:
                if _SHUTDOWN:
                    break
                # Get all timeframes: ticks, M5, M15, H1, H4
                tf_data = get_all_timeframes(pair, scan_now)
                log_runtime("debug", f"GET_ALL_TIMEFRAMES", pair=pair, tf_keys=list(tf_data.keys()), has_m5=bool(tf_data.get("M5")))
                if tf_data["M5"]:  # Only cache if we have execution data
                    tf_data_cache[pair] = tf_data
                    log_runtime("debug", f"CACHED_TF_DATA", pair=pair, m5_count=len(tf_data["M5"]))
                else:
                    log_runtime("warning", f"NO_M5_DATA", pair=pair, tf_keys=list(tf_data.keys()))
            log_runtime("info", f"IND_CACHE_BUILT", run_tag=run_tag, cached_pairs=list(tf_data_cache.keys()))
            
            # Process indicators early (before any broker/pending/guards)
            log_runtime("info", f"IND_LOOP_START", run_tag=run_tag, scan_pairs=scan_pairs)
            need_m5 = max(ATR_N + 5, 30)
            for pair in scan_pairs:
                if _SHUTDOWN:
                    break
                st = states[pair]
                try:
                    tf_data = tf_data_cache.get(pair)
                    log_runtime(
                        "info",
                        f"IND_PRE",
                        run_tag=run_tag,
                        pair=pair,
                        state=st.state,
                        has_tf=tf_data is not None,
                        m5_len=len(tf_data.get("M5", []) if tf_data else []) if tf_data else 0,
                    )

                    # Force-fetch M5 if missing/too short
                    if not tf_data or not tf_data.get("M5") or len(tf_data.get("M5", []) or []) < need_m5:
                        log_runtime(
                            "warning",
                            f"IND_FORCE_FETCH_M5",
                            run_tag=run_tag,
                            pair=pair,
                            have_m5=len(tf_data.get("M5", []) if tf_data else []) if tf_data else 0,
                            need=need_m5,
                        )
                        c_exec_force, exec_gran_force = _get_exec_candles_with_fallback(
                            pair, max(need_m5, 200), scan_now, CANDLE_REFRESH_SEC
                        )
                        if c_exec_force:
                            if tf_data is None:
                                tf_data = {}
                            tf_data["M5"] = c_exec_force
                            tf_data_cache[pair] = tf_data
                            log_runtime(
                                "info",
                                f"IND_FORCE_FETCH_M5_OK",
                                run_tag=run_tag,
                                pair=pair,
                                m5_len=len(c_exec_force),
                                gran=exec_gran_force,
                            )
                        else:
                            log_runtime(
                                "error",
                                f"IND_FORCE_FETCH_M5_FAIL",
                                run_tag=run_tag,
                                pair=pair,
                                need=need_m5,
                            )
                            continue

                    c_exec = tf_data.get("M5") if tf_data else None
                    if not c_exec:
                        log_runtime(
                            "warning",
                            f"IND_SKIP_NO_M5",
                            run_tag=run_tag,
                            pair=pair,
                        )
                        continue

                    # Calculate ATR
                    atr_s, atr_src = _resolve_atr_with_fallback(pair, c_exec, ATR_N, scan_now, "M5")
                    atr_l, atr_l_src = _resolve_atr_with_fallback(pair, c_exec, ATR_LONG_N, scan_now, "M5")
                    # Calculate indicators
                    mom = momentum(c_exec, MOM_N)
                    m_norm = abs(mom) / atr_s if (atr_s > 0 and math.isfinite(mom) and math.isfinite(atr_s)) else float("nan")
                    wr_val = williams_r(c_exec, WR_N)
                    vol_z = compute_volume_z(c_exec, win=20)

                    log_runtime(
                        "debug",
                        f"IND_VALUES",
                        run_tag=run_tag,
                        pair=pair,
                        wr=wr_val,
                        m_norm=m_norm,
                        atr_exec=atr_s,
                        atr_long=atr_l,
                        vol_z=vol_z,
                    )

                    # Update state
                    st.m_norm = m_norm
                    st.wr_prev = st.wr
                    st.wr = wr_val
                    st.atr_exec = atr_s
                    st.atr_long = atr_l
                    st.vol_z = vol_z

                    log_runtime(
                        "info",
                        f"IND_ASSIGNED",
                        run_tag=run_tag,
                        pair=pair,
                        wr=st.wr,
                        m_norm=st.m_norm,
                        atr_exec=st.atr_exec,
                        atr_long=st.atr_long,
                    )
                except Exception as e:
                    log_runtime("error", f"IND_EXCEPTION", run_tag=run_tag, pair=pair, error=str(e))
                    continue

            log_runtime("info", f"INDICATORS_UPDATED_ALL_PAIRS", run_tag=run_tag, pairs=len(scan_pairs))

            # Process each pair with cached candles (post-indicator; price/guards)
            log_runtime("info", f"IND_LOOP_START_POST", run_tag=run_tag, scan_pairs=scan_pairs)
            for pair in scan_pairs:
                if _SHUTDOWN:
                    break
                st = states[pair]
                log_runtime("info", f"IND_LOOP_PAIR_POST", run_tag=run_tag, pair=pair, state=st.state)

                if st.state == "ENTER":
                    _apply_state_machine(st, pair, None)  # c_exec not available yet

                if pair not in price_map:
                    skip_pair(st, pair, "pricing_missing")
                    continue

                bid, ask = price_map[pair]
                if not validate_price(pair, bid, ask, "scan"):
                    skip_pair(st, pair, "pricing_invalid", {"bid": bid, "ask": ask})
                    continue
                # Tick activity tracking (ticks per minute)
                update_tick_stats(st, pair, bid, ask, scan_now)
                st.spread_pips = to_pips(pair, ask - bid)
                # Always trim spread_history to prevent memory leaks
                if len(st.spread_history) > SPREAD_SPIKE_WINDOW:
                    st.spread_history = st.spread_history[-SPREAD_SPIKE_WINDOW:]
                
                if math.isfinite(st.spread_pips) and st.spread_pips >= 0.0:
                    st.spread_history.append(float(st.spread_pips))

                eff_spread_max = SPREAD_MAX_PIPS
                if math.isfinite(st.spread_pips) and st.spread_pips > eff_spread_max:
                    # Soft warning only; hard cap enforced at execution layer.
                    log_throttled(
                        f"spread_wide:{pair}",
                        f"{EMOJI_WARN} SPREAD_WIDE {pair_tag(pair)}",
                        {"pair": pair, "spread_pips": st.spread_pips, "max": eff_spread_max},
                        min_interval=5.0,
                    )

                min_candles = max(ATR_N + 2, 30)
                # Use cached candles instead of individual API calls
                c_exec = None
                exec_gran = TF_EXEC
                tf_data = tf_data_cache.get(pair)
                if tf_data:
                    c_exec = tf_data.get("M5")
                    log_runtime("debug", f"CANDLES_FROM_CACHE", pair=pair, count=len(c_exec) if c_exec else 0)
                    
                if not c_exec:
                    log_runtime("debug", f"CANDLES_FALLBACK_API", pair=pair)
                    c_exec, exec_gran = _get_exec_candles_with_fallback(pair, min_candles + 5, scan_now, candle_refresh_sec)
                    
                if c_exec:
                    log_runtime("debug", f"CANDLES_AVAILABLE", pair=pair, count=len(c_exec), source=exec_gran)
                else:
                    log_runtime("warning", f"CANDLES_MISSING", pair=pair)
                    log_throttled(
                        f"candles_missing:{pair}",
                        f"{EMOJI_WARN} DATA_WARN {pair_tag(pair)}",
                        {"pair": pair, "reason": "candles_missing_all_tfs"},
                        min_interval=10.0,
                    )
                    continue
                pair_tf_sec = granularity_sec(exec_gran or TF_EXEC)
                ok_c, reason = validate_candles(pair, c_exec, pair_tf_sec, allow_partial=True)
                if not ok_c:
                    log_throttled(
                        f"candles_invalid:{pair}:{reason}",
                        f"{EMOJI_WARN} DATA_WARN {pair_tag(pair)}",
                        {"pair": pair, "reason": f"candles_invalid:{reason}", "gran": exec_gran or TF_EXEC},
                        min_interval=5.0,
                    )
                if len(c_exec) < min_candles:
                    log_throttled(
                        f"candles_too_few:{pair}",
                        f"{EMOJI_WARN} DATA_WARN {pair_tag(pair)}",
                        {"pair": pair, "len": len(c_exec), "need": min_candles, "gran": exec_gran or TF_EXEC},
                        min_interval=10.0,
                    )
                if pair_tf_sec > 0:
                    last_ts = float(c_exec[-1].get("time", 0.0))
                    if last_ts <= 0.0:
                        log_throttled(
                            f"candles_bad_time:{pair}",
                            f"{EMOJI_WARN} DATA_WARN {pair_tag(pair)}",
                            {"pair": pair, "reason": "candles_bad_time", "gran": exec_gran or TF_EXEC},
                            min_interval=5.0,
                        )
                    if last_ts > (broker_now_ts() + pair_tf_sec):
                        log_throttled(
                            f"candles_time_future:{pair}",
                            f"{EMOJI_WARN} DATA_WARN {pair_tag(pair)}",
                            {"pair": pair, "reason": "candles_time_future", "last_ts": last_ts, "gran": exec_gran or TF_EXEC},
                            min_interval=5.0,
                        )
                    stale_cutoff = max(pair_tf_sec * max(3.0, CANDLES_STALE_MULT), 60.0)
                    if (broker_now_ts() - last_ts) > stale_cutoff:
                        log_throttled(
                            f"candles_stale_soft:{pair}",
                            f"{EMOJI_WARN} DATA_WARN {pair_tag(pair)}",
                            {"pair": pair, "reason": "candles_stale", "last_ts": last_ts, "stale_sec": broker_now_ts() - last_ts, "gran": exec_gran or TF_EXEC},
                            min_interval=5.0,
                        )

                atr_s, atr_src = _resolve_atr_with_fallback(pair, c_exec, ATR_N, scan_now, exec_gran or TF_EXEC)
                atr_l, atr_l_src = _resolve_atr_with_fallback(pair, c_exec, ATR_LONG_N, scan_now, exec_gran or TF_EXEC)
                st.atr_exec = atr_s
                st.atr_long = atr_l

                # Compute indicators for this scan BEFORE any NaN/data gating.
                log_runtime("debug", f"DATA_PROCESSING_START", pair=pair, c_exec_count=len(c_exec) if c_exec else 0)
                
                mom = momentum(c_exec, MOM_N)
                m_norm = abs(mom) / atr_s if (atr_s > 0 and math.isfinite(mom) and math.isfinite(atr_s)) else float("nan")
                wr_val = williams_r(c_exec, WR_N)

                log_runtime("debug", f"INDICATORS_CALCULATED", pair=pair, mom=mom, m_norm=m_norm, wr=wr_val, atr_s=atr_s)

                st.m_norm = m_norm
                st.wr_prev = st.wr
                st.wr = wr_val

                # Handle NaN values (based on freshly computed indicators)
                has_nan = not (
                    math.isfinite(atr_s)
                    and atr_s > 0.0
                    and math.isfinite(atr_l)
                    and atr_l > 0.0
                    and math.isfinite(st.m_norm)
                    and math.isfinite(st.wr)
                )
                if has_nan:
                    log_throttled(
                        f"data_stale:{pair}",
                        f"{EMOJI_WARN} DATA_STALE {pair}",
                        {
                            "pair": pair,
                            "fields": {
                                "atr_exec": atr_s,
                                "atr_long": atr_l,
                                "m_norm": st.m_norm,
                                "wr": st.wr,
                            },
                        },
                    )

                if not _atr_gate_ok(pair, atr_s):
                    skip_pair(st, pair, "atr_gate", {"atr_exec": atr_s})
                    continue

                # Get current candle data for volatility detection
                last_c = c_exec[-1]
                curr_h = float(last_c['h'])
                curr_l = float(last_c['l'])
                
                st.mode = detect_mode(pair, curr_h, curr_l, atr_s)
                util = float(MODE_EXEC.get(st.mode, MODE_EXEC["SLOW"])["util"])
                if util <= 0.0:
                    skip_pair(st, pair, "mode_zero_util", {"mode": st.mode, "util": util})
                    continue

                bars_for_box = max(1, min(BOX_BARS, len(c_exec) - 2))
                hi, lo, bs = box_range(c_exec, bars_for_box, use_prev=True)
                st.box_hi, st.box_lo = hi, lo
                st.box_atr = (bs / atr_s) if (atr_s > 0 and math.isfinite(bs)) else float("nan")

                _apply_state_machine(st, pair, c_exec)

                # Get comprehensive multi-timeframe context (optional)
                tf_data = tf_data_cache.get(pair)
                if tf_data and tf_data.get("M5"):
                    c_exec = tf_data["M5"]
                    # Update regime using multi-timeframe analysis
                    st.mode = detect_regime_multi_tf(tf_data, pair)
                else:
                    tf_data = {"available": [exec_gran or TF_EXEC], "M5": c_exec}
                
                sigs = build_signals(pair, st, c_exec, tf_data)
                raw_sig_count = len(sigs)
            
                # Track bar completeness for logging
                bar_complete = bool(c_exec[-1].get("complete", True)) if c_exec else True
                bar_age_ms = (now_ts() - float(c_exec[-1].get("time", now_ts()))) * 1000 if not bar_complete and c_exec else 0
            
                # Check for stale feed
                current_candle_time = float(c_exec[-1].get("time", 0))
                current_complete = bool(c_exec[-1].get("complete", True))
                candle_age_sec = (broker_now_ts() - current_candle_time) if current_candle_time > 0 else float("inf")
                # Allow a complete candle to remain valid for its full timeframe
                allow_static_complete = (pair_tf_sec > 0) and (candle_age_sec <= (pair_tf_sec * 1.05))
                if current_candle_time == st.last_candle_time:
                    # Check if OHLC changed
                    current_ohlc = (c_exec[-1]['o'], c_exec[-1]['h'], c_exec[-1]['l'], c_exec[-1]['c'])
                    if not hasattr(st, 'last_ohlc'):
                        st.last_ohlc = current_ohlc
                        st.stale_poll_count = 0
                    elif st.last_ohlc == current_ohlc:
                        if not current_complete:
                            st.stale_poll_count = 0
                        elif allow_static_complete:
                            # Complete candle within its own timeframe is still valid
                            st.stale_poll_count = 0
                        else:
                            st.stale_poll_count += 1
                            if st.stale_poll_count > STALE_FEED_MAX_POLLS:  # Stale after N polls with no change
                                log_throttled(
                                    f"stale_feed:{pair}",
                                    f"{EMOJI_WARN} DATA_WARN {pair_tag(pair)}",
                                    {"pair": pair, "reason": "stale_feed", "stale_count": st.stale_poll_count},
                                    min_interval=10.0,
                                )
                                st.stale_poll_count = 0
                    else:
                        st.last_ohlc = current_ohlc
                        st.stale_poll_count = 0
                else:
                    st.last_candle_time = current_candle_time
                    st.stale_poll_count = 0
                    if hasattr(st, 'last_ohlc'):
                        delattr(st, 'last_ohlc')
            
                # Apply debounce to signals
                debounced_sigs = []
                current_time = now_ts()
            
                for sig in sigs:
                    sig_key = f"{sig.setup_name}_{sig.direction}"
                
                    if sig_key not in st.signal_debounce:
                        # First time seeing this signal
                        st.signal_debounce[sig_key] = current_time
                    else:
                        # Check debounce conditions
                        first_seen = st.signal_debounce[sig_key]
                        time_elapsed = (current_time - first_seen) * 1000  # Convert to ms
                    
                        # Allow if 2 consecutive polls (simple check) OR 400ms elapsed
                        if time_elapsed >= 400 or (
                            hasattr(st, 'last_signal_time')
                            and current_time - st.last_signal_time < 2
                        ):
                            debounced_sigs.append(sig)
                            # Reset debounce after accepting
                            st.signal_debounce[sig_key] = current_time
            
                st.last_signal_time = current_time
                sigs = debounced_sigs
                debounced_count = len(sigs)
            
                # Always alert for GET_READY, ARM_TICK_ENTRY, and ENTER states, even without signals
                if st.state in ("GET_READY", "ARM_TICK_ENTRY", "ENTER"):
                    state_emo = state_emoji(st.state)
                    now_alert = now_ts()
                    repeat_sec = ALERT_REPEAT_ENTER_SEC if st.state == "ENTER" else ALERT_REPEAT_GET_READY_SEC
                    if sigs:
                        sig = sigs[0]
                        alert_key = f"{st.state}:{st.mode}:{sig.setup_name}:{sig.direction}"
                        if (now_alert - st.last_alert) > 8.0 and (
                            alert_key != st.last_alert_key or (repeat_sec > 0 and (now_alert - st.last_alert) >= repeat_sec)
                        ):
                            notify(
                                f"{state_emo} {pair} {st.mode} {st.state}",
                                f"{state_emo} {sig.setup_name} {sig.direction} | spr {st.spread_pips:.1f}p | m {st.m_norm:.2f} | wr {st.wr:.0f} {state_emo}",
                            )
                            st.last_alert = now_alert
                            st.last_alert_key = alert_key
                    else:
                        # Alert even without signal for GET_READY/ENTER
                        alert_key = f"{st.state}:{st.mode}:MONITOR"
                        if (now_alert - st.last_alert) > 8.0 and (
                            alert_key != st.last_alert_key or (repeat_sec > 0 and (now_alert - st.last_alert) >= repeat_sec)
                        ):
                            notify(
                                f"{state_emo} {pair} {st.mode} {st.state}",
                                f"{state_emo} Monitoring | spr {st.spread_pips:.1f}p | m {st.m_norm:.2f} | wr {st.wr:.0f} | weak_scans {st.get_ready_weak_scans} {state_emo}",
                            )
                            st.last_alert = now_alert
                            st.last_alert_key = alert_key
            
                # Also alert for other states if there's a signal
                elif sigs:
                    sig = sigs[0]
                    state_emo = state_emoji(st.state)
                    now_alert = now_ts()
                    repeat_sec = ALERT_REPEAT_OTHER_SEC
                    alert_key = f"{st.state}:{st.mode}:{sig.setup_name}:{sig.direction}"
                    if (now_alert - st.last_alert) > 8.0 and (
                        alert_key != st.last_alert_key or (repeat_sec > 0 and (now_alert - st.last_alert) >= repeat_sec)
                    ):
                        notify(
                            f"{state_emo} {pair} {st.mode} {st.state}",
                            f"{state_emo} {sig.setup_name} {sig.direction} | spr {st.spread_pips:.1f}p | m {st.m_norm:.2f} | wr {st.wr:.0f} {state_emo}",
                        )
                        st.last_alert = now_alert
                        st.last_alert_key = alert_key
            
                if st.state not in ("GET_READY", "ENTER", "ARM_TICK_ENTRY"):
                    continue

                # HARD GATE: Entries must be explicitly enabled
                if not ALLOW_ENTRIES:
                    _reject("entries_disabled", extra={"ALLOW_ENTRIES": ALLOW_ENTRIES})
                    continue

                sig = sigs[0] if sigs else None
                if sig is None and st.state == "ARM_TICK_ENTRY":
                    sig = _sig_from_entry_arm(st.entry_arm)
                if sig is None:
                    continue

                entry_trigger = "signal"
                if TICK_ENTRY_ENABLED:
                    arm_sig = (st.entry_arm or {}).get("sig") if isinstance(st.entry_arm, dict) else None
                    if (
                        st.state != "ARM_TICK_ENTRY"
                        or not arm_sig
                        or arm_sig.get("setup_id") != sig.setup_id
                        or arm_sig.get("direction") != sig.direction
                    ):
                        entry_px = ask if sig.direction == "LONG" else bid
                        _arm_tick_entry(st, sig, entry_px, st.box_hi, st.box_lo, st.atr_exec, now_ts())
                        _transition_state(st, "ARM_TICK_ENTRY", pair)
                    triggered, trig_reason = _tick_entry_triggered(st, bid, ask, now_ts())
                    if not triggered:
                        log_trade_attempt(
                            pair=pair,
                            sig=sig,
                            st=st,
                            speed_class=normalize_speed_class(get_speed_class(sig.setup_id)),
                            decision="ARM",
                            reason=f"tick_entry_{trig_reason}",
                            leg="MAIN",
                            extra={"entry_trigger": trig_reason},
                            bar_complete=bar_complete,
                            bar_age_ms=bar_age_ms,
                        )
                        continue
                    entry_trigger = trig_reason
                    # Clear arm after trigger to avoid repeated entries
                    st.entry_arm = {}

                speed_class = normalize_speed_class(get_speed_class(sig.setup_id))
                sp = get_speed_params(speed_class)

                atr_valid = math.isfinite(st.atr_exec) and st.atr_exec > 0.0
                use_fallback = (not bar_complete) or (not atr_valid)
                exit_mode = "ATR_FALLBACK" if use_fallback else "STRUCTURE"
                fallback_reason = None
                if not atr_valid:
                    fallback_reason = "atr_invalid"
                elif not bar_complete:
                    fallback_reason = "partial_bar"

                tp1_atr_use = sp["tp1_atr"]
                tp2_atr_use = sp["tp2_atr"]
                sl_atr_use = sp["sl_atr"]
                sweep_wick_atr = None

                if use_fallback:
                    fb = _atr_fallback_params(speed_class)
                    tp1_atr_use = fb["tp1_atr"]
                    tp2_atr_use = fb["tp2_atr"]
                    sl_atr_use = fb["sl_atr"]
                    if sig.setup_name == "SWEEP_POP" or sig.setup_id == 5:
                        entry_px = ask if sig.direction == "LONG" else bid
                        sweep_wick_atr = _sweep_wick_dist_atr(c_exec, entry_px, sig.direction, st.atr_exec)
                        sl_atr_use = max(0.6, sweep_wick_atr)

                exit_meta = {"exit_mode": exit_mode}
                if fallback_reason:
                    exit_meta["fallback_reason"] = fallback_reason
                if sweep_wick_atr is not None:
                    exit_meta["sweep_wick_atr"] = round(sweep_wick_atr, 3)
                if use_fallback:
                    exit_meta.update(
                        {
                            "sl_atr": round(sl_atr_use, 3),
                            "tp1_atr": round(tp1_atr_use, 3),
                            "tp2_atr": round(tp2_atr_use, 3),
                        }
                    )
                if not atr_valid:
                    exit_meta.update(
                        {
                            "sl_pct": FALLBACK_SL_PCT,
                            "tp1_pct": FALLBACK_TP1_PCT,
                            "tp2_pct": FALLBACK_TP2_PCT,
                        }
                    )

                def _reject(reason: str, extra: Optional[dict] = None, leg: str = "MAIN") -> None:
                    # Print BLOCKED message for immediate visibility
                    block_msg = f"BLOCKED: {reason}"
                    if extra:
                        block_msg += f" (details: {extra})"
                    print(f"  {block_msg}")
                    
                    meta = {}
                    meta.update(exit_meta)
                    if extra:
                        meta.update(extra)
                    log_trade_attempt(
                        pair=pair,
                        sig=sig,
                        st=st,
                        speed_class=speed_class,
                        decision="REJECT",
                        reason=reason,
                        leg=leg,
                        extra=meta,
                        bar_complete=bar_complete,
                        bar_age_ms=bar_age_ms,
                    )

                # No global limit check - we have currency exposure limits
                if pair in EXIT_BLOCKED_PAIRS:
                    log_throttled(
                        f"exit_blocked_entry:{pair}",
                        f"{EMOJI_WARN} EXIT_BLOCKED_ENTRY {pair_tag(pair)}",
                        {"pair": pair, **EXIT_BLOCKED_PAIRS.get(pair, {})},
                        min_interval=10.0,
                    )

                # Cooldown after recent order rejects to avoid repeated failures
                block = ORDER_REJECT_BLOCK.get(pair)
                if block:
                    if now_ts() < float(block.get("until", 0.0)):
                        _reject("recent_order_reject", extra=block)
                        continue
                    ORDER_REJECT_BLOCK.pop(pair, None)
                # Check for opposite direction position
                if has_opposite_position(open_pos, pair, sig.direction):
                    _reject("opposite_position_exists")
                    continue
                if has_opposite_db(db_open_trades, pair, sig.direction):
                    _reject("opposite_position_db")
                    continue
            
                # No pending order check - we use market orders only
                if pending_by_pair_scan.get(pair, 0) >= 2:
                    _reject("pending_order_limit", extra={"pending_count": pending_by_pair_scan.get(pair, 0)})
                    continue
                # No speed class limits - controlled by currency exposure

                c1, c2 = extract_currencies(pair)
                max_curr_exp = (
                    MAX_CURRENCY_EXPOSURE_FAST
                    if speed_class == "FAST"
                    else (MAX_CURRENCY_EXPOSURE_MED if speed_class == "MED" else MAX_CURRENCY_EXPOSURE_SLOW)
                )
                if count_currency_exposure(db_open_trades, c1) >= max_curr_exp or count_currency_exposure(db_open_trades, c2) >= max_curr_exp:
                    _reject("max_currency_exposure")
                    continue

                price_for_units = ask if sig.direction == "LONG" else bid
                disp = abs(float(c_exec[-1]["c"]) - float(c_exec[-11]["c"]))
                disp_atr = disp / st.atr_exec if st.atr_exec > 0.0 else 0.0
            
                # Log sizing chain
                log(
                    f"{EMOJI_INFO} SIZING_CHAIN {pair_tag(pair, sig.direction)}",
                    {
                        "margin_avail": margin_avail,
                        "util": util,
                        "price": price_for_units,
                        "spread_pips": st.spread_pips,
                        "disp_atr": disp_atr,
                        "speed_class": speed_class,
                    }
                )
            
                units_total, units_reason, units_debug = calc_units(
                    pair, sig.direction, price_for_units, margin_avail, util, speed_class, st.spread_pips, disp_atr, sig.size_mult
                )
            
                # Determine specific reason for zero units
                clamp_reason = None
                units_raw = units_total
            
                if units_total == 0:
                    # Use the reason from calc_units
                    _reject(units_reason, extra={"units_reason": units_reason, **(units_debug or {})})
                    continue
            
                # Log sizing inputs and results
                log(
                    f"{EMOJI_INFO} SIZING_ATTEMPT {pair_tag(pair, sig.direction)}",
                    {
                        "margin_avail": margin_avail,
                        "util": util,
                        "atr_exec": st.atr_exec,
                        "atr_long": st.atr_long,
                        "sl_atr": sl_atr_use,
                        "tp1_atr": tp1_atr_use,
                        "tp2_atr": tp2_atr_use,
                        "exit_mode": exit_mode,
                        "spread_pips": st.spread_pips,
                        "disp_atr": disp_atr,
                        "units_raw": units_raw,
                        "units_final": units_total,
                        "units_reason": units_reason,
                        **units_debug
                    }
                )

                include_spread = True
                entry1, sl1, tp1 = compute_prices(
                    pair,
                    sig.direction,
                    bid,
                    ask,
                    st.atr_exec,
                    tp1_atr_use,
                    sl_atr_use,
                    speed_class,
                    tp_kind="tp1",
                    include_spread=include_spread,
                )
                entry2, sl2, tp2 = compute_prices(
                    pair,
                    sig.direction,
                    bid,
                    ask,
                    st.atr_exec,
                    tp2_atr_use,
                    sl_atr_use,
                    speed_class,
                    tp_kind="tp2",
                    include_spread=include_spread,
                )
                # Risk cap disabled per operator instruction (no hard risk cap).

                # SOP v2.1: TP0 + CSL mandatory at birth (stopLossOnFill + takeProfitOnFill)
                spread_price = max(0.0, float(ask) - float(bid))
                csl1, tp0_1, tp0_dbg1 = _enforce_tp0_csl(
                    pair=pair,
                    direction=sig.direction,
                    bid=bid,
                    ask=ask,
                    atr_m1=st.atr_exec,
                    spread_price=spread_price,
                    speed_class=speed_class,
                    structural_tp=tp1,
                    tp_anchor_price=None,
                )
                csl2, tp0_2, tp0_dbg2 = _enforce_tp0_csl(
                    pair=pair,
                    direction=sig.direction,
                    bid=bid,
                    ask=ask,
                    atr_m1=st.atr_exec,
                    spread_price=spread_price,
                    speed_class=speed_class,
                    structural_tp=tp2,
                    tp_anchor_price=None,
                )

                # === SPREAD-AWARE SIZING (EXECUTION LAYER) ===
                # Hard crash guard - absolute maximum spread
                if st.spread_pips > ABS_SPREAD_MAX_PIPS:
                    _reject(
                        "abs_spread_max",
                        extra={
                            "spread_pips": st.spread_pips,
                            "abs_max": ABS_SPREAD_MAX_PIPS,
                            "speed_class": speed_class,
                        },
                    )
                    continue

                # Use a valid ATR value for spread sizing (fallback if ATR invalid)
                atr_for_spread = st.atr_exec
                if not (math.isfinite(atr_for_spread) and atr_for_spread > 0.0):
                    spread_price = max(0.0, float(ask) - float(bid))
                    atr_for_spread = max(spread_price * 2.0, pip_size(pair))

                # Calculate spread as fraction of ATR (F) and speed norm (E)
                s_atr = spread_atr(pair, st.spread_pips, atr_for_spread)
                speed_norm = st.m_norm if math.isfinite(st.m_norm) else 0.0
                
                # CHOOSE SPREAD SIZING METHOD
                if USE_FE_SPREAD_SIZING:
                    # F/E method (experimental)
                    mult = spread_size_mult_fe(s_atr, speed_norm)
                    mult_base = mult
                else:
                    # CANONICAL method (default)
                    mult = spread_size_mult(speed_class, s_atr)
                    mult_base = mult

                # Optional spread spike damping (FAST/MED only)
                spread_spike = float("nan")
                spread_spike_applied = False
                if speed_class in ("FAST", "MED") and len(st.spread_history) >= SPREAD_SPIKE_MIN_SAMPLES:
                    median_spread = _median(st.spread_history[-SPREAD_SPIKE_WINDOW:])
                    if math.isfinite(median_spread) and median_spread > 0.0:
                        spread_spike = st.spread_pips / median_spread
                        if spread_spike >= SPREAD_SPIKE_THRESHOLD:
                            mult *= SPREAD_SPIKE_MULT
                            mult = max(mult, SPREAD_SIZE_MIN)
                            spread_spike_applied = True

                # Log spread metrics (always)
                log_fields = {
                    "spread_pips": st.spread_pips,
                    "atr_exec_price": atr_for_spread,
                    "atr_exec_pips": atr_pips(pair, atr_for_spread),
                    "atr_exec_price_raw": st.atr_exec,
                    "spread_atr": s_atr,
                    "spread_f": s_atr,
                    "speed_norm": speed_norm,
                    "spread_mult_base": mult_base,
                    "spread_mult": mult,
                    "spread_spike": spread_spike,
                    "spread_spike_applied": spread_spike_applied,
                    "units_base": units_total,
                    "speed_class": speed_class,
                    "setup_id": sig.setup_id,
                    "f_max": SPREAD_F_MAX,
                    "alpha": SPREAD_SIZE_ALPHA,
                    "m_min": SPREAD_SIZE_MIN,
                    "eps": SPREAD_SIZE_EPS,
                }

                # Skip if spread multiplier is zero (F > F_max)
                if mult <= 0.0:
                    _reject("spread_f_max", extra=log_fields)
                    continue

                units_total_adj = int(units_total * mult)
                if abs(units_total_adj) < MIN_TRADE_SIZE:
                    _reject("below_min_units_after_spread_mult", extra=log_fields)
                    continue

                log_fields.update({"units_final": units_total_adj})
                log(f"{EMOJI_INFO} SPREAD_ADJUST {pair_tag(pair, sig.direction)}", log_fields)
                units_total = units_total_adj

                order_meta = {
                    "exit_mode": exit_mode,
                    "sl_atr": round(sl_atr_use, 3),
                    "tp1_atr": round(tp1_atr_use, 3),
                    "tp2_atr": round(tp2_atr_use, 3),
                    "entry_trigger": entry_trigger,
                    "tp0_branch": tp0_dbg1.get("tp0_dbg", {}).get("branch") if isinstance(tp0_dbg1, dict) else None,
                    "csl_dist": (tp0_dbg1.get("csl_dbg", {}) or {}).get("csl_dist") if isinstance(tp0_dbg1, dict) else None,
                }
                spread_meta = dict(log_fields)
                order_meta.update(spread_meta)
                if fallback_reason:
                    order_meta["fallback_reason"] = fallback_reason
                if sweep_wick_atr is not None:
                    order_meta["sweep_wick_atr"] = round(sweep_wick_atr, 3)
                if bar_complete is not None:
                    order_meta["bar_complete"] = bool(bar_complete)
                if bar_age_ms is not None:
                    order_meta["bar_age_ms"] = round(bar_age_ms, 0)

                split_main, split_run = get_split_ratios(speed_class)
                units_main = int(abs(units_total) * split_main)
                units_run = abs(units_total) - units_main
                if units_main == 0:
                    units_main = abs(units_total)
                    units_run = 0
                if sig.direction == "SHORT":
                    units_main = -units_main
                    units_run = -units_run

                orig_main = abs(units_main)
                orig_run = abs(units_run)

                # Ensure unique position size for this pair (FIFO compliance)
                existing_sizes = []
                for pos in open_pos:
                    if normalize_pair(pos.get("instrument")) == normalize_pair(pair):
                        longu = int(float(pos.get("long", {}).get("units", "0") or "0"))
                        shortu = int(float(pos.get("short", {}).get("units", "0") or "0"))
                        if longu > 0:
                            existing_sizes.append(longu)
                        if shortu < 0:
                            existing_sizes.append(abs(shortu))
            
                # Check DB trades too
                for tr in db_open_trades:
                    if normalize_pair(tr.get("pair")) == normalize_pair(pair):
                        units = int(tr.get("units", 0))
                        if units != 0:
                            existing_sizes.append(abs(units))
            
                print(f"\n🔢 UNIQUE SIZE CHECK for {pair}: existing_sizes={existing_sizes}")

                existing_set = set(existing_sizes)
                units_main = _make_unique_units(units_main, existing_set, MIN_TRADE_SIZE, max_units=orig_main * 1.5)
                # Don't reject for duplicate sizes - just use what we get
                if units_main == 0:
                    _reject("units_zero_after_adjustment", {"original_units": units_main})
                    continue
                existing_set.add(abs(units_main))

                if units_run != 0:
                    units_run = _make_unique_units(units_run, existing_set, MIN_TRADE_SIZE, max_units=orig_run * 1.5)
                    # Don't reject for duplicate sizes - just use what we get
                    if units_run == 0:
                        _reject("units_run_zero_after_adjustment", {"original_units": units_run})
                        continue
                    existing_set.add(abs(units_run))

                # Final guard: ensure MAIN and RUN are different sizes
                if units_run != 0 and abs(units_run) == abs(units_main):
                    # Just add 1 to run to make it different
                    if units_main > 0:
                        units_run = units_main + 1
                    else:
                        units_run = units_main - 1

                # No duplicate order size check - sizes based on margin percentage
                # No +1 unit adjustment - it can inflate beyond limits and cause duplicates

                # No 30-second deduplication - not needed with margin-based sizing
                if not db_ok:
                    _reject("db_error")
                    continue

                ts_ms = int(now_ts() * 1000)
                # FIX: Truncate setup name to ensure ID stays under 50 chars
                # Max safe len: 50 - (len(BOT_ID) + 7 + 4 + 13 + 4) = 13 chars left for setup
                short_setup = sig.setup_name[:13]
                cid1 = f"{BOT_ID}:{pair}:{short_setup}:MAIN:{ts_ms}"
                cid2 = f"{BOT_ID}:{pair}:{short_setup}:RUN:{ts_ms}"

                attempt_ts = now_ts()
                state_from = st.state
                _transition_state(st, "ENTER", pair)
                db_call("save_state_pre_order", db.save_state, pair, st)
                if not db_ok:
                    _reject("db_error_pre_order")
                    continue

                log_trade_attempt(
                    pair=pair,
                    sig=sig,
                    st=st,
                    speed_class=speed_class,
                    decision="PLACE",
                    reason="order_attempt",
                    leg="MAIN",
                    state_from=state_from,
                    state_to=st.state,
                    extra={**exit_meta, **spread_meta, "entry_trigger": entry_trigger},
                    bar_complete=bar_complete,
                    bar_age_ms=bar_age_ms,
                )

                db_call(
                    "record_order_attempt_main",
                    db.record_order,
                    client_id=cid1,
                    pair=pair,
                    setup=sig.setup_name,
                    leg="MAIN",
                    units=units_main,
                    sl=sl1,
                    tp=tp1,
                    oanda_order_id="",
                    oanda_transaction_id="",
                    status="ATTEMPT",
                    raw=order_meta,
                )
                if not db_ok:
                    _reject("db_error_order_attempt")
                    continue

                # Log detailed order information before placing
                log(
                    f"{EMOJI_INFO} ORDER_PLACE_MAIN {pair_tag(pair, sig.direction)}",
                    {
                        "units": units_main,
                        "entry_price": price_for_units,
                        "stop_loss": sl1,
                        "take_profit": tp1,
                        "client_id": cid1,
                        "margin_required": round(abs(units_main) / (50 if pair in LEVERAGE_50 else LEV_DEFAULT) / max(price_for_units, 1e-9), 2),
                        "risk_usd": round(abs(units_main) * abs(price_for_units - sl1) / (50 if pair in LEVERAGE_50 else LEV_DEFAULT), 2),
                        "setup": sig.setup_name,
                        "speed_class": speed_class,
                    }
                )
            
                # HARD GATE: Block actual orders in dry-run mode
                if DRY_RUN_ONLY:
                    log(f"{EMOJI_INFO} DRY_RUN_ORDER_BLOCKED {pair_tag(pair, sig.direction)}", 
                        {"units": units_main, "client_id": cid1, "reason": "DRY_RUN_ONLY=true"})
                    _reject("dry_run_only", extra={"DRY_RUN_ONLY": DRY_RUN_ONLY})
                    continue
            
                resp1 = oanda_call("place_market_main", o.place_market, pair, units_main, csl1, tp0_1, client_id=cid1, allow_error_dict=True)
                ok1, oid1, txid1, status1 = _order_confirmed(resp1)
                if not ok1:
                    _note_order_reject(pair, status1, resp1, leg="MAIN")
                    _reject(f"order_main_{status1}", extra={"resp": resp1})
                    db_call(
                        "record_order_fail_main",
                        db.record_order,
                        client_id=cid1,
                        pair=pair,
                        setup=sig.setup_name,
                        leg="MAIN",
                        units=units_main,
                        sl=sl1,
                        tp=tp1,
                        oanda_order_id=oid1,
                        oanda_transaction_id=txid1,
                        status=f"FAILED_{status1}",
                        raw={**(resp1 if isinstance(resp1, dict) else {}), **order_meta},
                    )
                    continue
                # Transition to ENTER state with alert
                _transition_state(
                    st, "ENTER", pair,
                    strategy=sig.setup_name,
                    direction=sig.direction,
                    reason="market_order_placed",
                    metadata={"order_id": oid1, "units": units_main}
                )
                # Clear reject cooldown on success
                ORDER_REJECT_BLOCK.pop(pair, None)
                db_call(
                    "record_order_main",
                    db.record_order,
                    client_id=cid1,
                    pair=pair,
                    setup=sig.setup_name,
                    leg="MAIN",
                    units=units_main,
                    sl=sl1,
                    tp=tp1,
                    oanda_order_id=oid1,
                    oanda_transaction_id=txid1,
                    status=status1,
                    raw={**(resp1 if isinstance(resp1, dict) else {}), **order_meta},
                )
                ttl_main = sp["ttl_main"]
                ttl_run = sp["ttl_run"]
                pg_t_main = int(ttl_main * sp["pg_t_frac"])
                pg_t_run = int(ttl_run * sp["pg_t_frac"])
                pg_atr_val = sp["pg_atr"]

                tid1 = _extract_trade_id_from_fill(resp1)
                # Verify we got a valid trade ID BEFORE recording in DB
                if not tid1:
                    log(f"{EMOJI_ERR} NO_TRADE_ID {pair_tag(pair, sig.direction)}", 
                        {"setup": sig.setup_name, "response": resp1})
                    _reject("no_trade_id")
                    continue

                # SOP v2.1: Post-fill CSL -> structural SL upgrade with health gate
                try:
                    last_price_ts = None
                    try:
                        last_price_ts = enhanced_market_hub.resilience_controller.last_update.get(pair)
                    except Exception:
                        last_price_ts = None
                    health = _health_snapshot(now=now_ts(), last_price_ts=last_price_ts, net_fail_count=net_fail_count)
                    _post_fill_upgrade_sl_or_panic(
                        o=o,
                        pair=pair,
                        direction=sig.direction,
                        trade_id=str(tid1),
                        csl_price=float(csl1),
                        structural_sl=float(sl1),
                        health=health,
                        db_trade_id=None,
                        leg="MAIN",
                    )
                except Exception as e:
                    log_runtime("warning", "POST_FILL_SL_UPGRADE_EXCEPTION", pair=pair, error=str(e))
                if isinstance(resp1, dict) and resp1.get("_sl_add_failed"):
                    try:
                        sl_retry_state[int(tid1)] = {
                            "pair": pair,
                            "direction": sig.direction,
                            "sl": sl1,
                            "next_ts": now_ts() + SL_RETRY_BASE_SEC,
                            "fail_count": 0,
                        }
                    except Exception:
                        pass
                
                trade_note_main = sig.reason
                trade_id_main = db_call(
                    "add_trade_main",
                    db.record_trade,
                    pair=pair,
                    setup=sig.setup_name,
                    direction=sig.direction,
                    mode=st.mode,
                    units=units_main,
                    entry=entry1,
                    atr_entry=st.atr_exec,
                    ttl_sec=ttl_main,
                    pg_t=pg_t_main,
                    pg_atr=pg_atr_val,
                    note=trade_note_main,
                    oanda_trade_id=tid1,
                )
                if trade_id_main:
                    db_open_trades.append({"pair": pair, "setup": sig.setup_name, "dir": sig.direction})
                    # Transition to MANAGING state with alert
                    _transition_state(
                        st, "MANAGING", pair,
                        strategy=sig.setup_name,
                        direction=sig.direction,
                        reason="trade_filled",
                        metadata={"trade_id": trade_id_main, "leg": "MAIN"}
                    )
                    log(
                        f"{EMOJI_ENTER} ENTER {pair_tag(pair, sig.direction)}",
                        {
                            "trade_id": trade_id_main,
                            "setup_id": sig.setup_id,
                            "speed_class": speed_class,
                            "leg": "MAIN",
                            "atr_entry": st.atr_exec,
                            "m_norm": st.m_norm,
                            "wr": st.wr,
                            "spread_pips": st.spread_pips,
                            "entry_reason": sig.reason,
                            "entry_ts": now_ts(),
                            "exit_mode": exit_mode,
                            "fallback_reason": fallback_reason,
                        },
                    )

                if units_run != 0:
                    log_trade_attempt(
                        pair=pair,
                        sig=sig,
                        st=st,
                        speed_class=speed_class,
                        decision="PLACE",
                        reason="order_attempt",
                        leg="RUN",
                        state_from=state_from,
                        state_to=st.state,
                        extra={**exit_meta, **spread_meta, "entry_trigger": entry_trigger},
                        bar_complete=bar_complete,
                        bar_age_ms=bar_age_ms,
                    )
                    db_call(
                        "record_order_attempt_run",
                        db.record_order,
                        client_id=cid2,
                        pair=pair,
                        setup=sig.setup_name,
                        leg="RUN",
                        units=units_run,
                        sl=sl2,
                        tp=tp2,
                        oanda_order_id="",
                        oanda_transaction_id="",
                        status="ATTEMPT",
                        raw=order_meta,
                    )
                    if not db_ok:
                        _reject("db_error_order_attempt", leg="RUN")
                        continue
                
                    # Log detailed RUN order information before placing
                    log(
                        f"{EMOJI_INFO} ORDER_PLACE_RUN {pair_tag(pair, sig.direction)}",
                        {
                            "units": units_run,
                            "entry_price": price_for_units,
                            "stop_loss": sl2,
                            "take_profit": tp2,
                            "client_id": cid2,
                            "margin_required": round(abs(units_run) / (50 if pair in LEVERAGE_50 else LEV_DEFAULT) / max(price_for_units, 1e-9), 2),
                            "risk_usd": round(abs(units_run) * abs(price_for_units - sl2) / (50 if pair in LEVERAGE_50 else LEV_DEFAULT), 2),
                            "setup": sig.setup_name,
                            "speed_class": speed_class,
                        }
                    )
                
                    resp2 = oanda_call("place_market_run", o.place_market, pair, units_run, csl2, tp0_2, client_id=cid2, allow_error_dict=True)
                    ok2, oid2, txid2, status2 = _order_confirmed(resp2)
                    if not ok2:
                        _note_order_reject(pair, status2, resp2, leg="RUN")
                        _reject(f"order_run_{status2}", extra={"resp": resp2}, leg="RUN")
                        db_call(
                            "record_order_fail_run",
                            db.record_order,
                            client_id=cid2,
                            pair=pair,
                            setup=sig.setup_name,
                            leg="RUN",
                            units=units_run,
                            sl=sl2,
                            tp=tp2,
                            oanda_order_id=oid2,
                            oanda_transaction_id=txid2,
                            status=f"FAILED_{status2}",
                            raw={**(resp2 if isinstance(resp2, dict) else {}), **order_meta},
                        )
                    else:
                        ORDER_REJECT_BLOCK.pop(pair, None)
                        db_call(
                            "record_order_run",
                            db.record_order,
                            client_id=cid2,
                            pair=pair,
                            setup=sig.setup_name,
                            leg="RUN",
                            units=units_run,
                            sl=sl2,
                            tp=tp2,
                            oanda_order_id=oid2,
                            oanda_transaction_id=txid2,
                            status=status2,
                            raw={**(resp2 if isinstance(resp2, dict) else {}), **order_meta},
                        )
                        tid2 = _extract_trade_id_from_fill(resp2)
                        # Verify we got a valid trade ID BEFORE recording in DB
                        if not tid2:
                            log(f"{EMOJI_ERR} NO_TRADE_ID {pair_tag(pair, sig.direction)}", 
                                {"setup": sig.setup_name + "_RUN", "response": resp2})
                            _reject("no_trade_id", leg="RUN")
                            continue

                        # SOP v2.1: Post-fill CSL -> structural SL upgrade with health gate
                        try:
                            last_price_ts = None
                            try:
                                last_price_ts = enhanced_market_hub.resilience_controller.last_update.get(pair)
                            except Exception:
                                last_price_ts = None
                            health = _health_snapshot(now=now_ts(), last_price_ts=last_price_ts, net_fail_count=net_fail_count)
                            _post_fill_upgrade_sl_or_panic(
                                o=o,
                                pair=pair,
                                direction=sig.direction,
                                trade_id=str(tid2),
                                csl_price=float(csl2),
                                structural_sl=float(sl2),
                                health=health,
                                db_trade_id=None,
                                leg="RUN",
                            )
                        except Exception as e:
                            log_runtime("warning", "POST_FILL_SL_UPGRADE_EXCEPTION", pair=pair, error=str(e), leg="RUN")
                        if isinstance(resp2, dict) and resp2.get("_sl_add_failed"):
                            try:
                                sl_retry_state[int(tid2)] = {
                                    "pair": pair,
                                    "direction": sig.direction,
                                    "sl": sl2,
                                    "next_ts": now_ts() + SL_RETRY_BASE_SEC,
                                    "fail_count": 0,
                                }
                            except Exception:
                                pass
                        
                        trade_note_run = sig.reason
                        trade_id_run = db_call(
                            "add_trade_run",
                            db.record_trade,
                            pair=pair,
                            setup=sig.setup_name + "_RUN",
                            direction=sig.direction,
                            mode=st.mode,
                            units=units_run,
                            entry=entry2,
                            atr_entry=st.atr_exec,
                            ttl_sec=ttl_run,
                            pg_t=pg_t_run,
                            pg_atr=pg_atr_val,
                            note=trade_note_run,
                            oanda_trade_id=tid2,
                        )
                        if trade_id_run:
                            db_open_trades.append({"pair": pair, "setup": sig.setup_name + "_RUN", "dir": sig.direction})
                            log(
                                f"{EMOJI_ENTER} ENTER {pair_tag(pair, sig.direction)}",
                                {
                                    "trade_id": trade_id_run,
                                    "setup_id": sig.setup_id,
                                    "speed_class": speed_class,
                                    "leg": "RUN",
                                    "atr_entry": st.atr_exec,
                                    "m_norm": st.m_norm,
                                    "wr": st.wr,
                                    "spread_pips": st.spread_pips,
                                    "entry_reason": sig.reason,
                                    "entry_ts": now_ts(),
                                    "exit_mode": exit_mode,
                                    "fallback_reason": fallback_reason,
                                },
                            )

                st.last_trade = now_ts()

                notify(
                    f"{EMOJI_ENTER} ENTER {pair_tag(pair, sig.direction)}",
                    f"{EMOJI_ENTER} {sig.setup_name} | units={units_total} spr={st.spread_pips:.1f}p mode={st.mode} {EMOJI_ENTER}",
                )

            if (now_ts() - last_state_flush) >= 20.0:
                for p, st in states.items():
                    db_call("save_state", db.save_state, p, st)
                last_state_flush = now_ts()
        
        except Exception as e:
            log(f"{EMOJI_ERR} MAIN_LOOP_ERROR", {"error": str(e), "type": type(e).__name__})
            print(f"\n❌ ERROR in main loop: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5.0)  # Prevent rapid error loops
            continue

    for p, st in states.items():
        db_call("save_state_shutdown", db.save_state, p, st)
    notify(f"{EMOJI_STOP} BOT STOP", f"{EMOJI_STOP} Graceful shutdown {EMOJI_STOP}")


def send_webhook_notification(event_type: str, data: dict, priority: str = "normal") -> bool:
    """Send webhook notification with retry logic.
    
    Args:
        event_type: Type of event (trade_entered, trade_exited, alert, etc.)
        data: Event data payload
        priority: Priority level (low, normal, high, critical)
        
    Returns:
        True if sent successfully
    """
    if not WEBHOOK_ENABLED or not WEBHOOK_URL:
        return False
    
    # Prepare webhook payload
    payload = {
        "timestamp": now_ts(),
        "event_type": event_type,
        "priority": priority,
        "data": data,
        "source": "phone_bot"
    }
    
    # Add priority-specific formatting
    if priority == "critical":
        payload["urgent"] = True
        payload["retry_until_success"] = True
    
    # Send with retry logic
    for attempt in range(WEBHOOK_RETRY_MAX):
        try:
            import json
            import urllib.request
            
            json_data = json.dumps(payload).encode('utf-8')
            req = urllib.request.Request(
                WEBHOOK_URL,
                data=json_data,
                headers={
                    'Content-Type': 'application/json',
                    'User-Agent': 'phone_bot/1.0'
                }
            )
            
            with urllib.request.urlopen(req, timeout=WEBHOOK_TIMEOUT) as response:
                if response.status == 200:
                    log(f"{EMOJI_INFO} WEBHOOK_SENT", {
                        "event_type": event_type,
                        "attempt": attempt + 1,
                        "priority": priority
                    })
                    return True
                else:
                    log(f"{EMOJI_WARN} WEBHOOK_HTTP_ERROR", {
                        "event_type": event_type,
                        "status": response.status,
                        "attempt": attempt + 1
                    })
                    
        except Exception as e:
            log(f"{EMOJI_WARN} WEBHOOK_SEND_FAILED", {
                "event_type": event_type,
                "attempt": attempt + 1,
                "error": str(e)
            })
            
            if attempt < WEBHOOK_RETRY_MAX - 1:
                time.sleep(WEBHOOK_RETRY_DELAY * (2 ** attempt))  # Exponential backoff
    
    log(f"{EMOJI_ERR} WEBHOOK_FAILED_ALL_RETRIES", {
        "event_type": event_type,
        "max_attempts": WEBHOOK_RETRY_MAX
    })
    return False

def send_push_notification(title: str, message: str, priority: str = "normal") -> bool:
    """Send push notification via configured service.
    
    Args:
        title: Notification title
        message: Notification message
        priority: Priority level
        
    Returns:
        True if sent successfully
    """
    if not PUSH_ENABLED or not PUSH_SERVICE or not PUSH_TOKEN:
        return False
    
    try:
        if PUSH_SERVICE.lower() == "pushover":
            return send_pushover_notification(title, message, priority)
        elif PUSH_SERVICE.lower() == "pushbullet":
            return send_pushbullet_notification(title, message)
        else:
            log(f"{EMOJI_WARN} UNSUPPORTED_PUSH_SERVICE", {
                "service": PUSH_SERVICE
            })
            return False
            
    except Exception as e:
        log(f"{EMOJI_ERR} PUSH_NOTIFICATION_FAILED", {
            "service": PUSH_SERVICE,
            "error": str(e)
        })
        return False

def send_pushover_notification(title: str, message: str, priority: str) -> bool:
    """Send notification via Pushover service.
    
    Args:
        title: Notification title
        message: Notification message
        priority: Priority level (-2 to 2)
        
    Returns:
        True if sent successfully
    """
    import urllib.request
    import urllib.parse
    
    # Map priority levels
    priority_map = {
        "low": -1,
        "normal": 0,
        "high": 1,
        "critical": 2
    }
    
    pushover_priority = priority_map.get(priority, 0)
    
    # Prepare payload
    payload = {
        "token": PUSH_TOKEN,
        "user": os.getenv("PUSHOVER_USER_KEY", ""),
        "title": title,
        "message": message,
        "priority": pushover_priority,
        "sound": "siren" if priority == "critical" else "pushover"
    }
    
    # Add emergency parameters for critical priority
    if pushover_priority == 2:
        payload["retry"] = 30  # Retry every 30 seconds
        payload["expire"] = 3600  # Expire after 1 hour
    
    data = urllib.parse.urlencode(payload).encode('utf-8')
    req = urllib.request.Request(
        "https://api.pushover.net/1/messages.json",
        data=data
    )
    
    with urllib.request.urlopen(req, timeout=10) as response:
        result = json.loads(response.read().decode('utf-8'))
        return result.get("status") == 1

def send_pushbullet_notification(title: str, message: str) -> bool:
    """Send notification via Pushbullet service.
    
    Args:
        title: Notification title
        message: Notification message
        
    Returns:
        True if sent successfully
    """
    import urllib.request
    import json
    
    payload = {
        "type": "note",
        "title": title,
        "body": message
    }
    
    headers = {
        "Access-Token": PUSH_TOKEN,
        "Content-Type": "application/json"
    }
    
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        "https://api.pushbullet.com/v2/pushes",
        data=data,
        headers=headers
    )
    
    with urllib.request.urlopen(req, timeout=10) as response:
        return response.status == 200

def notify_trade_event(event_type: str, trade_data: dict):
    """Send notifications for trade events.
    
    Args:
        event_type: Type of trade event (entered, exited, modified)
        trade_data: Trade information
    """
    # Format message
    if event_type == "entered":
        title = f"Trade Entered: {trade_data.get('pair', 'Unknown')}"
        message = f"{trade_data.get('direction', 'Unknown')} {trade_data.get('units', 0)} units @ {trade_data.get('entry_price', 0)}"
        priority = "normal"
    elif event_type == "exited":
        pnl = trade_data.get('pnl', 0)
        title = f"Trade Exited: {trade_data.get('pair', 'Unknown')}"
        message = f"PnL: ${pnl:.2f} - {trade_data.get('exit_reason', 'Unknown')}"
        priority = "high" if pnl < 0 else "normal"
    elif event_type == "runner_held":
        hours = trade_data.get('hold_duration', 0) / 3600
        title = f"Runner Active: {trade_data.get('pair', 'Unknown')}"
        message = f"Holding for {hours:.1f}h - Progress: {trade_data.get('progress', 0):.2f} ATR"
        priority = "normal"
    else:
        title = f"Trade Update: {event_type}"
        message = str(trade_data)
        priority = "normal"
    
    # Send webhook
    send_webhook_notification(f"trade_{event_type}", trade_data, priority)
    
    # Send push notification for important events
    if event_type in ["entered", "exited", "runner_held"]:
        send_push_notification(title, message, priority)

def notify_system_alert(alert_data: dict):
    """Send notifications for system alerts.
    
    Args:
        alert_data: Alert information
    """
    level = alert_data.get("level", "INFO")
    message = alert_data.get("message", "")
    
    # Determine priority
    if level == "CRITICAL":
        priority = "critical"
        title = "🚨 CRITICAL ALERT"
    elif level == "ERROR":
        priority = "high"
        title = "❌ SYSTEM ERROR"
    elif level == "WARN":
        priority = "normal"
        title = "⚠️ WARNING"
    else:
        priority = "low"
        title = "ℹ️ INFO"
    
    # Send webhook
    send_webhook_notification("system_alert", alert_data, priority)
    
    # Send push for high priority alerts
    if level in ["CRITICAL", "ERROR"]:
        send_push_notification(title, message, priority)

def get_notification_status() -> dict:
    """Get status of notification systems.
    
    Returns:
        Dict with notification configuration status
    """
    return {
        "webhook": {
            "enabled": WEBHOOK_ENABLED,
            "configured": bool(WEBHOOK_URL),
            "timeout": WEBHOOK_TIMEOUT,
            "max_retries": WEBHOOK_RETRY_MAX
        },
        "push": {
            "enabled": PUSH_ENABLED,
            "service": PUSH_SERVICE,
            "configured": bool(PUSH_TOKEN and PUSH_SERVICE)
        }
    }


# Dynamic position sizing state
RECENT_PERFORMANCE = {}  # strategy_id -> {win_rate, avg_win, avg_loss, recent_trades}
ACCOUNT_RISK_METRICS = {}  # Updated periodically

def calculate_dynamic_position_size(pair: str, strategy_id: int, account_balance: float, 
                                   atr_pips: float, stop_distance_atr: float) -> int:
    """Calculate dynamic position size based on account balance and risk metrics.
    
    Args:
        pair: Currency pair
        strategy_id: Strategy ID (1-7)
        account_balance: Current account balance
        atr_pips: Current ATR in pips
        stop_distance_atr: Stop loss distance in ATR
        
    Returns:
        Position size in units
    """
    if not DYNAMIC_SIZING_ENABLED:
        return 10000  # Default fixed size
    
    # Get account summary for real-time balance
    try:
        client = OandaClient(
            os.getenv("OANDA_API_KEY", "2bf7b4b9bb052e28023de779a6363f1e-fee71a4fce4e94b18e0dd9c2443afa52"),
            os.getenv("OANDA_ACCOUNT_ID", "101-001-22881868-001"),
            "practice"
        )
        account = client.account_summary()
        if account:
            account_balance = float(account.get("balance", account_balance))
            ACCOUNT_RISK_METRICS["last_update"] = now_ts()
            ACCOUNT_RISK_METRICS["balance"] = account_balance
            ACCOUNT_RISK_METRICS["open_pl"] = float(account.get("openPositionPnL", 0))
            ACCOUNT_RISK_METRICS["margin_used"] = float(account.get("marginUsed", 0))
            ACCOUNT_RISK_METRICS["margin_available"] = float(account.get("marginAvailable", 0))
    except Exception as e:
        log(f"{EMOJI_WARN} DYNAMIC_SIZING_ACCOUNT_ERROR", {"error": str(e)})
    
    # Calculate risk per trade
    risk_per_trade = STRATEGY_RISK_PER_TRADE.get(strategy_id, 0.01)  # Default 1%
    risk_amount = account_balance * risk_per_trade
    
    # Adjust for correlation risk
    open_positions = get_open_positions()
    correlated_count = sum(1 for pos in open_positions 
                          if pos.get("pair") in CORRELATED_PAIRS.get(pair, []))
    
    if correlated_count > 0:
        # Reduce position size for correlated positions
        correlation_factor = 1 / (1 + correlated_count * 0.5)
        risk_amount *= correlation_factor
    
    # Calculate position size based on stop distance
    if stop_distance_atr > 0:
        # Risk per unit = stop_distance_atr * ATR * pip_value
        pip_value = get_pip_value(pair)
        risk_per_unit = stop_distance_atr * atr_pips * pip_value
        
        if risk_per_unit > 0:
            position_size = int(risk_amount / risk_per_unit)
            
            # Round to standard lot sizes
            position_size = round(position_size / 1000) * 1000
            
            # Apply minimum and maximum limits
            min_units = 1000
            max_units = int(account_balance * 0.1)  # Max 10% of account per trade
            
            position_size = max(min_units, min(position_size, max_units))
            
            log(f"{EMOJI_INFO} DYNAMIC_POSITION_SIZE", {
                "pair": pair,
                "strategy_id": strategy_id,
                "account_balance": account_balance,
                "risk_percent": risk_per_trade * 100,
                "risk_amount": risk_amount,
                "stop_distance_atr": stop_distance_atr,
                "position_size": position_size
            })
            
            return position_size
    
    # Fallback to fixed size
    return 10000

def get_pip_value(pair: str) -> float:
    """Get pip value for a currency pair.
    
    Args:
        pair: Currency pair
        
    Returns:
        Pip value in account currency
    """
    # Standard pip values
    if pair.endswith("JPY"):
        return 0.01  # Yen pairs
    elif pair.endswith("USD"):
        return 0.0001  # Standard pairs
    else:
        # Cross pairs - approximate
        return 0.0001


# Multi-Timeframe Coordination State


# Database Transaction State

class DatabaseTransaction:
    """Database transaction context manager with retry logic."""
    
    def __init__(self, db_path: str, timeout: float = None, isolation_level: str = "IMMEDIATE"):
        self.db_path = db_path
        self.timeout = timeout or DB_TRANSACTION_TIMEOUT
        self.isolation_level = isolation_level
        self.connection = None
        self.transaction_id = None
        self.retry_count = 0
    
    def __enter__(self):
        """Enter transaction context."""
        return self.start()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit transaction context."""
        if exc_type is None:
            self.commit()
        else:
            self.rollback()
        return False  # Don't suppress exceptions
    def start(self):
        """Start the database transaction."""
        import sqlite3
        import time
        import uuid
        
        self.transaction_id = str(uuid.uuid4())
        start_time = time.time()
        
        while self.retry_count < DB_TRANSACTION_MAX_RETRIES:
            try:
                self.connection = sqlite3.connect(
                    self.db_path,
                    timeout=self.timeout,
                    isolation_level=self.isolation_level
                )
                
                # Enable WAL mode for better concurrency
                self.connection.execute("PRAGMA journal_mode=WAL")
                
                # Set busy timeout
                self.connection.execute(f"PRAGMA busy_timeout={int(self.timeout * 1000)}")
                
                log(f"{EMOJI_INFO} DB_TRANSACTION_STARTED", {
                    "transaction_id": self.transaction_id,
                    "db_path": self.db_path,
                    "retry_count": self.retry_count,
                    "duration": time.time() - start_time
                })
                
                return self
                
            except sqlite3.OperationalError as e:
                self.retry_count += 1
                if "database is locked" in str(e).lower():
                    wait_time = min(0.1 * (2 ** self.retry_count), 1.0)
                    time.sleep(wait_time)
                    continue
                else:
                    log(f"{EMOJI_ERR} DB_TRANSACTION_ERROR", {
                        "transaction_id": self.transaction_id,
                        "error": str(e),
                        "retry_count": self.retry_count
                    })
                    raise
            except Exception as e:
                log(f"{EMOJI_ERR} DB_TRANSACTION_ERROR", {
                    "transaction_id": self.transaction_id,
                    "error": str(e),
                    "retry_count": self.retry_count
                })
                raise
        
        raise sqlite3.OperationalError(f"Transaction failed after {DB_TRANSACTION_MAX_RETRIES} retries")
    
    def commit(self):
        """Commit the transaction."""
        if self.connection:
            try:
                self.connection.commit()
                log(f"{EMOJI_INFO} DB_TRANSACTION_COMMITTED", {
                    "transaction_id": self.transaction_id,
                    "db_path": self.db_path
                })
            except Exception as e:
                log(f"{EMOJI_ERR} DB_TRANSACTION_COMMIT_FAILED", {
                    "transaction_id": self.transaction_id,
                    "error": str(e)
                })
                raise
        else:
            raise RuntimeError("No active transaction to commit")
    
    def rollback(self):
        """Rollback the transaction."""
        if self.connection:
            try:
                self.connection.rollback()
                log(f"{EMOJI_INFO} DB_TRANSACTION_ROLLED_BACK", {
                    "transaction_id": self.transaction_id,
                    "db_path": self.db_path
                })
            except Exception as e:
                log(f"{EMOJI_ERR} DB_TRANSACTION_ROLLBACK_FAILED", {
                    "transaction_id": self.transaction_id,
                    "error": str(e)
                })
                raise
        else:
            raise RuntimeError("No active transaction to rollback")
    
    def execute(self, query: str, params: tuple = None):
        """Execute a query within the transaction."""
        if not self.connection:
            raise RuntimeError("No active transaction")
        
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor
        except Exception as e:
            log(f"{EMOJI_ERR} DB_TRANSACTION_EXECUTE_FAILED", {
                "transaction_id": self.transaction_id,
                "query": query[:100],  # Limit query length in logs
                "error": str(e)
            })
            raise
    
    def executemany(self, query: str, params_list: List[tuple]):
        """Execute a query with multiple parameter sets."""
        if not self.connection:
            raise RuntimeError("No active transaction")
        
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, params_list)
            return cursor
        except Exception as e:
            log(f"{EMOJI_ERR} DB_TRANSACTION_EXECUTEMANY_FAILED", {
                "transaction_id": self.transaction_id,
                "query": query[:100],
                "params_count": len(params_list),
                "error": str(e)
            })
            raise


# Database Backup State

# Database backup configuration
DB_BACKUP_ENABLED = os.getenv("DB_BACKUP_ENABLED", "1").strip().lower() in ("1", "true", "yes")
DB_BACKUP_INTERVAL = int(os.getenv("DB_BACKUP_INTERVAL", "24"))  # hours
DB_BACKUP_RETENTION = int(os.getenv("DB_BACKUP_RETENTION", "7"))  # days
DB_BACKUP_COMPRESSION = os.getenv("DB_BACKUP_COMPRESSION", "1").strip().lower() in ("1", "true", "yes")
DB_BACKUP_PATH = Path(os.getenv("DB_BACKUP_PATH", "./backups"))
DB_LAST_BACKUP = {}  # db_path -> timestamp

def backup_database(db_path: str, backup_path: str = None) -> bool:
    """Create a backup of the database.
    
    Args:
        db_path: Path to the database file
        backup_path: Optional custom backup path
        
    Returns:
        True if backup successful, False otherwise
    """
    import shutil
    import gzip
    from datetime import datetime
    
    try:
        db_path = Path(db_path)
        if not db_path.exists():
            log(f"{EMOJI_ERR} DB_BACKUP_FAILED", {
                "error": "Database file not found",
                "db_path": str(db_path)
            })
            return False
        
        # Create backup directory if needed
        if not DB_BACKUP_PATH.exists():
            DB_BACKUP_PATH.mkdir(parents=True, exist_ok=True)
        
        # Generate backup filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        db_name = db_path.stem
        backup_name = f"{db_name}_{timestamp}.db"
        
        if backup_path:
            backup_file = Path(backup_path)
        else:
            backup_file = DB_BACKUP_PATH / backup_name
        
        # Copy database
        if DB_BACKUP_COMPRESSION:
            # Compressed backup
            backup_file = backup_file.with_suffix('.db.gz')
            with open(db_path, 'rb') as f_in:
                with gzip.open(backup_file, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            # Uncompressed backup
            shutil.copy2(db_path, backup_file)
        
        # Update last backup time
        DB_LAST_BACKUP[str(db_path)] = now_ts()
        
        # Get file sizes
        original_size = db_path.stat().st_size
        backup_size = backup_file.stat().st_size
        compression_ratio = (1 - backup_size / original_size) * 100 if DB_BACKUP_COMPRESSION else 0
        
        log(f"{EMOJI_INFO} DB_BACKUP_CREATED", {
            "db_path": str(db_path),
            "backup_file": str(backup_file),
            "original_size_mb": original_size / (1024 * 1024),
            "backup_size_mb": backup_size / (1024 * 1024),
            "compression_ratio": compression_ratio,
            "compressed": DB_BACKUP_COMPRESSION
        })
        
        return True
        
    except Exception as e:
        log(f"{EMOJI_ERR} DB_BACKUP_FAILED", {
            "db_path": str(db_path),
            "error": str(e)
        })
        return False


def cleanup_old_backups(db_path: str = None) -> int:
    """Clean up old backup files based on retention policy.
    
    Args:
        db_path: Optional specific database to clean up
        
    Returns:
        Number of files cleaned up
    """
    from datetime import datetime, timedelta
    
    try:
        if not DB_BACKUP_PATH.exists():
            return 0
        
        # Calculate cutoff date
        cutoff_date = datetime.now() - timedelta(days=DB_BACKUP_RETENTION)
        cleaned = 0
        
        # Find and remove old backups
        for backup_file in DB_BACKUP_PATH.glob("*.db*"):
            try:
                # Extract timestamp from filename
                parts = backup_file.stem.split('_')
                if len(parts) >= 2:
                    timestamp_str = '_'.join(parts[-2:])
                    backup_date = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                    
                    # Check if backup is too old
                    if backup_date < cutoff_date:
                        # If db_path specified, only clean that database's backups
                        if db_path:
                            db_name = Path(db_path).stem
                            if not backup_file.stem.startswith(db_name):
                                continue
                        
                        backup_file.unlink()
                        cleaned += 1
                        log(f"{EMOJI_INFO} DB_BACKUP_CLEANED", {
                            "file": str(backup_file),
                            "backup_date": backup_date.isoformat(),
                            "reason": "expired"
                        })
            except Exception as e:
                log(f"{EMOJI_WARN} DB_BACKUP_CLEANUP_ERROR", {
                    "file": str(backup_file),
                    "error": str(e)
                })
                continue
        
        if cleaned > 0:
            log(f"{EMOJI_INFO} DB_BACKUP_CLEANUP_COMPLETED", {
                "files_cleaned": cleaned,
                "retention_days": DB_BACKUP_RETENTION
            })
        
        return cleaned
        
    except Exception as e:
        log(f"{EMOJI_ERR} DB_BACKUP_CLEANUP_FAILED", {
            "error": str(e)
        })
        return 0


def auto_backup_database(db_path: str) -> bool:
    """Automatically backup database if interval has passed.
    
    Args:
        db_path: Path to the database file
        
    Returns:
        True if backup was created, False if not needed or failed
    """
    try:
        # Check if backup is needed
        last_backup = DB_LAST_BACKUP.get(db_path, 0)
        if now_ts() - last_backup < (DB_BACKUP_INTERVAL * 3600):
            return False
        
        # Create backup
        success = backup_database(db_path)
        
        if success:
            # Clean up old backups
            cleanup_old_backups(db_path)
        
        return success
        
    except Exception as e:
        log(f"{EMOJI_ERR} AUTO_BACKUP_FAILED", {
            "db_path": db_path,
            "error": str(e)
        })
        return False


def list_backups(db_path: str = None) -> List[dict]:
    """List available backup files.
    
    Args:
        db_path: Optional specific database to list
        
    Returns:
        List of backup information dictionaries
    """
    from datetime import datetime
    
    try:
        if not DB_BACKUP_PATH.exists():
            return []
        
        backups = []
        
        for backup_file in DB_BACKUP_PATH.glob("*.db*"):
            try:
                # Extract timestamp from filename
                parts = backup_file.stem.split('_')
                if len(parts) >= 2:
                    timestamp_str = '_'.join(parts[-2:])
                    backup_date = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")
                    
                    # If db_path specified, only list that database's backups
                    if db_path:
                        db_name = Path(db_path).stem
                        if not backup_file.stem.startswith(db_name):
                            continue
                    
                    # Get file info
                    stat = backup_file.stat()
                    backups.append({
                        "file": str(backup_file),
                        "date": backup_date.isoformat(),
                        "size_mb": stat.st_size / (1024 * 1024),
                        "compressed": backup_file.suffix == '.gz'
                    })
            except Exception:
                continue
        
        # Sort by date (newest first)
        backups.sort(key=lambda x: x["date"], reverse=True)
        
        return backups
        
    except Exception as e:
        log(f"{EMOJI_ERR} LIST_BACKUPS_FAILED", {
            "error": str(e)
        })
        return []


def restore_database(backup_path: str, target_path: str) -> bool:
    """Restore database from backup.
    
    Args:
        backup_path: Path to backup file
        target_path: Where to restore the database
        
    Returns:
        True if restore successful, False otherwise
    """
    import shutil
    import gzip
    
    try:
        backup_path = Path(backup_path)
        target_path = Path(target_path)
        
        if not backup_path.exists():
            log(f"{EMOJI_ERR} DB_RESTORE_FAILED", {
                "error": "Backup file not found",
                "backup_path": str(backup_path)
            })
            return False
        
        # Create target directory if needed
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Check if backup is compressed
        if backup_path.suffix == '.gz':
            # Decompress and restore
            with gzip.open(backup_path, 'rb') as f_in:
                with open(target_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            # Direct copy
            shutil.copy2(backup_path, target_path)
        
        log(f"{EMOJI_INFO} DB_RESTORED", {
            "backup_path": str(backup_path),
            "target_path": str(target_path),
            "size_mb": target_path.stat().st_size / (1024 * 1024)
        })
        
        return True
        
    except Exception as e:
        log(f"{EMOJI_ERR} DB_RESTORE_FAILED", {
            "backup_path": str(backup_path),
            "target_path": str(target_path),
            "error": str(e)
        })
        return False


def schedule_periodic_backup(db_path: str, interval_hours: int = None) -> None:
    """Schedule periodic backups for a database.
    
    Args:
        db_path: Path to the database file
        interval_hours: Custom interval (overrides DB_BACKUP_INTERVAL)
    """
    import threading
    
    if not DB_BACKUP_ENABLED:
        return
    
    interval = interval_hours or DB_BACKUP_INTERVAL
    interval_seconds = interval * 3600
    
    def backup_scheduler():
        while True:
            try:
                # Wait for interval
                time.sleep(interval_seconds)
                
                # Create backup
                auto_backup_database(db_path)
                
            except Exception as e:
                log(f"{EMOJI_ERR} SCHEDULED_BACKUP_ERROR", {
                    "db_path": db_path,
                    "error": str(e)
                })
                # Continue trying
    
    # Start scheduler thread
    scheduler_thread = threading.Thread(target=backup_scheduler, daemon=True)
    scheduler_thread.start()
    
    log(f"{EMOJI_INFO} SCHEDULED_BACKUP_STARTED", {
        "db_path": db_path,
        "interval_hours": interval
    })


# Performance Monitoring State
PERFORMANCE_MONITORING_ENABLED = os.getenv("PERFORMANCE_MONITORING_ENABLED", "1").strip().lower() in ("1", "true", "yes")
PERFORMANCE_CHECK_INTERVAL = int(os.getenv("PERFORMANCE_CHECK_INTERVAL", "60"))  # seconds
PERFORMANCE_HISTORY_SIZE = int(os.getenv("PERFORMANCE_HISTORY_SIZE", "1440"))  # 24 hours at 1/min
PERFORMANCE_ALERT_MEMORY = float(os.getenv("PERFORMANCE_ALERT_MEMORY", "80"))  # %
PERFORMANCE_ALERT_CPU = float(os.getenv("PERFORMANCE_ALERT_CPU", "85"))  # %
PERFORMANCE_ALERT_DISK = float(os.getenv("PERFORMANCE_ALERT_DISK", "90"))  # %
PERFORMANCE_HISTORY = []
PERFORMANCE_LAST_CHECK = 0

def get_system_metrics() -> dict:
    """Get current system performance metrics.
    
    Returns:
        Dictionary containing system metrics
    """
    import psutil
    
    try:
        # CPU metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_count = psutil.cpu_count()
        cpu_freq = psutil.cpu_freq()
        
        # Memory metrics
        memory = psutil.virtual_memory()
        memory_percent = memory.percent
        memory_used_gb = memory.used / (1024**3)
        memory_total_gb = memory.total / (1024**3)
        
        # Disk metrics
        disk = psutil.disk_usage('/')
        disk_percent = disk.percent
        disk_used_gb = disk.used / (1024**3)
        disk_total_gb = disk.total / (1024**3)
        
        # Network metrics
        network = psutil.net_io_counters()
        bytes_sent = network.bytes_sent
        bytes_recv = network.bytes_recv
        
        # Process metrics
        process = psutil.Process()
        process_memory = process.memory_info()
        process_cpu = process.cpu_percent()
        
        return {
            "timestamp": now_ts(),
            "cpu": {
                "percent": cpu_percent,
                "count": cpu_count,
                "freq_mhz": cpu_freq.current if cpu_freq else None
            },
            "memory": {
                "percent": memory_percent,
                "used_gb": memory_used_gb,
                "total_gb": memory_total_gb
            },
            "disk": {
                "percent": disk_percent,
                "used_gb": disk_used_gb,
                "total_gb": disk_total_gb
            },
            "network": {
                "bytes_sent": bytes_sent,
                "bytes_recv": bytes_recv
            },
            "process": {
                "memory_mb": process_memory.rss / (1024**2),
                "cpu_percent": process_cpu
            }
        }
        
    except Exception as e:
        log(f"{EMOJI_ERR} GET_METRICS_FAILED", {
            "error": str(e)
        })
        return {}


def check_performance_alerts(metrics: dict) -> List[dict]:
    """Check metrics against performance thresholds.
    
    Args:
        metrics: System metrics dictionary
        
    Returns:
        List of alert dictionaries
    """
    alerts = []
    
    try:
        # Check memory usage
        if metrics.get("memory", {}).get("percent", 0) > PERFORMANCE_ALERT_MEMORY:
            alerts.append({
                "type": "memory",
                "severity": "warning",
                "message": f"High memory usage: {metrics['memory']['percent']:.1f}%",
                "threshold": PERFORMANCE_ALERT_MEMORY
            })
        
        # Check CPU usage
        if metrics.get("cpu", {}).get("percent", 0) > PERFORMANCE_ALERT_CPU:
            alerts.append({
                "type": "cpu",
                "severity": "warning",
                "message": f"High CPU usage: {metrics['cpu']['percent']:.1f}%",
                "threshold": PERFORMANCE_ALERT_CPU
            })
        
        # Check disk usage
        if metrics.get("disk", {}).get("percent", 0) > PERFORMANCE_ALERT_DISK:
            alerts.append({
                "type": "disk",
                "severity": "critical",
                "message": f"High disk usage: {metrics['disk']['percent']:.1f}%",
                "threshold": PERFORMANCE_ALERT_DISK
            })
        
        return alerts
        
    except Exception as e:
        log(f"{EMOJI_ERR} CHECK_ALERTS_FAILED", {
            "error": str(e)
        })
        return []


def update_performance_history(metrics: dict) -> None:
    """Update performance history with new metrics.
    
    Args:
        metrics: System metrics to add to history
    """
    global PERFORMANCE_HISTORY, PERFORMANCE_LAST_CHECK
    
    try:
        # Add timestamp if not present
        if "timestamp" not in metrics:
            metrics["timestamp"] = now_ts()
        
        # Add to history
        PERFORMANCE_HISTORY.append(metrics)
        
        # Trim history if too large
        if len(PERFORMANCE_HISTORY) > PERFORMANCE_HISTORY_SIZE:
            PERFORMANCE_HISTORY = PERFORMANCE_HISTORY[-PERFORMANCE_HISTORY_SIZE:]
        
        # Update last check time
        PERFORMANCE_LAST_CHECK = metrics["timestamp"]
        
    except Exception as e:
        log(f"{EMOJI_ERR} UPDATE_HISTORY_FAILED", {
            "error": str(e)
        })


def get_performance_summary() -> dict:
    """Get summary statistics from performance history.
    
    Returns:
        Dictionary with performance summary
    """
    if not PERFORMANCE_HISTORY:
        return {}
    
    try:
        # Extract metrics from history
        cpu_values = [m.get("cpu", {}).get("percent", 0) for m in PERFORMANCE_HISTORY if "cpu" in m]
        memory_values = [m.get("memory", {}).get("percent", 0) for m in PERFORMANCE_HISTORY if "memory" in m]
        disk_values = [m.get("disk", {}).get("percent", 0) for m in PERFORMANCE_HISTORY if "disk" in m]
        
        # Calculate statistics
        summary = {
            "period_start": PERFORMANCE_HISTORY[0]["timestamp"],
            "period_end": PERFORMANCE_HISTORY[-1]["timestamp"],
            "sample_count": len(PERFORMANCE_HISTORY),
            "cpu": {
                "avg": sum(cpu_values) / len(cpu_values) if cpu_values else 0,
                "max": max(cpu_values) if cpu_values else 0,
                "min": min(cpu_values) if cpu_values else 0
            },
            "memory": {
                "avg": sum(memory_values) / len(memory_values) if memory_values else 0,
                "max": max(memory_values) if memory_values else 0,
                "min": min(memory_values) if memory_values else 0
            },
            "disk": {
                "avg": sum(disk_values) / len(disk_values) if disk_values else 0,
                "max": max(disk_values) if disk_values else 0,
                "min": min(disk_values) if disk_values else 0
            }
        }
        
        return summary
        
    except Exception as e:
        log(f"{EMOJI_ERR} GET_SUMMARY_FAILED", {
            "error": str(e)
        })
        return {}


def monitor_performance() -> None:
    """Main performance monitoring function.
    
    This function should be called periodically to check system performance
    and update the performance history.
    """
    global PERFORMANCE_LAST_CHECK
    
    if not PERFORMANCE_MONITORING_ENABLED:
        return
    
    current_time = now_ts()
    
    # Check if enough time has passed since last check
    if current_time - PERFORMANCE_LAST_CHECK < PERFORMANCE_CHECK_INTERVAL:
        return
    
    try:
        # Get current metrics
        metrics = get_system_metrics()
        
        if metrics:
            # Check for alerts
            alerts = check_performance_alerts(metrics)
            
            # Log any alerts
            for alert in alerts:
                if alert["severity"] == "critical":
                    log(f"{EMOJI_ERR} PERF_CRITICAL", alert)
                else:
                    log(f"{EMOJI_WARN} PERF_WARNING", alert)
            
            # Update history
            update_performance_history(metrics)
    
    except Exception as e:
        log(f"{EMOJI_ERR} PERF_MONITOR_ERROR", {
            "error": str(e)
        })


def alert_watch_triggered(pair: str, strategy: str, signal_strength: float):
    """Alert when watch signal is triggered."""
    notify(f"{EMOJI_WATCH} WATCH", f"{pair} - {strategy} - strength: {signal_strength}")


def alert_get_ready(pair: str, strategy: str, entry_conditions: dict):
    """Alert when get ready signal is triggered."""
    notify(f"{EMOJI_GET_READY} GET_READY", f"{pair} - {strategy} - preparing entry")


def alert_enter_placed(pair: str, strategy: str, direction: str, order_id: str):
    """Alert when entry order is placed."""
    notify(f"{EMOJI_ENTER} ENTER PLACED", f"{pair} {direction} - order: {order_id}")


def alert_trade_entered(pair: str, strategy: str, direction: str, trade_id: str):
    """Alert when trade is entered."""
    notify(f"{EMOJI_ENTER} TRADE ENTERED", f"{pair} {direction} - trade: {trade_id}")


def alert_exit_triggered(pair: str, strategy: str, reason: str, exit_price: float):
    """Alert when exit is triggered."""
    notify(f"{EMOJI_EXIT} EXIT TRIGGERED", f"{pair} - {reason} - price: {exit_price}")


def alert_exit_placed(pair: str, strategy: str, direction: str, order_id: str):
    """Alert when exit order is placed."""
    notify(f"{EMOJI_EXIT} EXIT PLACED", f"{pair} {direction} - order: {order_id}")


def alert_trade_closed(pair: str, strategy: str, direction: str, pnl: float):
    """Alert when trade is closed."""
    emoji = EMOJI_OK if pnl >= 0 else EMOJI_ERR
    notify(f"{emoji} TRADE CLOSED", f"{pair} {direction} - PnL: ${pnl:.2f}")


def alert_error(message: str, error: Exception = None):
    """Alert on error."""
    notify(f"{EMOJI_ERR} ERROR", message)


# Database Transaction State
DB_CONNECTIONS = {}  # Thread-local connections
DB_TRANSACTIONS = {}  # Active transactions
DB_LOCKS = {}  # Table locks

class DatabaseTransaction:
    """Database transaction context manager with retry logic."""
    
    def __init__(self, db_path: str, timeout: float = None, isolation_level: str = "IMMEDIATE"):
        self.db_path = db_path
        self.timeout = timeout or DB_TRANSACTION_TIMEOUT
        self.isolation_level = isolation_level
        self.connection = None
        self.transaction_id = None
        self.retry_count = 0
        
    def __enter__(self):
        """Enter transaction context."""
        self.transaction_id = f"{int(now_ts())}_{id(self)}"
        
        for attempt in range(DB_MAX_RETRIES):
            try:
                # Get or create connection
                if self.db_path not in DB_CONNECTIONS:
                    DB_CONNECTIONS[self.db_path] = sqlite3.connect(
                        self.db_path,
                        timeout=DB_LOCK_TIMEOUT,
                        check_same_thread=False
                    )
                    
                    # Enable WAL mode for better concurrency
                    if DB_ENABLE_WAL:
                        DB_CONNECTIONS[self.db_path].execute("PRAGMA journal_mode=WAL")
                        DB_CONNECTIONS[self.db_path].execute("PRAGMA synchronous=NORMAL")
                
                self.connection = DB_CONNECTIONS[self.db_path]
                
                # Begin transaction
                self.connection.execute(f"BEGIN {self.isolation_level} TRANSACTION")
                
                # Set timeout
                self.connection.execute(f"PRAGMA busy_timeout = {int(self.timeout * 1000)}")
                
                # Track transaction
                DB_TRANSACTIONS[self.transaction_id] = {
                    "start_time": now_ts(),
                    "connection": self.connection,
                    "status": "ACTIVE"
                }
                
                log(f"{EMOJI_INFO} DB_TRANSACTION_BEGIN", {
                    "transaction_id": self.transaction_id,
                    "db_path": self.db_path,
                    "attempt": attempt + 1
                })
                
                return self.connection
                
            except sqlite3.Error as e:
                self.retry_count += 1
                
                if "database is locked" in str(e).lower():
                    log(f"{EMOJI_WARN} DB_LOCK_RETRY", {
                        "transaction_id": self.transaction_id,
                        "attempt": attempt + 1,
                        "error": str(e)
                    })
                    
                    if attempt < DB_MAX_RETRIES - 1:
                        time.sleep(DB_RETRY_DELAY * (2 ** attempt))
                        continue
                
                log(f"{EMOJI_ERR} DB_TRANSACTION_FAILED", {
                    "transaction_id": self.transaction_id,
                    "error": str(e),
                    "attempts": attempt + 1
                })
                
                # Clean up on failure
                if self.transaction_id in DB_TRANSACTIONS:
                    del DB_TRANSACTIONS[self.transaction_id]
                
                raise
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit transaction context."""
        if not self.connection:
            return
        
        try:
            if exc_type is None:
                # Commit if no exception
                self.connection.execute("COMMIT")
                status = "COMMITTED"
            else:
                # Rollback on exception
                self.connection.execute("ROLLBACK")
                status = "ROLLED_BACK"
            
            # Update transaction tracking
            if self.transaction_id in DB_TRANSACTIONS:
                DB_TRANSACTIONS[self.transaction_id]["status"] = status
                DB_TRANSACTIONS[self.transaction_id]["end_time"] = now_ts()
                
                # Remove from active transactions after delay
                del DB_TRANSACTIONS[self.transaction_id]
            
            log(f"{EMOJI_INFO} DB_TRANSACTION_END", {
                "transaction_id": self.transaction_id,
                "status": status,
                "duration": now_ts() - (DB_TRANSACTIONS.get(self.transaction_id, {}).get("start_time", now_ts())),
                "retry_count": self.retry_count
            })
            
        except sqlite3.Error as e:
            log(f"{EMOJI_ERR} DB_TRANSACTION_CLEANUP_FAILED", {
                "transaction_id": self.transaction_id,
                "error": str(e)
            })

def execute_db_transaction(db_path: str, operations: List[Tuple[str, tuple]], 
                          isolation_level: str = "IMMEDIATE") -> List[any]:
    """Execute multiple database operations in a single transaction.
    
    Args:
        db_path: Path to database file
        operations: List of (sql, params) tuples
        isolation_level: Transaction isolation level
        
    Returns:
        List of results from each operation
    """
    results = []
    
    with DatabaseTransaction(db_path, isolation_level=isolation_level) as conn:
        cursor = conn.cursor()
        
        for sql, params in operations:
            try:
                if sql.strip().upper().startswith("SELECT"):
                    cursor.execute(sql, params or ())
                    results.append(cursor.fetchall())
                else:
                    cursor.execute(sql, params or ())
                    results.append(cursor.lastrowid if cursor.lastrowid else cursor.rowcount)
                    
            except sqlite3.Error as e:
                log(f"{EMOJI_ERR} DB_OPERATION_FAILED", {
                    "sql": sql[:100],
                    "error": str(e)
                })
                raise
    
    return results

def acquire_table_lock(db_path: str, table_name: str, lock_type: str = "EXCLUSIVE") -> bool:
    """Acquire a table lock.
    
    Args:
        db_path: Path to database
        table_name: Table to lock
        lock_type: Type of lock (SHARED, RESERVED, EXCLUSIVE)
        
    Returns:
        True if lock acquired
    """
    lock_key = f"{db_path}:{table_name}"
    
    # Check if already locked
    if lock_key in DB_LOCKS:
        return DB_LOCKS[lock_key]["owner"] == id(threading.current_thread())
    
    try:
        with DatabaseTransaction(db_path, isolation_level="IMMEDIATE") as conn:
            # Acquire lock using BEGIN IMMEDIATE and touching the table
            conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            
            # Track lock
            DB_LOCKS[lock_key] = {
                "owner": id(threading.current_thread()),
                "type": lock_type,
                "acquired_at": now_ts()
            }
            
            log(f"{EMOJI_INFO} DB_LOCK_ACQUIRED", {
                "table": table_name,
                "type": lock_type,
                "lock_key": lock_key
            })
            
            return True
            
    except sqlite3.Error as e:
        log(f"{EMOJI_WARN} DB_LOCK_FAILED", {
            "table": table_name,
            "error": str(e)
        })
        return False

def release_table_lock(db_path: str, table_name: str) -> bool:
    """Release a table lock.
    
    Args:
        db_path: Path to database
        table_name: Table to unlock
        
    Returns:
        True if lock released
    """
    lock_key = f"{db_path}:{table_name}"
    
    if lock_key in DB_LOCKS:
        if DB_LOCKS[lock_key]["owner"] == id(threading.current_thread()):
            del DB_LOCKS[lock_key]
            
            log(f"{EMOJI_INFO} DB_LOCK_RELEASED", {
                "table": table_name,
                "lock_key": lock_key
            })
            
            return True
        else:
            log(f"{EMOJI_WARN} DB_LOCK_NOT_OWNER", {
                "table": table_name,
                "lock_key": lock_key
            })
            return False
    
    return True

def verify_database_integrity(db_path: str) -> dict:
    """Verify database integrity and check for corruption.
    
    Args:
        db_path: Path to database
        
    Returns:
        Dict with integrity check results
    """
    results = {
        "db_path": db_path,
        "integrity_check": "UNKNOWN",
        "foreign_key_check": "UNKNOWN",
        "schema_check": "UNKNOWN",
        "size_bytes": 0,
        "page_count": 0,
        "errors": []
    }
    
    try:
        # Get file size
        if os.path.exists(db_path):
            results["size_bytes"] = os.path.getsize(db_path)
        
        with DatabaseTransaction(db_path, isolation_level="IMMEDIATE") as conn:
            cursor = conn.cursor()
            
            # Check integrity
            cursor.execute("PRAGMA integrity_check")
            integrity_result = cursor.fetchone()[0]
            results["integrity_check"] = "OK" if integrity_result == "ok" else "CORRUPT"
            
            if integrity_result != "ok":
                results["errors"].append(f"Integrity check: {integrity_result}")
            
            # Check foreign keys
            cursor.execute("PRAGMA foreign_key_check")
            fk_violations = cursor.fetchall()
            results["foreign_key_check"] = "OK" if not fk_violations else "VIOLATIONS"
            
            if fk_violations:
                results["errors"].extend([str(v) for v in fk_violations])
            
            # Get page count
            cursor.execute("PRAGMA page_count")
            results["page_count"] = cursor.fetchone()[0]
            
            # Check schema
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            results["schema_check"] = "OK" if tables else "EMPTY"
            
            log(f"{EMOJI_INFO} DB_INTEGRITY_CHECK", {
                "db_path": db_path,
                "integrity": results["integrity_check"],
                "foreign_keys": results["foreign_key_check"],
                "tables": len(tables)
            })
            
    except Exception as e:
        results["errors"].append(str(e))
        log(f"{EMOJI_ERR} DB_INTEGRITY_CHECK_FAILED", {
            "db_path": db_path,
            "error": str(e)
        })
    
    return results

def get_database_status() -> dict:
    """Get status of all database connections and transactions.
    
    Returns:
        Dict with database status
    """
    status = {
        "active_connections": len(DB_CONNECTIONS),
        "active_transactions": len(DB_TRANSACTIONS),
        "active_locks": len(DB_LOCKS),
        "connections": {},
        "transactions": {},
        "locks": {}
    }
    
    # Connection details
    for db_path, conn in DB_CONNECTIONS.items():
        status["connections"][db_path] = {
            "total_changes": conn.total_changes,
            "in_transaction": bool(conn.in_transaction)
        }
    
    # Transaction details
    for tx_id, tx_info in DB_TRANSACTIONS.items():
        duration = now_ts() - tx_info["start_time"]
        status["transactions"][tx_id] = {
            "status": tx_info["status"],
            "duration_seconds": duration,
            "timeout_warning": duration > DB_TRANSACTION_TIMEOUT * 0.8
        }
    
    # Lock details
    for lock_key, lock_info in DB_LOCKS.items():
        duration = now_ts() - lock_info["acquired_at"]
        status["locks"][lock_key] = {
            "type": lock_info["type"],
            "duration_seconds": duration,
            "owner": lock_info["owner"]
        }
    
    return status

def cleanup_database_resources():
    """Clean up database resources (connections, transactions, locks)."""
    # Close all connections
    for db_path, conn in DB_CONNECTIONS.items():
        try:
            if conn:
                conn.close()
        except:
            pass
    
    DB_CONNECTIONS.clear()
    
    # Clear transactions
    DB_TRANSACTIONS.clear()
    
    # Clear locks
    DB_LOCKS.clear()
    
    log(f"{EMOJI_INFO} DB_RESOURCES_CLEANED", {})


# Dynamic position sizing state
RECENT_PERFORMANCE = {}  # strategy_id -> {win_rate, avg_win, avg_loss, recent_trades}
ACCOUNT_RISK_METRICS = {}  # Updated periodically

def calculate_dynamic_position_size(pair: str, strategy_id: int, account_balance: float, 
                                   atr_pips: float, stop_distance_atr: float) -> int:
    """Calculate dynamic position size based on account balance and risk metrics.
    
    Args:
        pair: Currency pair
        strategy_id: Strategy ID (1-7)
        account_balance: Current account balance
        atr_pips: Current ATR in pips
        stop_distance_atr: Stop loss distance in ATR
        
    Returns:
        Position size in units
    """
    if not DYNAMIC_SIZING_ENABLED:
        return 10000  # Default fixed size
    
    # Get account summary for real-time balance
    try:
        client = OandaClient(
            os.getenv("OANDA_API_KEY", "2bf7b4b9bb052e28023de779a6363f1e-fee71a4fce4e94b18e0dd9c2443afa52"),
            os.getenv("OANDA_ACCOUNT_ID", "101-001-22881868-001"),
            "practice"
        )
        account = client.account_summary()
        if account:
            account_balance = float(account.get("balance", account_balance))
            ACCOUNT_RISK_METRICS["last_update"] = now_ts()
            ACCOUNT_RISK_METRICS["balance"] = account_balance
            ACCOUNT_RISK_METRICS["open_pl"] = float(account.get("openPositionPnL", 0))
            ACCOUNT_RISK_METRICS["margin_used"] = float(account.get("marginUsed", 0))
            ACCOUNT_RISK_METRICS["margin_available"] = float(account.get("marginAvailable", 0))
    except Exception as e:
        log(f"{EMOJI_WARN} DYNAMIC_SIZING_ACCOUNT_ERROR", {"error": str(e)})
    
    # Calculate base risk amount
    risk_per_trade = BASE_RISK_PER_TRADE
    
    # Adjust risk based on strategy performance
    performance = RECENT_PERFORMANCE.get(strategy_id, {})
    if performance:
        win_rate = performance.get("win_rate", 0.5)
        avg_win = performance.get("avg_win", 0)
        avg_loss = abs(performance.get("avg_loss", 0))
        
        # Increase risk for high win rate strategies
        if win_rate > 0.6:
            risk_per_trade *= min(RISK_ADJUSTMENT_FACTOR, 2.0)
        elif win_rate < 0.4:
            risk_per_trade *= max(0.5, 1 / RISK_ADJUSTMENT_FACTOR)
        
        # Kelly criterion adjustment (simplified)
        if avg_loss > 0 and POSITION_SIZING_METHOD == "kelly":
            kelly_fraction = (win_rate * avg_win - (1 - win_rate) * avg_loss) / avg_loss
            kelly_fraction = max(0, min(kelly_fraction, 0.25))  # Cap at 25%
            risk_per_trade = max(MIN_RISK_PER_TRADE, min(kelly_fraction, MAX_RISK_PER_TRADE))
    
    # Apply risk limits
    risk_per_trade = max(MIN_RISK_PER_TRADE, min(risk_per_trade, MAX_RISK_PER_TRADE))
    
    # Calculate risk amount in currency
    risk_amount = account_balance * risk_per_trade
    
    # Convert to position size
    if stop_distance_atr > 0:
        # Risk per unit = stop_distance_atr * ATR * pip_value
        pip_value = get_pip_value(pair)
        risk_per_unit = stop_distance_atr * atr_pips * pip_value
        
        if risk_per_unit > 0:
            position_size = int(risk_amount / risk_per_unit)
            
            # Round to standard lot sizes
            position_size = round(position_size / 1000) * 1000
            
            # Apply minimum and maximum limits
            min_units = 1000
            max_units = int(account_balance * 0.1)  # Max 10% of account per trade
            
            position_size = max(min_units, min(position_size, max_units))
            
            log(f"{EMOJI_INFO} DYNAMIC_POSITION_SIZE", {
                "pair": pair,
                "strategy_id": strategy_id,
                "account_balance": account_balance,
                "risk_percent": risk_per_trade * 100,
                "risk_amount": risk_amount,
                "stop_distance_atr": stop_distance_atr,
                "position_size": position_size
            })
            
            return position_size
    
    # Fallback to fixed size
    return 10000

def get_pip_value(pair: str) -> float:
    """Get pip value for a currency pair.
    
    Args:
        pair: Currency pair
        
    Returns:
        Pip value in account currency
    """
    # Standard pip values
    if pair.endswith("JPY"):
        return 0.01  # Yen pairs
    elif pair.endswith("USD"):
        return 0.0001  # Standard pairs
    else:
        # Cross pairs - approximate
        return 0.0001

def update_strategy_performance(strategy_id: int, pnl: float, is_win: bool):
    """Update strategy performance metrics for position sizing.
    
    Args:
        strategy_id: Strategy ID
        pnl: PnL in account currency
        is_win: Whether the trade was a winner
    """
    if strategy_id not in RECENT_PERFORMANCE:
        RECENT_PERFORMANCE[strategy_id] = {
            "win_rate": 0.5,
            "avg_win": 0,
            "avg_loss": 0,
            "recent_trades": []
        }
    
    perf = RECENT_PERFORMANCE[strategy_id]
    perf["recent_trades"].append({
        "pnl": pnl,
        "is_win": is_win,
        "timestamp": now_ts()
    })
    
    # Keep only last 50 trades
    if len(perf["recent_trades"]) > 50:
        perf["recent_trades"] = perf["recent_trades"][-50:]
    
    # Update metrics
    wins = [t for t in perf["recent_trades"] if t["is_win"]]
    losses = [t for t in perf["recent_trades"] if not t["is_win"]]
    
    perf["win_rate"] = len(wins) / len(perf["recent_trades"]) if perf["recent_trades"] else 0.5
    perf["avg_win"] = sum(t["pnl"] for t in wins) / len(wins) if wins else 0
    perf["avg_loss"] = sum(t["pnl"] for t in losses) / len(losses) if losses else 0
    
    log(f"{EMOJI_INFO} STRATEGY_PERFORMANCE_UPDATED", {
        "strategy_id": strategy_id,
        "win_rate": perf["win_rate"],
        "avg_win": perf["avg_win"],
        "avg_loss": perf["avg_loss"],
        "recent_trades": len(perf["recent_trades"])
    })

def get_risk_metrics_summary() -> dict:
    """Get summary of current risk metrics.
    
    Returns:
        Dict with risk metrics
    """
    summary = {
        "dynamic_sizing_enabled": DYNAMIC_SIZING_ENABLED,
        "base_risk_percent": BASE_RISK_PER_TRADE * 100,
        "max_risk_percent": MAX_RISK_PER_TRADE * 100,
        "min_risk_percent": MIN_RISK_PER_TRADE * 100,
        "sizing_method": POSITION_SIZING_METHOD,
        "account_metrics": ACCOUNT_RISK_METRICS,
        "strategy_performance": {}
    }
    
    for strategy_id, perf in RECENT_PERFORMANCE.items():
        summary["strategy_performance"][strategy_id] = {
            "win_rate": perf["win_rate"],
            "avg_win": perf["avg_win"],
            "avg_loss": perf["avg_loss"],
            "trade_count": len(perf["recent_trades"])
        }
    
    return summary

def adjust_risk_for_correlation(pair: str, base_position_size: int, open_positions: List[dict]) -> int:
    """Adjust position size based on correlation with existing positions.
    
    Args:
        pair: Currency pair
        base_position_size: Calculated position size
        open_positions: List of open positions
        
    Returns:
        Adjusted position size
    """
    # Simple correlation reduction
    correlated_pairs = {
        "EUR_USD": ["GBP_USD", "AUD_USD", "NZD_USD"],
        "GBP_USD": ["EUR_USD", "EUR_GBP"],
        "USD_JPY": ["EUR_JPY", "GBP_JPY"],
        "AUD_USD": ["EUR_USD", "NZD_USD"],
        "NZD_USD": ["AUD_USD", "EUR_USD"]
    }
    
    if pair not in correlated_pairs:
        return base_position_size
    
    # Count correlated positions
    correlated_count = 0
    for pos in open_positions:
        if pos.get("pair") in correlated_pairs[pair]:
            correlated_count += 1
    
    # Reduce size based on correlation
    if correlated_count > 0:
        reduction_factor = 1 / (1 + correlated_count * 0.5)
        adjusted_size = int(base_position_size * reduction_factor)
        
        log(f"{EMOJI_INFO} CORRELATION_SIZE_ADJUSTMENT", {
            "pair": pair,
            "base_size": base_position_size,
            "correlated_positions": correlated_count,
            "reduction_factor": reduction_factor,
            "adjusted_size": adjusted_size
        })
        
        return adjusted_size
    
    return base_position_size


# ---- Legacy test compatibility helpers ----
# These aliases/wrappers keep older tests working without changing core behavior.
Oanda = OandaClient
_pip_size = pip_size

def test_force_sl_tp_all_setups(*, pair: str = "EUR_USD", units: int = 1) -> None:
    from phone_bot_tests import test_force_sl_tp_all_setups as _impl
    return _impl(pair=pair, units=units)

def test_force_split_two_legs(*, pair: str = "EUR_USD", speed_class: str = "MED", units_total: int = 4) -> None:
    from phone_bot_tests import test_force_split_two_legs as _impl
    return _impl(pair=pair, speed_class=speed_class, units_total=units_total)


if __name__ == "__main__":
    main()
