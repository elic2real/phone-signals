#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import time
import json
import math
import sqlite3
import signal
import subprocess
import random
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Any

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

# MAX_HTTP_PER_MIN = 120  # Removed - no longer rate limiting

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
    if isinstance(t, (int, float)):
        # UNIX timestamp (may be in milliseconds)
        if t > 1e12:  # Milliseconds
            return float(t) / 1000.0
        return float(t)
    else:
        # RFC3339 format
        ts = str(t)
        # Handle trailing 'Z'
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(ts).timestamp()
        except Exception:
            return float("nan")


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


# Adaptive Exit Engine (AEE) - Core functions (Updated for Path-Space)
def calculate_aee_metrics(trade: dict, current_price: float, atr: float, candles: List[dict]) -> dict:
    """Calculate all AEE metrics for a trade using path-space primitives"""
    
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
            
            return {
                "progress": progress,
                "speed": speed,
                "velocity": velocity,
                "pullback": pullback,
                "local_high": local_high,
                "local_low": local_low,
                "efficiency": efficiency,
                "overlap": overlap,
                "data_quality": data_quality,
                "source": "path_space"
            }
    except Exception as e:
        # Fallback to candle-based calculation if path-space fails
        log(f"{EMOJI_WARN} AEE_PATH_SPACE_FALLBACK {pair}", {"error": str(e)})
    
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
    return atr_price / ps if ps > 0 else float("nan")


def spread_atr(pair: str, spread_pips: float, atr_price: float) -> float:
    """Calculate spread as fraction of ATR (universal metric)."""
    ap = atr_pips(pair, atr_price)
    if not math.isfinite(ap) or ap <= 0:
        return float("nan")
    return spread_pips / ap


# Spread thresholds for each speed class (S0, S1)
SPREAD_ATR_THRESH = {
    "FAST": (0.10, 0.45),
    "MED":  (0.12, 0.55),
    "SLOW": (0.15, 0.70),
}

# Absolute maximum spread as crash guard
ABS_SPREAD_MAX_PIPS = 15.0

# F/E spread sizing (universal)
SPREAD_F_MAX = float(os.getenv("SPREAD_F_MAX", "0.8") or "0.8")
SPREAD_SIZE_ALPHA = float(os.getenv("SPREAD_SIZE_ALPHA", "0.7") or "0.7")
SPREAD_SIZE_MIN = float(os.getenv("SPREAD_SIZE_MIN", "0.25") or "0.25")
SPREAD_SIZE_EPS = float(os.getenv("SPREAD_SIZE_EPS", "0.1") or "0.1")

# Spread spike handling (microstructure shock)
SPREAD_SPIKE_WINDOW = 20
SPREAD_SPIKE_MIN_SAMPLES = 5
SPREAD_SPIKE_THRESHOLD = 2.0
SPREAD_SPIKE_MULT = 0.5


def spread_size_mult(speed_class: str, s_atr: float) -> float:
    """Calculate size multiplier based on spread/ATR ratio."""
    if not math.isfinite(s_atr):
        return 0.0
    s0, s1 = SPREAD_ATR_THRESH.get(speed_class, (0.12, 0.55))
    if s_atr <= s0:
        return 1.0
    if s_atr >= s1:
        return 0.0
    return 1.0 - (s_atr - s0) / (s1 - s0)


def spread_size_mult_fe(spread_atr: float, speed_norm: float) -> float:
    """F/E sizing multiplier: M = clip(1 - alpha * F/(E+eps), M_min, 1)."""
    if not math.isfinite(spread_atr):
        return 0.0
    if spread_atr > SPREAD_F_MAX:
        return 0.0
    e = speed_norm if (math.isfinite(speed_norm) and speed_norm > 0.0) else 0.0
    m = 1.0 - (SPREAD_SIZE_ALPHA * spread_atr / (e + SPREAD_SIZE_EPS))
    return clamp(m, SPREAD_SIZE_MIN, 1.0)


def tick_size(pair: str) -> float:
    # OANDA typically quotes non-JPY to 5 decimals (tick=1e-5) and JPY to 3 decimals (tick=1e-3).
    # Added HUF and THB for safety
    pair = normalize_pair(pair)
    if "JPY" in pair or "HUF" in pair or "THB" in pair:
        return 0.001
    return 0.00001


def to_pips(pair: str, price_delta: float) -> float:
    return float(price_delta) / pip_size(pair)


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def round_tick(x: float, pair: str) -> float:
    tick = tick_size(pair)
    return round(round(x / tick) * tick, 6)


def round_tick_down(x: float, pair: str) -> float:
    tick = tick_size(pair)
    if tick <= 0:
        return x
    return round(math.floor(float(x) / tick) * tick, 6)


def round_tick_up(x: float, pair: str) -> float:
    tick = tick_size(pair)
    if tick <= 0:
        return x
    return round(math.ceil(float(x) / tick) * tick, 6)


def is_valid_price(x) -> bool:
    return x is not None and math.isfinite(x) and x > 0.0


def _safe_float(x, default: float = float("nan")) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default

EMOJI_START = "🚀"
EMOJI_STOP = "🛑"
EMOJI_INFO = "ℹ️"
EMOJI_WARN = "⚠️"
EMOJI_ERR = "❌"
EMOJI_OK = "✅"
EMOJI_SCAN = "🔎"
EMOJI_SIGNAL = "📣"
EMOJI_ENTER = "🎯"
EMOJI_ORDER = "🧾"
EMOJI_EXIT = "🚪"
EMOJI_DB = "🗄️"
EMOJI_RL = "⏱️"
EMOJI_UP = "📈"
EMOJI_DN = "📉"
EMOJI_WATCH = "👁️"
EMOJI_GET_READY = "⚡"

# Termux notification tuning
TERMUX_VIBRATE_PATTERN = os.getenv("TERMUX_VIBRATE_PATTERN", "350,150,350")
TERMUX_VIBRATE_MS = int(os.getenv("TERMUX_VIBRATE_MS", "250") or "250")
TERMUX_LED_COLOR = os.getenv("TERMUX_LED_COLOR", "").strip()
TERMUX_LED_ON_MS = int(os.getenv("TERMUX_LED_ON_MS", "350") or "350")
TERMUX_LED_OFF_MS = int(os.getenv("TERMUX_LED_OFF_MS", "350") or "350")


def dir_emoji(direction: str) -> str:
    return EMOJI_UP if str(direction).upper() == "LONG" else EMOJI_DN


def pair_tag(pair: str, direction: Optional[str] = None) -> str:
    pair = normalize_pair(pair)
    if direction:
        return f"{dir_emoji(direction)} {pair}"
    return f"{pair}"


def _run_notify_cmd(argv: List[str], *, timeout: float) -> bool:
    try:
        r = subprocess.run(argv, timeout=timeout, capture_output=True)
        return r.returncode == 0
    except Exception:
        return False


def notify(title: str, msg: str) -> None:
    # Primary target: Termux (Android).
    termux_cmd = [
        "termux-notification",
        "-t",
        str(title),
        "-c",
        str(msg),
        "--priority",
        "high",
        "--vibrate",
        str(TERMUX_VIBRATE_PATTERN),
        "--sound",
    ]
    if TERMUX_LED_COLOR:
        termux_cmd += [
            "--led-color",
            str(TERMUX_LED_COLOR),
            "--led-on",
            str(TERMUX_LED_ON_MS),
            "--led-off",
            str(TERMUX_LED_OFF_MS),
        ]
    if _run_notify_cmd(termux_cmd, timeout=2):
        _run_notify_cmd(["termux-vibrate", "-d", str(TERMUX_VIBRATE_MS)], timeout=1)
        return

    # Desktop fallbacks.
    # Linux: libnotify
    if _run_notify_cmd(["notify-send", "-u", "critical", str(title), str(msg)], timeout=2):
        return
    # Linux GUI fallback
    if _run_notify_cmd(["zenity", "--notification", f"--text={title}: {msg}"], timeout=2):
        return
    # macOS fallback
    if _run_notify_cmd(
        [
            "osascript",
            "-e",
            f'display notification "{str(msg).replace("\"", "\\\"")}" with title "{str(title).replace("\"", "\\\"")}"',
        ],
        timeout=2,
    ):
        return
    
    # Windows fallback
    if _run_notify_cmd(
        ["powershell", "-Command", f"Add-Type -AssemblyName System.Windows.Forms; [System.Windows.Forms.MessageBox]::Show('{msg}', '{title}')"],
        timeout=2,
    ):
        return


def log(msg: str, meta: Optional[dict] = None) -> None:
    line = f"[{ts_str()}] {msg}"
    if meta:
        try:
            meta_json = json.dumps(meta, default=str)
        except Exception as e:
            meta_json = str(meta)
            print(f"[{ts_str()}] {EMOJI_WARN} LOG_META_FAIL | {e}", flush=True)
        line += f" | {meta_json}"
    print(line, flush=True)
    try:
        if os.path.exists(LOG_PATH) and os.path.getsize(LOG_PATH) > MAX_LOG_SIZE:
            os.rename(LOG_PATH, LOG_PATH + ".old")
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as e:
        print(f"[{ts_str()}] {EMOJI_WARN} LOG_WRITE_FAIL | {e}", flush=True)


_LOG_THROTTLE: Dict[str, float] = {}


def log_throttled(key: str, msg: str, meta: Optional[dict] = None, min_interval: float = 30.0) -> None:
    now = now_ts()
    last = _LOG_THROTTLE.get(key, 0.0)
    if (now - last) >= min_interval:
        _LOG_THROTTLE[key] = now
        log(msg, meta)


# RateLimiter class removed - OANDA allows 120 requests/second, no artificial limits needed


def validate_price(pair: str, bid: float, ask: float, ctx: str) -> bool:
    pair = normalize_pair(pair)
    if not (is_valid_price(bid) and is_valid_price(ask)):
        log_throttled(
            f"price_invalid:{pair}:{ctx}",
            f"{EMOJI_WARN} INVALID_PRICE {pair_tag(pair)}",
            {"pair": pair, "bid": bid, "ask": ask, "ctx": ctx},
        )
        return False
    if ask < bid:
        log_throttled(
            f"price_inverted:{pair}:{ctx}",
            f"{EMOJI_WARN} INVALID_SPREAD {pair_tag(pair)}",
            {"pair": pair, "bid": bid, "ask": ask, "ctx": ctx},
        )
        return False
    return True


def validate_candles(pair: str, candles: List[dict], tf_sec: int = 0, allow_partial: bool = True) -> Tuple[bool, str]:
    """Validate candle data. Rejects future candles and stale feed."""
    pair = normalize_pair(pair)
    if not candles:
        return False, "empty_candles"
    
    now = now_ts()
    tolerance = 1.0  # 1 second tolerance for future candles
    
    for idx, c in enumerate(candles):
        # Basic OHLC sanity
        if not all(k in c for k in ("o", "h", "l", "c")):
            return False, f"missing_ohlc_idx_{idx}"
        
        try:
            o = float(c.get("o", 0.0))
            h = float(c.get("h", 0.0))
            l = float(c.get("l", 0.0))
            cl = float(c.get("c", 0.0))
        except (TypeError, ValueError):
            return False, f"bad_ohlc_idx_{idx}"
        
        if not (is_valid_price(o) and is_valid_price(h) and is_valid_price(l) and is_valid_price(cl)):
            return False, f"nonfinite_ohlc_idx_{idx}"
        
        if not (h >= max(o, cl) and l <= min(o, cl)):
            return False, f"ohlc_sanity_idx_{idx}"
        
        # Check for future candle
        candle_time = float(c["time"])
        if candle_time > now + tolerance:
            log(f"❌ FUTURE_CANDLE {pair} | {{\"pair\": \"{pair}\", \"candle_idx\": {idx}, \"candle_time\": {candle_time}, \"current_time\": {now}}}")
            return False, f"future_candle_idx_{idx}"
        
        # Allow partial candles by default now
        if not allow_partial and not bool(c.get("complete", True)):
            return False, f"partial_candle_idx_{idx}"
    
    return True, ""


def log_trade_attempt(
    *,
    pair: str,
    sig: "SignalDef",
    st: "PairState",
    speed_class: str,
    decision: str,
    reason: str,
    leg: str = "MAIN",
    state_from: Optional[str] = None,
    state_to: Optional[str] = None,
    extra: Optional[dict] = None,
    bar_complete: Optional[bool] = None,
    bar_age_ms: Optional[float] = None,
) -> None:
    pair = normalize_pair(pair)
    meta = {
        "pair": pair,
        "setup_id": sig.setup_id,
        "setup_name": sig.setup_name,
        "speed_class": speed_class,
        "decision": decision,
        "reason": reason,
        "leg": leg,
        "mode": st.mode,
        "state_from": state_from or st.state,
        "state_to": state_to or st.state,
        "state_age_sec": round(now_ts() - st.state_since, 2),
        "atr": st.atr_exec,
        "atr_long": st.atr_long,
        "m_norm": st.m_norm,
        "wr": st.wr,
        "spread_pips": st.spread_pips,
        "signal_reason": sig.reason,
    }
    
    # Add bar completeness info if provided
    if bar_complete is not None:
        meta["bar_complete"] = bar_complete
    if bar_age_ms is not None:
        meta["bar_age_ms"] = round(bar_age_ms, 0)
    
    # Add candle time if available
    if hasattr(st, 'last_candle_time') and st.last_candle_time > 0:
        meta["candle_time"] = st.last_candle_time
    
    # Add required friction metrics
    if math.isfinite(st.atr_exec) and st.atr_exec > 0:
        spread_atr = (st.spread_pips * pip_size(pair)) / st.atr_exec
        meta["spread_atr"] = round(spread_atr, 3)
        # edge_est is TP1 ATR - spread_atr
        tp1_atr = getattr(sig, 'tp1_atr', 1.0)
        meta["edge_est"] = round(tp1_atr - spread_atr, 2)
    if extra:
        meta.update(extra)
    log(f"{EMOJI_SIGNAL} ATTEMPT {pair_tag(pair, sig.direction)}", meta)

class OandaClient:
    """Canonical OANDA v3 REST API client for Termux bot.
    
    Centralizes all HTTP operations with proper error handling,
    rate limiting, and retry logic. Single-file implementation.
    """
    def __init__(self, api_key: str, account_id: str, env: str):
        """Initialize OANDA client.
        
        Args:
            api_key: OANDA API key
            account_id: Account ID for trading
            env: 'practice' or 'live'
        """
        if not _HAS_REQUESTS:
            print("ERROR: requests library not available. Install with: pip install requests")
            sys.stdout.flush()
            # Disable API functionality gracefully
            self.api_key = None
            self.account_id = None
            self.env = None
            self.base = None
            self.sess = None
            self.rate_limited_until = 0.0
            return
            
        self.api_key = api_key.strip()
        self.account_id = str(account_id or "").strip()
        self.env = normalize_oanda_env(env)
        if not self.env:
            raise ValueError(f"invalid_oanda_env:{env}")
            
        self.base = "https://api-fxpractice.oanda.com" if self.env == "practice" else "https://api-fxtrade.oanda.com"
        self.sess = requests.Session()  # type: ignore[attr-defined]
        self.rate_limited_until = 0.0
        self.hdr = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Accept-Datetime-Format": "UNIX",
        }

    def _note_rate_limit(self, r: "requests.Response") -> float:  # type: ignore[name-defined]
        """Handle rate limiting from OANDA API.
        
        Updates internal rate limit timestamp and returns wait duration.
        """
        try:
            retry_after = r.headers.get("Retry-After")
            wait = float(retry_after) if retry_after is not None else 30.0
        except Exception:
            wait = 30.0
        self.rate_limited_until = max(self.rate_limited_until, now_ts() + wait)
        return wait

    def _expect_ok(self, j: dict, ctx: str) -> dict:
        """Validate API response and raise errors if needed.
        
        Args:
            j: Response dict from HTTP methods
            ctx: Context string for error reporting
            
        Returns:
            Valid response dict
            
        Raises:
            RuntimeError: On any API error
        """
        if not isinstance(j, dict):
            raise RuntimeError(f"{ctx}: invalid_response")
        if j.get("_rate_limited"):
            raise RuntimeError(f"{ctx}: rate_limited")
        if j.get("_http_error"):
            raise RuntimeError(f"{ctx}: http_{j.get('_status')}")
        if j.get("_json_error"):
            raise RuntimeError(f"{ctx}: json_error")
        # Log OANDA error messages for broker rejections
        if "errorMessage" in j:
            # Extract pair from context if possible
            pair = "unknown"
            if ":" in ctx:
                parts = ctx.split(":")
                if len(parts) >= 2 and parts[1] in PAIRS:
                    pair = parts[1]
                elif "order" in ctx.lower() and len(parts) >= 3:
                    pair = parts[2]
            log_broker_reject(
                "api_error",
                pair,
                {
                    "context": ctx,
                    "errorMessage": j.get("errorMessage"),
                    "rejectReason": j.get("rejectReason"),
                    "lastTransactionID": j.get("lastTransactionID"),
                    "relatedTransactionIDs": j.get("relatedTransactionIDs"),
                    "raw_response": j
                }
            )
        return j

    def _check_api_available(self) -> bool:
        """Check if requests library is available for API calls.
        
        Returns:
            True if API is available, False otherwise
        """
        return _HAS_REQUESTS and self.sess is not None
        
    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        """Perform GET request to OANDA API.
        
        Args:
            path: API endpoint path
            params: Query parameters
            
        Returns:
            Response dict or error dict
        """
        if not self._check_api_available():
            return {"_http_error": True, "_status": 503, "_text": "API not available - requests library missing"}
            
        url = self.base + path
        r = self.sess.get(url, headers=self.hdr, params=params, timeout=18)
        
        if r.status_code == 429:
            wait = self._note_rate_limit(r)
            return {"_rate_limited": True, "_status": 429, "_text": r.text[:160], "_retry_after": wait}
        if r.status_code >= 400:
            return {"_http_error": True, "_status": r.status_code, "_text": r.text[:200]}
            
        try:
            return r.json()
        except Exception as e:
            return {"_json_error": True, "_err": str(e), "_text": r.text[:200]}

    def _post(self, path: str, body: dict) -> dict:
        """Perform POST request to OANDA API.
        
        Args:
            path: API endpoint path
            body: Request body as dict
            
        Returns:
            Response dict or error dict
        """
        if not self._check_api_available():
            return {"_http_error": True, "_status": 503, "_text": "API not available - requests library missing"}
            
        url = self.base + path
        r = self.sess.post(url, headers=self.hdr, json=body, timeout=18)
        
        if r.status_code == 429:
            wait = self._note_rate_limit(r)
            return {"_rate_limited": True, "_status": 429, "_text": r.text[:160], "_retry_after": wait}
        if r.status_code >= 400:
            return {"_http_error": True, "_status": r.status_code, "_text": r.text[:200]}
            
        try:
            return r.json()
        except Exception as e:
            return {"_json_error": True, "_err": str(e), "_text": r.text[:200]}

    def _put(self, path: str, body: dict) -> dict:
        """Perform PUT request to OANDA API.
        
        Args:
            path: API endpoint path
            body: Request body as dict
            
        Returns:
            Response dict or error dict
        """
        if not self._check_api_available():
            return {"_http_error": True, "_status": 503, "_text": "API not available - requests library missing"}
            
        url = self.base + path
        r = self.sess.put(url, headers=self.hdr, json=body, timeout=18)
        
        if r.status_code == 429:
            wait = self._note_rate_limit(r)
            return {"_rate_limited": True, "_status": 429, "_text": r.text[:160], "_retry_after": wait}
        if r.status_code >= 400:
            return {"_http_error": True, "_status": r.status_code, "_text": r.text[:200]}
            
        try:
            return r.json()
        except Exception as e:
            return {"_json_error": True, "_err": str(e), "_text": r.text[:200]}

    def account_summary(self) -> dict:
        """Get account summary information.
        
        Returns:
            Account summary dict with balance, P&L, etc.
            
        Raises:
            RuntimeError: On API errors or missing data
        """
        j = self._expect_ok(self._get(f"/v3/accounts/{self.account_id}/summary"), "account_summary")
        acct = j.get("account")
        if not isinstance(acct, dict):
            raise RuntimeError("account_summary: missing_account")
        return acct

    def pricing(self, pair: str) -> Tuple[float, float]:
        """Get current bid/ask pricing for a pair.
        
        Args:
            pair: Currency pair (e.g., 'EUR_USD')
            
        Returns:
            Tuple of (bid, ask) prices
            
        Raises:
            RuntimeError: On API errors or invalid data
        """
        pair = normalize_pair(pair)
        j = self._expect_ok(self._get(f"/v3/accounts/{self.account_id}/pricing", params={"instruments": pair}), f"pricing:{pair}")
        prices = j.get("prices", [])
        if not prices:
            raise RuntimeError(f"no pricing for {pair}")
        p = prices[0]
        bids = p.get("bids", [])
        asks = p.get("asks", [])
        bid = float(bids[0]["price"]) if bids else float("nan")
        ask = float(asks[0]["price"]) if asks else float("nan")
        if not (is_valid_price(bid) and is_valid_price(ask)):
            raise RuntimeError(f"invalid pricing bid/ask for {pair}: {bid}/{ask}")
        if ask < bid:
            raise RuntimeError(f"invalid pricing ask<bid for {pair}: {bid}/{ask}")
        return bid, ask

    def pricing_multi(self, pairs: List[str]) -> Dict[str, Tuple[float, float]]:
        """Get current bid/ask pricing for multiple pairs.
        
        Args:
            pairs: List of currency pairs
            
        Returns:
            Dict mapping pair to (bid, ask) tuples
        """
        pairs_norm: List[str] = []
        seen = set()
        for p in pairs:
            np = normalize_pair(p)
            if np and np not in seen:
                pairs_norm.append(np)
                seen.add(np)
        if not pairs_norm:
            return {}
        instruments = ",".join(pairs_norm)
        j = self._expect_ok(
            self._get(f"/v3/accounts/{self.account_id}/pricing", params={"instruments": instruments}),
            f"pricing_multi:{instruments}",
        )
        prices = j.get("prices", [])
        out: Dict[str, Tuple[float, float]] = {}
        for p in prices:
            inst = normalize_pair(p.get("instrument"))
            bids = p.get("bids", [])
            asks = p.get("asks", [])
            try:
                bid = float(bids[0]["price"]) if bids else float("nan")
                ask = float(asks[0]["price"]) if asks else float("nan")
            except (TypeError, ValueError):
                continue
            if not (is_valid_price(bid) and is_valid_price(ask)):
                continue
            if ask < bid:
                continue
            if inst:
                out[str(inst)] = (bid, ask)
        return out

    def candles(self, pair: str, gran: str, count: int) -> List[dict]:
        """Get candle data for a pair.
        
        Args:
            pair: Currency pair
            gran: Granularity (e.g., 'M5', 'H1')
            count: Number of candles to fetch
            
        Returns:
            List of candle dicts with OHLC data
        """
        pair = normalize_pair(pair)
        gran = normalize_granularity(gran)
        j = self._expect_ok(
            self._get(
            f"/v3/instruments/{pair}/candles",
            params={"granularity": gran, "count": str(int(count)), "price": "M"},
            ),
            f"candles:{pair}:{gran}",
        )
        out: List[dict] = []
        bad = 0
        for c in j.get("candles", []):
            complete = bool(c.get("complete", True))
            m = c.get("mid", {})
            try:
                o = float(m.get("o", 0.0))
                h = float(m.get("h", 0.0))
                l = float(m.get("l", 0.0))
                cl = float(m.get("c", 0.0))
                ts = parse_time_oanda(c.get("time"))
            except (TypeError, ValueError):
                bad += 1
                continue
            if not math.isfinite(ts):
                bad += 1
                continue
            if not (is_valid_price(o) and is_valid_price(h) and is_valid_price(l) and is_valid_price(cl)):
                bad += 1
                continue
            if not (h >= l and h >= max(o, cl) and l <= min(o, cl)):
                bad += 1
                continue
            out.append({"time": ts, "complete": complete, "o": o, "h": h, "l": l, "c": cl})
        if bad > 0:
            raise RuntimeError(f"invalid_candles:{pair}:{bad}")
        return out

    def trade(self, trade_id: str) -> dict:
        """Get trade details by ID.
        
        Args:
            trade_id: OANDA trade ID
            
        Returns:
            Trade details dict
        """
        j = self._expect_ok(self._get(f"/v3/accounts/{self.account_id}/trades/{trade_id}"), f"trade:{trade_id}")
        tr = j.get("trade")
        if not isinstance(tr, dict):
            raise RuntimeError("trade: missing_trade")
        return tr

    def close_trade(self, trade_id: str) -> dict:
        """Close a specific trade by ID.
        
        Args:
            trade_id: OANDA trade ID
            
        Returns:
            Close trade response dict
        """
        return self._put(f"/v3/accounts/{self.account_id}/trades/{trade_id}/close", {})

    def update_stop_loss_order(self, order_id: str, price: float) -> dict:
        """Update a stop loss order price.
        
        Args:
            order_id: OANDA order ID
            price: New stop loss price
            
        Returns:
            Response dict or error dict
        """
        body = {"stopLossOnFill": {"price": str(round(price, 5))}}
        return self._put(f"/v3/accounts/{self.account_id}/orders/{order_id}", body)

    def set_trade_stop_loss(self, trade_id: str, price: float) -> dict:
        """Create or replace stop loss on an open trade."""
        body = {
            "stopLoss": {
                "price": str(round(price, 5)),
                "timeInForce": "GTC",
            }
        }
        return self._put(f"/v3/accounts/{self.account_id}/trades/{trade_id}/orders", body)

    def close_position(
        self,
        pair: str,
        direction: Optional[str] = None,
        *,
        long_units: Optional[str] = None,
        short_units: Optional[str] = None,
    ) -> dict:
        """Close position by pair and side.
        
        Args:
            pair: Currency pair
            direction: 'LONG' or 'SHORT' (optional)
            long_units: Number of long units to close (optional)
            short_units: Number of short units to close (optional)
            
        Returns:
            Close position response dict
        """
        pair = normalize_pair(pair)
        body: Dict[str, str] = {}
        if direction and not (long_units or short_units):
            d = str(direction or "").strip().upper()
            if d in ("LONG", "L", "BUY", "LONGS", "LONGUNIT", "LONGUNITS", "LONG_UNITS"):
                body["longUnits"] = "ALL"
            elif d in ("SHORT", "S", "SELL", "SHORTS", "SHORTUNIT", "SHORTUNITS", "SHORT_UNITS"):
                body["shortUnits"] = "ALL"
            else:
                if str(direction or "").lower().startswith("l"):
                    body["longUnits"] = "ALL"
                else:
                    body["shortUnits"] = "ALL"
        else:
            if long_units:
                body["longUnits"] = long_units
            if short_units:
                body["shortUnits"] = short_units
        return self._put(f"/v3/accounts/{self.account_id}/positions/{pair}/close", body)

    def open_positions(self) -> List[dict]:
        """Get all open positions.
        
        Returns:
            List of open position dicts
        """
        j = self._expect_ok(self._get(f"/v3/accounts/{self.account_id}/openPositions"), "open_positions")
        positions = j.get("positions")
        if not isinstance(positions, list):
            raise RuntimeError("open_positions: missing_positions")
        return positions

    def pending_orders(self) -> List[dict]:
        """Get all pending orders.
        
        Returns:
            List of pending order dicts
        """
        j = self._expect_ok(
            self._get(f"/v3/accounts/{self.account_id}/orders", params={"state": "PENDING"}), "pending_orders"
        )
        orders = j.get("orders")
        if not isinstance(orders, list):
            raise RuntimeError("pending_orders: missing_orders")
        return orders

    # NOTE: close_position definition consolidated above.

    def place_market(self, pair: str, units: int, sl: float, tp: float, *, client_id: str) -> dict:
        """Place a market order with TP-first ordering.
        
        TP-first implementation:
        1. Place order with TP only (no SL to avoid rejection)
        2. Add SL after successful fill
        
        Args:
            pair: Currency pair
            units: Position units (positive for long, negative for short)
            sl: Stop loss price
            tp: Take profit price
            client_id: Client order ID
            
        Returns:
            Order response dict
        """
        if not self._check_api_available():
            return {"_http_error": True, "_status": 503, "_text": "API not available"}
        
        # TP-first: Place with TP only
        body = {
            "order": {
                "type": "MARKET",
                "instrument": pair,
                "units": str(units),
                "timeInForce": "FOK",
                "positionFill": "DEFAULT",
                "clientExtensions": {"id": client_id},
                "takeProfitOnFill": {"price": str(round(tp, 5))},
            }
        }
        
        resp = self._post(f"/v3/accounts/{self.account_id}/orders", body)
        
        # If order filled, try to add SL after fill (retry on failure)
        if isinstance(resp, dict) and resp.get("orderFillTransaction"):
            trade_id = resp.get("orderFillTransaction", {}).get("tradeID")
            if trade_id:
                ok = False
                last_err: Optional[dict] = None
                for attempt in range(max(1, SL_RETRY_MAX)):
                    sl_resp = self.set_trade_stop_loss(trade_id, sl)
                    last_err = sl_resp if isinstance(sl_resp, dict) else None
                    if isinstance(sl_resp, dict) and not (
                        sl_resp.get("_http_error")
                        or sl_resp.get("_rate_limited")
                        or sl_resp.get("_json_error")
                        or sl_resp.get("_exception")
                    ):
                        ok = True
                        break
                    time.sleep(SL_RETRY_BASE_SEC * (2 ** attempt))
                if not ok:
                    log_broker_reject(
                        "sl_add_failed",
                        pair,
                        {
                            "trade_id": trade_id,
                            "sl": sl,
                            "attempts": SL_RETRY_MAX,
                            "last_error": last_err,
                        },
                    )
                    resp["_sl_add_failed"] = True
                else:
                    resp["_sl_add_failed"] = False
        
        return resp


def atr(candles: List[dict], n: int) -> float:
    if len(candles) < n + 1:
        return float("nan")
    
    trs: List[float] = []
    for i in range(-n, 0):
        try:
            h = float(candles[i]["h"])
            l = float(candles[i]["l"])
            pc = float(candles[i - 1]["c"])
            
            # Validate all values are finite
            if not (math.isfinite(h) and math.isfinite(l) and math.isfinite(pc)):
                continue
                
            tr = max(h - l, abs(h - pc), abs(l - pc))
            if math.isfinite(tr) and tr > 0:
                trs.append(tr)
        except (KeyError, ValueError, TypeError, IndexError):
            continue
    
    if not trs:
        return float("nan")
    
    atr_val = sum(trs) / len(trs)
    return atr_val if math.isfinite(atr_val) and atr_val > 0 else float("nan")


def momentum(candles: List[dict], n: int) -> float:
    """Return price momentum over last n candles (close-to-close)."""
    if len(candles) < n + 2:  
        return float("nan")
    try:
        c1 = float(candles[-1]["c"])
        c2 = float(candles[-1 - n]["c"])
        mom = (c1 - c2) / c1
        return mom if math.isfinite(mom) else float("nan")
    except (KeyError, ValueError, TypeError, IndexError):
        return float("nan")


def williams_r(candles: List[dict], n: int) -> float:
    if len(candles) < n:
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


def _velocity_atr_per_sec(samples: List[Tuple[float, float]]) -> float:
    if len(samples) < 2:
        return 0.0
    t1, v1 = samples[-2]
    t2, v2 = samples[-1]
    dt = t2 - t1
    if dt <= 0:
        return 0.0
    return (v2 - v1) / dt


def _microtrend_alive(samples: List[Tuple[float, float]], k: int = 3) -> bool:
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
            "reason": sig.reason,
            "size_mult": sig.size_mult,
        },
    }


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

def detect_mode(pair: str, current_high: float, current_low: float, atr: float) -> str:
    """
    Quantitatively determines Market Regime based on Volatility Ratio (VR).
    VR = Current Candle Range / ATR
    """
    try:
        # Safety check to avoid division by zero
        if atr <= 0:
            return "SLOW"

        # Calculate Realized Volatility of the current candle
        # This triggers IMMEDIATE reaction to crashes/news
        current_range = current_high - current_low
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
    pair = normalize_pair(pair)
    out: List[SignalDef] = []
    if len(c_exec) < max(ATR_N + 5, 35):
        return out
    if not (st.atr_exec > 0.0) or not math.isfinite(st.spread_pips):
        return out

    # Spread is handled in profit calculation, not as a hard gate
    # if st.spread_pips > SPREAD_MAX_PIPS:
    #     return out

    atrv = st.atr_exec
    m = st.m_norm
    box_hi, box_lo = st.box_hi, st.box_lo
    box_atr = st.box_atr
    close = float(c_exec[-1]["c"])
    last_ts = float(c_exec[-1].get("time", now_ts()))
    if not math.isfinite(last_ts):
        last_ts = now_ts()
    
    # Track bar completeness for logging
    bar_complete = bool(c_exec[-1].get("complete", True))
    bar_age_ms = (now_ts() - last_ts) * 1000 if not bar_complete else 0

    break_up = math.isfinite(box_hi) and close > box_hi
    break_dn = math.isfinite(box_lo) and close < box_lo
    reenter = math.isfinite(box_hi) and math.isfinite(box_lo) and (box_lo <= close <= box_hi)
    compression = math.isfinite(box_atr) and box_atr <= 1.20

    disp = abs(float(c_exec[-1]["c"]) - float(c_exec[-11]["c"]))
    disp_atr = disp / atrv if atrv > 0.0 else 0.0

    sw_up, sw_dn = wick_sweep(c_exec, SWEEP_L, atrv)
    if sw_up or sw_dn:
        speed_class = "FAST"
        sp = get_speed_params(speed_class)
        direction = "SHORT" if sw_up else "LONG"
        out.append(
            SignalDef(
                pair=pair,
                setup_id=5,
                setup_name="SWEEP_POP",
                direction=direction,
                mode=st.mode,
                ttl_sec=sp["ttl_main"],
                pg_t=int(sp["ttl_main"] * sp["pg_t_frac"]),
                pg_atr=sp["pg_atr"],
                tp1_atr=sp["tp1_atr"],
                tp2_atr=sp["tp2_atr"],
                sl_atr=sp["sl_atr"],
                reason=_with_friction_reason(
                    reason=f"sweep={'UP' if sw_up else 'DN'} wick>={SWEEP_ATR_THRESHOLD}ATR | bar_complete={bar_complete}",
                    pair=pair,
                    atr=atrv,
                    spread_pips=st.spread_pips,
                    tp1_atr=sp["tp1_atr"],
                    sl_atr=sp["sl_atr"],
                ),
            )
        )

    if break_up:
        st.breakout_ts = last_ts
        st.breakout_dir = "LONG"
    elif break_dn:
        st.breakout_ts = last_ts
        st.breakout_dir = "SHORT"

    if st.breakout_ts > 0.0 and (now_ts() - st.breakout_ts) <= 20.0:
        if reenter and (m < FAIL_BREAK_EXTENSION_MAX):  # Optimized to FAIL_BREAK_EXTENSION_MAX
            speed_class = "FAST"
            sp = get_speed_params(speed_class)
            direction = "SHORT" if st.breakout_dir == "LONG" else "LONG"
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
                        reason=f"break->reenter + m<{FAIL_BREAK_EXTENSION_MAX:.2f} | bar_complete={bar_complete}",
                        pair=pair,
                        atr=atrv,
                        spread_pips=st.spread_pips,
                        tp1_atr=sp["tp1_atr"],
                        sl_atr=sp["sl_atr"],
                    ),
                )
            )

    # Check compression width
    box_width_atr = (box_hi - box_lo) / atrv if atrv > 0 else float('inf')
    
    if compression and (box_width_atr < COMPRESSION_WIDTH_MAX) and (m >= 0.50) and (break_up ^ break_dn):
        # Minimum pip requirement disabled - using ATR-based filtering instead
        # For brokers with fixed pip requirements, uncomment and implement:
        # pip_move = abs(close - (box_hi if break_up else box_lo)) / pip_size(pair)
        # if pip_move < MIN_BREAKOUT_PIPS:
        #     return out  # Skip tiny breakouts
        
        speed_class = "MED"
        sp = get_speed_params(speed_class)
        direction = "LONG" if break_up else "SHORT"
        
        # Multi-timeframe filter for SLOW strategies
        skip_signal = False
        if tf_data and st.mode == "SLOW":
            # Get trend directions from higher timeframes
            m15_trend = get_trend_direction(tf_data.get("M15", []), lookback=8) if tf_data.get("M15") else "NEUTRAL"
            h1_trend = get_trend_direction(tf_data.get("H1", []), lookback=12) if tf_data.get("H1") else "NEUTRAL"
            h4_trend = get_trend_direction(tf_data.get("H4", []), lookback=16) if tf_data.get("H4") else "NEUTRAL"
            
            # For SLOW mode, require higher timeframe alignment
            if direction == "LONG":
                if (m15_trend == "DOWNTREND" or h1_trend == "DOWNTREND" or h4_trend == "DOWNTREND"):
                    # Skip LONG signals when higher timeframes are bearish
                    skip_signal = True
            elif direction == "SHORT":
                if (m15_trend == "UPTREND" or h1_trend == "UPTREND" or h4_trend == "UPTREND"):
                    # Skip SHORT signals when higher timeframes are bullish
                    skip_signal = True
            
            # Session timing filters for specific strategies
            if tf_data.get("sessions"):
                active_sessions = tf_data["sessions"]["active"]
                session_progress = tf_data["sessions"]["progress"]
                
                # Example: Only take trades in first half of London session
                if "LONDON" in active_sessions:
                    london_progress = session_progress.get("LONDON", {})
                    if london_progress.get("progress", 0) > 0.7:  # Past 70% of session
                        # Skip trades late in London session
                        skip_signal = True
                
                # Example: Only take trades at start of New York session
                if "NEW_YORK" in active_sessions:
                    ny_progress = session_progress.get("NEW_YORK", {})
                    if ny_progress.get("time_since_start", 0) > 3600:  # Past 1 hour into session
                        # Skip trades after first hour of NY session
                        skip_signal = True
        
        if not skip_signal:
            out.append(
            SignalDef(
                pair=pair,
                setup_id=1,
                setup_name="COMPRESSION_RELEASE",
                direction=direction,
                mode=st.mode,
                ttl_sec=sp["ttl_main"],
                pg_t=int(sp["ttl_main"] * sp["pg_t_frac"]),
                pg_atr=sp["pg_atr"],
                tp1_atr=sp["tp1_atr"],
                tp2_atr=sp["tp2_atr"],
                sl_atr=sp["sl_atr"],
                reason=_with_friction_reason(
                    reason=f"box width<{COMPRESSION_WIDTH_MAX}ATR + breakout + m>=0.50 | bar_complete={bar_complete}",
                    pair=pair,
                    atr=atrv,
                    spread_pips=st.spread_pips,
                    tp1_atr=sp["tp1_atr"],
                    sl_atr=sp["sl_atr"],
                ),
            )
            )

    if (not compression) and (m >= 0.50) and (disp_atr < LATE_IMPULSE_BLOCK_ATR):  # Relaxed from 1.00
        size_mult = 1.0
        if disp_atr >= LATE_IMPULSE_SCALE_START:
            size_mult = max(0.0, 1.0 - (disp_atr - LATE_IMPULSE_SCALE_START) / (LATE_IMPULSE_BLOCK_ATR - LATE_IMPULSE_SCALE_START))
        if directional_closes(c_exec, 3, "LONG"):
            speed_class = "MED"
            sp = get_speed_params(speed_class)
            out.append(
                SignalDef(
                    pair=pair,
                    setup_id=2,
                    setup_name="CONTINUATION_PUSH",
                    direction="LONG",
                    mode=st.mode,
                    ttl_sec=sp["ttl_main"],
                    pg_t=int(sp["ttl_main"] * sp["pg_t_frac"]),
                    pg_atr=sp["pg_atr"],
                    tp1_atr=sp["tp1_atr"],
                    tp2_atr=sp["tp2_atr"],
                    sl_atr=sp["sl_atr"],
                    reason=_with_friction_reason(
                        reason=f"3 up closes + m>=0.50 + disp={disp_atr:.2f}ATR | bar_complete={bar_complete} | size_mult={size_mult:.2f}",
                        pair=pair,
                        atr=atrv,
                        spread_pips=st.spread_pips,
                        tp1_atr=sp["tp1_atr"],
                        sl_atr=sp["sl_atr"],
                    ),
                    size_mult=size_mult,
                )
            )
        elif directional_closes(c_exec, 3, "SHORT"):
            speed_class = "MED"
            sp = get_speed_params(speed_class)
            out.append(
                SignalDef(
                    pair=pair,
                    setup_id=2,
                    setup_name="CONTINUATION_PUSH",
                    direction="SHORT",
                    mode=st.mode,
                    ttl_sec=sp["ttl_main"],
                    pg_t=int(sp["ttl_main"] * sp["pg_t_frac"]),
                    pg_atr=sp["pg_atr"],
                    tp1_atr=sp["tp1_atr"],
                    tp2_atr=sp["tp2_atr"],
                    sl_atr=sp["sl_atr"],
                    reason=_with_friction_reason(
                        reason=f"3 dn closes + m>=0.50 + disp={disp_atr:.2f}ATR | bar_complete={bar_complete} | size_mult={size_mult:.2f}",
                        pair=pair,
                        atr=atrv,
                        spread_pips=st.spread_pips,
                        tp1_atr=sp["tp1_atr"],
                        sl_atr=sp["sl_atr"],
                    ),
                    size_mult=size_mult,
                )
            )

    if len(c_exec) >= 30:
        mom_prev = abs(float(c_exec[-2]["c"]) - float(c_exec[-2 - MOM_N]["c"]))
        m_prev = (mom_prev / atrv) if atrv > 0.0 else 0.0
        if (m_prev >= MOMENTUM_THRESHOLD) and (m < 1.20):  # Lower threshold for earlier entry
            prior_dir = "LONG" if (float(c_exec[-2]["c"]) - float(c_exec[-2 - MOM_N]["c"])) > 0 else "SHORT"
            speed_class = "SLOW"
            sp = get_speed_params(speed_class)
            direction = "SHORT" if prior_dir == "LONG" else "LONG"
            out.append(
                SignalDef(
                    pair=pair,
                    setup_id=3,
                    setup_name="EXHAUSTION_SNAP",
                    direction=direction,
                    mode=st.mode,
                    ttl_sec=sp["ttl_main"],
                    pg_t=int(sp["ttl_main"] * sp["pg_t_frac"]),
                    pg_atr=sp["pg_atr"],
                    tp1_atr=sp["tp1_atr"],
                    tp2_atr=sp["tp2_atr"],
                    sl_atr=sp["sl_atr"],
                    reason=_with_friction_reason(
                        reason=f"m_prev>={MOMENTUM_THRESHOLD:.2f} then m<1.20 | bar_complete={bar_complete}",
                        pair=pair,
                        atr=atrv,
                        spread_pips=st.spread_pips,
                        tp1_atr=sp["tp1_atr"],
                        sl_atr=sp["sl_atr"],
                    ),
                )
            )

    a0 = atr(c_exec, ATR_N)
    a1 = atr(c_exec[:-1], ATR_N)
    a2 = atr(c_exec[:-2], ATR_N)
    if (a0 > a1 > a2) and (m >= 0.50) and (not compression):  # Relaxed from 1.00
        dirn = "LONG" if momentum(c_exec, MOM_N) > 0 else "SHORT"
        speed_class = "SLOW"
        sp = get_speed_params(speed_class)
        out.append(
            SignalDef(
                pair=pair,
                setup_id=6,
                setup_name="VOL_REIGNITE",
                direction=dirn,
                mode=st.mode,
                ttl_sec=sp["ttl_main"],
                pg_t=int(sp["ttl_main"] * sp["pg_t_frac"]),
                pg_atr=sp["pg_atr"],
                tp1_atr=sp["tp1_atr"],
                tp2_atr=sp["tp2_atr"],
                sl_atr=sp["sl_atr"],
                reason=_with_friction_reason(
                    reason=f"vol_reignite a0={a0:.2f} a1={a1:.2f} a2={a2:.2f} m={m:.2f} | bar_complete={bar_complete}",
                    pair=pair,
                    atr=atrv,
                    spread_pips=st.spread_pips,
                    tp1_atr=sp["tp1_atr"],
                    sl_atr=sp["sl_atr"],
                ),
            )
        )

    pr = {4: 1, 5: 2, 1: 3, 3: 4, 2: 5, 6: 6}
    out.sort(key=lambda s: pr.get(s.setup_id, 99))
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


def calc_units(pair: str, side: str, price: float, margin_avail: float, util: float, speed_class: str = "MED", spread_pips: float = 0.0, disp_atr: float = 0.0, size_mult: float = 1.0) -> Tuple[int, str, dict]:
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
        return 0, "invalid_inputs", debug

    if disp_atr >= LATE_IMPULSE_BLOCK_ATR:
        impulse_mult = 0.0
        debug["impulse_mult"] = impulse_mult
        log(f"{EMOJI_ERR} CALC_UNITS_FAIL {pair}", {"reason": "late_impulse_block", **debug})
        return 0, "late_impulse_block", debug
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
        return 0, "insufficient_margin", {**debug, "min_trade": MIN_TRADE_SIZE}
        
    raw_units = max_units_margin * speed_mult * spread_mult * impulse_mult * size_mult
    
    # Apply minimum trade size enforcement
    if raw_units < MIN_TRADE_SIZE:
        # If we have margin but calculation is too small, enforce minimum
        if max_units_margin >= MIN_TRADE_SIZE:
            log(f"{EMOJI_WARN} CALC_UNITS_ENFORCE_MIN {pair}", {"raw_units": raw_units, "enforced_units": MIN_TRADE_SIZE, "reason": "raw_below_min_but_margin_ok"})
            raw_units = MIN_TRADE_SIZE
        else:
            log(f"{EMOJI_ERR} CALC_UNITS_FAIL {pair}", {"reason": "below_min_units", "raw_units": raw_units, "min_trade": MIN_TRADE_SIZE, **debug})
            return 0, "below_min_units", {**debug, "raw_units": raw_units, "min_trade": MIN_TRADE_SIZE}
    units = int(round(raw_units))
    if units < MIN_TRADE_SIZE:
        log(f"{EMOJI_ERR} CALC_UNITS_FAIL {pair}", {"reason": "units_below_min_after_round", "units": units, "min_trade": MIN_TRADE_SIZE, **debug})
        return 0, "units_below_min_after_round", {**debug, "units": units, "min_trade": MIN_TRADE_SIZE}
    units_cap = int(max_units_margin)
    if units > units_cap:
        units = units_cap
    # No MAX_UNITS_PER_TRADE limit - size controlled by margin utilization
    
    debug.update({
        "raw_units": raw_units,
    })
    return units if side == "LONG" else -units, "success", debug


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


def _transition_state(st: PairState, new_state: str, pair: str = "") -> None:
    pair = normalize_pair(pair)
    if new_state == st.state:
        return
    old_state = st.state
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


def _apply_state_machine(st: PairState, pair: str, c_exec: Optional[List[dict]] = None) -> None:
    pair = normalize_pair(pair)
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

# Partial candle refresh behavior (allow intrabar updates without global rate spikes)
ALLOW_PARTIAL_CANDLES = os.getenv("ALLOW_PARTIAL_CANDLES", "1").strip().lower() in ("1", "true", "yes")
PARTIAL_CANDLE_REFRESH_SEC = float(os.getenv("PARTIAL_CANDLE_REFRESH_SEC", "1.0") or "1.0")
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
SPREAD_MAX_PIPS = 6.0
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

# ===== AEE PHASES =====
class AEEPhase:
    PROTECT = "PROTECT"
    BUILD = "BUILD"
    HARVEST = "HARVEST"
    RUNNER = "RUNNER"
    PANIC = "PANIC"

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

def check_aee_exits(metrics: dict, aee_state: AEEState, current_price: float) -> Optional[str]:
    """Check AEE exit conditions in priority order."""
    
    progress = metrics["progress"]
    speed = metrics["speed"]
    velocity = metrics["velocity"]
    pullback = metrics["pullback"]
    dist_to_tp = metrics["dist_to_tp"]
    near_tp_band = metrics["near_tp_band"]
    
    # Priority 1: PANIC EXIT
    if (velocity <= PANIC_VELOCITY or 
        pullback >= PANIC_PULLBACK):
        return "PANIC_EXIT"
    
    # Priority 2: Near-TP stall capture
    if (dist_to_tp <= near_tp_band and 
        progress >= 0.50 and
        aee_state.armed_by == "D"):
        
        if (velocity < 0 or  # 2 consecutive evals simplified
            pullback >= STALL_PULLBACK_ATR):
            return "NEAR_TP_STALL_CAPTURE"
    
    # Priority 3: Pulse harvest
    if (speed >= PULSE_SPEED and 
        progress >= PULSE_PROGRESS):
        
        # Set exit line if not set
        if aee_state.pulse_exit_line is None:
            if aee_state.direction == "LONG":
                aee_state.pulse_exit_line = aee_state.local_high - PULSE_EXITLINE_ATR * 0.0010  # Simplified ATR
            else:
                aee_state.pulse_exit_line = aee_state.local_low + PULSE_EXITLINE_ATR * 0.0010
        
        # Check cross
        if aee_state.direction == "LONG" and current_price <= aee_state.pulse_exit_line:
            return "PULSE_STALL_CAPTURE"
        elif aee_state.direction == "SHORT" and current_price >= aee_state.pulse_exit_line:
            return "PULSE_STALL_CAPTURE"
    
    # Priority 4: Decay exit
    if (progress >= DECAY_PROGRESS and 
        speed < DECAY_SPEED and 
        velocity < 0 and 
        pullback >= DECAY_PULLBACK):
        return "FAILED_TO_CONTINUE_DECAY"
    
    return None

def execute_aee_exit(pair: str, aee_state: AEEState, exit_reason: str) -> dict:
    """Execute AEE exit by position side."""
    
    direction = "longUnits" if aee_state.direction == "LONG" else "shortUnits"
    body = {direction: "ALL"}
    
    resp = oanda_call(f"aee_exit_{exit_reason}", o.close_position, pair, **body)
    
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
    if spread_pips > 12.0:  # ABS_SPREAD_MAX_PIPS
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

# ===== DATA QUALITY STATES =====
class DataQuality:
    OK = "OK"
    DEGRADED = "DEGRADED"
    BAD = "BAD"

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
        
        # Efficiency and overlap
        efficiency = abs(displacement) / max(path_len, 1e-10)
        overlap = path_len / max(abs(displacement), 1e-10)
        
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
        
        # Pullback
        if direction == "LONG":
            pullback = (extrema["high"] - bids[-1]) / atr if atr > 0 else 0
        else:
            pullback = (asks[-1] - extrema["low"]) / atr if atr > 0 else 0
        
        # Quote-side pricing
        entry_price_exec = asks[-1] if direction == "LONG" else bids[-1]
        current_price_exec = entry_price_exec
        
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

# ===== RESILIENCE CONTROLLER =====
class ResilienceController:
    """Manage data quality and fallback behavior."""
    
    def __init__(self):
        self.feed_health: Dict[str, str] = {}
        self.last_update: Dict[str, float] = {}
        self.median_spread_5m: Dict[str, float] = {}
        self._lock = threading.Lock()
    
    def update_feed_health(self, instrument: str, event: PriceEvent):
        """Update feed health based on price events."""
        with self._lock:
            now = event.t_local
            self.last_update[instrument] = now
            
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

def main(*, run_for_sec: Optional[float] = None) -> None:
    global _SHUTDOWN
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # Load credentials
    # HARDCODED API KEYS FOR TERMUX DEPLOYMENT
    OANDA_API_KEY = "2bf7b4b9bb052e28023de779a6363f1e-fee71a4fce4e94b18e0dd9c2443afa52"
    OANDA_ACCOUNT_ID = "101-001-22881868-001"
    OANDA_ENV = str(os.getenv("OANDA_ENV", "practice") or "practice").strip()
    
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

                res = fn(*args, **kwargs)
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
        m5_candles = _get_candles(pair, 50, now, CANDLE_REFRESH_SEC)
        if m5_candles:
            data["M5"] = m5_candles
            data["available"].append("M5")
    
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
    if HOURLY_SCAN_MODE:
        for k in scan_cfg:
            scan_cfg[k] = max(scan_cfg[k], HOURLY_SCAN_INTERVAL_SEC)

    def _scan_candle_refresh_sec(scan_type: str) -> float:
        base = CANDLE_REFRESH_SKIP_SEC
        if scan_type == "FOCUS":
            base = CANDLE_REFRESH_FOCUS_SEC
        elif scan_type == "WATCH":
            base = CANDLE_REFRESH_WATCH_SEC
        # Always respect global minimums and timeframe
        base = max(base, CANDLE_REFRESH_SEC)
        if tf_sec > 0:
            base = max(base, float(tf_sec) * 3.0)
        if HOURLY_SCAN_MODE:
            base = max(base, HOURLY_SCAN_INTERVAL_SEC)
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
            if o.rate_limited_until > now_ts():
                time.sleep(min(1.0, o.rate_limited_until - now_ts()))
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
                    and dd_from_peak >= RUNNER_GIVEBACK_ATR
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
                            {"reason": "RUNNER_GIVEBACK", "resp": resp},
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
                continue
            for bp in list(EXIT_BLOCKED_PAIRS.keys()):
                if count_pair_positions(open_pos, bp) == 0:
                    EXIT_BLOCKED_PAIRS.pop(bp, None)
            pending_orders = _get_pending_orders(scan_now, "pending_orders_scan")
            if pending_orders is None:
                continue
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
                continue

            # Batch fetch comprehensive multi-timeframe data for all scan pairs
            tf_data_cache = {}
            for pair in scan_pairs:
                if _SHUTDOWN:
                    break
                # Get all timeframes: ticks, M5, M15, H1, H4
                tf_data = get_all_timeframes(pair, scan_now)
                if tf_data["M5"]:  # Only cache if we have execution data
                    tf_data_cache[pair] = tf_data
            
            # Process each pair with cached candles
            for pair in scan_pairs:
                if _SHUTDOWN:
                    break
                st = states[pair]

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
                if not c_exec:
                    c_exec, exec_gran = _get_exec_candles_with_fallback(pair, min_candles + 5, scan_now, candle_refresh_sec)
                if not c_exec:
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
                mom = momentum(c_exec, MOM_N)
                m_norm = abs(mom) / atr_s if (atr_s > 0 and math.isfinite(mom) and math.isfinite(atr_s)) else float("nan")
                wr_val = williams_r(c_exec, WR_N)

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
                mult = spread_size_mult_fe(s_atr, speed_norm)
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
            
                resp1 = oanda_call("place_market_main", o.place_market, pair, units_main, sl1, tp1, client_id=cid1, allow_error_dict=True)
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
                
                    resp2 = oanda_call("place_market_run", o.place_market, pair, units_run, sl2, tp2, client_id=cid2, allow_error_dict=True)
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


if __name__ == "__main__":
    main()
