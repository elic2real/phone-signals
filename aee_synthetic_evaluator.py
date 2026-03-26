#!/usr/bin/env python3
"""
AEE evaluation engine for synthetic path testing.

Implements the AEE decision loop for post-entry trade management
on synthetic price paths.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

from synthetic_path_generator import SyntheticPath, PathClass


class ExitReason(Enum):
    """Standardized exit reasons for AEE evaluation."""
    PANIC = "PANIC"
    DECAY = "DECAY"
    GIVEBACK = "GIVEBACK"
    PROFIT_CAPTURE = "PROFIT_CAPTURE"
    POST_TP = "POST_TP"
    SL_HIT = "SL_HIT"
    TIMEOUT = "TIMEOUT"


@dataclass
class AEEState:
    """AEE state tracking for a single trade."""
    entry_price: float
    direction: str
    tp_anchor: float
    sl_price: float
    atr_pips: float
    
    # Tracking variables
    local_high: float = 0.0
    local_low: float = 0.0
    peak_progress: float = 0.0
    locked_peak: float = 0.0
    allowed_giveback_atr: float = 0.35
    
    # Velocity and momentum
    price_history: List[Tuple[float, float]] = field(default_factory=list)  # (timestamp, price)
    velocity: float = 0.0
    pullback: float = 0.0
    pullback_rate: float = 0.0
    
    # Timing
    entry_time: float = 0.0
    last_eval_time: float = 0.0
    
    # Exit tracking
    exit_reason: Optional[str] = None
    exit_time: Optional[float] = None


@dataclass
class AEEKnobs:
    """AEE configuration knobs for Phase 1 testing."""
    # Core profit capture - increased based on testing
    profit_capture_min_atr: float = 0.55
    
    # Giveback protection - tightened based on testing but with extension tolerance
    allowed_giveback_atr_mult: float = 0.25
    
    # Stall detection
    stall_min_profit_atr: float = 0.25
    stall_no_extension_time: float = 15.0  # seconds
    
    # Decay logic
    decay_exit_min_profit: float = 0.30
    decay_min_hold_sec: float = 120.0
    decay_progress: float = 0.60
    
    # Panic logic
    panic_velocity: float = -0.80
    panic_pullback: float = 0.60
    panic_pullback_rate: float = 0.06
    
    # Time limits
    max_hold_sec: float = 6000.0  # 100 minutes


class AEEEvaluator:
    """AEE evaluation engine for synthetic paths."""
    
    def __init__(self, knobs: AEEKnobs):
        self.knobs = knobs
        self.reset()
    
    def reset(self) -> None:
        """Reset evaluator state."""
        self.trades: Dict[str, AEEState] = {}
    
    def evaluate_path(self, path: SyntheticPath, trade_id: str = "test") -> Dict:
        """Evaluate AEE on a complete synthetic path."""
        
        # Initialize AEE state
        state = AEEState(
            entry_price=path.entry_price,
            direction=path.direction,
            tp_anchor=path.tp_price,
            sl_price=path.sl_price,
            atr_pips=path.atr_pips,
            local_high=path.entry_price,
            local_low=path.entry_price,
            entry_time=0.0,
        )
        
        # Simulate AEE evaluation at each tick
        for i, (timestamp, mid_price, spread) in enumerate(zip(path.timestamps, path.mid_prices, path.spreads)):
            
            # Update state
            self._update_state(state, timestamp, mid_price)
            
            # Check for hard boundaries first
            exit_reason = self._check_hard_boundaries(state, mid_price, timestamp)
            if exit_reason:
                state.exit_reason = exit_reason
                state.exit_time = timestamp
                break
            
            # AEE evaluation order matters!
            exit_reason = self._evaluate_panic(state, mid_price, timestamp)
            if exit_reason:
                state.exit_reason = exit_reason
                state.exit_time = timestamp
                break
            
            exit_reason = self._evaluate_decay(state, mid_price, timestamp)
            if exit_reason:
                state.exit_reason = exit_reason
                state.exit_time = timestamp
                break
            
            exit_reason = self._evaluate_giveback(state, mid_price, timestamp)
            if exit_reason:
                state.exit_reason = exit_reason
                state.exit_time = timestamp
                break
            
            exit_reason = self._evaluate_profit_capture(state, mid_price, timestamp)
            if exit_reason:
                state.exit_reason = exit_reason
                state.exit_time = timestamp
                break
            
            exit_reason = self._evaluate_post_tp(state, mid_price, timestamp)
            if exit_reason:
                state.exit_reason = exit_reason
                state.exit_time = timestamp
                break
            
            # Check timeout
            if timestamp >= self.knobs.max_hold_sec:
                state.exit_reason = "TIMEOUT"
                state.exit_time = timestamp
                break
        
        # Calculate final metrics
        result = self._calculate_result(state, path)
        return result
    
    def _update_state(self, state: AEEState, timestamp: float, mid_price: float) -> None:
        """Update AEE state with new price data."""
        
        # Update price history
        state.price_history.append((timestamp, mid_price))
        if len(state.price_history) > 100:  # Keep last 100 points
            state.price_history = state.price_history[-100:]
        
        # Update highs/lows
        state.local_high = max(state.local_high, mid_price)
        state.local_low = min(state.local_low, mid_price)
        
        # Calculate progress in ATR
        if state.direction == "LONG":
            progress = (mid_price - state.entry_price) / (state.entry_price - state.sl_price)
            pullback = max(0.0, (state.local_high - mid_price) / (state.entry_price - state.sl_price))
        else:
            progress = (state.entry_price - mid_price) / (state.sl_price - state.entry_price)
            pullback = max(0.0, (mid_price - state.local_low) / (state.sl_price - state.entry_price))
        
        # Update peak progress
        state.peak_progress = max(state.peak_progress, progress)
        
        # Calculate velocity
        if len(state.price_history) >= 2:
            t1, p1 = state.price_history[-2]
            t2, p2 = state.price_history[-1]
            dt = max(0.1, t2 - t1)
            state.velocity = ((p2 - p1) / state.atr_pips * 10000) / dt  # ATR per second
        
        # Update pullback metrics
        prev_pullback = state.pullback
        state.pullback = pullback
        state.pullback_rate = max(0.0, pullback - prev_pullback)
        
        state.last_eval_time = timestamp
    
    def _check_hard_boundaries(self, state: AEEState, mid_price: float, timestamp: float) -> Optional[str]:
        """Check for hard SL/TP boundaries."""
        
        if state.direction == "LONG":
            if mid_price <= state.sl_price:
                return "SL_HIT"
            if mid_price >= state.tp_anchor:
                return "POST_TP"  # Let post-TP logic handle it
        else:  # SHORT
            if mid_price >= state.sl_price:
                return "SL_HIT"
            if mid_price <= state.tp_anchor:
                return "POST_TP"
        
        return None
    
    def _evaluate_panic(self, state: AEEState, mid_price: float, timestamp: float) -> Optional[str]:
        """Evaluate panic exit conditions."""
        
        # Check velocity threshold
        if state.velocity <= self.knobs.panic_velocity:
            return "PANIC"
        
        # Check pullback threshold
        if state.pullback >= self.knobs.panic_pullback:
            return "PANIC"
        
        # Check pullback rate
        if state.pullback_rate >= self.knobs.panic_pullback_rate:
            return "PANIC"
        
        return None
    
    def _evaluate_decay(self, state: AEEState, mid_price: float, timestamp: float) -> Optional[str]:
        """Evaluate decay exit conditions."""
        
        # Must hold minimum time
        if timestamp < self.knobs.decay_min_hold_sec:
            return None
        
        # Must have some profit
        if state.peak_progress < self.knobs.decay_exit_min_profit:
            return None
        
        # Check if progress has decayed significantly
        if state.peak_progress > 0:
            decay_ratio = state.peak_progress * (1 - self.knobs.decay_progress)
            current_progress = abs(mid_price - state.entry_price) / abs(state.entry_price - state.sl_price)
            
            if current_progress < decay_ratio:
                return "DECAY"
        
        return None
    
    def _evaluate_giveback(self, state: AEEState, mid_price: float, timestamp: float) -> Optional[str]:
        """Evaluate giveback protection."""
        
        # Need to have reached minimum profit
        if state.peak_progress < self.knobs.profit_capture_min_atr:
            return None
        
        # For extension scenarios (trades that have reached or exceeded TP), be much more tolerant
        is_near_tp = abs(mid_price - state.tp_anchor) / abs(state.entry_price - state.sl_price) < 0.25  # Wider near-TP zone
        is_extension = state.peak_progress >= 1.0
        
        if is_near_tp or is_extension:
            # Much more tolerant near TP or in extension phase for historical data
            max_giveback = self.knobs.allowed_giveback_atr_mult * 4.0  # 4x more tolerant
        else:
            max_giveback = self.knobs.allowed_giveback_atr_mult
        
        # Check if giveback exceeds allowed amount
        if state.direction == "LONG":
            current_profit = (mid_price - state.entry_price) / (state.entry_price - state.sl_price)
        else:
            current_profit = (state.entry_price - mid_price) / (state.sl_price - state.entry_price)
        
        giveback_amount = state.peak_progress - current_profit
        
        if giveback_amount > max_giveback:
            return "GIVEBACK"
        
        return None
    
    def _evaluate_profit_capture(self, state: AEEState, mid_price: float, timestamp: float) -> Optional[str]:
        """Evaluate profit capture conditions."""
        
        # Check if minimum profit threshold reached
        if state.direction == "LONG":
            profit_atr = (mid_price - state.entry_price) / (state.atr_pips / 10000)
        else:
            profit_atr = (state.entry_price - mid_price) / (state.atr_pips / 10000)
        
        if profit_atr >= self.knobs.profit_capture_min_atr:
            # For extension scenarios, be much more lenient on momentum requirements
            is_near_tp = abs(mid_price - state.tp_anchor) / abs(state.entry_price - state.sl_price) < 0.3
            is_extension = state.peak_progress >= 1.0
            
            if is_near_tp or is_extension:
                # Allow profit capture in extension phase even with negative momentum
                # but require sustained negative momentum (not just a blip)
                if state.velocity < -0.3 and state.pullback_rate > 0.15:  # More aggressive reversal signal
                    return "PROFIT_CAPTURE"
            else:
                # Normal profit capture logic for regular trades
                # Require more sustained negative momentum for historical data
                if state.velocity < -0.15 and state.pullback_rate > 0.08:
                    return "PROFIT_CAPTURE"
        
        return None
    
    def _evaluate_post_tp(self, state: AEEState, mid_price: float, timestamp: float) -> Optional[str]:
        """Evaluate post-TP continuation logic."""
        
        # Only active after TP has been hit
        if state.peak_progress < 1.0:
            return None
        
        # Allow continuation if momentum is still positive
        if state.velocity > 0.05:
            return None  # Keep running
        
        # Exit if momentum turns negative with acceleration
        if state.velocity < -0.15 and state.pullback_rate > 0.08:
            return "POST_TP"
        
        # Exit if significant giveback from peak
        current_profit = (mid_price - state.entry_price) / abs(state.entry_price - state.sl_price) if state.direction == "LONG" else (state.entry_price - mid_price) / abs(state.sl_price - state.entry_price)
        giveback_from_peak = state.peak_progress - current_profit
        
        if giveback_from_peak > 0.5:  # Allow up to 0.5R giveback in extension phase
            return "POST_TP"
        
        return None
    
    def _calculate_result(self, state: AEEState, path: SyntheticPath) -> Dict:
        """Calculate final result metrics."""
        
        # Determine exit price
        if state.exit_time is not None:
            # Find price at exit time
            exit_price = path.entry_price  # Default
            for i, timestamp in enumerate(path.timestamps):
                if timestamp >= state.exit_time:
                    exit_price = path.mid_prices[i]
                    break
        else:
            # Use final price
            exit_price = path.mid_prices[-1]
            state.exit_reason = "TIMEOUT"
            state.exit_time = path.timestamps[-1]
        
        # Calculate R multiple
        if state.direction == "LONG":
            actual_r = (exit_price - state.entry_price) / (state.entry_price - state.sl_price)
        else:
            actual_r = (state.entry_price - exit_price) / (state.sl_price - state.entry_price)
        
        # Determine outcome flags
        closed_before_sl = state.exit_reason in ["PANIC", "DECAY", "GIVEBACK", "PROFIT_CAPTURE"]
        closed_before_tp = closed_before_sl and actual_r > 0
        closed_after_tp = state.exit_reason == "POST_TP" and actual_r > 1.0
        
        return {
            "exit_reason": state.exit_reason,
            "exit_time": state.exit_time,
            "actual_r": actual_r,
            "mfe_r": state.peak_progress,
            "mae_r": -1.0 if actual_r < -0.99 else min(0, actual_r - state.peak_progress),
            "closed_before_sl": closed_before_sl,
            "closed_before_tp": closed_before_tp,
            "closed_after_tp": closed_after_tp,
            "bars_held": int(state.exit_time / 60),  # Convert to minutes
            "hold_minutes": state.exit_time / 60,
            "giveback_r": state.peak_progress - actual_r if actual_r > 0 else 0,
        }


def run_static_baseline(path: SyntheticPath) -> Dict:
    """Run static TP/SL management on a path (no AEE)."""
    
    # Static management only exits at TP, SL, or timeout
    exit_reason = path.exit_reason
    exit_time = path.exit_time
    actual_r = path.final_r
    
    # Calculate metrics
    mfe_r = path.mfe_r
    mae_r = path.mae_r
    
    closed_before_sl = False  # Static never closes before SL
    closed_before_tp = False
    closed_after_tp = exit_reason == "HIT_TP" and actual_r > 1.0
    
    return {
        "exit_reason": exit_reason,
        "exit_time": exit_time,
        "actual_r": actual_r,
        "mfe_r": mfe_r,
        "mae_r": mae_r,
        "closed_before_sl": closed_before_sl,
        "closed_before_tp": closed_before_tp,
        "closed_after_tp": closed_after_tp,
        "bars_held": int(exit_time / 60),
        "hold_minutes": exit_time / 60,
        "giveback_r": max(0, mfe_r - actual_r) if actual_r > 0 else 0,
    }
