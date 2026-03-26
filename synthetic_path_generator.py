#!/usr/bin/env python3
"""
Synthetic path generator for Phase 1 AEE testing.

Generates realistic post-entry trade paths using regime-switching
drift + noise + mean-reversion model with explicit spread and event shocks.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple

import numpy as np


class PathClass(Enum):
    """Core path classes for Phase 1 testing."""
    CLEAN_CONTINUATION = "clean_continuation"
    STALL_THEN_CONTINUE = "stall_then_continue"
    STALL_THEN_FAIL = "stall_then_fail"
    IMMEDIATE_REVERSAL = "immediate_reversal"
    SLOW_BLEED = "slow_bleed"
    SMALL_PROFIT_THEN_FADE = "small_profit_then_fade"
    TP_TOUCH_THEN_EXTENSION = "tp_touch_then_extension"
    WHIPSAW = "whipsaw"


@dataclass
class PathParameters:
    """Parameters for path generation."""
    # Core drift and volatility
    base_drift: float  # ATR per minute
    base_vol: float  # ATR per minute
    decay_speed: float  # How fast drift decays (0-1)
    
    # Mean reversion (for chop/fade)
    reversion_strength: float  # Pull toward local mean
    reversion_speed: float  # How fast mean reversion acts
    
    # Shock parameters
    shock_probability: float  # Chance of shock per minute
    shock_magnitude: float  # Size of shock in ATR
    
    # Spread behavior
    base_spread: float  # Base spread in pips
    spread_volatility: float  # How much spread varies
    spread_shock_ratio: float  # Spread widening during shocks


@dataclass
class SyntheticPath:
    """Generated synthetic trade path."""
    path_class: PathClass
    direction: str  # "LONG" or "SHORT"
    
    # Entry parameters
    entry_price: float
    entry_spread: float
    atr_pips: float
    
    # TP/SL references
    tp_price: float
    sl_price: float
    
    # Path data
    timestamps: List[float] = field(default_factory=list)
    mid_prices: List[float] = field(default_factory=list)
    spreads: List[float] = field(default_factory=list)
    
    # Performance metrics
    mfe_r: float = 0.0  # Maximum favorable excursion in R
    mae_r: float = 0.0  # Maximum adverse excursion in R
    final_r: float = 0.0  # Final R at exit
    
    # Exit info
    exit_reason: str = ""
    exit_time: Optional[float] = None


# Path class parameter presets
PATH_CLASS_PARAMS: Dict[PathClass, PathParameters] = {
    PathClass.CLEAN_CONTINUATION: PathParameters(
        base_drift=0.15,  # Strong directional drift
        base_vol=0.08,
        decay_speed=0.02,  # Very slow decay
        reversion_strength=0.05,
        reversion_speed=0.1,
        shock_probability=0.02,
        shock_magnitude=0.3,
        base_spread=1.2,
        spread_volatility=0.2,
        spread_shock_ratio=1.5,
    ),
    
    PathClass.STALL_THEN_CONTINUE: PathParameters(
        base_drift=0.08,
        base_vol=0.06,
        decay_speed=0.15,  # Decay to near zero
        reversion_strength=0.15,
        reversion_speed=0.2,
        shock_probability=0.03,
        shock_magnitude=0.2,
        base_spread=1.3,
        spread_volatility=0.25,
        spread_shock_ratio=1.6,
    ),
    
    PathClass.STALL_THEN_FAIL: PathParameters(
        base_drift=0.06,
        base_vol=0.07,
        decay_speed=0.25,  # Fast decay then reversal
        reversion_strength=0.3,
        reversion_speed=0.25,
        shock_probability=0.05,
        shock_magnitude=0.25,
        base_spread=1.4,
        spread_volatility=0.3,
        spread_shock_ratio=1.8,
    ),
    
    PathClass.IMMEDIATE_REVERSAL: PathParameters(
        base_drift=-0.12,  # Negative drift from start
        base_vol=0.12,
        decay_speed=0.05,
        reversion_strength=0.1,
        reversion_speed=0.15,
        shock_probability=0.08,  # High shock probability
        shock_magnitude=0.4,
        base_spread=1.5,
        spread_volatility=0.4,
        spread_shock_ratio=2.2,
    ),
    
    PathClass.SLOW_BLEED: PathParameters(
        base_drift=-0.03,  # Slow negative drift
        base_vol=0.05,
        decay_speed=0.01,
        reversion_strength=0.2,
        reversion_speed=0.1,
        shock_probability=0.02,
        shock_magnitude=0.15,
        base_spread=1.3,
        spread_volatility=0.2,
        spread_shock_ratio=1.4,
    ),
    
    PathClass.SMALL_PROFIT_THEN_FADE: PathParameters(
        base_drift=0.04,
        base_vol=0.06,
        decay_speed=0.3,  # Very fast decay
        reversion_strength=0.35,
        reversion_speed=0.3,
        shock_probability=0.06,
        shock_magnitude=0.2,
        base_spread=1.4,
        spread_volatility=0.3,
        spread_shock_ratio=1.7,
    ),
    
    PathClass.TP_TOUCH_THEN_EXTENSION: PathParameters(
        base_drift=0.18,
        base_vol=0.1,
        decay_speed=0.01,  # Almost no decay
        reversion_strength=0.02,
        reversion_speed=0.05,
        shock_probability=0.03,
        shock_magnitude=0.5,  # Large possible extension
        base_spread=1.2,
        spread_volatility=0.25,
        spread_shock_ratio=1.6,
    ),
    
    PathClass.WHIPSAW: PathParameters(
        base_drift=0.0,  # No directional drift
        base_vol=0.09,
        decay_speed=0.1,
        reversion_strength=0.4,  # Strong mean reversion
        reversion_speed=0.3,
        shock_probability=0.1,  # Frequent small shocks
        shock_magnitude=0.15,
        base_spread=1.6,
        spread_volatility=0.5,
        spread_shock_ratio=1.9,
    ),
}


def generate_synthetic_path(
    path_class: PathClass,
    direction: str,
    entry_price: float,
    atr_pips: float,
    spread_pips: float,
    tp_distance_atr: float = 2.5,
    sl_distance_atr: float = 2.5,
    max_minutes: int = 100,
    random_seed: Optional[int] = None,
) -> SyntheticPath:
    """Generate a single synthetic trade path."""
    
    if random_seed is not None:
        random.seed(random_seed)
        np.random.seed(random_seed)
    
    params = PATH_CLASS_PARAMS[path_class]
    
    # Calculate TP/SL prices
    tp_distance = tp_distance_atr * atr_pips / 10000  # Convert to price
    sl_distance = sl_distance_atr * atr_pips / 10000
    
    if direction == "LONG":
        tp_price = entry_price + tp_distance
        sl_price = entry_price - sl_distance
    else:  # SHORT
        tp_price = entry_price - tp_distance
        sl_price = entry_price + tp_distance
    
    # Initialize path
    path = SyntheticPath(
        path_class=path_class,
        direction=direction,
        entry_price=entry_price,
        entry_spread=spread_pips,
        atr_pips=atr_pips,
        tp_price=tp_price,
        sl_price=sl_price,
    )
    
    # State variables
    current_price = entry_price
    current_drift = params.base_drift * atr_pips / 10000
    current_vol = params.base_vol * atr_pips / 10000
    local_mean = entry_price
    
    # Volatility clustering
    vol_persistence = 0.7
    
    # Generate path
    for minute in range(max_minutes + 1):
        timestamp = float(minute * 60)  # Minutes to seconds
        
        # Update volatility with clustering
        vol_noise = np.random.normal(0, 0.1)
        current_vol = vol_persistence * current_vol + (1 - vol_persistence) * (params.base_vol * atr_pips / 10000) + vol_noise * current_vol * 0.1
        current_vol = max(0.001 * atr_pips / 10000, current_vol)  # Ensure positive
        
        # Update drift with decay
        current_drift *= (1 - params.decay_speed)
        
        # Mean reversion force
        reversion_force = params.reversion_strength * (local_mean - current_price) * params.reversion_speed
        
        # Random shock
        shock = 0
        if random.random() < params.shock_probability:
            shock = np.random.normal(0, params.shock_magnitude * atr_pips / 10000)
            local_mean = current_price + shock  # Update local mean after shock
        
        # Generate price change
        noise = np.random.normal(0, current_vol)
        direction_mult = 1 if direction == "LONG" else -1
        
        price_change = direction_mult * (current_drift + reversion_force + noise + shock)
        current_price += price_change
        
        # Update local mean slowly
        local_mean += 0.05 * (current_price - local_mean)
        
        # Generate spread
        spread_base = params.base_spread
        spread_noise = np.random.normal(0, params.spread_volatility)
        spread_multiplier = params.spread_shock_ratio if abs(shock) > 0 else 1.0
        current_spread = max(0.5, spread_base + spread_noise) * spread_multiplier
        
        # Store data
        path.timestamps.append(timestamp)
        path.mid_prices.append(current_price)
        path.spreads.append(current_spread)
        
        # Calculate running MFE/MAE
        if direction == "LONG":
            profit_r = (current_price - entry_price) / (sl_distance)
        else:
            profit_r = (entry_price - current_price) / (sl_distance)
        
        path.mfe_r = max(path.mfe_r, profit_r)
        path.mae_r = min(path.mae_r, profit_r)
        
        # Check for TP/SL hit
        if direction == "LONG":
            if current_price <= sl_price:
                path.exit_reason = "HIT_SL"
                path.exit_time = timestamp
                path.final_r = -1.0
                break
            elif current_price >= tp_price:
                path.exit_reason = "HIT_TP"
                path.exit_time = timestamp
                path.final_r = profit_r
                break
        else:  # SHORT
            if current_price >= sl_price:
                path.exit_reason = "HIT_SL"
                path.exit_time = timestamp
                path.final_r = -1.0
                break
            elif current_price <= tp_price:
                path.exit_reason = "HIT_TP"
                path.exit_time = timestamp
                path.final_r = profit_r
                break
    
    # If no exit occurred, it's a timeout
    if path.exit_time is None:
        path.exit_reason = "TIMEOUT"
        path.exit_time = float(max_minutes * 60)
        if direction == "LONG":
            path.final_r = (path.mid_prices[-1] - entry_price) / sl_distance
        else:
            path.final_r = (entry_price - path.mid_prices[-1]) / sl_distance
    
    return path


def generate_path_library(
    paths_per_class: int,
    atr_pips: float = 15.0,
    spread_pips: float = 1.5,
    random_seed: Optional[int] = None,
) -> List[SyntheticPath]:
    """Generate a library of synthetic paths across all classes."""
    
    library = []
    
    if random_seed is not None:
        random.seed(random_seed)
    
    # Generate paths for each class
    for path_class in PathClass:
        for i in range(paths_per_class):
            # Alternate direction for balance
            direction = "LONG" if i % 2 == 0 else "SHORT"
            
            # Random entry price around 1.0000
            entry_price = 1.0000 + random.uniform(-0.0500, 0.0500)
            
            # Generate path with unique seed
            path = generate_synthetic_path(
                path_class=path_class,
                direction=direction,
                entry_price=entry_price,
                atr_pips=atr_pips,
                spread_pips=spread_pips,
                random_seed=None if random_seed is None else (hash(f"{path_class}_{i}") % (2**32 - 1)),
            )
            
            library.append(path)
    
    return library


def get_path_class_weights() -> Dict[PathClass, float]:
    """Get realistic weighting for path classes."""
    return {
        PathClass.CLEAN_CONTINUATION: 0.12,
        PathClass.STALL_THEN_CONTINUE: 0.15,
        PathClass.STALL_THEN_FAIL: 0.20,
        PathClass.IMMEDIATE_REVERSAL: 0.08,
        PathClass.SLOW_BLEED: 0.10,
        PathClass.SMALL_PROFIT_THEN_FADE: 0.15,
        PathClass.TP_TOUCH_THEN_EXTENSION: 0.06,
        PathClass.WHIPSAW: 0.04,
    }


def generate_weighted_paths(
    total_paths: int,
    atr_pips: float = 15.0,
    spread_pips: float = 1.5,
    random_seed: Optional[int] = None,
) -> List[SyntheticPath]:
    """Generate weighted path library matching realistic distribution."""
    
    weights = get_path_class_weights()
    library = []
    
    if random_seed is not None:
        random.seed(random_seed)
    
    for path_class, weight in weights.items():
        count = int(total_paths * weight)
        
        for i in range(count):
            direction = "LONG" if i % 2 == 0 else "SHORT"
            entry_price = 1.0000 + random.uniform(-0.0500, 0.0500)
            
            path = generate_synthetic_path(
                path_class=path_class,
                direction=direction,
                entry_price=entry_price,
                atr_pips=atr_pips,
                spread_pips=spread_pips,
                random_seed=None if random_seed is None else (hash(f"{path_class}_{i}") % (2**32 - 1)),
            )
            
            library.append(path)
    
    # Fill any remaining paths with most common class
    while len(library) < total_paths:
        path_class = PathClass.STALL_THEN_FAIL
        i = len(library)
        direction = "LONG" if i % 2 == 0 else "SHORT"
        entry_price = 1.0000 + random.uniform(-0.0500, 0.0500)
        
        path = generate_synthetic_path(
            path_class=path_class,
            direction=direction,
            entry_price=entry_price,
            atr_pips=atr_pips,
            spread_pips=spread_pips,
            random_seed=None if random_seed is None else (hash(f"fill_{i}") % (2**32 - 1)),
        )
        
        library.append(path)
    
    return library
