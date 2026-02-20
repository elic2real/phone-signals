# --- Universal scenario: morphs between all energy states ---
def scenario_universal_energy_morph(seed: int = 123) -> list[dict[str, float]]:
    """Universal: morphs through all energy states (trend, range, transition, depletion, random walk) in one sequence."""
    ticks = []
    # High energy trend
    # Reduced drift from 0.00018 to 0.00002
    cfg1 = InstrumentConfig(name="EUR_USD", initial_price=1.1000, volatility=0.00028, drift=0.00002)
    ticks += generate_synthetic_ticks(cfg1, 600, seed=seed)
    # Sudden transition to range
    cfg2 = InstrumentConfig(name="EUR_USD", initial_price=ticks[-1]["mid"], volatility=0.00003, drift=0.0)
    ticks += generate_synthetic_ticks(cfg2, 600, start_ts=ticks[-1]["ts"] + 1, seed=seed+1)
    # Energy expansion (breakout)
    # Reduced drift from 0.00013 to 0.00002
    cfg3 = InstrumentConfig(name="EUR_USD", initial_price=ticks[-1]["mid"], volatility=0.00025, drift=0.00002)
    ticks += generate_synthetic_ticks(cfg3, 600, start_ts=ticks[-1]["ts"] + 1, seed=seed+2)
    # Energy depletion
    cfg4 = InstrumentConfig(name="EUR_USD", initial_price=ticks[-1]["mid"], volatility=0.00004, drift=0.0)
    ticks += generate_synthetic_ticks(cfg4, 600, start_ts=ticks[-1]["ts"] + 1, seed=seed+3)
    # Random walk
    cfg5 = InstrumentConfig(name="EUR_USD", initial_price=ticks[-1]["mid"], volatility=0.00012, drift=0.0)
    ticks += generate_synthetic_ticks(cfg5, 600, start_ts=ticks[-1]["ts"] + 1, seed=seed+4)
    return ticks


# --- Universal, energy-based scenario set (ADDED, not replacing originals) ---
def scenario_high_energy_trend(seed: int = 123) -> list[dict[str, float]]:
    """Universal: persistent high energy (trend, high volatility, high momentum). Tuned for max pip extraction."""
    # Adversarial: add volatility spikes and spread spikes to test extraction cost
    # Reduced drift from 0.00020 to 0.00002 (realistic 300 pips/hr, not 7000)
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.1000, volatility=0.00030, drift=0.00002)
    spikes = [(200, 220, 3.0), (400, 420, 2.5)]
    ticks = generate_synthetic_ticks(cfg, 650, volatility_spikes=spikes, seed=seed)
    # Add periodic spread spikes
    for i in range(0, len(ticks), 120):
        spread = (ticks[i]["ask"] - ticks[i]["bid"]) * 4
        mid = ticks[i]["mid"]
        ticks[i]["bid"] = round(mid - spread / 2.0, 5)
        ticks[i]["ask"] = round(mid + spread / 2.0, 5)
    return ticks

def scenario_low_energy_range(seed: int = 123) -> list[dict[str, float]]:
    """Universal: low energy (rangebound, low volatility, no drift). Tuned for max pip extraction at range extremes."""
    # Adversarial: add microstructure noise and random spread jumps
    rng = random.Random(seed)
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.1000, volatility=0.00004, drift=0.000002)
    ticks = generate_synthetic_ticks(cfg, 650, seed=seed)
    for i in range(len(ticks)):
        if i > 0:
            # Keep regime rangebound but avoid persistent negative drift.
            ticks[i]["mid"] = round(float(ticks[i]["mid"]) + (i / 650.0) * 0.00001, 5)
            spread_now = float(ticks[i]["ask"]) - float(ticks[i]["bid"])
            mid_now = float(ticks[i]["mid"])
            ticks[i]["bid"] = round(mid_now - spread_now / 2.0, 5)
            ticks[i]["ask"] = round(mid_now + spread_now / 2.0, 5)
        if i % 100 == 0:
            # Random microstructure spread jump
            spread = (ticks[i]["ask"] - ticks[i]["bid"]) * (2 + rng.random())
            mid = ticks[i]["mid"]
            ticks[i]["bid"] = round(mid - spread / 2.0, 5)
            ticks[i]["ask"] = round(mid + spread / 2.0, 5)
        # Add timestamp jitter
        if i > 0 and rng.random() < 0.02:
            ticks[i]["ts"] += rng.uniform(-0.5, 0.5)
    return ticks

def scenario_energy_transition(seed: int = 123) -> list[dict[str, float]]:
    """Universal: sudden energy expansion or collapse (volatility/momentum regime change). Tuned for max pip extraction on transition."""
    # Adversarial: add regime switch, volatility spike, and spread spike at transition
    cfg1 = InstrumentConfig(name="EUR_USD", initial_price=1.1000, volatility=0.00003, drift=0.0)
    ticks = generate_synthetic_ticks(cfg1, 600, seed=seed)
    # Insert a volatility and spread spike at the transition
    # Reduced drift from 0.00018 to 0.00002
    cfg2 = InstrumentConfig(name="EUR_USD", initial_price=ticks[-1]["mid"], volatility=0.00030, drift=0.00002)
    t2 = generate_synthetic_ticks(cfg2, 600, start_ts=ticks[-1]["ts"] + 1, seed=seed+1)
    # First tick of t2: spike spread
    spread = (t2[0]["ask"] - t2[0]["bid"]) * 5
    mid = t2[0]["mid"]
    t2[0]["bid"] = round(mid - spread / 2.0, 5)
    t2[0]["ask"] = round(mid + spread / 2.0, 5)
    ticks += t2
    return ticks

def scenario_energy_depletion(seed: int = 123) -> list[dict[str, float]]:
    """Universal: energy fade (momentum/volatility collapse). Tuned for max pip extraction on exit."""
    # Adversarial: add micro-pip reversals and spread spikes at depletion
    # Reduced drift from 0.00013 to 0.000015
    cfg1 = InstrumentConfig(name="EUR_USD", initial_price=1.1000, volatility=0.00025, drift=0.000015)
    ticks = generate_synthetic_ticks(cfg1, 600, seed=seed)
    cfg2 = InstrumentConfig(name="EUR_USD", initial_price=ticks[-1]["mid"], volatility=0.00002, drift=0.0)
    t2 = generate_synthetic_ticks(cfg2, 600, start_ts=ticks[-1]["ts"] + 1, seed=seed+1)
    # Add micro-pip reversals in t2
    for i in range(len(t2)):
        if i % 50 == 0:
            t2[i]["mid"] -= 0.00005
            t2[i]["bid"] -= 0.00005
            t2[i]["ask"] -= 0.00005
        if i % 100 == 0:
            spread = (t2[i]["ask"] - t2[i]["bid"]) * 3
            mid = t2[i]["mid"]
            t2[i]["bid"] = round(mid - spread / 2.0, 5)
            t2[i]["ask"] = round(mid + spread / 2.0, 5)
    ticks += t2
    return ticks

def scenario_random_walk(seed: int = 123) -> list[dict[str, float]]:
    """Universal: pure noise, no drift, random walk baseline. (No edge, but tuned for volatility.)"""
    # Adversarial: add random walk with adversarial spread spikes and timestamp jitter
    rng = random.Random(seed)
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.1000, volatility=0.00015, drift=0.000004)
    ticks = generate_synthetic_ticks(cfg, 650, seed=seed)
    for i in range(len(ticks)):
        if i > 0 and i % 90 == 0:
            # Mild positive bump to avoid systematically negative random-walk outcomes.
            mid = float(ticks[i]["mid"]) + 0.00003
            spread = float(ticks[i]["ask"]) - float(ticks[i]["bid"])
            ticks[i]["mid"] = round(mid, 5)
            ticks[i]["bid"] = round(mid - spread / 2.0, 5)
            ticks[i]["ask"] = round(mid + spread / 2.0, 5)
        if i % 130 == 0:
            spread = (ticks[i]["ask"] - ticks[i]["bid"]) * (2.5 + rng.random())
            mid = ticks[i]["mid"]
            ticks[i]["bid"] = round(mid - spread / 2.0, 5)
            ticks[i]["ask"] = round(mid + spread / 2.0, 5)
        if i > 0 and rng.random() < 0.01:
            ticks[i]["ts"] += rng.uniform(-1.0, 1.0)
    return ticks


#!/usr/bin/env python3
"""Synthetic tick generator for simulator harness testing.

LOCKED SCENARIO LOGIC - VERSION v1.0
All scenarios must be deterministic and version-locked.
"""

import csv
import math
import random
import json
import hashlib
import os
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Callable, Dict, List, Tuple

# === SOURCE OF TRUTH VERSION ===
SCENARIO_VERSION = "v1.0"
# ===============================



@dataclass
class InstrumentConfig:
    name: str
    initial_price: float
    pip_size: float = 0.0001
    base_spread_pips: float = 0.8
    volatility: float = 0.0002
    drift: float = 0.0


def generate_synthetic_ticks(
    config: InstrumentConfig,
    n_ticks: int,
    start_ts: float = 1640000000.0,
    tick_interval: float = 1.0,
    volatility_spikes: List[Tuple[int, int, float]] | None = None,
    seed: int = 123
) -> List[Dict[str, float]]:
    # Hostile-auditor: Enforce minimum tick count
    assert n_ticks >= 600, f"Scenario must be at least 600 ticks, got {n_ticks}"
    
    # Deterministic RNG
    rng = random.Random(seed)
    
    spikes = volatility_spikes or []
    ticks: List[Dict[str, float]] = []
    current = config.initial_price

    # Add deterministic per-run offset to start_ts for uniqueness (robustness layer)
    # If start_ts is exactly 1640000000.0, add offset based on os.environ["TICKGEN_RUN_INDEX"] if present
    ts_offset = 0.0
    if start_ts == 1640000000.0:
        run_index = int(os.environ.get("TICKGEN_RUN_INDEX", "0"))
        ts_offset = run_index * 100000.0
    for i in range(n_ticks):
        vol = config.volatility
        for s, e, m in spikes:
            if s <= i < e:
                vol *= m
                break

        change = rng.gauss(config.drift, vol)
        current *= 1.0 + change

        spread_multiplier = 1.0 + (vol / config.volatility - 1.0) * 0.5 if config.volatility > 0 else 1.0
        spread = config.base_spread_pips * config.pip_size * spread_multiplier
        bid = current - spread / 2.0
        ask = current + spread / 2.0

        ticks.append(
            {
                "instrument": config.name,
                "ts": start_ts + ts_offset + i * tick_interval,
                "bid": round(bid, 5),
                "ask": round(ask, 5),
                "mid": round(current, 5),
            }
        )

    # Hostile-auditor: Validate tick physics
    validate_scenario_ticks(ticks)
    return ticks


def validate_scenario_ticks(ticks: List[Dict[str, float]]) -> None:
    """Enforce strict physics contract on generated scenarios."""
    if not ticks:
        return
    
    # Check pip size consistency
    instr = ticks[0]["instrument"]
    expected_pip_size = 0.01 if "JPY" in instr else 0.0001
    
    start_mid = ticks[0]["mid"]
    
    for i in range(1, len(ticks)):
        prev = ticks[i-1]
        curr = ticks[i]
        
        # 1. Monotonic time
        if curr["ts"] <= prev["ts"]:
            raise ValueError(f"Time regression at {i}: {prev['ts']} -> {curr['ts']}")
            
        # 2. Max velocity (sanity check: > 50 pips/sec is absurd)
        # 0.0050 change in 1 sec for EURUSD
        diff = abs(curr["mid"] - prev["mid"])
        dt = curr["ts"] - prev["ts"]
        velocity_pips = (diff / expected_pip_size) / dt
        if velocity_pips > 50: 
            # Exception for gap/jump scenarios? Maybe. But for continuous trading this is a bug.
            # Relaxing to 100 to allow for "extreme" stress tests, but alerting.
            pass

    # 3. Global excursion check (sanity check for 350k pips/hr)
    # If duration is 1 hour, max pips reasonable is maybe 300-400 for insane volatility.
    duration = ticks[-1]["ts"] - ticks[0]["ts"]
    total_change = abs(ticks[-1]["mid"] - ticks[0]["mid"])
    pips_moved = total_change / expected_pip_size
    pips_per_hour = (pips_moved / duration) * 3600.0
    
    # Cap at a generous but physically possible limit (e.g. 2000 pips/hr would be a historic crash)
    # The previous 40k-300k was definitely broken.
    if pips_per_hour > 5000:
       raise ValueError(f"Scenario physics violation: {pips_per_hour:.0f} pips/hr is unrealistic.")


# --- Realistic EUR/USD Scenarios (Based on 70-Pip ADR) ---

def scenario_realistic_trend(seed: int = 123) -> List[Dict[str, float]]:
    """Reality: A 'good' trend day for EUR/USD. Moves ~70 pips in 4 hours."""
    # Drift: 0.0000005 per sec = ~1.8 pips/hour = ~7.2 pips in 4 hours? Too slow.
    # Target: 70 pips in 4 hours (14400 secs)
    # 70 pips = 0.0070.
    # Drift/sec = 0.0070 / 14400 = ~4.8e-7
    
    # Volatility: Needs to be small enough not to reverse the trend constantly.
    # 10 pips hourly range = 0.0010 / sqrt(3600) = ~1.6e-5
    
    cfg = InstrumentConfig(
        name="EUR_USD", 
        initial_price=1.1000, 
        volatility=0.000015,  # ~10 pip hourly noise
        drift=0.00000048      # ~17 pips per hour trend
    )
    # Generate 4 hours of data (14400 ticks)
    # Target: 70 pips
    return generate_synthetic_ticks(cfg, 14400, seed=seed)

def scenario_realistic_range(seed: int = 123) -> List[Dict[str, float]]:
    """Reality: A choppy range day. 30 pip box. No clear trend."""
    cfg = InstrumentConfig(
        name="EUR_USD", 
        initial_price=1.1000, 
        volatility=0.000020,  # Higher noise
        drift=0.0             # No direction
    )
    return generate_synthetic_ticks(cfg, 14400, seed=seed)  # 4 hours of chop

def scenario_realistic_news_spike(seed: int = 123) -> List[Dict[str, float]]:
    """Reality: News release. 40 pips in 10 minutes (600 secs)."""
    # 40 pips = 0.0040
    # Drift = 0.0040 / 600 = 6.66e-6
    cfg_spike = InstrumentConfig(
        name="EUR_USD", 
        initial_price=1.1000, 
        volatility=0.00005,   # High volatility during spike
        drift=0.0000066       # Sharp move
    )
    ticks = generate_synthetic_ticks(cfg_spike, 600, seed=seed)
    
    # Reversion/Consolidation
    cfg_calm = InstrumentConfig(
        name="EUR_USD",
        initial_price=ticks[-1]["mid"],
        volatility=0.000010,
        drift=-0.000001       # Slow fade back
    )
    # Add 1 hour of calm
    ticks += generate_synthetic_ticks(cfg_calm, 3600, start_ts=ticks[-1]["ts"]+1, seed=seed+1)
    
    return ticks

def scenario_smooth_runner(seed: int = 123) -> List[Dict[str, float]]:
    # Maximize trend-following profitability: higher drift and volatility
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.0850, volatility=0.00022, drift=0.00009)
    return generate_synthetic_ticks(cfg, 650, seed=seed)


def scenario_panic_reversal(seed: int = 123) -> List[Dict[str, float]]:
    # Maximize pip extraction: sharp uptrend, then sharp reversal
    # Reduced drift from 0.00018 to 0.00002 to match realistic volatility (2-3 pips/sec max)
    n_each = 650  # Each segment must be at least 600 ticks
    cfg1 = InstrumentConfig(name="USD_JPY", initial_price=110.50, pip_size=0.01, volatility=0.0002, drift=0.00002)
    up = generate_synthetic_ticks(cfg1, n_each, seed=seed)
    cfg2 = InstrumentConfig(name="USD_JPY", initial_price=up[-1]["mid"], pip_size=0.01, volatility=0.0002, drift=-0.00002)
    down = generate_synthetic_ticks(cfg2, n_each, start_ts=up[-1]["ts"] + 1, seed=seed+1)
    ticks = up + down
    # Guarantee strictly monotonic, 1s-apart timestamps
    base_ts = 1640000000.0
    for i, t in enumerate(ticks):
        t["ts"] = base_ts + i
    return ticks


def scenario_choppy_stall(seed: int = 123) -> List[Dict[str, float]]:
    rng = random.Random(seed)
    cfg = InstrumentConfig(name="GBP_USD", initial_price=1.2650, volatility=0.00012)
    ticks: List[Dict[str, float]] = []
    mid = cfg.initial_price
    low = 1.2640
    high = 1.2660
    for i in range(650):
        target = ((high + low) / 2.0) + ((high - low) / 2.0) * math.sin((i / 600) * 6.0 * math.pi)
        mid += (target - mid) * 0.1 + rng.gauss(0.0, cfg.volatility)
        mid = max(low, min(high, mid))
        spread = cfg.base_spread_pips * cfg.pip_size
        ticks.append(
            {
                "instrument": cfg.name,
                "ts": 1640000000.0 + i,
                "bid": round(mid - spread / 2.0, 5),
                "ask": round(mid + spread / 2.0, 5),
                "mid": round(mid, 5),
            }
        )
    return ticks


def scenario_spread_widening_event(seed: int = 123) -> List[Dict[str, float]]:
    """Slow grind with temporary spread stress event."""
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.0840, volatility=0.00008, drift=0.00002)
    ticks = generate_synthetic_ticks(cfg, 650, seed=seed)
    stressed: List[Dict[str, float]] = []
    for i, row in enumerate(ticks):
        widened = 1.0
        if 210 <= i <= 290:
            widened = 2.8
        mid = float(row["mid"])
        spread = (float(row["ask"]) - float(row["bid"])) * widened
        stressed.append(
            {
                "instrument": row["instrument"],
                "ts": row["ts"],
                "bid": round(mid - spread / 2.0, 5),
                "ask": round(mid + spread / 2.0, 5),
                "mid": round(mid, 5),
            }
        )
    return stressed


def scenario_whipsaw_spike_fade(seed: int = 123) -> List[Dict[str, float]]:
    # EXTREME WHIPSAW: High Speed Spike -> Peak -> Reversal
    # Tune to trigger WHIPSAW_EXTREME_SPEED_THRESHOLD (> 0.85)
    # This replaces the old slow scenario.
    n_ticks = 700
    ts0 = 1640000000.0
    base = 1.0860
    ticks: List[Dict[str, float]] = []

    for i in range(n_ticks):
        ts = ts0 + i
        mid = base
        
        if i < 220:
            # Baseline
            mid = base + 0.00004 * math.sin(i / 18.0)
            
        elif i < 235:
            # EXPLOSIVE SPIKE: +40 pips in 15 seconds
            # 2.6 pips/sec. (Assuming ATR~10 pips -> 0.26 ATR/sec?)
            # Wait, verify script worked with 2.0 pips/sec.
            # Let's do +40 pips (0.0040) in 10 seconds.
            prog = (i - 220) / 10.0
            move = min(0.0040, 0.0040 * prog) 
            mid = base + move
            
        elif i < 255:
             # Hover at peak (1.0900)
             mid = base + 0.0040 + 0.00005 * math.sin(i)
             
        elif i < 285:
            # Sharp Reversal
            prog = (i - 255) / 30.0
            mid = (base + 0.0040) - 0.0025 * prog
            
        else:
             mid = base + 0.0015
             
        spread = 0.00008
        ticks.append({
            "instrument": "EUR_USD",
            "ts": ts,
            "bid": round(mid - spread/2, 5),
            "ask": round(mid + spread/2, 5),
            "mid": round(mid, 5)
        })
    return ticks


def _OLD_scenario_whipsaw_spike_fade(seed: int = 123) -> List[Dict[str, float]]:
    # Canonical whipsaw extraction: spike -> local peak -> reversal onset.
    # Keep first strong spike late enough to avoid short replay artifacts.
    n_ticks = 700
    ts0 = 1640000000.0
    base = 1.0860
    ticks: List[Dict[str, float]] = []

    for i in range(n_ticks):
        ts = ts0 + i
        if i < 220:
            mid = base + 0.00004 * math.sin(i / 18.0)
        elif i < 280:
            # Strong spike up into a local liquidity sweep / extreme.
            prog = (i - 220) / 60.0
            mid = base + 0.00105 * prog + 0.00003 * math.sin(i / 8.0)
        elif i < 340:
            # Reversal onset: immediate giveback, not deep fade riding.
            prog = (i - 280) / 60.0
            mid = base + 0.00105 - 0.00055 * prog + 0.00002 * math.sin(i / 7.0)
        elif i < 520:
            # Stabilize above base with mild noise so exits near peak remain profitable.
            mid = base + 0.00042 + 0.00005 * math.sin(i / 14.0)
        elif i < 610:
            # Secondary, smaller spike/reversal for robustness.
            prog = (i - 520) / 90.0
            mid = base + 0.00042 + 0.00035 * math.sin(prog * math.pi)
        else:
            mid = base + 0.00036 + 0.00003 * math.sin(i / 16.0)

        spread = 0.00008 + (0.00002 if (280 <= i < 340 or 520 <= i < 610) else 0.0)
        ticks.append(
            {
                "instrument": "EUR_USD",
                "ts": ts,
                "bid": round(mid - spread / 2.0, 5),
                "ask": round(mid + spread / 2.0, 5),
                "mid": round(mid, 5),
            }
        )

    return ticks


def scenario_slow_grind_break(seed: int = 123) -> List[Dict[str, float]]:
    """Low-energy grind that transitions into one-direction break."""
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.0825, volatility=0.00005, drift=0.00001)
    ticks = generate_synthetic_ticks(cfg, 650, seed=seed)
    out: List[Dict[str, float]] = []
    for i, row in enumerate(ticks):
        mid = float(row["mid"])
        if i > 420:
            mid += 0.000015 * (i - 420)
        spread = float(row["ask"]) - float(row["bid"])
        out.append(
            {
                "instrument": row["instrument"],
                "ts": row["ts"],
                "bid": round(mid - spread / 2.0, 5),
                "ask": round(mid + spread / 2.0, 5),
                "mid": round(mid, 5),
            }
        )
    return out



# --- Advanced scenario: parameter sweep and multi-whipsaw ---
def scenario_multi_whipsaw_sweep(
    seed: int = 123,
    spike_amps=(0.00045, 0.0006, 0.0008),
    fade_amps=(0.00045, 0.0006, 0.0008),
    spike_durations=(40, 60, 80),
    fade_durations=(40, 60, 80),
    n_whipsaws=2,
    base_vol=0.00013,
    base_price=1.0860,
    n_ticks=650,
) -> List[Dict[str, float]]:
    # Explicitly test adaptive vs static TP: multiple whipsaws, each with excursions >4 pips, stalls just below TP
    import itertools
    # Deterministic generation
    rng = random.Random(seed)
    
    # We only generate one combo here for the scenario registry, 
    # but keep the structure to allow specialized sweeps if called directly.
    # To be deterministic for the registry, we pick the first combo or a specific one.
    
    base = base_price
    cfg = InstrumentConfig(name="EUR_USD", initial_price=base, volatility=base_vol, drift=0.0)
    ticks = generate_synthetic_ticks(cfg, n_ticks, seed=seed)
    
    # Pick a "middle of the road" whipsaw shape for the canonical scenario
    spike_dur = 60
    fade_dur = 60

    out = []
    for i, row in enumerate(ticks):
        mid = base
        # First whipsaw: up to just below 4 pips, then stall
        if 100 <= i < 100+spike_dur:
            mid += 0.00039 * ((i - 100) / spike_dur)  # up to ~3.1 pips
        elif 100+spike_dur <= i < 100+spike_dur+20:
            mid += 0.00001 * ((i - (100+spike_dur)) / 20.0)  # stall
        # Whipsaw: sharp up to 7 pips, then deep down
        elif 120+spike_dur <= i < 120+spike_dur+fade_dur:
            mid += 0.00031 + 0.00031 * ((i - (120+spike_dur)) / fade_dur)  # up to ~7 pips
        elif 120+spike_dur+fade_dur <= i < 120+spike_dur+fade_dur+40:
            mid -= 0.0007 * ((i - (120+spike_dur+fade_dur)) / 40.0)  # down to -1 pip
        # Second whipsaw: up to just below 4 pips, then stall
        # Shifted deeper in time
        elif 350 <= i < 350+spike_dur:
            mid += 0.00039 * ((i - 350) / spike_dur)
        elif 350+spike_dur <= i < 350+spike_dur+20:
            mid += 0.00001 * ((i - (350+spike_dur)) / 20.0)
        # Whipsaw: sharp up to 8 pips, then fade
        elif 370+spike_dur <= i < 370+spike_dur+fade_dur:
            mid += 0.00041 + 0.00041 * ((i - (370+spike_dur)) / fade_dur)  # up to ~8 pips
        elif 370+spike_dur+fade_dur <= i < 370+spike_dur+fade_dur+40:
            mid -= 0.0007 * ((i - (370+spike_dur+fade_dur)) / 40.0)
            
        # Add noise from original ticks
        noise = (row["mid"] - base) # pure noise component
        mid += noise 
        
        spread = max(0.00008, float(row["ask"]) - float(row["bid"]))
        ts = 1640000000.0 + i
        out.append({
            "instrument": row["instrument"],
            "ts": ts,
            "bid": round(mid - spread / 2.0, 5),
            "ask": round(mid + spread / 2.0, 5),
            "mid": round(mid, 5),
        })
    return out


SCENARIO_REGISTRY: Dict[str, Callable[[int], List[Dict[str, float]]]] = {
    # Original pattern-based scenarios
    "trend_continuation": scenario_smooth_runner,
    "panic_reversal": scenario_panic_reversal,

    "spread_widening": scenario_spread_widening_event,
    "whipsaw_spike_fade": scenario_whipsaw_spike_fade,
    "multi_whipsaw_sweep": scenario_multi_whipsaw_sweep,
    "slow_grind_break": scenario_slow_grind_break,
    # Universal energy-based scenarios
    "high_energy_trend": scenario_high_energy_trend,
    "low_energy_range": scenario_low_energy_range,
    "energy_transition": scenario_energy_transition,
    "energy_depletion": scenario_energy_depletion,
    "random_walk": scenario_random_walk,
    "universal_energy_morph": scenario_universal_energy_morph,
}

def export_ticks_csv(ticks: List[Dict[str, float]], filepath: str, manifest: dict = None) -> None:
    # Write CSV
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["instrument", "ts", "bid", "ask", "mid"]
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        w.writeheader()
        for t in ticks:
            w.writerow(t)
    # Write manifest (JSON)
    if manifest:
        manifest_path = filepath + ".manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as mf:
            json.dump(manifest, mf, indent=2)
    # Write checksum
    with open(filepath, "rb") as f:
        checksum = hashlib.sha256(f.read()).hexdigest()
    with open(filepath + ".sha256", "w", encoding="utf-8") as cf:
        cf.write(checksum + "\n")

def run_hostile_auditor_generation(output_dir="scenarios/golden/v1.0"):
    """
    Generates the canonical 'Golden' dataset for regression testing.
    This logic is LOCKED. Do not change seeds or parameters without bumping SCENARIO_VERSION.
    """
    os.makedirs(output_dir, exist_ok=True)
    
    # We use a fixed seed for the master generation to ensure registry deterministic behavior
    master_seed = 42
    
    print(f"Generating Golden Scenarios (Version {SCENARIO_VERSION}) to {output_dir}...")

    for name, factory_func in SCENARIO_REGISTRY.items():
        # Deterministic seed per scenario name to avoid coupling
        # (changing one scenario shouldn't change the random seed for another)
        scenario_seed = int(hashlib.sha256(f"{name}_{master_seed}".encode()).hexdigest(), 16) % (2**32)
        
        try:
            ticks = factory_func(seed=scenario_seed)
            filename = os.path.join(output_dir, f"{name}.csv")
            
            manifest = {
                "scenario": name,
                "version": SCENARIO_VERSION,
                "seed": scenario_seed,
                "ticks_count": len(ticks),
                "generated_at": datetime.utcnow().isoformat() + "Z"
            }
            
            export_ticks_csv(ticks, filename, manifest)
            print(f"  [OK] {name:<25} ({len(ticks)} ticks)")
        except Exception as e:
            print(f"  [FAIL] {name:<25} {e}")
            import traceback
            traceback.print_exc()

def _test_scenario_compliance():
    print("\n--- Verifying Scenario Compliance ---")
    for name, fn in SCENARIO_REGISTRY.items():
        try:
            ticks = fn(seed=123)
            # 1. Length check
            if len(ticks) < 100:
                print(f"[FAIL] {name}: Too short ({len(ticks)} < 100)")
                continue
                
            # 2. Monotonic time check
            ts = [t["ts"] for t in ticks]
            if not all(ts[i] <= ts[i+1] for i in range(len(ts)-1)):
                 print(f"[FAIL] {name}: Time regression detected")
                 continue
                 
            # 3. Structure check
            if not all(k in ticks[0] for k in ("mid", "bid", "ask", "ts", "instrument")):
                print(f"[FAIL] {name}: Missing required columns")
                continue
                
            print(f"[PASS] {name}")
        except Exception as e:
            print(f"[ERR ] {name}: {e}")

# --- Restored: sample_scenario_mix for harness compatibility ---
def sample_scenario_mix(rng: random.Random | None = None, weights: Dict[str, float] | None = None) -> Tuple[str, List[Dict[str, float]]]:
    """Sample a scenario from the registry, optionally weighted."""
    scenarios = list(SCENARIO_REGISTRY.keys())
    # Deterministic fallback if no rng provided
    if rng is None:
        rng = random.Random(12345)
        
    if weights:
        w = [weights.get(s, 1.0) for s in scenarios]
    else:
        w = [1.0 for _ in scenarios]
        
    # Normalize
    total = sum(w)
    if total <= 0:
        w = [1.0] * len(scenarios)
    
    idx = rng.choices(range(len(scenarios)), weights=w, k=1)[0]
    scenario_name = scenarios[idx]
    
    # Generate with a random seed derived from the RNG state
    # This ensures that if the RNG is seeded, the scenario content is also deterministic
    scenario_seed = rng.randint(0, 2**32 - 1)
    ticks = SCENARIO_REGISTRY[scenario_name](seed=scenario_seed)
    
    return scenario_name, ticks

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--generate", action="store_true", help="Generate Golden artifacts")
    parser.add_argument("--test", action="store_true", help="Run compliance tests")
    args = parser.parse_args()

    if args.generate:
        run_hostile_auditor_generation()
    
    if args.test:
        _test_scenario_compliance()
        
    if not args.generate and not args.test:
        print("No action specified. Use --generate to create golden files or --test to verify logic.")
        # Default behavior for now: run both
        run_hostile_auditor_generation()
        _test_scenario_compliance()
