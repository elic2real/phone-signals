#!/usr/bin/env python3
"""Synthetic tick generator for simulator harness testing."""

from __future__ import annotations

import csv
import math
import random
from dataclasses import dataclass
from typing import Callable, Dict, List, Tuple


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
) -> List[Dict[str, float]]:
    spikes = volatility_spikes or []
    ticks: List[Dict[str, float]] = []
    current = config.initial_price

    for i in range(n_ticks):
        vol = config.volatility
        for s, e, m in spikes:
            if s <= i < e:
                vol *= m
                break

        change = random.gauss(config.drift, vol)
        current *= 1.0 + change

        spread_multiplier = 1.0 + (vol / config.volatility - 1.0) * 0.5 if config.volatility > 0 else 1.0
        spread = config.base_spread_pips * config.pip_size * spread_multiplier
        bid = current - spread / 2.0
        ask = current + spread / 2.0

        ticks.append(
            {
                "instrument": config.name,
                "ts": start_ts + i * tick_interval,
                "bid": round(bid, 5),
                "ask": round(ask, 5),
                "mid": round(current, 5),
            }
        )

    return ticks


def scenario_smooth_runner() -> List[Dict[str, float]]:
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.0850, volatility=0.0001)
    return generate_synthetic_ticks(cfg, 500, volatility_spikes=[(250, 320, 0.6)])


def scenario_panic_reversal() -> List[Dict[str, float]]:
    cfg = InstrumentConfig(name="USD_JPY", initial_price=110.50, pip_size=0.01, volatility=0.0002)
    return generate_synthetic_ticks(cfg, 400, volatility_spikes=[(200, 260, 8.0)])


def scenario_choppy_stall() -> List[Dict[str, float]]:
    cfg = InstrumentConfig(name="GBP_USD", initial_price=1.2650, volatility=0.00012)
    ticks: List[Dict[str, float]] = []
    mid = cfg.initial_price
    low = 1.2640
    high = 1.2660
    for i in range(600):
        target = ((high + low) / 2.0) + ((high - low) / 2.0) * math.sin((i / 600) * 6.0 * math.pi)
        mid += (target - mid) * 0.1 + random.gauss(0.0, cfg.volatility)
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


def scenario_spread_widening_event() -> List[Dict[str, float]]:
    """Slow grind with temporary spread stress event."""
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.0840, volatility=0.00008, drift=0.00002)
    ticks = generate_synthetic_ticks(cfg, 520)
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


def scenario_whipsaw_spike_fade() -> List[Dict[str, float]]:
    """Fast spike, brief overshoot, then fade/reversal."""
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.0860, volatility=0.00007, drift=0.0)
    ticks = generate_synthetic_ticks(cfg, 420)
    out: List[Dict[str, float]] = []
    for i, row in enumerate(ticks):
        mid = float(row["mid"])
        # impulse up, then abrupt fade
        if 120 <= i < 150:
            mid += 0.0009 * ((i - 120) / 30.0)
        elif 150 <= i < 190:
            mid += 0.0009 * max(0.0, 1.0 - ((i - 150) / 40.0))
        elif 190 <= i < 240:
            mid -= 0.0005 * ((i - 190) / 50.0)
        spread = max(0.00006, float(row["ask"]) - float(row["bid"]))
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


def scenario_slow_grind_break() -> List[Dict[str, float]]:
    """Low-energy grind that transitions into one-direction break."""
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.0825, volatility=0.00005, drift=0.00001)
    ticks = generate_synthetic_ticks(cfg, 620)
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


SCENARIO_REGISTRY: Dict[str, Callable[[], List[Dict[str, float]]]] = {
    "trend_continuation": scenario_smooth_runner,
    "panic_reversal": scenario_panic_reversal,
    "chop_mean_reversion": scenario_choppy_stall,
    "spread_widening": scenario_spread_widening_event,
    "whipsaw_spike_fade": scenario_whipsaw_spike_fade,
    "slow_grind_break": scenario_slow_grind_break,
}


def sample_scenario_mix(rng: random.Random | None = None, weights: Dict[str, float] | None = None) -> Tuple[str, List[Dict[str, float]]]:
    """Sample one scenario family by weighted mix and return name + ticks."""
    rng = rng or random.Random()
    if not weights:
        weights = {
            "trend_continuation": 1.0,
            "panic_reversal": 1.0,
            "chop_mean_reversion": 1.0,
            "spread_widening": 1.0,
            "whipsaw_spike_fade": 1.0,
            "slow_grind_break": 1.0,
        }
    names = [k for k in SCENARIO_REGISTRY if float(weights.get(k, 0.0)) > 0]
    if not names:
        names = list(SCENARIO_REGISTRY.keys())
    vals = [max(0.0, float(weights.get(k, 1.0))) for k in names]
    total = sum(vals)
    if total <= 0:
        vals = [1.0 for _ in names]
    pick = rng.choices(names, weights=vals, k=1)[0]
    return pick, SCENARIO_REGISTRY[pick]()


def export_ticks_csv(ticks: List[Dict[str, float]], filepath: str) -> None:
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["instrument", "ts", "bid", "ask"])
        w.writeheader()
        for t in ticks:
            w.writerow({"instrument": t["instrument"], "ts": t["ts"], "bid": t["bid"], "ask": t["ask"]})


if __name__ == "__main__":
    export_ticks_csv(scenario_smooth_runner(), "scenario_runner.csv")
    export_ticks_csv(scenario_panic_reversal(), "scenario_panic.csv")
    export_ticks_csv(scenario_choppy_stall(), "scenario_stall.csv")
    combo = scenario_smooth_runner() + scenario_panic_reversal() + scenario_choppy_stall()
    combo.sort(key=lambda x: x["ts"])
    export_ticks_csv(combo, "scenario_multi.csv")
    print("Synthetic scenarios written: scenario_runner.csv, scenario_panic.csv, scenario_stall.csv, scenario_multi.csv")
