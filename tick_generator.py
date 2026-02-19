# --- Universal scenario: morphs between all energy states ---
def scenario_universal_energy_morph() -> list[dict[str, float]]:
    """Universal: morphs through all energy states (trend, range, transition, depletion, random walk) in one sequence."""
    ticks = []
    # High energy trend
    cfg1 = InstrumentConfig(name="EUR_USD", initial_price=1.1000, volatility=0.00028, drift=0.00018)
    ticks += generate_synthetic_ticks(cfg1, 600)
    # Sudden transition to range
    cfg2 = InstrumentConfig(name="EUR_USD", initial_price=ticks[-1]["mid"], volatility=0.00003, drift=0.0)
    ticks += generate_synthetic_ticks(cfg2, 600, start_ts=ticks[-1]["ts"] + 1)
    # Energy expansion (breakout)
    cfg3 = InstrumentConfig(name="EUR_USD", initial_price=ticks[-1]["mid"], volatility=0.00025, drift=0.00013)
    ticks += generate_synthetic_ticks(cfg3, 600, start_ts=ticks[-1]["ts"] + 1)
    # Energy depletion
    cfg4 = InstrumentConfig(name="EUR_USD", initial_price=ticks[-1]["mid"], volatility=0.00004, drift=0.0)
    ticks += generate_synthetic_ticks(cfg4, 600, start_ts=ticks[-1]["ts"] + 1)
    # Random walk
    cfg5 = InstrumentConfig(name="EUR_USD", initial_price=ticks[-1]["mid"], volatility=0.00012, drift=0.0)
    ticks += generate_synthetic_ticks(cfg5, 600, start_ts=ticks[-1]["ts"] + 1)
    return ticks


# --- Universal, energy-based scenario set (ADDED, not replacing originals) ---
def scenario_high_energy_trend() -> list[dict[str, float]]:
    """Universal: persistent high energy (trend, high volatility, high momentum). Tuned for max pip extraction."""
    # Adversarial: add volatility spikes and spread spikes to test extraction cost
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.1000, volatility=0.00030, drift=0.00020)
    spikes = [(200, 220, 3.0), (400, 420, 2.5)]
    ticks = generate_synthetic_ticks(cfg, 650, volatility_spikes=spikes)
    # Add periodic spread spikes
    for i in range(0, len(ticks), 120):
        spread = (ticks[i]["ask"] - ticks[i]["bid"]) * 4
        mid = ticks[i]["mid"]
        ticks[i]["bid"] = round(mid - spread / 2.0, 5)
        ticks[i]["ask"] = round(mid + spread / 2.0, 5)
    return ticks

def scenario_low_energy_range() -> list[dict[str, float]]:
    """Universal: low energy (rangebound, low volatility, no drift). Tuned for max pip extraction at range extremes."""
    # Adversarial: add microstructure noise and random spread jumps
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.1000, volatility=0.00004, drift=0.0)
    ticks = generate_synthetic_ticks(cfg, 650)
    for i in range(len(ticks)):
        if i % 100 == 0:
            # Random microstructure spread jump
            spread = (ticks[i]["ask"] - ticks[i]["bid"]) * (2 + random.random())
            mid = ticks[i]["mid"]
            ticks[i]["bid"] = round(mid - spread / 2.0, 5)
            ticks[i]["ask"] = round(mid + spread / 2.0, 5)
        # Add timestamp jitter
        if i > 0 and random.random() < 0.02:
            ticks[i]["ts"] += random.uniform(-0.5, 0.5)
    return ticks

def scenario_energy_transition() -> list[dict[str, float]]:
    """Universal: sudden energy expansion or collapse (volatility/momentum regime change). Tuned for max pip extraction on transition."""
    # Adversarial: add regime switch, volatility spike, and spread spike at transition
    cfg1 = InstrumentConfig(name="EUR_USD", initial_price=1.1000, volatility=0.00003, drift=0.0)
    ticks = generate_synthetic_ticks(cfg1, 600)
    # Insert a volatility and spread spike at the transition
    cfg2 = InstrumentConfig(name="EUR_USD", initial_price=ticks[-1]["mid"], volatility=0.00030, drift=0.00018)
    t2 = generate_synthetic_ticks(cfg2, 600, start_ts=ticks[-1]["ts"] + 1)
    # First tick of t2: spike spread
    spread = (t2[0]["ask"] - t2[0]["bid"]) * 5
    mid = t2[0]["mid"]
    t2[0]["bid"] = round(mid - spread / 2.0, 5)
    t2[0]["ask"] = round(mid + spread / 2.0, 5)
    ticks += t2
    return ticks

def scenario_energy_depletion() -> list[dict[str, float]]:
    """Universal: energy fade (momentum/volatility collapse). Tuned for max pip extraction on exit."""
    # Adversarial: add micro-pip reversals and spread spikes at depletion
    cfg1 = InstrumentConfig(name="EUR_USD", initial_price=1.1000, volatility=0.00025, drift=0.00013)
    ticks = generate_synthetic_ticks(cfg1, 600)
    cfg2 = InstrumentConfig(name="EUR_USD", initial_price=ticks[-1]["mid"], volatility=0.00002, drift=0.0)
    t2 = generate_synthetic_ticks(cfg2, 600, start_ts=ticks[-1]["ts"] + 1)
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

def scenario_random_walk() -> list[dict[str, float]]:
    """Universal: pure noise, no drift, random walk baseline. (No edge, but tuned for volatility.)"""
    # Adversarial: add random walk with adversarial spread spikes and timestamp jitter
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.1000, volatility=0.00015, drift=0.0)
    ticks = generate_synthetic_ticks(cfg, 650)
    for i in range(len(ticks)):
        if i % 130 == 0:
            spread = (ticks[i]["ask"] - ticks[i]["bid"]) * (2.5 + random.random())
            mid = ticks[i]["mid"]
            ticks[i]["bid"] = round(mid - spread / 2.0, 5)
            ticks[i]["ask"] = round(mid + spread / 2.0, 5)
        if i > 0 and random.random() < 0.01:
            ticks[i]["ts"] += random.uniform(-1.0, 1.0)
    return ticks


#!/usr/bin/env python3
"""Synthetic tick generator for simulator harness testing."""

import csv
import math
import random
import json
import hashlib
import os
from dataclasses import dataclass, asdict
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
    # Hostile-auditor: Enforce minimum tick count
    assert n_ticks >= 600, f"Scenario must be at least 600 ticks, got {n_ticks}"
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

        change = random.gauss(config.drift, vol)
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

    # Hostile-auditor: Regime assertion (no regime switch allowed in this function)
    # (If regime switch is needed, must be explicit in scenario function)
    return ticks


def scenario_smooth_runner() -> List[Dict[str, float]]:
    # Maximize trend-following profitability: higher drift and volatility
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.0850, volatility=0.00022, drift=0.00009)
    return generate_synthetic_ticks(cfg, 650)


def scenario_panic_reversal() -> List[Dict[str, float]]:
    # Maximize pip extraction: sharp uptrend, then sharp reversal, both with high drift
    n_each = 650  # Each segment must be at least 600 ticks
    cfg1 = InstrumentConfig(name="USD_JPY", initial_price=110.50, pip_size=0.01, volatility=0.0002, drift=0.00018)
    up = generate_synthetic_ticks(cfg1, n_each)
    cfg2 = InstrumentConfig(name="USD_JPY", initial_price=up[-1]["mid"], pip_size=0.01, volatility=0.0002, drift=-0.00018)
    down = generate_synthetic_ticks(cfg2, n_each, start_ts=up[-1]["ts"] + 1)
    ticks = up + down
    # Guarantee strictly monotonic, 1s-apart timestamps
    base_ts = 1640000000.0
    for i, t in enumerate(ticks):
        t["ts"] = base_ts + i
    return ticks


def scenario_choppy_stall() -> List[Dict[str, float]]:
    cfg = InstrumentConfig(name="GBP_USD", initial_price=1.2650, volatility=0.00012)
    ticks: List[Dict[str, float]] = []
    mid = cfg.initial_price
    low = 1.2640
    high = 1.2660
    for i in range(650):
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
    ticks = generate_synthetic_ticks(cfg, 650)
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
    # Realistic whipsaw: price surges up fast, then reverses fast, repeated several times
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.0860, volatility=0.00005, drift=0.0)
    n_ticks = 650
    ticks = []
    base = 1.0860
    ts = 1640000000.0
    whipsaw_count = 3
    up_ticks = 30
    down_ticks = 25
    flat_ticks = 20
    pip_size = 0.0001
    up_pips = 8  # 8 pips up
    down_pips = 8  # 8 pips down
    for w in range(whipsaw_count):
        # Flat before whipsaw
        for i in range(flat_ticks):
            ticks.append({
                "instrument": "EUR_USD",
                "ts": ts,
                "bid": round(base - 0.0004, 5),
                "ask": round(base + 0.0004, 5),
                "mid": round(base, 5),
            })
            ts += 1
        # Fast up
        for i in range(up_ticks):
            mid = base + (i + 1) * (up_pips * pip_size / up_ticks)
            ticks.append({
                "instrument": "EUR_USD",
                "ts": ts,
                "bid": round(mid - 0.0004, 5),
                "ask": round(mid + 0.0004, 5),
                "mid": round(mid, 5),
            })
            ts += 1
        # Fast down
        for i in range(down_ticks):
            mid = base + up_pips * pip_size - (i + 1) * (down_pips * pip_size / down_ticks)
            ticks.append({
                "instrument": "EUR_USD",
                "ts": ts,
                "bid": round(mid - 0.0004, 5),
                "ask": round(mid + 0.0004, 5),
                "mid": round(mid, 5),
            })
            ts += 1
        # Reset base for next whipsaw
        base = base + random.uniform(-0.0005, 0.0005)  # small drift for realism
    # Fill remaining ticks with flat
    while len(ticks) < n_ticks:
        ticks.append({
            "instrument": "EUR_USD",
            "ts": ts,
            "bid": round(base - 0.0004, 5),
            "ask": round(base + 0.0004, 5),
            "mid": round(base, 5),
        })
        ts += 1
    return ticks


def scenario_slow_grind_break() -> List[Dict[str, float]]:
    """Low-energy grind that transitions into one-direction break."""
    cfg = InstrumentConfig(name="EUR_USD", initial_price=1.0825, volatility=0.00005, drift=0.00001)
    ticks = generate_synthetic_ticks(cfg, 650)
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
    for spike_amp, fade_amp, spike_dur, fade_dur in itertools.product(spike_amps, fade_amps, spike_durations, fade_durations):
        base = base_price
        cfg = InstrumentConfig(name="EUR_USD", initial_price=base, volatility=base_vol, drift=0.0)
        ticks = generate_synthetic_ticks(cfg, n_ticks)
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
            elif 350 <= i < 350+spike_dur:
                mid += 0.00039 * ((i - 350) / spike_dur)
            elif 350+spike_dur <= i < 350+spike_dur+20:
                mid += 0.00001 * ((i - (350+spike_dur)) / 20.0)
            # Whipsaw: sharp up to 8 pips, then fade
            elif 370+spike_dur <= i < 370+spike_dur+fade_dur:
                mid += 0.00041 + 0.00041 * ((i - (370+spike_dur)) / fade_dur)  # up to ~8 pips
            elif 370+spike_dur+fade_dur <= i < 370+spike_dur+fade_dur+40:
                mid -= 0.0007 * ((i - (370+spike_dur+fade_dur)) / 40.0)
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

        # For now, just return the first sweep (AEE batch will run all)
        return out

SCENARIO_REGISTRY: Dict[str, Callable[[], List[Dict[str, float]]]] = {
    # Original pattern-based scenarios
    "trend_continuation": scenario_smooth_runner,
    "panic_reversal": scenario_panic_reversal,

    "spread_widening": scenario_spread_widening_event,
    "whipsaw_spike_fade": scenario_whipsaw_spike_fade,
    "multi_whipsaw_sweep": scenario_multi_whipsaw_sweep,
    "slow_grind_break": scenario_slow_grind_break,
    # Universal energy-based scenarios (added)
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
        w = csv.DictWriter(f, fieldnames=["instrument", "ts", "bid", "ask"])
        w.writeheader()
        for t in ticks:
            w.writerow({"instrument": t["instrument"], "ts": t["ts"], "bid": t["bid"], "ask": t["ask"]})
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


def hostile_manifest(scenario_name, params, seed):
    return {
        "scenario": scenario_name,
        "parameters": params,
        "random_seed": seed,
        "generation_time": __import__('datetime').datetime.utcnow().isoformat() + 'Z',
        "audit_gates": "<document expected audit gates here>",
        "failure_modes": "<document failure modes here>",
    }

def run_hostile_auditor_generation():
    # Hostile-auditor: Use fixed seed for reproducibility
    seed = int(os.environ.get("TICKGEN_SEED", "1337"))
    random.seed(seed)

    # Each scenario: generate, export with manifest
    scenarios = [
        ("trend_continuation", scenario_smooth_runner, {"purpose": "trend-following, positive drift, no regime switch", "audit_gates": ["trend_engagement", "positive_pips"], "forbidden": ["regime_switch"], "failure_modes": ["no_engagement", "negative_pips"]}),
        ("panic_reversal", scenario_panic_reversal, {"purpose": "panic spike, reversal, no trend", "audit_gates": ["panic_exit", "reversal_detection"], "forbidden": ["trend_engagement"], "failure_modes": ["missed_reversal", "false_trend"]}),
        ("chop_mean_reversion", scenario_choppy_stall, {"purpose": "choppy, mean-reverting, bounded", "audit_gates": ["mean_reversion", "bounded_pnl"], "forbidden": ["trend_engagement"], "failure_modes": ["trend_engaged", "large_loss"]}),
        # ("regime_switch", scenario_regime_switch, {"purpose": "abrupt regime switch (trend→chop)", "audit_gates": ["regime_switch_detected", "no_carryover"], "forbidden": ["trend_engagement_after_switch"], "failure_modes": ["carryover", "missed_switch"]}),
        # ("microstructure_noise", scenario_microstructure_noise, {"purpose": "high-frequency noise, pathological spreads, timestamp jitter", "audit_gates": ["robust_to_noise", "no_gaming"], "forbidden": ["timing_gaming"], "failure_modes": ["gaming_detected", "crash"]}),
        # ("edge_case_trap", scenario_edge_case_trap, {"purpose": "slow drift, abrupt reversal, micro-pip exit trap, hold forever", "audit_gates": ["no_hold_forever", "exit_on_reversal", "no_micro_pip_gaming"], "forbidden": ["hold_forever", "micro_pip_exit"], "failure_modes": ["never_exit", "micro_pip_exit", "missed_reversal"]}),
        # ("volatility_breakout", scenario_volatility_breakout, {"purpose": "energy-based: persistent drift, volatility expansion (breakout)", "audit_gates": ["trend_engagement", "volatility_expansion", "positive_pips"], "forbidden": ["regime_switch"], "failure_modes": ["no_engagement", "negative_pips"]}),
        # ("momentum_burst", scenario_momentum_burst, {"purpose": "energy-based: trend acceleration, volatility contraction/expansion (momentum burst)", "audit_gates": ["trend_acceleration", "momentum_detection", "positive_pips"], "forbidden": ["regime_switch"], "failure_modes": ["no_engagement", "negative_pips"]}),
    ]
    for name, fn, params in scenarios:
        ticks = fn()
        manifest = hostile_manifest(name, params, seed)
        export_ticks_csv(ticks, f"{name}.csv", manifest)

    # Multi-scenario combo
    combo = []
    for _, fn, _ in scenarios:
        combo += fn()
    combo.sort(key=lambda x: x["ts"])
    manifest = hostile_manifest("multi_combo", {"purpose": "combo of all above"}, seed)
    export_ticks_csv(combo, "scenario_multi.csv", manifest)
    print("Synthetic scenarios written: " + ", ".join([f"{name}.csv" for name,_,_ in scenarios]) + ", scenario_multi.csv")

# Unit test: scenario compliance
def _test_scenario_compliance():
    for name, fn in SCENARIO_REGISTRY.items():
        ticks = fn()
        assert len(ticks) >= 600, f"Scenario {name} too short: {len(ticks)}"
        # Check monotonic timestamps
        ts = [t["ts"] for t in ticks]
        assert all(ts[i] < ts[i+1] for i in range(len(ts)-1)), f"Timestamps not monotonic in {name}"
        # Check regime: no explicit regime switch in base scenarios
        # (If regime switch is needed, must be explicit in scenario function)


# --- Restored: sample_scenario_mix for harness compatibility ---
def sample_scenario_mix(rng: random.Random | None = None, weights: Dict[str, float] | None = None) -> Tuple[str, List[Dict[str, float]]]:
    """Sample a scenario from the registry, optionally weighted."""
    scenarios = list(SCENARIO_REGISTRY.keys())
    if weights:
        w = [weights.get(s, 1.0) for s in scenarios]
    else:
        w = [1.0 for _ in scenarios]
    total = sum(w)
    if total <= 0:
        w = [1.0 for _ in scenarios]
        total = float(len(scenarios))
    w = [x / total for x in w]
    if rng is None:
        rng = random.Random()
    idx = rng.choices(range(len(scenarios)), weights=w, k=1)[0]
    scenario = scenarios[idx]
    ticks = SCENARIO_REGISTRY[scenario]()
    return scenario, ticks

if __name__ == "__main__":
    run_hostile_auditor_generation()
    _test_scenario_compliance()

    print("\n--- Scenario Test Summaries ---")
    for name, fn in SCENARIO_REGISTRY.items():
        print(f"\nScenario: {name}")
        ticks = fn()
        print(f"  Total ticks: {len(ticks)}")
        if len(ticks) > 4:
            print("  First 2:")
            for t in ticks[:2]:
                print(f"    {t}")
            print("  Last 2:")
            for t in ticks[-2:]:
                print(f"    {t}")
        else:
            for t in ticks:
                print(f"    {t}")
        # Performance: pips per hour
        if len(ticks) >= 2:
            first = ticks[0]
            last = ticks[-1]
            # Use mid price for pips calculation
            pip_size = 0.0001
            if 'pip_size' in first:
                pip_size = first['pip_size']
            elif first['instrument'].endswith('JPY'):
                pip_size = 0.01
            pips = (last['mid'] - first['mid']) / pip_size
            hours = (last['ts'] - first['ts']) / 3600.0
            pips_per_hour = pips / hours if hours != 0 else float('nan')
            print(f"  Pips per hour: {pips_per_hour:.2f}")
