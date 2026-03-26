#!/usr/bin/env python3
"""Stats-driven synthetic session generator (V1).

Consumes stats/session_<SESSION>.json produced by tools/extract_market_stats.py.
"""

from __future__ import annotations

import argparse
import csv
import json
import random
from pathlib import Path
from typing import Dict, List, Tuple


def _parse_bucket(b: str) -> Tuple[float, float]:
    if "+" in b:
        v = float(b.replace("+", ""))
        return v, v * 1.6
    lo, hi = b.split("-")
    return float(lo), float(hi)


def _weighted_pick(d: Dict[str, float], rng: random.Random, default: str) -> str:
    if not d:
        return default
    items = list(d.items())
    total = sum(max(0.0, float(v)) for _, v in items)
    if total <= 0:
        return default
    u = rng.random() * total
    c = 0.0
    for k, v in items:
        c += max(0.0, float(v))
        if u <= c:
            return k
    return items[-1][0]


def _session_phase_bucket(phase: float) -> str:
    if phase < 0.1:
        return "0.00-0.10"
    if phase < 0.3:
        return "0.10-0.30"
    if phase < 0.7:
        return "0.30-0.70"
    return "0.70-1.01"


def generate_session(
    stats: dict,
    *,
    pair: str = "EUR_USD",
    start_price: float = 1.1000,
    n_ticks: int = 3000,
    seed: int = 123,
) -> List[dict]:
    rng = random.Random(seed)
    pip = 0.01 if "JPY" in pair else 0.0001
    base_atr_price = 10.0 * pip
    spread_base = float((stats.get("spread_profile") or {}).get("base_median_pips", 1.2) or 1.2)

    impulse_bins = stats.get("impulse_atr_bins") or {}
    pullback_bins = stats.get("pullback_ratio_bins") or {}
    outcome_probs = stats.get("post_pullback_outcome") or {"continue": 0.5, "stall": 0.3, "reverse": 0.2}
    vol_profile = stats.get("session_vol_profile") or {}
    spread_profile = stats.get("session_spread_profile") or {}

    rows: List[dict] = []
    mid = float(start_price)
    floor_price = 50.0 if "JPY" in pair else (0.2 if "USD" in pair else 0.05)
    ts0 = 1640000000.0
    dir_sign = 1
    state = "range"
    state_ttl = 0
    pending_impulse_size = 0.0

    for i in range(n_ticks):
        phase = i / max(1, n_ticks - 1)
        pb = _session_phase_bucket(phase)
        vol_mult = float(vol_profile.get(pb, 1.0) or 1.0)
        atr_now = base_atr_price * vol_mult

        if state_ttl <= 0:
            if rng.random() < 0.35:
                state = "impulse"
                imp_bucket = _weighted_pick(impulse_bins, rng, "0.30-0.60")
                lo, hi = _parse_bucket(imp_bucket)
                pending_impulse_size = rng.uniform(lo, hi) * atr_now
                dir_sign = 1 if rng.random() < 0.5 else -1
                state_ttl = rng.randint(8, 30)
            elif rng.random() < 0.5:
                state = "range"
                state_ttl = rng.randint(10, 40)
            else:
                state = "stall"
                state_ttl = rng.randint(6, 24)

        if state == "impulse":
            step = dir_sign * (pending_impulse_size / max(1, state_ttl))
            noise = rng.gauss(0.0, 0.08 * atr_now)
            mid += step + noise
            state_ttl -= 1
            if state_ttl <= 0:
                pb_bucket = _weighted_pick(pullback_bins, rng, "0.20-0.40")
                lo, hi = _parse_bucket(pb_bucket)
                pb_ratio = max(0.0, min(1.5, rng.uniform(lo, hi)))
                pb_size = pb_ratio * max(1e-12, abs(pending_impulse_size))
                outcome = _weighted_pick(outcome_probs, rng, "stall")
                if outcome == "continue":
                    state = "impulse"
                    pending_impulse_size = (0.6 + rng.random() * 0.8) * max(atr_now, pb_size)
                    state_ttl = rng.randint(6, 24)
                elif outcome == "reverse":
                    dir_sign *= -1
                    state = "impulse"
                    pending_impulse_size = max(atr_now, pb_size)
                    state_ttl = rng.randint(8, 26)
                else:
                    state = "stall"
                    state_ttl = rng.randint(8, 30)
        elif state == "range":
            mid += rng.gauss(0.0, 0.12 * atr_now)
            state_ttl -= 1
        else:
            mid += rng.gauss(0.0, 0.04 * atr_now)
            state_ttl -= 1

        mid = max(floor_price, mid)
        spread_pips = float(spread_profile.get(pb, spread_base) or spread_base)
        spread = spread_pips * pip
        rows.append(
            {
                "instrument": pair,
                "ts": ts0 + i,
                "bid": round(mid - spread / 2.0, 5),
                "ask": round(mid + spread / 2.0, 5),
                "mid": round(mid, 5),
                "spread_pips": round(spread_pips, 4),
            }
        )
    return rows


def write_csv(rows: List[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["instrument", "ts", "bid", "ask", "mid", "spread_pips"])
        w.writeheader()
        w.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Generate synthetic session from extracted market stats JSON.")
    ap.add_argument("--stats", default="stats/session_LONDON.json", help="Path to stats/session_<SESSION>.json")
    ap.add_argument("--pair", default="EUR_USD")
    ap.add_argument("--start-price", type=float, default=1.1000)
    ap.add_argument("--ticks", type=int, default=3000)
    ap.add_argument("--seed", type=int, default=123)
    ap.add_argument("--out", default="synthetic/session_generated.csv")
    args = ap.parse_args()

    stats = json.loads(Path(args.stats).read_text(encoding="utf-8"))
    rows = generate_session(
        stats,
        pair=args.pair,
        start_price=args.start_price,
        n_ticks=max(100, int(args.ticks)),
        seed=int(args.seed),
    )
    out = Path(args.out)
    write_csv(rows, out)
    print(str(out))


if __name__ == "__main__":
    main()
