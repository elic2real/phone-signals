from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean
from typing import Dict, List, Tuple

from state_key import compute_quarter


ROOT = Path(__file__).resolve().parent


@dataclass
class Candidate:
    cluster_id: str
    direction: str
    distance: float
    timestamp: str
    quarter: str
    pre_range_pips: float
    pre_trend_pips: float
    pre_volatility: float
    pre_speed: float
    pre_efficiency: float
    cluster_progress: float
    harvester_profit: float
    runner_extension: float
    future_mfe: float
    future_mae: float
    stop_hit: bool


def percentile(vals: List[float], q: float) -> float:
    if not vals:
        return 0.0
    vals = sorted(vals)
    if len(vals) == 1:
        return vals[0]
    idx = q * (len(vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(vals) - 1)
    frac = idx - lo
    return vals[lo] * (1 - frac) + vals[hi] * frac


def load_candidates() -> List[Candidate]:
    pre = {}
    with (ROOT / "entry_pretrade_states.csv").open() as f:
        for row in csv.DictReader(f):
            key = (row["cluster_id"], row["timestamp"])
            pre[key] = row
    out: List[Candidate] = []
    with (ROOT / "entry_outcomes.csv").open() as f:
        for row in csv.DictReader(f):
            key = (row["cluster_id"], row["timestamp"])
            p = pre.get(key)
            if not p:
                continue
            quarter = compute_quarter(row["timestamp"], "LONDON")
            out.append(
                Candidate(
                    cluster_id=row["cluster_id"],
                    direction=row["direction"],
                    distance=float(row["distance"]),
                    timestamp=row["timestamp"],
                    quarter=quarter,
                    pre_range_pips=float(p["pre_range_pips"]),
                    pre_trend_pips=float(p["pre_trend_pips"]),
                    pre_volatility=float(p["pre_volatility"]),
                    pre_speed=float(p["pre_speed"]),
                    pre_efficiency=float(p["pre_efficiency"]),
                    cluster_progress=float(p["cluster_progress"]),
                    harvester_profit=float(row["harvester_profit"]),
                    runner_extension=float(row["runner_extension"]),
                    future_mfe=float(row["future_mfe"]),
                    future_mae=float(row["future_mae"]),
                    stop_hit=str(row["stop_hit"]).lower() == "true",
                )
            )
    return out


def score_subset(cands: List[Candidate]) -> Dict[str, float]:
    total = sum(c.harvester_profit for c in cands)
    wins = sum(1 for c in cands if c.harvester_profit > 0)
    losses = sum(1 for c in cands if c.harvester_profit < 0)
    return {
        "trade_count": len(cands),
        "wins": wins,
        "losses": losses,
        "win_rate": wins / len(cands) if cands else 0.0,
        "avg_pips": mean(c.harvester_profit for c in cands) if cands else 0.0,
        "total_pips": total,
        "pips_per_hour": total / 2.0 if cands else 0.0,
        "mean_future_mfe": mean(c.future_mfe for c in cands) if cands else 0.0,
        "mean_runner_extension": mean(c.runner_extension for c in cands) if cands else 0.0,
    }


def build_subset(cands: List[Candidate], cfg: Dict[str, float]) -> List[Candidate]:
    chosen = [
        c
        for c in cands
        if c.pre_speed >= cfg["pre_speed_min"]
        and c.pre_efficiency >= cfg["pre_efficiency_min"]
        and c.pre_volatility >= cfg["pre_volatility_min"]
        and c.pre_range_pips >= cfg["pre_range_min"]
        and cfg["cluster_progress_min"] <= c.cluster_progress <= cfg["cluster_progress_max"]
    ]
    return chosen


def search_node(cands: List[Candidate]) -> Dict[str, object]:
    speeds = [c.pre_speed for c in cands]
    effs = [c.pre_efficiency for c in cands]
    vols = [c.pre_volatility for c in cands]
    ranges = [c.pre_range_pips for c in cands]
    progresses = [c.cluster_progress for c in cands]
    best = None
    best_cfg = None
    for sq in [0.1, 0.2, 0.3, 0.4]:
        for eq in [0.1, 0.2, 0.3, 0.4]:
            for vq in [0.0, 0.1, 0.2, 0.3]:
                for rq in [0.0, 0.1, 0.2, 0.3]:
                    for pgmax in [0.4, 0.5, 0.6, 0.7, 0.8, 1.0]:
                        cfg = {
                            "pre_speed_min": percentile(speeds, sq),
                            "pre_efficiency_min": percentile(effs, eq),
                            "pre_volatility_min": percentile(vols, vq),
                            "pre_range_min": percentile(ranges, rq),
                            "cluster_progress_min": 0.0,
                            "cluster_progress_max": percentile(progresses, pgmax),
                        }
                        subset = build_subset(cands, cfg)
                        if len(subset) < 10:
                            continue
                        metrics = score_subset(subset)
                        score = (metrics["pips_per_hour"], metrics["win_rate"], metrics["avg_pips"])
                        if best is None or score > (best["pips_per_hour"], best["win_rate"], best["avg_pips"]):
                            best = metrics
                            best_cfg = cfg
    return {"config": best_cfg, "metrics": best}


def main() -> None:
    candidates = load_candidates()
    targets = [("LONG", "Q1"), ("LONG", "Q3"), ("SHORT", "Q2"), ("SHORT", "Q4")]
    by_node: Dict[Tuple[str, str], List[Candidate]] = defaultdict(list)
    for c in candidates:
        by_node[(c.direction, c.quarter)].append(c)

    report = {}
    for direction, quarter in targets:
        node = by_node[(direction, quarter)]
        report[f"{direction}_{quarter}"] = {
            "raw_population": score_subset(node),
            "search": search_node(node),
            "distance_counts": dict(sorted(Counter(c.distance for c in node).items())),
        }

    (ROOT / "quarter_native_entry_search.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
