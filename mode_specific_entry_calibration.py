#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List

import entry_mode_distance_sweep as emds


ROOT = Path(".")
DISTANCES = emds.DISTANCES


def load_feature_comparison() -> Dict[str, Any]:
    with (ROOT / "entry_feature_comparison.json").open() as f:
        return json.load(f)


def cluster_meta(clusters: List[Dict[str, Any]], distance: float, direction: str) -> Dict[str, str]:
    return {
        c["cluster_id"]: c["teacher_label"]
        for c in clusters
        if c["distance"] == distance and c["direction"] == direction
    }


def simulate_runner_static(row: Dict[str, Any], distance: float) -> Dict[str, float]:
    return emds.simulate_runner_trade(row, distance)


def score_mode(rows: List[Dict[str, Any]], clusters: List[Dict[str, Any]], direction: str, mode: str, cfg: Dict[str, float]) -> Dict[str, Any]:
    per_distance = []
    merged_rows = []
    for distance in DISTANCES:
        rel_rows = [r for r in rows if r["distance"] == distance and r["direction"] == direction]
        if not rel_rows:
            continue
        rel_clusters = [c for c in clusters if c["distance"] == distance and c["direction"] == direction]
        cluster_labels = cluster_meta(clusters, distance, direction)
        chosen = []
        last_by_cluster = {}
        for row in sorted(rel_rows, key=lambda r: r["dt"]):
            if row["pre_speed"] < cfg["pre_speed_min"]:
                continue
            if row["pre_efficiency"] < cfg["pre_efficiency_min"]:
                continue
            if row["pre_volatility"] < cfg["pre_volatility_min"]:
                continue
            if row["pre_range_pips"] < cfg.get("pre_range_min", 0.0):
                continue
            if row["cluster_progress"] < cfg["cluster_progress_min"] or row["cluster_progress"] > cfg["cluster_progress_max"]:
                continue
            if mode == "runner" and distance < emds.RUNNER_MIN_DISTANCE:
                continue
            cid = row["cluster_id"]
            if mode == "harvester":
                prev = last_by_cluster.get(cid)
                if prev is not None and row["dt"] < prev + emds.timedelta(minutes=3):
                    continue
                last_by_cluster[cid] = row["dt"]
                sim = emds.simulate_harvester_trade(row, distance)
            else:
                if cid in last_by_cluster:
                    continue
                last_by_cluster[cid] = row["dt"]
                sim = simulate_runner_static(row, distance)
            chosen_row = {**row, **sim}
            chosen.append(chosen_row)
            merged_rows.append(chosen_row)
        wins = sum(1 for r in chosen if r["pips"] > 0)
        losses = sum(1 for r in chosen if r["pips"] < 0)
        chosen_cluster_ids = {r["cluster_id"] for r in chosen}
        taken_labels = Counter(cluster_labels[cid] for cid in chosen_cluster_ids if cid in cluster_labels)
        cluster_counts = Counter(cluster_labels.values())
        per_distance.append(
            {
                "distance": distance,
                "trade_count": len(chosen),
                "wins": wins,
                "losses": losses,
                "capture_rate": taken_labels.get("GOOD", 0) / max(1, cluster_counts.get("GOOD", 0)),
                "bad_trigger": taken_labels.get("BAD", 0) / max(1, cluster_counts.get("BAD", 0)) if cluster_counts.get("BAD", 0) else 0.0,
                "noise_trigger": taken_labels.get("NOISE", 0) / max(1, cluster_counts.get("NOISE", 0)) if cluster_counts.get("NOISE", 0) else 0.0,
                "total_pips": sum(r["pips"] for r in chosen),
                "pips_per_hour": sum(r["pips"] for r in chosen) / 9.0 if chosen else 0.0,
                "avg_pips": mean(r["pips"] for r in chosen) if chosen else 0.0,
            }
        )
    total_pips = sum(x["total_pips"] for x in per_distance)
    total_trades = sum(x["trade_count"] for x in per_distance)
    total_wins = sum(x["wins"] for x in per_distance)
    total_losses = sum(x["losses"] for x in per_distance)
    return {
        "mode": mode,
        "direction": direction,
        "config": cfg,
        "trade_count": total_trades,
        "wins": total_wins,
        "losses": total_losses,
        "win_rate": total_wins / total_trades if total_trades else 0.0,
        "total_pips": total_pips,
        "pips_per_hour": total_pips / 9.0 if total_trades else 0.0,
        "avg_pips": total_pips / total_trades if total_trades else 0.0,
        "estimated_equity_per_hour_at_2pct_risk": sum((x["total_pips"] / max(x["distance"], 1e-9) * 0.02) / 9.0 for x in per_distance),
        "distance_results": per_distance,
        "rows": merged_rows,
    }


def candidate_configs(side_stats: Dict[str, Any], mode: str) -> List[Dict[str, float]]:
    feats = side_stats["features"]
    if mode == "harvester":
        speed_vals = [feats["pre_speed"]["harvester_q25"], feats["pre_speed"]["harvester_median"]]
        eff_vals = [feats["pre_efficiency"]["harvester_q25"], feats["pre_efficiency"]["harvester_median"]]
        vol_vals = [0.0, feats["pre_volatility"]["harvester_q25"]]
        range_vals = [0.0]
        prog_ranges = [(0.0, 1.0), (0.0, feats["cluster_progress"]["harvester_q75"])]
    else:
        speed_vals = [feats["pre_speed"]["runner_median"], feats["pre_speed"]["runner_q75"], feats["pre_speed"]["runner_q75"] * 1.1]
        eff_vals = [feats["pre_efficiency"]["runner_median"], feats["pre_efficiency"]["runner_q75"]]
        vol_vals = [feats["pre_volatility"]["runner_q25"], feats["pre_volatility"]["runner_median"], feats["pre_volatility"]["runner_q75"]]
        range_vals = [feats["pre_range_pips"]["runner_q25"], feats["pre_range_pips"]["runner_median"], feats["pre_range_pips"]["runner_q75"]]
        prog_ranges = [
            (0.0, feats["cluster_progress"]["runner_q75"]),
            (0.0, feats["cluster_progress"]["runner_median"]),
            (feats["cluster_progress"]["runner_q25"], feats["cluster_progress"]["runner_q75"]),
        ]
    out = []
    for s in speed_vals:
        for e in eff_vals:
            for v in vol_vals:
                for r in range_vals:
                    for lo, hi in prog_ranges:
                        out.append(
                            {
                                "pre_speed_min": s,
                                "pre_efficiency_min": e,
                                "pre_volatility_min": v,
                                "pre_range_min": r,
                                "cluster_progress_min": lo,
                                "cluster_progress_max": hi,
                            }
                        )
    return out


def pick_best(rows: List[Dict[str, Any]], clusters: List[Dict[str, Any]], side_stats: Dict[str, Any], direction: str, mode: str) -> Dict[str, Any]:
    best = None
    for cfg in candidate_configs(side_stats, mode):
        result = score_mode(rows, clusters, direction, mode, cfg)
        score = (
            result["total_pips"],
            result["wins"] - result["losses"],
            result["win_rate"],
            -sum(d["bad_trigger"] for d in result["distance_results"]),
            -sum(d["noise_trigger"] for d in result["distance_results"]),
        )
        if best is None or score > best[0]:
            best = (score, result)
    return best[1]


def main() -> None:
    prices = emds.load_prices()
    feature_stats = load_feature_comparison()
    discovered = []
    clusters = []
    state_rows = []
    for distance in DISTANCES:
        dist_rows = emds.discover_for_distance(prices, distance)
        dist_clusters = emds.cluster_rows(dist_rows)
        discovered.extend(dist_rows)
        clusters.extend(dist_clusters)
        for direction in ("LONG", "SHORT"):
            state_rows.extend(emds.build_cluster_state_rows(prices, dist_clusters, distance, direction))

    outputs = {}
    for direction, side_key in (("LONG", "long"), ("SHORT", "short")):
        side_stats = feature_stats[side_key]
        for mode in ("harvester", "runner"):
            result = pick_best(state_rows, clusters, side_stats, direction, mode)
            name = f"{side_key}_{mode}_mode_config.json"
            with (ROOT / name).open("w") as f:
                json.dump(result, f, indent=2, default=str)
            outputs[f"{side_key}_{mode}"] = result

    with (ROOT / "mode_specific_entry_summary.json").open("w") as f:
        json.dump(outputs, f, indent=2, default=str)


if __name__ == "__main__":
    main()
