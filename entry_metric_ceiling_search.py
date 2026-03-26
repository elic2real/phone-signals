#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Tuple

import entry_mode_distance_sweep as emds


ROOT = Path(".")
DISTANCES = emds.DISTANCES


def cluster_labels_for(clusters: List[Dict[str, Any]], distance: float, direction: str) -> Dict[str, str]:
    return {
        c["cluster_id"]: c["teacher_label"]
        for c in clusters
        if c["distance"] == distance and c["direction"] == direction
    }


def simulate(mode: str, row: Dict[str, Any], distance: float) -> Dict[str, float]:
    if mode == "harvester":
        return emds.simulate_harvester_trade(row, distance)
    partial_tp = row.get("_partial_tp")
    partial_fraction = row.get("_partial_fraction", emds.RUNNER_PARTIAL_FRACTION)
    return emds.simulate_runner_trade(row, distance, partial_tp=partial_tp, partial_fraction=partial_fraction)


def choose_rows(
    rows: List[Dict[str, Any]],
    mode: str,
    distance: float,
    config: Dict[str, float],
) -> List[Dict[str, Any]]:
    chosen = []
    last_by_cluster = {}
    for row in sorted(rows, key=lambda r: r["dt"]):
        if row["pre_speed"] < config["pre_speed_min"]:
            continue
        if row["pre_efficiency"] < config["pre_efficiency_min"]:
            continue
        if row["pre_volatility"] < config["pre_volatility_min"]:
            continue
        if row["pre_range_pips"] < config["pre_range_min"]:
            continue
        if row["cluster_progress"] < config["cluster_progress_min"] or row["cluster_progress"] > config["cluster_progress_max"]:
            continue
        cid = row["cluster_id"]
        if mode == "harvester":
            prev = last_by_cluster.get(cid)
            if prev is not None and row["dt"] < prev + emds.timedelta(minutes=3):
                continue
            last_by_cluster[cid] = row["dt"]
        else:
            if distance < emds.RUNNER_MIN_DISTANCE or cid in last_by_cluster:
                continue
            last_by_cluster[cid] = row["dt"]
        chosen_row = dict(row)
        if mode == "runner":
            chosen_row["_partial_tp"] = config["partial_tp"]
            chosen_row["_partial_fraction"] = config["partial_fraction"]
        chosen.append(chosen_row)
    return chosen


def summarize_run(
    chosen_rows: List[Dict[str, Any]],
    cluster_labels: Dict[str, str],
    mode: str,
    direction: str,
    distance: float,
    config: Dict[str, float],
) -> Dict[str, Any]:
    sim_rows = [{**row, **simulate(mode, row, distance)} for row in chosen_rows]
    wins = sum(1 for r in sim_rows if r["pips"] > 0)
    losses = sum(1 for r in sim_rows if r["pips"] < 0)
    breakeven = len(sim_rows) - wins - losses
    chosen_cluster_ids = {r["cluster_id"] for r in sim_rows}
    taken_labels = Counter(cluster_labels[cid] for cid in chosen_cluster_ids if cid in cluster_labels)
    total_labels = Counter(cluster_labels.values())
    total_pips = sum(r["pips"] for r in sim_rows)
    avg_pips = mean(r["pips"] for r in sim_rows) if sim_rows else 0.0
    avg_r = mean(r["r"] for r in sim_rows) if sim_rows else 0.0
    return {
        "mode": mode,
        "direction": direction,
        "distance": distance,
        "config": config,
        "trade_count": len(sim_rows),
        "unique_clusters_traded": len(chosen_cluster_ids),
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "win_rate": wins / len(sim_rows) if sim_rows else 0.0,
        "good_clusters": total_labels.get("GOOD", 0),
        "bad_clusters": total_labels.get("BAD", 0),
        "noise_clusters": total_labels.get("NOISE", 0),
        "traded_good_clusters": taken_labels.get("GOOD", 0),
        "traded_bad_clusters": taken_labels.get("BAD", 0),
        "traded_noise_clusters": taken_labels.get("NOISE", 0),
        "capture_rate": taken_labels.get("GOOD", 0) / max(1, total_labels.get("GOOD", 0)),
        "bad_trigger": taken_labels.get("BAD", 0) / max(1, total_labels.get("BAD", 0)) if total_labels.get("BAD", 0) else 0.0,
        "noise_trigger": taken_labels.get("NOISE", 0) / max(1, total_labels.get("NOISE", 0)) if total_labels.get("NOISE", 0) else 0.0,
        "total_pips": total_pips,
        "avg_pips": avg_pips,
        "pips_per_hour": total_pips / 9.0 if sim_rows else 0.0,
        "avg_R": avg_r,
        "estimated_equity_per_hour_at_2pct_risk": ((total_pips / max(distance, 1e-9)) * 0.02) / 9.0 if sim_rows else 0.0,
        "partial_bank_avg_pips": mean(r.get("partial_bank_pips", 0.0) for r in sim_rows) if sim_rows else 0.0,
        "runner_avg_pips": mean(r.get("runner_pips", 0.0) for r in sim_rows) if sim_rows else 0.0,
        "rows": sim_rows,
    }


def candidate_configs(distance: float, mode: str) -> List[Dict[str, float]]:
    speed_vals = [0.0, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2]
    eff_vals = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5]
    vol_vals = [0.0, 0.4, 0.6, 0.8, 1.0]
    range_vals = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0]
    prog_ranges = [(0.0, 1.0), (0.0, 0.75), (0.0, 0.6), (0.15, 0.75), (0.2, 0.67)]
    if mode == "harvester":
        return [
            {
                "pre_speed_min": s,
                "pre_efficiency_min": e,
                "pre_volatility_min": v,
                "pre_range_min": r,
                "cluster_progress_min": lo,
                "cluster_progress_max": hi,
            }
            for s in speed_vals
            for e in eff_vals
            for v in vol_vals[:4]
            for r in range_vals[:4]
            for lo, hi in prog_ranges[:3]
        ]
    return [
        {
            "pre_speed_min": s,
            "pre_efficiency_min": e,
            "pre_volatility_min": v,
            "pre_range_min": r,
            "cluster_progress_min": lo,
            "cluster_progress_max": hi,
            "partial_tp": pt,
            "partial_fraction": pf,
        }
        for s in speed_vals[2:]
        for e in eff_vals[1:]
        for v in vol_vals[1:]
        for r in range_vals[1:]
        for lo, hi in prog_ranges
        for pt in [x for x in [1.0, 1.25, 1.5, 1.75, 2.0, 2.5] if x <= distance]
        for pf in [0.7, 0.8, 0.9]
        if distance >= emds.RUNNER_MIN_DISTANCE
    ]


def best_by_metric(rows: List[Dict[str, Any]], cluster_labels: Dict[str, str], mode: str, direction: str, distance: float) -> Dict[str, Any]:
    candidates = candidate_configs(distance, mode)
    metrics_best: Dict[str, Tuple[Tuple[float, ...], Dict[str, Any]]] = {}
    metric_score_defs = {
        "profit_ceiling": lambda s: (s["total_pips"], s["win_rate"], -s["losses"]),
        "win_rate_ceiling": lambda s: (s["win_rate"], s["total_pips"], -s["losses"]),
        "pips_per_hour_ceiling": lambda s: (s["pips_per_hour"], s["win_rate"], -s["losses"]),
        "equity_per_hour_ceiling": lambda s: (s["estimated_equity_per_hour_at_2pct_risk"], s["win_rate"], -s["losses"]),
        "capture_ceiling": lambda s: (s["capture_rate"], s["total_pips"], s["win_rate"]),
    }
    for cfg in candidates:
        chosen = choose_rows(rows, mode, distance, cfg)
        summary = summarize_run(chosen, cluster_labels, mode, direction, distance, cfg)
        if summary["trade_count"] == 0:
            continue
        for name, scorer in metric_score_defs.items():
            score = scorer(summary)
            if name not in metrics_best or score > metrics_best[name][0]:
                metrics_best[name] = (score, summary)
    return {name: payload[1] for name, payload in metrics_best.items()}


def main() -> None:
    prices = emds.load_prices()
    all_clusters = []
    all_state_rows = []
    for distance in DISTANCES:
        dist_discovered = emds.discover_for_distance(prices, distance)
        dist_clusters = emds.cluster_rows(dist_discovered)
        all_clusters.extend(dist_clusters)
        for direction in ("LONG", "SHORT"):
            all_state_rows.extend(emds.build_cluster_state_rows(prices, dist_clusters, distance, direction))

    report: Dict[str, Any] = {"results": {}}
    for direction in ("LONG", "SHORT"):
        dir_key = direction.lower()
        report["results"][dir_key] = {}
        for mode in ("harvester", "runner"):
            report["results"][dir_key][mode] = {}
            for distance in DISTANCES:
                distance_key = f"{distance:g}"
                rows = [r for r in all_state_rows if r["direction"] == direction and r["distance"] == distance]
                labels = cluster_labels_for(all_clusters, distance, direction)
                best = best_by_metric(rows, labels, mode, direction, distance)
                report["results"][dir_key][mode][distance_key] = best
                for metric_name, summary in best.items():
                    out = ROOT / f"ceiling_{dir_key}_{mode}_{distance_key}_{metric_name}.json"
                    out.write_text(json.dumps(summary, indent=2, default=str))

    (ROOT / "entry_metric_ceiling_report.json").write_text(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
