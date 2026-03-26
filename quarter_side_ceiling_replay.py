from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

from entry_mode_distance_sweep import (
    DISTANCES,
    build_cluster_state_rows,
    cluster_rows,
    load_prices,
    simulate_harvester_trade,
)
from quarter_ceiling_tuner import BASE, PRESETS, summarize as summarize_aee
from quarter_native_entry_search import percentile
from state_key import compute_quarter
from aee_state_machine import load_thresholds


ROOT = Path(__file__).resolve().parent


def build_state_rows() -> List[Dict[str, Any]]:
    prices = load_prices()
    all_rows: List[Dict[str, Any]] = []
    for distance in DISTANCES:
        discovered = [
            row
            for row in __import__("entry_mode_distance_sweep").discover_for_distance(prices, distance)
        ]
        distance_clusters = cluster_rows(discovered)
        for direction in ("LONG", "SHORT"):
            all_rows.extend(build_cluster_state_rows(prices, distance_clusters, distance, direction))
    return all_rows


def build_subset(rows: List[Dict[str, Any]], cfg: Dict[str, float]) -> List[Dict[str, Any]]:
    return [
        r
        for r in rows
        if r["pre_speed"] >= cfg["pre_speed_min"]
        and r["pre_efficiency"] >= cfg["pre_efficiency_min"]
        and r["pre_volatility"] >= cfg["pre_volatility_min"]
        and r["pre_range_pips"] >= cfg["pre_range_min"]
        and cfg["cluster_progress_min"] <= r["cluster_progress"] <= cfg["cluster_progress_max"]
    ]


def score_rows(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    total = sum(float(r["harvester_profit"]) for r in rows)
    wins = sum(1 for r in rows if float(r["harvester_profit"]) > 0)
    losses = sum(1 for r in rows if float(r["harvester_profit"]) < 0)
    return {
        "trade_count": len(rows),
        "wins": wins,
        "losses": losses,
        "win_rate": wins / len(rows) if rows else 0.0,
        "avg_pips": total / len(rows) if rows else 0.0,
        "total_pips": total,
        "pips_per_hour": total / 2.0 if rows else 0.0,
        "mean_future_mfe": sum(float(r["future_mfe"]) for r in rows) / len(rows) if rows else 0.0,
        "mean_runner_extension": sum(float(r["runner_extension"]) for r in rows) / len(rows) if rows else 0.0,
    }


def search_entry(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"config": None, "metrics": score_rows([]), "rows": []}
    speeds = [r["pre_speed"] for r in rows]
    effs = [r["pre_efficiency"] for r in rows]
    vols = [r["pre_volatility"] for r in rows]
    ranges = [r["pre_range_pips"] for r in rows]
    progresses = [r["cluster_progress"] for r in rows]
    best_cfg = None
    best_metrics = None
    best_rows: List[Dict[str, Any]] = []
    for sq in [0.1, 0.2, 0.3, 0.4]:
        for eq in [0.1, 0.2, 0.3, 0.4]:
            for vq in [0.0, 0.1, 0.2, 0.3]:
                for rq in [0.0, 0.1, 0.2, 0.3]:
                    for pgmax in [0.3333333333, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0]:
                        cfg = {
                            "pre_speed_min": percentile(speeds, sq),
                            "pre_efficiency_min": percentile(effs, eq),
                            "pre_volatility_min": percentile(vols, vq),
                            "pre_range_min": percentile(ranges, rq),
                            "cluster_progress_min": 0.0,
                            "cluster_progress_max": percentile(progresses, pgmax),
                        }
                        subset = build_subset(rows, cfg)
                        if len(subset) < 10:
                            continue
                        metrics = score_rows(subset)
                        score = (metrics["pips_per_hour"], metrics["win_rate"], metrics["avg_pips"])
                        if best_metrics is None or score > (
                            best_metrics["pips_per_hour"],
                            best_metrics["win_rate"],
                            best_metrics["avg_pips"],
                        ):
                            best_cfg = cfg
                            best_metrics = metrics
                            best_rows = subset
    return {"config": best_cfg, "metrics": best_metrics or score_rows([]), "rows": best_rows}


def tune_aee(rows: List[Dict[str, Any]], label: str, thresholds: Dict[str, Any]) -> Dict[str, Any]:
    prepared = []
    for row in rows:
        clean = dict(row)
        clean["entry_mode"] = "harvester"
        sim = simulate_harvester_trade(clean, float(clean["distance"]))
        clean["pips"] = sim["pips"]
        clean["reason"] = sim["reason"]
        prepared.append(clean)
    best_name = None
    best_report = None
    for preset_name, overrides in PRESETS.items():
        params = dict(BASE)
        params.update(overrides)
        report = summarize_aee(prepared, label, thresholds, params)
        score = (report["delta_pips_per_hour"], report["delta_avg_R"], report["aee_pips_per_hour"])
        if best_report is None or score > (
            best_report["delta_pips_per_hour"],
            best_report["delta_avg_R"],
            best_report["aee_pips_per_hour"],
        ):
            best_name = preset_name
            best_report = report
    return {"preset": best_name, "report": best_report}


def serializable_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []
    for row in rows:
        clean = dict(row)
        if "dt" in clean:
            clean["dt"] = clean["dt"].isoformat()
        out.append(clean)
    return out


def main() -> None:
    thresholds = load_thresholds()
    state_rows = build_state_rows()
    by_node: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in state_rows:
        quarter = compute_quarter(row["timestamp_start"], "LONDON")
        by_node[(row["direction"], quarter)].append(row)

    report: Dict[str, Any] = {"nodes": {}, "combined_by_quarter": {}}
    quarter_combo: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for direction in ("LONG", "SHORT"):
        for quarter in ("Q1", "Q2", "Q3", "Q4"):
            label = f"{direction}_{quarter}"
            rows = by_node[(direction, quarter)]
            raw_metrics = score_rows(rows)
            searched = search_entry(rows)
            selected_rows = searched["rows"]
            aee = tune_aee(selected_rows, label, thresholds) if selected_rows else {"preset": None, "report": None}
            report["nodes"][label] = {
                "raw_population": raw_metrics,
                "entry_config": searched["config"],
                "entry_metrics": searched["metrics"],
                "selected_trade_rows": len(selected_rows),
                "best_aee_preset": aee["preset"],
                "aee_report": aee["report"],
            }
            if selected_rows:
                quarter_combo[quarter].extend(selected_rows)
                node_file = ROOT / f"quarter_side_{label.lower()}.json"
                node_file.write_text(
                    json.dumps(
                        {
                            "label": label,
                            "raw_population": raw_metrics,
                            "entry_config": searched["config"],
                            "entry_metrics": searched["metrics"],
                            "selected_rows": serializable_rows(selected_rows),
                            "best_aee_preset": aee["preset"],
                            "aee_report": aee["report"],
                        },
                        indent=2,
                    )
                )

    for quarter in ("Q1", "Q2", "Q3", "Q4"):
        rows = quarter_combo.get(quarter, [])
        combo = tune_aee(rows, f"COMBINED_{quarter}", thresholds) if rows else {"preset": None, "report": None}
        report["combined_by_quarter"][quarter] = {
            "selected_trade_rows": len(rows),
            "best_aee_preset": combo["preset"],
            "aee_report": combo["report"],
        }

    (ROOT / "quarter_side_ceiling_replay.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
