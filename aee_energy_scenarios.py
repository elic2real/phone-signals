#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from statistics import median
from typing import Any, Dict, List


ROOT = Path(".")
STALL_VEL_THRESHOLD = 0.15
STALL_WINDOW = 3
REVERSAL_VEL_THRESHOLD = -0.4
PROFIT_BAND = 1.5


def load_winning_trade_rows() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for path in sorted(ROOT.glob("ceiling_*_profit_ceiling.json")):
        with path.open() as f:
            payload = json.load(f)
        for row in payload.get("rows", []):
            if row.get("pips", 0) > 0:
                row = dict(row)
                row["_source_file"] = path.name
                rows.append(row)
    return rows


def pnl(direction: str, start: float, px: float) -> float:
    pip = 0.0001
    return ((px - start) / pip) if direction == "LONG" else ((start - px) / pip)


def classify_state(
    profit_now: float,
    giveback_now: float,
    velocity_now: float,
    velocity_change: float,
    time_under_profit_threshold: int,
    opposite_direction_strength: float,
    peak_profit: float,
) -> tuple[str, str]:
    if opposite_direction_strength >= max(2.5, peak_profit * 0.8) and velocity_now <= REVERSAL_VEL_THRESHOLD:
        return "opposite-impulse", "PANIC"
    if profit_now <= 0 and velocity_now <= REVERSAL_VEL_THRESHOLD:
        return "panic", "PANIC"
    if giveback_now >= max(1.0, peak_profit * 0.5) and velocity_now <= 0:
        return "decay", "DECAY_EXIT"
    if time_under_profit_threshold >= 5 and abs(velocity_now) <= STALL_VEL_THRESHOLD:
        return "stall", "HARVEST"
    if peak_profit >= PROFIT_BAND and velocity_now > 0:
        return "continuation", "HOLD"
    if abs(velocity_now) <= STALL_VEL_THRESHOLD:
        return "noise", "DO_NOT_ENTER"
    return "whipsaw-giveback", "HARVEST"


def build_life_metrics(row: Dict[str, Any], assumed_direction: str) -> List[Dict[str, Any]]:
    path = row["price_path"]
    start = row["price_start"]
    source_id = f"{row['cluster_id']}::{row['timestamp_start']}::{assumed_direction}"
    scenario_rows: List[Dict[str, Any]] = []
    peak_profit = 0.0
    peak_idx = 0
    time_under_profit_threshold = 0
    stall_points = 0
    reversal_points = 0

    for idx in range(1, len(path)):
        current_profit = pnl(assumed_direction, start, path[idx])
        prev_profit = pnl(assumed_direction, start, path[idx - 1])
        velocity_now = current_profit - prev_profit
        prev_velocity = pnl(assumed_direction, start, path[idx - 1]) - pnl(assumed_direction, start, path[idx - 2]) if idx > 1 else 0.0
        velocity_change = velocity_now - prev_velocity
        peak_profit = max(peak_profit, current_profit)
        if current_profit >= peak_profit:
            peak_idx = idx
        giveback_now = max(0.0, peak_profit - current_profit)
        if current_profit < PROFIT_BAND:
            time_under_profit_threshold += 1
        else:
            time_under_profit_threshold = 0
        if idx >= STALL_WINDOW:
            recent_vels = [
                pnl(assumed_direction, start, path[j]) - pnl(assumed_direction, start, path[j - 1])
                for j in range(idx - STALL_WINDOW + 1, idx + 1)
            ]
            if max(abs(v) for v in recent_vels) <= STALL_VEL_THRESHOLD:
                stall_points += 1
        if velocity_now <= REVERSAL_VEL_THRESHOLD:
            reversal_points += 1
        opposite_direction_strength = pnl("SHORT" if assumed_direction == "LONG" else "LONG", start, path[idx])
        scenario_type, action = classify_state(
            current_profit,
            giveback_now,
            velocity_now,
            velocity_change,
            time_under_profit_threshold,
            opposite_direction_strength,
            peak_profit,
        )
        scenario_rows.append(
            {
                "scenario_id": f"{source_id}::{idx}",
                "source_path_id": row["timestamp_start"],
                "cluster_id": row["cluster_id"],
                "original_direction": row["direction"],
                "assumed_direction": assumed_direction,
                "mode": row.get("mode"),
                "distance": row["distance"],
                "time_index": idx,
                "time_open": idx,
                "profit_now": current_profit,
                "mfe_so_far": peak_profit,
                "giveback_now": giveback_now,
                "velocity_now": velocity_now,
                "velocity_change": velocity_change,
                "time_since_peak": idx - peak_idx,
                "time_under_profit_threshold": time_under_profit_threshold,
                "cluster_progress": row.get("cluster_progress", 0.0),
                "distance_to_recent_extreme": peak_profit - current_profit,
                "opposite_direction_strength": opposite_direction_strength,
                "stall_points_so_far": stall_points,
                "reversal_points_so_far": reversal_points,
                "scenario_type": scenario_type,
                "recommended_action": action,
            }
        )
    return scenario_rows


def summarize_thresholds(rows: List[Dict[str, Any]], scenario_type: str) -> Dict[str, Any]:
    subset = [r for r in rows if r["scenario_type"] == scenario_type]
    if not subset:
        return {"count": 0}
    return {
        "count": len(subset),
        "velocity_now_median": median(r["velocity_now"] for r in subset),
        "giveback_now_median": median(r["giveback_now"] for r in subset),
        "time_open_median": median(r["time_open"] for r in subset),
        "time_since_peak_median": median(r["time_since_peak"] for r in subset),
        "time_under_profit_threshold_median": median(r["time_under_profit_threshold"] for r in subset),
        "opposite_direction_strength_median": median(r["opposite_direction_strength"] for r in subset),
    }


def main() -> None:
    winning_rows = load_winning_trade_rows()
    scenario_rows: List[Dict[str, Any]] = []
    for row in winning_rows:
        scenario_rows.extend(build_life_metrics(row, row["direction"]))
        scenario_rows.extend(build_life_metrics(row, "SHORT" if row["direction"] == "LONG" else "LONG"))

    import csv

    fieldnames = [
        "scenario_id",
        "source_path_id",
        "cluster_id",
        "original_direction",
        "assumed_direction",
        "mode",
        "distance",
        "time_index",
        "time_open",
        "profit_now",
        "mfe_so_far",
        "giveback_now",
        "velocity_now",
        "velocity_change",
        "time_since_peak",
        "time_under_profit_threshold",
        "cluster_progress",
        "distance_to_recent_extreme",
        "opposite_direction_strength",
        "stall_points_so_far",
        "reversal_points_so_far",
        "scenario_type",
        "recommended_action",
    ]
    with (ROOT / "aee_energy_scenarios.csv").open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(scenario_rows)

    scenario_counts: Dict[str, int] = {}
    action_counts: Dict[str, int] = {}
    for row in scenario_rows:
        scenario_counts[row["scenario_type"]] = scenario_counts.get(row["scenario_type"], 0) + 1
        action_counts[row["recommended_action"]] = action_counts.get(row["recommended_action"], 0) + 1

    bad_trade_rows = [r for r in scenario_rows if r["recommended_action"] in {"PANIC", "DO_NOT_ENTER", "DECAY_EXIT"}]
    report = {
        "winning_trade_count_used": len(winning_rows),
        "scenario_row_count": len(scenario_rows),
        "scenario_counts": scenario_counts,
        "action_counts": action_counts,
        "bad_trade_detector": {
            "bad_trade_rate": len(bad_trade_rows) / max(1, len(scenario_rows)),
            "reversed_direction_rows": sum(1 for r in scenario_rows if r["assumed_direction"] != r["original_direction"]),
            "panic_rows": scenario_counts.get("panic", 0),
            "opposite_impulse_rows": scenario_counts.get("opposite-impulse", 0),
        },
        "by_assumed_direction": {},
    }
    for direction in ("LONG", "SHORT"):
        subset = [r for r in scenario_rows if r["assumed_direction"] == direction]
        report["by_assumed_direction"][direction] = {
            "rows": len(subset),
            "scenario_counts": {
                k: sum(1 for r in subset if r["scenario_type"] == k)
                for k in sorted({r["scenario_type"] for r in subset})
            },
            "action_counts": {
                k: sum(1 for r in subset if r["recommended_action"] == k)
                for k in sorted({r["recommended_action"] for r in subset})
            },
        }

    (ROOT / "bad_trade_detector_report.json").write_text(json.dumps(report, indent=2))
    (ROOT / "aee_stall_thresholds.json").write_text(json.dumps(summarize_thresholds(scenario_rows, "stall"), indent=2))
    (ROOT / "aee_panic_thresholds.json").write_text(json.dumps(summarize_thresholds(scenario_rows, "panic"), indent=2))
    (ROOT / "aee_decay_thresholds.json").write_text(json.dumps(summarize_thresholds(scenario_rows, "decay"), indent=2))
    (ROOT / "aee_bad_trade_thresholds.json").write_text(json.dumps({
        "count": len(bad_trade_rows),
        "opposite_direction_strength_median": median(r["opposite_direction_strength"] for r in bad_trade_rows) if bad_trade_rows else 0,
        "giveback_now_median": median(r["giveback_now"] for r in bad_trade_rows) if bad_trade_rows else 0,
        "time_open_median": median(r["time_open"] for r in bad_trade_rows) if bad_trade_rows else 0,
        "reversal_points_median": median(r["reversal_points_so_far"] for r in bad_trade_rows) if bad_trade_rows else 0,
    }, indent=2))


if __name__ == "__main__":
    main()
