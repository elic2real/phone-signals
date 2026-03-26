from __future__ import annotations

import csv
import json
from pathlib import Path
from statistics import median
from typing import Any, Dict, List


ROOT = Path(__file__).resolve().parent
TARGETS = ["LONG_Q1", "LONG_Q3", "SHORT_Q2", "SHORT_Q4"]
STALL_VEL_THRESHOLD = 0.15
STALL_WINDOW = 3
REVERSAL_VEL_THRESHOLD = -0.4
PROFIT_BAND = 1.5


def pnl(direction: str, start: float, px: float) -> float:
    pip = 0.0001
    return ((px - start) / pip) if direction == "LONG" else ((start - px) / pip)


def classify_state(
    profit_now: float,
    giveback_now: float,
    velocity_now: float,
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


def build_life_metrics(row: Dict[str, Any], assumed_direction: str, node_label: str) -> List[Dict[str, Any]]:
    path = row["price_path"]
    start = row["price_start"]
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
        if current_profit > peak_profit:
            peak_profit = current_profit
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
            time_under_profit_threshold,
            opposite_direction_strength,
            peak_profit,
        )
        scenario_rows.append(
            {
                "node_label": node_label,
                "cluster_id": row["cluster_id"],
                "timestamp_start": row["timestamp_start"],
                "original_direction": row["direction"],
                "assumed_direction": assumed_direction,
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
    report: Dict[str, Any] = {}
    for label in TARGETS:
        payload = json.loads((ROOT / f"quarter_side_{label.lower()}.json").read_text())
        rows = payload["selected_rows"]
        scenario_rows: List[Dict[str, Any]] = []
        for row in rows:
            scenario_rows.extend(build_life_metrics(row, row["direction"], label))
            scenario_rows.extend(build_life_metrics(row, "SHORT" if row["direction"] == "LONG" else "LONG", label))
        fieldnames = list(scenario_rows[0].keys()) if scenario_rows else ["node_label"]
        csv_path = ROOT / f"quarter_node_{label.lower()}_scenarios.csv"
        with csv_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(scenario_rows)
        thresholds = {
            "stall": summarize_thresholds(scenario_rows, "stall"),
            "panic": summarize_thresholds(scenario_rows, "panic"),
            "decay": summarize_thresholds(scenario_rows, "decay"),
        }
        bad_rows = [r for r in scenario_rows if r["recommended_action"] in {"PANIC", "DO_NOT_ENTER", "DECAY_EXIT"}]
        thresholds["bad_trade"] = {
            "count": len(bad_rows),
            "opposite_direction_strength_median": median(r["opposite_direction_strength"] for r in bad_rows) if bad_rows else 0.0,
            "giveback_now_median": median(r["giveback_now"] for r in bad_rows) if bad_rows else 0.0,
            "time_open_median": median(r["time_open"] for r in bad_rows) if bad_rows else 0.0,
            "reversal_points_median": median(r["reversal_points_so_far"] for r in bad_rows) if bad_rows else 0.0,
        }
        threshold_path = ROOT / f"quarter_node_{label.lower()}_thresholds.json"
        threshold_path.write_text(json.dumps(thresholds, indent=2))
        report[label] = {
            "trade_count": len(rows),
            "scenario_row_count": len(scenario_rows),
            "scenario_counts": {
                k: sum(1 for r in scenario_rows if r["scenario_type"] == k)
                for k in sorted({r["scenario_type"] for r in scenario_rows})
            },
            "threshold_file": threshold_path.name,
            "scenario_file": csv_path.name,
        }
    (ROOT / "quarter_node_scenario_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
