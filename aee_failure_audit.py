from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Tuple

from aee_state_machine import pnl
from entry_mode_distance_sweep import simulate_harvester_trade
from quarter_ceiling_tuner import BASE, replay_trade_param


ROOT = Path(__file__).resolve().parent
TARGETS = ["LONG_Q1", "LONG_Q3", "SHORT_Q2", "SHORT_Q4"]


def load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text())


def prepare_rows(label: str) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    base_payload = load_json(ROOT / f"quarter_side_{label.lower()}.json")
    local_payload = load_json(ROOT / f"quarter_node_local_{label.lower()}_replay.json")
    thresholds = load_json(ROOT / f"quarter_node_{label.lower()}_thresholds.json")
    params = dict(BASE)
    params.update(local_payload["params"])
    rows = []
    for row in base_payload["selected_rows"]:
        clean = dict(row)
        clean["entry_mode"] = "harvester"
        sim = simulate_harvester_trade(clean, float(clean["distance"]))
        clean["pips"] = sim["pips"]
        clean["reason"] = sim["reason"]
        rows.append(clean)
    return rows, thresholds, params


def first_action_snapshot(row: Dict[str, Any], thresholds: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    path = row["price_path"]
    start = float(row["price_start"])
    direction = row["direction"]
    distance = float(row["distance"])
    peak_profit = 0.0
    peak_idx = 0
    time_under_profit_threshold = 0

    for idx in range(1, len(path)):
        current_profit = pnl(direction, start, path[idx])
        prev_profit = pnl(direction, start, path[idx - 1])
        velocity_now = current_profit - prev_profit
        if current_profit > peak_profit:
            peak_profit = current_profit
            peak_idx = idx
        giveback_now = max(0.0, peak_profit - current_profit)
        if current_profit < 1.5:
            time_under_profit_threshold += 1
        else:
            time_under_profit_threshold = 0
        opposite_direction_strength = pnl("SHORT" if direction == "LONG" else "LONG", start, path[idx])
        replay = replay_trade_param(
            {
                "price_path": path[idx - 1 :],
                "price_start": path[idx - 1],
                "direction": direction,
                "distance": distance,
            },
            thresholds,
            params,
        )
        # We don't use replay exit here; just continue with explicit action priority from the same logic below.
        from quarter_ceiling_tuner import state_action_param

        action = state_action_param(
            current_profit=current_profit,
            giveback_now=giveback_now,
            velocity_now=velocity_now,
            time_open=idx,
            time_since_peak=idx - peak_idx,
            time_under_profit_threshold=time_under_profit_threshold,
            opposite_direction_strength=opposite_direction_strength,
            direction=direction,
            distance=distance,
            thresholds=thresholds,
            params=params,
        )
        if action in {"PANIC", "DECAY_EXIT", "HARVEST", "DO_NOT_ENTER"}:
            static_sim = simulate_harvester_trade(row, distance)
            future_static = static_sim["pips"]
            return {
                "decision_index": idx,
                "exit_reason": action,
                "profit_at_action": current_profit,
                "mfe_at_action": peak_profit,
                "giveback_at_action": giveback_now,
                "velocity_at_action": velocity_now,
                "time_open": idx,
                "time_since_peak": idx - peak_idx,
                "time_under_profit_threshold": time_under_profit_threshold,
                "opposite_direction_strength": opposite_direction_strength,
                "counterfactual_static_pips": future_static,
                "counterfactual_gap_pips": future_static - current_profit,
            }
    return {
        "decision_index": len(path) - 1,
        "exit_reason": "HOLD",
        "profit_at_action": pnl(direction, start, path[-1]),
        "mfe_at_action": peak_profit,
        "giveback_at_action": max(0.0, peak_profit - pnl(direction, start, path[-1])),
        "velocity_at_action": pnl(direction, start, path[-1]) - pnl(direction, start, path[-2]) if len(path) > 1 else 0.0,
        "time_open": len(path) - 1,
        "time_since_peak": len(path) - 1 - peak_idx,
        "time_under_profit_threshold": time_under_profit_threshold,
        "opposite_direction_strength": pnl("SHORT" if direction == "LONG" else "LONG", start, path[-1]),
        "counterfactual_static_pips": simulate_harvester_trade(row, distance)["pips"],
        "counterfactual_gap_pips": 0.0,
    }


def audit_label(label: str) -> Dict[str, Any]:
    rows, thresholds, params = prepare_rows(label)
    failures = []
    action_counter = Counter()
    action_regions: Dict[str, List[float]] = defaultdict(list)

    for row in rows:
        static_sim = simulate_harvester_trade(row, float(row["distance"]))
        aee = replay_trade_param(row, thresholds, params)
        if aee["aee_pips"] < static_sim["pips"]:
            snapshot = first_action_snapshot(row, thresholds, params)
            action_counter[snapshot["exit_reason"]] += 1
            action_regions[f"{snapshot['exit_reason']}|profit"] .append(snapshot["profit_at_action"])
            action_regions[f"{snapshot['exit_reason']}|giveback"].append(snapshot["giveback_at_action"])
            action_regions[f"{snapshot['exit_reason']}|velocity"].append(snapshot["velocity_at_action"])
            failures.append(
                {
                    "cluster_id": row["cluster_id"],
                    "timestamp_start": row["timestamp_start"],
                    "distance": row["distance"],
                    "static_pips": static_sim["pips"],
                    "aee_pips": aee["aee_pips"],
                    "underperformance_pips": static_sim["pips"] - aee["aee_pips"],
                    **snapshot,
                }
            )

    aggregate = {
        "failure_count": len(failures),
        "failure_rate": len(failures) / len(rows) if rows else 0.0,
        "action_failure_counts": dict(action_counter),
        "action_state_regions": {},
    }
    for key, vals in action_regions.items():
        action, metric = key.split("|")
        aggregate["action_state_regions"].setdefault(action, {})[metric] = {
            "mean": mean(vals) if vals else 0.0,
            "min": min(vals) if vals else 0.0,
            "max": max(vals) if vals else 0.0,
        }

    return {
        "label": label,
        "trade_count": len(rows),
        "failures": failures,
        "aggregate": aggregate,
    }


def main() -> None:
    combined = {}
    for label in TARGETS:
        payload = audit_label(label)
        combined[label] = payload["aggregate"]
        (ROOT / f"aee_failure_audit_{label.lower()}.json").write_text(json.dumps(payload, indent=2))
    (ROOT / "aee_failure_audit_summary.json").write_text(json.dumps(combined, indent=2))
    print(json.dumps(combined, indent=2))


if __name__ == "__main__":
    main()
