#!/usr/bin/env python3
from __future__ import annotations

import copy
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

import run_aee_stage_compiler as aee


ROOT = Path(__file__).resolve().parent
INPUT_DIR = ROOT / "compiled_aee_stage_11_sessions_canonical"
OUT_DIR = ROOT / "compiled_aee_trade_types_11_sessions"
HARVESTER_TARGETS = {"1.5", "2.5"}
RUNNER_TARGETS = {"4.5", "6.0", "7.0", "8.0", "9.0", "11.0", "13.0", "15.0"}


def load_json(path: Path):
    return json.loads(path.read_text())


def load_csv(path: Path):
    with path.open() as f:
        return list(csv.DictReader(f))


def to_replay_trade(row: dict) -> dict:
    return {
        "trade_id": row["trade_id"],
        "entry_time": row["entry_time"],
        "direction": row["direction"],
        "target_distance": float(row["target_distance"]),
        "quarter": row["quarter"],
        "session_id": row["session_id"],
        "static_pips": float(row["static_pips"]),
        "static_R": float(row["static_R"]),
        "static_reason": row["static_reason"],
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    replay = load_json(INPUT_DIR / "aee_replay" / "target_selective_aee.json")
    trade_rows = replay["trade_rows"]
    state_rows = load_csv(INPUT_DIR / "aee_state_stream" / "aee_state_stream.csv")
    rules = load_json(INPUT_DIR / "aee_rules" / "aee_rule_derivation_report.json")

    by_trade_states = defaultdict(list)
    for row in state_rows:
        by_trade_states[row["trade_id"]].append(row)
    for rows in by_trade_states.values():
        rows.sort(key=lambda r: int(r["bar_index"]))

    groups = {
        "harvester": [r for r in trade_rows if str(float(r["target_distance"])) in HARVESTER_TARGETS],
        "runner": [r for r in trade_rows if str(float(r["target_distance"])) in RUNNER_TARGETS],
    }

    summary = {"aggregate": {}, "trade_types": {}}
    for trade_type, subset in groups.items():
        subset_states = []
        for tr in subset:
            subset_states.extend(by_trade_states[tr["trade_id"]])
        base = aee.replay_variant([to_replay_trade(tr) for tr in subset], subset_states, rules, "bias_plus_context_aee")
        best = base
        best_rules = copy.deepcopy(rules)
        if trade_type == "harvester":
            for hp_add in [0.0, -0.5, -1.0]:
                for hgb_add in [0.0, -0.25]:
                    for dt_add in [0.0, -6.0]:
                        cand = copy.deepcopy(rules)
                        for direction in ("LONG", "SHORT"):
                            cand["direction_modifiers"][direction]["harvest_profit_floor"] = round(
                                float(rules["direction_modifiers"][direction]["harvest_profit_floor"]) + hp_add, 6
                            )
                        for tgt in HARVESTER_TARGETS:
                            cand["target_modifiers"].setdefault(tgt, {})
                            cand["target_modifiers"][tgt]["harvest_giveback_tolerance"] = round(
                                float(rules["target_modifiers"].get(tgt, {}).get("harvest_giveback_tolerance", 0.0)) + hgb_add, 6
                            )
                            cand["target_modifiers"][tgt]["decay_time_since_peak"] = round(
                                float(rules["target_modifiers"].get(tgt, {}).get("decay_time_since_peak", 0.0)) + dt_add, 6
                            )
                        res = aee.replay_variant([to_replay_trade(tr) for tr in subset], subset_states, cand, "bias_plus_context_aee")
                        if (res.metrics["pips_per_hour"], res.metrics["avg_aee_R"]) > (best.metrics["pips_per_hour"], best.metrics["avg_aee_R"]):
                            best = res
                            best_rules = cand
        else:
            for hp_add in [0.0, 0.5, 1.0]:
                for dt_add in [0.0, 8.0, 16.0]:
                    for eb_add in [0.0, 0.06, 0.12]:
                        cand = copy.deepcopy(rules)
                        for direction in ("LONG", "SHORT"):
                            cand["direction_modifiers"][direction]["harvest_profit_floor"] = round(
                                float(rules["direction_modifiers"][direction]["harvest_profit_floor"]) + hp_add, 6
                            )
                        for tgt in RUNNER_TARGETS:
                            cand["target_modifiers"].setdefault(tgt, {})
                            cand["target_modifiers"][tgt]["decay_time_since_peak"] = round(
                                float(rules["target_modifiers"].get(tgt, {}).get("decay_time_since_peak", 0.0)) + dt_add, 6
                            )
                            cand["target_modifiers"][tgt]["extension_budget_floor"] = round(
                                float(rules["target_modifiers"].get(tgt, {}).get("extension_budget_floor", 0.0)) + eb_add, 6
                            )
                        res = aee.replay_variant([to_replay_trade(tr) for tr in subset], subset_states, cand, "bias_plus_context_aee")
                        if (res.metrics["pips_per_hour"], res.metrics["avg_aee_R"]) > (best.metrics["pips_per_hour"], best.metrics["avg_aee_R"]):
                            best = res
                            best_rules = cand

        summary["trade_types"][trade_type] = {
            "baseline_metrics": base.metrics,
            "optimized_metrics": best.metrics,
            "trade_count": len(subset),
            "rules": best_rules,
        }

    (OUT_DIR / "aee_trade_type_report.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps({"status": "PASS", "trade_types": list(summary["trade_types"].keys())}, indent=2))


if __name__ == "__main__":
    main()
