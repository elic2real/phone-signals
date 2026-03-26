#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import run_aee_stage_compiler as aee


ROOT = Path(__file__).resolve().parent


def load_trade_rows(report_path: Path) -> list[dict[str, Any]]:
    data = json.loads(report_path.read_text())
    return data["trade_rows"]


def load_state_rows(path: Path) -> list[dict[str, Any]]:
    return aee.load_csv(path)


def load_trades_from_selected(path: Path) -> list[dict[str, Any]]:
    return aee.load_csv(path)


def apply_overrides(
    rules: dict[str, Any],
    decay_time_add: float = 0.0,
    decay_giveback_add: float = 0.0,
    decay_energy_add: float = 0.0,
    harvest_profit_add: float = 0.0,
    harvest_giveback_add: float = 0.0,
    hold_budget_add: float = 0.0,
    panic_opp_add: float = 0.0,
    long_harvest_floor_add: float = 0.0,
    short_harvest_floor_add: float = 0.0,
) -> dict[str, Any]:
    out = deepcopy(rules)
    base_rules = {r["rule_id"]: r for r in out["base_rules"]}
    base_rules["base_decay"]["conditions"]["time_since_peak_min"] = round(
        float(base_rules["base_decay"]["conditions"]["time_since_peak_min"]) + decay_time_add, 6
    )
    base_rules["base_decay"]["conditions"]["giveback_now_min"] = round(
        float(base_rules["base_decay"]["conditions"]["giveback_now_min"]) + decay_giveback_add, 6
    )
    base_rules["base_decay"]["conditions"]["energy_ratio_max"] = round(
        float(base_rules["base_decay"]["conditions"]["energy_ratio_max"]) + decay_energy_add, 6
    )
    base_rules["base_harvest"]["conditions"]["profit_now_min"] = round(
        float(base_rules["base_harvest"]["conditions"]["profit_now_min"]) + harvest_profit_add, 6
    )
    base_rules["base_harvest"]["conditions"]["giveback_now_min"] = round(
        float(base_rules["base_harvest"]["conditions"]["giveback_now_min"]) + harvest_giveback_add, 6
    )
    base_rules["base_hold"]["conditions"]["remaining_budget_min"] = round(
        float(base_rules["base_hold"]["conditions"]["remaining_budget_min"]) + hold_budget_add, 6
    )
    base_rules["base_panic"]["conditions"]["opposite_direction_strength_min"] = round(
        float(base_rules["base_panic"]["conditions"]["opposite_direction_strength_min"]) + panic_opp_add, 6
    )
    out["direction_modifiers"]["LONG"]["harvest_profit_floor"] = round(
        float(out["direction_modifiers"]["LONG"]["harvest_profit_floor"]) + long_harvest_floor_add, 6
    )
    out["direction_modifiers"]["SHORT"]["harvest_profit_floor"] = round(
        float(out["direction_modifiers"]["SHORT"]["harvest_profit_floor"]) + short_harvest_floor_add, 6
    )
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--aee-report", type=Path, default=ROOT / "compiled_aee_stage_11_sessions" / "aee_stage_report.json")
    ap.add_argument("--replay-json", type=Path, default=ROOT / "compiled_aee_stage_11_sessions" / "aee_replay" / "bias_plus_context_aee.json")
    ap.add_argument("--selected-csv", type=Path, default=ROOT / "compiled_aee_stage_11_sessions" / "aee_state_stream" / "selected_entry_population.csv")
    ap.add_argument("--state-csv", type=Path, default=ROOT / "compiled_aee_stage_11_sessions" / "aee_state_stream" / "aee_state_stream.csv")
    ap.add_argument("--rules-json", type=Path, default=ROOT / "compiled_aee_stage_11_sessions" / "aee_rules" / "aee_rules.json")
    ap.add_argument("--output-dir", type=Path, default=ROOT / "compiled_aee_stage_11_sessions_tuned")
    args = ap.parse_args()

    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)

    report = json.loads(args.aee_report.read_text())
    rules = json.loads(args.rules_json.read_text())
    state_rows = load_state_rows(args.state_csv)
    trades = load_trades_from_selected(args.selected_csv)

    best = {
        "score": report["performance"]["aee_metrics"]["pips_per_hour"],
        "params": {},
        "result": None,
        "rules": rules,
    }

    # Fixed-population deterministic search around the current champion.
    for decay_time_add in (0.0, 5.0, 10.0, 15.0, 20.0):
        for decay_giveback_add in (0.0, 1.0, 2.0):
            for decay_energy_add in (0.0, 0.05):
                for harvest_profit_add in (0.0, 1.0, 2.0):
                    for harvest_giveback_add in (0.0, 0.5):
                        for hold_budget_add in (0.0, 0.03):
                            for panic_opp_add in (0.0, 0.05):
                                trial_rules = apply_overrides(
                                    rules,
                                    decay_time_add=decay_time_add,
                                    decay_giveback_add=decay_giveback_add,
                                    decay_energy_add=decay_energy_add,
                                    harvest_profit_add=harvest_profit_add,
                                    harvest_giveback_add=harvest_giveback_add,
                                    hold_budget_add=hold_budget_add,
                                    panic_opp_add=panic_opp_add,
                                )
                                trial_struct = aee.build_rule_family(state_rows)[1]
                                trial_struct["base"] = aee.build_rule_family(state_rows)[1]["base"]
                                # overwrite compiled rule structures used by decide_action
                                # by translating derivation back to runtime structure
                                base_map = {}
                                for r in trial_rules["base_rules"]:
                                    rid = r["rule_id"].replace("base_", "")
                                    base_map[rid] = {
                                        "profit_now": r["conditions"].get("profit_now_min", r["conditions"].get("profit_now_max", 0.0)),
                                        "giveback_now": r["conditions"].get("giveback_now_min", 0.0),
                                        "velocity_now": r["conditions"].get("velocity_now_max", 0.0),
                                        "time_open": r["conditions"].get("time_open_min", 0.0),
                                        "time_since_peak": r["conditions"].get("time_since_peak_min", 0.0),
                                        "progress_ratio": r["conditions"].get("progress_ratio_min", r["conditions"].get("progress_ratio_max", 0.0)),
                                        "energy_ratio": r["conditions"].get("energy_ratio_min", r["conditions"].get("energy_ratio_max", 0.0)),
                                        "opposite_direction_strength": r["conditions"].get("opposite_direction_strength_min", 0.0),
                                        "remaining_budget": r["conditions"].get("remaining_budget_min", 0.0),
                                        "sample_size": r.get("sample_size", 0),
                                    }
                                runtime_rules = {
                                    "base": base_map,
                                    "direction_modifiers": trial_rules["direction_modifiers"],
                                    "target_modifiers": trial_rules["target_modifiers"],
                                }
                                result = aee.replay_variant(trades, state_rows, runtime_rules, "bias_plus_context_aee")
                                score = result.metrics["pips_per_hour"]
                                if score > best["score"]:
                                    best = {
                                        "score": score,
                                        "params": {
                                            "decay_time_add": decay_time_add,
                                            "decay_giveback_add": decay_giveback_add,
                                            "decay_energy_add": decay_energy_add,
                                            "harvest_profit_add": harvest_profit_add,
                                            "harvest_giveback_add": harvest_giveback_add,
                                            "hold_budget_add": hold_budget_add,
                                            "panic_opp_add": panic_opp_add,
                                        },
                                        "result": {
                                            "metrics": result.metrics,
                                            "action_counts": dict(result.action_counts),
                                        },
                                        "rules": trial_rules,
                                    }

    (out / "best_rules.json").write_text(json.dumps(best["rules"], indent=2))
    (out / "best_result.json").write_text(json.dumps(best, indent=2))
    print(json.dumps({"best_pph": best["score"], "params": best["params"]}, indent=2))


if __name__ == "__main__":
    main()
