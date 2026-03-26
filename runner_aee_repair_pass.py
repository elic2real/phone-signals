#!/usr/bin/env python3
from __future__ import annotations

import copy
import csv
import json
from collections import defaultdict
from pathlib import Path

import run_aee_stage_compiler as aee


ROOT = Path(__file__).resolve().parent
INPUT_DIR = ROOT / "compiled_aee_stage_11_sessions_canonical"
OUT_DIR = ROOT / "compiled_aee_runner_repair_11_sessions"
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
    trade_rows = [r for r in replay["trade_rows"] if str(float(r["target_distance"])) in RUNNER_TARGETS]
    state_rows = load_csv(INPUT_DIR / "aee_state_stream" / "aee_state_stream.csv")
    rules = load_json(INPUT_DIR / "aee_rules" / "aee_rule_derivation_report.json")

    by_trade_states = defaultdict(list)
    for row in state_rows:
        by_trade_states[row["trade_id"]].append(row)
    for rows in by_trade_states.values():
        rows.sort(key=lambda r: int(r["bar_index"]))
    subset_states = []
    for tr in trade_rows:
        subset_states.extend(by_trade_states[tr["trade_id"]])

    baseline = aee.replay_variant([to_replay_trade(tr) for tr in trade_rows], subset_states, rules, "bias_plus_context_aee")
    best = baseline
    best_rules = copy.deepcopy(rules)

    # Same style as earlier successful repairs: make decay much harder and delay harvest on runners.
    for hp_add in [1.0, 1.5, 2.0, 2.5]:
        for dt_add in [16.0, 24.0, 32.0]:
            for dg_add in [1.0, 2.0, 3.0]:
                for er_add in [0.05, 0.10, 0.15]:
                    cand = copy.deepcopy(rules)
                    # Delay harvest globally for runner-style trades.
                    for direction in ("LONG", "SHORT"):
                        cand["direction_modifiers"][direction]["harvest_profit_floor"] = round(
                            float(rules["direction_modifiers"][direction]["harvest_profit_floor"]) + hp_add, 6
                        )
                    # Make decay harder target-locally.
                    for tgt in RUNNER_TARGETS:
                        cand["target_modifiers"].setdefault(tgt, {})
                        cand["target_modifiers"][tgt]["decay_time_since_peak"] = round(
                            float(rules["target_modifiers"].get(tgt, {}).get("decay_time_since_peak", 0.0)) + dt_add, 6
                        )
                    cand["base"]["decay"]["giveback_now"] = round(float(rules["base"]["decay"]["giveback_now"]) + dg_add, 6)
                    cand["base"]["hold"]["energy_ratio"] = round(float(rules["base"]["hold"]["energy_ratio"]) + er_add, 6)
                    res = aee.replay_variant([to_replay_trade(tr) for tr in trade_rows], subset_states, cand, "bias_plus_context_aee")
                    if (res.metrics["pips_per_hour"], res.metrics["avg_aee_R"]) > (best.metrics["pips_per_hour"], best.metrics["avg_aee_R"]):
                        best = res
                        best_rules = cand

    report = {
        "baseline_metrics": baseline.metrics,
        "optimized_metrics": best.metrics,
        "trade_count": len(trade_rows),
        "rules": best_rules,
    }
    (OUT_DIR / "runner_aee_repair_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps({"status": "PASS", "trade_count": len(trade_rows)}, indent=2))


if __name__ == "__main__":
    main()
