#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import build_entry_trigger_state_machine as trig


def replay_for_rules(rows_all: list[dict[str, Any]], rules: list[dict[str, Any]]) -> dict[str, Any]:
    selected_rows: list[dict[str, Any]] = []
    for row in rows_all:
        for rule in rules:
            if trig.match_rule(row, rule):
                selected_rows.append(row)
                break
    return trig.summarize_replay(selected_rows, rows_all)


def rule_key(rule: dict[str, Any]) -> str:
    return f"{rule['direction']}|{rule['quarter']}|{rule['path_class_name']}|{rule['path_class_id']}"


def recommendation(delta_pph: float, delta_exp: float, delta_bad: float, delta_noise: float) -> str:
    contamination = delta_bad + delta_noise
    if delta_pph >= 0.25:
        return "KEEP"
    if delta_pph >= 0.10 and contamination <= 0.02:
        return "KEEP"
    if delta_pph <= 0.05 and contamination >= 0.02:
        return "CUT"
    if delta_exp < 0 and contamination > 0:
        return "CUT"
    return "RE-GATE"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-truth-csv", required=True, type=Path)
    ap.add_argument("--rules-json", required=True, type=Path)
    ap.add_argument("--output-json", required=True, type=Path)
    args = ap.parse_args()

    rows_all = trig.load_csv(args.state_truth_csv)
    rules = json.loads(args.rules_json.read_text())["path_classes"]

    full_replay = replay_for_rules(rows_all, rules)
    report: dict[str, Any] = {
        "full_blend": full_replay,
        "islands": [],
    }

    for idx, rule in enumerate(rules):
        key = rule_key(rule)
        standalone = replay_for_rules(rows_all, [rule])
        remaining = [r for j, r in enumerate(rules) if j != idx]
        minus_one = replay_for_rules(rows_all, remaining)
        delta_pph = full_replay["pips_per_hour"] - minus_one["pips_per_hour"]
        delta_exp = full_replay["expectancy"] - minus_one["expectancy"]
        delta_bad = full_replay["bad_trigger"] - minus_one["bad_trigger"]
        delta_noise = full_replay["noise_trigger"] - minus_one["noise_trigger"]
        delta_trades = full_replay["trade_count"] - minus_one["trade_count"]
        delta_good = full_replay["good_capture"] - minus_one["good_capture"]
        report["islands"].append(
            {
                "rule_key": key,
                "direction": rule["direction"],
                "quarter": rule["quarter"],
                "path_class_name": rule["path_class_name"],
                "path_class_id": rule["path_class_id"],
                "standalone": standalone,
                "leave_one_out": minus_one,
                "marginal_if_removed": {
                    "delta_trades": delta_trades,
                    "delta_good_capture": delta_good,
                    "delta_bad_trigger": delta_bad,
                    "delta_noise_trigger": delta_noise,
                    "delta_expectancy": delta_exp,
                    "delta_pips_per_hour": delta_pph,
                    "good_to_dirt_ratio": (
                        delta_good / max(1e-9, delta_bad + delta_noise)
                        if (delta_bad + delta_noise) != 0
                        else 0.0
                    ),
                },
                "recommendation": recommendation(delta_pph, delta_exp, delta_bad, delta_noise),
            }
        )

    report["islands"].sort(
        key=lambda x: (
            x["marginal_if_removed"]["delta_pips_per_hour"],
            x["marginal_if_removed"]["delta_expectancy"],
            x["marginal_if_removed"]["delta_good_capture"],
        ),
        reverse=True,
    )

    args.output_json.write_text(json.dumps(report, indent=2))
    print(json.dumps({
        "full_pips_per_hour": full_replay["pips_per_hour"],
        "island_count": len(report["islands"]),
        "top_island": report["islands"][0]["rule_key"] if report["islands"] else None,
        "top_delta_pph": report["islands"][0]["marginal_if_removed"]["delta_pips_per_hour"] if report["islands"] else 0.0,
    }, indent=2))


if __name__ == "__main__":
    main()
