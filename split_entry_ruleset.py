#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import build_entry_trigger_state_machine as trig


def replay_for_rules(rows_all: list[dict[str, Any]], rules: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    selected_rows: list[dict[str, Any]] = []
    for row in rows_all:
        for rule in rules:
            if trig.match_rule(row, rule):
                selected_rows.append(row)
                break
    return selected_rows, trig.summarize_replay(selected_rows, rows_all)


def classify_rule(rule: dict[str, Any]) -> str:
    cr = rule["candidate_replay"]
    if (
        cr["expectancy"] >= 0.55
        and cr["win_rate"] >= 0.60
        and cr["bad_trigger"] <= 0.08
        and cr["noise_trigger"] <= 0.02
    ):
        return "core_candidate"
    if (
        cr["expectancy"] > 0.0
        and cr["good_capture"] > cr["bad_trigger"]
        and cr["bad_trigger"] <= 0.10
        and cr["noise_trigger"] <= 0.10
    ):
        return "expansion_candidate"
    return "research"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-truth-csv", required=True, type=Path)
    ap.add_argument("--rules-json", required=True, type=Path)
    ap.add_argument("--output-dir", required=True, type=Path)
    args = ap.parse_args()

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    rows_all = trig.load_csv(args.state_truth_csv)
    rules_obj = json.loads(args.rules_json.read_text())
    rules = rules_obj["path_classes"]

    ranked: list[dict[str, Any]] = []
    for idx, rule in enumerate(rules):
        cr = rule["candidate_replay"]
        standalone_score = (
            cr["expectancy"] * 5.0
            + cr["good_capture"] * 4.0
            - cr["bad_trigger"] * 3.0
            - cr["noise_trigger"] * 2.0
        )
        ranked.append(
            {
                "index": idx,
                "rule_key": f"{rule['direction']}|{rule['quarter']}|{rule['path_class_name']}|{rule['path_class_id']}",
                "rule": rule,
                "standalone": cr,
                "standalone_score": standalone_score,
                "tier_hint": classify_rule(rule),
            }
        )

    ranked.sort(key=lambda r: (r["standalone_score"], r["standalone"]["pips_per_hour"]), reverse=True)

    core_rules: list[dict[str, Any]] = []
    core_selected: list[dict[str, Any]] = []
    _, empty_replay = replay_for_rules(rows_all, [])
    current_core_replay = empty_replay
    frontier: list[dict[str, Any]] = []

    for item in ranked:
        if item["tier_hint"] != "core_candidate":
            continue
        test_rules = core_rules + [item["rule"]]
        test_rows, test_replay = replay_for_rules(rows_all, test_rules)
        if (
            test_replay["expectancy"] >= max(0.45, current_core_replay["expectancy"])
            and test_replay["bad_trigger"] <= 0.09
            and test_replay["noise_trigger"] <= 0.03
            and (
                test_replay["pips_per_hour"] > current_core_replay["pips_per_hour"]
                or test_replay["good_capture"] > current_core_replay["good_capture"]
            )
        ):
            delta = {
                "delta_trades": test_replay["trade_count"] - current_core_replay["trade_count"],
                "delta_good_capture": test_replay["good_capture"] - current_core_replay["good_capture"],
                "delta_bad_trigger": test_replay["bad_trigger"] - current_core_replay["bad_trigger"],
                "delta_noise_trigger": test_replay["noise_trigger"] - current_core_replay["noise_trigger"],
                "delta_expectancy": test_replay["expectancy"] - current_core_replay["expectancy"],
                "delta_pips_per_hour": test_replay["pips_per_hour"] - current_core_replay["pips_per_hour"],
                "good_to_dirt_ratio": (
                    (test_replay["good_capture"] - current_core_replay["good_capture"])
                    / max(
                        1e-9,
                        (test_replay["bad_trigger"] - current_core_replay["bad_trigger"])
                        + (test_replay["noise_trigger"] - current_core_replay["noise_trigger"]),
                    )
                ),
            }
            frontier.append(
                {
                    "tier": "core",
                    "added_rule": item["rule_key"],
                    "replay": test_replay,
                    "delta": delta,
                }
            )
            core_rules = test_rules
            core_selected = test_rows
            current_core_replay = test_replay

    expansion_rules: list[dict[str, Any]] = []
    current_blend_rules = list(core_rules)
    current_blend_replay = current_core_replay

    for item in ranked:
        if item["rule"] in core_rules:
            continue
        if item["tier_hint"] == "research":
            continue
        test_rules = current_blend_rules + [item["rule"]]
        _, test_replay = replay_for_rules(rows_all, test_rules)
        delta_bad = test_replay["bad_trigger"] - current_blend_replay["bad_trigger"]
        delta_noise = test_replay["noise_trigger"] - current_blend_replay["noise_trigger"]
        delta_good = test_replay["good_capture"] - current_blend_replay["good_capture"]
        delta_pph = test_replay["pips_per_hour"] - current_blend_replay["pips_per_hour"]
        delta_exp = test_replay["expectancy"] - current_blend_replay["expectancy"]
        good_to_dirt = delta_good / max(1e-9, delta_bad + delta_noise)
        if (
            delta_pph > 0.0
            and test_replay["expectancy"] >= max(0.35, current_core_replay["expectancy"] - 0.15)
            and test_replay["bad_trigger"] <= 0.13
            and test_replay["noise_trigger"] <= 0.12
            and good_to_dirt >= 0.35
        ):
            frontier.append(
                {
                    "tier": "expansion",
                    "added_rule": item["rule_key"],
                    "replay": test_replay,
                    "delta": {
                        "delta_trades": test_replay["trade_count"] - current_blend_replay["trade_count"],
                        "delta_good_capture": delta_good,
                        "delta_bad_trigger": delta_bad,
                        "delta_noise_trigger": delta_noise,
                        "delta_expectancy": delta_exp,
                        "delta_pips_per_hour": delta_pph,
                        "good_to_dirt_ratio": good_to_dirt,
                    },
                }
            )
            expansion_rules.append(item["rule"])
            current_blend_rules = test_rules
            current_blend_replay = test_replay

    rejected_rules = [item["rule"] for item in ranked if item["rule"] not in core_rules and item["rule"] not in expansion_rules]

    _, full_blend_replay = replay_for_rules(rows_all, current_blend_rules)

    report = {
        "core_rule_count": len(core_rules),
        "expansion_rule_count": len(expansion_rules),
        "research_rule_count": len(rejected_rules),
        "core_only_replay": current_core_replay,
        "core_plus_expansion_replay": current_blend_replay,
        "full_current_blend_replay": full_blend_replay,
        "ranked_rules": [
            {
                "rule_key": item["rule_key"],
                "tier_hint": item["tier_hint"],
                "standalone_score": item["standalone_score"],
                "standalone": item["standalone"],
            }
            for item in ranked
        ],
        "frontier": frontier,
    }

    (out_dir / "entry_ruleset_split_report.json").write_text(json.dumps(report, indent=2))
    (out_dir / "core_ruleset.json").write_text(json.dumps({"path_classes": core_rules}, indent=2))
    (out_dir / "expansion_ruleset.json").write_text(json.dumps({"path_classes": expansion_rules}, indent=2))
    (out_dir / "research_ruleset.json").write_text(json.dumps({"path_classes": rejected_rules}, indent=2))
    print(json.dumps({
        "core_rule_count": len(core_rules),
        "expansion_rule_count": len(expansion_rules),
        "research_rule_count": len(rejected_rules),
        "core_pph": current_core_replay["pips_per_hour"],
        "blend_pph": current_blend_replay["pips_per_hour"],
    }, indent=2))


if __name__ == "__main__":
    main()
