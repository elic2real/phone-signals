#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from optimize_target_entry_classes_contextual_v2 import (
    match_target_rule,
    pass_profile,
    summarize_replay,
)


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def rule_matches_stage(row: dict[str, Any], rule: dict[str, Any], stage: str) -> bool:
    if not match_target_rule(row, rule):
        return False
    if stage == "raw":
        return True
    allowed = rule.get("allowed_regimes") or []
    if allowed and row["energy_regime"] not in allowed:
        return False
    if stage == "regime":
        return True
    if not pass_profile(row, rule.get("context_profile")):
        return False
    if stage == "context":
        return True
    if not pass_profile(row, rule.get("point_profile")):
        return False
    return True


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--truth-csv",
        default="compiled_target_entry_classes_contextual_v2_11_sessions/target_entry_truth_table.csv",
        type=Path,
    )
    ap.add_argument(
        "--rules-json",
        default="compiled_target_entry_classes_contextual_v2_11_sessions/target_entry_classes.json",
        type=Path,
    )
    ap.add_argument(
        "--population-csv",
        default="compiled_target_entry_classes_contextual_v2_11_sessions/target_entry_population.csv",
        type=Path,
    )
    ap.add_argument(
        "--output-json",
        default="compiled_target_entry_classes_contextual_v2_11_sessions/target_blocker_audit.json",
        type=Path,
    )
    args = ap.parse_args()

    repo = Path(__file__).resolve().parent
    truth_csv = args.truth_csv if args.truth_csv.is_absolute() else repo / args.truth_csv
    rules_json = args.rules_json if args.rules_json.is_absolute() else repo / args.rules_json
    population_csv = args.population_csv if args.population_csv.is_absolute() else repo / args.population_csv
    output_json = args.output_json if args.output_json.is_absolute() else repo / args.output_json

    truth_rows = load_csv(truth_csv)
    selected_rows = load_csv(population_csv)
    rules = json.loads(rules_json.read_text())["entry_classes"]

    selected_keys = {
        (
            r["timestamp"],
            r["session_id"],
            r["quarter"],
            r["direction_assumed"],
            float(r["target_distance"]),
        )
        for r in selected_rows
    }

    by_key_rules: dict[tuple[str, float], list[dict[str, Any]]] = defaultdict(list)
    for rule in rules:
        by_key_rules[(rule["direction"], float(rule["target_distance"]))].append(rule)

    audit_rows: list[dict[str, Any]] = []
    for (direction, target), rule_set in sorted(by_key_rules.items()):
        subset = [
            r for r in truth_rows
            if r["direction_assumed"] == direction and float(r["target_distance"]) == target
        ]
        enter_like = [
            r for r in subset if r["action_truth"] in {f"ENTER_{direction}", f"HOLD_{direction}"}
        ]
        raw_match = [
            r for r in subset
            if any(rule_matches_stage(r, rule, "raw") for rule in rule_set)
        ]
        regime_match = [
            r for r in subset
            if any(rule_matches_stage(r, rule, "regime") for rule in rule_set)
        ]
        context_match = [
            r for r in subset
            if any(rule_matches_stage(r, rule, "context") for rule in rule_set)
        ]
        point_match = [
            r for r in subset
            if any(rule_matches_stage(r, rule, "point") for rule in rule_set)
        ]
        final_selected = [
            r for r in subset
            if (
                r["timestamp"],
                r["session_id"],
                r["quarter"],
                r["direction_assumed"],
                float(r["target_distance"]),
            )
            in selected_keys
        ]

        raw_replay = summarize_replay(raw_match, subset)
        final_replay = summarize_replay(final_selected, subset)

        quarter_truth = Counter(r["quarter"] for r in enter_like)
        quarter_rules = Counter(r["quarter"] for r in rule_set)
        quarter_truth_count = len(quarter_truth)
        quarter_rule_count = len(quarter_rules)
        quarter_choke = quarter_truth_count > quarter_rule_count

        metrics = {
            "enter_like_count": len(enter_like),
            "raw_rule_match_count": len(raw_match),
            "regime_match_count": len(regime_match),
            "context_match_count": len(context_match),
            "point_match_count": len(point_match),
            "final_selected_count": len(final_selected),
            "quarter_truth_count": quarter_truth_count,
            "quarter_rule_count": quarter_rule_count,
            "quarter_truth": dict(quarter_truth),
            "quarter_rules": dict(quarter_rules),
            "rule_count": len(rule_set),
            "raw_replay": raw_replay,
            "final_replay": final_replay,
            "drop_raw_to_regime": len(raw_match) - len(regime_match),
            "drop_regime_to_context": len(regime_match) - len(context_match),
            "drop_context_to_point": len(context_match) - len(point_match),
            "drop_point_to_final": len(point_match) - len(final_selected),
        }

        stages = {
            "quarter_scope": quarter_choke,
            "regime_gate": metrics["drop_raw_to_regime"] > max(20, 0.25 * max(1, len(raw_match))),
            "context_gate": metrics["drop_regime_to_context"] > max(20, 0.25 * max(1, len(regime_match))),
            "point_gate": metrics["drop_context_to_point"] > max(20, 0.25 * max(1, len(context_match))),
            "rule_competition": metrics["drop_point_to_final"] > max(20, 0.10 * max(1, len(point_match))),
        }
        dominant_blocker = max(stages, key=lambda k: int(stages[k]))
        if not any(stages.values()):
            dominant_blocker = "none"

        audit_rows.append(
            {
                "direction": direction,
                "target_distance": target,
                **metrics,
                "stage_flags": stages,
                "dominant_blocker": dominant_blocker,
            }
        )

    report = {
        "rows": audit_rows,
        "summary": {
            "dominant_blocker_counts": dict(Counter(r["dominant_blocker"] for r in audit_rows)),
        },
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(report, indent=2))
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
