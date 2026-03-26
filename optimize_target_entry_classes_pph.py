#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from optimize_target_entry_classes_contextual_v2 import (
    pass_profile,
    replay_rule,
)
from optimize_target_entry_classes_contextual import match_target_rule, summarize_replay


def load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open() as f:
        return list(csv.DictReader(f))


def rule_applies(row: dict[str, Any], rule: dict[str, Any]) -> bool:
    if not match_target_rule(row, rule):
        return False
    if (rule.get("allowed_regimes") or []) and row["energy_regime"] not in rule["allowed_regimes"]:
        return False
    if not pass_profile(row, rule.get("context_profile")):
        return False
    if not pass_profile(row, rule.get("point_profile")):
        return False
    return True


def replay_rules(rows: list[dict[str, Any]], rules: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for row in rows:
        for rule in rules:
            if rule_applies(row, rule):
                selected.append(row)
                break
    return selected, summarize_replay(selected, rows)


def greedy_optimize(rows: list[dict[str, Any]], candidate_rules: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    chosen: list[dict[str, Any]] = []
    remaining = candidate_rules[:]
    _, best = replay_rules(rows, chosen)
    best_pph = float(best.get("pips_per_hour", 0.0))

    improved = True
    while improved and remaining:
        improved = False
        best_candidate_idx = None
        best_candidate_replay = None
        best_candidate_pph = best_pph
        for idx, rule in enumerate(remaining):
            _, replay = replay_rules(rows, chosen + [rule])
            pph = float(replay.get("pips_per_hour", 0.0))
            if pph > best_candidate_pph + 1e-12:
                best_candidate_pph = pph
                best_candidate_idx = idx
                best_candidate_replay = replay
        if best_candidate_idx is not None:
            chosen.append(remaining.pop(best_candidate_idx))
            best = best_candidate_replay
            best_pph = best_candidate_pph
            improved = True

    pruned = True
    while pruned and len(chosen) > 1:
        pruned = False
        for idx in range(len(chosen)):
            trial = chosen[:idx] + chosen[idx + 1 :]
            _, replay = replay_rules(rows, trial)
            pph = float(replay.get("pips_per_hour", 0.0))
            if pph >= best_pph - 1e-12:
                chosen = trial
                best = replay
                best_pph = pph
                pruned = True
                break

    return chosen, best


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-rules", default="compiled_target_entry_classes_contextual_v2_11_sessions/target_entry_classes.json", type=Path)
    ap.add_argument("--targeted-rules", default="compiled_target_entry_classes_contextual_v2_targeted_11_sessions/target_entry_classes.json", type=Path)
    ap.add_argument("--truth-csv", default="compiled_target_entry_classes_contextual_v2_11_sessions/target_entry_truth_table.csv", type=Path)
    ap.add_argument("--output-dir", default="compiled_target_entry_classes_pph_11_sessions", type=Path)
    args = ap.parse_args()

    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)

    truth_rows = load_csv(args.truth_csv)
    base_rules = load_json(args.base_rules)["entry_classes"]
    targeted_rules = load_json(args.targeted_rules)["entry_classes"]

    targeted_keys = {
        ("LONG", 11.0),
        ("LONG", 13.0),
        ("LONG", 15.0),
        ("SHORT", 1.5),
    }
    merged_rules = []
    for rule in base_rules:
        key = (rule["direction"], float(rule["target_distance"]))
        if key not in targeted_keys:
            merged_rules.append(rule)
    for rule in targeted_rules:
        key = (rule["direction"], float(rule["target_distance"]))
        if key in targeted_keys:
            merged_rules.append(rule)

    grouped_rules: dict[tuple[str, float], list[dict[str, Any]]] = defaultdict(list)
    for rule in merged_rules:
        grouped_rules[(rule["direction"], float(rule["target_distance"]))].append(rule)

    grouped_rows: dict[tuple[str, float], list[dict[str, Any]]] = defaultdict(list)
    for row in truth_rows:
        grouped_rows[(row["direction_assumed"], float(row["target_distance"]))].append(row)

    summary = []
    kept_rules = []
    for key in sorted(grouped_rows.keys()):
        direction, target = key
        rows = grouped_rows[key]
        candidates = grouped_rules.get(key, [])
        chosen, replay = greedy_optimize(rows, candidates)
        for rule in chosen:
            kept_rules.append(rule)
        summary.append(
            {
                "direction": direction,
                "target_distance": target,
                "rule_count": len(chosen),
                "trade_count": replay["trade_count"],
                "wins": replay["wins"],
                "losses": replay["losses"],
                "win_rate": replay["win_rate"],
                "avg_win": replay["avg_win"],
                "avg_loss": replay["avg_loss"],
                "expectancy": replay["expectancy"],
                "avg_R": replay["avg_R"],
                "pips_per_hour": replay["pips_per_hour"],
                "good_capture": replay["good_capture"],
                "bad_trigger": replay["bad_trigger"],
                "noise_trigger": replay["noise_trigger"],
            }
        )

    report = {"summary": summary}
    (out / "target_entry_class_report.json").write_text(json.dumps(report, indent=2))
    (out / "target_entry_classes.json").write_text(json.dumps({"entry_classes": kept_rules}, indent=2))
    with (out / "target_entry_class_summary.csv").open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "direction",
                "target_distance",
                "rule_count",
                "trade_count",
                "wins",
                "losses",
                "win_rate",
                "avg_win",
                "avg_loss",
                "expectancy",
                "avg_R",
                "pips_per_hour",
                "good_capture",
                "bad_trigger",
                "noise_trigger",
            ],
        )
        writer.writeheader()
        writer.writerows(summary)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
