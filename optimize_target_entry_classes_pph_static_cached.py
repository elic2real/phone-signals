#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from optimize_target_entry_classes_contextual_v2 import pass_profile
from optimize_target_entry_classes_contextual import match_target_rule


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


def summarize(rows: list[dict[str, Any]], target: float, total_hours: float = 88.0) -> dict[str, Any]:
    tp_hits = 0
    sl_hits = 0
    timeouts = 0
    total_pips = 0.0
    for row in rows:
        pips = float(row["static_pips"])
        total_pips += pips
        if abs(pips - target) < 1e-9:
            tp_hits += 1
        elif abs(pips + target) < 1e-9:
            sl_hits += 1
        else:
            timeouts += 1
    count = len(rows)
    avg_pips = total_pips / count if count else 0.0
    return {
        "trade_count": count,
        "tp_hits": tp_hits,
        "sl_hits": sl_hits,
        "timeouts": timeouts,
        "tp_hit_rate": (tp_hits / count) if count else 0.0,
        "avg_pips": avg_pips,
        "avg_R": (avg_pips / target) if target else 0.0,
        "expectancy": avg_pips,
        "total_pips": total_pips,
        "pips_per_hour": total_pips / total_hours,
        "equity_per_hour_at_2pct_risk": (((avg_pips / target) if target else 0.0) * count * 0.02) / total_hours if total_hours else 0.0,
    }


def replay_rules(rows: list[dict[str, Any]], rules: list[dict[str, Any]], target: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for row in rows:
        for rule in rules:
            if rule_applies(row, rule):
                selected.append(row)
                break
    return selected, summarize(selected, target)


def better(a: dict[str, Any], b: dict[str, Any]) -> bool:
    # Objective is pips/hour only, with expectancy as first safety tiebreak and then TP rate.
    return (a["pips_per_hour"], a["expectancy"], a["tp_hit_rate"], a["trade_count"]) > (
        b["pips_per_hour"], b["expectancy"], b["tp_hit_rate"], b["trade_count"]
    )


def greedy_optimize(rows: list[dict[str, Any]], candidate_rules: list[dict[str, Any]], target: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    chosen: list[dict[str, Any]] = []
    remaining = candidate_rules[:]
    _, best = replay_rules(rows, chosen, target)

    improved = True
    while improved and remaining:
        improved = False
        best_idx = None
        best_candidate = best
        for idx, rule in enumerate(remaining):
            _, replay = replay_rules(rows, chosen + [rule], target)
            if replay["trade_count"] > 0 and better(replay, best_candidate):
                best_idx = idx
                best_candidate = replay
        if best_idx is not None:
            chosen.append(remaining.pop(best_idx))
            best = best_candidate
            improved = True

    pruned = True
    while pruned and len(chosen) > 1:
        pruned = False
        for idx in range(len(chosen)):
            trial = chosen[:idx] + chosen[idx + 1 :]
            _, replay = replay_rules(rows, trial, target)
            if better(replay, best):
                chosen = trial
                best = replay
                pruned = True
                break

    return chosen, best


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-rules", default="compiled_target_entry_classes_contextual_v2_11_sessions/target_entry_classes.json", type=Path)
    ap.add_argument("--targeted-rules", default="compiled_target_entry_classes_contextual_v2_targeted_11_sessions/target_entry_classes.json", type=Path)
    ap.add_argument("--truth-csv", default="compiled_target_entry_classes_contextual_v2_11_sessions/target_entry_truth_table.csv", type=Path)
    ap.add_argument("--output-dir", default="compiled_target_entry_classes_pph_static_cached_11_sessions", type=Path)
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
        chosen, replay = greedy_optimize(rows, candidates, target)
        kept_rules.extend(chosen)
        summary.append(
            {
                "direction": direction,
                "target_distance": target,
                "rule_count": len(chosen),
                **replay,
            }
        )

    (out / "target_entry_class_report.json").write_text(json.dumps({"summary": summary}, indent=2))
    (out / "target_entry_classes.json").write_text(json.dumps({"entry_classes": kept_rules}, indent=2))
    with (out / "target_entry_class_summary.csv").open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "direction",
                "target_distance",
                "rule_count",
                "trade_count",
                "tp_hits",
                "sl_hits",
                "timeouts",
                "tp_hit_rate",
                "avg_pips",
                "avg_R",
                "expectancy",
                "total_pips",
                "pips_per_hour",
            ],
        )
        writer.writeheader()
        writer.writerows(summary)
    print(json.dumps({"summary": summary}, indent=2))


if __name__ == "__main__":
    main()
