#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from copy import deepcopy
from collections import defaultdict
from pathlib import Path
from typing import Any

from optimize_target_entry_classes_pph_static_cached import load_csv, load_json, rule_applies, summarize

SYMMETRIC_BREAK_EVEN_WIN_RATE = 0.505
SESSION_HOURS = 88.0
LOCAL_TUNING_MAX_ADDITIONS = 2
LOCAL_TUNING_MAX_PRUNES = 1
LOCAL_TUNING_MAX_THRESHOLD_STEPS = 2


def replay_rules(rows: list[dict[str, Any]], rules: list[dict[str, Any]], target: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for row in rows:
        for rule in rules:
            if rule_applies(row, rule):
                selected.append(row)
                break
    replay = summarize(selected, target)
    replay["opportunity_count"] = len(rows)
    replay["capture_rate"] = (replay["trade_count"] / len(rows)) if rows else 0.0
    replay["opportunity_density_per_hour"] = (len(rows) / SESSION_HOURS) if rows else 0.0
    replay["selected_density_per_hour"] = (float(replay.get("trade_count", 0)) / SESSION_HOURS) if replay.get("trade_count", 0) else 0.0
    replay["objective_score"] = replay["total_pips"] * math.sqrt(replay["capture_rate"]) if replay["capture_rate"] > 0 else 0.0
    return selected, replay


def minimum_trade_floor(opportunity_count: int, target: float) -> int:
    if opportunity_count <= 0:
        return 0
    if target <= 2.5:
        proportional_floor = int(math.ceil(opportunity_count * 0.04))
        if opportunity_count < 80:
            return max(3, int(math.ceil(opportunity_count * 0.08)))
        return max(8, proportional_floor)
    proportional_floor = int(math.ceil(opportunity_count * 0.03))
    if opportunity_count < 80:
        return max(2, int(math.ceil(opportunity_count * 0.06)))
    return max(5, proportional_floor)


def minimum_capture_floor(opportunity_count: int, target: float) -> float:
    if opportunity_count <= 0:
        return 0.0
    floor = 0.025 if target <= 2.5 else 0.012
    if opportunity_count < 120:
        floor *= 0.5
    return floor


def minimum_density_floor(opportunity_count: int, target: float) -> float:
    if opportunity_count <= 0:
        return 0.0
    return 0.08 if target <= 2.5 else 0.03


def maximum_rule_count(target: float) -> int:
    return 10 if target <= 2.5 else 8


def replay_is_robust(replay: dict[str, Any], target: float) -> bool:
    trade_floor = minimum_trade_floor(int(replay.get("opportunity_count", 0)), target)
    capture_floor = minimum_capture_floor(int(replay.get("opportunity_count", 0)), target)
    density_floor = minimum_density_floor(int(replay.get("opportunity_count", 0)), target)
    return (
        int(replay.get("trade_count", 0)) >= trade_floor
        and float(replay.get("capture_rate", 0.0)) >= capture_floor
        and float(replay.get("selected_density_per_hour", 0.0)) >= density_floor
        and float(replay.get("tp_hit_rate", 0.0)) >= SYMMETRIC_BREAK_EVEN_WIN_RATE
        and float(replay.get("expectancy", 0.0)) > 0.0
    )


def expansion_is_quality_additive(base: dict[str, Any], candidate: dict[str, Any], target: float) -> bool:
    if not replay_is_robust(candidate, target):
        return False
    added_trades = int(candidate.get("trade_count", 0)) - int(base.get("trade_count", 0))
    opportunity_count = int(candidate.get("opportunity_count", 0))
    minimum_added_trades = max(1, int(math.ceil(opportunity_count * (0.005 if target <= 2.5 else 0.003))))
    if added_trades < minimum_added_trades:
        return False
    wr_tolerance = 0.08 if target <= 2.5 else 0.10
    exp_tolerance = 0.20 if target <= 2.5 else 0.25
    return (
        float(candidate.get("tp_hit_rate", 0.0)) >= SYMMETRIC_BREAK_EVEN_WIN_RATE
        and float(candidate.get("tp_hit_rate", 0.0)) >= float(base.get("tp_hit_rate", 0.0)) - wr_tolerance
        and float(candidate.get("expectancy", 0.0)) >= float(base.get("expectancy", 0.0)) - exp_tolerance
        and float(candidate.get("avg_R", 0.0)) >= float(base.get("avg_R", 0.0)) - exp_tolerance
    )


def objective_key(replay: dict[str, Any], priority_mode: str) -> tuple[Any, ...]:
    if priority_mode == "expand_quality_entries":
        return (
            1 if replay["timeouts"] == 0 else 0,
            1 if replay_is_robust(replay, float(replay.get("target_distance", 0.0) or 0.0)) else 0,
            int(replay.get("trade_count", 0)),
            round(float(replay.get("capture_rate", 0.0)), 6),
            round(float(replay.get("objective_score", 0.0)), 6),
            round(float(replay.get("tp_hit_rate", 0.0)), 6),
            round(float(replay.get("expectancy", 0.0)), 6),
            round(float(replay.get("avg_R", 0.0)), 6),
            round(float(replay.get("pips_per_hour", 0.0)), 6),
            round(float(replay.get("total_pips", 0.0)), 6),
        )
    if priority_mode == "winrate_first":
        return (
            1 if replay["timeouts"] == 0 else 0,
            1 if replay_is_robust(replay, float(replay.get("target_distance", 0.0) or 0.0)) else 0,
            round(float(replay.get("tp_hit_rate", 0.0)), 6),
            round(float(replay.get("expectancy", 0.0)), 6),
            round(float(replay.get("avg_R", 0.0)), 6),
            round(float(replay.get("capture_rate", 0.0)), 6),
            int(replay.get("trade_count", 0)),
            round(float(replay.get("objective_score", 0.0)), 6),
            round(float(replay.get("total_pips", 0.0)), 6),
            round(float(replay.get("pips_per_hour", 0.0)), 6),
        )
    return (
        1 if replay["timeouts"] == 0 else 0,
        round(float(replay.get("objective_score", 0.0)), 6),
        round(float(replay.get("total_pips", 0.0)), 6),
        round(float(replay.get("pips_per_hour", 0.0)), 6),
        round(float(replay.get("capture_rate", 0.0)), 6),
        int(replay.get("trade_count", 0)),
        round(float(replay.get("expectancy", 0.0)), 6),
        round(float(replay.get("tp_hit_rate", 0.0)), 6),
    )


def better(a: dict[str, Any], b: dict[str, Any], priority_mode: str) -> bool:
    # Realized extraction first, then throughput, then how much of the mapped class we actually capture.
    return objective_key(a, priority_mode) > objective_key(b, priority_mode)


def parse_group_spec(spec: str) -> tuple[str, float]:
    direction, _, raw_target = str(spec or "").strip().upper().partition(":")
    if direction not in {"LONG", "SHORT"} or not raw_target:
        raise ValueError(f"Invalid optimize group '{spec}'. Expected DIRECTION:TARGET_DISTANCE.")
    return direction, float(raw_target)


def shrink_feature_bounds(rule: dict[str, Any], factor: float = 0.9) -> dict[str, Any] | None:
    feature_bounds = rule.get("feature_bounds") or {}
    if not isinstance(feature_bounds, dict) or not feature_bounds:
        return None
    mutated = deepcopy(rule)
    new_bounds: dict[str, Any] = {}
    changed = False
    for feature, span in feature_bounds.items():
        if not isinstance(span, dict):
            new_bounds[feature] = span
            continue
        lo = float(span.get("min", 0.0))
        hi = float(span.get("max", 0.0))
        if hi < lo:
            lo, hi = hi, lo
        center = (lo + hi) * 0.5
        half_width = (hi - lo) * 0.5
        new_half_width = half_width * factor
        if new_half_width < half_width:
            changed = True
        new_bounds[feature] = {
            "min": center - new_half_width,
            "max": center + new_half_width,
        }
    if not changed:
        return None
    mutated["feature_bounds"] = new_bounds
    return mutated


def tighten_profile_score(rule: dict[str, Any], profile_key: str, delta: float = 0.05) -> dict[str, Any] | None:
    profile = rule.get(profile_key)
    if not isinstance(profile, dict):
        return None
    score_min = float(profile.get("score_min", 0.0))
    tightened = min(0.95, score_min + delta)
    if tightened <= score_min + 1e-9:
        return None
    mutated = deepcopy(rule)
    mutated_profile = dict(profile)
    mutated_profile["score_min"] = tightened
    mutated[profile_key] = mutated_profile
    return mutated


def threshold_tune_variants(rule: dict[str, Any]) -> list[dict[str, Any]]:
    variants: list[dict[str, Any]] = []
    for candidate in (
        tighten_profile_score(rule, "context_profile", 0.05),
        tighten_profile_score(rule, "point_profile", 0.05),
        shrink_feature_bounds(rule, 0.9),
    ):
        if candidate is not None:
            variants.append(candidate)
    return variants


def threshold_tune_rules(
    rows: list[dict[str, Any]],
    chosen: list[dict[str, Any]],
    target: float,
    priority_mode: str,
    max_steps: int = LOCAL_TUNING_MAX_THRESHOLD_STEPS,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    tuned = chosen[:]
    _, best = replay_rules(rows, tuned, target)
    best["target_distance"] = target
    steps = 0
    improved = True
    while improved and steps < max_steps:
        improved = False
        best_trial_rules = tuned
        best_trial_replay = best
        for idx, rule in enumerate(tuned):
            for variant in threshold_tune_variants(rule):
                trial = tuned[:idx] + [variant] + tuned[idx + 1 :]
                _, replay = replay_rules(rows, trial, target)
                replay["target_distance"] = target
                if replay["trade_count"] > 0 and better(replay, best_trial_replay, priority_mode):
                    best_trial_rules = trial
                    best_trial_replay = replay
        if best_trial_rules is not tuned:
            tuned = best_trial_rules
            best = best_trial_replay
            steps += 1
            improved = True
    return tuned, best


def greedy_optimize(
    rows: list[dict[str, Any]],
    candidate_rules: list[dict[str, Any]],
    target: float,
    priority_mode: str,
    seed_rules: list[dict[str, Any]] | None = None,
    max_additions: int | None = None,
    max_prunes: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    seed_rules = seed_rules or []
    chosen: list[dict[str, Any]] = [rule for rule in candidate_rules if rule in seed_rules]
    remaining = [rule for rule in candidate_rules if rule not in chosen]
    _, best = replay_rules(rows, chosen, target)
    rule_cap = maximum_rule_count(target)
    additions_used = 0

    improved = True
    while improved and remaining and len(chosen) < rule_cap:
        if max_additions is not None and additions_used >= max_additions:
            break
        improved = False
        best_idx = None
        best_candidate = best
        for idx, rule in enumerate(remaining):
            _, replay = replay_rules(rows, chosen + [rule], target)
            replay["target_distance"] = target
            if replay["trade_count"] > 0 and better(replay, best_candidate, priority_mode):
                best_idx = idx
                best_candidate = replay
        if best_idx is not None:
            chosen.append(remaining.pop(best_idx))
            best = best_candidate
            additions_used += 1
            improved = True

    pruned = True
    prunes_used = 0
    while pruned and len(chosen) > 1:
        if max_prunes is not None and prunes_used >= max_prunes:
            break
        pruned = False
        for idx in range(len(chosen)):
            trial = chosen[:idx] + chosen[idx + 1 :]
            _, replay = replay_rules(rows, trial, target)
            replay["target_distance"] = target
            if replay["trade_count"] > 0 and objective_key(replay, priority_mode) >= objective_key(best, priority_mode):
                chosen = trial
                best = replay
                prunes_used += 1
                pruned = True
                break

    # In win-rate-first mode, reject tiny cherry-picked subsets and fall back to broader logic.
    if priority_mode == "winrate_first":
        best["target_distance"] = target
        if not replay_is_robust(best, target):
            broadened = []
            for rule in candidate_rules:
                _, replay = replay_rules(rows, broadened + [rule], target)
                replay["target_distance"] = target
                if replay["trade_count"] > 0 and (
                    replay_is_robust(replay, target)
                    or not broadened
                ):
                    broadened.append(rule)
                    best = replay
                if replay_is_robust(best, target):
                    break
            chosen = broadened or chosen

    return chosen, best


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-rules", default="compiled_target_entry_classes_contextual_v2_11_sessions/target_entry_classes.json", type=Path)
    ap.add_argument("--targeted-rules", default="compiled_target_entry_classes_contextual_v2_targeted_11_sessions/target_entry_classes.json", type=Path)
    ap.add_argument("--truth-csv", default="compiled_target_entry_classes_contextual_v2_11_sessions/target_entry_truth_table.csv", type=Path)
    ap.add_argument("--output-dir", default="compiled_target_entry_classes_no_timeouts_11_sessions", type=Path)
    ap.add_argument("--priority-mode", choices=["balanced", "winrate_first", "expand_quality_entries"], default="balanced")
    ap.add_argument(
        "--frozen-rules",
        type=Path,
        help="Existing no-timeout rules to preserve for groups outside the scoped optimize set.",
    )
    ap.add_argument(
        "--optimize-group",
        action="append",
        default=[],
        help="Restrict optimization to specific class buckets formatted as DIRECTION:TARGET_DISTANCE.",
    )
    ap.add_argument(
        "--freeze-unlisted-groups",
        action="store_true",
        help="Keep every class bucket not listed in --optimize-group frozen to its merged baseline rules.",
    )
    args = ap.parse_args()

    out = args.output_dir
    out.mkdir(parents=True, exist_ok=True)

    truth_rows = load_csv(args.truth_csv)
    base_rules = load_json(args.base_rules)["entry_classes"]
    targeted_rules = load_json(args.targeted_rules)["entry_classes"]
    frozen_rules = load_json(args.frozen_rules)["entry_classes"] if args.frozen_rules and args.frozen_rules.exists() else []

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

    grouped_frozen_rules: dict[tuple[str, float], list[dict[str, Any]]] = defaultdict(list)
    for rule in frozen_rules:
        grouped_frozen_rules[(rule["direction"], float(rule["target_distance"]))].append(rule)

    grouped_rows: dict[tuple[str, float], list[dict[str, Any]]] = defaultdict(list)
    for row in truth_rows:
        grouped_rows[(row["direction_assumed"], float(row["target_distance"]))].append(row)

    optimize_groups = {parse_group_spec(spec) for spec in args.optimize_group}

    summary = []
    kept_rules = []
    selected_population: list[dict[str, Any]] = []
    population_fieldnames = ["direction", "target_distance"]
    if truth_rows:
        population_fieldnames.extend([k for k in truth_rows[0].keys() if k not in {"direction", "target_distance"}])
    class_reports: dict[str, Any] = {}
    for key in sorted(grouped_rows.keys()):
        direction, target = key
        rows = grouped_rows[key]
        candidates = grouped_rules.get(key, [])
        if args.freeze_unlisted_groups and optimize_groups and key not in optimize_groups:
            chosen = grouped_frozen_rules.get(key, candidates[:])
            _, replay = replay_rules(rows, chosen, target)
            replay["target_distance"] = target
            replay["optimization_scope"] = "frozen_baseline"
        else:
            optimization_mode = "winrate_first" if args.priority_mode == "expand_quality_entries" else args.priority_mode
            local_seed_rules = grouped_frozen_rules.get(key, [])
            local_tuning_active = bool(args.freeze_unlisted_groups and optimize_groups)
            chosen, replay = greedy_optimize(
                rows,
                candidates,
                target,
                optimization_mode,
                seed_rules=local_seed_rules if local_tuning_active else None,
                max_additions=LOCAL_TUNING_MAX_ADDITIONS if local_tuning_active else None,
                max_prunes=LOCAL_TUNING_MAX_PRUNES if local_tuning_active else None,
            )
            if local_tuning_active and chosen:
                chosen, replay = threshold_tune_rules(
                    rows,
                    chosen,
                    target,
                    optimization_mode,
                )
            replay["target_distance"] = target
            replay["optimization_scope"] = "optimized_local_tuning" if local_tuning_active else "optimized"
            if args.priority_mode == "expand_quality_entries":
                remaining = [rule for rule in candidates if rule not in chosen]
                improved = True
                while improved and remaining and len(chosen) < maximum_rule_count(target):
                    if local_tuning_active and len(chosen) >= len(local_seed_rules) + LOCAL_TUNING_MAX_ADDITIONS:
                        break
                    improved = False
                    best_idx = None
                    best_candidate = replay
                    for idx, rule in enumerate(remaining):
                        _, candidate_replay = replay_rules(rows, chosen + [rule], target)
                        candidate_replay["target_distance"] = target
                        if expansion_is_quality_additive(replay, candidate_replay, target):
                            if (
                                objective_key(candidate_replay, "expand_quality_entries")
                                > objective_key(best_candidate, "expand_quality_entries")
                            ):
                                best_idx = idx
                                best_candidate = candidate_replay
                    if best_idx is not None:
                        chosen.append(remaining.pop(best_idx))
                        replay = best_candidate
                        replay["optimization_scope"] = "optimized_local_tuning" if local_tuning_active else "optimized"
                        improved = True
        selected_rows, _ = replay_rules(rows, chosen, target)
        kept_rules.extend(chosen)
        for row in selected_rows:
            selected_population.append(
                {
                    "direction": direction,
                    "target_distance": target,
                    **row,
                }
            )
        class_key = f"{direction}_{target:.1f}"
        payload = {
            "direction": direction,
            "target_distance": target,
            "rule_count": len(chosen),
            "rules": chosen,
            "selected_trade_count": len(selected_rows),
            **replay,
        }
        class_reports[class_key] = payload
        summary.append(payload)

    report = {
        "summary": summary,
        "class_reports": class_reports,
        "priority_mode": args.priority_mode,
        "frozen_rules": str(args.frozen_rules) if args.frozen_rules else None,
        "optimize_groups": sorted(f"{direction}:{target:.1f}" for direction, target in optimize_groups),
        "freeze_unlisted_groups": bool(args.freeze_unlisted_groups),
    }
    (out / "target_entry_class_report.json").write_text(json.dumps(report, indent=2))
    (out / "target_entry_classes.json").write_text(json.dumps({"entry_classes": kept_rules}, indent=2))
    with (out / "target_entry_population.csv").open("w", newline="") as f:
        fieldnames = list(selected_population[0].keys()) if selected_population else population_fieldnames
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if selected_population:
            writer.writerows(selected_population)
    with (out / "target_entry_class_summary.csv").open("w", newline="") as f:
        fieldnames = [
            "direction",
            "target_distance",
            "rule_count",
            "opportunity_count",
            "opportunity_density_per_hour",
            "selected_density_per_hour",
            "capture_rate",
            "objective_score",
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
            "equity_per_hour_at_2pct_risk",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows({k: row.get(k, "") for k in fieldnames} for row in summary)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
