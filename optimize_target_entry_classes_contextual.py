#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

import numpy as np

import build_entry_trigger_state_machine as trig
import build_energy_regime_classifier as reg
import build_session_state_stream as stream
from optimize_target_entry_classes import TARGETS, build_target_truth, write_csv


HOURS_TOTAL = 11 * 8.0
POS_TRAJ_KEYS = [
    "pre_build_slope",
    "pre_build_accel",
    "pre_compression_release_delta",
    "pre_macro_micro_alignment",
    "release_to_exhaustion_delta",
    "post_continuation_persistence",
]
NEG_TRAJ_KEYS = [
    "post_noise_rise",
    "post_exhaustion_rise",
    "post_budget_decay",
]


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def summarize_replay(rows: list[dict[str, Any]], population: list[dict[str, Any]]) -> dict[str, Any]:
    wins = [r for r in rows if float(r["static_pips"]) > 0]
    losses = [r for r in rows if float(r["static_pips"]) < 0]
    total_pips = sum(float(r["static_pips"]) for r in rows)
    good = [r for r in rows if r["outcome_label"] == "GOOD"]
    bad = [r for r in rows if r["outcome_label"] == "BAD"]
    noise = [r for r in rows if r["outcome_label"] == "NOISE"]
    return {
        "trade_count": len(rows),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(rows) if rows else 0.0,
        "avg_win": mean(float(r["static_pips"]) for r in wins) if wins else 0.0,
        "avg_loss": mean(float(r["static_pips"]) for r in losses) if losses else 0.0,
        "expectancy": mean(float(r["static_pips"]) for r in rows) if rows else 0.0,
        "avg_R": mean(float(r["static_R"]) for r in rows) if rows else 0.0,
        "pips_per_hour": total_pips / HOURS_TOTAL if rows else 0.0,
        "good_capture": len(good) / max(1, sum(1 for r in population if r["outcome_label"] == "GOOD")),
        "bad_trigger": len(bad) / max(1, sum(1 for r in population if r["outcome_label"] == "BAD")),
        "noise_trigger": len(noise) / max(1, sum(1 for r in population if r["outcome_label"] == "NOISE")),
    }


def join_context(truth_rows: list[dict[str, Any]], context_csv: Path, trajectory_csv: Path) -> list[dict[str, Any]]:
    context_rows = load_csv(context_csv)
    traj_rows = load_csv(trajectory_csv)
    context_map = {
        (r["timestamp"], r["session_id"], r["quarter"], r["direction_assumed"]): r
        for r in context_rows
    }
    context_map_fallback = {
        (r["timestamp"], r["session_id"], r["direction_assumed"]): r
        for r in context_rows
    }
    traj_map = {
        (r["timestamp"], r["session_id"], r["quarter"], r["direction_assumed"]): r
        for r in traj_rows
    }
    traj_map_fallback = {
        (r["timestamp"], r["session_id"], r["direction_assumed"]): r
        for r in traj_rows
    }
    joined: list[dict[str, Any]] = []
    for row in truth_rows:
        key = (row["timestamp"], row["session_id"], row["quarter"], row["direction_assumed"])
        ctx = context_map.get(key)
        trj = traj_map.get(key)
        if ctx is None:
            fallback_key = (row["timestamp"], row["session_id"], row["direction_assumed"])
            ctx = context_map_fallback.get(fallback_key)
            trj = traj_map_fallback.get(fallback_key)
        if ctx is None:
            continue
        merged = {**row}
        for k in (
            "macro_dir_score",
            "micro_dir_score",
            "compression_score",
            "release_quality_score",
            "exhaustion_score",
            "noise_score",
            "remaining_budget_score",
            "energy_state",
            "energy_regime",
        ):
            merged[k] = ctx[k]
        if trj:
            for k in POS_TRAJ_KEYS + NEG_TRAJ_KEYS:
                merged[k] = trj[k]
        else:
            for k in POS_TRAJ_KEYS + NEG_TRAJ_KEYS:
                merged[k] = 0.0
        joined.append(merged)
    return joined


def class_name(center: dict[str, float], direction: str) -> str:
    if center["release_quality_score"] > 0.55 and center["compression"] > 0.45:
        return f"{direction.lower()}_compression_release"
    if center["macro_dir_score"] > 0.55 and center["micro_dir_score"] > 0.55:
        return f"{direction.lower()}_trend_follow"
    if center["breakout_distance"] > 0.45 and center["pressure_ratio_5_15"] > 0.10:
        return f"{direction.lower()}_breakout_continuation"
    return f"{direction.lower()}_reclaim_continuation"


def rule_key(rule: dict[str, Any]) -> str:
    return f"{rule['direction']}|{rule['target_distance']}|{rule['quarter']}|{rule['path_class_name']}|{rule['path_class_id']}"


def match_target_rule(row: dict[str, Any], rule: dict[str, Any]) -> bool:
    if row["direction_assumed"] != rule["direction"]:
        return False
    if float(row["target_distance"]) != float(rule["target_distance"]):
        return False
    if row["quarter"] != rule["quarter"]:
        return False
    for feat, bounds in rule["feature_bounds"].items():
        val = float(row[feat])
        if val < bounds["min"] or val > bounds["max"]:
            return False
    return True


def quantile(vals: list[float], q: float) -> float:
    if not vals:
        return 0.0
    vals = sorted(vals)
    idx = max(0, min(len(vals) - 1, int(round((len(vals) - 1) * q))))
    return vals[idx]


def derive_regime_allowed(rows: list[dict[str, Any]], rule: dict[str, Any]) -> list[str]:
    bucket = [r for r in rows if match_target_rule(r, rule)]
    regimes = defaultdict(list)
    for row in bucket:
        regimes[row["energy_regime"]].append(row)
    keep: list[str] = []
    for regime_name, regime_rows in regimes.items():
        replay = summarize_replay(regime_rows, rows)
        if replay["expectancy"] > 0.10 and replay["good_capture"] > replay["bad_trigger"]:
            keep.append(regime_name)
    return keep


def derive_point_profile(rows: list[dict[str, Any]], rule: dict[str, Any], allowed_regimes: list[str]) -> dict[str, Any] | None:
    bucket = [
        r for r in rows
        if match_target_rule(r, rule) and (not allowed_regimes or r["energy_regime"] in allowed_regimes)
    ]
    wins = [r for r in bucket if float(r["static_pips"]) > 0]
    losses = [r for r in bucket if float(r["static_pips"]) <= 0]
    if len(wins) < 6:
        return None
    thresholds: dict[str, float] = {}
    active: list[str] = []
    for key in POS_TRAJ_KEYS:
        win_q = quantile([float(r[key]) for r in wins], 0.35)
        loss_q = quantile([float(r[key]) for r in losses] or [0.0], 0.55)
        if win_q > loss_q:
            thresholds[key] = win_q
            active.append(key)
    for key in NEG_TRAJ_KEYS:
        win_q = quantile([float(r[key]) for r in wins], 0.65)
        loss_q = quantile([float(r[key]) for r in losses] or [0.0], 0.45)
        if win_q < loss_q:
            thresholds[key] = win_q
            active.append(key)
    if not active:
        return None
    return {
        "active_features": active,
        "thresholds": thresholds,
        "score_min": 0.6 if len(active) >= 5 else 0.5,
    }


def pass_profile(row: dict[str, Any], profile: dict[str, Any] | None) -> bool:
    if not profile:
        return True
    active = profile["active_features"]
    passed = 0
    for key in active:
        value = float(row[key])
        threshold = float(profile["thresholds"][key])
        if key in POS_TRAJ_KEYS:
            passed += 1 if value >= threshold else 0
        else:
            passed += 1 if value <= threshold else 0
    return (passed / len(active)) >= float(profile["score_min"])


def replay_rule(
    rows: list[dict[str, Any]],
    rule: dict[str, Any],
    population: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    allowed = rule.get("allowed_regimes") or []
    profile = rule.get("point_profile")
    selected = []
    for row in rows:
        if not match_target_rule(row, rule):
            continue
        if allowed and row["energy_regime"] not in allowed:
            continue
        if not pass_profile(row, profile):
            continue
        selected.append(row)
    return selected, summarize_replay(selected, population)


def merge_same_family_rules(rules: list[dict[str, Any]], population: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, float, str, str], list[dict[str, Any]]] = defaultdict(list)
    for rule in rules:
        grouped[(rule["direction"], float(rule["target_distance"]), rule["quarter"], rule["path_class_name"])].append(rule)

    merged: list[dict[str, Any]] = []
    for (_direction, _target, _quarter, _family), group in grouped.items():
        if len(group) == 1:
            merged.append(group[0])
            continue
        feature_names = list(group[0]["feature_bounds"].keys())
        union_rule = {
            "path_class_id": "+".join(sorted({g["path_class_id"] for g in group})),
            "direction": group[0]["direction"],
            "quarter": group[0]["quarter"],
            "target_distance": group[0]["target_distance"],
            "path_class_name": group[0]["path_class_name"],
            "feature_bounds": {},
            "allowed_regimes": sorted({regime for g in group for regime in (g.get("allowed_regimes") or [])}),
            "point_profile": None,
        }
        for feat in feature_names:
            union_rule["feature_bounds"][feat] = {
                "min": min(g["feature_bounds"][feat]["min"] for g in group),
                "max": max(g["feature_bounds"][feat]["max"] for g in group),
            }
        _, union_replay = replay_rule(population, union_rule, population)
        best_single = max(group, key=lambda g: (g["candidate_replay"]["expectancy"], g["candidate_replay"]["pips_per_hour"]))
        best_replay = best_single["candidate_replay"]
        if (
            union_replay["expectancy"] >= max(0.15, best_replay["expectancy"] - 0.10)
            and union_replay["bad_trigger"] <= max(0.08, best_replay["bad_trigger"] + 0.01)
            and union_replay["noise_trigger"] <= max(0.08, best_replay["noise_trigger"] + 0.01)
            and union_replay["trade_count"] >= best_replay["trade_count"]
        ):
            union_rule["candidate_replay"] = union_replay
            merged.append(union_rule)
        else:
            merged.extend(group)
    return merged


def expand_rule(rule: dict[str, Any], population: list[dict[str, Any]]) -> dict[str, Any]:
    feature_names = list(rule["feature_bounds"].keys())
    best = json.loads(json.dumps(rule))
    _, best_replay = replay_rule(population, best, population)
    deltas = [0.03, 0.06, 0.10]
    improved = True
    while improved:
        improved = False
        candidate_best = None
        candidate_best_replay = None
        for feat in feature_names:
            for delta in deltas:
                candidate = json.loads(json.dumps(best))
                candidate["feature_bounds"][feat]["min"] -= delta
                candidate["feature_bounds"][feat]["max"] += delta
                _, replay = replay_rule(population, candidate, population)
                if (
                    replay["expectancy"] >= 0.12
                    and replay["bad_trigger"] <= 0.10
                    and replay["noise_trigger"] <= 0.12
                ):
                    if (
                        candidate_best_replay is None
                        or replay["pips_per_hour"] > candidate_best_replay["pips_per_hour"]
                        or (
                            replay["pips_per_hour"] == candidate_best_replay["pips_per_hour"]
                            and replay["expectancy"] > candidate_best_replay["expectancy"]
                        )
                    ):
                        candidate_best = candidate
                        candidate_best_replay = replay
        if candidate_best and candidate_best_replay["pips_per_hour"] >= best_replay["pips_per_hour"]:
            best = candidate_best
            best_replay = candidate_best_replay
            improved = True
    best["candidate_replay"] = best_replay
    return best


def build_classes(rows_all: list[dict[str, Any]], direction: str, target: float) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    subset = [r for r in rows_all if r["direction_assumed"] == direction and float(r["target_distance"]) == target]
    trigger_rows = [r for r in subset if r["action_truth"] in {f"ENTER_{direction}", f"HOLD_{direction}"}]
    if len(trigger_rows) < 20:
        return [], summarize_replay([], subset), []

    feature_names = [
        "pressure_5",
        "pressure_15",
        "pressure_ratio_5_15",
        "quarter_relative_bias",
        "directional_dominance_qtd",
        "signed_close_position_5",
        "breakout_distance",
        "compression",
        "velocity_now",
        "recent_vol_10",
        "macro_dir_score",
        "micro_dir_score",
        "release_quality_score",
        "remaining_budget_score",
    ]
    points = np.asarray([[float(r[f]) for f in feature_names] for r in trigger_rows], dtype=float)
    centroids, labels = trig.kmeans(points, k=4)
    for idx, row in enumerate(trigger_rows):
        row["path_class_id"] = f"{direction}_{target}_{labels[idx]}"

    candidate_rules: list[dict[str, Any]] = []
    for c_idx, centroid in enumerate(centroids):
        class_id = f"{direction}_{target}_{c_idx}"
        members = [r for r in trigger_rows if r["path_class_id"] == class_id]
        if not members:
            continue
        quarter = Counter(r["quarter"] for r in members).most_common(1)[0][0]
        quarter_scope = [r for r in subset if r["quarter"] == quarter]
        feat_bounds = {}
        for feat in feature_names:
            vals = [float(r[feat]) for r in members]
            feat_bounds[feat] = {
                "min": float(np.percentile(vals, 15)),
                "max": float(np.percentile(vals, 85)),
            }
        rule = {
            "path_class_id": class_id,
            "direction": direction,
            "quarter": quarter,
            "target_distance": target,
            "path_class_name": class_name({k: float(v) for k, v in zip(feature_names, centroid)}, direction),
            "feature_bounds": feat_bounds,
        }
        base_rows = [r for r in quarter_scope if match_target_rule(r, rule)]
        base_replay = summarize_replay(base_rows, quarter_scope)
        if base_replay["expectancy"] <= 0 or base_replay["good_capture"] <= base_replay["bad_trigger"]:
            continue
        allowed_regimes = derive_regime_allowed(quarter_scope, rule)
        profile = derive_point_profile(quarter_scope, rule, allowed_regimes)
        gated_rows = [
            r for r in quarter_scope
            if match_target_rule(r, rule)
            and (not allowed_regimes or r["energy_regime"] in allowed_regimes)
            and pass_profile(r, profile)
        ]
        gated_replay = summarize_replay(gated_rows, quarter_scope)
        if gated_replay["expectancy"] > 0 and gated_replay["good_capture"] > gated_replay["bad_trigger"]:
            rule["allowed_regimes"] = allowed_regimes
            rule["point_profile"] = profile
            rule["candidate_replay"] = gated_replay
            candidate_rules.append(rule)

    candidate_rules = merge_same_family_rules(candidate_rules, subset)
    candidate_rules = [expand_rule(rule, subset) for rule in candidate_rules]
    candidate_rules.sort(key=lambda r: (r["candidate_replay"]["pips_per_hour"], r["candidate_replay"]["expectancy"]), reverse=True)

    selected_rows: list[dict[str, Any]] = []
    for row in subset:
        for rule in candidate_rules:
            if not match_target_rule(row, rule):
                continue
            allowed = rule.get("allowed_regimes") or []
            if allowed and row["energy_regime"] not in allowed:
                continue
            if not pass_profile(row, rule.get("point_profile")):
                continue
            selected_rows.append(row)
            break

    return candidate_rules, summarize_replay(selected_rows, subset), selected_rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", default="london_session_data_11", type=Path)
    ap.add_argument("--targets", nargs="*", type=float, default=TARGETS)
    ap.add_argument(
        "--context-csv",
        default="compiled_energy_context_11_sessions/regime_classifier/full_stream_regimes.csv",
        type=Path,
    )
    ap.add_argument(
        "--trajectory-csv",
        default="compiled_point_energy_trajectory_11_sessions/point_energy_trajectory.csv",
        type=Path,
    )
    ap.add_argument("--output-dir", default="compiled_target_entry_classes_contextual_11_sessions", type=Path)
    args = ap.parse_args()

    repo = Path(__file__).resolve().parent
    data_root = args.data_root if args.data_root.is_absolute() else repo / args.data_root
    context_csv = args.context_csv if args.context_csv.is_absolute() else repo / args.context_csv
    trajectory_csv = args.trajectory_csv if args.trajectory_csv.is_absolute() else repo / args.trajectory_csv
    out_dir = args.output_dir if args.output_dir.is_absolute() else repo / args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    truth_rows = build_target_truth(data_root, args.targets)
    joined_rows = join_context(truth_rows, context_csv, trajectory_csv)
    write_csv(out_dir / "target_entry_truth_table.csv", joined_rows, list(joined_rows[0].keys()) if joined_rows else ["timestamp"])

    summary_rows = []
    selected_population: list[dict[str, Any]] = []
    payload = {"entry_classes": []}
    for direction in ("LONG", "SHORT"):
        for target in args.targets:
            rules, replay, selected_rows = build_classes(joined_rows, direction, target)
            payload["entry_classes"].extend(rules)
            selected_population.extend(selected_rows)
            summary_rows.append({"direction": direction, "target_distance": target, "rule_count": len(rules), **replay})

    write_csv(out_dir / "target_entry_class_summary.csv", summary_rows, list(summary_rows[0].keys()) if summary_rows else ["direction"])
    write_csv(out_dir / "target_entry_population.csv", selected_population, list(selected_population[0].keys()) if selected_population else ["timestamp"])
    (out_dir / "target_entry_classes.json").write_text(json.dumps(payload, indent=2))
    report = {"targets": args.targets, "row_count": len(joined_rows), "summary": summary_rows}
    (out_dir / "target_entry_class_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
