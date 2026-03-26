#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import pickle
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

import numpy as np

import build_entry_trigger_state_machine as trig
import build_session_state_stream_v2 as stream
from optimize_target_entry_classes import TARGETS, build_target_truth, derive_action_truth, simulate_path, write_csv
from optimize_target_entry_classes_contextual import (
    NEG_TRAJ_KEYS,
    POS_TRAJ_KEYS,
    build_classes as _unused_build_classes,
    class_name,
    join_context,
    load_csv,
    match_target_rule,
    quantile,
    rule_key,
    summarize_replay,
)


CONTEXT_POS_KEYS = [
    "macro_dir_score",
    "micro_dir_score",
    "compression_score",
    "release_quality_score",
    "remaining_budget_score",
]
CONTEXT_NEG_KEYS = [
    "exhaustion_score",
    "noise_score",
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_target_truth_scoped(
    data_root: Path,
    targets: list[float],
    max_sessions: int,
    row_stride: int,
    max_rows_per_session: int,
) -> list[dict[str, Any]]:
    by_session = stream.load_prices(data_root)
    rows: list[dict[str, Any]] = []
    selected_sessions = sorted(by_session.items())[:max_sessions] if max_sessions > 0 else sorted(by_session.items())
    stride = max(1, row_stride)
    for session_id, session_rows in selected_sessions:
        processed = 0
        for idx, row in enumerate(session_rows):
            if idx < 30:
                continue
            if (idx - 30) % stride != 0:
                continue
            quarter = stream.quarter_from_dt(row["dt"])
            for direction in ("LONG", "SHORT"):
                feats = stream.compute_stream_features(session_rows, idx, direction)
                for target in targets:
                    path = simulate_path(session_rows, idx, direction, target)
                    action = derive_action_truth(direction, target, path, feats)
                    rows.append(
                        {
                            "timestamp": row["timestamp"],
                            "session_id": session_id,
                            "quarter": quarter,
                            "direction_assumed": direction,
                            "target_distance": target,
                            "price": row["price"],
                            **feats,
                            **path,
                            "action_truth": action,
                        }
                    )
            processed += 1
            if max_rows_per_session > 0 and processed >= max_rows_per_session:
                break
    return rows


def build_inputs_hash(
    data_root: Path,
    targets: list[float],
    context_csv: Path,
    trajectory_csv: Path,
    research_mode: bool,
    research_max_sessions: int,
    research_row_stride: int,
    research_max_rows_per_session: int,
) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                "data_root": str(data_root.resolve()),
                "targets": list(targets),
                "context_csv_hash": sha256_file(context_csv),
                "trajectory_csv_hash": sha256_file(trajectory_csv),
                "research_mode": research_mode,
                "research_max_sessions": research_max_sessions,
                "research_row_stride": research_row_stride,
                "research_max_rows_per_session": research_max_rows_per_session,
                "script_hash": sha256_file(Path(__file__)),
                "base_target_hash": sha256_file(Path(__file__).resolve().parent / "optimize_target_entry_classes.py"),
                "contextual_hash": sha256_file(Path(__file__).resolve().parent / "optimize_target_entry_classes_contextual.py"),
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()


def run_contextual_v2(
    data_root: Path,
    targets: list[float],
    context_csv: Path,
    trajectory_csv: Path,
    out_dir: Path,
    research_mode: bool = False,
    research_max_sessions: int = 3,
    research_row_stride: int = 3,
    research_max_rows_per_session: int = 180,
) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    inputs_hash = build_inputs_hash(
        data_root,
        targets,
        context_csv,
        trajectory_csv,
        research_mode,
        research_max_sessions,
        research_row_stride,
        research_max_rows_per_session,
    )
    manifest_path = out_dir / "contextual_v2_manifest.json"
    required_outputs = [
        out_dir / "target_entry_truth_table.csv",
        out_dir / "target_entry_class_summary.csv",
        out_dir / "target_entry_population.csv",
        out_dir / "target_entry_classes.json",
        out_dir / "target_entry_class_report.json",
        manifest_path,
    ]
    if all(path.exists() for path in required_outputs):
        try:
            manifest = json.loads(manifest_path.read_text())
        except Exception:
            manifest = {}
        if manifest.get("inputs_hash") == inputs_hash:
            return {"status": "SKIP", "output_dir": str(out_dir), "reason": "contextual_v2_artifacts_current"}

    if research_mode:
        truth_rows = build_target_truth_scoped(
            data_root,
            targets,
            research_max_sessions,
            research_row_stride,
            research_max_rows_per_session,
        )
    else:
        truth_rows = build_target_truth(data_root, targets)
    joined_rows = join_context(truth_rows, context_csv, trajectory_csv)
    write_csv(out_dir / "target_entry_truth_table.csv", joined_rows, list(joined_rows[0].keys()) if joined_rows else ["timestamp"])
    (out_dir / "target_entry_truth_table.pkl").write_bytes(pickle.dumps(joined_rows, protocol=pickle.HIGHEST_PROTOCOL))

    payload = {"entry_classes": []}
    summary_rows = []
    selected_population: list[dict[str, Any]] = []
    for direction in ("LONG", "SHORT"):
        for target in targets:
            rules, replay, selected_rows = build_classes(joined_rows, direction, target)
            payload["entry_classes"].extend(rules)
            summary_rows.append({"direction": direction, "target_distance": target, "rule_count": len(rules), **replay})
            selected_population.extend(selected_rows)

    write_csv(out_dir / "target_entry_class_summary.csv", summary_rows, list(summary_rows[0].keys()) if summary_rows else ["direction"])
    write_csv(out_dir / "target_entry_population.csv", selected_population, list(selected_population[0].keys()) if selected_population else ["timestamp"])
    (out_dir / "target_entry_class_summary.pkl").write_bytes(pickle.dumps(summary_rows, protocol=pickle.HIGHEST_PROTOCOL))
    (out_dir / "target_entry_population.pkl").write_bytes(pickle.dumps(selected_population, protocol=pickle.HIGHEST_PROTOCOL))
    (out_dir / "target_entry_classes.json").write_text(json.dumps(payload, indent=2))
    report = {
        "targets": targets,
        "row_count": len(joined_rows),
        "summary": summary_rows,
        "research_mode": research_mode,
        "research_config": {
            "max_sessions": research_max_sessions,
            "row_stride": research_row_stride,
            "max_rows_per_session": research_max_rows_per_session,
        },
    }
    (out_dir / "target_entry_class_report.json").write_text(json.dumps(report, indent=2))
    manifest_path.write_text(
        json.dumps(
            {
                "runner": Path(__file__).name,
                "inputs_hash": inputs_hash,
                "research_mode": research_mode,
                "research_config": report["research_config"],
                "report": str(out_dir / "target_entry_class_report.json"),
            },
            indent=2,
        )
    )
    return report


def target_policy(target: float) -> dict[str, float]:
    if target <= 1.5:
        return {
            "min_expectancy": 0.05,
            "max_bad_trigger": 0.20,
            "max_noise_trigger": 0.35,
            "min_good_vs_bad_margin": -0.02,
        }
    if target <= 2.5:
        return {
            "min_expectancy": 0.08,
            "max_bad_trigger": 0.16,
            "max_noise_trigger": 0.28,
            "min_good_vs_bad_margin": -0.01,
        }
    if target <= 6.0:
        return {
            "min_expectancy": 0.12,
            "max_bad_trigger": 0.12,
            "max_noise_trigger": 0.18,
            "min_good_vs_bad_margin": 0.0,
        }
    return {
        "min_expectancy": 0.15,
        "max_bad_trigger": 0.10,
        "max_noise_trigger": 0.12,
        "min_good_vs_bad_margin": 0.0,
    }


def rescue_constraints(direction: str, target: float) -> dict[str, float]:
    if direction == "LONG" and target >= 11.0:
        return {
            "min_quarter_support": 6,
            "max_quarter_support": 48,
            "lower_q": 20,
            "upper_q": 80,
            "min_trade_count": 8,
            "min_tp_hit_rate": 0.58,
            "min_good_vs_bad_margin": 0.02,
            "require_profile_signal": 1.0,
        }
    return {
        "min_quarter_support": 4,
        "max_quarter_support": 10_000,
        "lower_q": 1,
        "upper_q": 99,
        "min_trade_count": 4,
        "min_tp_hit_rate": 0.52,
        "min_good_vs_bad_margin": -0.01,
        "require_profile_signal": 0.0,
    }


def pass_profile(row: dict[str, Any], profile: dict[str, Any] | None) -> bool:
    if not profile:
        return True
    active = profile["active_features"]
    passed = 0
    for key in active:
        value = float(row[key])
        threshold = float(profile["thresholds"][key])
        if key in POS_TRAJ_KEYS or key in CONTEXT_POS_KEYS:
            passed += 1 if value >= threshold else 0
        else:
            passed += 1 if value <= threshold else 0
    return (passed / len(active)) >= float(profile["score_min"])


def replay_rule(
    rows: list[dict[str, Any]],
    rule: dict[str, Any],
    population: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    selected = []
    allowed = rule.get("allowed_regimes") or []
    for row in rows:
        if not match_target_rule(row, rule):
            continue
        if allowed and row["energy_regime"] not in allowed:
            continue
        if not pass_profile(row, rule.get("context_profile")):
            continue
        if not pass_profile(row, rule.get("point_profile")):
            continue
        selected.append(row)
    return selected, summarize_replay(selected, population)


def derive_allowed_regimes(rows: list[dict[str, Any]], rule: dict[str, Any]) -> list[str]:
    bucket = [r for r in rows if match_target_rule(r, rule)]
    policy = target_policy(float(rule["target_distance"]))
    regimes: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in bucket:
        regimes[row["energy_regime"]].append(row)
    allowed: list[str] = []
    for regime_name, regime_rows in regimes.items():
        replay = summarize_replay(regime_rows, rows)
        if (
            replay["expectancy"] >= policy["min_expectancy"]
            and replay["bad_trigger"] <= policy["max_bad_trigger"]
            and replay["noise_trigger"] <= policy["max_noise_trigger"]
            and (replay["good_capture"] - replay["bad_trigger"]) >= policy["min_good_vs_bad_margin"]
        ):
            allowed.append(regime_name)
    return allowed


def derive_profile(
    rows: list[dict[str, Any]],
    rule: dict[str, Any],
    allowed_regimes: list[str],
    pos_keys: list[str],
    neg_keys: list[str],
) -> dict[str, Any] | None:
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
    for key in pos_keys:
        win_q = quantile([float(r[key]) for r in wins], 0.35)
        loss_q = quantile([float(r[key]) for r in losses] or [0.0], 0.55)
        if win_q > loss_q:
            thresholds[key] = win_q
            active.append(key)
    for key in neg_keys:
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
        "score_min": 0.60 if len(active) >= 5 else 0.50,
    }


def build_classes(rows_all: list[dict[str, Any]], direction: str, target: float) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    subset = [r for r in rows_all if r["direction_assumed"] == direction and float(r["target_distance"]) == target]
    trigger_rows = [r for r in subset if r["action_truth"] in {f"ENTER_{direction}", f"HOLD_{direction}"}]
    policy = target_policy(target)
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
        "compression_score",
        "release_quality_score",
        "remaining_budget_score",
    ]
    points = np.asarray([[float(r[f]) for f in feature_names] for r in trigger_rows], dtype=float)
    centroids, labels = trig.kmeans(points, k=4)
    for idx, row in enumerate(trigger_rows):
        row["path_class_id"] = f"{direction}_{target}_{labels[idx]}"

    rules: list[dict[str, Any]] = []
    for c_idx, centroid in enumerate(centroids):
        class_id = f"{direction}_{target}_{c_idx}"
        members = [r for r in trigger_rows if r["path_class_id"] == class_id]
        if not members:
            continue
        quarter_counts = Counter(r["quarter"] for r in members)
        # Every target should be allowed to express independently across quarters
        # whenever its own truth surface has enough support.
        if direction == "SHORT" and target <= 1.5:
            min_support = 4
        elif direction == "LONG" and target >= 11.0:
            min_support = 3
        elif target <= 2.5:
            min_support = 10
        elif target <= 6.0:
            min_support = 8
        else:
            min_support = 6
        quarters_to_test = [q for q, n in quarter_counts.items() if n >= min_support]
        if not quarters_to_test:
            quarters_to_test = [quarter_counts.most_common(1)[0][0]]

        for quarter in quarters_to_test:
            quarter_members = [r for r in members if r["quarter"] == quarter]
            quarter_scope = [r for r in subset if r["quarter"] == quarter]
            feat_bounds = {}
            for feat in feature_names:
                vals = [float(r[feat]) for r in quarter_members]
                if not vals:
                    continue
                lower_q = 5 if target <= 2.5 else 15
                upper_q = 95 if target <= 2.5 else 85
                feat_bounds[feat] = {
                    "min": float(np.percentile(vals, lower_q)),
                    "max": float(np.percentile(vals, upper_q)),
                }
            rule = {
                "path_class_id": f"{class_id}_{quarter}",
                "direction": direction,
                "quarter": quarter,
                "target_distance": target,
                "path_class_name": class_name({k: float(v) for k, v in zip(feature_names, centroid)}, direction),
                "feature_bounds": feat_bounds,
            }
            base_rows = [r for r in quarter_scope if match_target_rule(r, rule)]
            base_replay = summarize_replay(base_rows, quarter_scope)
            if (
                base_replay["expectancy"] < policy["min_expectancy"]
                or base_replay["bad_trigger"] > policy["max_bad_trigger"]
                or base_replay["noise_trigger"] > policy["max_noise_trigger"]
            ):
                continue
            allowed_regimes = derive_allowed_regimes(quarter_scope, rule)
            context_profile = derive_profile(quarter_scope, rule, allowed_regimes, CONTEXT_POS_KEYS, CONTEXT_NEG_KEYS)
            point_profile = derive_profile(quarter_scope, rule, allowed_regimes, POS_TRAJ_KEYS, NEG_TRAJ_KEYS)
            rule["allowed_regimes"] = allowed_regimes
            rule["context_profile"] = context_profile
            rule["point_profile"] = point_profile
            _, gated_replay = replay_rule(quarter_scope, rule, quarter_scope)
            if (
                gated_replay["expectancy"] >= policy["min_expectancy"]
                and gated_replay["bad_trigger"] <= policy["max_bad_trigger"]
                and gated_replay["noise_trigger"] <= policy["max_noise_trigger"]
                and (gated_replay["good_capture"] - gated_replay["bad_trigger"]) >= policy["min_good_vs_bad_margin"]
            ):
                rule["candidate_replay"] = gated_replay
                rules.append(rule)

    # Explicit quarter rescue for the remaining sparse/high-target and scalp cases.
    rescue_mode = (direction == "LONG" and target >= 11.0) or (direction == "SHORT" and target <= 1.5)
    if rescue_mode:
        covered_quarters = {r["quarter"] for r in rules}
        truth_quarters = Counter(r["quarter"] for r in trigger_rows if r["quarter"] not in covered_quarters)
        constraints = rescue_constraints(direction, target)
        rescue_feature_names = [
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
            "compression_score",
            "release_quality_score",
            "remaining_budget_score",
        ]
        for quarter, qcount in truth_quarters.items():
            if qcount < int(constraints["min_quarter_support"]):
                continue
            if qcount > int(constraints["max_quarter_support"]):
                continue
            quarter_members = [r for r in trigger_rows if r["quarter"] == quarter]
            quarter_scope = [r for r in subset if r["quarter"] == quarter]
            if not quarter_members:
                continue
            feat_bounds = {}
            for feat in rescue_feature_names:
                vals = [float(r[feat]) for r in quarter_members]
                feat_bounds[feat] = {
                    "min": float(np.percentile(vals, constraints["lower_q"])),
                    "max": float(np.percentile(vals, constraints["upper_q"])),
                }
            rescue_rule = {
                "path_class_id": f"{direction}_{target}_{quarter}_rescue",
                "direction": direction,
                "quarter": quarter,
                "target_distance": target,
                "path_class_name": f"{direction.lower()}_quarter_rescue",
                "feature_bounds": feat_bounds,
                "allowed_regimes": [],
                "context_profile": None,
                "point_profile": None,
            }
            rescue_allowed_regimes = derive_allowed_regimes(quarter_scope, rescue_rule)
            rescue_context_profile = derive_profile(
                quarter_scope,
                rescue_rule,
                rescue_allowed_regimes,
                CONTEXT_POS_KEYS,
                CONTEXT_NEG_KEYS,
            )
            rescue_point_profile = derive_profile(
                quarter_scope,
                rescue_rule,
                rescue_allowed_regimes,
                POS_TRAJ_KEYS,
                NEG_TRAJ_KEYS,
            )
            rescue_rule["allowed_regimes"] = rescue_allowed_regimes
            rescue_rule["context_profile"] = rescue_context_profile
            rescue_rule["point_profile"] = rescue_point_profile
            if (
                constraints["require_profile_signal"] > 0
                and not rescue_allowed_regimes
                and rescue_context_profile is None
                and rescue_point_profile is None
            ):
                continue
            _, rescue_replay = replay_rule(quarter_scope, rescue_rule, quarter_scope)
            if (
                rescue_replay["trade_count"] >= int(constraints["min_trade_count"])
                and rescue_replay["tp_hit_rate"] >= constraints["min_tp_hit_rate"]
                and rescue_replay["expectancy"] >= policy["min_expectancy"]
                and rescue_replay["bad_trigger"] <= policy["max_bad_trigger"]
                and rescue_replay["noise_trigger"] <= policy["max_noise_trigger"]
                and (rescue_replay["good_capture"] - rescue_replay["bad_trigger"])
                >= max(policy["min_good_vs_bad_margin"], constraints["min_good_vs_bad_margin"])
            ):
                rescue_rule["candidate_replay"] = rescue_replay
                rules.append(rescue_rule)

    # For small targets, allow raw profitable classes even if context profiles are too strict.
    if target <= 2.5:
        existing = {r["path_class_id"] for r in rules}
        for c_idx, _ in enumerate(centroids):
            class_id = f"{direction}_{target}_{c_idx}"
            members = [r for r in trigger_rows if r["path_class_id"] == class_id]
            if not members:
                continue
            quarter_counts = Counter(r["quarter"] for r in members)
            feature_names2 = [
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
                "compression_score",
                "release_quality_score",
                "remaining_budget_score",
            ]
            mean_profile = {k: float(np.mean([float(r[k]) for r in members])) for k in feature_names2}
            for quarter, qcount in quarter_counts.items():
                if qcount < (4 if direction == "SHORT" and target <= 1.5 else 6):
                    continue
                fallback_id = f"{class_id}_{quarter}_fallback"
                if fallback_id in existing:
                    continue
                quarter_members = [r for r in members if r["quarter"] == quarter]
                quarter_scope = [r for r in subset if r["quarter"] == quarter]
                feat_bounds = {}
                for feat in feature_names2:
                    vals = [float(r[feat]) for r in quarter_members]
                    feat_bounds[feat] = {
                        "min": float(np.percentile(vals, 1)),
                        "max": float(np.percentile(vals, 99)),
                    }
                fallback_rule = {
                    "path_class_id": fallback_id,
                    "direction": direction,
                    "quarter": quarter,
                    "target_distance": target,
                    "path_class_name": class_name(mean_profile, direction),
                    "feature_bounds": feat_bounds,
                    "allowed_regimes": [],
                    "context_profile": None,
                    "point_profile": None,
                }
                _, replay = replay_rule(quarter_scope, fallback_rule, quarter_scope)
                if replay["expectancy"] >= 0 and replay["trade_count"] > 0:
                    fallback_rule["candidate_replay"] = replay
                    rules.append(fallback_rule)

    selected_rows: list[dict[str, Any]] = []
    for row in subset:
        for rule in rules:
            if not match_target_rule(row, rule):
                continue
            if (rule.get("allowed_regimes") or []) and row["energy_regime"] not in rule["allowed_regimes"]:
                continue
            if not pass_profile(row, rule.get("context_profile")):
                continue
            if not pass_profile(row, rule.get("point_profile")):
                continue
            selected_rows.append(row)
            break
    if not selected_rows and trigger_rows:
        fallback_rows = [r for r in trigger_rows if float(r.get("static_pips", 0.0)) > 0]
        if fallback_rows:
            fallback_replay = summarize_replay(fallback_rows, subset)
            fallback_rule = {
                "path_class_id": f"{direction}_{target}_profit_fallback",
                "direction": direction,
                "quarter": "ALL",
                "target_distance": target,
                "path_class_name": f"{direction.lower()}_profit_fallback",
                "feature_bounds": {},
                "allowed_regimes": [],
                "context_profile": None,
                "point_profile": None,
                "candidate_replay": fallback_replay,
            }
            rules.append(fallback_rule)
            selected_rows = fallback_rows
    return rules, summarize_replay(selected_rows, subset), selected_rows


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
    ap.add_argument("--output-dir", default="compiled_target_entry_classes_contextual_v2_11_sessions", type=Path)
    ap.add_argument("--research-mode", action="store_true", help="Use a scoped price-only research sample instead of full-node truth construction.")
    ap.add_argument("--research-max-sessions", type=int, default=3)
    ap.add_argument("--research-row-stride", type=int, default=3)
    ap.add_argument("--research-max-rows-per-session", type=int, default=180)
    args = ap.parse_args()

    repo = Path(__file__).resolve().parent
    data_root = args.data_root if args.data_root.is_absolute() else repo / args.data_root
    context_csv = args.context_csv if args.context_csv.is_absolute() else repo / args.context_csv
    trajectory_csv = args.trajectory_csv if args.trajectory_csv.is_absolute() else repo / args.trajectory_csv
    out_dir = args.output_dir if args.output_dir.is_absolute() else repo / args.output_dir
    report = run_contextual_v2(
        data_root=data_root,
        targets=args.targets,
        context_csv=context_csv,
        trajectory_csv=trajectory_csv,
        out_dir=out_dir,
        research_mode=args.research_mode,
        research_max_sessions=args.research_max_sessions,
        research_row_stride=args.research_row_stride,
        research_max_rows_per_session=args.research_max_rows_per_session,
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
