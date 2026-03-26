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


HOURS_TOTAL = 11 * 8.0


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def euclidean(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.sqrt(((a - b) ** 2).sum()))


def kmeans(points: np.ndarray, k: int, iterations: int = 30) -> tuple[np.ndarray, np.ndarray]:
    if len(points) == 0:
        return np.empty((0, points.shape[1] if points.ndim == 2 else 0)), np.array([], dtype=int)
    k = min(k, len(points))
    centroids = points[np.linspace(0, len(points) - 1, k, dtype=int)].copy()
    labels = np.zeros(len(points), dtype=int)
    for _ in range(iterations):
        for i, p in enumerate(points):
            labels[i] = int(np.argmin([euclidean(p, c) for c in centroids]))
        new_centroids = centroids.copy()
        for c in range(k):
            members = points[labels == c]
            if len(members):
                new_centroids[c] = members.mean(axis=0)
        if np.allclose(new_centroids, centroids):
            break
        centroids = new_centroids
    return centroids, labels


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


def class_name(center: dict[str, float], direction: str) -> str:
    if center["pressure_5"] > 0.25 and center["breakout_distance"] > 0.5:
        return f"{direction.lower()}_breakout_continuation"
    if center["pressure_ratio_5_15"] > 0.15 and center["compression"] < 0.45:
        return f"{direction.lower()}_compression_release"
    if center["signed_close_position_5"] > 0.65 and center["quarter_relative_bias"] > 0.0:
        return f"{direction.lower()}_trend_follow"
    return f"{direction.lower()}_reclaim_continuation"


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    if len(vals) == 1:
        return vals[0]
    idx = q * (len(vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(vals) - 1)
    frac = idx - lo
    return vals[lo] * (1 - frac) + vals[hi] * frac


def match_rule(row: dict[str, Any], rule: dict[str, Any]) -> bool:
    if row["direction_assumed"] != rule["direction"]:
        return False
    if row["quarter"] != rule["quarter"]:
        return False
    for feat, bounds in rule["feature_bounds"].items():
        val = float(row[feat])
        if val < bounds["min"] or val > bounds["max"]:
            return False
    return True


def merge_same_family_rules(
    rules_list: list[dict[str, Any]],
    rows_all: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged_groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for rule in rules_list:
        key = (rule["direction"], rule["quarter"], rule["path_class_name"])
        merged_groups[key].append(rule)

    merged_rules: list[dict[str, Any]] = []
    for (direction, quarter, class_name_value), group in merged_groups.items():
        if len(group) == 1:
            merged_rules.append(group[0])
            continue

        feature_names = list(group[0]["feature_bounds"].keys())
        union_rule = {
            "path_class_id": "+".join(sorted({g["path_class_id"] for g in group})),
            "direction": direction,
            "quarter": quarter,
            "path_class_name": class_name_value,
            "feature_bounds": {},
        }
        for feat in feature_names:
            union_rule["feature_bounds"][feat] = {
                "min": min(g["feature_bounds"][feat]["min"] for g in group),
                "max": max(g["feature_bounds"][feat]["max"] for g in group),
            }

        quarter_scope = [
            r for r in rows_all
            if r["direction_assumed"] == direction and r["quarter"] == quarter
        ]
        union_rows = [r for r in quarter_scope if match_rule(r, union_rule)]
        union_replay = summarize_replay(union_rows, quarter_scope)
        union_rule["candidate_replay"] = union_replay

        best_single = max(
            group,
            key=lambda g: (
                g["candidate_replay"]["expectancy"],
                g["candidate_replay"]["good_capture"],
                -g["candidate_replay"]["bad_trigger"],
            ),
        )
        best_single_replay = best_single["candidate_replay"]

        if (
            union_replay["expectancy"] >= max(0.12, best_single_replay["expectancy"] - 0.02)
            and union_replay["bad_trigger"] <= max(0.16, best_single_replay["bad_trigger"] + 0.02)
            and union_replay["noise_trigger"] <= max(0.16, best_single_replay["noise_trigger"] + 0.02)
            and union_replay["good_capture"] >= best_single_replay["good_capture"]
        ):
            merged_rules.append(union_rule)
        else:
            merged_rules.extend(group)

    return merged_rules


def expand_rule(
    base_rule: dict[str, Any],
    rows_scope: list[dict[str, Any]],
    min_expectancy: float = 0.15,
    max_bad_trigger: float = 0.08,
    max_noise_trigger: float = 0.12,
) -> dict[str, Any]:
    feature_names = list(base_rule["feature_bounds"].keys())
    best_rule = json.loads(json.dumps(base_rule))
    best_rows = [r for r in rows_scope if match_rule(r, best_rule)]
    best_replay = summarize_replay(best_rows, rows_scope)
    deltas = [0.05, 0.10, 0.15]
    improved = True
    while improved:
        improved = False
        candidate_best = None
        candidate_best_replay = None
        for feat in feature_names:
            for delta in deltas:
                candidate = json.loads(json.dumps(best_rule))
                candidate["feature_bounds"][feat]["min"] -= delta
                candidate["feature_bounds"][feat]["max"] += delta
                cand_rows = [r for r in rows_scope if match_rule(r, candidate)]
                cand_replay = summarize_replay(cand_rows, rows_scope)
                if (
                    cand_replay["expectancy"] >= min_expectancy
                    and cand_replay["bad_trigger"] <= max_bad_trigger
                    and cand_replay["noise_trigger"] <= max_noise_trigger
                ):
                    if (
                        candidate_best_replay is None
                        or cand_replay["good_capture"] > candidate_best_replay["good_capture"]
                        or (
                            cand_replay["good_capture"] == candidate_best_replay["good_capture"]
                            and cand_replay["expectancy"] > candidate_best_replay["expectancy"]
                        )
                    ):
                        candidate_best = candidate
                        candidate_best_replay = cand_replay
        if candidate_best is not None and (
            candidate_best_replay["good_capture"] > best_replay["good_capture"]
            or candidate_best_replay["trade_count"] > best_replay["trade_count"]
        ):
            best_rule = candidate_best
            best_replay = candidate_best_replay
            improved = True
    best_rule["candidate_replay"] = best_replay
    return best_rule


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--state-truth-csv", required=True, type=Path)
    ap.add_argument("--output-dir", required=True, type=Path)
    args = ap.parse_args()

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    rows_all = load_csv(args.state_truth_csv)
    trigger_rows = [r for r in rows_all if r["action_truth"] in {"ENTER_LONG", "ENTER_SHORT", "HOLD_LONG", "HOLD_SHORT"}]
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
    ]

    path_class_rows: list[dict[str, Any]] = []
    path_class_summary: dict[str, Any] = {"classes": []}
    rules: dict[str, Any] = {"logic": "stream_trigger_state_machine_v2", "path_classes": []}

    for direction in ("LONG", "SHORT"):
        direction_rows = [r for r in trigger_rows if r["direction_assumed"] == direction]
        points = np.asarray([[float(r[f]) for f in feature_names] for r in direction_rows], dtype=float)
        centroids, labels = kmeans(points, k=4)
        for idx, row in enumerate(direction_rows):
            row["path_class_id"] = f"{direction}_{labels[idx]}"
            path_class_rows.append(row)
        for c_idx, centroid in enumerate(centroids):
            members = [r for r in direction_rows if r["path_class_id"] == f"{direction}_{c_idx}"]
            quarter_counts = Counter(r["quarter"] for r in members)
            dominant_quarter = quarter_counts.most_common(1)[0][0] if quarter_counts else "Q1"
            center = {feature_names[i]: float(centroid[i]) for i in range(len(feature_names))}
            cls_name = class_name(center, direction)
            class_summary = {
                "path_class_id": f"{direction}_{c_idx}",
                "path_class_name": cls_name,
                "direction": direction,
                "row_count": len(members),
                "action_counts": dict(Counter(r["action_truth"] for r in members)),
                "outcome_counts": dict(Counter(r["outcome_label"] for r in members)),
                "quarter_counts": dict(quarter_counts),
                "dominant_quarter": dominant_quarter,
                "center": center,
                "expectancy": mean(float(r["static_pips"]) for r in members) if members else 0.0,
            }
            path_class_summary["classes"].append(class_summary)
            if cls_name.endswith(("breakout_continuation", "compression_release", "trend_follow")):
                selected_rule_features = [
                    "pressure_5",
                    "pressure_ratio_5_15",
                    "compression",
                    "breakout_distance",
                    "signed_close_position_5",
                    "recent_vol_10",
                ]
                profitable_quarter_rules: list[dict[str, Any]] = []
                for quarter in ("Q1", "Q2", "Q3", "Q4"):
                    quarter_members = [r for r in members if r["quarter"] == quarter]
                    if len(quarter_members) < 25:
                        continue
                    feature_bounds = {}
                    for feat in selected_rule_features:
                        vals = [float(r[feat]) for r in quarter_members]
                        feature_bounds[feat] = {
                            "min": percentile(vals, 0.15),
                            "max": percentile(vals, 0.85),
                        }
                    candidate_rule = {
                        "path_class_id": f"{direction}_{c_idx}",
                        "direction": direction,
                        "quarter": quarter,
                        "path_class_name": cls_name,
                        "feature_bounds": feature_bounds,
                    }
                    quarter_scope = [
                        r for r in rows_all
                        if r["direction_assumed"] == direction and r["quarter"] == quarter
                    ]
                    candidate_rows = [r for r in quarter_scope if match_rule(r, candidate_rule)]
                    candidate_replay = summarize_replay(candidate_rows, quarter_scope)
                    candidate_rule["candidate_replay"] = candidate_replay
                    if (
                        candidate_replay["expectancy"] > 0.0
                        and candidate_replay["good_capture"] > candidate_replay["bad_trigger"]
                        and candidate_replay["bad_trigger"] <= 0.16
                        and candidate_replay["noise_trigger"] <= 0.16
                    ):
                        profitable_quarter_rules.append(expand_rule(candidate_rule, quarter_scope))

                profitable_quarter_rules.sort(
                    key=lambda r: (
                        r["candidate_replay"]["expectancy"],
                        r["candidate_replay"]["good_capture"],
                        -r["candidate_replay"]["bad_trigger"],
                    ),
                    reverse=True,
                )

                kept_for_class: list[dict[str, Any]] = []
                for rule in profitable_quarter_rules:
                    duplicate = False
                    for kept in kept_for_class:
                        overlap_feats = 0
                        for feat in selected_rule_features:
                            a = rule["feature_bounds"][feat]
                            b = kept["feature_bounds"][feat]
                            overlaps = min(a["max"], b["max"]) >= max(a["min"], b["min"])
                            overlap_feats += int(overlaps)
                        if kept["quarter"] == rule["quarter"] and overlap_feats >= len(selected_rule_features) - 1:
                            duplicate = True
                            break
                    if not duplicate:
                        kept_for_class.append(rule)
                rules["path_classes"].extend(kept_for_class)

    rules["path_classes"] = merge_same_family_rules(rules["path_classes"], rows_all)

    # replay using derived rules
    selected_rows: list[dict[str, Any]] = []
    for row in rows_all:
        for rule in rules["path_classes"]:
            if not match_rule(row, rule):
                continue
            selected_rows.append(row)
            break

    replay = summarize_replay(selected_rows, rows_all)

    write_csv(
        out_dir / "path_class_state_rows.csv",
        path_class_rows,
        list(path_class_rows[0].keys()) if path_class_rows else ["timestamp"],
    )
    write_csv(
        out_dir / "entry_trigger_population.csv",
        selected_rows,
        list(selected_rows[0].keys()) if selected_rows else ["timestamp"],
    )

    (out_dir / "path_class_clusters.json").write_text(json.dumps(path_class_summary, indent=2))
    (out_dir / "entry_trigger_state_machine.json").write_text(json.dumps(rules, indent=2))
    (out_dir / "entry_trigger_replay_report.json").write_text(json.dumps(replay, indent=2))

    print(
        json.dumps(
            {
                "path_class_count": len(path_class_summary["classes"]),
                "selected_trades": replay["trade_count"],
                "good_capture": replay["good_capture"],
                "bad_trigger": replay["bad_trigger"],
                "noise_trigger": replay["noise_trigger"],
                "pips_per_hour": replay["pips_per_hour"],
            },
            indent=2,
        )
    )
