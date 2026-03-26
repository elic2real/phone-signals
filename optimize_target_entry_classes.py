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

import build_session_state_stream as stream
import build_entry_trigger_state_machine as trig

PIP = 0.0001
SESSION_HOURS = 11 * 8.0
TARGETS = [1.5, 2.5, 4.5, 6.0, 7.0, 8.0, 9.0, 11.0, 13.0, 15.0]


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def precompute_forward_paths(session_rows: list[dict[str, Any]], idx: int, targets: list[float]) -> dict[str, dict[str, Any]]:
    start = session_rows[idx]["price"]
    pair = session_rows[idx].get("pair", "EUR_USD")
    max_target = max(float(target) for target in targets) if targets else 0.0
    path_state = {
        "LONG": {
            "mfe": 0.0,
            "mae": 0.0,
            "tp_hits": {float(target): None for target in targets},
            "sl_hits": {float(target): None for target in targets},
        },
        "SHORT": {
            "mfe": 0.0,
            "mae": 0.0,
            "tp_hits": {float(target): None for target in targets},
            "sl_hits": {float(target): None for target in targets},
        },
    }

    for fwd, row in enumerate(session_rows[idx + 1 :], start=1):
        long_sp = stream.signed_pips("LONG", start, row["price"], pair)
        short_sp = -long_sp

        for direction, signed_pips in (("LONG", long_sp), ("SHORT", short_sp)):
            state = path_state[direction]
            state["mfe"] = max(state["mfe"], signed_pips)
            state["mae"] = min(state["mae"], signed_pips)
            for target in state["tp_hits"]:
                if state["tp_hits"][target] is None and signed_pips >= target:
                    state["tp_hits"][target] = fwd
                if state["sl_hits"][target] is None and signed_pips <= -target:
                    state["sl_hits"][target] = fwd

        long_done = (
            path_state["LONG"]["tp_hits"].get(max_target) is not None
            and path_state["LONG"]["sl_hits"].get(max_target) is not None
        )
        short_done = (
            path_state["SHORT"]["tp_hits"].get(max_target) is not None
            and path_state["SHORT"]["sl_hits"].get(max_target) is not None
        )
        if fwd >= 100 and long_done and short_done:
            break

    return path_state


def derive_path_from_forward(forward_state: dict[str, Any], target: float) -> dict[str, Any]:
    target = float(target)
    mfe = float(forward_state["mfe"])
    mae = float(forward_state["mae"])
    tp_hit = forward_state["tp_hits"].get(target)
    sl_hit = forward_state["sl_hits"].get(target)
    if tp_hit is not None and (sl_hit is None or tp_hit <= sl_hit):
        outcome = "GOOD"
        static_pips = target
    elif sl_hit is not None and (tp_hit is None or sl_hit < tp_hit):
        outcome = "BAD"
        static_pips = -target
    else:
        outcome = "NOISE"
        static_pips = 0.0
    return {
        "future_mfe_pips": round(mfe, 6),
        "future_mae_pips": round(abs(mae), 6),
        "tp_hit_min": tp_hit or 0,
        "sl_hit_min": sl_hit or 0,
        "outcome_label": outcome,
        "static_pips": round(static_pips, 6),
        "static_R": round(static_pips / target, 6),
    }


def simulate_path(session_rows: list[dict[str, Any]], idx: int, direction: str, target: float) -> dict[str, Any]:
    forward = precompute_forward_paths(session_rows, idx, [target])
    return derive_path_from_forward(forward[direction], target)


def derive_action_truth(direction: str, target: float, path: dict[str, Any], feats: dict[str, float]) -> str:
    target_scale = target / 2.5
    fast_trigger = path["tp_hit_min"] and path["tp_hit_min"] <= max(8, int(round(10 * target_scale)))
    strong_transition = (
        feats["pressure_5"] > max(0.10, 0.18 - 0.02 * target_scale)
        and feats["pressure_ratio_5_15"] > max(0.02, 0.10 - 0.02 * target_scale)
        and feats["compression"] < min(0.75, 0.60 + 0.06 * target_scale)
        and feats["signed_close_position_5"] > 0.55
    )
    breakout_trigger = feats["breakout_distance"] > max(0.10, 0.20 * target_scale) and feats["velocity_now"] > -0.10
    hold_continuation = (
        feats["pressure_15"] > max(0.05, 0.12 - 0.015 * target_scale)
        and feats["velocity_3"] > -0.08
        and feats["quarter_relative_bias"] > -0.08
    )
    bias_aligned = feats["directional_dominance_qtd"] > 0.05
    trend_bias_trigger = (
        feats["pressure_15"] > max(0.08, 0.16 - 0.015 * target_scale)
        and feats["signed_close_position_5"] > 0.65
        and feats["compression"] < min(0.80, 0.58 + 0.05 * target_scale)
        and feats["recent_vol_10"] > max(0.30, 0.35 * target_scale)
    )

    if path["outcome_label"] == "GOOD":
        if fast_trigger and (strong_transition or breakout_trigger):
            return f"ENTER_{direction}"
        if bias_aligned and trend_bias_trigger and path["future_mfe_pips"] >= target * 1.25:
            return f"ENTER_{direction}"
        if hold_continuation and path["future_mfe_pips"] >= target * 1.4:
            return f"HOLD_{direction}"
        if path["future_mfe_pips"] >= target * 1.15:
            return f"HARVEST_{direction}"
        return "DO_NOT_ENTER"

    if path["outcome_label"] == "BAD":
        if (
            feats["pressure_5"] < -0.25
            or feats["pressure_ratio_5_15"] < -0.18
            or feats["velocity_now"] < -0.35
            or feats["velocity_change"] < -0.25
        ):
            return f"PANIC_{direction}"
        return "DO_NOT_ENTER"

    if bias_aligned and path["future_mfe_pips"] > target * 0.9 and feats["pressure_15"] > 0.08:
        return f"HARVEST_{direction}"
    return "DO_NOT_ENTER"


def build_target_truth(data_root: Path, targets: list[float]) -> list[dict[str, Any]]:
    by_session = stream.load_prices(data_root)
    rows: list[dict[str, Any]] = []
    targets = [float(target) for target in targets]
    for session_id, session_rows in sorted(by_session.items()):
        for idx, row in enumerate(session_rows):
            if idx < 30:
                continue
            quarter = stream.quarter_from_dt(row["dt"])
            forward_paths = precompute_forward_paths(session_rows, idx, targets)
            for direction in ("LONG", "SHORT"):
                feats = stream.compute_stream_features(session_rows, idx, direction)
                for target in targets:
                    path = derive_path_from_forward(forward_paths[direction], target)
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
    return rows


def build_classes_for_target(rows_all: list[dict[str, Any]], direction: str, target: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    subset = [r for r in rows_all if r["direction_assumed"] == direction and float(r["target_distance"]) == target]
    trigger_rows = [r for r in subset if r["action_truth"] in {f"ENTER_{direction}", f"HOLD_{direction}"}]
    if len(trigger_rows) < 20:
        return [], {"trade_count": 0, "wins": 0, "losses": 0, "win_rate": 0.0, "expectancy": 0.0, "avg_R": 0.0, "pips_per_hour": 0.0, "good_capture": 0.0, "bad_trigger": 0.0, "noise_trigger": 0.0}
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
    points = np.asarray([[float(r[f]) for f in feature_names] for r in trigger_rows], dtype=float)
    centroids, labels = trig.kmeans(points, k=4)
    for idx, row in enumerate(trigger_rows):
        row["path_class_id"] = f"{direction}_{labels[idx]}"
    candidate_rules: list[dict[str, Any]] = []
    for c_idx, centroid in enumerate(centroids):
        members = [r for r in trigger_rows if r["path_class_id"] == f"{direction}_{c_idx}"]
        if not members:
            continue
        quarter = Counter(r["quarter"] for r in members).most_common(1)[0][0]
        quarter_scope = [r for r in subset if r["quarter"] == quarter]
        feat_bounds = {}
        for feat in feature_names:
            vals = [float(r[feat]) for r in members]
            feat_bounds[feat] = {"min": float(np.percentile(vals, 15)), "max": float(np.percentile(vals, 85))}
        rule = {
            "path_class_id": f"{direction}_{c_idx}",
            "direction": direction,
            "quarter": quarter,
            "target_distance": target,
            "path_class_name": trig.class_name({k: float(v) for k, v in zip(feature_names, centroid)}, direction),
            "feature_bounds": feat_bounds,
        }
        matched = [r for r in quarter_scope if trig.match_rule(r, rule)]
        replay = trig.summarize_replay(matched, quarter_scope)
        if replay["expectancy"] > 0 and replay["good_capture"] > replay["bad_trigger"]:
            rule["candidate_replay"] = replay
            candidate_rules.append(rule)
    selected_rows = []
    for row in subset:
        for rule in candidate_rules:
            if trig.match_rule(row, rule):
                selected_rows.append(row)
                break
    replay = trig.summarize_replay(selected_rows, subset)
    return candidate_rules, replay


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", default="london_session_data_11", type=Path)
    ap.add_argument("--targets", nargs="*", type=float, default=TARGETS)
    ap.add_argument("--output-dir", default="compiled_target_entry_classes_11_sessions", type=Path)
    args = ap.parse_args()

    data_root = args.data_root
    if not data_root.is_absolute():
        data_root = Path(__file__).resolve().parent / data_root
    out_dir = args.output_dir
    if not out_dir.is_absolute():
        out_dir = Path(__file__).resolve().parent / out_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    truth_rows = build_target_truth(data_root, args.targets)
    write_csv(out_dir / "target_entry_truth_table.csv", truth_rows, list(truth_rows[0].keys()) if truth_rows else ["timestamp"])

    summary_rows = []
    rules_payload = {"entry_classes": []}
    for direction in ("LONG", "SHORT"):
        for target in args.targets:
            rules, replay = build_classes_for_target(truth_rows, direction, target)
            rules_payload["entry_classes"].extend(rules)
            summary_rows.append(
                {
                    "direction": direction,
                    "target_distance": target,
                    "rule_count": len(rules),
                    **replay,
                }
            )

    write_csv(out_dir / "target_entry_class_summary.csv", summary_rows, list(summary_rows[0].keys()) if summary_rows else ["direction"])
    (out_dir / "target_entry_classes.json").write_text(json.dumps(rules_payload, indent=2))
    report = {
        "targets": args.targets,
        "row_count": len(truth_rows),
        "summary": summary_rows,
    }
    (out_dir / "target_entry_class_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
