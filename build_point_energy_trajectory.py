#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from statistics import mean
from typing import Any


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def f(row: dict[str, Any], key: str) -> float:
    return float(row[key])


def slope(seq: list[float]) -> float:
    if len(seq) < 2:
        return 0.0
    return (seq[-1] - seq[0]) / (len(seq) - 1)


def accel(seq: list[float]) -> float:
    if len(seq) < 3:
        return 0.0
    first = slope(seq[:-1])
    second = slope(seq[1:])
    return second - first


def derive_point_trajectory(rows: list[dict[str, Any]], idx: int) -> dict[str, float]:
    lo = max(0, idx - 20)
    hi = min(len(rows), idx + 21)
    window = rows[lo:hi]
    rel = idx - lo

    pre = window[: rel + 1]
    post = window[rel:]

    pre_release = [f(r, "release_quality_score") for r in pre]
    pre_comp = [f(r, "compression_score") for r in pre]
    pre_macro = [f(r, "macro_dir_score") for r in pre]
    pre_micro = [f(r, "micro_dir_score") for r in pre]
    pre_budget = [f(r, "remaining_budget_score") for r in pre]
    pre_noise = [f(r, "noise_score") for r in pre]
    pre_exhaust = [f(r, "exhaustion_score") for r in pre]

    post_release = [f(r, "release_quality_score") for r in post]
    post_budget = [f(r, "remaining_budget_score") for r in post]
    post_noise = [f(r, "noise_score") for r in post]
    post_exhaust = [f(r, "exhaustion_score") for r in post]
    post_micro = [f(r, "micro_dir_score") for r in post]

    return {
        "pre_build_slope": round(slope(pre_release), 6),
        "pre_build_accel": round(accel(pre_release), 6),
        "pre_compression_release_delta": round((pre_release[-1] if pre_release else 0.0) - (pre_comp[-1] if pre_comp else 0.0), 6),
        "pre_macro_micro_alignment": round((pre_macro[-1] if pre_macro else 0.0) - abs((pre_macro[-1] if pre_macro else 0.0) - (pre_micro[-1] if pre_micro else 0.0)), 6),
        "pre_budget_slope": round(slope(pre_budget), 6),
        "pre_noise_slope": round(slope(pre_noise), 6),
        "pre_exhaustion_slope": round(slope(pre_exhaust), 6),
        "release_to_exhaustion_delta": round((post_release[0] if post_release else 0.0) - max(post_exhaust[:5] or [0.0]), 6),
        "post_continuation_persistence": round(mean(post_micro[:5]) if post_micro else 0.0, 6),
        "post_budget_decay": round((post_budget[0] if post_budget else 0.0) - (post_budget[min(5, len(post_budget)-1)] if len(post_budget) > 1 else 0.0), 6),
        "post_noise_rise": round((post_noise[min(5, len(post_noise)-1)] if len(post_noise) > 1 else 0.0) - (post_noise[0] if post_noise else 0.0), 6),
        "post_exhaustion_rise": round((post_exhaust[min(5, len(post_exhaust)-1)] if len(post_exhaust) > 1 else 0.0) - (post_exhaust[0] if post_exhaust else 0.0), 6),
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_action: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_action[r["action_truth"]].append(r)
    action_profiles = {}
    feature_keys = [
        "pre_build_slope",
        "pre_build_accel",
        "pre_compression_release_delta",
        "pre_macro_micro_alignment",
        "pre_budget_slope",
        "pre_noise_slope",
        "pre_exhaustion_slope",
        "release_to_exhaustion_delta",
        "post_continuation_persistence",
        "post_budget_decay",
        "post_noise_rise",
        "post_exhaustion_rise",
    ]
    for action, bucket in by_action.items():
        action_profiles[action] = {
            "count": len(bucket),
            **{
                f"{k}_mean": mean(float(r[k]) for r in bucket) if bucket else 0.0
                for k in feature_keys
            },
        }
    return {
        "row_count": len(rows),
        "action_counts": dict(Counter(r["action_truth"] for r in rows)),
        "action_profiles": action_profiles,
    }


def simple_trigger_surface(rows: list[dict[str, Any]]) -> dict[str, Any]:
    enter_rows = [r for r in rows if r["action_truth"] in {"ENTER_LONG", "ENTER_SHORT"}]
    if not enter_rows:
        return {"rules": []}
    return {
        "rules": [
            {
                "pre_build_slope_min": mean(float(r["pre_build_slope"]) for r in enter_rows),
                "pre_build_accel_min": mean(float(r["pre_build_accel"]) for r in enter_rows),
                "pre_compression_release_delta_min": mean(float(r["pre_compression_release_delta"]) for r in enter_rows),
                "pre_macro_micro_alignment_min": mean(float(r["pre_macro_micro_alignment"]) for r in enter_rows),
                "post_continuation_persistence_min": mean(float(r["post_continuation_persistence"]) for r in enter_rows),
                "post_noise_rise_max": mean(float(r["post_noise_rise"]) for r in enter_rows),
                "post_exhaustion_rise_max": mean(float(r["post_exhaustion_rise"]) for r in enter_rows),
            }
        ]
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--context-stream-csv", required=True, type=Path)
    ap.add_argument("--truth-csv", required=True, type=Path)
    ap.add_argument("--output-dir", required=True, type=Path)
    args = ap.parse_args()

    stream_rows = load_csv(args.context_stream_csv)
    truth_rows = load_csv(args.truth_csv)
    truth_map = {
        (r["timestamp"], r["session_id"], r["quarter"], r["direction_assumed"]): r
        for r in truth_rows
    }

    by_session_dir: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for r in stream_rows:
        by_session_dir[(r["session_id"], r["direction_assumed"])].append(r)

    enriched: list[dict[str, Any]] = []
    for (_, _), rows in by_session_dir.items():
        rows.sort(key=lambda r: r["timestamp"])
        for idx, row in enumerate(rows):
            key = (row["timestamp"], row["session_id"], row["quarter"], row["direction_assumed"])
            truth = truth_map.get(key)
            if truth is None:
                continue
            traj = derive_point_trajectory(rows, idx)
            enriched.append({**truth, **row, **traj})

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    fields = list(enriched[0].keys()) if enriched else ["timestamp"]
    write_csv(out_dir / "point_energy_trajectory.csv", enriched, fields)

    transition_report = summarize(enriched)
    trigger_surface = simple_trigger_surface(enriched)

    (out_dir / "point_energy_transition_report.json").write_text(json.dumps(transition_report, indent=2))
    (out_dir / "point_trigger_curvature_report.json").write_text(json.dumps(trigger_surface, indent=2))
    print(json.dumps({
        "rows": len(enriched),
        "action_counts": transition_report["action_counts"],
    }, indent=2))


if __name__ == "__main__":
    main()
