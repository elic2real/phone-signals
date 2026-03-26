#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from statistics import mean
from typing import Any


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


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def join_candidates(entry_rows: list[dict[str, Any]], labeled_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    label_index = {(r["timestamp_start"], r["direction"]): r for r in labeled_rows}
    out: list[dict[str, Any]] = []
    for row in entry_rows:
        key = (row["timestamp"], row["direction"])
        labeled = label_index.get(key)
        if not labeled:
            continue
        out.append(
            {
                "cluster_id": row["cluster_id"],
                "timestamp_start": row["timestamp"],
                "direction": row["direction"],
                "session": row["session"],
                "weekday": row["weekday"],
                "speed": float(labeled["speed"]),
                "efficiency": float(labeled["efficiency"]),
                "label": labeled["zone_label"],
                "max_mfe_pips": float(labeled["max_mfe_pips"]),
                "max_mae_pips": float(labeled["max_mae_pips"]),
                "target_distance": float(labeled["target_distance"]),
            }
        )
    return out


def determine_threshold(candidates: list[dict[str, Any]]) -> float:
    good = [r["speed"] for r in candidates if r["label"] == "GOOD"]
    bad = [r["speed"] for r in candidates if r["label"] == "BAD"]
    if not good:
        return 0.0
    return round((percentile(good, 0.40) + percentile(bad, 0.90) if bad else percentile(good, 0.40)) / 2.0, 6)


def evaluate(candidates: list[dict[str, Any]], threshold: float) -> dict[str, Any]:
    good = [r for r in candidates if r["label"] == "GOOD"]
    bad = [r for r in candidates if r["label"] == "BAD"]
    noise = [r for r in candidates if r["label"] == "NOISE"]
    triggered = [r for r in candidates if r["speed"] >= threshold]
    good_triggered = [r for r in good if r["speed"] >= threshold]
    bad_triggered = [r for r in bad if r["speed"] >= threshold]
    noise_triggered = [r for r in noise if r["speed"] >= threshold]
    pips_mean = mean(r["max_mfe_pips"] for r in triggered) if triggered else 0.0
    return {
        "best_config": {
            "entry_speed_threshold": threshold,
            "logic": "stage3_window_plus_stage4_speed_threshold",
        },
        "top_configs": [
            {
                "entry_speed_threshold": threshold,
                "good_capture": len(good_triggered) / len(good) if good else 0.0,
                "bad_trigger": len(bad_triggered) / len(bad) if bad else 0.0,
                "noise_trigger": len(noise_triggered) / len(noise) if noise else 0.0,
                "pips_mean": pips_mean,
                "trade_count": len(triggered),
            }
        ],
        "good_capture": len(good_triggered) / len(good) if good else 0.0,
        "bad_trigger": len(bad_triggered) / len(bad) if bad else 0.0,
        "noise_trigger": len(noise_triggered) / len(noise) if noise else 0.0,
        "pips_mean": pips_mean,
        "trade_count": len(triggered),
    }


def blockers(candidates: list[dict[str, Any]], threshold: float) -> dict[str, Any]:
    good = [r for r in candidates if r["label"] == "GOOD"]
    counts = Counter()
    for row in good:
        if row["speed"] < threshold:
            counts["speed_below_threshold"] += 1
    return {
        "first_blocker_reason_counts": dict(counts),
        "threshold_used": threshold,
        "candidate_entry_states": len(candidates),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 7 deterministic entry fit for the 11-session compiler")
    parser.add_argument("--entry-windows-csv", default="compiled_stage1_6_11_sessions/phase3/entry_window_states.csv")
    parser.add_argument("--labeled-csv", default="compiled_stage1_6_11_sessions/phase4/opportunity_zones_labeled.csv")
    parser.add_argument("--output-dir", default="compiled_stage1_6_11_sessions/phase7")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    entry_rows = load_csv(Path(args.entry_windows_csv))
    labeled_rows = load_csv(Path(args.labeled_csv))
    candidates = join_candidates(entry_rows, labeled_rows)
    threshold = determine_threshold(candidates)

    both = evaluate(candidates, threshold)
    long = evaluate([r for r in candidates if r["direction"] == "LONG"], threshold)
    short = evaluate([r for r in candidates if r["direction"] == "SHORT"], threshold)
    blocker_report = blockers(candidates, threshold)

    (output_dir / "entry_fit_long.json").write_text(json.dumps(long, indent=2))
    (output_dir / "entry_fit_short.json").write_text(json.dumps(short, indent=2))
    (output_dir / "entry_fit_both.json").write_text(json.dumps(both, indent=2))
    (output_dir / "entry_blockers.json").write_text(json.dumps(blocker_report, indent=2))

    print(json.dumps({"long": long, "short": short, "both": both, "blockers": blocker_report}, indent=2))


if __name__ == "__main__":
    main()
