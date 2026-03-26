#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

import build_entry_trigger_state_machine as trig


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def pass_point_gate(row: dict[str, Any], curve: dict[str, float]) -> bool:
    return (
        float(row["pre_build_slope"]) >= float(curve["pre_build_slope_min"])
        and float(row["pre_build_accel"]) >= float(curve["pre_build_accel_min"])
        and float(row["pre_compression_release_delta"]) >= float(curve["pre_compression_release_delta_min"])
        and float(row["pre_macro_micro_alignment"]) >= float(curve["pre_macro_micro_alignment_min"])
        and float(row["post_continuation_persistence"]) >= float(curve["post_continuation_persistence_min"])
        and float(row["post_noise_rise"]) <= float(curve["post_noise_rise_max"])
        and float(row["post_exhaustion_rise"]) <= float(curve["post_exhaustion_rise_max"])
    )


def replay(rows: list[dict[str, Any]], rules: list[dict[str, Any]], curve: dict[str, float] | None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for row in rows:
        if curve is not None and not pass_point_gate(row, curve):
            continue
        for rule in rules:
            if trig.match_rule(row, rule):
                selected.append(row)
                break
    return selected, trig.summarize_replay(selected, rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trajectory-csv", required=True, type=Path)
    ap.add_argument("--rules-json", required=True, type=Path)
    ap.add_argument("--curve-json", required=True, type=Path)
    ap.add_argument("--output-dir", required=True, type=Path)
    args = ap.parse_args()

    rows = load_csv(args.trajectory_csv)
    rules = json.loads(args.rules_json.read_text())["path_classes"]
    curve = json.loads(args.curve_json.read_text())["rules"][0]
    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    base_rows, base_replay = replay(rows, rules, None)
    gated_rows, gated_replay = replay(rows, rules, curve)

    report = {
        "before": base_replay,
        "after": gated_replay,
        "delta": {
            "trade_count": gated_replay["trade_count"] - base_replay["trade_count"],
            "win_rate": gated_replay["win_rate"] - base_replay["win_rate"],
            "expectancy": gated_replay["expectancy"] - base_replay["expectancy"],
            "avg_R": gated_replay["avg_R"] - base_replay["avg_R"],
            "pips_per_hour": gated_replay["pips_per_hour"] - base_replay["pips_per_hour"],
            "good_capture": gated_replay["good_capture"] - base_replay["good_capture"],
            "bad_trigger": gated_replay["bad_trigger"] - base_replay["bad_trigger"],
            "noise_trigger": gated_replay["noise_trigger"] - base_replay["noise_trigger"],
        },
        "curve": curve,
    }

    write_csv(out_dir / "before_point_gate_population.csv", base_rows, list(base_rows[0].keys()) if base_rows else ["timestamp"])
    write_csv(out_dir / "after_point_gate_population.csv", gated_rows, list(gated_rows[0].keys()) if gated_rows else ["timestamp"])
    (out_dir / "point_trajectory_gate_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
