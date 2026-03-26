#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter, defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DEFAULT_ENTRY_TRUTH = ROOT / "compiled_target_entry_classes_contextual_v2_11_sessions" / "target_entry_truth_table.csv"
DEFAULT_AEE_STATE = ROOT / "compiled_aee_stage_11_sessions_canonical" / "aee_state_stream" / "aee_state_stream.csv"
DEFAULT_OUT_DIR = ROOT / "compiled_trade_type_truth_11_sessions"

HARVESTER_TARGETS = {1.5, 2.5}
RUNNER_TARGETS = {4.5, 6.0, 7.0, 8.0, 9.0, 11.0, 13.0, 15.0}


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open() as f:
        return list(csv.DictReader(f))


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["timestamp"])
            writer.writeheader()
        return
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def mean0(vals: list[float]) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def classify_trade_type(target: float) -> str | None:
    if target in HARVESTER_TARGETS:
        return "harvester"
    if target in RUNNER_TARGETS:
        return "runner"
    return None


def main() -> None:
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--entry-truth", type=Path, default=DEFAULT_ENTRY_TRUTH)
    ap.add_argument("--aee-state", type=Path, default=DEFAULT_AEE_STATE)
    ap.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    args = ap.parse_args()

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "trade_type_truth_manifest.json"
    inputs_hash = hashlib.sha256(
        json.dumps(
            {
                "entry_truth_hash": sha256_file(args.entry_truth),
                "aee_state_hash": sha256_file(args.aee_state),
                "script_hash": sha256_file(Path(__file__)),
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    if (
        (out_dir / "trade_type_truth_report.json").exists()
        and (out_dir / "harvester_truth_table.csv").exists()
        and (out_dir / "runner_truth_table.csv").exists()
        and manifest_path.exists()
    ):
        try:
            manifest = json.loads(manifest_path.read_text())
        except Exception:
            manifest = {}
        if manifest.get("inputs_hash") == inputs_hash:
            print(json.dumps({"status": "SKIP", "reason": "trade_type_truth_current", "output_dir": str(out_dir)}, indent=2))
            return
    entry_rows = load_csv(args.entry_truth)
    aee_rows = load_csv(args.aee_state)

    assignment = {
        "harvester_targets": sorted(HARVESTER_TARGETS),
        "runner_targets": sorted(RUNNER_TARGETS),
        "rules": {
            "harvester": {
                "objective": "speed_and_volume",
                "target_range": "<4 pips",
                "entry_focus": [
                    "speed",
                    "oscillation_capture",
                    "continuation_pullback",
                    "quick_tp",
                ],
                "aee_focus": [
                    "fast_harvest",
                    "fast_panic_on_collapse",
                    "minimal_hold_patience",
                ],
            },
            "runner": {
                "objective": "intraday_extension",
                "target_range": ">=4.5 pips",
                "entry_focus": [
                    "macro_micro_alignment",
                    "remaining_budget",
                    "continuation_quality",
                    "release_quality",
                ],
                "aee_focus": [
                    "80_percent_partial",
                    "20_percent_runner",
                    "hold_extend_after_partial",
                    "continuation_aware_decay",
                ],
            },
        },
    }
    (out_dir / "trade_type_assignment.json").write_text(json.dumps(assignment, indent=2))

    # Aggregate during-trade behavior from the existing fixed AEE state stream by direction/target.
    state_groups: dict[tuple[str, float], list[dict[str, str]]] = defaultdict(list)
    for row in aee_rows:
        direction = row.get("direction") or row.get("direction_assumed")
        if not direction:
            continue
        state_groups[(direction, float(row["target_distance"]))].append(row)

    state_agg: dict[tuple[str, float], dict[str, float]] = {}
    for key, rows in state_groups.items():
        state_agg[key] = {
            "during_profit_mean": round(mean0([float(r["profit_now"]) for r in rows]), 6),
            "during_mfe_mean": round(mean0([float(r["mfe_so_far"]) for r in rows]), 6),
            "during_mae_mean": round(mean0([float(r["mae_so_far"]) for r in rows]), 6),
            "during_giveback_mean": round(mean0([float(r["giveback_now"]) for r in rows]), 6),
            "during_velocity_mean": round(mean0([float(r["velocity_now"]) for r in rows]), 6),
            "during_time_open_mean": round(mean0([float(r["time_open"]) for r in rows]), 6),
            "during_time_since_peak_mean": round(mean0([float(r["time_since_peak"]) for r in rows]), 6),
            "during_progress_ratio_mean": round(mean0([float(r["progress_ratio"]) for r in rows]), 6),
            "during_energy_ratio_mean": round(mean0([float(r["energy_ratio"]) for r in rows]), 6),
        }

    harvester_rows: list[dict[str, object]] = []
    runner_rows: list[dict[str, object]] = []

    counts = Counter()
    by_dir_target = defaultdict(Counter)

    for row in entry_rows:
        target = float(row["target_distance"])
        trade_type = classify_trade_type(target)
        if trade_type is None:
            continue
        direction = row["direction_assumed"]
        agg = state_agg.get((direction, target), {})
        future_mfe = float(row["future_mfe_pips"])
        tp_hit_min = float(row["tp_hit_min"])
        static_pips = float(row["static_pips"])
        breakout_distance = float(row["breakout_distance"])
        pressure_5 = float(row["pressure_5"])
        pressure_15 = float(row["pressure_15"])
        compression = float(row["compression"])
        remaining_budget_score = float(row["remaining_budget_score"])

        out = {
            **row,
            "trade_type": trade_type,
            "target_bucket": "small" if trade_type == "harvester" else "standard",
            "quick_tp": int(tp_hit_min >= 0 and tp_hit_min <= 8),
            "extension_available": int(future_mfe >= target + 2.0),
            "speed_bias": round(pressure_5 - pressure_15, 6),
            "compression_breakout_ratio": round(breakout_distance / max(compression, 1e-6), 6),
            "continuation_budget_proxy": round(remaining_budget_score * max(future_mfe - target, 0.0), 6),
            "speed_objective_score": round(max(static_pips, 0.0) * max(0.0, 1.0 - (tp_hit_min / 20.0 if tp_hit_min >= 0 else 1.0)), 6),
            "runner_objective_score": round(max(future_mfe - min(target, 2.5), 0.0) * remaining_budget_score, 6),
            **agg,
        }
        counts[trade_type] += 1
        by_dir_target[trade_type][f"{direction}_{target:g}"] += 1
        if trade_type == "harvester":
            harvester_rows.append(out)
        else:
            runner_rows.append(out)

    write_csv(out_dir / "harvester_truth_table.csv", harvester_rows)
    write_csv(out_dir / "runner_truth_table.csv", runner_rows)

    report = {
        "status": "PASS",
        "entry_truth_source": str(args.entry_truth),
        "aee_state_source": str(args.aee_state),
        "harvester_row_count": len(harvester_rows),
        "runner_row_count": len(runner_rows),
        "by_trade_type": {
            "harvester": {
                "targets": sorted(HARVESTER_TARGETS),
                "rows": len(harvester_rows),
                "by_direction_target": dict(by_dir_target["harvester"]),
                "quick_tp_rate": round(mean0([float(r["quick_tp"]) for r in harvester_rows]), 6) if harvester_rows else 0.0,
                "speed_objective_mean": round(mean0([float(r["speed_objective_score"]) for r in harvester_rows]), 6) if harvester_rows else 0.0,
            },
            "runner": {
                "targets": sorted(RUNNER_TARGETS),
                "rows": len(runner_rows),
                "by_direction_target": dict(by_dir_target["runner"]),
                "extension_available_rate": round(mean0([float(r["extension_available"]) for r in runner_rows]), 6) if runner_rows else 0.0,
                "runner_objective_mean": round(mean0([float(r["runner_objective_score"]) for r in runner_rows]), 6) if runner_rows else 0.0,
            },
        },
    }
    (out_dir / "trade_type_truth_report.json").write_text(json.dumps(report, indent=2))
    manifest_path.write_text(
        json.dumps(
            {
                "runner": Path(__file__).name,
                "inputs_hash": inputs_hash,
                "entry_truth": str(args.entry_truth),
                "aee_state": str(args.aee_state),
                "report": str(out_dir / "trade_type_truth_report.json"),
            },
            indent=2,
        )
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
