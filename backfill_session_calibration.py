#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import session_calibration


ROOT = Path(__file__).resolve().parent
OUTPUT_ROOT = ROOT / "compiled_market_nodes"


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def first_existing(paths: Iterable[Path]) -> Path | None:
    for path in paths:
        if path.exists():
            return path
    return None


def resolve_inputs(node_dir: Path) -> tuple[Path | None, Path | None, Path | None, Path | None]:
    parts = node_dir.name.split("__")
    pair, weekday, session = parts[0], parts[1], parts[2]
    dataset_lock = first_existing(
        [
            node_dir / "dataset_lock_11_sessions.json",
            ROOT / f"dataset_lock__{pair.lower()}__{weekday}__{session}__11.json",
        ]
    )
    truth_csv = first_existing(
        [
            node_dir / "target_entry_stage" / "target_contextual_v2" / "target_entry_truth_table.csv",
            node_dir / "target_entry_stage" / "target_contextual_v2_targeted" / "target_entry_truth_table.csv",
        ]
    )
    entry_population_csv = first_existing(
        [
            node_dir / "target_entry_no_timeouts" / "target_entry_population.csv",
            node_dir / "target_entry_stage" / "target_no_timeouts" / "target_entry_population.csv",
            node_dir / "target_entry_stage" / "target_contextual_v2" / "target_entry_population.csv",
        ]
    )
    trade_rows_json = first_existing(
        [
            node_dir / "aee_target_local_fixedpop" / "target_local_fixedpop_aee_trade_rows.json",
            node_dir / "aee_stage" / "target_local_hotspot_merged" / "aee_target_local_hotspot_merged_trade_rows.json",
            node_dir / "aee_stage" / "target_local_aee" / "target_local_aee_trade_rows.json",
            node_dir / "aee_hotspot" / "aee_hotspot_trade_rows.json",
            node_dir / "aee_target_local_hotspot_merged" / "aee_target_local_hotspot_merged_trade_rows.json",
        ]
    )
    return dataset_lock, truth_csv, entry_population_csv, trade_rows_json


def write_missing(node_dir: Path, missing: list[str]) -> None:
    out = node_dir / "session_calibration"
    out.mkdir(parents=True, exist_ok=True)
    report = {
        "status": "MISSING_INPUTS",
        "mode": "session_calibration_backfill",
        "timestamp": iso_now(),
        "node": node_dir.name,
        "missing_inputs": missing,
        "action_counts": {},
        "zones": [],
    }
    manifest = {
        "runner": "backfill_session_calibration.py",
        "status": "MISSING_INPUTS",
        "inputs_hash": None,
        "missing_inputs": missing,
        "report": str(out / "session_calibration_report.json"),
    }
    (out / "session_calibration_report.json").write_text(json.dumps(report, indent=2))
    (out / "session_calibration_manifest.json").write_text(json.dumps(manifest, indent=2))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sessions", nargs="*", default=["london"])
    ap.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    args = ap.parse_args()

    results: list[dict[str, str | list[str]]] = []
    for node_dir in sorted(args.output_root.glob("*__*__*")):
        try:
            _, _, session = node_dir.name.split("__")
        except ValueError:
            continue
        if session not in args.sessions:
            continue
        dataset_lock, truth_csv, entry_population_csv, trade_rows_json = resolve_inputs(node_dir)
        missing = []
        if dataset_lock is None:
            missing.append("dataset_lock")
        if truth_csv is None:
            missing.append("truth_csv")
        if entry_population_csv is None:
            missing.append("entry_population_csv")
        if missing:
            write_missing(node_dir, missing)
            results.append({"node": node_dir.name, "status": "MISSING_INPUTS", "missing": missing})
            continue
        session_calibration.run(
            dataset_lock=dataset_lock,
            truth_csv=truth_csv,
            entry_population_csv=entry_population_csv,
            output_dir=node_dir / "session_calibration",
            trade_rows_json=trade_rows_json,
            symmetric_break_even=0.505,
        )
        results.append({"node": node_dir.name, "status": "PASS"})

    report = {"status": "PASS", "results": results}
    (ROOT / "session_calibration_backfill_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
