#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = ROOT / "compiled_market_nodes"

STAGES = [
    ("stage1_6", Path("stage1_6/compiler_report.json")),
    ("target_entry_stage", Path("target_entry_stage/target_stage_manifest.json")),
    ("target_entry_no_timeouts", Path("target_entry_no_timeouts/target_entry_class_report.json")),
    ("aee_stage", Path("aee_stage/aee_stage_report.json")),
    ("trade_type_truth", Path("trade_type_truth/trade_type_truth_report.json")),
    ("aee_target_local_fixedpop", Path("aee_target_local_fixedpop/target_local_fixedpop_aee_report.json")),
    ("aee_target_theoretical_ceiling", Path("aee_target_theoretical_ceiling/aee_target_theoretical_ceiling_report.json")),
    ("node_manifest", Path("node_manifest.json")),
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def first_missing(node_dir: Path) -> str | None:
    for name, rel in STAGES:
        if not (node_dir / rel).exists():
            return name
    return None


def write_manifest(lock_path: Path, dataset_lock: dict, node_dir: Path) -> None:
    data_root = Path(str(dataset_lock["data_root"]))
    dataset_data_root = data_root if data_root.is_absolute() else ROOT / data_root
    payload = {
        "compiler": "market_node_compiler_v1_surgical",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "node": {
            "pair": dataset_lock.get("pair"),
            "weekday": dataset_lock.get("weekday"),
            "session": dataset_lock.get("session"),
        },
        "dataset_lock_path": str(lock_path.resolve()),
        "dataset_hash": sha256_file(lock_path),
        "historical_fast": True,
        "data_root": str(dataset_data_root),
        "seed_entry_node": None,
        "seed_aee_node": None,
        "artifacts": {
            "dataset_lock": str(lock_path.resolve()),
            "stage1_6": str(node_dir / "stage1_6"),
            "target_entry_stage": str(node_dir / "target_entry_stage"),
            "target_entry_no_timeouts": str(node_dir / "target_entry_no_timeouts"),
            "trade_type_truth": str(node_dir / "trade_type_truth"),
            "aee_stage": str(node_dir / "aee_stage"),
            "aee_target_local_fixedpop": str(node_dir / "aee_target_local_fixedpop"),
            "aee_target_theoretical_ceiling": str(node_dir / "aee_target_theoretical_ceiling"),
        },
    }
    (node_dir / "node_manifest.json").write_text(json.dumps(payload, indent=2))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-lock", type=Path, required=True)
    ap.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    ap.add_argument("--historical-fast", action="store_true", help="Resume using frozen-rule fast mode.")
    args = ap.parse_args()

    dataset_lock = json.loads(args.dataset_lock.read_text())
    pair = str(dataset_lock["pair"])
    weekday = str(dataset_lock["weekday"]).lower()
    session = str(dataset_lock["session"]).lower()
    node_dir = args.output_root / f"{pair}__{weekday}__{session}"
    node_dir.mkdir(parents=True, exist_ok=True)

    data_root = Path(str(dataset_lock["data_root"]))
    dataset_data_root = data_root if data_root.is_absolute() else ROOT / data_root

    missing = first_missing(node_dir)
    if missing is None:
        print(json.dumps({"node": node_dir.name, "status": "already_complete"}, indent=2))
        return

    if missing == "target_entry_stage":
        run(
            [
                "python3",
                "run_target_entry_stage_compiler.py",
                "--dataset-lock",
                str(args.dataset_lock),
                "--data-root",
                str(dataset_data_root),
                "--output-dir",
                str(node_dir / "target_entry_stage"),
                *(["--historical-fast"] if args.historical_fast else []),
            ]
        )
        missing = first_missing(node_dir)

    if missing == "target_entry_no_timeouts":
        run(
            [
                "python3",
                "run_target_entry_no_timeout.py",
                "--base-rules",
                str(node_dir / "target_entry_stage" / "target_contextual_v2" / "target_entry_classes.json"),
                "--targeted-rules",
                str(node_dir / "target_entry_stage" / "target_contextual_v2_targeted" / "target_entry_classes.json"),
                "--truth-csv",
                str(node_dir / "target_entry_stage" / "target_contextual_v2" / "target_entry_truth_table.csv"),
                "--output-dir",
                str(node_dir / "target_entry_no_timeouts"),
                *(["--historical-fast"] if args.historical_fast else []),
            ]
        )
        missing = first_missing(node_dir)

    if missing == "aee_stage":
        run(
            [
                "python3",
                "run_aee_stage_compiler.py",
                "--dataset-lock",
                str(args.dataset_lock),
                "--truth-csv",
                str(node_dir / "target_entry_stage" / "target_contextual_v2" / "target_entry_truth_table.csv"),
                "--entry-rules-json",
                str(node_dir / "target_entry_no_timeouts" / "target_entry_classes.json"),
                "--output-dir",
                str(node_dir / "aee_stage"),
            ]
        )
        missing = first_missing(node_dir)

    if missing == "trade_type_truth":
        run(
            [
                "python3",
                "build_trade_type_truth.py",
                "--entry-truth",
                str(node_dir / "target_entry_stage" / "target_contextual_v2" / "target_entry_truth_table.csv"),
                "--aee-state",
                str(node_dir / "aee_stage" / "aee_state_stream" / "aee_state_stream.csv"),
                "--output-dir",
                str(node_dir / "trade_type_truth"),
            ]
        )
        missing = first_missing(node_dir)

    if missing == "aee_target_local_fixedpop":
        run(
            [
                "python3",
                "optimize_aee_target_local_from_entry_population.py",
                "--dataset-lock",
                str(args.dataset_lock),
                "--entry-population",
                str(node_dir / "target_entry_no_timeouts" / "target_entry_population.csv"),
                "--seed-aee-dir",
                str(node_dir / "aee_stage"),
                "--output-dir",
                str(node_dir / "aee_target_local_fixedpop"),
            ]
        )
        missing = first_missing(node_dir)

    if missing == "aee_target_theoretical_ceiling":
        run(
            [
                "python3",
                "optimize_aee_target_theoretical_ceiling.py",
                "--dataset-lock",
                str(args.dataset_lock),
                "--entry-population",
                str(node_dir / "target_entry_no_timeouts" / "target_entry_population.csv"),
                "--seed-aee-dir",
                str(node_dir / "aee_stage"),
                "--output-dir",
                str(node_dir / "aee_target_theoretical_ceiling"),
            ]
        )
        missing = first_missing(node_dir)

    if missing == "node_manifest":
        write_manifest(args.dataset_lock, dataset_lock, node_dir)
        missing = first_missing(node_dir)

    print(
        json.dumps(
            {
                "node": node_dir.name,
                "status": "complete" if missing is None else "partial",
                "next_missing": missing,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
