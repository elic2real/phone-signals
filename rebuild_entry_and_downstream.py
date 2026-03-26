#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
OUTPUT_ROOT = ROOT / "compiled_market_nodes"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def has_files(*paths: Path) -> bool:
    return all(path.exists() for path in paths)


def build_downstream_inputs_hash(dataset_hash: str, target_stage_dir: Path) -> str:
    payload = {
        "dataset_hash": dataset_hash,
        "base_rules_hash": sha256_file(target_stage_dir / "target_contextual_v2" / "target_entry_classes.json"),
        "targeted_rules_hash": sha256_file(target_stage_dir / "target_contextual_v2_targeted" / "target_entry_classes.json"),
        "truth_hash": sha256_file(target_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"),
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()


def downstream_outputs_exist(node_dir: Path) -> bool:
    return has_files(
        node_dir / "target_entry_no_timeouts" / "target_entry_classes.json",
        node_dir / "target_entry_no_timeouts" / "target_entry_population.csv",
        node_dir / "target_entry_no_timeouts" / "target_entry_class_report.json",
        node_dir / "aee_stage" / "aee_stage_report.json",
        node_dir / "aee_stage" / "aee_manifest.json",
        node_dir / "trade_type_truth" / "trade_type_truth_report.json",
        node_dir / "aee_target_local_fixedpop" / "target_local_fixedpop_aee_report.json",
        node_dir / "aee_target_theoretical_ceiling" / "aee_target_theoretical_ceiling_report.json",
        node_dir / "node_manifest.json",
    )


def node_is_up_to_date(node_dir: Path, dataset_hash: str, inputs_hash: str) -> bool:
    manifest_path = node_dir / "node_manifest.json"
    if not manifest_path.exists() or not downstream_outputs_exist(node_dir):
        return False
    try:
        manifest = json.loads(manifest_path.read_text())
    except Exception:
        return False
    rebuild_meta = manifest.get("downstream_rebuild", {})
    return manifest.get("dataset_hash") == dataset_hash and rebuild_meta.get("inputs_hash") == inputs_hash


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
    ap.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    args = ap.parse_args()

    dataset_lock = json.loads(args.dataset_lock.read_text())
    pair = str(dataset_lock["pair"])
    weekday = str(dataset_lock["weekday"]).lower()
    session = str(dataset_lock["session"]).lower()
    node_dir = args.output_root / f"{pair}__{weekday}__{session}"
    node_dir.mkdir(parents=True, exist_ok=True)

    target_stage_dir = node_dir / "target_entry_stage"
    target_no_timeout_dir = node_dir / "target_entry_no_timeouts"
    dataset_hash = sha256_file(args.dataset_lock)
    inputs_hash = build_downstream_inputs_hash(dataset_hash, target_stage_dir)

    if node_is_up_to_date(node_dir, dataset_hash, inputs_hash):
        print(f"SKIP: downstream artifacts already up to date for {pair} {weekday} {session}")
        return

    for path in [
        target_no_timeout_dir,
        node_dir / "aee_stage",
        node_dir / "trade_type_truth",
        node_dir / "aee_target_local_fixedpop",
        node_dir / "aee_target_theoretical_ceiling",
    ]:
        shutil.rmtree(path, ignore_errors=True)
    (node_dir / "node_manifest.json").unlink(missing_ok=True)

    run(
        [
            "python3",
            "run_target_entry_no_timeout.py",
            "--base-rules",
            str(target_stage_dir / "target_contextual_v2" / "target_entry_classes.json"),
            "--targeted-rules",
            str(target_stage_dir / "target_contextual_v2_targeted" / "target_entry_classes.json"),
            "--truth-csv",
            str(target_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"),
            "--output-dir",
            str(target_no_timeout_dir),
        ]
    )

    population_path = target_no_timeout_dir / "target_entry_population.csv"
    selected_population_path = node_dir / "aee_stage" / "aee_state_stream" / "selected_entry_population.csv"
    selected_population_path.parent.mkdir(parents=True, exist_ok=True)
    with population_path.open() as f:
        reader = csv.DictReader(f)
        pop_rows = list(reader)
    selected_rows = []
    for seq, row in enumerate(pop_rows, start=1):
        static_pips = float(row.get("static_pips", 0.0))
        selected_rows.append(
            {
                **row,
                "pair": pair,
                "trade_id": f"T{seq:06d}",
                "entry_time": row.get("timestamp", ""),
                "direction": row.get("direction_assumed", row.get("direction", "")),
                "static_reason": "TP_HIT" if static_pips > 0 else "SL_HIT",
            }
        )
    with selected_population_path.open("w", newline="") as f:
        fieldnames = list(selected_rows[0].keys()) if selected_rows else ["trade_id"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if selected_rows:
            writer.writerows(selected_rows)

    run(
        [
            "python3",
            "run_aee_stage_compiler.py",
            "--dataset-lock",
            str(args.dataset_lock),
            "--truth-csv",
            str(target_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"),
            "--entry-rules-json",
            str(target_no_timeout_dir / "target_entry_classes.json"),
            "--output-dir",
            str(node_dir / "aee_stage"),
        ]
    )

    run(
        [
            "python3",
            "build_trade_type_truth.py",
            "--entry-truth",
            str(target_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"),
            "--aee-state",
            str(node_dir / "aee_stage" / "aee_state_stream" / "aee_state_stream.csv"),
            "--output-dir",
            str(node_dir / "trade_type_truth"),
        ]
    )

    run(
        [
            "python3",
            "optimize_aee_target_local_from_entry_population.py",
            "--dataset-lock",
            str(args.dataset_lock),
            "--entry-population",
            str(target_no_timeout_dir / "target_entry_population.csv"),
            "--seed-aee-dir",
            str(node_dir / "aee_stage"),
            "--output-dir",
            str(node_dir / "aee_target_local_fixedpop"),
        ]
    )

    run(
        [
            "python3",
            "optimize_aee_target_theoretical_ceiling.py",
            "--dataset-lock",
            str(args.dataset_lock),
            "--entry-population",
            str(target_no_timeout_dir / "target_entry_population.csv"),
            "--seed-aee-dir",
            str(node_dir / "aee_stage"),
            "--output-dir",
            str(node_dir / "aee_target_theoretical_ceiling"),
        ]
    )

    write_manifest(args.dataset_lock, dataset_lock, node_dir)
    manifest_path = node_dir / "node_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["downstream_rebuild"] = {"mode": "full", "inputs_hash": inputs_hash}
    manifest_path.write_text(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
