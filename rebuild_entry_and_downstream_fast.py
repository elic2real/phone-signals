#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
OUTPUT_ROOT = ROOT / "compiled_market_nodes"


def run(cmd: list[str]) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True)


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def has_files(*paths: Path) -> bool:
    return all(path.exists() for path in paths)


def build_downstream_inputs_hash(
    dataset_hash: str,
    target_stage_dir: Path,
    *,
    fast_mode: bool,
    priority_mode: str,
) -> str:
    payload: dict[str, Any] = {
        "dataset_hash": dataset_hash,
        "fast_mode": fast_mode,
        "priority_mode": priority_mode,
    }
    if fast_mode:
        payload["frozen_classes_hash"] = sha256_file(target_stage_dir / "target_contextual_v2" / "target_entry_classes.json")
        payload["truth_hash"] = sha256_file(target_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv")
    else:
        payload["base_rules_hash"] = sha256_file(target_stage_dir / "target_contextual_v2" / "target_entry_classes.json")
        payload["targeted_rules_hash"] = sha256_file(target_stage_dir / "target_contextual_v2_targeted" / "target_entry_classes.json")
        payload["truth_hash"] = sha256_file(target_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv")
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


def entry_outputs_exist(node_dir: Path) -> bool:
    return has_files(
        node_dir / "target_entry_no_timeouts" / "target_entry_classes.json",
        node_dir / "target_entry_no_timeouts" / "target_entry_population.csv",
        node_dir / "target_entry_no_timeouts" / "target_entry_class_report.json",
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


def entry_is_up_to_date(node_dir: Path, dataset_hash: str, inputs_hash: str) -> bool:
    manifest_path = node_dir / "node_manifest.json"
    if not manifest_path.exists() or not entry_outputs_exist(node_dir):
        return False
    try:
        manifest = json.loads(manifest_path.read_text())
    except Exception:
        return False
    rebuild_meta = manifest.get("downstream_rebuild", {})
    return (
        manifest.get("dataset_hash") == dataset_hash
        and rebuild_meta.get("inputs_hash") == inputs_hash
        and rebuild_meta.get("scope") == "entry_only"
    )


def write_manifest(dataset_lock_path: Path, dataset_lock: dict[str, Any], node_dir: Path) -> None:
    manifest = {
        "dataset_lock": dataset_lock,
        "dataset_hash": dataset_lock["hash"],
        "compiler_version": "fast_compile_v1",
        "outputs": {
            "target_entry_no_timeouts": str(node_dir / "target_entry_no_timeouts"),
            "aee_stage": str(node_dir / "aee_stage"),
            "trade_type_truth": str(node_dir / "trade_type_truth"),
            "aee_target_local_fixedpop": str(node_dir / "aee_target_local_fixedpop"),
            "aee_target_theoretical_ceiling": str(node_dir / "aee_target_theoretical_ceiling"),
        },
    }
    (node_dir / "node_manifest.json").write_text(json.dumps(manifest, indent=2))


def normalized_entry_report(
    *,
    status: str,
    mode: str,
    pair: str,
    session: str,
    weekday: str,
    class_count: int,
    selected_trade_count: int,
    note: str | None = None,
) -> dict[str, Any]:
    report = {
        "summary": [],
        "class_reports": {},
        "status": status,
        "mode": mode,
        "pair": pair,
        "session": session,
        "weekday": weekday,
        "class_count": class_count,
        "selected_trade_count": selected_trade_count,
        "empty_population": selected_trade_count == 0,
    }
    if note:
        report["note"] = note
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-lock", type=Path, required=True)
    ap.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    ap.add_argument("--fast-mode", action="store_true", help="Skip expensive entry optimization")
    ap.add_argument("--priority-mode", choices=["balanced", "winrate_first", "expand_quality_entries"], default="balanced")
    ap.add_argument("--entry-only", action="store_true", help="Rebuild target_entry_no_timeouts only and skip downstream AEE stages.")
    args = ap.parse_args()

    dataset_lock = json.loads(args.dataset_lock.read_text())
    pair = str(dataset_lock["pair"])
    weekday = str(dataset_lock["weekday"]).lower()
    session = str(dataset_lock["session"]).lower()
    node_dir = args.output_root / f"{pair}__{weekday}__{session}"
    node_dir.mkdir(parents=True, exist_ok=True)

    target_stage_dir = node_dir / "target_entry_stage"
    target_no_timeout_dir = node_dir / "target_entry_no_timeouts"
    dataset_hash = str(dataset_lock.get("hash") or sha256_file(args.dataset_lock))
    inputs_hash = build_downstream_inputs_hash(dataset_hash, target_stage_dir, fast_mode=args.fast_mode, priority_mode=args.priority_mode)

    if args.entry_only:
        if entry_is_up_to_date(node_dir, dataset_hash, inputs_hash):
            print(f"SKIP: entry artifacts already up to date for {pair} {weekday} {session}")
            return
    else:
        if node_is_up_to_date(node_dir, dataset_hash, inputs_hash):
            print(f"SKIP: downstream artifacts already up to date for {pair} {weekday} {session}")
            return

    # Clean outputs
    cleanup_paths = [target_no_timeout_dir]
    if not args.entry_only:
        cleanup_paths.extend(
            [
                node_dir / "aee_stage",
                node_dir / "trade_type_truth",
                node_dir / "aee_target_local_fixedpop",
                node_dir / "aee_target_theoretical_ceiling",
            ]
        )
    for path in cleanup_paths:
        shutil.rmtree(path, ignore_errors=True)
    if args.entry_only:
        existing_manifest_path = node_dir / "node_manifest.json"
        if existing_manifest_path.exists():
            try:
                manifest = json.loads(existing_manifest_path.read_text())
            except Exception:
                manifest = {}
        else:
            manifest = {}
    else:
        (node_dir / "node_manifest.json").unlink(missing_ok=True)
        manifest = {}

    if args.fast_mode:
        # FAST MODE: Skip optimization, use existing rules directly
        print(f"FAST MODE: Using frozen entry rules for {pair}")
        
        # Copy frozen entry classes directly
        frozen_classes_path = target_stage_dir / "target_contextual_v2" / "target_entry_classes.json"
        if frozen_classes_path.exists():
            target_no_timeout_dir.mkdir(parents=True, exist_ok=True)
            
            # Load frozen classes
            frozen_classes = json.loads(frozen_classes_path.read_text())
            
            # Write minimal required outputs
            (target_no_timeout_dir / "target_entry_classes.json").write_text(json.dumps(frozen_classes, indent=2))
            
            # Create minimal report
            selected_trade_count = len(rows) if truth_path.exists() else 0
            report = normalized_entry_report(
                status="FAST_COMPILE",
                mode="historical_fast",
                pair=pair,
                session=session,
                weekday=weekday,
                class_count=len(frozen_classes.get("entry_classes", [])),
                selected_trade_count=selected_trade_count,
                note="Using frozen entry classes - optimization skipped",
            )
            (target_no_timeout_dir / "target_entry_class_report.json").write_text(json.dumps(report, indent=2))
            
            # Create full population from truth table (not limited)
            truth_path = target_stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"
            if truth_path.exists():
                with truth_path.open() as f:
                    reader = csv.DictReader(f)
                    rows = []
                    for i, row in enumerate(reader):
                        # Only include rows that hit targets
                        if abs(float(row["static_pips"])) == float(row["target_distance"]):
                            rows.append({
                                **row,
                                "trade_id": f"T{i:06d}",
                                "entry_time": row["timestamp"],
                                "direction": row["direction_assumed"],
                                "static_reason": "TP_HIT" if float(row["static_pips"]) > 0 else "SL_HIT",
                            })
                
                if rows:
                    with (target_no_timeout_dir / "target_entry_population.csv").open("w", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
                        writer.writeheader()
                        writer.writerows(rows)
        else:
            raise FileNotFoundError(f"No frozen entry classes found at {frozen_classes_path}")
    else:
        # SLOW MODE: Run full optimization
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
                "--priority-mode",
                args.priority_mode,
            ]
        )

    if args.entry_only:
        manifest_path = node_dir / "node_manifest.json"
        manifest["dataset_hash"] = dataset_hash
        manifest["downstream_rebuild"] = {
            "mode": "fast" if args.fast_mode else "full",
            "inputs_hash": inputs_hash,
            "priority_mode": args.priority_mode,
            "scope": "entry_only",
        }
        manifest_path.write_text(json.dumps(manifest, indent=2))
        return

    # Pre-write selected population for AEE stage only when downstream rebuild is requested.
    population_path = target_no_timeout_dir / "target_entry_population.csv"
    selected_population_path = node_dir / "aee_stage" / "aee_state_stream" / "selected_entry_population.csv"
    selected_population_path.parent.mkdir(parents=True, exist_ok=True)
    if population_path.exists():
        with population_path.open() as f:
            reader = csv.DictReader(f)
            pop_rows = list(reader)
        selected_rows = []
        for seq, row in enumerate(pop_rows, start=1):
            selected_rows.append({
                **row,
                "trade_id": f"T{seq:06d}",
                "entry_time": row["timestamp"],
                "direction": row["direction_assumed"],
                "static_reason": "TP_HIT" if float(row["static_pips"]) > 0 else "SL_HIT",
            })
        with selected_population_path.open("w", newline="") as f:
            fieldnames = list(selected_rows[0].keys()) if selected_rows else ["trade_id", "entry_time", "direction", "static_reason"]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            if selected_rows:
                writer.writerows(selected_rows)

    # Run downstream stages (unchanged)
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
    manifest["dataset_hash"] = dataset_hash
    manifest["downstream_rebuild"] = {
        "mode": "fast" if args.fast_mode else "full",
        "inputs_hash": inputs_hash,
        "priority_mode": args.priority_mode,
        "scope": "full",
    }
    manifest_path.write_text(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    import subprocess
    main()
