#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent

REQUIRED_OUTPUTS = [
    Path("phase1/opportunity_map_raw.csv"),
    Path("phase1/opportunity_map_summary.json"),
    Path("phase1/opportunity_map_audit.json"),
    Path("phase2/opportunity_clusters.csv"),
    Path("phase2/cluster_summary.json"),
    Path("phase2/cluster_audit.json"),
    Path("phase3/entry_window_states.csv"),
    Path("phase3/entry_window_summary.json"),
    Path("phase3/entry_window_audit.json"),
    Path("phase4/opportunity_zones_labeled.csv"),
    Path("phase4/zone_label_summary.json"),
    Path("phase4/zone_label_audit.json"),
    Path("phase5/zone_label_separability.json"),
    Path("phase6/odm_ceiling_report.json"),
    Path("phase6/odm_audit.json"),
    Path("phase6/cluster_resolved_labels.csv"),
    Path("compiler_manifest.json"),
    Path("compiler_report.json"),
]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def run_step(name: str, cmd: list[str], expected: list[Path]) -> dict[str, Any]:
    proc = subprocess.run(cmd, cwd=ROOT, text=True, capture_output=True)
    result = {
        "step": name,
        "command": cmd,
        "returncode": proc.returncode,
        "stdout": proc.stdout[-4000:],
        "stderr": proc.stderr[-4000:],
        "expected_outputs": [str(p) for p in expected],
        "outputs_present": {str(p): p.exists() for p in expected},
    }
    if proc.returncode != 0 or not all(p.exists() for p in expected):
        raise RuntimeError(json.dumps(result, indent=2))
    return result


def build_invariants(output_root: Path) -> dict[str, Any]:
    s1 = read_json(output_root / "phase1" / "opportunity_map_summary.json")
    s1_audit = read_json(output_root / "phase1" / "opportunity_map_audit.json")
    s2 = read_json(output_root / "phase2" / "cluster_summary.json")
    s2_audit = read_json(output_root / "phase2" / "cluster_audit.json")
    s3 = read_json(output_root / "phase3" / "entry_window_summary.json")
    s3_audit = read_json(output_root / "phase3" / "entry_window_audit.json")
    s4_audit = read_json(output_root / "phase4" / "zone_label_audit.json")
    s5 = read_json(output_root / "phase5" / "zone_label_separability.json")
    s6_audit = read_json(output_root / "phase6" / "odm_audit.json")

    stage1_pass = s1_audit.get("overall_phase1_status") == "PHASE1_PASS"
    stage2_pass = s2_audit.get("overall_phase2_status") == "PHASE2_PASS"
    stage3_pass = s3_audit.get("overall_phase3_status") in {"PHASE3_PASS", "PHASE3_PARTIAL"} and s3["clusters_with_valid_entries"] == s3["cluster_count"]
    stage4_pass = s4_audit.get("overall_phase4_status") == "PHASE4_PASS"
    stage5_pass = s5.get("separability_status") == "PASS"
    stage6_pass = s6_audit.get("overall_phase6_status") == "PHASE6_PASS"

    counts_reconcile = (
        s1["total_rows_processed"] > 0
        and s2["total_clusters"] > 0
        and s3["clusters_with_valid_entries"] == s2["total_clusters"]
        and s4_audit["label_counts"]["GOOD"] + s4_audit["label_counts"]["BAD"] + s4_audit["label_counts"]["NOISE"] == sum(1 for _ in open(output_root / "phase4" / "opportunity_zones_labeled.csv")) - 1
        and s5["good_count"] <= s4_audit["label_counts"]["GOOD"]
        and s5["bad_count"] == s4_audit["label_counts"]["BAD"]
    )

    return {
        "stage1_pass": stage1_pass,
        "stage2_pass": stage2_pass,
        "stage3_pass": stage3_pass,
        "stage4_pass": stage4_pass,
        "stage5_pass": stage5_pass,
        "stage6_pass": stage6_pass,
        "counts_reconcile": counts_reconcile,
        "final_status": "PASS" if all([stage1_pass, stage2_pass, stage3_pass, stage4_pass, stage5_pass, stage6_pass, counts_reconcile]) else "FAIL",
    }


def outputs_complete(output_root: Path) -> bool:
    return all((output_root / rel).exists() for rel in REQUIRED_OUTPUTS)


def build_inputs_fingerprint(dataset_lock: Path, data_root: Path) -> dict[str, Any]:
    return {
        "dataset_lock_hash": sha256_file(dataset_lock),
        "data_root": str(data_root.resolve()),
        "compiler_scripts": {
            name: sha256_file(ROOT / name)
            for name in [
                "phase1_multi_session_compile.py",
                "phase2_11_sessions_cluster_compile.py",
                "phase3_11_sessions_entry_windows.py",
                "phase4_11_sessions_oae.py",
                "phase5_11_sessions_separability.py",
                "stage6_11_sessions_odm.py",
                Path(__file__).name,
            ]
        },
    }


def manifest_matches(output_root: Path, expected: dict[str, Any]) -> bool:
    manifest_path = output_root / "compiler_manifest.json"
    if not manifest_path.exists():
        return False
    try:
        manifest = json.loads(manifest_path.read_text())
    except Exception:
        return False
    return (
        manifest.get("dataset_lock_hash") == expected["dataset_lock_hash"]
        and manifest.get("data_root") == expected["data_root"]
        and manifest.get("compiler_scripts") == expected["compiler_scripts"]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Deterministic stage 1-6 compiler for EUR/USD Monday London datasets")
    parser.add_argument("--dataset-lock", default="dataset_lock_11_sessions.json")
    parser.add_argument("--data-root", default="london_session_data_11")
    parser.add_argument("--pair", default=None)
    parser.add_argument("--output-root", default="compiled_stage1_5_11_sessions")
    args = parser.parse_args()

    dataset_lock = ROOT / args.dataset_lock
    data_root = ROOT / args.data_root
    output_root = ROOT / args.output_root
    inputs_fingerprint = build_inputs_fingerprint(dataset_lock, data_root)
    if outputs_complete(output_root) and manifest_matches(output_root, inputs_fingerprint):
        print(json.dumps({"status": "SKIP", "output_root": str(output_root), "reason": "deterministic_stage_current"}, indent=2))
        return
    lock = json.loads(dataset_lock.read_text())
    pair = str(args.pair or lock.get("pair") or "EUR_USD")
    session_label = str(lock.get("session", "unknown")).lower()
    weekday_label = str(lock.get("weekday", "unknown")).lower()
    if output_root.exists():
        shutil.rmtree(output_root)
    (output_root / "phase1").mkdir(parents=True, exist_ok=True)
    (output_root / "phase2").mkdir(parents=True, exist_ok=True)
    (output_root / "phase3").mkdir(parents=True, exist_ok=True)
    (output_root / "phase4").mkdir(parents=True, exist_ok=True)
    (output_root / "phase5").mkdir(parents=True, exist_ok=True)
    (output_root / "phase6").mkdir(parents=True, exist_ok=True)

    run_log: list[dict[str, Any]] = []

    run_log.append(
        run_step(
            "stage1",
            [
                sys.executable,
                str(ROOT / "phase1_multi_session_compile.py"),
                "--data-root",
                str(data_root),
                "--pair",
                pair,
                "--session-label",
                session_label,
                "--weekday-label",
                weekday_label,
                "--output-dir",
                str(output_root / "phase1"),
            ],
            [
                output_root / "phase1" / "opportunity_map_raw.csv",
                output_root / "phase1" / "opportunity_map_summary.json",
                output_root / "phase1" / "opportunity_map_audit.json",
            ],
        )
    )

    run_log.append(
        run_step(
            "stage2",
            [
                sys.executable,
                str(ROOT / "phase2_11_sessions_cluster_compile.py"),
                "--input-csv",
                str(output_root / "phase1" / "opportunity_map_raw.csv"),
                "--output-dir",
                str(output_root / "phase2"),
            ],
            [
                output_root / "phase2" / "opportunity_clusters.csv",
                output_root / "phase2" / "cluster_summary.json",
                output_root / "phase2" / "cluster_audit.json",
            ],
        )
    )

    run_log.append(
        run_step(
            "stage3",
            [
                sys.executable,
                str(ROOT / "phase3_11_sessions_entry_windows.py"),
                "--phase1-csv",
                str(output_root / "phase1" / "opportunity_map_raw.csv"),
                "--clusters-csv",
                str(output_root / "phase2" / "opportunity_clusters.csv"),
                "--output-dir",
                str(output_root / "phase3"),
            ],
            [
                output_root / "phase3" / "entry_window_states.csv",
                output_root / "phase3" / "entry_window_summary.json",
                output_root / "phase3" / "entry_window_audit.json",
            ],
        )
    )

    run_log.append(
        run_step(
            "stage4",
            [
                sys.executable,
                str(ROOT / "phase4_11_sessions_oae.py"),
                "--phase1-csv",
                str(output_root / "phase1" / "opportunity_map_raw.csv"),
                "--output-dir",
                str(output_root / "phase4"),
            ],
            [
                output_root / "phase4" / "opportunity_zones_labeled.csv",
                output_root / "phase4" / "zone_label_summary.json",
                output_root / "phase4" / "zone_label_audit.json",
            ],
        )
    )

    run_log.append(
        run_step(
            "stage5",
            [
                sys.executable,
                str(ROOT / "phase5_11_sessions_separability.py"),
                "--labeled-csv",
                str(output_root / "phase4" / "opportunity_zones_labeled.csv"),
                "--output-dir",
                str(output_root / "phase5"),
            ],
            [
                output_root / "phase5" / "zone_label_separability.json",
            ],
        )
    )

    run_log.append(
        run_step(
            "stage6",
            [
                sys.executable,
                str(ROOT / "stage6_11_sessions_odm.py"),
                "--clusters-csv",
                str(output_root / "phase2" / "opportunity_clusters.csv"),
                "--entry-windows-csv",
                str(output_root / "phase3" / "entry_window_states.csv"),
                "--labeled-csv",
                str(output_root / "phase4" / "opportunity_zones_labeled.csv"),
                "--dataset-lock",
                str(dataset_lock),
                "--output-dir",
                str(output_root / "phase6"),
            ],
            [
                output_root / "phase6" / "odm_ceiling_report.json",
                output_root / "phase6" / "odm_audit.json",
                output_root / "phase6" / "cluster_resolved_labels.csv",
            ],
        )
    )

    manifest = {
        "dataset_lock": str(dataset_lock),
        **inputs_fingerprint,
        "run_log": run_log,
        "invariants": build_invariants(output_root),
    }
    (output_root / "compiler_manifest.json").write_text(json.dumps(manifest, indent=2))

    summary = {
        "output_root": str(output_root),
        "stage_outputs": {
            "phase1": str(output_root / "phase1"),
            "phase2": str(output_root / "phase2"),
            "phase3": str(output_root / "phase3"),
            "phase4": str(output_root / "phase4"),
            "phase5": str(output_root / "phase5"),
            "phase6": str(output_root / "phase6"),
        },
        "invariants": manifest["invariants"],
    }
    (output_root / "compiler_report.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
