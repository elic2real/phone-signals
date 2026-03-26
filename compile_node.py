#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _run_stage(cmd: list[str], cwd: Path, env: dict[str, str] | None = None) -> dict[str, Any]:
    started = _utc_now_iso()
    proc = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True, env=env)
    ended = _utc_now_iso()
    return {
        "cmd": cmd,
        "cwd": str(cwd),
        "returncode": proc.returncode,
        "started": started,
        "ended": ended,
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def _require_file(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required {label}: {path}")
    if path.is_dir():
        raise IsADirectoryError(f"Expected file for {label}, got directory: {path}")


def _require_dir(path: Path, label: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required {label}: {path}")
    if not path.is_dir():
        raise NotADirectoryError(f"Expected directory for {label}, got file: {path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="PC2 node compiler runner: stage1->stage6")
    parser.add_argument("--base-dir", default=str(ROOT), help="Root of the PC2 bundle (contains compiler/, production/, datasets/)")
    parser.add_argument("--pair", required=True)
    parser.add_argument("--weekday", required=True)
    parser.add_argument("--session", required=True)

    parser.add_argument("--data-root", required=True, help="Root of parquet data used by phase1")
    parser.add_argument("--dataset-lock", required=False, default=None, help="Path to dataset lock json")
    parser.add_argument("--lock", default=None, help="Alias for --dataset-lock")

    parser.add_argument("--output-root", default="compiled_node")

    # Allow overriding stage scripts (but default to local filenames in the same dir)
    parser.add_argument("--phase1-script", default="compiler/phase1_multi_session_compile.py")
    parser.add_argument("--phase2-script", default="compiler/phase2_11_sessions_cluster_compile.py")
    parser.add_argument("--phase3-script", default="compiler/phase3_11_sessions_entry_windows.py")
    parser.add_argument("--phase4-script", default="compiler/phase4_11_sessions_oae.py")
    parser.add_argument("--phase5-script", default="compiler/phase5_11_sessions_separability.py")
    parser.add_argument("--phase6-script", default="compiler/stage6_11_sessions_odm.py")

    args = parser.parse_args()

    base_dir = Path(args.base_dir).resolve()

    dataset_lock_arg = args.lock if args.lock else args.dataset_lock
    if not dataset_lock_arg:
        raise SystemExit("compile_node.py: error: one of --dataset-lock or --lock is required")

    # Load dataset lock early (fail fast) and also capture into manifest.
    dataset_lock_path = Path(dataset_lock_arg)
    if not dataset_lock_path.is_absolute():
        # Allow callers to pass paths like "PC2/datasets/x.json" even when --base-dir is "PC2".
        # If the relative path already starts with the base dir name, don't double-prefix.
        parts = dataset_lock_path.parts
        if parts and parts[0] == base_dir.name:
            dataset_lock_path = (base_dir.parent / dataset_lock_path).resolve()
        else:
            dataset_lock_path = (base_dir / dataset_lock_path).resolve()
    _require_file(dataset_lock_path, "dataset_lock")
    dataset_lock = json.loads(dataset_lock_path.read_text())

    out_root = Path(args.output_root)
    if not out_root.is_absolute():
        out_root = (base_dir / out_root).resolve()
    node_dir = out_root / f"{args.pair}__{args.weekday}__{args.session}"
    node_dir.mkdir(parents=True, exist_ok=True)

    # Phase output directories inside node folder (deterministic layout)
    p1_dir = node_dir / "phase1"
    p2_dir = node_dir / "phase2"
    p3_dir = node_dir / "phase3"
    p4_dir = node_dir / "phase4"
    p5_dir = node_dir / "phase5"
    p6_dir = node_dir / "phase6"
    for d in (p1_dir, p2_dir, p3_dir, p4_dir, p5_dir, p6_dir):
        d.mkdir(parents=True, exist_ok=True)

    # Verify stage scripts exist.
    phase_scripts = {
        "phase1": Path(args.phase1_script),
        "phase2": Path(args.phase2_script),
        "phase3": Path(args.phase3_script),
        "phase4": Path(args.phase4_script),
        "phase5": Path(args.phase5_script),
        "phase6": Path(args.phase6_script),
    }
    for k, script in phase_scripts.items():
        script_path = (base_dir / script).resolve() if not script.is_absolute() else script
        _require_file(script_path, f"{k}_script")
        phase_scripts[k] = script_path

    # Validate data-root directory exists.
    data_root = Path(args.data_root)
    _require_dir(data_root, "data_root")

    # Ensure stage subprocesses can import both compiler/ and production/ modules.
    stage_env = dict(os.environ)
    add_paths = [str(base_dir), str(base_dir / "compiler"), str(base_dir / "production")]
    existing = stage_env.get("PYTHONPATH", "")
    stage_env["PYTHONPATH"] = os.pathsep.join([*add_paths, existing]) if existing else os.pathsep.join(add_paths)

    # Build commands (use python from current environment)
    # NOTE: phase2-5 defaults in scripts point to legacy dirs; we override inputs to match our node_dir layout.
    stage_runs: list[dict[str, Any]] = []
    overall_ok = True
    failure_stage: str | None = None

    # Phase1
    cmd1 = [
        sys.executable,
        str(phase_scripts["phase1"]),
        "--data-root",
        str(data_root),
        "--pair",
        args.pair,
        "--session-label",
        args.session,
        "--weekday-label",
        args.weekday,
        "--output-dir",
        str(p1_dir),
    ]
    r1 = _run_stage(cmd1, cwd=base_dir, env=stage_env)
    stage_runs.append({"stage": "phase1", **r1})
    if r1["returncode"] != 0:
        overall_ok = False
        failure_stage = "phase1"

    # Expected phase1 outputs
    p1_raw = p1_dir / "opportunity_map_raw.csv"

    if overall_ok:
        _require_file(p1_raw, "phase1_output opportunity_map_raw.csv")

        # Phase2
        cmd2 = [
            sys.executable,
            str(phase_scripts["phase2"]),
            "--input-csv",
            str(p1_raw),
            "--output-dir",
            str(p2_dir),
        ]
        r2 = _run_stage(cmd2, cwd=base_dir, env=stage_env)
        stage_runs.append({"stage": "phase2", **r2})
        if r2["returncode"] != 0:
            overall_ok = False
            failure_stage = "phase2"

    p2_clusters = p2_dir / "opportunity_clusters.csv"

    if overall_ok:
        _require_file(p2_clusters, "phase2_output opportunity_clusters.csv")

        # Phase3
        cmd3 = [
            sys.executable,
            str(phase_scripts["phase3"]),
            "--phase1-csv",
            str(p1_raw),
            "--clusters-csv",
            str(p2_clusters),
            "--output-dir",
            str(p3_dir),
        ]
        r3 = _run_stage(cmd3, cwd=base_dir, env=stage_env)
        stage_runs.append({"stage": "phase3", **r3})
        if r3["returncode"] != 0:
            overall_ok = False
            failure_stage = "phase3"

    p3_windows = p3_dir / "entry_window_states.csv"

    if overall_ok:
        _require_file(p3_windows, "phase3_output entry_window_states.csv")

        # Phase4
        cmd4 = [
            sys.executable,
            str(phase_scripts["phase4"]),
            "--phase1-csv",
            str(p1_raw),
            "--output-dir",
            str(p4_dir),
        ]
        r4 = _run_stage(cmd4, cwd=base_dir, env=stage_env)
        stage_runs.append({"stage": "phase4", **r4})
        if r4["returncode"] != 0:
            overall_ok = False
            failure_stage = "phase4"

    p4_labeled = p4_dir / "opportunity_zones_labeled.csv"

    if overall_ok:
        _require_file(p4_labeled, "phase4_output opportunity_zones_labeled.csv")

        # Phase5
        cmd5 = [
            sys.executable,
            str(phase_scripts["phase5"]),
            "--labeled-csv",
            str(p4_labeled),
            "--output-dir",
            str(p5_dir),
        ]
        r5 = _run_stage(cmd5, cwd=base_dir, env=stage_env)
        stage_runs.append({"stage": "phase5", **r5})
        if r5["returncode"] != 0:
            overall_ok = False
            failure_stage = "phase5"

    if overall_ok:
        # Phase6 expects legacy defaults; override to our node_dir layout.
        cmd6 = [
            sys.executable,
            str(phase_scripts["phase6"]),
            "--clusters-csv",
            str(p2_clusters),
            "--entry-windows-csv",
            str(p3_windows),
            "--labeled-csv",
            str(p4_labeled),
            "--dataset-lock",
            str(dataset_lock_path),
            "--output-dir",
            str(p6_dir),
        ]
        r6 = _run_stage(cmd6, cwd=base_dir, env=stage_env)
        stage_runs.append({"stage": "phase6", **r6})
        if r6["returncode"] != 0:
            overall_ok = False
            failure_stage = "phase6"

    # Write summary + manifest (always)
    summary = {
        "ts_utc": _utc_now_iso(),
        "pair": args.pair,
        "weekday": args.weekday,
        "session": args.session,
        "node_dir": str(node_dir),
        "data_root": str(data_root),
        "dataset_lock": {
            "path": str(dataset_lock_path),
            "sha256": _sha256_file(dataset_lock_path),
            "session_count": dataset_lock.get("session_count"),
        },
        "stages": stage_runs,
        "ok": overall_ok,
        "failure_stage": failure_stage,
    }
    (node_dir / "compile_summary.json").write_text(json.dumps(summary, indent=2))

    # compile_manifest.json = hashes of produced outputs only (not huge data inputs)
    produced_files: list[Path] = []
    for rel in [
        "phase1/opportunity_map_raw.csv",
        "phase1/opportunity_map_summary.json",
        "phase1/opportunity_map_audit.json",
        "phase2/opportunity_clusters.csv",
        "phase2/cluster_summary.json",
        "phase2/cluster_audit.json",
        "phase3/entry_window_states.csv",
        "phase3/entry_window_summary.json",
        "phase3/entry_window_audit.json",
        "phase4/opportunity_zones_labeled.csv",
        "phase4/zone_label_summary.json",
        "phase4/zone_label_audit.json",
        "phase5/zone_label_separability.json",
        "phase6/odm_ceiling_report.json",
        "phase6/odm_audit.json",
        "phase6/cluster_resolved_labels.csv",
        "compile_summary.json",
    ]:
        p = node_dir / rel
        if p.exists() and p.is_file():
            produced_files.append(p)

    manifest = {
        "ts_utc": _utc_now_iso(),
        "node_dir": str(node_dir),
        "files": [
            {
                "path": str(p.relative_to(node_dir)),
                "bytes": p.stat().st_size,
                "sha256": _sha256_file(p),
            }
            for p in sorted(produced_files)
        ],
    }
    (node_dir / "compile_manifest.json").write_text(json.dumps(manifest, indent=2))

    # Print minimal console status
    if overall_ok:
        print(f"OK: compiled node -> {node_dir}")
        return 0

    print(f"FAIL: stage={failure_stage} node={node_dir}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
