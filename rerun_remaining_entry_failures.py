#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
OUTPUT_ROOT = ROOT / "compiled_market_nodes"
DEFAULT_REPORT = ROOT / "artifacts" / "remaining_entry_failure_rerun_report.json"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def issue_names(report: dict) -> list[str]:
    return sorted({item.get("issue") for item in (report.get("issues") or []) if isinstance(item, dict) and item.get("issue")})


def find_targets(output_root: Path) -> list[str]:
    targets: list[str] = []
    for node_dir in sorted(path for path in output_root.iterdir() if path.is_dir()):
        manifest_path = node_dir / "node_manifest.json"
        perf_path = node_dir / "session_performance_check" / "session_performance_check_report.json"
        if not manifest_path.exists() or not perf_path.exists():
            continue
        manifest = load_json(manifest_path)
        performance = load_json(perf_path)
        if manifest.get("pipeline_mode") != "entry-only":
            continue
        if performance.get("status") == "PASS":
            continue
        targets.append(node_dir.name)
    return targets


def dataset_lock_for_node(node_name: str) -> Path:
    pair, weekday, session = node_name.split("__")
    return ROOT / f"dataset_lock__{pair.lower()}__{weekday}__{session}__11.json"


def snapshot_node(output_root: Path, node_name: str) -> dict:
    node_dir = output_root / node_name
    manifest = load_json(node_dir / "node_manifest.json")
    performance = load_json(node_dir / "session_performance_check" / "session_performance_check_report.json")
    return {
        "node": node_name,
        "node_class": manifest.get("node_class"),
        "failure_route": manifest.get("failure_route"),
        "reason": manifest.get("reason"),
        "perf_status": performance.get("status"),
        "issue_names": issue_names(performance),
    }


def write_report(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=OUTPUT_ROOT)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--nodes", nargs="*", default=[])
    args = parser.parse_args()
    output_root = args.output_root
    targets = args.nodes or find_targets(output_root)
    if args.limit > 0:
        targets = targets[: args.limit]

    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "targets": targets,
        "results": [],
    }
    write_report(args.report, report)

    for node_name in targets:
        lock_path = dataset_lock_for_node(node_name)
        cmd = [
            sys.executable,
            "run_market_node_compiler.py",
            "--dataset-lock",
            str(lock_path),
            "--output-root",
            str(output_root),
            "--pipeline-mode",
            "entry-only",
            "--force-heavy-delta-optimize",
        ]
        started_at = datetime.now(timezone.utc).isoformat()
        proc = subprocess.run(
            cmd,
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        result = {
            "node": node_name,
            "dataset_lock": str(lock_path),
            "started_at": started_at,
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "returncode": proc.returncode,
            **snapshot_node(output_root, node_name),
        }
        report["results"].append(result)
        write_report(args.report, report)

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    report["remaining_failures"] = [item for item in report["results"] if item.get("perf_status") != "PASS"]
    write_report(args.report, report)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
