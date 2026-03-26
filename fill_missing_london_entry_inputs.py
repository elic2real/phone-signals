#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def run_captured(cmd: list[str]) -> tuple[bool, str]:
    proc = subprocess.run(
        cmd,
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    output = (proc.stdout or "") + (proc.stderr or "")
    return proc.returncode == 0, output


def fill_node(node: str) -> dict[str, str]:
    pair, weekday, session = node.split("__")
    lock_path = ROOT / f"dataset_lock__{pair.lower()}__{weekday}__{session}__11.json"
    ok, output = run_captured(
        [
            "python3",
            "build_market_node_dataset_lock.py",
            "--pair",
            pair,
            "--weekday",
            weekday,
            "--session",
            session,
            "--session-count",
            "11",
            "--lock-path",
            str(lock_path),
        ]
    )
    if not ok:
        return {"node": node, "status": "LOCK_FAIL", "error": output[-4000:]}

    node_dir = ROOT / "compiled_market_nodes" / node
    data_root = ROOT / "market_node_data" / f"{pair.lower()}__{weekday}__{session}__11"
    stage_dir = node_dir / "target_entry_stage"
    no_timeout_dir = node_dir / "target_entry_no_timeouts"

    ok, output = run_captured(
        [
            "python3",
            "run_target_entry_stage_compiler.py",
            "--dataset-lock",
            str(lock_path),
            "--data-root",
            str(data_root),
            "--output-dir",
            str(stage_dir),
            "--historical-fast",
            "--research-max-sessions",
            "3",
            "--research-row-stride",
            "3",
            "--research-max-rows-per-session",
            "180",
        ]
    )
    if not ok:
        return {"node": node, "status": "STAGE_FAIL", "error": output[-4000:]}

    ok, output = run_captured(
        [
            "python3",
            "run_target_entry_no_timeout.py",
            "--base-rules",
            str(stage_dir / "target_contextual_v2" / "target_entry_classes.json"),
            "--targeted-rules",
            str(stage_dir / "target_contextual_v2_targeted" / "target_entry_classes.json"),
            "--truth-csv",
            str(stage_dir / "target_contextual_v2" / "target_entry_truth_table.csv"),
            "--output-dir",
            str(no_timeout_dir),
            "--historical-fast",
        ]
    )
    if not ok:
        return {"node": node, "status": "NO_TIMEOUT_FAIL", "error": output[-4000:]}
    return {"node": node, "status": "PASS"}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=Path, default=ROOT / "session_calibration_backfill_report.json")
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()

    payload = json.loads(args.report.read_text())
    nodes = [row["node"] for row in payload["results"] if row["status"] == "MISSING_INPUTS"]
    results: list[dict[str, str]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(fill_node, node) for node in nodes]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())
    results.sort(key=lambda x: x["node"])
    out = {"status": "PASS", "results": results}
    (ROOT / "fill_missing_london_entry_inputs_report.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
