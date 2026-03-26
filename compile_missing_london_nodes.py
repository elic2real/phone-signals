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


def compile_node(node: str) -> dict[str, str]:
    pair, weekday, session = node.split("__")
    lock_path = ROOT / f"dataset_lock__{pair.lower()}__{weekday}__{session}__11.json"
    lock_ok, lock_output = run_captured(
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
    if not lock_ok:
        return {"node": node, "status": "LOCK_FAIL", "error": lock_output[-4000:]}

    ok, output = run_captured(
        [
            "python3",
            "run_market_node_compiler.py",
            "--dataset-lock",
            str(lock_path),
            "--output-root",
            str(ROOT / "compiled_market_nodes"),
            "--historical-fast",
            "--research-max-sessions",
            "3",
            "--research-row-stride",
            "3",
            "--research-max-rows-per-session",
            "180",
        ]
    )
    if ok:
        return {"node": node, "status": "PASS"}
    return {"node": node, "status": "FAIL", "error": output[-4000:]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", type=Path, default=ROOT / "session_calibration_backfill_report.json")
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()

    payload = json.loads(args.report.read_text())
    nodes = [row["node"] for row in payload["results"] if row["status"] == "MISSING_INPUTS"]
    results: list[dict[str, str]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(compile_node, node) for node in nodes]
        for future in concurrent.futures.as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda x: x["node"])
    out = {"status": "PASS", "results": results}
    (ROOT / "compile_missing_london_nodes_report.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
