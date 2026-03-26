#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path


ROOT = Path(__file__).resolve().parent
COMPILED_ROOT = ROOT / "compiled_market_nodes"
TEMPLATE_ROOT = ROOT / "compiled_session_templates"
MAJORS = (
    "AUD_USD",
    "EUR_USD",
    "GBP_USD",
    "NZD_USD",
    "USD_CAD",
    "USD_CHF",
    "USD_JPY",
)
WEEKDAYS = {"thursday", "friday"}


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def choose_route(report: dict) -> str:
    route = report.get("recommended_failure_route") or "quality_repair"
    issues = {
        item.get("issue") if isinstance(item, dict) else str(item)
        for item in report.get("issues", [])
    }
    # Escalate thin / missing-side "quality" failures into real local rebuilds.
    if route == "quality_repair" and (
        "missing_directional_coverage" in issues
        or "pathological_best_class_trade_count" in issues
        or "ultra_thin_best_class_trade_count" in issues
        or "underutilized_expected_direction" in issues
        or (
            "below_symmetric_break_even" in issues
            and "side_trade_count_too_low" in issues
        )
        or (
            "below_symmetric_break_even" in issues
            and "side_density_too_low" in issues
        )
    ):
        return "state_surface_rebuild"
    return route


def find_failing_major_nodes() -> list[dict]:
    rows: list[dict] = []
    for node_dir in sorted(COMPILED_ROOT.iterdir()):
        if not node_dir.is_dir():
            continue
        parts = node_dir.name.split("__")
        if len(parts) != 3:
            continue
        pair, weekday, session = parts
        if pair not in MAJORS or weekday not in WEEKDAYS:
            continue
        report_path = node_dir / "session_performance_check" / "session_performance_check_report.json"
        if not report_path.exists():
            continue
        report = load_json(report_path)
        if report.get("status") == "PASS":
            continue
        rows.append(
            {
                "node": node_dir.name,
                "pair": pair,
                "weekday": weekday,
                "session": session,
                "lock": f"dataset_lock__{pair.lower()}__{weekday}__{session}__11.json",
                "route": choose_route(report),
                "issues": [
                    item.get("issue") if isinstance(item, dict) else str(item)
                    for item in report.get("issues", [])
                ],
            }
        )
    return rows


def run_node(lock: str, route: str) -> dict:
    cmd = [
        "python3",
        "run_market_node_compiler.py",
        "--dataset-lock",
        lock,
        "--output-root",
        "compiled_market_nodes",
        "--template-root",
        "compiled_session_templates",
        "--pipeline-mode",
        "entry-only",
        "--batch-compile",
        "--force-heavy-delta-optimize",
        "--failure-route-override",
        route,
    ]
    proc = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    return {
        "lock": lock,
        "route": route,
        "returncode": proc.returncode,
        "stdout_tail": proc.stdout[-1200:],
        "stderr_tail": proc.stderr[-1200:],
    }


def summarize() -> dict:
    status_counter: Counter[str] = Counter()
    pair_counter: Counter[str] = Counter()
    for node_dir in COMPILED_ROOT.iterdir():
        if not node_dir.is_dir():
            continue
        parts = node_dir.name.split("__")
        if len(parts) != 3:
            continue
        pair, weekday, _session = parts
        if pair not in MAJORS or weekday not in WEEKDAYS:
            continue
        report_path = node_dir / "session_performance_check" / "session_performance_check_report.json"
        if not report_path.exists():
            continue
        report = load_json(report_path)
        status = report.get("status") or "UNKNOWN"
        status_counter[status] += 1
        if status != "PASS":
            pair_counter[pair] += 1
    return {
        "status_counts": dict(status_counter),
        "remaining_by_pair": dict(pair_counter),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    rows = find_failing_major_nodes()
    if args.limit is not None:
        rows = rows[: args.limit]

    print(json.dumps({"status": "STARTED", "failing_nodes": len(rows), "workers": args.workers}, indent=2))
    if not rows:
        print(json.dumps({"status": "DONE", "summary": summarize()}, indent=2))
        return 0

    failures = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [executor.submit(run_node, row["lock"], row["route"]) for row in rows]
        for future in as_completed(futures):
            result = future.result()
            compact = {k: v for k, v in result.items() if k not in {"stdout_tail", "stderr_tail"}}
            print(json.dumps(compact, indent=2))
            if result["returncode"] != 0:
                failures.append(result)

    summary = summarize()
    payload = {"status": "DONE", "summary": summary, "failures": len(failures)}
    print(json.dumps(payload, indent=2))
    if failures:
        print(json.dumps(failures[:5], indent=2))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
