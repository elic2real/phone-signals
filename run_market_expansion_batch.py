#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = ROOT / "compiled_market_nodes"
DEFAULT_LOCK_DIR = ROOT
DEFAULT_TEMPLATE_ROOT = ROOT / "compiled_session_templates"

DEFAULT_PAIRS = [
    "EUR_USD",
    "GBP_USD",
    "AUD_USD",
    "NZD_USD",
    "USD_CAD",
    "USD_CHF",
    "USD_JPY",
    "EUR_GBP",
    "EUR_JPY",
    "GBP_JPY",
    "AUD_JPY",
    "NZD_JPY",
    "CHF_JPY",
    "EUR_CHF",
    "AUD_CAD",
    "GBP_CHF",
]
DEFAULT_WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday"]
DEFAULT_SESSIONS = ["london", "new_york", "asia", "sydney"]


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


def node_key(pair: str, weekday: str, session: str) -> str:
    return f"{pair}__{weekday}__{session}"


def ensure_lock(pair: str, weekday: str, session: str, session_count: int, date_selection: str) -> tuple[bool, Path, str]:
    lock_path = DEFAULT_LOCK_DIR / f"dataset_lock__{pair.lower()}__{weekday}__{session}__{session_count}.json"
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
            str(session_count),
            "--date-selection",
            date_selection,
            "--lock-path",
            str(lock_path),
        ]
    )
    return ok, lock_path, output


def run_node(
    pair: str,
    weekday: str,
    session: str,
    *,
    session_count: int,
    output_root: Path,
    template_root: Path,
    pipeline_mode: str,
    batch_compile: bool,
    force_heavy_delta_optimize: bool,
    historical_fast: bool,
    research_lite: bool,
    research_max_sessions: int,
    research_row_stride: int,
    research_max_rows_per_session: int,
    date_selection: str,
    batch_tiny_sample: bool,
) -> dict[str, Any]:
    key = node_key(pair, weekday, session)
    t0 = time.time()
    lock_ok, lock_path, lock_output = ensure_lock(pair, weekday, session, session_count, date_selection)
    if not lock_ok:
        return {
            "node": key,
            "phase": pipeline_mode,
            "status": "LOCK_FAIL",
            "duration_sec": round(time.time() - t0, 4),
            "error": lock_output[-4000:],
        }
    cmd = [
        "python3",
        "run_market_node_compiler.py",
        "--dataset-lock",
        str(lock_path),
        "--output-root",
        str(output_root),
        "--template-root",
        str(template_root),
        "--pipeline-mode",
        pipeline_mode,
        *(["--batch-compile"] if batch_compile else []),
        *(["--force-heavy-delta-optimize"] if force_heavy_delta_optimize else []),
        *(["--batch-tiny-sample"] if batch_tiny_sample else []),
        *(["--historical-fast"] if historical_fast else []),
        *(["--research-lite"] if research_lite else []),
        "--research-max-sessions",
        str(research_max_sessions),
        "--research-row-stride",
        str(research_row_stride),
        "--research-max-rows-per-session",
        str(research_max_rows_per_session),
    ]
    ok, output = run_captured(cmd)
    duration = round(time.time() - t0, 4)
    if not ok:
        return {
            "node": key,
            "phase": pipeline_mode,
            "status": "FAIL",
            "duration_sec": duration,
            "error": output[-4000:],
        }
    try:
        payload = json.loads(output.strip().splitlines()[-1])
    except Exception:
        payload = {"status": "PASS"}
    payload["node"] = key
    payload["phase"] = pipeline_mode
    payload["duration_sec"] = duration
    payload["lock_path"] = str(lock_path)
    return payload


def cheap_phase_node(
    pair: str,
    weekday: str,
    session: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    return run_node(
        pair,
        weekday,
        session,
        session_count=args.session_count,
        output_root=args.output_root,
        template_root=args.template_root,
        pipeline_mode="entry-only",
        batch_compile=True,
        force_heavy_delta_optimize=False,
        historical_fast=args.historical_fast,
        research_lite=args.research_lite,
        research_max_sessions=args.research_max_sessions,
        research_row_stride=args.research_row_stride,
        research_max_rows_per_session=args.research_max_rows_per_session,
        date_selection=args.date_selection,
        batch_tiny_sample=args.batch_tiny_sample,
    )


def heavy_entry_phase_node(
    pair: str,
    weekday: str,
    session: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    return run_node(
        pair,
        weekday,
        session,
        session_count=args.session_count,
        output_root=args.output_root,
        template_root=args.template_root,
        pipeline_mode="entry-only",
        batch_compile=True,
        force_heavy_delta_optimize=True,
        historical_fast=args.historical_fast,
        research_lite=args.research_lite,
        research_max_sessions=args.research_max_sessions,
        research_row_stride=args.research_row_stride,
        research_max_rows_per_session=args.research_max_rows_per_session,
        date_selection=args.date_selection,
        batch_tiny_sample=args.batch_tiny_sample,
    )


def downstream_phase_node(
    pair: str,
    weekday: str,
    session: str,
    args: argparse.Namespace,
) -> dict[str, Any]:
    return run_node(
        pair,
        weekday,
        session,
        session_count=args.session_count,
        output_root=args.output_root,
        template_root=args.template_root,
        pipeline_mode="downstream-only",
        batch_compile=args.batch_compile,
        force_heavy_delta_optimize=False,
        historical_fast=args.historical_fast,
        research_lite=args.research_lite,
        research_max_sessions=args.research_max_sessions,
        research_row_stride=args.research_row_stride,
        research_max_rows_per_session=args.research_max_rows_per_session,
        date_selection=args.date_selection,
        batch_tiny_sample=False,
    )


def parse_jobs(args: argparse.Namespace) -> list[tuple[str, str, str]]:
    weekdays = args.weekdays
    if args.weekday_pairs:
        weekdays = []
        for pair in args.weekday_pairs:
            left, right = pair.split("+", 1) if "+" in pair else (pair, "")
            weekdays.append(left)
            if right:
                weekdays.append(right)
    jobs = [
        (pair, weekday, session)
        for session in args.sessions
        for weekday in weekdays
        for pair in args.pairs
    ]
    if args.limit:
        jobs = jobs[: args.limit]
    return jobs


def node_tuple_from_key(node: str) -> tuple[str, str, str]:
    pair, weekday, session = node.split("__", 2)
    return pair, weekday, session


def collect_template_builds(
    jobs: list[tuple[str, str, str]],
    output_root: Path,
    template_root: Path,
    workers: int,
) -> list[dict[str, Any]]:
    seen = set()
    unique_pairs_sessions = []
    for pair, _, session in jobs:
        key = (pair, session)
        if key in seen:
            continue
        seen.add(key)
        unique_pairs_sessions.append((pair, session))
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = {
            executor.submit(
                run_captured,
                [
                    "python3",
                    "session_template.py",
                    "--pair",
                    pair,
                    "--session",
                    session,
                    "--output-root",
                    str(output_root),
                    "--template-root",
                    str(template_root),
                ],
            ): (pair, session)
            for pair, session in unique_pairs_sessions
        }
        for future in concurrent.futures.as_completed(futures):
            pair, session = futures[future]
            ok, output = future.result()
            results.append({"pair": pair, "session": session, "status": "PASS" if ok else "FAIL", "output_tail": output[-2000:]})
    return results


def main() -> None:
    ap = argparse.ArgumentParser(description="Template-first batch runner for pair x weekday x session market nodes.")
    ap.add_argument("--pairs", nargs="*", default=DEFAULT_PAIRS)
    ap.add_argument("--weekdays", nargs="*", default=DEFAULT_WEEKDAYS)
    ap.add_argument("--weekday-pairs", nargs="*", default=[])
    ap.add_argument("--sessions", nargs="*", default=DEFAULT_SESSIONS)
    ap.add_argument("--session-count", type=int, default=11)
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    ap.add_argument("--template-root", type=Path, default=DEFAULT_TEMPLATE_ROOT)
    ap.add_argument("--date-selection", choices=["oldest", "newest"], default="newest")
    ap.add_argument("--historical-fast", action="store_true", default=False)
    ap.add_argument("--research-lite", action="store_true", default=False)
    ap.add_argument("--research-max-sessions", type=int, default=3)
    ap.add_argument("--research-row-stride", type=int, default=3)
    ap.add_argument("--research-max-rows-per-session", type=int, default=180)
    ap.add_argument("--cheap-workers", type=int, default=max(1, min(8, os.cpu_count() or 4)))
    ap.add_argument("--heavy-workers", type=int, default=3)
    ap.add_argument("--batch-compile", action="store_true", default=True)
    ap.add_argument("--batch-tiny-sample", action="store_true", default=True)
    args = ap.parse_args()

    jobs = parse_jobs(args)
    batch_start = time.time()
    template_results = collect_template_builds(jobs, args.output_root, args.template_root, args.cheap_workers)

    cheap_results: list[dict[str, Any]] = []
    heavy_results: list[dict[str, Any]] = []
    downstream_results: list[dict[str, Any]] = []
    heavy_queue: list[tuple[str, str, str]] = []
    downstream_queue: list[tuple[str, str, str]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.cheap_workers) as executor:
        futures = {
            executor.submit(cheap_phase_node, pair, weekday, session, args): (pair, weekday, session)
            for pair, weekday, session in jobs
        }
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            cheap_results.append(result)
            if result.get("node_class") == "heavy_delta" or result.get("status") == "HEAVY_DELTA":
                heavy_queue.append(node_tuple_from_key(result["node"]))
            elif result.get("node_class") in {"accept", "light_delta"} or result.get("status") == "ENTRY_PASS":
                downstream_queue.append(node_tuple_from_key(result["node"]))

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.heavy_workers) as executor:
        futures = {
            executor.submit(heavy_entry_phase_node, pair, weekday, session, args): (pair, weekday, session)
            for pair, weekday, session in heavy_queue
        }
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            heavy_results.append(result)
            if result.get("node_class") in {"accept", "light_delta"} or result.get("status") == "ENTRY_PASS":
                downstream_queue.append(node_tuple_from_key(result["node"]))

    downstream_seen = set()
    downstream_queue = [job for job in downstream_queue if not (job in downstream_seen or downstream_seen.add(job))]
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.heavy_workers) as executor:
        futures = {
            executor.submit(downstream_phase_node, pair, weekday, session, args): (pair, weekday, session)
            for pair, weekday, session in downstream_queue
        }
        for future in concurrent.futures.as_completed(futures):
            downstream_results.append(future.result())

    report = {
        "status": "PASS",
        "timestamp": time.time(),
        "duration_sec": round(time.time() - batch_start, 4),
        "cheap_workers": args.cheap_workers,
        "heavy_workers": args.heavy_workers,
        "jobs": [node_key(*job) for job in jobs],
        "template_results": template_results,
        "cheap_phase": cheap_results,
        "heavy_phase": heavy_results,
        "downstream_phase": downstream_results,
    }
    (ROOT / "market_expansion_batch_report.json").write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
