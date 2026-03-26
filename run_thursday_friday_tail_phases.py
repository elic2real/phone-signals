#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import json
import subprocess
import sys
import time
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parent
COMPILED_ROOT = ROOT / "compiled_market_nodes"


PHASES: dict[str, str] = {
    "phase1_exclude_thursday_london": "Exclude Thursday London nodes; repair the rest of Thursday/Friday.",
    "phase2_exclude_thursday_london_new_york": "Exclude Thursday London and New York nodes; repair the rest.",
    "phase3_focus_asia_sydney_friday": "Only focus on Friday nodes and Thursday Asia/Sydney nodes.",
}


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def iter_thursday_friday_reports() -> list[Path]:
    reports = list(COMPILED_ROOT.glob("*__thursday__*/session_performance_check/session_performance_check_report.json"))
    reports += list(COMPILED_ROOT.glob("*__friday__*/session_performance_check/session_performance_check_report.json"))
    return sorted(reports)


def include_node(phase: str, weekday: str, session: str) -> bool:
    weekday = weekday.lower()
    session = session.lower()
    if phase == "phase1_exclude_thursday_london":
        return not (weekday == "thursday" and session == "london")
    if phase == "phase2_exclude_thursday_london_new_york":
        return not (weekday == "thursday" and session in {"london", "new_york"})
    if phase == "phase3_focus_asia_sydney_friday":
        return weekday == "friday" or session in {"asia", "sydney"}
    raise ValueError(f"unknown phase: {phase}")


def collect_locks(phase: str) -> list[tuple[str, Path]]:
    locks: list[tuple[str, Path]] = []
    for rep in iter_thursday_friday_reports():
        data = load_json(rep)
        if data.get("status") != "REPAIR_REQUIRED":
            continue
        node = rep.parent.parent.name
        pair, weekday, session = node.split("__")
        if not include_node(phase, weekday, session):
            continue
        lock = ROOT / f"dataset_lock__{pair.lower()}__{weekday}__{session}__11.json"
        if lock.exists():
            locks.append((node, lock))
    return locks


def run_node(node: str, lock: Path) -> tuple[str, int, float]:
    started = time.time()
    proc = subprocess.run(
        [
            "python3",
            "run_market_node_compiler.py",
            "--dataset-lock",
            str(lock),
            "--output-root",
            "compiled_market_nodes",
            "--historical-fast",
            "--pipeline-mode",
            "entry-only",
            "--batch-compile",
            "--force-heavy-delta-optimize",
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    elapsed = time.time() - started
    if proc.returncode != 0:
        tail = proc.stdout[-3000:]
        print(f"{node} fail {elapsed:.1f}s", flush=True)
        print(tail, flush=True)
    else:
        print(f"{node} ok {elapsed:.1f}s", flush=True)
    return node, proc.returncode, elapsed


def refresh_stale_reports() -> int:
    refreshed = 0
    for rep in iter_thursday_friday_reports():
        base = rep.parent.parent
        pop = base / "target_entry_no_timeouts" / "target_entry_population.csv"
        if not pop.exists() or pop.stat().st_mtime <= rep.stat().st_mtime:
            continue
        pair, weekday, session = base.name.split("__")
        lock = ROOT / f"dataset_lock__{pair.lower()}__{weekday}__{session}__11.json"
        if not lock.exists():
            continue
        proc = subprocess.run(
            [
                "python3",
                "session_performance_check.py",
                "--dataset-lock",
                str(lock),
                "--entry-population-csv",
                str(pop),
                "--output-dir",
                str(base / "session_performance_check"),
                "--trade-rows-json",
                str(base / "aee_target_local_fixedpop" / "aee_target_local_fixedpop_trade_rows.json"),
                "--session-potential-json",
                str(base / "session_potential" / "session_potential_report.json"),
                "--session-calibration-json",
                str(base / "session_calibration" / "session_calibration_report.json"),
            ],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if proc.returncode == 0:
            refreshed += 1
    return refreshed


def summarize() -> tuple[Counter, Counter, Counter]:
    status = Counter()
    pairs = Counter()
    issues = Counter()
    for rep in iter_thursday_friday_reports():
        data = load_json(rep)
        st = data.get("status", "UNKNOWN")
        status[st] += 1
        if st == "REPAIR_REQUIRED":
            node = rep.parent.parent.name
            pairs[node.split("__")[0]] += 1
            for issue in data.get("issues", []):
                if isinstance(issue, dict):
                    issues[issue.get("issue", "?")] += 1
                else:
                    issues[str(issue)] += 1
    return status, pairs, issues


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=sorted(PHASES), required=True)
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()

    locks = collect_locks(args.phase)
    print(json.dumps({"phase": args.phase, "description": PHASES[args.phase], "repairing": len(locks)}, indent=2))
    if not locks:
        status, pairs, issues = summarize()
        print(json.dumps({"status": dict(status), "top_pairs": pairs.most_common(), "top_issues": issues.most_common()}, indent=2))
        return 0

    failures = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        for _, code, _ in ex.map(lambda item: run_node(*item), locks):
            if code != 0:
                failures += 1

    refreshed = refresh_stale_reports()
    status, pairs, issues = summarize()
    print(
        json.dumps(
            {
                "phase": args.phase,
                "repaired_nodes": len(locks),
                "node_failures": failures,
                "refreshed_reports": refreshed,
                "status": dict(status),
                "top_pairs": pairs.most_common(),
                "top_issues": issues.most_common(),
            },
            indent=2,
        )
    )
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
