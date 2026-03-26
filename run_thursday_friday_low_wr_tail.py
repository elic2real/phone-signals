#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import json
import subprocess
import time
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parent
COMPILED_ROOT = ROOT / "compiled_market_nodes"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def iter_reports() -> list[Path]:
    reports = list(COMPILED_ROOT.glob("*__thursday__*/session_performance_check/session_performance_check_report.json"))
    reports += list(COMPILED_ROOT.glob("*__friday__*/session_performance_check/session_performance_check_report.json"))
    return sorted(reports)


def collect_ranked_nodes(limit: int | None) -> list[tuple[str, Path]]:
    ranked: list[tuple[float, str, Path]] = []
    for rep in iter_reports():
        data = load_json(rep)
        if data.get("status") != "REPAIR_REQUIRED":
            continue
        node = rep.parent.parent.name
        pair, weekday, session = node.split("__")
        lock = ROOT / f"dataset_lock__{pair.lower()}__{weekday}__{session}__11.json"
        if not lock.exists():
            continue
        worst = 1.0
        for _, payload in (data.get("sides") or {}).items():
            try:
                wr = float(payload.get("effective_win_rate", payload.get("entry_win_rate", 1.0)) or 0.0)
            except Exception:
                wr = 1.0
            if wr < worst:
                worst = wr
        ranked.append((worst, node, lock))
    ranked.sort(key=lambda x: (x[0], x[1]))
    if limit is not None:
        ranked = ranked[:limit]
    return [(node, lock) for _, node, lock in ranked]


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
        print(f"{node} fail {elapsed:.1f}s", flush=True)
        print(proc.stdout[-3000:], flush=True)
    else:
        print(f"{node} ok {elapsed:.1f}s", flush=True)
    return node, proc.returncode, elapsed


def refresh_stale_reports() -> int:
    refreshed = 0
    for rep in iter_reports():
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
                str(base / "aee_target_local_fixedpop" / "target_local_fixedpop_aee_trade_rows.json"),
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
    for rep in iter_reports():
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
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--limit", type=int)
    args = ap.parse_args()

    locks = collect_ranked_nodes(args.limit)
    print(json.dumps({"mode": "low_wr_first", "repairing": len(locks), "limit": args.limit}, indent=2))
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
                "mode": "low_wr_first",
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
