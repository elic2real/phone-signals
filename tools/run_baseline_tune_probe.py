#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime, timezone


def _count_events(path: Path, kinds: set[str]) -> dict:
    out = {k: 0 for k in sorted(kinds)}
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            o = json.loads(line)
        except Exception:
            continue
        k = str(o.get("kind") or "").upper()
        if k in kinds:
            out[k] += 1
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-minutes", type=float, default=10.0)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    run_sec = max(5.0, float(args.run_minutes) * 60.0)
    cmd = ["/usr/bin/python3", "phone_bot.py", "--log-proof"]
    env = dict(**__import__("os").environ)
    env["PHONE_BOT_BASE_DIR"] = str(Path.cwd())
    env["PHONE_BOT_LOG_DIR"] = str(Path.cwd() / "logs")
    env["OUTCOME_ACCELERATOR"] = env.get("OUTCOME_ACCELERATOR", "1")
    env["OA_MIN_HOLD_SEC"] = env.get("OA_MIN_HOLD_SEC", "30")
    env["OA_MAX_HOLD_SEC"] = env.get("OA_MAX_HOLD_SEC", "120")

    t0 = time.time()
    # log-proof is internally bounded (~8s). Loop until requested wall time elapses.
    loops = 0
    rc = 0
    failures = 0
    last_stderr = ""
    deadline = t0 + run_sec
    while time.time() < deadline:
        loops += 1
        p = subprocess.run(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        rc = p.returncode
        if rc != 0:
            # Continue probing the full time window even if individual launches fail.
            failures += 1
            last_stderr = (p.stderr or "").strip()[-400:]
            rc = 0
            time.sleep(1.0)
            continue
        # Brief pause prevents tight spin if log-proof exits faster than expected.
        time.sleep(0.25)

    run_seconds_elapsed = time.time() - t0

    kinds = {
        "ENTRY_GATE_EVAL",
        "ENTRY_ATTEMPT",
        "ENTRY_RESULT",
        "EXIT_RESULT",
        "TUNE_APPLIED",
        "AEE_TUNE_APPLIED",
        "SEED_LOADED",
        "OA_FORCE_CLOSE_TRIGGER",
    }
    counts = _count_events(Path("logs/trades.jsonl"), kinds)
    run_hours = run_seconds_elapsed / 3600.0
    artifact = {
        "run_minutes_requested": args.run_minutes,
        "run_seconds_elapsed": run_seconds_elapsed,
        "run_hours": run_hours,
        "loops": loops,
        "rc": rc,
        "failures": failures,
        "last_stderr_tail": last_stderr,
        "counts": counts,
        "orders_sent_per_h": counts.get("ENTRY_ATTEMPT", 0) / run_hours if run_hours > 0 else 0,
        "fills_per_h": counts.get("ENTRY_RESULT", 0) / run_hours if run_hours > 0 else 0,
        "entry_result_per_h": counts.get("ENTRY_RESULT", 0) / run_hours if run_hours > 0 else 0,
        "exit_result_per_h": counts.get("EXIT_RESULT", 0) / run_hours if run_hours > 0 else 0,
        "pass_minimum": {
            "ENTRY_GATE_EVAL_gt_0": counts.get("ENTRY_GATE_EVAL", 0) > 0,
            "TUNE_APPLIED_gt_0": counts.get("TUNE_APPLIED", 0) > 0,
            "AEE_TUNE_APPLIED_gte_0": counts.get("AEE_TUNE_APPLIED", 0) >= 0,
        },
    }
    apath = Path(args.output)
    apath.parent.mkdir(parents=True, exist_ok=True)
    apath.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
    print(json.dumps(artifact, indent=2))
    return 0 if rc == 0 else rc


if __name__ == "__main__":
    raise SystemExit(main())
