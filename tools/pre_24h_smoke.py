#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable


def _iter_events(paths: Iterable[Path]):
    for p in paths:
        if not p.exists():
            continue
        with p.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except Exception:
                    continue


def _parse_ts(e: Dict[str, Any]) -> float:
    tsu = e.get("ts_utc")
    if isinstance(tsu, str):
        try:
            return datetime.fromisoformat(tsu.replace("Z", "+00:00")).timestamp()
        except Exception:
            pass
    ts = e.get("ts")
    if isinstance(ts, str):
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
        except Exception:
            pass
    return 0.0


def main() -> int:
    ap = argparse.ArgumentParser(description="Run pre-24h non-proof smoke and enforce hard gates.")
    ap.add_argument("--minutes", type=int, default=12)
    ap.add_argument("--require-filled", type=int, default=1)
    ap.add_argument("--active-artifacts", default="calibration/active/ACTIVE_ARTIFACTS.json")
    ap.add_argument("--out-dir", default="proof_artifacts")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    ts_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_log = Path("logs") / f"pre_24h_smoke_{ts_tag}.log"

    env = os.environ.copy()
    env["PROOF_MODE"] = "0"
    env["PROOF_FORCE_DECISION_TICKS"] = "0"
    env["PROOF_FORCE_CAL_APPLY"] = "0"
    env["SMOKE_MODE"] = "1"
    env["SMOKE_FORCE_ENTRY_EVAL_FROM_WATCH"] = env.get("SMOKE_FORCE_ENTRY_EVAL_FROM_WATCH", "0")
    env["CALIBRATION_ENABLED"] = env.get("CALIBRATION_ENABLED", "1")
    env["ACTIVE_ARTIFACTS_PATH"] = args.active_artifacts

    start = time.time()
    timeout_sec = max(60, int(args.minutes * 60))
    with run_log.open("w", encoding="utf-8") as out:
        try:
            subprocess.run(
                ["python3", "phone_bot.py"],
                stdout=out,
                stderr=subprocess.STDOUT,
                env=env,
                timeout=timeout_sec,
                check=False,
            )
        except subprocess.TimeoutExpired:
            # Expected in smoke mode: timeout defines run length.
            pass
    end = time.time()

    trade_logs = [Path("logs/trades.jsonl")] + sorted(Path("logs").glob("trades.jsonl.*"))
    stage = Counter()
    block_reasons = Counter()
    reject_codes = Counter()
    by_pair = defaultdict(Counter)
    by_session = defaultdict(Counter)
    by_session_quarter = defaultdict(Counter)
    by_pocket = defaultdict(Counter)
    skip_reasons = Counter()
    promote_blocked = Counter()
    stuck_watch = Counter()

    for e in _iter_events(trade_logs):
        t = _parse_ts(e)
        if t < start or t > end + 5:
            continue
        kind = str(e.get("kind") or e.get("event") or "")
        if kind in {"TUNE_MATCH", "ENTRY_EVAL", "ENTRY_GATE_EVAL", "ENTRY_ATTEMPT", "ORDER_SUBMITTED", "ORDER_FILLED", "ORDER_REJECTED"}:
            stage[kind] += 1
            pair = str(e.get("pair", ""))
            by_pair[pair][kind] += 1
            by_session[str(e.get("session", ""))][kind] += 1
            by_session_quarter[str(e.get("session_quarter", ""))][kind] += 1
            by_pocket[str(e.get("pocket_key", ""))][kind] += 1
        if kind == "ENTRY_GATE_EVAL":
            br = str(e.get("block_reason") or "")
            if br:
                block_reasons[br] += 1
        if kind == "ORDER_REJECTED":
            rc = str(e.get("reject_code") or "REJECT_UNKNOWN")
            reject_codes[rc] += 1
        if kind == "ENTRY_PATH_SKIP_REASON":
            r = str(e.get("reason") or "unknown_skip")
            sr = str(e.get("subreason") or "")
            skip_reasons[f"{r}:{sr}"] += 1
            if str(e.get("state") or "") == "WATCH":
                stuck_watch[str(e.get("pair") or "")] += 1
        if kind == "STATE_PROMOTE_BLOCKED":
            promote_blocked[str(e.get("reason") or "unknown")] += 1

    eval_n = stage.get("ENTRY_GATE_EVAL", 0)
    att_n = stage.get("ENTRY_ATTEMPT", 0)
    sub_n = stage.get("ORDER_SUBMITTED", 0)
    fill_n = stage.get("ORDER_FILLED", 0)
    conv_eval_attempt = (att_n / eval_n) if eval_n else 0.0
    conv_attempt_sub = (sub_n / att_n) if att_n else 0.0
    conv_sub_fill = (fill_n / sub_n) if sub_n else 0.0

    funnel_report = {
        "window": {"start_ts": start, "end_ts": end, "minutes": args.minutes},
        "counts": dict(stage),
        "conversion": {
            "eval_to_attempt": conv_eval_attempt,
            "attempt_to_submitted": conv_attempt_sub,
            "submitted_to_filled": conv_sub_fill,
        },
        "top_block_reasons": block_reasons.most_common(20),
        "top_reject_codes": reject_codes.most_common(20),
        "top_entry_path_skip_reasons": skip_reasons.most_common(20),
        "top_state_promote_blocked": promote_blocked.most_common(20),
        "pairs_stuck_watch": stuck_watch.most_common(20),
        "by_pair": {k: dict(v) for k, v in by_pair.items()},
        "by_session": {k: dict(v) for k, v in by_session.items() if k},
        "by_session_quarter": {k: dict(v) for k, v in by_session_quarter.items() if k},
        "by_pocket": {k: dict(v) for k, v in by_pocket.items() if k},
    }
    funnel_out = out_dir / f"FUNNEL_REPORT_{ts_tag}.json"
    funnel_out.write_text(json.dumps(funnel_report, indent=2), encoding="utf-8")

    gates = {
        "TUNE_MATCH_gt_0": stage.get("TUNE_MATCH", 0) > 0,
        "ENTRY_GATE_EVAL_gt_0": eval_n > 0,
        "ORDER_SUBMITTED_gt_0": sub_n > 0,
        "ORDER_FILLED_gt_0": (fill_n > 0) if int(args.require_filled) == 1 else True,
    }
    # Watchdog line requested: eval high, submitted 0
    watchdog = {}
    if eval_n > 0 and sub_n == 0:
        watchdog = {
            "warning": "ENTRY_GATE_EVAL high but ORDER_SUBMITTED is 0",
            "top3_block_reasons": block_reasons.most_common(3),
        }
    if eval_n == 0:
        watchdog = {
            "warning": "ENTRY_GATE_EVAL is 0",
            "top5_entry_path_skip_reasons": skip_reasons.most_common(5),
            "top5_state_promote_blocked": promote_blocked.most_common(5),
            "top5_pairs_stuck_watch": stuck_watch.most_common(5),
            "signal_sprint_override": env.get("SIGNAL_SPRINT_OVERRIDE", ""),
            "entry_sprint_override": env.get("ENTRY_SPRINT_OVERRIDE", ""),
            "smoke_force_entry_eval_from_watch": env.get("SMOKE_FORCE_ENTRY_EVAL_FROM_WATCH", ""),
        }

    smoke = {
        "run_log": str(run_log),
        "funnel_report": str(funnel_out),
        "gates": gates,
        "pass": all(gates.values()),
        "watchdog": watchdog,
    }
    smoke_out = out_dir / f"SMOKE_GATE_REPORT_{ts_tag}.json"
    smoke_out.write_text(json.dumps(smoke, indent=2), encoding="utf-8")

    print(json.dumps(smoke, indent=2))
    print(json.dumps({"top_block_reasons": block_reasons.most_common(10), "top_reject_codes": reject_codes.most_common(10)}, indent=2))
    return 0 if smoke["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
