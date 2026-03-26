#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

def _iter(path: Path):
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue

def _to_epoch(ts) -> float | None:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return float(ts)
    s = str(ts).strip()
    if not s:
        return None
    try:
        # Handle Zulu timestamps.
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", default="logs/trades.jsonl")
    ap.add_argument("--out", default="proof_artifacts/LANE_B_TEACHER_HEALTH.json")
    args = ap.parse_args()

    emitted = 0
    skipped_incomplete = 0
    total_manual_teacher = 0
    total_teach_heartbeat = 0
    state_complete_ok_list = []
    heartbeat_intervals = []

    last_heartbeat_per_trade = {}

    for e in _iter(Path(args.log)):
        kind = str(e.get("kind") or "")
        if kind == "TEACHER_EMIT_SKIPPED_INCOMPLETE":
            skipped_incomplete += 1
        elif kind in ("MANUAL_TEACHER", "TEACH_HEARTBEAT"):
            total_manual_teacher += 1 if kind == "MANUAL_TEACHER" else 0
            total_teach_heartbeat += 1 if kind == "TEACH_HEARTBEAT" else 0
            emitted += 1
            if e.get("state_complete_ok"):
                state_complete_ok_list.append(True)
            else:
                state_complete_ok_list.append(False)
            if kind == "TEACH_HEARTBEAT":
                trade_id = str(e.get("trade_id", ""))
                ts = _to_epoch(e.get("ts_utc"))
                if trade_id and ts:
                    if trade_id in last_heartbeat_per_trade:
                        interval = float(ts) - float(last_heartbeat_per_trade[trade_id])
                        if interval >= 0:
                            heartbeat_intervals.append(interval)
                    last_heartbeat_per_trade[trade_id] = ts

    total_evals = emitted + skipped_incomplete
    state_complete_ok_rate = len([x for x in state_complete_ok_list if x]) / len(state_complete_ok_list) if state_complete_ok_list else 0

    heartbeat_intervals.sort()
    n = len(heartbeat_intervals)
    median_interval = heartbeat_intervals[n // 2] if n else None
    p90_interval = heartbeat_intervals[int(n * 0.9)] if n else None

    out = {
        "state_complete_ok_rate": state_complete_ok_rate,
        "teacher_emit_emitted_count": emitted,
        "teacher_emit_skipped_incomplete_count": skipped_incomplete,
        "total_manual_teacher": total_manual_teacher,
        "total_teach_heartbeat": total_teach_heartbeat,
        "heartbeat_median_interval_sec": median_interval,
        "heartbeat_p90_interval_sec": p90_interval,
        "total_evals": total_evals,
    }
    p = Path(args.out)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
