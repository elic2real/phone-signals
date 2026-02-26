#!/usr/bin/env python3
import argparse, json, os, time
from datetime import datetime, timezone


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def tail_counts(path: str):
    if not os.path.exists(path):
        return {"exists": False, "lines": 0, "sample": []}
    with open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        size = f.tell()
        block = min(size, 2_000_000)
        if block > 0:
            f.seek(-block, os.SEEK_END)
        data = f.read().decode("utf-8", errors="replace").splitlines()
    return {"exists": True, "lines": len(data), "sample": data[-50:]}


def jsonl_stats(path: str, max_lines: int = 20000):
    if not os.path.exists(path):
        return {"exists": False, "lines": 0, "last": None}
    last = None
    lines = 0
    with open(path, "rb") as f:
        data = f.read().decode("utf-8", errors="replace").splitlines()
    if len(data) > max_lines:
        data = data[-max_lines:]
    for line in data:
        line = line.strip()
        if not line:
            continue
        lines += 1
        try:
            last = json.loads(line)
        except Exception:
            continue
    return {"exists": True, "lines": lines, "last": last}


def extract_reject_counts(lines):
    keys = ["SPREAD_GATE", "FRICTION_NOT_COVERED", "SIGNAL_REJECT_COUNTS"]
    out = {k: 0 for k in keys}
    for ln in lines:
        for k in keys:
            if k in ln:
                out[k] += 1
    return out


def write_jsonl(path, obj):
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--bot-log", required=True)
    ap.add_argument("--cadence-min", type=int, default=10)
    ap.add_argument("--hourly", type=int, default=1)
    args = ap.parse_args()

    os.makedirs(args.run_dir, exist_ok=True)
    trades_path = os.path.join(args.run_dir, "trades.jsonl")
    metrics_path = os.path.join(args.run_dir, "metrics.jsonl")
    out_path = os.path.join(args.run_dir, "audit_summaries.jsonl")

    cadence_sec = max(60, args.cadence_min * 60)
    hourly_sec = 3600 if args.hourly else None
    next_cadence = time.time() + cadence_sec
    next_hourly = time.time() + hourly_sec if hourly_sec else None

    print(f"[MON] {utc_now()} monitor online | cadence={args.cadence_min}m | hourly={bool(args.hourly)}")
    while True:
        now = time.time()
        if now >= next_cadence:
            bot_tail = tail_counts(args.bot_log)
            tstats = jsonl_stats(trades_path)
            mstats = jsonl_stats(metrics_path)
            rejects = extract_reject_counts(bot_tail["sample"])
            snap = {
                "ts": utc_now(), "kind": "cadence_snapshot",
                "bot_log_tail_lines": bot_tail["lines"],
                "trades_jsonl": {"exists": tstats["exists"], "lines": tstats["lines"]},
                "metrics_jsonl": {"exists": mstats["exists"], "lines": mstats["lines"]},
                "reject_mentions_tail": rejects,
                "last_trade": tstats["last"], "last_metric": mstats["last"],
            }
            write_jsonl(out_path, snap)
            print(f"[SNAP] {snap['ts']} | trades={tstats['lines']} metrics={mstats['lines']} | tail_rejects={rejects}")
            next_cadence = now + cadence_sec

        if next_hourly and now >= next_hourly:
            bot_tail = tail_counts(args.bot_log)
            tstats = jsonl_stats(trades_path)
            mstats = jsonl_stats(metrics_path)
            rejects = extract_reject_counts(bot_tail["sample"])
            hour = {
                "ts": utc_now(), "kind": "hourly_summary",
                "trades_lines": tstats["lines"], "metrics_lines": mstats["lines"],
                "tail_reject_mentions": rejects,
                "notes": "Hourly summary based on jsonl growth + tail log signals.",
            }
            write_jsonl(out_path, hour)
            print(f"[HOUR] {hour['ts']} | trades={tstats['lines']} metrics={mstats['lines']} | rejects_tail={rejects}")
            next_hourly = now + hourly_sec

        time.sleep(1)


if __name__ == "__main__":
    main()
