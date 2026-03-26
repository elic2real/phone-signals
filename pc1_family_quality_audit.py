#!/usr/bin/env python3
import argparse
import ast
import json
import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

FAMILIES = [
    "BREAK",
    "PULLBACK_RECLAIM",
    "OSCILLATION_BOUNCE",
    "BIAS_ALIGNMENT_CONTINUATION",
]

SETUP_TO_FAMILY = {
    "COMPRESSION_EXPANSION": "BREAK",
    "COMPRESSION_EXPANSION_RUN": "BREAK",
    "FAILED_BREAKOUT_FADE": "PULLBACK_RECLAIM",
    "EXHAUSTION_SNAPBACK": "OSCILLATION_BOUNCE",
    "LIQUIDITY_SWEEP": "OSCILLATION_BOUNCE",
    "CONTINUATION_PUSH": "BIAS_ALIGNMENT_CONTINUATION",
    "CONTINUATION_PUSH_RUN": "BIAS_ALIGNMENT_CONTINUATION",
    "VOL_REIGNITE": "BIAS_ALIGNMENT_CONTINUATION",
    "INTENTIONAL_RUNNER": "BIAS_ALIGNMENT_CONTINUATION",
}

AEE_EXIT_EVENTS = {
    "AEE_IMPULSE_EXIT",
    "AEE_PROGRESS_FAILURE_EXIT",
    "AEE_PRE_SL_EXIT",
    "AEE_SL_LOCK",
    "AEE_RUNNER_PARTIAL",
    "AEE_RUNNER_FINAL_EXIT",
    "AEE_STALL_EXIT",
}

# Conservative stop/full-loss detector. Pre-SL events are included in this bucket.
SL_OR_FULL_LOSS_HINTS = (
    "SL",
    "STOP",
    "FULL_LOSS",
)


def parse_iso_to_epoch(ts: str) -> float:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()


def parse_stdout_window_and_allow(stdout_path: Path):
    pat = re.compile(r"ENTRY_FAMILY_DECISION \| (\{.*\})")
    seen = set()
    decisions = []

    for line in stdout_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = pat.search(line)
        if not m:
            continue
        payload_txt = m.group(1)
        try:
            payload = ast.literal_eval(payload_txt)
        except Exception:
            continue

        key = (
            payload.get("pair"),
            payload.get("setup_name"),
            payload.get("direction"),
            payload.get("allowed_or_blocked"),
            payload.get("reason"),
            payload.get("ts"),
        )
        if key in seen:
            continue
        seen.add(key)
        decisions.append(payload)

    if not decisions:
        raise RuntimeError(f"No ENTRY_FAMILY_DECISION payloads found in {stdout_path}")

    ts_values = [d.get("ts") for d in decisions if isinstance(d.get("ts"), str)]
    if not ts_values:
        raise RuntimeError("No decision timestamps found")

    start_ts = min(ts_values)
    end_ts = max(ts_values)

    allow_by_family = Counter()
    for d in decisions:
        if d.get("allowed_or_blocked") == "ALLOWED":
            fam = str(d.get("entry_family") or "UNKNOWN")
            allow_by_family[fam] += 1

    return start_ts, end_ts, decisions, allow_by_family


def audit_trades_jsonl(trades_path: Path, start_ts: str, end_ts: str):
    def in_window(t):
        return isinstance(t, str) and start_ts <= t <= end_ts

    # trade_id opened in-window -> family and open-ts from ENTRY_RESULT
    trade_family = {}
    trade_entry_ts = {}
    fills_by_family = Counter()
    trade_attempt_by_family = Counter()

    # earliest first green ts by trade_id
    earliest_first_green = {}

    # exits only for trade_ids opened in-window
    exits_by_family_reason = Counter()
    aee_exits_by_family = Counter()
    sl_full_loss_by_family = Counter()

    with trades_path.open("r", encoding="utf-8", errors="ignore") as fh:
        for ln in fh:
            ln = ln.strip()
            if not ln.startswith("{"):
                continue
            try:
                d = json.loads(ln)
            except Exception:
                continue

            t = d.get("ts") or d.get("ts_utc") or d.get("timestamp")
            if not in_window(t):
                continue

            ev = d.get("event") or d.get("kind") or d.get("event_type") or ""

            if ev == "TRADE_ATTEMPT":
                fam = SETUP_TO_FAMILY.get(str(d.get("setup") or ""), "UNKNOWN")
                trade_attempt_by_family[fam] += 1

            if ev == "ENTRY_RESULT":
                tid = str(d.get("trade_id") or "")
                fam = str(d.get("entry_family") or "").strip() or SETUP_TO_FAMILY.get(str(d.get("setup") or ""), "UNKNOWN")
                if tid:
                    trade_family[tid] = fam
                    trade_entry_ts[tid] = t
                    fills_by_family[fam] += 1

            fg = d.get("first_green_ts")
            tid = str(d.get("trade_id") or "")
            if tid and tid in trade_family and fg is not None:
                try:
                    fg = float(fg)
                except Exception:
                    fg = None
                if fg is not None:
                    prev = earliest_first_green.get(tid)
                    if prev is None or fg < prev:
                        earliest_first_green[tid] = fg

            if ev in AEE_EXIT_EVENTS:
                tid = str(d.get("trade_id") or "")
                if tid not in trade_family:
                    continue
                fam = trade_family[tid]
                reason = str(d.get("reason") or ev)
                exits_by_family_reason[(fam, reason)] += 1
                aee_exits_by_family[fam] += 1

                label = f"{ev}|{reason}".upper()
                if any(h in label for h in SL_OR_FULL_LOSS_HINTS):
                    sl_full_loss_by_family[fam] += 1

    first_green_touch_by_family = Counter()
    time_to_first_green_by_family = defaultdict(list)

    for tid, first_green_epoch in earliest_first_green.items():
        fam = trade_family.get(tid, "UNKNOWN")
        first_green_touch_by_family[fam] += 1
        entry_ts = trade_entry_ts.get(tid)
        if isinstance(entry_ts, str):
            try:
                delta = max(0.0, first_green_epoch - parse_iso_to_epoch(entry_ts))
                time_to_first_green_by_family[fam].append(delta)
            except Exception:
                pass

    avg_time_to_first_green = {}
    for fam, vals in time_to_first_green_by_family.items():
        if vals:
            avg_time_to_first_green[fam] = round(sum(vals) / len(vals), 2)

    first_green_rate_by_family = {}
    for fam in set(list(fills_by_family.keys()) + list(first_green_touch_by_family.keys())):
        filled = fills_by_family.get(fam, 0)
        green = first_green_touch_by_family.get(fam, 0)
        first_green_rate_by_family[fam] = round((100.0 * green / filled), 2) if filled > 0 else 0.0

    return {
        "trade_attempt_by_family": trade_attempt_by_family,
        "fills_by_family": fills_by_family,
        "first_green_touch_by_family": first_green_touch_by_family,
        "first_green_rate_by_family": first_green_rate_by_family,
        "avg_time_to_first_green": avg_time_to_first_green,
        "aee_exits_by_family_reason": exits_by_family_reason,
        "sl_full_loss_by_family": sl_full_loss_by_family,
        "aee_exits_by_family": aee_exits_by_family,
    }


def print_sections(start_ts, end_ts, allow_by_family, data):
    print(f"WINDOW_START={start_ts}")
    print(f"WINDOW_END={end_ts}")
    print()

    # Required numbered sections
    print("1. ALLOW_BY_FAMILY")
    for fam in FAMILIES:
        print(f"- {fam}: {allow_by_family.get(fam, 0)}")
    print()

    print("2. FILLS_BY_FAMILY")
    for fam in FAMILIES:
        print(f"- {fam}: {data['fills_by_family'].get(fam, 0)}")
    print()

    print("3. FIRST_GREEN_TOUCH_BY_FAMILY")
    for fam in FAMILIES:
        print(f"- {fam}: {data['first_green_touch_by_family'].get(fam, 0)}")
    print()

    print("4. FIRST_GREEN_TOUCH_RATE_BY_FAMILY")
    for fam in FAMILIES:
        print(f"- {fam}: {data['first_green_rate_by_family'].get(fam, 0.0):.2f}%")
    print()

    print("5. AVG_TIME_TO_FIRST_GREEN_SEC_BY_FAMILY")
    for fam in FAMILIES:
        val = data["avg_time_to_first_green"].get(fam)
        txt = f"{val:.2f}" if isinstance(val, (int, float)) else "NA"
        print(f"- {fam}: {txt}")
    print()

    print("6. AEE_EXITS_BY_FAMILY_REASON")
    rows = sorted(
        ((fam, reason, c) for (fam, reason), c in data["aee_exits_by_family_reason"].items()),
        key=lambda x: (-x[2], x[0], x[1]),
    )
    if not rows:
        print("- NONE")
    else:
        for fam, reason, c in rows:
            print(f"- {fam} | {reason}: {c}")
    print()

    print("7. FULL_LOSS_OR_STOP_HITS_BY_FAMILY")
    for fam in FAMILIES:
        print(f"- {fam}: {data['sl_full_loss_by_family'].get(fam, 0)}")
    print()

    # Clean summary table requested by user
    print("TABLE: FAMILY_QUALITY_SUMMARY")
    print("Family\tAllowed\tFilled\tFirst Green\tFirst Green %\tAvg Time to First Green\tAEE Exits\tSL / Full Loss")
    for fam in FAMILIES:
        allowed = allow_by_family.get(fam, 0)
        filled = data["fills_by_family"].get(fam, 0)
        fg = data["first_green_touch_by_family"].get(fam, 0)
        fg_rate = data["first_green_rate_by_family"].get(fam, 0.0)
        avg_t = data["avg_time_to_first_green"].get(fam)
        avg_t_txt = f"{avg_t:.2f}s" if isinstance(avg_t, (int, float)) else "NA"
        aee_exits = data["aee_exits_by_family"].get(fam, 0)
        sl_loss = data["sl_full_loss_by_family"].get(fam, 0)
        print(f"{fam}\t{allowed}\t{filled}\t{fg}\t{fg_rate:.2f}%\t{avg_t_txt}\t{aee_exits}\t{sl_loss}")
    print()

    print("TABLE: EXIT_REASON_BREAKDOWN")
    print("Family\tExit Reason\tCount")
    if not rows:
        print("NONE\tNONE\t0")
    else:
        for fam, reason, c in rows:
            print(f"{fam}\t{reason}\t{c}")


def main():
    parser = argparse.ArgumentParser(description="PC1 family quality audit using stdout window + trades.jsonl truth")
    parser.add_argument("--stdout-file", default="", help="Path to bounded stdout file. If omitted, latest logs/pc1_live_bounded_*.stdout is used.")
    parser.add_argument("--trades-file", default="logs/trades.jsonl", help="Path to trades jsonl")
    args = parser.parse_args()

    if args.stdout_file:
        stdout_path = Path(args.stdout_file)
    else:
        matches = sorted(Path("logs").glob("pc1_live_bounded_*.stdout"))
        if not matches:
            raise RuntimeError("No live bounded stdout file found under logs/")
        stdout_path = matches[-1]

    trades_path = Path(args.trades_file)
    if not trades_path.exists():
        raise RuntimeError(f"Trades file not found: {trades_path}")

    start_ts, end_ts, _decisions, allow_by_family = parse_stdout_window_and_allow(stdout_path)
    data = audit_trades_jsonl(trades_path, start_ts, end_ts)
    print_sections(start_ts, end_ts, allow_by_family, data)


if __name__ == "__main__":
    main()
