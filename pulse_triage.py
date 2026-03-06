#!/usr/bin/env python3
"""PULSE_STALL_CAPTURE Triage: Focused report on worst buckets and trades."""

import json
from pathlib import Path
from collections import defaultdict

def load_audit() -> dict:
    with Path("extraction_audit_report.json").open("r", encoding="utf-8") as fh:
        return json.load(fh)

def triage_pulse_stall():
    audit = load_audit()
    print("=== PULSE_STALL_CAPTURE TRIAGE ===")
    print()

    # Filter trades with PULSE_STALL_CAPTURE
    pulse_trades = [t for t in audit.get("runs", []) if t.get("exit_reason") == "PULSE_STALL_CAPTURE"]

    if not pulse_trades:
        print("No PULSE_STALL_CAPTURE trades found.")
        return

    # Group by bucket (using instrument as proxy for bucket)
    buckets = defaultdict(list)
    for t in pulse_trades:
        bucket = t.get("instrument", "unknown")
        buckets[bucket].append(t)

    # List buckets with most negative pips
    bucket_summary = []
    for bucket, trades in buckets.items():
        total_pips = sum(t.get("exit_pips", 0) for t in trades)
        count = len(trades)
        avg_pips = total_pips / count if count > 0 else 0
        bucket_summary.append((bucket, total_pips, avg_pips, count))

    bucket_summary.sort(key=lambda x: x[1])  # Sort by total pips ascending (most negative first)

    print("Buckets with PULSE_STALL_CAPTURE (most negative total pips first):")
    for bucket, total, avg, count in bucket_summary:
        print(f"  {bucket}: Total {total:.2f}, Avg {avg:.2f}, Count {count}")
    print()

    # 20 worst trades
    worst_trades = sorted(pulse_trades, key=lambda x: x.get("exit_pips", 0))[:20]
    print("20 Worst PULSE_STALL_CAPTURE Trades:")
    for i, t in enumerate(worst_trades, 1):
        file = t.get("file", "")
        exit_pips = t.get("exit_pips", 0)
        hold_sec = t.get("hold_sec", 0)
        print(f"  {i}. {file}: Exit {exit_pips:.2f} pips, Hold {hold_sec:.0f}s")
    print()

    # Suggestions
    print("Suggested Changes to Reduce Losses:")
    print("  1. Tighten stall capture confirmation: Increase aee.strictness_mult to 1.2")
    print("  2. Earlier close on death-spiral: Adjust AEE logic to detect and close sooner")
    print("  3. Switch to harvest close when CER is low: If CER < 0.1, use stricter exit")
    print("  Re-run dd-gate after each change to check if mean loss improves without worsening tail.")

if __name__ == "__main__":
    triage_pulse_stall()
