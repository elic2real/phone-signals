#!/usr/bin/env python3
"""CER Tuner: Analyze extraction audit and suggest AEE knob nudges based on CER (Capture Extension Ratio)."""

import json
from pathlib import Path

def load_audit() -> dict:
    with Path("extraction_audit_report.json").open("r", encoding="utf-8") as fh:
        return json.load(fh)

def calculate_cer(exit_pips: float, mfe_pips: float) -> float:
    if exit_pips <= 0:
        return 0.0
    return (mfe_pips - exit_pips) / exit_pips

def analyze_cer():
    audit = load_audit()
    print("=== CER TUNER: Suggest AEE Knob Nudges ===")
    print()

    total_cer = 0.0
    count = 0
    for trade in audit.get("runs", []):
        exit_pips = trade.get("exit_pips", 0.0)
        mfe_pips = trade.get("mfe_pips", 0.0)
        cer = calculate_cer(exit_pips, mfe_pips)
        total_cer += cer
        count += 1
        print(f"Trade {trade.get('file', '')}: Exit {exit_pips:.2f}, MFE {mfe_pips:.2f}, CER {cer:.2f}")

    avg_cer = total_cer / count if count > 0 else 0.0
    print(f"\nAvg CER: {avg_cer:.2f}")

    # Suggestions
    suggestions = []
    if avg_cer > 0.5:
        suggestions.append("High CER: Increase aee.strictness_mult to capture more extension.")
    elif avg_cer < 0.1:
        suggestions.append("Low CER: Decrease aee.strictness_mult to allow more giveback.")

    tail_loss = audit["summary"].get("tail_losses", {}).get("p95", 0)
    if tail_loss < -5:
        suggestions.append("High tail loss: Tighten aee.near_tp_band_atr.")

    print("\nSuggestions:")
    for s in suggestions:
        print(f"  - {s}")

if __name__ == "__main__":
    analyze_cer()
