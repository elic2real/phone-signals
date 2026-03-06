#!/usr/bin/env python3
"""Reconciler tool to compare baseline sim metrics vs extraction audit metrics."""

import json
from pathlib import Path

def load_json(path: str) -> dict:
    with Path(path).open("r", encoding="utf-8") as fh:
        return json.load(fh)

def reconcile():
    baseline = load_json("baseline_performance.json")
    extraction = load_json("extraction_audit_report.json")

    print("=== RECONCILER: Baseline Sim vs Extraction Audit ===")
    print()

    # Compare global metrics
    baseline_global = baseline.get("global_avg", 0)
    extraction_global = extraction["summary"].get("weighted_pips_per_hour", 0)

    print(f"Global Pips/Hour:")
    print(f"  Baseline Sim: {baseline_global:.2f}")
    print(f"  Extraction Audit: {extraction_global:.2f}")
    print(f"  Diff: {extraction_global - baseline_global:.2f}")
    print()

    # Compare per-scenario if possible
    baseline_scenarios = baseline.get("summary", {})
    extraction_scenarios = extraction.get("scenario_breakdown", {})

    print("Scenario Breakdown:")
    all_scenarios = set(baseline_scenarios.keys()) | set(extraction_scenarios.keys())
    for scenario in sorted(all_scenarios):
        b_val = baseline_scenarios.get(scenario, 0) if isinstance(baseline_scenarios.get(scenario), (int, float)) else baseline_scenarios.get(scenario, {}).get("weighted_pips_per_hour", 0)
        e_val = extraction_scenarios.get(scenario, {}).get("weighted_pips_per_hour", 0)
        diff = e_val - b_val
        print(f"  {scenario}: Baseline {b_val:.2f}, Extraction {e_val:.2f}, Diff {diff:.2f}")
    print()

    # Divergence analysis
    print("Potential Divergence Points:")
    if extraction_global < 0 and baseline_global > 0:
        print("  - Extraction shows losses while baseline shows profits: Check exit reasons, hold times, spread costs.")
    if extraction["summary"].get("avg_capture_percent", 0) < 0:
        print("  - Low capture percent in extraction: Check left_on_table, MFE/MAE.")
    print(f"  - Entry counts: Baseline N/A, Extraction {extraction['summary']['total_trades']}")
    print(f"  - Exit reasons: {extraction['by_exit_reason']}")
    print(f"  - Hold times: {extraction['summary']['median_hold_time_sec']}s median")
    print(f"  - Spread cost proxy: Check if included in pips/h calc")
    print(f"  - Slip proxy: Check if slippage accounted for")

if __name__ == "__main__":
    reconcile()
