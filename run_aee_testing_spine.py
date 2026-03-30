#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from run_aee_kernel_benchmark import run_kernel_benchmark
from run_aee_scenario_layering import run_scenario_layering


def run_testing_spine(
    *,
    unified_input: Path,
    max_trades: int,
    slice_out: Path,
    kernel_report_out: Path,
    packets_out: Path,
    scenario_report_out: Path,
    scenario_playbooks_out: Path,
    manifest_out: Path,
) -> dict[str, str | int]:
    kernel_summary = run_kernel_benchmark(
        unified_path=unified_input,
        max_trades=max_trades,
        benchmark_slice_out=slice_out,
        report_out=kernel_report_out,
        packets_out=packets_out,
    )
    scenario_summary = run_scenario_layering(
        kernel_report_path=kernel_report_out,
        scenario_report_out=scenario_report_out,
        scenario_playbooks_out=scenario_playbooks_out,
    )

    manifest = {
        "spine_version": "AEE_TESTING_SPINE_V1",
        "order": [
            "packet_standard",
            "replay_harness",
            "baseline_comparison",
            "kernel_benchmark",
            "scenario_layering",
        ],
        "inputs": {
            "unified_report": str(unified_input),
            "max_trades": int(max_trades),
        },
        "outputs": {
            "benchmark_slice": str(slice_out),
            "kernel_report": str(kernel_report_out),
            "packet_stream": str(packets_out),
            "scenario_report": str(scenario_report_out),
            "scenario_playbooks": str(scenario_playbooks_out),
        },
        "kernel_summary": kernel_summary,
        "scenario_summary": scenario_summary,
    }
    manifest_out.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return {
        "spine_version": "AEE_TESTING_SPINE_V1",
        "manifest_path": str(manifest_out),
        "trade_count": int(kernel_summary.get("benchmark_trade_count", 0)),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run fixed AEE testing spine (kernel benchmark + scenario layering).")
    ap.add_argument("--input", default="entry_metric_ceiling_report_unified.json")
    ap.add_argument("--max-trades", type=int, default=20)
    ap.add_argument("--slice-out", default="control/aee_kernel_benchmark_slice.json")
    ap.add_argument("--kernel-report-out", default="control/aee_kernel_benchmark_report.json")
    ap.add_argument("--packets-out", default="control/aee_kernel_benchmark_packets.json")
    ap.add_argument("--scenario-report-out", default="control/aee_scenario_layering_report.json")
    ap.add_argument("--scenario-playbooks-out", default="control/aee_scenario_playbooks.json")
    ap.add_argument("--manifest-out", default="control/aee_testing_spine_manifest.json")
    args = ap.parse_args()

    summary = run_testing_spine(
        unified_input=Path(args.input),
        max_trades=max(1, int(args.max_trades)),
        slice_out=Path(args.slice_out),
        kernel_report_out=Path(args.kernel_report_out),
        packets_out=Path(args.packets_out),
        scenario_report_out=Path(args.scenario_report_out),
        scenario_playbooks_out=Path(args.scenario_playbooks_out),
        manifest_out=Path(args.manifest_out),
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
