#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from aee_replay_harness_adapter import build_baseline_comparison_report, replay_trade_path

PIP = 0.0001


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _profit_pips(direction: str, start: float, px: float) -> float:
    if str(direction).upper() == "LONG":
        return (px - start) / PIP
    return (start - px) / PIP


def _row_sort_key(row: dict[str, Any]) -> tuple[str, str]:
    return (str(row.get("timestamp_start", "")), str(row.get("cluster_id", "")))


def _convert_row_to_replay_trade(row: dict[str, Any], side: str, mode: str, distance_key: str) -> dict[str, Any]:
    start = _safe_float(row.get("price_start", 0.0), 0.0)
    direction = str(row.get("direction", side.upper())).upper()
    path = list(row.get("price_path") or [])
    if not path:
        return {
            "trade_id": f"{row.get('cluster_id', 'UNKNOWN')}::{row.get('timestamp_start', 'NA')}",
            "target_distance": _safe_float(row.get("distance", distance_key), 1.0),
            "baseline_final_pips": _safe_float(row.get("pips", 0.0), 0.0),
            "meta": {
                "mode": mode,
                "side": side,
                "distance_bucket": str(distance_key),
                "static_reason": str(row.get("reason", "UNKNOWN")),
            },
            "rows": [],
        }

    trade_rows: list[dict[str, Any]] = []
    prev_profit = 0.0
    for i, px in enumerate(path, start=1):
        pips = _profit_pips(direction, start, _safe_float(px, start))
        vel = pips - prev_profit if i > 1 else 0.0
        td = max(0.1, _safe_float(row.get("distance", distance_key), 1.0))
        progress_ratio = pips / td
        trade_rows.append(
            {
                "bar_index": i,
                "timestamp": "",
                "profit_now": pips,
                "velocity_now": vel,
                "progress_ratio": progress_ratio,
            }
        )
        prev_profit = pips

    return {
        "trade_id": f"{row.get('cluster_id', 'UNKNOWN')}::{row.get('timestamp_start', 'NA')}",
        "target_distance": max(0.1, _safe_float(row.get("distance", distance_key), 1.0)),
        "baseline_final_pips": _safe_float(row.get("pips", trade_rows[-1]["profit_now"]), trade_rows[-1]["profit_now"]),
        "meta": {
            "mode": mode,
            "side": side,
            "distance_bucket": str(distance_key),
            "cluster_id": str(row.get("cluster_id", "")),
            "session": str(row.get("session", "")),
            "weekday": str(row.get("weekday", "")),
            "static_reason": str(row.get("reason", "UNKNOWN")),
        },
        "rows": trade_rows,
    }


def extract_fixed_benchmark_slice(unified: dict[str, Any], max_trades: int) -> list[dict[str, Any]]:
    results = unified.get("results") or {}
    trades: list[dict[str, Any]] = []

    for side in sorted(results.keys()):
        side_payload = results.get(side) or {}
        for mode in sorted(side_payload.keys()):
            mode_payload = side_payload.get(mode) or {}
            for distance_key in sorted(mode_payload.keys(), key=lambda x: float(x)):
                distance_payload = mode_payload.get(distance_key) or {}
                rows = list(((distance_payload.get("profit_ceiling") or {}).get("rows") or []))
                for row in sorted(rows, key=_row_sort_key):
                    trade = _convert_row_to_replay_trade(row, side, mode, distance_key)
                    if trade["rows"]:
                        trades.append(trade)
                    if len(trades) >= max_trades:
                        return trades
    return trades


def run_kernel_benchmark(
    *,
    unified_path: Path,
    max_trades: int,
    benchmark_slice_out: Path,
    report_out: Path,
    packets_out: Path,
) -> dict[str, Any]:
    unified = json.loads(unified_path.read_text(encoding="utf-8"))
    benchmark_trades = extract_fixed_benchmark_slice(unified, max_trades=max_trades)
    benchmark_slice = {
        "schema_version": "AEE_REPLAY_SLICE_V1",
        "selection_method": "deterministic_first_n_sorted_by_timestamp_and_cluster",
        "source_unified_report": str(unified_path),
        "count": len(benchmark_trades),
        "trades": benchmark_trades,
    }
    benchmark_slice_out.write_text(json.dumps(benchmark_slice, indent=2) + "\n", encoding="utf-8")

    trade_results = [replay_trade_path(t) for t in benchmark_trades]
    report = build_baseline_comparison_report(trade_results)
    report["kernel_benchmark"] = {
        "spine_version": "AEE_TESTING_SPINE_V1",
        "source": str(unified_path),
        "benchmark_trade_count": len(benchmark_trades),
        "benchmark_slice_path": str(benchmark_slice_out),
        "baseline_comparisons": [
            {
                "name": "static_trade_baseline",
                "value_field": "baseline_money_result_pips",
                "description": "Static TP/SL baseline pips from source benchmark row.pips",
            },
            {
                "name": "replay_kernel_result",
                "value_field": "final_money_result_pips",
                "description": "Packet-emitting replay kernel final pips",
            },
        ],
        "proof_question": "Can AEE replay kernel beat static baseline on fixed slice?",
    }
    report_out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    all_packets: list[dict[str, Any]] = []
    for tr in trade_results:
        all_packets.extend(list(tr.get("packets") or []))
    packets_out.write_text(json.dumps(all_packets, indent=2) + "\n", encoding="utf-8")

    return {
        "benchmark_trade_count": len(benchmark_trades),
        "report_path": str(report_out),
        "packets_path": str(packets_out),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run AEE kernel benchmark on fixed replay trade paths.")
    ap.add_argument("--input", default="entry_metric_ceiling_report_unified.json", help="Unified ceiling report JSON")
    ap.add_argument("--max-trades", type=int, default=60, help="Maximum trades in the fixed benchmark slice")
    ap.add_argument("--benchmark-slice-out", default="control/aee_kernel_benchmark_slice.json")
    ap.add_argument("--report-out", default="control/aee_kernel_benchmark_report.json")
    ap.add_argument("--packets-out", default="control/aee_kernel_benchmark_packets.json")
    args = ap.parse_args()

    summary = run_kernel_benchmark(
        unified_path=Path(args.input),
        max_trades=max(1, int(args.max_trades)),
        benchmark_slice_out=Path(args.benchmark_slice_out),
        report_out=Path(args.report_out),
        packets_out=Path(args.packets_out),
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
