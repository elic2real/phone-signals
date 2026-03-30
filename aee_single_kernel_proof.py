#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from aee_replay_harness_adapter import build_baseline_comparison_report, replay_trade_path


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _pip_factor(pair: str) -> float:
    p = (pair or "").upper()
    return 100.0 if "JPY" in p else 10000.0


def _read_ticks(csv_path: str) -> tuple[str, list[dict[str, float]]]:
    pair = "EUR_USD"
    rows: list[dict[str, float]] = []
    with open(csv_path, "r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for r in reader:
            pair = str(r.get("instrument") or pair)
            ts = _safe_float(r.get("ts"), 0.0)
            mid = _safe_float(r.get("mid"), 0.0)
            if ts <= 0.0 or mid <= 0.0:
                continue
            rows.append({"ts": ts, "mid": mid})
    rows.sort(key=lambda x: x["ts"])
    return pair, rows


def _build_candles(ticks: list[dict[str, float]], bucket_sec: int) -> list[dict[str, float]]:
    if not ticks:
        return []
    candles: list[dict[str, float]] = []
    bucket = int(ticks[0]["ts"] // bucket_sec) * bucket_sec
    o = h = l = c = ticks[0]["mid"]
    for t in ticks:
        b = int(t["ts"] // bucket_sec) * bucket_sec
        px = t["mid"]
        if b != bucket:
            candles.append({"time": float(bucket), "o": o, "h": h, "l": l, "c": c})
            bucket = b
            o = h = l = c = px
        else:
            h = max(h, px)
            l = min(l, px)
            c = px
    candles.append({"time": float(bucket), "o": o, "h": h, "l": l, "c": c})
    return candles


def _atr_like(candles: list[dict[str, float]], idx: int, lookback: int = 14) -> float:
    start = max(1, idx - lookback + 1)
    trs: list[float] = []
    for i in range(start, idx + 1):
        c = candles[i]
        prev_close = candles[i - 1]["c"]
        tr = max(c["h"] - c["l"], abs(c["h"] - prev_close), abs(c["l"] - prev_close))
        trs.append(max(0.0, tr))
    if not trs:
        return 0.0
    return sum(trs) / len(trs)


def build_fixed_trade_paths_from_glob(
    scenario_glob: str,
    *,
    bucket_sec: int,
    horizon_bars: int,
    stride_bars: int,
    max_trades_per_scenario: int,
) -> list[dict[str, Any]]:
    trades: list[dict[str, Any]] = []
    paths = sorted(glob.glob(scenario_glob))
    for csv_path in paths:
        if csv_path.endswith(".manifest.json") or csv_path.endswith(".sha256"):
            continue
        scenario = Path(csv_path).name
        pair, ticks = _read_ticks(csv_path)
        candles = _build_candles(ticks, bucket_sec)
        if len(candles) < (horizon_bars + 30):
            continue

        built = 0
        for i in range(20, len(candles) - horizon_bars - 1, max(1, stride_bars)):
            if built >= max_trades_per_scenario:
                break

            entry = candles[i]["c"]
            next_close = candles[i + 1]["c"]
            is_long = next_close >= entry
            atr = _atr_like(candles, i, lookback=14)
            pf = _pip_factor(pair)
            target_pips = max(3.0, atr * pf)

            rows: list[dict[str, Any]] = []
            prev_pips = 0.0
            for k in range(1, horizon_bars + 1):
                c = candles[i + k]
                if is_long:
                    pips = (c["c"] - entry) * pf
                else:
                    pips = (entry - c["c"]) * pf
                velocity = pips - prev_pips
                prev_pips = pips
                rows.append(
                    {
                        "bar_index": k,
                        "timestamp": str(int(c["time"])),
                        "profit_now": pips,
                        "velocity_now": velocity,
                        "progress_ratio": pips / max(0.1, target_pips),
                    }
                )

            trade_id = f"{scenario}::{i}::{('LONG' if is_long else 'SHORT')}"
            trades.append(
                {
                    "trade_id": trade_id,
                    "target_distance": target_pips,
                    "baseline_final_pips": rows[-1]["profit_now"],
                    "meta": {
                        "scenario": scenario,
                        "pair": pair,
                        "direction": "LONG" if is_long else "SHORT",
                    },
                    "rows": rows,
                }
            )
            built += 1
    return trades


def _aggregate_with_reason_ranking(results: list[dict[str, Any]]) -> dict[str, Any]:
    base = build_baseline_comparison_report(results)

    by_scenario: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_reason: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in results:
        scenario = str(((r.get("packets") or [{}])[0].get("meta", {}) or {}).get("scenario", "UNKNOWN"))
        by_scenario[scenario].append(r)
        by_reason[str(r.get("final_reason_code", "UNKNOWN"))].append(r)

    scenario_rows: list[dict[str, Any]] = []
    for scenario, rows in sorted(by_scenario.items()):
        agg = {
            "scenario": scenario,
            "count": len(rows),
            "avg_delta_vs_1to1_baseline_pips": sum(_safe_float(x.get("delta_vs_1to1_baseline_pips", 0.0)) for x in rows) / max(1, len(rows)),
            "avg_delta_vs_protective_baseline_pips": sum(_safe_float(x.get("delta_vs_protective_baseline_pips", 0.0)) for x in rows) / max(1, len(rows)),
        }
        scenario_rows.append(agg)

    reason_rows: list[dict[str, Any]] = []
    for reason, rows in by_reason.items():
        avg_delta = sum(_safe_float(x.get("delta_vs_1to1_baseline_pips", 0.0)) for x in rows) / max(1, len(rows))
        reason_rows.append({"reason_code": reason, "count": len(rows), "avg_delta_vs_1to1_baseline_pips": avg_delta})

    reason_rows_sorted = sorted(reason_rows, key=lambda x: x["avg_delta_vs_1to1_baseline_pips"])

    base["per_scenario_delta"] = scenario_rows
    base["top_losing_reason_codes"] = reason_rows_sorted[:5]
    base["top_winning_reason_codes"] = list(reversed(reason_rows_sorted[-5:]))
    return base


def _run_policy_eval(trades: list[dict[str, Any]], policy_name: str, policy: dict[str, float] | None) -> list[dict[str, Any]]:
    return [
        replay_trade_path(t, policy_name=policy_name, policy_overrides=policy)
        for t in trades
    ]


def main() -> int:
    ap = argparse.ArgumentParser(description="Single-kernel AEE proof runner on fixed trade paths.")
    ap.add_argument("--input", default="", help="Optional fixed trade path input JSON. If empty, build from scenario glob.")
    ap.add_argument("--scenario-glob", default="scenarios/golden/v1.0/*.csv")
    ap.add_argument("--bucket-sec", type=int, default=300)
    ap.add_argument("--horizon-bars", type=int, default=12)
    ap.add_argument("--stride-bars", type=int, default=20)
    ap.add_argument("--max-trades-per-scenario", type=int, default=16)
    ap.add_argument("--fixed-output", default="control/aee_fixed_trade_paths_v1.json")
    ap.add_argument("--proof-output", default="control/aee_single_kernel_proof_report.json")
    args = ap.parse_args()

    if args.input:
        payload = json.loads(Path(args.input).read_text(encoding="utf-8"))
        trades = list(payload.get("trades") or payload)
    else:
        trades = build_fixed_trade_paths_from_glob(
            args.scenario_glob,
            bucket_sec=max(60, int(args.bucket_sec)),
            horizon_bars=max(4, int(args.horizon_bars)),
            stride_bars=max(1, int(args.stride_bars)),
            max_trades_per_scenario=max(1, int(args.max_trades_per_scenario)),
        )
        Path(args.fixed_output).write_text(json.dumps({"trades": trades}, indent=2) + "\n", encoding="utf-8")

    # Single kernel candidate: Progress kernel only.
    # Isolated change: lower protect->build progress threshold from 0.25 to 0.20.
    kernel_candidate_name = "progress_kernel_v1"
    kernel_candidate_policy = {
        "protect_progress_r": 0.20,
    }

    current_results = _run_policy_eval(trades, policy_name="current_aee", policy=None)
    candidate_results = _run_policy_eval(trades, policy_name=kernel_candidate_name, policy=kernel_candidate_policy)

    current_report = _aggregate_with_reason_ranking(current_results)
    candidate_report = _aggregate_with_reason_ranking(candidate_results)

    out = {
        "proof_question": "Can AEE now be tested quickly on fixed trade paths against baseline?",
        "answer": "yes",
        "trade_count": len(trades),
        "kernel_candidate": {
            "name": kernel_candidate_name,
            "family": "progress",
            "policy": kernel_candidate_policy,
        },
        "current_aee": current_report,
        "candidate_aee": candidate_report,
        "candidate_vs_current": {
            "avg_delta_vs_current_pips": _safe_float(candidate_report["summary"].get("avg_final_money_result_pips", 0.0)) - _safe_float(current_report["summary"].get("avg_final_money_result_pips", 0.0)),
            "avg_delta_vs_current_1to1_baseline_gap": _safe_float(candidate_report["summary"].get("avg_delta_vs_1to1_baseline_pips", 0.0)) - _safe_float(current_report["summary"].get("avg_delta_vs_1to1_baseline_pips", 0.0)),
            "avg_delta_vs_current_protective_baseline_gap": _safe_float(candidate_report["summary"].get("avg_delta_vs_protective_baseline_pips", 0.0)) - _safe_float(current_report["summary"].get("avg_delta_vs_protective_baseline_pips", 0.0)),
        },
    }

    out_path = Path(args.proof_output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")

    print(f"wrote {out_path}")
    print(json.dumps({
        "trade_count": out["trade_count"],
        "kernel_candidate": out["kernel_candidate"],
        "current_summary": out["current_aee"]["summary"],
        "candidate_summary": out["candidate_aee"]["summary"],
        "candidate_vs_current": out["candidate_vs_current"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
