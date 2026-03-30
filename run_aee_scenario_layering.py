#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def classify_scenario(trade_result: dict[str, Any]) -> str:
    reason = str(trade_result.get("final_reason_code", "")).strip()
    transition = str(trade_result.get("final_state_transition", "")).strip()
    t_sec = _safe_float(trade_result.get("time_in_trade_sec", 0.0), 0.0)
    giveback_r = _safe_float(trade_result.get("max_giveback_r", 0.0), 0.0)
    delta = _safe_float(trade_result.get("delta_vs_baseline_pips", 0.0), 0.0)
    locked = _safe_float(trade_result.get("locked_profit_pips", 0.0), 0.0)

    if reason == "panic_trigger" and t_sec <= 180:
        return "FAST_PANIC_FAILURE"
    if transition == "PROTECT->PANIC":
        return "PROTECT_LAYER_BREAK"
    if reason == "build_safety_breach" and giveback_r >= 1.0:
        return "BUILD_GIVEBACK_CASCADE"
    if locked > 0.0 and delta >= 0.0:
        return "LOCKED_PROFIT_HOLD"
    if delta >= 0.0:
        return "BASELINE_OUTPERFORM"
    return "BASELINE_UNDERPERFORM"


def _avg(values: list[float]) -> float:
    return (sum(values) / len(values)) if values else 0.0


def _scenario_playbooks() -> dict[str, dict[str, Any]]:
    return {
        "FAST_PANIC_FAILURE": {
            "focus": "init/protect panic suppression",
            "objective": "Reduce early panic exits without increasing terminal loss tail.",
            "next_checks": [
                "raise panic activation evidence threshold in PROTECT",
                "require dual-condition panic confirmation",
                "retest fast-failure windows on fixed benchmark slice"
            ],
        },
        "PROTECT_LAYER_BREAK": {
            "focus": "protect state guard calibration",
            "objective": "Prevent direct PROTECT->PANIC transitions caused by transient noise.",
            "next_checks": [
                "add minimum hold bars before panic eligibility",
                "compare protect hold outcomes vs immediate panic outcomes",
                "validate giveback impact remains bounded"
            ],
        },
        "BUILD_GIVEBACK_CASCADE": {
            "focus": "build state giveback control",
            "objective": "Convert high-giveback BUILD exits into harvest-safe exits.",
            "next_checks": [
                "introduce earlier BUILD->HARVEST transition candidate",
                "tighten giveback guard before panic boundary",
                "measure delta_vs_baseline and max_giveback_r shifts"
            ],
        },
        "LOCKED_PROFIT_HOLD": {
            "focus": "runner/harvest protection",
            "objective": "Preserve scenarios where locked profit already outperforms baseline.",
            "next_checks": [
                "freeze current lock thresholds for this scenario",
                "verify no regression in time-in-trade efficiency",
                "expand sample size before modifying logic"
            ],
        },
        "BASELINE_OUTPERFORM": {
            "focus": "stability verification",
            "objective": "Confirm outperformance remains stable across larger fixed slices.",
            "next_checks": [
                "repeat benchmark with additional fixed contexts",
                "check reason_code drift",
                "promote only after consistency proof"
            ],
        },
        "BASELINE_UNDERPERFORM": {
            "focus": "loss source isolation",
            "objective": "Identify which transition/reason combinations drive negative delta.",
            "next_checks": [
                "prioritize top negative scenario buckets",
                "compare against transition-level baseline report",
                "target one guard change at a time"
            ],
        },
    }


def build_scenario_layering_report(kernel_report: dict[str, Any]) -> dict[str, Any]:
    trade_results = list(kernel_report.get("trade_results") or [])
    playbooks = _scenario_playbooks()

    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for tr in trade_results:
        scenario = classify_scenario(tr)
        enriched = dict(tr)
        enriched["scenario_id"] = scenario
        buckets[scenario].append(enriched)

    by_scenario: dict[str, dict[str, Any]] = {}
    for scenario, rows in sorted(buckets.items()):
        delta_vals = [_safe_float(r.get("delta_vs_baseline_pips", 0.0), 0.0) for r in rows]
        positive_delta_trades = sum(1 for d in delta_vals if d > 1e-9)
        negative_delta_trades = sum(1 for d in delta_vals if d < -1e-9)
        flat_delta_trades = len(delta_vals) - positive_delta_trades - negative_delta_trades
        reason_counts = Counter(str(r.get("final_reason_code", "UNKNOWN")) for r in rows)
        transition_counts = Counter(str(r.get("final_state_transition", "UNKNOWN->UNKNOWN")) for r in rows)
        by_scenario[scenario] = {
            "count": len(rows),
            "total_delta_vs_baseline_pips": sum(delta_vals),
            "avg_final_money_result_pips": _avg([_safe_float(r.get("final_money_result_pips", 0.0), 0.0) for r in rows]),
            "avg_baseline_money_result_pips": _avg([_safe_float(r.get("baseline_money_result_pips", 0.0), 0.0) for r in rows]),
            "avg_delta_vs_baseline_pips": _avg([_safe_float(r.get("delta_vs_baseline_pips", 0.0), 0.0) for r in rows]),
            "avg_time_in_trade_sec": _avg([_safe_float(r.get("time_in_trade_sec", 0.0), 0.0) for r in rows]),
            "avg_max_giveback_r": _avg([_safe_float(r.get("max_giveback_r", 0.0), 0.0) for r in rows]),
            "avg_locked_profit_pips": _avg([_safe_float(r.get("locked_profit_pips", 0.0), 0.0) for r in rows]),
            "positive_delta_trades": positive_delta_trades,
            "negative_delta_trades": negative_delta_trades,
            "flat_delta_trades": flat_delta_trades,
            "top_reason_codes": dict(reason_counts.most_common(3)),
            "top_state_transitions": dict(transition_counts.most_common(3)),
            "playbook": playbooks.get(scenario, {}),
        }

    return {
        "input_summary": {
            "trade_count": len(trade_results),
            "kernel_summary": kernel_report.get("summary", {}),
        },
        "scenario_order": [
            "FAST_PANIC_FAILURE",
            "PROTECT_LAYER_BREAK",
            "BUILD_GIVEBACK_CASCADE",
            "LOCKED_PROFIT_HOLD",
            "BASELINE_OUTPERFORM",
            "BASELINE_UNDERPERFORM",
        ],
        "by_scenario": by_scenario,
    }


def run_scenario_layering(*, kernel_report_path: Path, scenario_report_out: Path, scenario_playbooks_out: Path) -> dict[str, Any]:
    kernel_report = json.loads(kernel_report_path.read_text(encoding="utf-8"))
    scenario_report = build_scenario_layering_report(kernel_report)
    scenario_report_out.write_text(json.dumps(scenario_report, indent=2) + "\n", encoding="utf-8")
    scenario_playbooks_out.write_text(json.dumps(_scenario_playbooks(), indent=2) + "\n", encoding="utf-8")
    return {
        "scenario_count": len(scenario_report.get("by_scenario", {})),
        "report_path": str(scenario_report_out),
        "playbook_path": str(scenario_playbooks_out),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Build scenario layering report from kernel benchmark output.")
    ap.add_argument("--input", default="control/aee_kernel_benchmark_report.json")
    ap.add_argument("--report-out", default="control/aee_scenario_layering_report.json")
    ap.add_argument("--playbooks-out", default="control/aee_scenario_playbooks.json")
    args = ap.parse_args()

    summary = run_scenario_layering(
        kernel_report_path=Path(args.input),
        scenario_report_out=Path(args.report_out),
        scenario_playbooks_out=Path(args.playbooks_out),
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
