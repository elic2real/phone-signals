#!/usr/bin/env python3
"""Execute MVP Phase 14 counterfactual simulation (no tuning, offline label-only)."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List

PHASE13_PATH = Path("control/mvp_phase13_counterfactual_simulation_plan_no_tuning.json")
EUR_TELE_PATH = Path("control/mvp_phase9_runtime_eur_usd_telemetry.json")
GBP_TELE_PATH = Path("control/mvp_phase9_runtime_gbp_usd_telemetry.json")
OUTPUT_PATH = Path("control/mvp_phase14_counterfactual_simulation_execution_no_tuning.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _extract_first_float(text: str, default: float = 0.0) -> float:
    m = re.search(r"-?\d+(?:\.\d+)?", text)
    return float(m.group(0)) if m else default


def _rows_from_telemetry(tele: Dict[str, Any]) -> List[Dict[str, Any]]:
    return list(tele.get("trade_lifecycle_samples", []))


def _triggered(trial_id: str, rule_if: str, row: Dict[str, Any]) -> bool:
    t_draw = float(row.get("time_in_drawdown_seconds", 0.0) or 0.0)
    t_peak = float(row.get("time_from_peak_to_close_seconds", 0.0) or 0.0)
    t_ttfp = float(row.get("time_to_first_profit_seconds", 0.0) or 0.0)
    loss_after_peak = float(row.get("loss_after_peak_pips", 0.0) or 0.0)
    outcome = str(row.get("outcome_label", ""))

    if trial_id == "P12-T1":
        threshold = _extract_first_float(rule_if, default=0.0)
        return t_peak > threshold and loss_after_peak >= 1.0
    if trial_id == "P12-T2":
        threshold = _extract_first_float(rule_if, default=0.0)
        return t_draw > threshold and outcome != "WIN"
    if trial_id == "P12-T3":
        return t_ttfp < 600.0 and t_draw > 1800.0
    return False


def _summarize(rows: List[Dict[str, Any]], trigger_flags: List[bool], max_false_cut_rate: float) -> Dict[str, Any]:
    sample_count = len(rows)
    triggered_count = sum(1 for f in trigger_flags if f)

    baseline_nets = [float(r.get("net_pips", 0.0) or 0.0) for r in rows]
    saved_loss = [float(r.get("loss_after_peak_pips", 0.0) or 0.0) if f else 0.0 for r, f in zip(rows, trigger_flags)]
    cf_nets = [
        float(r.get("net_pips", 0.0) or 0.0) + (float(r.get("loss_after_peak_pips", 0.0) or 0.0) if f else 0.0)
        for r, f in zip(rows, trigger_flags)
    ]

    winners = [r for r in rows if float(r.get("net_pips", 0.0) or 0.0) > 0.0]
    false_cut_winners = [
        r
        for r, f in zip(rows, trigger_flags)
        if f and float(r.get("net_pips", 0.0) or 0.0) > 0.0
    ]

    triggered_long_drawdown = sum(
        1
        for r, f in zip(rows, trigger_flags)
        if f and float(r.get("time_in_drawdown_seconds", 0.0) or 0.0) > 1800.0
    )

    false_cut_rate = (len(false_cut_winners) / len(winners)) if winners else 0.0

    primary = {
        "peak_to_close_loss_distribution_delta_positive": sum(saved_loss) > 0.0,
        "drawdown_duration_bucket_shift_positive": triggered_long_drawdown > 0,
        "false_cut_rate_within_limit": false_cut_rate <= max_false_cut_rate,
    }

    return {
        "sample_count": sample_count,
        "triggered_count": triggered_count,
        "trigger_rate": (triggered_count / sample_count) if sample_count else 0.0,
        "baseline_net_mean_pips": float(mean(baseline_nets)) if baseline_nets else 0.0,
        "estimated_counterfactual_net_mean_pips": float(mean(cf_nets)) if cf_nets else 0.0,
        "estimated_net_delta_mean_pips": (float(mean(cf_nets)) - float(mean(baseline_nets))) if baseline_nets else 0.0,
        "estimated_saved_loss_total_pips": float(sum(saved_loss)),
        "estimated_saved_loss_mean_pips": (float(sum(saved_loss)) / triggered_count) if triggered_count else 0.0,
        "winners_count": len(winners),
        "false_cut_winner_count": len(false_cut_winners),
        "false_cut_rate_on_winners": false_cut_rate,
        "triggered_long_drawdown_count": triggered_long_drawdown,
        "primary_metric_signals": primary,
        "primary_positive_count": sum(1 for v in primary.values() if v),
    }


def _stacked_summary(
    rows: List[Dict[str, Any]],
    pair_cells: List[Dict[str, Any]],
    max_false_cut_rate: float,
) -> Dict[str, Any]:
    ordered = sorted(pair_cells, key=lambda c: int(c.get("priority", 999) or 999))
    flags = [False] * len(rows)
    trigger_chain_counts: Dict[str, int] = {}
    for cell in ordered:
        trial_id = str(cell.get("trial_id", ""))
        rule_if = str(cell.get("counterfactual_rule", {}).get("if", ""))
        hit_count = 0
        for i, row in enumerate(rows):
            if not flags[i] and _triggered(trial_id, rule_if, row):
                flags[i] = True
                hit_count += 1
        trigger_chain_counts[trial_id] = hit_count

    summary = _summarize(rows, flags, max_false_cut_rate)
    summary["stack_trigger_chain_counts"] = trigger_chain_counts
    return summary


def _stress_slice_summary(
    rows: List[Dict[str, Any]],
    pair_cells: List[Dict[str, Any]],
    peak_p75: float,
    max_false_cut_rate: float,
) -> Dict[str, Any]:
    stress_rows = [
        r
        for r in rows
        if float(r.get("time_in_drawdown_seconds", 0.0) or 0.0) > 1800.0
        or float(r.get("time_from_peak_to_close_seconds", 0.0) or 0.0) > peak_p75
    ]
    return _stacked_summary(stress_rows, pair_cells, max_false_cut_rate)


def main() -> None:
    phase13 = _load_json(PHASE13_PATH)
    eur_tele = _load_json(EUR_TELE_PATH)
    gbp_tele = _load_json(GBP_TELE_PATH)

    max_false_cut_rate = float(
        phase13.get("acceptance_protocol", {})
        .get("promotion_thresholds", {})
        .get("max_false_cut_rate_on_winners", 0.15)
        or 0.15
    )

    rows_by_pair = {
        "EUR_USD": _rows_from_telemetry(eur_tele),
        "GBP_USD": _rows_from_telemetry(gbp_tele),
    }

    matrix = list(phase13.get("simulation_matrix", []))
    by_pair_cells: Dict[str, List[Dict[str, Any]]] = {"EUR_USD": [], "GBP_USD": []}
    for cell in matrix:
        p = str(cell.get("pair", ""))
        if p in by_pair_cells:
            by_pair_cells[p].append(cell)

    stage1: Dict[str, Any] = {}
    for pair in ["EUR_USD", "GBP_USD"]:
        rows = rows_by_pair[pair]
        trial_results: Dict[str, Any] = {}
        for cell in sorted(by_pair_cells[pair], key=lambda c: int(c.get("priority", 999) or 999)):
            trial_id = str(cell.get("trial_id", ""))
            rule_if = str(cell.get("counterfactual_rule", {}).get("if", ""))
            flags = [_triggered(trial_id, rule_if, r) for r in rows]
            trial_results[trial_id] = _summarize(rows, flags, max_false_cut_rate)
        stage1[pair] = trial_results

    stage2: Dict[str, Any] = {}
    for pair in ["EUR_USD", "GBP_USD"]:
        stage2[pair] = _stacked_summary(rows_by_pair[pair], by_pair_cells[pair], max_false_cut_rate)

    stage3: Dict[str, Any] = {}
    for pair in ["EUR_USD", "GBP_USD"]:
        peak_p75 = float(
            phase13.get("baseline_snapshot", {})
            .get(pair, {})
            .get("thresholds", {})
            .get("peak_to_close_p75", 0.0)
            or 0.0
        )
        stage3[pair] = _stress_slice_summary(rows_by_pair[pair], by_pair_cells[pair], peak_p75, max_false_cut_rate)

    by_pair_gate: Dict[str, Any] = {}
    requires_positive = int(
        phase13.get("acceptance_protocol", {})
        .get("promotion_thresholds", {})
        .get("requires_positive_primary_metrics", 2)
        or 2
    )
    for pair in ["EUR_USD", "GBP_USD"]:
        stacked = stage2[pair]
        by_pair_gate[pair] = {
            "primary_positive_count": int(stacked.get("primary_positive_count", 0) or 0),
            "requires_positive_primary_metrics": requires_positive,
            "false_cut_rate_on_winners": float(stacked.get("false_cut_rate_on_winners", 0.0) or 0.0),
            "max_false_cut_rate_on_winners": max_false_cut_rate,
            "passes_pair_gate": (
                int(stacked.get("primary_positive_count", 0) or 0) >= requires_positive
                and float(stacked.get("false_cut_rate_on_winners", 0.0) or 0.0) <= max_false_cut_rate
            ),
        }

    overall_pass = all(v["passes_pair_gate"] for v in by_pair_gate.values())

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE14_COUNTERFACTUAL_SIMULATION_EXECUTION_NO_TUNING",
        "scope_lock": phase13.get("scope_lock", {}),
        "dependency": {
            "phase13_status": phase13.get("status"),
            "phase13_path": str(PHASE13_PATH),
            "telemetry_paths": [str(EUR_TELE_PATH), str(GBP_TELE_PATH)],
        },
        "execution_mode": "OFFLINE_LABEL_ONLY",
        "stage_results": {
            "stage1_single_trial_isolation": stage1,
            "stage2_ordered_additive_stack": stage2,
            "stage3_stress_slices": stage3,
        },
        "acceptance_gate_evaluation": {
            "by_pair": by_pair_gate,
            "overall_pass": overall_pass,
            "promotion_thresholds": phase13.get("acceptance_protocol", {}).get("promotion_thresholds", {}),
        },
        "pass_conditions": {
            "phase13_dependency_passed": phase13.get("status") == "PASS",
            "stage1_completed": True,
            "stage2_completed": True,
            "stage3_completed": True,
            "acceptance_gate_evaluated": True,
            "no_tuning_applied": True,
        },
    }
    report["status"] = "PASS" if all(report["pass_conditions"].values()) else "FAIL"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"wrote {OUTPUT_PATH}")
    print(
        json.dumps(
            {
                "status": report["status"],
                "overall_pass": overall_pass,
                "pair_gate": {k: v["passes_pair_gate"] for k, v in by_pair_gate.items()},
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()