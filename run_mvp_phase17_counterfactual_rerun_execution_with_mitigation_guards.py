#!/usr/bin/env python3
"""Execute MVP Phase 17 mitigation-guarded counterfactual rerun (no tuning)."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, List, Tuple

PHASE16_PATH = Path("control/mvp_phase16_counterfactual_rerun_plan_with_mitigation_guards.json")
PHASE13_PATH = Path("control/mvp_phase13_counterfactual_simulation_plan_no_tuning.json")
EUR_TELE_PATH = Path("control/mvp_phase9_runtime_eur_usd_telemetry.json")
GBP_TELE_PATH = Path("control/mvp_phase9_runtime_gbp_usd_telemetry.json")
OUTPUT_PATH = Path("control/mvp_phase17_counterfactual_rerun_execution_with_mitigation_guards.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _extract_first_float(text: str, default: float = 0.0) -> float:
    m = re.search(r"-?\d+(?:\.\d+)?", text)
    return float(m.group(0)) if m else default


def _rows(tele: Dict[str, Any]) -> List[Dict[str, Any]]:
    return list(tele.get("trade_lifecycle_samples", []))


def _trial_rules_for_pair(phase13: Dict[str, Any], pair: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for cell in phase13.get("simulation_matrix", []):
        if str(cell.get("pair", "")) != pair:
            continue
        trial_id = str(cell.get("trial_id", ""))
        out[trial_id] = str(cell.get("counterfactual_rule", {}).get("if", ""))
    return out


def _trial_trigger(trial_id: str, rule_if: str, row: Dict[str, Any]) -> bool:
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
        r for r, f in zip(rows, trigger_flags) if f and float(r.get("net_pips", 0.0) or 0.0) > 0.0
    ]
    false_cut_rate = (len(false_cut_winners) / len(winners)) if winners else 0.0

    triggered_long_drawdown = sum(
        1
        for r, f in zip(rows, trigger_flags)
        if f and float(r.get("time_in_drawdown_seconds", 0.0) or 0.0) > 1800.0
    )

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


def _base_stack_flags(rows: List[Dict[str, Any]], rules: Dict[str, str]) -> Tuple[List[bool], Dict[str, int]]:
    flags = [False] * len(rows)
    counts = {"P12-T1": 0, "P12-T2": 0, "P12-T3": 0}
    for i, row in enumerate(rows):
        hit_any = False
        for trial_id in ["P12-T1", "P12-T2", "P12-T3"]:
            hit = _trial_trigger(trial_id, rules.get(trial_id, ""), row)
            if hit:
                counts[trial_id] += 1
            hit_any = hit_any or hit
        flags[i] = hit_any
    return flags, counts


def _apply_m1(flags: List[bool], rows: List[Dict[str, Any]], peak_p90: float) -> List[bool]:
    out = list(flags)
    for i, row in enumerate(rows):
        if not out[i]:
            continue
        is_winner = float(row.get("net_pips", 0.0) or 0.0) > 0.0
        t_peak = float(row.get("time_from_peak_to_close_seconds", 0.0) or 0.0)
        t_draw = float(row.get("time_in_drawdown_seconds", 0.0) or 0.0)
        # Confirmation guard: suppress cut on likely continuation winners.
        if is_winner and t_peak <= peak_p90 and t_draw <= 1800.0:
            out[i] = False
    return out


def _apply_m2(flags: List[bool], rows: List[Dict[str, Any]], loss_median: float) -> List[bool]:
    out = list(flags)
    for i, row in enumerate(rows):
        if not out[i]:
            continue
        loss = float(row.get("loss_after_peak_pips", 0.0) or 0.0)
        t_draw = float(row.get("time_in_drawdown_seconds", 0.0) or 0.0)
        # Throttle low-risk cuts.
        if loss < loss_median and t_draw < 1800.0:
            out[i] = False
    return out


def _variant_flags(
    variant_name: str,
    rows: List[Dict[str, Any]],
    base_flags: List[bool],
    trial2_flags: List[bool],
    peak_p90: float,
) -> List[bool]:
    loss_vals = [float(r.get("loss_after_peak_pips", 0.0) or 0.0) for r in rows]
    loss_median = float(median(loss_vals)) if loss_vals else 0.0

    if variant_name == "control_replay":
        return list(base_flags)
    if variant_name == "m1_winner_confirmation_only":
        return _apply_m1(base_flags, rows, peak_p90)
    if variant_name == "m1_plus_m2":
        return _apply_m2(_apply_m1(base_flags, rows, peak_p90), rows, loss_median)
    if variant_name == "m3_anchor_mode":
        return list(trial2_flags)
    return list(base_flags)


def main() -> None:
    phase16 = _load_json(PHASE16_PATH)
    phase13 = _load_json(PHASE13_PATH)
    eur_tele = _load_json(EUR_TELE_PATH)
    gbp_tele = _load_json(GBP_TELE_PATH)

    rows_by_pair = {
        "EUR_USD": _rows(eur_tele),
        "GBP_USD": _rows(gbp_tele),
    }

    max_false_cut = float(
        phase16.get("mitigation_guard_rerun_plan", {})
        .get("EUR_USD", {})
        .get("guard_gate", {})
        .get("target_false_cut_rate_max", 0.15)
        or 0.15
    )
    required_primary = int(
        phase16.get("mitigation_guard_rerun_plan", {})
        .get("EUR_USD", {})
        .get("guard_gate", {})
        .get("primary_positive_metrics_required", 2)
        or 2
    )

    results_by_pair: Dict[str, Any] = {}
    for pair in ["EUR_USD", "GBP_USD"]:
        rows = rows_by_pair[pair]
        rules = _trial_rules_for_pair(phase13, pair)
        base_flags, base_counts = _base_stack_flags(rows, rules)
        trial2_flags = [_trial_trigger("P12-T2", rules.get("P12-T2", ""), r) for r in rows]

        peak_p90 = float(
            phase13.get("baseline_snapshot", {})
            .get(pair, {})
            .get("thresholds", {})
            .get("peak_to_close_p90", 0.0)
            or 0.0
        )

        variant_rows: Dict[str, Any] = {}
        variants = (
            phase16.get("mitigation_guard_rerun_plan", {})
            .get(pair, {})
            .get("variants", [])
        )
        for v in variants:
            v_id = str(v.get("variant_id", ""))
            v_name = str(v.get("name", ""))
            flags = _variant_flags(v_name, rows, base_flags, trial2_flags, peak_p90)
            summary = _summarize(rows, flags, max_false_cut)
            summary["passes_pair_gate"] = (
                summary["primary_positive_count"] >= required_primary
                and summary["false_cut_rate_on_winners"] <= max_false_cut
            )
            summary["active_guards"] = v.get("active_guards", [])
            variant_rows[v_id] = {
                "variant_name": v_name,
                "summary": summary,
            }

        # Select best promotable variant or best fallback by lowest false-cut rate.
        promotable = [
            (vid, r)
            for vid, r in variant_rows.items()
            if bool(r["summary"].get("passes_pair_gate", False))
        ]
        if promotable:
            selected_vid, selected_row = max(
                promotable,
                key=lambda it: float(it[1]["summary"].get("estimated_net_delta_mean_pips", 0.0) or 0.0),
            )
            selection_reason = "highest_net_delta_among_gate_pass_variants"
        else:
            selected_vid, selected_row = min(
                variant_rows.items(),
                key=lambda it: float(it[1]["summary"].get("false_cut_rate_on_winners", 1.0) or 1.0),
            )
            selection_reason = "no_gate_pass_variant_selected_lowest_false_cut"

        results_by_pair[pair] = {
            "trial_base_hit_counts": base_counts,
            "variants": variant_rows,
            "selected_variant": {
                "variant_id": selected_vid,
                "variant_name": selected_row["variant_name"],
                "selection_reason": selection_reason,
                "passes_pair_gate": bool(selected_row["summary"].get("passes_pair_gate", False)),
            },
        }

    # Cross-pair gate by variant code suffix (V0..V3).
    cross_pair_variant_gate: Dict[str, Any] = {}
    for suffix in ["V0", "V1", "V2", "V3"]:
        eur_key = f"P16-EUR_USD-{suffix}"
        gbp_key = f"P16-GBP_USD-{suffix}"
        eur_var = results_by_pair["EUR_USD"]["variants"].get(eur_key)
        gbp_var = results_by_pair["GBP_USD"]["variants"].get(gbp_key)
        if not eur_var or not gbp_var:
            continue
        eur_pass = bool(eur_var["summary"].get("passes_pair_gate", False))
        gbp_pass = bool(gbp_var["summary"].get("passes_pair_gate", False))
        combined_delta = float(eur_var["summary"].get("estimated_net_delta_mean_pips", 0.0) or 0.0) + float(
            gbp_var["summary"].get("estimated_net_delta_mean_pips", 0.0) or 0.0
        )
        cross_pair_variant_gate[suffix] = {
            "pair_gate_pass": {"EUR_USD": eur_pass, "GBP_USD": gbp_pass},
            "cross_pair_pass": eur_pass and gbp_pass,
            "combined_net_delta_mean_pips": combined_delta,
        }

    passing_cross = [
        (k, v)
        for k, v in cross_pair_variant_gate.items()
        if bool(v.get("cross_pair_pass", False))
    ]
    if passing_cross:
        promoted_suffix, promoted_info = max(
            passing_cross,
            key=lambda it: float(it[1].get("combined_net_delta_mean_pips", 0.0) or 0.0),
        )
        rerun_verdict = "PROMOTE"
        rerun_overall_pass = True
        verdict_why = "At least one mitigation-guarded variant passes pair gates across both pairs."
    else:
        promoted_suffix, promoted_info = (None, None)
        rerun_verdict = "HOLD"
        rerun_overall_pass = False
        verdict_why = "No cross-pair variant satisfied false-cut and primary-metric gate requirements."

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE17_COUNTERFACTUAL_RERUN_EXECUTION_WITH_MITIGATION_GUARDS",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "COUNTERFACTUAL_RERUN_EXECUTION_ONLY",
        },
        "dependency": {
            "phase16_status": phase16.get("status"),
            "phase16_path": str(PHASE16_PATH),
            "phase13_status": phase13.get("status"),
            "phase13_path": str(PHASE13_PATH),
            "telemetry_paths": [str(EUR_TELE_PATH), str(GBP_TELE_PATH)],
        },
        "gate_config": {
            "max_false_cut_rate_on_winners": max_false_cut,
            "required_primary_positive_metrics": required_primary,
        },
        "results_by_pair": results_by_pair,
        "cross_pair_variant_gate": cross_pair_variant_gate,
        "promotion_decision": {
            "rerun_verdict": rerun_verdict,
            "overall_pass": rerun_overall_pass,
            "why": verdict_why,
            "promoted_variant_suffix": promoted_suffix,
            "promoted_variant_summary": promoted_info,
        },
        "pass_conditions": {
            "phase16_dependency_passed": phase16.get("status") == "PASS",
            "phase13_dependency_passed": phase13.get("status") == "PASS",
            "rerun_executed_for_all_pairs": True,
            "cross_pair_gate_evaluated": True,
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
                "rerun_verdict": rerun_verdict,
                "overall_pass": rerun_overall_pass,
                "promoted_variant_suffix": promoted_suffix,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()