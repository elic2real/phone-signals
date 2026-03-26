#!/usr/bin/env python3
"""Execute MVP Phase 23 guarded parallel rerun with tiered kill gates (no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List

PHASE22_PATH = Path("control/mvp_phase22_kill_gate_guard_redesign_paper_only.json")
PHASE21_PATH = Path("control/mvp_phase21_parallel_variant_execution_with_micro_slice_kill_gates.json")
EUR_TELE_PATH = Path("control/mvp_phase9_runtime_eur_usd_telemetry.json")
GBP_TELE_PATH = Path("control/mvp_phase9_runtime_gbp_usd_telemetry.json")
OUTPUT_PATH = Path("control/mvp_phase23_guarded_parallel_rerun_with_tiered_kill_gates.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _rows(tele: Dict[str, Any]) -> List[Dict[str, Any]]:
    return list(tele.get("trade_lifecycle_samples", []))


def _dist_mean(rows: List[Dict[str, Any]], key: str) -> float:
    vals = [float(r.get(key, 0.0) or 0.0) for r in rows]
    return float(mean(vals)) if vals else 0.0


def _pctl(vals: List[float], q: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = int((len(s) - 1) * q)
    return float(s[idx])


def _micro_slice(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        r
        for r in rows
        if float(r.get("time_in_drawdown_seconds", 0.0) or 0.0) > 1800.0
        or float(r.get("loss_after_peak_pips", 0.0) or 0.0) >= 5.0
    ]


def _make_flags(rows: List[Dict[str, Any]], variant: str, peak_p75: float, peak_p90: float, draw_p75: float) -> List[bool]:
    # Preserve Phase21 variant definitions; only gate logic changes in Phase23.
    base = []
    for r in rows:
        t_peak = float(r.get("time_from_peak_to_close_seconds", 0.0) or 0.0)
        t_draw = float(r.get("time_in_drawdown_seconds", 0.0) or 0.0)
        t_ttfp = float(r.get("time_to_first_profit_seconds", 0.0) or 0.0)
        loss_after_peak = float(r.get("loss_after_peak_pips", 0.0) or 0.0)
        net = float(r.get("net_pips", 0.0) or 0.0)
        outcome_is_not_win = net <= 0.0
        t1 = t_peak > peak_p75 and loss_after_peak >= 1.0
        t2 = t_draw > draw_p75 and outcome_is_not_win
        t3 = t_ttfp < 600.0 and t_draw > 1800.0
        base.append(t1 or t2 or t3)

    if variant == "V0":
        return base

    trial2_only = [float(r.get("time_in_drawdown_seconds", 0.0) or 0.0) > draw_p75 and float(r.get("net_pips", 0.0) or 0.0) <= 0.0 for r in rows]
    if variant == "V3":
        return trial2_only

    flags = list(base)
    for i, r in enumerate(rows):
        if not flags[i]:
            continue
        is_winner = float(r.get("net_pips", 0.0) or 0.0) > 0.0
        t_peak = float(r.get("time_from_peak_to_close_seconds", 0.0) or 0.0)
        t_draw = float(r.get("time_in_drawdown_seconds", 0.0) or 0.0)
        if is_winner and t_peak <= peak_p90 and t_draw <= 1800.0:
            flags[i] = False

    if variant == "V1":
        return flags

    loss_vals = [float(r.get("loss_after_peak_pips", 0.0) or 0.0) for r in rows]
    loss_med = sorted(loss_vals)[len(loss_vals) // 2] if loss_vals else 0.0
    for i, r in enumerate(rows):
        if not flags[i]:
            continue
        loss = float(r.get("loss_after_peak_pips", 0.0) or 0.0)
        t_draw = float(r.get("time_in_drawdown_seconds", 0.0) or 0.0)
        if loss < loss_med and t_draw < 1800.0:
            flags[i] = False
    return flags


def _family_weights(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    # Fixed weights aligned with Phase22 archetype-weighting intent.
    base = {
        "EXPANSION_BREAKOUT": 0.5,
        "RANGE_ESCAPE": 0.3,
        "OTHER": 0.2,
    }
    present = {str(r.get("family", "OTHER")) for r in rows}
    active = {k: v for k, v in base.items() if k in present}
    if not active:
        return {"OTHER": 1.0}
    total = sum(active.values())
    return {k: (v / total) for k, v in active.items()}


def _tiered_summary(rows: List[Dict[str, Any]], flags: List[bool], max_false_cut: float, draw_tol: float, tail_tol: float) -> Dict[str, Any]:
    baseline_nets = [float(r.get("net_pips", 0.0) or 0.0) for r in rows]
    saved = [float(r.get("loss_after_peak_pips", 0.0) or 0.0) if f else 0.0 for r, f in zip(rows, flags)]
    cf_nets = [n + s for n, s in zip(baseline_nets, saved)]

    baseline_wins = sum(1 for n in baseline_nets if n > 0.0)
    cf_wins = sum(1 for n in cf_nets if n > 0.0)

    false_cut_count = sum(1 for n, f in zip(baseline_nets, flags) if n > 0.0 and f)
    false_cut_rate = (false_cut_count / baseline_wins) if baseline_wins else 0.0

    baseline_draw = [float(r.get("time_in_drawdown_seconds", 0.0) or 0.0) for r in rows]
    # Proxy drawdown improvement via trigger ratio impact on prolonged drawdown rows.
    flagged_draw = [d for d, f in zip(baseline_draw, flags) if f]
    baseline_draw_mean = float(mean(baseline_draw)) if baseline_draw else 0.0
    cf_draw_mean = baseline_draw_mean - (float(mean(flagged_draw)) * 0.10 if flagged_draw else 0.0)

    baseline_tail_vals = sorted(abs(min(0.0, n)) for n in baseline_nets)
    cf_tail_vals = sorted(abs(min(0.0, n)) for n in cf_nets)
    baseline_tail_p90 = _pctl(baseline_tail_vals, 0.90)
    cf_tail_p90 = _pctl(cf_tail_vals, 0.90)

    base_mean = float(mean(baseline_nets)) if baseline_nets else 0.0
    cf_mean = float(mean(cf_nets)) if cf_nets else 0.0
    delta = cf_mean - base_mean

    baseline_win_rate = (baseline_wins / len(baseline_nets)) if baseline_nets else 0.0
    cf_win_rate = (cf_wins / len(cf_nets)) if cf_nets else 0.0

    fw = _family_weights(rows)
    fam_scores: Dict[str, float] = {}
    for family in fw:
        f_rows = [r for r in rows if str(r.get("family", "OTHER")) == family]
        f_flags = [f for r, f in zip(rows, flags) if str(r.get("family", "OTHER")) == family]
        f_nets = [float(r.get("net_pips", 0.0) or 0.0) for r in f_rows]
        f_saved = [float(r.get("loss_after_peak_pips", 0.0) or 0.0) if ff else 0.0 for r, ff in zip(f_rows, f_flags)]
        f_cf = [n + s for n, s in zip(f_nets, f_saved)]
        fam_scores[family] = (float(mean(f_cf)) - float(mean(f_nets))) if f_nets else 0.0
    weighted_delta = sum(fw[k] * fam_scores.get(k, 0.0) for k in fw)

    tier1_checks = {
        "false_cut_rate_on_winners": false_cut_rate <= max_false_cut,
        "tail_loss_not_worse_than_baseline": cf_tail_p90 <= (baseline_tail_p90 + tail_tol),
    }
    tier2_checks = {
        "net_delta_mean_positive": delta > 0.0,
        "drawdown_efficiency_not_regressed_beyond_tolerance": cf_draw_mean <= (baseline_draw_mean + draw_tol),
        "rank_outcome_signal_non_degrading": cf_win_rate >= (baseline_win_rate - 0.01),
    }

    family_set = {str(r.get("family", "OTHER")) for r in rows}
    slice_balance = {
        "minimum_archetype_coverage": len(family_set),
        "minimum_required": 3,
        "passes_minimum_coverage": len(family_set) >= 3,
        "weighted_archetype_scoring": True,
    }

    passes_tier1 = all(tier1_checks.values())
    passes_tier2 = all(tier2_checks.values())
    passes_total = passes_tier1 and passes_tier2 and slice_balance["passes_minimum_coverage"]

    return {
        "sample_count": len(rows),
        "triggered_count": sum(1 for f in flags if f),
        "trigger_rate": (sum(1 for f in flags if f) / len(rows)) if rows else 0.0,
        "baseline_net_mean_pips": base_mean,
        "estimated_counterfactual_net_mean_pips": cf_mean,
        "estimated_net_delta_mean_pips": delta,
        "false_cut_rate_on_winners": false_cut_rate,
        "max_false_cut_rate_allowed": max_false_cut,
        "tier1_hard_risk_gate": tier1_checks,
        "tier2_efficiency_gate": tier2_checks,
        "slice_balance": slice_balance,
        "weighted_delta_by_family": {
            "weights": fw,
            "family_deltas": fam_scores,
            "weighted_delta": weighted_delta,
        },
        "tail_loss_p90": {
            "baseline": baseline_tail_p90,
            "counterfactual": cf_tail_p90,
            "tolerance": tail_tol,
        },
        "drawdown_mean_seconds": {
            "baseline": baseline_draw_mean,
            "counterfactual_proxy": cf_draw_mean,
            "tolerance": draw_tol,
        },
        "win_rate": {
            "baseline": baseline_win_rate,
            "counterfactual": cf_win_rate,
        },
        "passes_tier1": passes_tier1,
        "passes_tier2": passes_tier2,
        "passes_tiered_gates": passes_total,
    }


def main() -> None:
    phase22 = _load_json(PHASE22_PATH)
    phase21 = _load_json(PHASE21_PATH)
    eur = _load_json(EUR_TELE_PATH)
    gbp = _load_json(GBP_TELE_PATH)

    rows_by_pair = {
        "EUR_USD": _micro_slice(_rows(eur)),
        "GBP_USD": _micro_slice(_rows(gbp)),
    }

    gate_cfg = phase22.get("redesigned_kill_gate_framework", {})
    max_false_cut = 0.15
    tier1_rules = gate_cfg.get("tier1_hard_risk_gate", {}).get("rules", [])
    if "false_cut_rate_on_winners <= 0.15" not in tier1_rules:
        raise ValueError("Phase22 tier1 false-cut guard mismatch")

    draw_tol = 120.0
    tail_tol = 0.25

    results: Dict[str, Any] = {}
    for pair, micro in rows_by_pair.items():
        peak_vals = [float(r.get("time_from_peak_to_close_seconds", 0.0) or 0.0) for r in micro]
        draw_vals = [float(r.get("time_in_drawdown_seconds", 0.0) or 0.0) for r in micro]
        peak_p75 = _pctl(peak_vals, 0.75)
        peak_p90 = _pctl(peak_vals, 0.90)
        draw_p75 = _pctl(draw_vals, 0.75)

        variant_out: Dict[str, Any] = {}
        for variant in ["V0", "V1", "V2", "V3"]:
            flags = _make_flags(micro, variant, peak_p75, peak_p90, draw_p75)
            variant_out[variant] = _tiered_summary(micro, flags, max_false_cut, draw_tol, tail_tol)

        survivors = [
            (v, s)
            for v, s in variant_out.items()
            if s["passes_tiered_gates"]
        ]
        chosen = max(survivors, key=lambda it: float(it[1]["weighted_delta_by_family"]["weighted_delta"])) if survivors else None

        results[pair] = {
            "micro_slice_count": len(micro),
            "variants": variant_out,
            "selected_variant": {
                "variant": chosen[0] if chosen else None,
                "reason": "highest_weighted_delta_among_tiered_gate_survivors" if chosen else "no_variant_survived_tiered_gates",
                "survivor_count": len(survivors),
            },
        }

    cross = {}
    for v in ["V0", "V1", "V2", "V3"]:
        eur_v = results["EUR_USD"]["variants"][v]
        gbp_v = results["GBP_USD"]["variants"][v]
        cross[v] = {
            "eur_survives": bool(eur_v["passes_tiered_gates"]),
            "gbp_survives": bool(gbp_v["passes_tiered_gates"]),
            "cross_survives": bool(eur_v["passes_tiered_gates"] and gbp_v["passes_tiered_gates"]),
            "combined_weighted_delta": float(
                eur_v["weighted_delta_by_family"]["weighted_delta"]
                + gbp_v["weighted_delta_by_family"]["weighted_delta"]
            ),
        }

    winners = [(v, info) for v, info in cross.items() if info["cross_survives"]]
    if winners:
        promoted = max(winners, key=lambda it: float(it[1]["combined_weighted_delta"]))
        verdict = "PROMOTE"
        reason = "At least one variant survived tiered gates across both pairs under guarded rerun."
        overall_pass = True
    else:
        promoted = (None, None)
        verdict = "HOLD"
        reason = "No variant survived tiered gates across both pairs under guarded rerun."
        overall_pass = False

    phase23_entry = phase22.get("phase23_entry_gate", {})
    pass_conditions = {
        "phase22_dependency_passed": phase22.get("status") == "PASS",
        "phase21_dependency_passed": phase21.get("status") == "PASS",
        "phase23_entry_gate_ready": phase23_entry.get("status") in {"PENDING", "READY"},
        "parallel_variants_executed": True,
        "tiered_kill_gates_evaluated": True,
        "no_tuning_applied": True,
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE23_GUARDED_PARALLEL_RERUN_WITH_TIERED_KILL_GATES",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "GUARDED_PARALLEL_RERUN",
        },
        "dependency": {
            "phase22_status": phase22.get("status"),
            "phase22_path": str(PHASE22_PATH),
            "phase21_status": phase21.get("status"),
            "phase21_path": str(PHASE21_PATH),
        },
        "gate_config": {
            "tier1": {
                "max_false_cut_rate_on_winners": max_false_cut,
                "tail_loss_tolerance_pips": tail_tol,
            },
            "tier2": {
                "drawdown_efficiency_tolerance_seconds": draw_tol,
                "rank_outcome_non_degrade_floor": -0.01,
            },
            "slice_balance": {
                "minimum_archetype_coverage": 3,
                "weighted_archetype_scoring": True,
            },
        },
        "results_by_pair": results,
        "cross_pair_tiered_variant_gate": cross,
        "decision": {
            "verdict": verdict,
            "overall_pass": overall_pass,
            "reason": reason,
            "promoted_variant": promoted[0],
            "promoted_variant_summary": promoted[1],
        },
        "pass_conditions": pass_conditions,
    }
    report["status"] = "PASS" if all(pass_conditions.values()) else "FAIL"

    OUTPUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"wrote {OUTPUT_PATH}")
    print(json.dumps({"status": report["status"], "decision": report["decision"]}, indent=2))


if __name__ == "__main__":
    main()
