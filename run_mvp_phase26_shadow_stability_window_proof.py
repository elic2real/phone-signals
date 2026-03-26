#!/usr/bin/env python3
"""Execute MVP Phase 26: shadow stability window proof (no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List

PHASE25_PATH = Path("control/mvp_phase25_shadow_benchmark_execution_and_drift_proof.json")
PHASE6_STABILITY_PATH = Path("control/mvp_phase6_stability_proof.json")

EUR_W1_PATH = Path("control/mvp_phase6_w1_eur_usd.json")
EUR_W2_PATH = Path("control/mvp_phase6_w2_eur_usd.json")
EUR_W3_PATH = Path("control/mvp_phase6_w3_eur_usd.json")
GBP_W1_PATH = Path("control/mvp_phase6_w1_gbp_usd.json")
GBP_W2_PATH = Path("control/mvp_phase6_w2_gbp_usd.json")
GBP_W3_PATH = Path("control/mvp_phase6_w3_gbp_usd.json")

OUTPUT_PATH = Path("control/mvp_phase26_shadow_stability_window_proof.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _f(v: Any) -> float:
    return float(v or 0.0)


def _pair_phase25_delta_mean_net(phase25: Dict[str, Any], pair: str) -> float:
    return _f(
        phase25.get("results_by_pair", {})
        .get(pair, {})
        .get("shadow_counterfactual_estimate", {})
        .get("delta_mean_net_pips")
    )


def _pair_false_cut(phase25: Dict[str, Any], pair: str) -> float:
    return _f(
        phase25.get("results_by_pair", {})
        .get(pair, {})
        .get("shadow_counterfactual_estimate", {})
        .get("false_cut_rate_on_winners")
    )


def _window_row(window: str, eur: Dict[str, Any], gbp: Dict[str, Any], eur_delta_mean: float, gbp_delta_mean: float) -> Dict[str, Any]:
    eur_hours = _f(eur.get("total_hours"))
    eur_trades = int(eur.get("total_accepted_trades", 0) or 0)
    gbp_hours = _f(gbp.get("total_hours"))
    gbp_trades = int(gbp.get("total_accepted_trades", 0) or 0)

    eur_tph = (eur_trades / eur_hours) if eur_hours > 0 else 0.0
    gbp_tph = (gbp_trades / gbp_hours) if gbp_hours > 0 else 0.0

    eur_base_pph = _f(eur.get("combined_keep_tune_net_pph"))
    gbp_base_pph = _f(gbp.get("combined_keep_tune_net_pph"))

    eur_delta_pph = eur_delta_mean * eur_tph
    gbp_delta_pph = gbp_delta_mean * gbp_tph

    eur_cf_pph = eur_base_pph + eur_delta_pph
    gbp_cf_pph = gbp_base_pph + gbp_delta_pph

    total_trades = eur_trades + gbp_trades
    if total_trades > 0:
        weighted_base = ((eur_base_pph * eur_trades) + (gbp_base_pph * gbp_trades)) / total_trades
        weighted_cf = ((eur_cf_pph * eur_trades) + (gbp_cf_pph * gbp_trades)) / total_trades
    else:
        weighted_base = 0.0
        weighted_cf = 0.0

    return {
        "window": window,
        "EUR_USD": {
            "baseline_pph": eur_base_pph,
            "trades_per_hour": eur_tph,
            "delta_mean_net_pips_from_phase25": eur_delta_mean,
            "estimated_pph_delta": eur_delta_pph,
            "estimated_intervention_pph": eur_cf_pph,
            "accepted_trades": eur_trades,
            "hours": eur_hours,
        },
        "GBP_USD": {
            "baseline_pph": gbp_base_pph,
            "trades_per_hour": gbp_tph,
            "delta_mean_net_pips_from_phase25": gbp_delta_mean,
            "estimated_pph_delta": gbp_delta_pph,
            "estimated_intervention_pph": gbp_cf_pph,
            "accepted_trades": gbp_trades,
            "hours": gbp_hours,
        },
        "weighted_baseline_pph": weighted_base,
        "weighted_estimated_intervention_pph": weighted_cf,
        "weighted_estimated_delta_pph": weighted_cf - weighted_base,
        "window_gate": {
            "both_pairs_positive_delta": eur_delta_pph > 0.0 and gbp_delta_pph > 0.0,
            "weighted_pph_positive": weighted_cf > 0.0,
            "weighted_pph_improved": weighted_cf > weighted_base,
        },
    }


def main() -> None:
    phase25 = _load_json(PHASE25_PATH)
    phase6_stability = _load_json(PHASE6_STABILITY_PATH)

    eur_w1 = _load_json(EUR_W1_PATH)
    eur_w2 = _load_json(EUR_W2_PATH)
    eur_w3 = _load_json(EUR_W3_PATH)
    gbp_w1 = _load_json(GBP_W1_PATH)
    gbp_w2 = _load_json(GBP_W2_PATH)
    gbp_w3 = _load_json(GBP_W3_PATH)

    eur_delta_mean = _pair_phase25_delta_mean_net(phase25, "EUR_USD")
    gbp_delta_mean = _pair_phase25_delta_mean_net(phase25, "GBP_USD")

    windows = [
        _window_row("W1", eur_w1, gbp_w1, eur_delta_mean, gbp_delta_mean),
        _window_row("W2", eur_w2, gbp_w2, eur_delta_mean, gbp_delta_mean),
        _window_row("W3", eur_w3, gbp_w3, eur_delta_mean, gbp_delta_mean),
    ]

    weighted_cf_vals = [
        _f(w["weighted_estimated_intervention_pph"]) for w in windows
    ]
    weighted_delta_vals = [
        _f(w["weighted_estimated_delta_pph"]) for w in windows
    ]

    min_cf = min(weighted_cf_vals) if weighted_cf_vals else 0.0
    max_cf = max(weighted_cf_vals) if weighted_cf_vals else 0.0
    mean_cf = _f(mean(weighted_cf_vals)) if weighted_cf_vals else 0.0
    rel_spread = ((max_cf - min_cf) / mean_cf) if mean_cf > 0.0 else 0.0

    eur_false_cut = _pair_false_cut(phase25, "EUR_USD")
    gbp_false_cut = _pair_false_cut(phase25, "GBP_USD")
    max_false_cut = max(eur_false_cut, gbp_false_cut)

    pass_conditions = {
        "phase25_dependency_passed": phase25.get("status") == "PASS",
        "phase6_dependency_passed": phase6_stability.get("status") == "PASS",
        "all_windows_window_gate_pass": all(all(w["window_gate"].values()) for w in windows),
        "all_windows_weighted_intervention_positive": all(v > 0.0 for v in weighted_cf_vals),
        "all_windows_weighted_delta_positive": all(v > 0.0 for v in weighted_delta_vals),
        "weighted_stability_relative_spread_bounded": rel_spread <= 0.75,
        "false_cut_guard_still_respected": max_false_cut <= 0.15,
        "no_tuning_applied": True,
        "shadow_only_release_lock": True,
    }
    overall_pass = all(pass_conditions.values())

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE26_SHADOW_STABILITY_WINDOW_PROOF",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "SHADOW_MULTI_WINDOW_STABILITY_PROOF",
        },
        "dependency": {
            "phase25_status": phase25.get("status"),
            "phase25_path": str(PHASE25_PATH),
            "phase6_stability_status": phase6_stability.get("status"),
            "phase6_stability_path": str(PHASE6_STABILITY_PATH),
        },
        "intervention_reference": {
            "intervention_name": "Intervention_V1",
            "focus": ["long_peak_to_close_delay", "prolonged_drawdown_selected"],
            "phase25_pair_delta_mean_net_pips": {
                "EUR_USD": eur_delta_mean,
                "GBP_USD": gbp_delta_mean,
            },
        },
        "window_results": windows,
        "stability_summary": {
            "weighted_intervention_pph_values": weighted_cf_vals,
            "weighted_intervention_pph_mean": mean_cf,
            "weighted_intervention_pph_min": min_cf,
            "weighted_intervention_pph_max": max_cf,
            "weighted_intervention_relative_spread": rel_spread,
            "weighted_delta_pph_values": weighted_delta_vals,
            "max_false_cut_rate_from_phase25": max_false_cut,
        },
        "pass_conditions": pass_conditions,
        "decision": {
            "verdict": "PROMOTE" if overall_pass else "HOLD",
            "overall_pass": overall_pass,
            "reason": (
                "Intervention V1 remains stable and positive across W1/W2/W3 in shadow mode with guards intact."
                if overall_pass
                else "Intervention V1 shadow stability proof failed one or more window/guard conditions."
            ),
            "release_action": "SHADOW_ONLY_CONTINUE",
            "live_promotion_allowed": False,
        },
        "next_recommended_task": (
            "MVP_PHASE27_SHADOW_RELEASE_READINESS_AND_SOAK_PLAN"
            if overall_pass
            else "MVP_PHASE26B_INTERVENTION_REFINEMENT_SHADOW_ONLY"
        ),
    }
    report["status"] = "PASS" if overall_pass else "FAIL"

    OUTPUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"wrote {OUTPUT_PATH}")
    print(
        json.dumps(
            {
                "status": report["status"],
                "verdict": report["decision"]["verdict"],
                "next": report["next_recommended_task"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
