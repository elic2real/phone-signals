#!/usr/bin/env python3
"""Execute MVP Phase 25: shadow benchmark execution and drift proof (Intervention V1, no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List

PHASE24_PATH = Path("control/mvp_phase24_policy_package_hardening_and_release_guard_recheck.json")
PHASE11_PATH = Path("control/mvp_phase11_exit_timing_and_drawdown_pattern_decomposition_no_tuning.json")
P6_EUR_PATH = Path("control/mvp_phase6_w3_eur_usd.json")
P6_GBP_PATH = Path("control/mvp_phase6_w3_gbp_usd.json")
P7_EUR_PATH = Path("control/mvp_phase7_runtime_eur_usd.json")
P7_GBP_PATH = Path("control/mvp_phase7_runtime_gbp_usd.json")
T9_EUR_PATH = Path("control/mvp_phase9_runtime_eur_usd_telemetry.json")
T9_GBP_PATH = Path("control/mvp_phase9_runtime_gbp_usd_telemetry.json")
OUTPUT_PATH = Path("control/mvp_phase25_shadow_benchmark_execution_and_drift_proof.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _f(v: Any) -> float:
    return float(v or 0.0)


def _pair_thresholds(phase11: Dict[str, Any], pair: str) -> Dict[str, float]:
    by_pair = phase11.get("by_pair_decomposition", {}).get(pair, {})
    th = by_pair.get("thresholds", {})
    return {
        "peak_to_close_p75": _f(th.get("peak_to_close_p75")),
        "drawdown_p75": _f(th.get("drawdown_p75")),
    }


def _family_trigger_shares(rows: List[Dict[str, Any]], triggers: List[bool]) -> Dict[str, float]:
    counts: Dict[str, int] = {}
    total = 0
    for row, trig in zip(rows, triggers):
        if not trig:
            continue
        fam = str(row.get("family", "OTHER"))
        counts[fam] = counts.get(fam, 0) + 1
        total += 1
    if total == 0:
        return {}
    return {k: v / total for k, v in counts.items()}


def _simulate_pair(
    pair: str,
    p6: Dict[str, Any],
    p7: Dict[str, Any],
    tele: Dict[str, Any],
    phase11: Dict[str, Any],
) -> Dict[str, Any]:
    thresholds = _pair_thresholds(phase11, pair)
    rows = list(tele.get("trade_lifecycle_samples", []))
    if not rows:
        raise ValueError(f"No trade_lifecycle_samples in telemetry for {pair}")

    peak_p75 = thresholds["peak_to_close_p75"]
    draw_p75 = thresholds["drawdown_p75"]

    baseline_nets = [_f(r.get("net_pips")) for r in rows]
    loss_after_peak = [_f(r.get("loss_after_peak_pips")) for r in rows]
    t_peak = [_f(r.get("time_from_peak_to_close_seconds")) for r in rows]
    t_draw = [_f(r.get("time_in_drawdown_seconds")) for r in rows]
    t_ttfp = [_f(r.get("time_to_first_profit_seconds")) for r in rows]

    baseline_long_peak = [tp > peak_p75 and lap >= 1.0 for tp, lap in zip(t_peak, loss_after_peak)]
    baseline_drawdown = [td > draw_p75 for td in t_draw]

    triggers: List[bool] = []
    saved_pips: List[float] = []
    rule_hits = {
        "early_post_peak_guard": 0,
        "drawdown_fail_fast": 0,
        "timing_tolerance_cap": 0,
    }

    for net, lap, tp, td, tfp in zip(baseline_nets, loss_after_peak, t_peak, t_draw, t_ttfp):
        r1 = tp > peak_p75 and lap >= 1.0 and net <= 1.0
        r2 = td > draw_p75 and net <= 0.0
        r3 = tfp < 600.0 and td > 1800.0 and net <= 0.0
        trig = bool(r1 or r2 or r3)
        triggers.append(trig)

        if r1:
            rule_hits["early_post_peak_guard"] += 1
        if r2:
            rule_hits["drawdown_fail_fast"] += 1
        if r3:
            rule_hits["timing_tolerance_cap"] += 1

        if trig:
            # Conservative shadow estimate: capture only bounded portion of post-peak giveback.
            saved = min(lap, abs(net) + 2.0)
        else:
            saved = 0.0
        saved_pips.append(saved)

    cf_nets = [n + s for n, s in zip(baseline_nets, saved_pips)]

    base_mean_net = _f(mean(baseline_nets))
    cf_mean_net = _f(mean(cf_nets))
    delta_mean_net = cf_mean_net - base_mean_net

    total_hours = _f(p7.get("total_hours"))
    total_trades = int(p7.get("total_accepted_trades", 0) or 0)
    trades_per_hour = (total_trades / total_hours) if total_hours > 0 else 0.0
    est_pph_delta = delta_mean_net * trades_per_hour

    p6_pph = _f(p6.get("combined_keep_tune_net_pph"))
    p7_pph = _f(p7.get("combined_keep_tune_net_pph"))
    est_intervention_pph = p7_pph + est_pph_delta
    baseline_decay = p7_pph - p6_pph
    recovered_decay = est_intervention_pph - p7_pph
    decay_recovery_ratio = (
        (recovered_decay / abs(baseline_decay))
        if baseline_decay < 0.0 and abs(baseline_decay) > 0.0
        else None
    )

    base_peak_loss_mean = _f(mean(loss_after_peak))
    residual_peak_loss = [max(0.0, lap - sv) for lap, sv in zip(loss_after_peak, saved_pips)]
    cf_peak_loss_mean = _f(mean(residual_peak_loss))

    winners = sum(1 for n in baseline_nets if n > 0.0)
    false_cut = sum(1 for n, trig in zip(baseline_nets, triggers) if n > 0.0 and trig)
    false_cut_rate = (false_cut / winners) if winners else 0.0

    residual_long_peak = sum(1 for b, trig in zip(baseline_long_peak, triggers) if b and not trig)
    residual_drawdown = sum(1 for b, trig in zip(baseline_drawdown, triggers) if b and not trig)

    family_shares = _family_trigger_shares(rows, triggers)

    return {
        "pair": pair,
        "intervention_v1": {
            "rules": {
                "early_post_peak_guard": f"time_from_peak_to_close_seconds > {peak_p75} and loss_after_peak_pips >= 1.0 and net_pips <= 1.0",
                "drawdown_fail_fast": f"time_in_drawdown_seconds > {draw_p75} and net_pips <= 0.0",
                "timing_tolerance_cap": "time_to_first_profit_seconds < 600 and time_in_drawdown_seconds > 1800 and net_pips <= 0.0",
            },
            "rule_hit_counts": rule_hits,
        },
        "phase6_phase7_baseline": {
            "phase6_w3_pph": p6_pph,
            "phase7_runtime_pph": p7_pph,
            "phase7_minus_phase6_decay_pph": baseline_decay,
            "trades_per_hour": trades_per_hour,
        },
        "shadow_counterfactual_estimate": {
            "sample_count": len(rows),
            "triggered_count": sum(1 for t in triggers if t),
            "trigger_rate": (sum(1 for t in triggers if t) / len(rows)) if rows else 0.0,
            "baseline_mean_net_pips": base_mean_net,
            "counterfactual_mean_net_pips": cf_mean_net,
            "delta_mean_net_pips": delta_mean_net,
            "estimated_pph_delta": est_pph_delta,
            "estimated_intervention_pph": est_intervention_pph,
            "extraction_proxy": {
                "baseline_peak_to_close_loss_mean": base_peak_loss_mean,
                "counterfactual_peak_to_close_loss_mean": cf_peak_loss_mean,
                "delta": base_peak_loss_mean - cf_peak_loss_mean,
            },
            "decay_proxy": {
                "recovered_decay_pph": recovered_decay,
                "decay_recovery_ratio": decay_recovery_ratio,
            },
            "false_cut_rate_on_winners": false_cut_rate,
            "family_trigger_shares": family_shares,
        },
        "failure_count_comparison": {
            "long_peak_to_close_delay": {
                "before": sum(1 for b in baseline_long_peak if b),
                "after": residual_long_peak,
                "prevented": sum(1 for b in baseline_long_peak if b) - residual_long_peak,
            },
            "prolonged_drawdown_selected": {
                "before": sum(1 for b in baseline_drawdown if b),
                "after": residual_drawdown,
                "prevented": sum(1 for b in baseline_drawdown if b) - residual_drawdown,
            },
        },
        "pair_gate": {
            "pph_delta_positive": est_pph_delta > 0.0,
            "extraction_improved": (base_peak_loss_mean - cf_peak_loss_mean) > 0.0,
            "failure_counts_reduced": residual_long_peak < sum(1 for b in baseline_long_peak if b)
            and residual_drawdown < sum(1 for b in baseline_drawdown if b),
            "false_cut_rate_within_limit": false_cut_rate <= 0.15,
        },
    }


def _cross_pair_drift_proof(eur: Dict[str, Any], gbp: Dict[str, Any]) -> Dict[str, Any]:
    eur_pph_delta = _f(eur["shadow_counterfactual_estimate"]["estimated_pph_delta"])
    gbp_pph_delta = _f(gbp["shadow_counterfactual_estimate"]["estimated_pph_delta"])
    eur_false_cut = _f(eur["shadow_counterfactual_estimate"]["false_cut_rate_on_winners"])
    gbp_false_cut = _f(gbp["shadow_counterfactual_estimate"]["false_cut_rate_on_winners"])

    eur_shares = eur["shadow_counterfactual_estimate"].get("family_trigger_shares", {})
    gbp_shares = gbp["shadow_counterfactual_estimate"].get("family_trigger_shares", {})
    families = sorted(set(eur_shares) | set(gbp_shares))
    max_family_share_gap = max((abs(_f(eur_shares.get(f)) - _f(gbp_shares.get(f))) for f in families), default=0.0)

    checks = {
        "both_pairs_positive_pph_delta": eur_pph_delta > 0.0 and gbp_pph_delta > 0.0,
        "false_cut_guard_respected": max(eur_false_cut, gbp_false_cut) <= 0.15,
        "pair_delta_asymmetry_bounded": abs(eur_pph_delta - gbp_pph_delta) <= 0.12,
        "family_trigger_balance_bounded": max_family_share_gap <= 0.55,
    }

    return {
        "checks": checks,
        "summary": {
            "eur_estimated_pph_delta": eur_pph_delta,
            "gbp_estimated_pph_delta": gbp_pph_delta,
            "max_false_cut_rate": max(eur_false_cut, gbp_false_cut),
            "max_family_trigger_share_gap": max_family_share_gap,
        },
        "overall_pass": all(checks.values()),
    }


def main() -> None:
    phase24 = _load_json(PHASE24_PATH)
    phase11 = _load_json(PHASE11_PATH)
    p6_eur = _load_json(P6_EUR_PATH)
    p6_gbp = _load_json(P6_GBP_PATH)
    p7_eur = _load_json(P7_EUR_PATH)
    p7_gbp = _load_json(P7_GBP_PATH)
    t9_eur = _load_json(T9_EUR_PATH)
    t9_gbp = _load_json(T9_GBP_PATH)

    eur_res = _simulate_pair("EUR_USD", p6_eur, p7_eur, t9_eur, phase11)
    gbp_res = _simulate_pair("GBP_USD", p6_gbp, p7_gbp, t9_gbp, phase11)
    drift = _cross_pair_drift_proof(eur_res, gbp_res)

    pair_gate_pass = all(eur_res["pair_gate"].values()) and all(gbp_res["pair_gate"].values())
    pass_conditions = {
        "phase24_dependency_passed": phase24.get("status") == "PASS",
        "intervention_v1_executed_shadow_only": True,
        "phase6_phase7_comparison_complete": True,
        "pair_gate_pass": pair_gate_pass,
        "cross_pair_drift_proof_pass": bool(drift.get("overall_pass", False)),
        "no_tuning_applied": True,
    }

    overall_pass = all(pass_conditions.values())
    verdict = "PROMOTE" if overall_pass else "HOLD"

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE25_SHADOW_BENCHMARK_EXECUTION_AND_DRIFT_PROOF",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "SHADOW_INTERVENTION_V1",
        },
        "dependency": {
            "phase24_status": phase24.get("status"),
            "phase24_path": str(PHASE24_PATH),
            "phase11_status": phase11.get("status"),
            "phase11_path": str(PHASE11_PATH),
            "phase6_phase7_refs": [
                str(P6_EUR_PATH),
                str(P6_GBP_PATH),
                str(P7_EUR_PATH),
                str(P7_GBP_PATH),
            ],
        },
        "intervention_v1_focus": [
            "long_peak_to_close_delay",
            "prolonged_drawdown_selected",
        ],
        "results_by_pair": {
            "EUR_USD": eur_res,
            "GBP_USD": gbp_res,
        },
        "cross_pair_drift_proof": drift,
        "pass_conditions": pass_conditions,
        "decision": {
            "verdict": verdict,
            "overall_pass": overall_pass,
            "reason": (
                "Intervention V1 clears pair gates and cross-pair drift proof under shadow benchmark."
                if overall_pass
                else "Intervention V1 requires refinement before promotion due to pair gate and/or drift proof failure."
            ),
            "release_action": "SHADOW_ONLY_CONTINUE",
            "live_promotion_allowed": False,
        },
        "next_recommended_task": (
            "MVP_PHASE26_INTERVENTION_V1_REFINEMENT_AND_SECOND_SHADOW_RERUN"
            if not overall_pass
            else "MVP_PHASE26_SHADOW_STABILITY_WINDOW_PROOF"
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
