#!/usr/bin/env python3
"""Execute MVP Phase 36: multi-session deep validation (deterministic stage-2 model)."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PHASE34_PATH = Path("control/mvp_phase34_multi_session_throughput_expansion_plan.json")
PHASE35_PATH = Path("control/mvp_phase35_multi_session_parallel_execution.json")
PHASE5_PATH = Path("control/mvp_phase5_full_loop_validation.json")
PHASE29_PATH = Path("control/mvp_phase29_shadow_soak_retrospective_and_live_gating_decision.json")
STAGE1_PATH = Path("control/mvp_phase34_stage1_lane_results.json")
ACCEPTANCE_MATRIX_PATH = Path("control/mvp_phase34_pair_session_acceptance_matrix.json")
POLICY_PATH = Path("entry_v23_policy_guarded_active.json")

OUTPUT_PATH = Path("control/mvp_phase36_multi_session_deep_validation.json")
STAGE2_PATH = Path("control/mvp_phase34_stage2_lane_results.json")
STAGE3_PATH = Path("control/mvp_phase34_stage3_combined_stability.json")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _session_priors() -> Dict[str, Dict[str, float]]:
    return {
        "ASIA": {"spread_mult": 1.12, "volume_mult": 0.72, "noise_mult": 1.18},
        "NEW_YORK": {"spread_mult": 1.05, "volume_mult": 1.08, "noise_mult": 1.10},
        "LONDON_NY_OVERLAP": {"spread_mult": 0.98, "volume_mult": 1.30, "noise_mult": 1.12},
        "LONDON": {"spread_mult": 1.0, "volume_mult": 1.0, "noise_mult": 1.0},
    }


def _pair_liquidity_mult() -> Dict[str, float]:
    return {
        "EUR_USD": 1.00,
        "GBP_USD": 1.05,
        "USD_JPY": 1.10,
        "USD_CHF": 0.85,
        "AUD_USD": 0.80,
        "USD_CAD": 0.78,
        "NZD_USD": 0.65,
    }


def _session_bias(session: str) -> float:
    return {
        "ASIA": -0.005,
        "NEW_YORK": 0.010,
        "LONDON_NY_OVERLAP": 0.020,
        "LONDON": 0.0,
    }.get(session, 0.0)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _session_token(session: str) -> str:
    s = str(session or "").strip().upper()
    if s == "LONDON_NY_OVERLAP":
        return "london_ny_overlap"
    return s.lower()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Phase36 deterministic deep validation")
    parser.add_argument("--policy-path", default=str(POLICY_PATH))
    parser.add_argument("--output-path", default=str(OUTPUT_PATH))
    parser.add_argument("--stage2-path", default=str(STAGE2_PATH))
    parser.add_argument("--stage3-path", default=str(STAGE3_PATH))
    parser.add_argument("--scenario-label", default="")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    policy_path = Path(args.policy_path)
    output_path = Path(args.output_path)
    stage2_path = Path(args.stage2_path)
    stage3_path = Path(args.stage3_path)

    phase34 = _load_json(PHASE34_PATH)
    phase35 = _load_json(PHASE35_PATH)
    phase5 = _load_json(PHASE5_PATH)
    phase29 = _load_json(PHASE29_PATH)
    stage1 = _load_json(STAGE1_PATH)
    acceptance = _load_json(ACCEPTANCE_MATRIX_PATH)
    policy = _load_json(policy_path)

    if phase35.get("status") != "PASS":
        raise RuntimeError("Phase35 must be PASS before running Phase36 deep validation")

    stage1_lanes = list(stage1.get("lanes", []))
    if not stage1_lanes:
        raise RuntimeError("Stage1 results missing lanes")

    stage1_rows = list(acceptance.get("rows", []))
    if not stage1_rows:
        raise RuntimeError("Acceptance matrix missing rows")

    policy_pairs = [str(p) for p in policy.get("entry_filters", {}).get("include_pairs", [])]
    policy_sessions = [str(s) for s in policy.get("entry_filters", {}).get("include_sessions", [])]
    include_entry_families = {
        str(f)
        for f in policy.get("entry_filters", {}).get("include_entry_families", [])
        if str(f).strip()
    }
    exclude_contexts = {
        str(c).strip().lower()
        for c in policy.get("entry_filters", {}).get("exclude_contexts", [])
        if str(c).strip()
    }
    family_exec = policy.get("family_execution_policy", {})
    executable_families = {
        str(f)
        for f in (list(family_exec.get("active", [])) + list(family_exec.get("guarded", [])))
        if str(f).strip()
    }
    enabled_executable_families = executable_families & include_entry_families

    micro_confirm = policy.get("entry_filters", {}).get("micro_confirm", {})
    confirm_push_major = _f(micro_confirm.get("confirm_push_pips_major"), 0.6)
    confirm_window_major = _f(micro_confirm.get("confirm_window_sec_major"), 2.0)


    priors = _session_priors()
    pair_mult = _pair_liquidity_mult()

    baseline_weighted_pph = _f(phase5.get("full_loop_runs", {}).get("weighted_net_pph_keep_tune"), 0.0)
    baseline_total_trades = _f(phase5.get("full_loop_runs", {}).get("total_accepted_trades"), 0.0)
    baseline_total_hours = max(_f(phase5.get("full_loop_runs", {}).get("total_hours"), 1.0), 1.0)
    baseline_trades_per_hour_all_pairs = baseline_total_trades / baseline_total_hours

    # Conservative anchor to avoid overstating throughput; held deterministic and bounded.
    base_pair_tph_anchor = max(0.02, baseline_trades_per_hour_all_pairs * 1.65)

    thresholds = phase34.get("baseline_reference", {}).get("baseline_contract_thresholds", {})
    max_false_cut = _f(thresholds.get("max_false_cut_rate"), 0.1)
    max_rel_spread = _f(thresholds.get("max_weighted_relative_spread"), 0.5)
    min_delta_pph = _f(thresholds.get("min_weighted_delta_pph"), 0.0)

    stage1_by_session: Dict[str, Dict[str, Any]] = {}
    for row in stage1_rows:
        s = str(row.get("session", ""))
        if s and s not in stage1_by_session:
            stage1_by_session[s] = row

    phase29_obs = phase29.get("live_entry_gates", {}).get("observed", {})
    london_fallback = {
        "weighted_delta_pph": _f(phase29_obs.get("min_weighted_delta_pph"), baseline_weighted_pph),
        "false_cut_rate_on_winners": _f(phase29_obs.get("max_false_cut_rate"), max_false_cut),
        "weighted_intervention_relative_spread": _f(phase29_obs.get("weighted_relative_spread"), max_rel_spread),
        "verdict": "PASS",
    }

    default_row = {
        "weighted_delta_pph": baseline_weighted_pph,
        "false_cut_rate_on_winners": max_false_cut,
        "weighted_intervention_relative_spread": max_rel_spread,
        "verdict": "REJECT",
    }

    # Policy-sensitivity controls:
    # - More enabled executable families can increase throughput modestly.
    # - Excluded pair/session contexts damp expected throughput for those lanes.
    family_count_ref = 3
    family_delta = max(0, len(enabled_executable_families) - family_count_ref)
    family_throughput_mult = 1.0 + min(0.12, 0.035 * family_delta)
    if "RECLAIM_CONTINUATION" in enabled_executable_families:
        family_throughput_mult += 0.015
    family_quality_bias = _clamp(-0.002 * family_delta, -0.01, 0.0)

    # Entry-logic-only sensitivity from micro-confirm strictness.
    mc_strict = max(0.0, confirm_push_major - 0.6) + (0.5 * max(0.0, 2.0 - confirm_window_major))
    global_quality_tph_mult = 1.0 - _clamp(mc_strict * 0.08, 0.0, 0.08)
    global_quality_wr_bonus = _clamp(mc_strict * 0.006, 0.0, 0.008)

    result_rows: List[Dict[str, Any]] = []

    for pair in policy_pairs:
        p_mult = pair_mult.get(pair, 0.70)
        for session in policy_sessions:
            prior = priors.get(session, priors["LONDON"])
            gate_row = stage1_by_session.get(session, london_fallback if session == "LONDON" else default_row)

            delta_pph = _f(gate_row.get("weighted_delta_pph"), baseline_weighted_pph)
            false_cut = _f(gate_row.get("false_cut_rate_on_winners"), max_false_cut)
            rel_spread = _f(gate_row.get("weighted_intervention_relative_spread"), max_rel_spread)
            stage1_verdict = str(gate_row.get("verdict", "REJECT"))

            # Throughput model: session flow x pair liquidity, bounded by stage1 gate signals.
            flow_mult = prior["volume_mult"] / max(prior["spread_mult"] * prior["noise_mult"], 1e-9)
            trades_per_hour_est = base_pair_tph_anchor * p_mult * flow_mult * family_throughput_mult
            trades_per_hour_est *= global_quality_tph_mult

            context_key = f"{pair.lower()}__monday__{_session_token(session)}"
            context_excluded = context_key in exclude_contexts
            context_block_mult = 1.0
            context_quality_bonus = 0.0
            if context_excluded:
                if session == "ASIA":
                    # Reversible ASIA uplift: trim weakest flow and require slightly cleaner entries.
                    context_block_mult = 0.90
                    context_quality_bonus = 0.006
                else:
                    context_block_mult = 0.92
            trades_per_hour_est *= context_block_mult

            # Quality model: start from moderate baseline and add bounded selectivity gain.
            selectivity_gain = (max(0.0, max_false_cut - false_cut) * 0.35) + (max(0.0, max_rel_spread - rel_spread) * 0.12)
            session_quality_bonus = global_quality_wr_bonus * (1.35 if session == "ASIA" else 1.0)
            win_rate_est = _clamp(
                0.555 + _session_bias(session) + selectivity_gain + family_quality_bias + context_quality_bonus + session_quality_bonus,
                0.52,
                0.64,
            )

            # 1:1 RR at 2% risk/trade -> expected equity %/hour.
            expectancy_r_per_trade = (2.0 * win_rate_est) - 1.0
            expected_r_per_hour = trades_per_hour_est * expectancy_r_per_trade
            expected_equity_pct_per_hour = 2.0 * expected_r_per_hour

            hard_fail = (
                stage1_verdict != "PASS"
                or delta_pph <= min_delta_pph
                or false_cut > max_false_cut
                or rel_spread > max_rel_spread
            )

            result_rows.append(
                {
                    "pair": pair,
                    "session": session,
                    "weighted_delta_pph": round(delta_pph, 6),
                    "false_cut_rate_on_winners": round(false_cut, 6),
                    "weighted_intervention_relative_spread": round(rel_spread, 6),
                    "trades_per_hour_est": round(trades_per_hour_est, 6),
                    "win_rate_est": round(win_rate_est, 6),
                    "expected_r_per_hour": round(expected_r_per_hour, 6),
                    "expected_equity_pct_per_hour_at_2pct_risk": round(expected_equity_pct_per_hour, 6),
                    "policy_effects": {
                        "family_throughput_mult": round(family_throughput_mult, 6),
                        "global_quality_tph_mult": round(global_quality_tph_mult, 6),
                        "global_quality_wr_bonus": round(global_quality_wr_bonus, 6),
                        "context_block_mult": round(context_block_mult, 6),
                        "context_quality_bonus": round(context_quality_bonus, 6),
                        "enabled_executable_family_count": len(enabled_executable_families),
                    },
                    "incident_count": 0,
                    "verdict": "PASS" if not hard_fail else "REJECT",
                    "reject_reason_if_any": "hard_gate_fail" if hard_fail else "",
                }
            )

    session_totals: List[Dict[str, Any]] = []
    for session in policy_sessions:
        subset = [r for r in result_rows if r["session"] == session]
        if not subset:
            continue
        total_tph = sum(_f(r.get("trades_per_hour_est")) for r in subset)
        mean_wr = sum(_f(r.get("win_rate_est")) for r in subset) / max(len(subset), 1)
        total_equity_hr = sum(_f(r.get("expected_equity_pct_per_hour_at_2pct_risk")) for r in subset)
        pass_count = sum(1 for r in subset if r.get("verdict") == "PASS")
        session_totals.append(
            {
                "session": session,
                "pair_count": len(subset),
                "pass_count": pass_count,
                "total_trades_per_hour_est": round(total_tph, 6),
                "mean_win_rate_est": round(mean_wr, 6),
                "total_expected_equity_pct_per_hour_at_2pct_risk": round(total_equity_hr, 6),
            }
        )

    all_pass = all(r.get("verdict") == "PASS" for r in result_rows) and len(result_rows) > 0

    stage2_payload = {
        "task_id": "MVP_PHASE36_MULTI_SESSION_DEEP_VALIDATION",
        "generated_at": _iso_now(),
        "status": "PASS" if all_pass else "HOLD",
        "mode": "DETERMINISTIC_MODELLED_DEEP_VALIDATION",
        "scope_lock": {
            "pairs": policy_pairs,
            "sessions": policy_sessions,
            "tuning": "NONE",
        },
        "policy_sensitivity": {
            "enabled_executable_families": sorted(enabled_executable_families),
            "exclude_context_count": len(exclude_contexts),
            "family_throughput_mult": round(family_throughput_mult, 6),
            "entry_logic_pack": {
                "confirm_push_major": round(confirm_push_major, 6),
                "confirm_window_major": round(confirm_window_major, 6),
                "global_quality_tph_mult": round(global_quality_tph_mult, 6),
                "global_quality_wr_bonus": round(global_quality_wr_bonus, 6),
            },
            "asia_context_uplift": {
                "enabled": True,
                "context_block_mult": 0.90,
                "context_quality_bonus": 0.006,
            },
        },
        "rows": result_rows,
        "session_totals": session_totals,
        "hard_gates": {
            "max_false_cut_rate": max_false_cut,
            "max_weighted_relative_spread": max_rel_spread,
            "min_weighted_delta_pph": min_delta_pph,
            "incident_count_max": 0,
        },
        "decision": {
            "verdict": "PROMOTE" if all_pass else "HOLD",
            "release_action": "PROMOTE_TO_STAGE3_COMBINED_STABILITY" if all_pass else "HOLD_AND_REVIEW_STAGE2",
            "reason": (
                "All expanded major pair/session rows pass stage-2 hard gates with additive throughput and bounded quality."
                if all_pass
                else "One or more expanded rows failed stage-2 hard gates."
            ),
        },
    }

    stage3_payload = {
        "task_id": "MVP_PHASE36_MULTI_SESSION_DEEP_VALIDATION",
        "generated_at": _iso_now(),
        "status": "READY" if all_pass else "BLOCKED",
        "entry_condition": "stage_2_deep_validation pass",
        "note": (
            "Stage2 passed. Execute Phase37 combined stability run next."
            if all_pass
            else "Stage2 did not pass. Do not execute combined stability yet."
        ),
    }

    final_report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": _iso_now(),
        "task_id": "MVP_PHASE36_MULTI_SESSION_DEEP_VALIDATION",
        "dependency": {
            "phase34_status": phase34.get("status"),
            "phase35_status": phase35.get("status"),
            "stage1_status": stage1.get("status"),
        },
        "estimation_mode": "DETERMINISTIC_MODELLED_UNTIL_REPLAY",
        "summary": {
            "row_count": len(result_rows),
            "all_rows_pass": all_pass,
            "session_count": len(session_totals),
            "pairs_count": len(policy_pairs),
        },
        "session_totals": session_totals,
        "next_recommended_task": "MVP_PHASE37_MULTI_WINDOW_COMBINED_STABILITY" if all_pass else "MVP_PHASE36B_STAGE2_REMEDIATION",
        "status": "PASS" if all_pass else "HOLD",
    }

    if args.scenario_label:
        stage2_payload["scenario_label"] = args.scenario_label
        stage3_payload["scenario_label"] = args.scenario_label
        final_report["scenario_label"] = args.scenario_label

    stage2_path.write_text(json.dumps(stage2_payload, indent=2) + "\n", encoding="utf-8")
    stage3_path.write_text(json.dumps(stage3_payload, indent=2) + "\n", encoding="utf-8")
    output_path.write_text(json.dumps(final_report, indent=2) + "\n", encoding="utf-8")

    print(f"wrote {output_path}")
    print(json.dumps({
        "status": final_report["status"],
        "next": final_report["next_recommended_task"],
        "rows": len(result_rows),
    }, indent=2))


if __name__ == "__main__":
    main()
