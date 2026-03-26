#!/usr/bin/env python3
"""Run parallel entry-logic scenario matrix and pick best 24h extraction path.

Scope: entry logic only (tight/loose knobs). No sizing changes and no extra throttling layers.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PHASE5_PATH = Path("control/mvp_phase5_full_loop_validation.json")
PHASE34_PATH = Path("control/mvp_phase34_multi_session_throughput_expansion_plan.json")
PHASE29_PATH = Path("control/mvp_phase29_shadow_soak_retrospective_and_live_gating_decision.json")
STAGE1_MATRIX_PATH = Path("control/mvp_phase34_pair_session_acceptance_matrix.json")
POLICY_PATH = Path("entry_v23_policy_guarded_active.json")
OUT_PATH = Path("control/mvp_entry_logic_scenario_matrix.json")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _f(v: Any, d: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(d)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _session_priors() -> Dict[str, Dict[str, float]]:
    return {
        "ASIA": {"spread_mult": 1.12, "volume_mult": 0.72, "noise_mult": 1.18},
        "NEW_YORK": {"spread_mult": 1.05, "volume_mult": 1.08, "noise_mult": 1.10},
        "LONDON_NY_OVERLAP": {"spread_mult": 0.98, "volume_mult": 1.30, "noise_mult": 1.12},
        "LONDON": {"spread_mult": 1.0, "volume_mult": 1.0, "noise_mult": 1.0},
    }


def _pair_mult() -> Dict[str, float]:
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


def _profiles() -> List[Dict[str, Any]]:
    return [
        {
            "profile_id": "E1_LOOSE",
            "label": "loose_entry",
            "confirm_window_sec_major": 2.1,
            "min_progress_ratio": 0.08,
            "min_release_quality": 0.08,
            "max_noise": 0.72,
            "asia_extra_context_penalty": 0.0,
        },
        {
            "profile_id": "E2_BASELINE",
            "label": "baseline_entry",
            "confirm_window_sec_major": 2.0,
            "min_progress_ratio": 0.08,
            "min_release_quality": 0.08,
            "max_noise": 0.72,
            "asia_extra_context_penalty": 0.0,
        },
        {
            "profile_id": "E3_BALANCED",
            "label": "balanced_entry",
            "confirm_window_sec_major": 1.9,
            "min_progress_ratio": 0.09,
            "min_release_quality": 0.09,
            "max_noise": 0.70,
            "asia_extra_context_penalty": 0.02,
        },
        {
            "profile_id": "E4_TIGHT",
            "label": "tight_entry",
            "confirm_window_sec_major": 1.8,
            "min_progress_ratio": 0.10,
            "min_release_quality": 0.10,
            "max_noise": 0.68,
            "asia_extra_context_penalty": 0.04,
        },
        {
            "profile_id": "E5_ASIA_TIGHT",
            "label": "asia_tight_balanced_rest",
            "confirm_window_sec_major": 1.9,
            "min_progress_ratio": 0.09,
            "min_release_quality": 0.09,
            "max_noise": 0.70,
            "asia_extra_context_penalty": 0.08,
        },
    ]


def _daily_ret(eq_hr_pct: float) -> float:
    return ((1.0 + (eq_hr_pct / 100.0)) ** 24 - 1.0) * 100.0


def main() -> None:
    phase5 = _load(PHASE5_PATH)
    phase34 = _load(PHASE34_PATH)
    phase29 = _load(PHASE29_PATH)
    stage1 = _load(STAGE1_MATRIX_PATH)
    policy = _load(POLICY_PATH)

    priors = _session_priors()
    pair_mult = _pair_mult()

    baseline_weighted_pph = _f(phase5.get("full_loop_runs", {}).get("weighted_net_pph_keep_tune"), 0.0)
    baseline_total_trades = _f(phase5.get("full_loop_runs", {}).get("total_accepted_trades"), 0.0)
    baseline_total_hours = max(_f(phase5.get("full_loop_runs", {}).get("total_hours"), 1.0), 1.0)
    baseline_tph_all = baseline_total_trades / baseline_total_hours
    base_pair_tph_anchor = max(0.02, baseline_tph_all * 1.65)

    thresholds = phase34.get("baseline_reference", {}).get("baseline_contract_thresholds", {})
    max_false_cut = _f(thresholds.get("max_false_cut_rate"), 0.1)
    max_rel_spread = _f(thresholds.get("max_weighted_relative_spread"), 0.5)

    phase29_obs = phase29.get("live_entry_gates", {}).get("observed", {})
    london_fallback = {
        "weighted_delta_pph": _f(phase29_obs.get("min_weighted_delta_pph"), baseline_weighted_pph),
        "false_cut_rate_on_winners": _f(phase29_obs.get("max_false_cut_rate"), max_false_cut),
        "weighted_intervention_relative_spread": _f(phase29_obs.get("weighted_relative_spread"), max_rel_spread),
        "verdict": "PASS",
    }

    stage1_rows = list(stage1.get("rows", []))
    by_session: Dict[str, Dict[str, Any]] = {}
    for r in stage1_rows:
        s = str(r.get("session", ""))
        if s and s not in by_session:
            by_session[s] = r

    pairs = [str(p) for p in policy.get("entry_filters", {}).get("include_pairs", [])]
    sessions = [str(s) for s in policy.get("entry_filters", {}).get("include_sessions", [])]
    exclude_contexts = {str(c).strip().lower() for c in policy.get("entry_filters", {}).get("exclude_contexts", []) if str(c).strip()}

    scenario_rows: List[Dict[str, Any]] = []

    for prof in _profiles():
        # Entry-logic strictness score from knob deltas around baseline.
        strictness = (
            (2.0 - _f(prof.get("confirm_window_sec_major"), 2.0)) * 0.9
            + (_f(prof.get("min_progress_ratio"), 0.08) - 0.08) * 9.0
            + (_f(prof.get("min_release_quality"), 0.08) - 0.08) * 8.0
            + (0.72 - _f(prof.get("max_noise"), 0.72)) * 2.0
        )
        strictness = _clamp(strictness, -0.25, 0.6)

        # Natural entry tight/loose behavior (not a separate throttle layer).
        logic_tph_mult = _clamp(1.0 - (0.16 * strictness), 0.90, 1.04)
        logic_wr_bonus = _clamp(0.018 * strictness, -0.006, 0.016)

        eq_rows: List[Dict[str, Any]] = []
        for pair in pairs:
            for session in sessions:
                prior = priors.get(session, priors["LONDON"])
                gate_row = by_session.get(session, london_fallback if session == "LONDON" else london_fallback)

                false_cut = _f(gate_row.get("false_cut_rate_on_winners"), max_false_cut)
                rel_spread = _f(gate_row.get("weighted_intervention_relative_spread"), max_rel_spread)
                flow_mult = prior["volume_mult"] / max(prior["spread_mult"] * prior["noise_mult"], 1e-9)

                context_key = f"{pair.lower()}__monday__{session.lower()}"
                context_penalty = 1.0
                if context_key in exclude_contexts and session == "ASIA":
                    context_penalty -= _f(prof.get("asia_extra_context_penalty"), 0.0)
                context_penalty = _clamp(context_penalty, 0.86, 1.0)

                tph = base_pair_tph_anchor * pair_mult.get(pair, 0.7) * flow_mult * logic_tph_mult * context_penalty
                selectivity_gain = (max(0.0, max_false_cut - false_cut) * 0.35) + (max(0.0, max_rel_spread - rel_spread) * 0.12)
                wr = _clamp(0.555 + _session_bias(session) + selectivity_gain + logic_wr_bonus, 0.52, 0.64)
                eq_hr = 2.0 * tph * ((2.0 * wr) - 1.0)

                eq_rows.append({"session": session, "tph": tph, "wr": wr, "eq_hr": eq_hr})

        full_eq = sum(r["eq_hr"] for r in eq_rows)
        no_overlap_eq = sum(r["eq_hr"] for r in eq_rows if r["session"] != "LONDON_NY_OVERLAP")
        total_tph = sum(r["tph"] for r in eq_rows)
        wr_w = (sum(r["wr"] * r["tph"] for r in eq_rows) / total_tph) if total_tph > 0 else 0.0

        scenario_rows.append(
            {
                "profile_id": prof["profile_id"],
                "label": prof["label"],
                "entry_logic": {
                    "confirm_window_sec_major": prof["confirm_window_sec_major"],
                    "min_progress_ratio": prof["min_progress_ratio"],
                    "min_release_quality": prof["min_release_quality"],
                    "max_noise": prof["max_noise"],
                    "asia_extra_context_penalty": prof["asia_extra_context_penalty"],
                },
                "derived": {
                    "strictness": round(strictness, 6),
                    "logic_tph_mult": round(logic_tph_mult, 6),
                    "logic_wr_bonus": round(logic_wr_bonus, 6),
                },
                "metrics": {
                    "total_tph": round(total_tph, 6),
                    "weighted_win_rate": round(wr_w, 6),
                    "eq_hr_full_additive_pct": round(full_eq, 6),
                    "eq_hr_no_overlap_pct": round(no_overlap_eq, 6),
                    "ret24_full_additive_pct": round(_daily_ret(full_eq), 6),
                    "ret24_no_overlap_pct": round(_daily_ret(no_overlap_eq), 6),
                },
            }
        )

    ranked = sorted(scenario_rows, key=lambda r: (r["metrics"]["ret24_no_overlap_pct"], r["metrics"]["ret24_full_additive_pct"]), reverse=True)
    best = ranked[0]

    payload = {
        "task_id": "MVP_ENTRY_LOGIC_SCENARIO_MATRIX",
        "generated_at": _iso_now(),
        "scope": "entry_logic_only_no_sizing_no_extra_throttle_layer",
        "ranking_basis": "ret24_no_overlap_pct_then_ret24_full_additive_pct",
        "best_profile": best,
        "scenarios": ranked,
    }
    OUT_PATH.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    print(json.dumps({
        "artifact": str(OUT_PATH),
        "best_profile_id": best["profile_id"],
        "ret24_full_additive_pct": best["metrics"]["ret24_full_additive_pct"],
        "ret24_no_overlap_pct": best["metrics"]["ret24_no_overlap_pct"],
    }, indent=2))


if __name__ == "__main__":
    main()
