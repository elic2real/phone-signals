#!/usr/bin/env python3
"""Execute MVP Phase 37: multi-window combined stability (deterministic, no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

STAGE2_PATH = Path("control/mvp_phase34_stage2_lane_results.json")
PHASE36_PATH = Path("control/mvp_phase36_multi_session_deep_validation.json")
STAGE3_PATH = Path("control/mvp_phase34_stage3_combined_stability.json")
OUTPUT_PATH = Path("control/mvp_phase37_multi_window_combined_stability.json")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _build_windows(adverse_throughput_mult: float, adverse_winrate_shift: float) -> List[Dict[str, Any]]:
    # Deterministic stress windows that test quality/throughput resilience.
    return [
        {
            "window_id": "W1_BASELINE",
            "name": "baseline_current",
            "throughput_mult": 1.00,
            "winrate_shift": 0.000,
            "expected_mode": "BASELINE",
        },
        {
            "window_id": "W2_ADVERSE",
            "name": "higher_noise_higher_spread",
            "throughput_mult": adverse_throughput_mult,
            "winrate_shift": adverse_winrate_shift,
            "expected_mode": "STRESS_DOWN",
        },
        {
            "window_id": "W3_FAVORABLE",
            "name": "cleaner_flow",
            "throughput_mult": 1.06,
            "winrate_shift": 0.007,
            "expected_mode": "STRESS_UP",
        },
    ]


def _build_profiles() -> List[Dict[str, Any]]:
    # Parallel scenario matrix: evaluate multiple resilience outcomes in a single run.
    return [
        {
            "profile_id": "P1_CURRENT",
            "name": "current_assumption",
            "adverse_throughput_mult": 0.90,
            "adverse_winrate_shift": -0.012,
        },
        {
            "profile_id": "P2_MILD_ADVERSE",
            "name": "mild_adverse",
            "adverse_throughput_mult": 0.93,
            "adverse_winrate_shift": -0.009,
        },
        {
            "profile_id": "P3_BALANCED_ADVERSE",
            "name": "balanced_adverse",
            "adverse_throughput_mult": 0.92,
            "adverse_winrate_shift": -0.010,
        },
        {
            "profile_id": "P4_HARD_ADVERSE",
            "name": "hard_adverse",
            "adverse_throughput_mult": 0.88,
            "adverse_winrate_shift": -0.015,
        },
    ]


def main() -> None:
    stage2 = _load_json(STAGE2_PATH)
    phase36 = _load_json(PHASE36_PATH)

    if phase36.get("status") != "PASS":
        raise RuntimeError("Phase36 must be PASS before running Phase37")

    rows = list(stage2.get("rows", []))
    if not rows:
        raise RuntimeError("Stage2 rows missing; cannot run Phase37")

    base_tph = sum(_f(r.get("trades_per_hour_est"), 0.0) for r in rows)
    base_eq_hr = sum(_f(r.get("expected_equity_pct_per_hour_at_2pct_risk"), 0.0) for r in rows)
    base_wr = 0.0
    if base_tph > 0:
        base_wr = sum(_f(r.get("win_rate_est"), 0.0) * _f(r.get("trades_per_hour_est"), 0.0) for r in rows) / base_tph

    profiles = _build_profiles()
    profile_results: List[Dict[str, Any]] = []

    for p in profiles:
        windows = _build_windows(
            adverse_throughput_mult=_f(p.get("adverse_throughput_mult"), 0.90),
            adverse_winrate_shift=_f(p.get("adverse_winrate_shift"), -0.012),
        )
        window_results: List[Dict[str, Any]] = []

        for w in windows:
            tph = base_tph * _f(w.get("throughput_mult"), 1.0)
            wr = _clamp(base_wr + _f(w.get("winrate_shift"), 0.0), 0.52, 0.64)
            exp_r_hr = tph * ((2.0 * wr) - 1.0)
            eq_hr = 2.0 * exp_r_hr

            pass_gate = (
                wr >= 0.555
                and eq_hr > 0.0
                and (eq_hr >= 0.90 * base_eq_hr)
            )

            window_results.append(
                {
                    "window_id": w.get("window_id"),
                    "name": w.get("name"),
                    "mode": w.get("expected_mode"),
                    "throughput_mult": round(_f(w.get("throughput_mult"), 1.0), 6),
                    "winrate_shift": round(_f(w.get("winrate_shift"), 0.0), 6),
                    "total_trades_per_hour_est": round(tph, 6),
                    "weighted_win_rate_est": round(wr, 6),
                    "expected_equity_pct_per_hour_at_2pct_risk": round(eq_hr, 6),
                    "pass_gate": bool(pass_gate),
                    "fail_reason_if_any": "window_quality_or_resilience_fail" if not pass_gate else "",
                }
            )

        profile_pass = all(bool(w.get("pass_gate")) for w in window_results)
        profile_results.append(
            {
                "profile_id": p.get("profile_id"),
                "name": p.get("name"),
                "adverse_throughput_mult": round(_f(p.get("adverse_throughput_mult"), 0.90), 6),
                "adverse_winrate_shift": round(_f(p.get("adverse_winrate_shift"), -0.012), 6),
                "status": "PASS" if profile_pass else "HOLD",
                "windows": window_results,
                "windows_pass": sum(1 for w in window_results if w.get("pass_gate")),
                "windows_total": len(window_results),
            }
        )

    current_profile = next((p for p in profile_results if p.get("profile_id") == "P1_CURRENT"), profile_results[0])
    current_pass = current_profile.get("status") == "PASS"
    pass_profiles = [p for p in profile_results if p.get("status") == "PASS"]

    stage3_payload = {
        "task_id": "MVP_PHASE37_MULTI_WINDOW_COMBINED_STABILITY",
        "generated_at": _iso_now(),
        "status": "PASS" if current_pass else "HOLD",
        "entry_condition": "stage_2_deep_validation pass",
        "summary": {
            "profiles_total": len(profile_results),
            "profiles_pass": len(pass_profiles),
            "current_profile": current_profile.get("profile_id"),
            "current_profile_status": current_profile.get("status"),
            "baseline_expected_equity_pct_per_hour_at_2pct_risk": round(base_eq_hr, 6),
        },
        "profiles": profile_results,
        "next_recommended_task": (
            "MVP_PHASE38_LIVE_GUARDED_ROLLOUT"
            if current_pass
            else ("MVP_PHASE37C_SELECT_PASSING_PROFILE" if pass_profiles else "MVP_PHASE37B_STABILITY_REMEDIATION")
        ),
    }

    final_payload = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": _iso_now(),
        "task_id": "MVP_PHASE37_MULTI_WINDOW_COMBINED_STABILITY",
        "dependency": {
            "phase36_status": phase36.get("status"),
            "stage2_status": stage2.get("status"),
        },
        "baseline": {
            "total_trades_per_hour_est": round(base_tph, 6),
            "weighted_win_rate_est": round(base_wr, 6),
            "expected_equity_pct_per_hour_at_2pct_risk": round(base_eq_hr, 6),
        },
        "profiles": profile_results,
        "status": "PASS" if current_pass else "HOLD",
        "decision": {
            "verdict": "PROMOTE" if current_pass else "HOLD",
            "release_action": (
                "PROMOTE_TO_PHASE38_LIVE_GUARDED_ROLLOUT"
                if current_pass
                else ("SELECT_PASSING_PROFILE_AND_REVALIDATE" if pass_profiles else "HOLD_AND_REMEDIATE_STABILITY")
            ),
            "reason": (
                "Current profile passed multi-window stability gates."
                if current_pass
                else (
                    "Current profile failed, but at least one parallel profile passed."
                    if pass_profiles
                    else "No parallel profile passed stability gates."
                )
            ),
        },
    }

    STAGE3_PATH.write_text(json.dumps(stage3_payload, indent=2) + "\n", encoding="utf-8")
    OUTPUT_PATH.write_text(json.dumps(final_payload, indent=2) + "\n", encoding="utf-8")

    print(f"wrote {OUTPUT_PATH}")
    print(
        json.dumps(
            {
                "status": final_payload["status"],
                "profiles_total": len(profile_results),
                "profiles_pass": len(pass_profiles),
                "current_profile": current_profile.get("profile_id"),
                "current_profile_status": current_profile.get("status"),
                "next": stage3_payload["next_recommended_task"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
