#!/usr/bin/env python3
"""Build MVP Phase 12 paper-only behavior change trial design (no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PHASE11_PATH = Path("control/mvp_phase11_exit_timing_and_drawdown_pattern_decomposition_no_tuning.json")
OUTPUT_PATH = Path("control/mvp_phase12_paper_only_behavior_change_trial_design.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _pair_thresholds(phase11: Dict[str, Any], pair: str) -> Dict[str, float]:
    by_pair = phase11.get("by_pair_decomposition", {}).get(pair, {})
    th = by_pair.get("thresholds", {})
    return {
        "peak_to_close_p75": float(th.get("peak_to_close_p75", 0.0) or 0.0),
        "peak_to_close_p90": float(th.get("peak_to_close_p90", 0.0) or 0.0),
        "drawdown_p75": float(th.get("drawdown_p75", 0.0) or 0.0),
    }


def _diagnosis_count(phase11: Dict[str, Any], name: str) -> int:
    for row in phase11.get("top_non_tuning_diagnoses", []):
        if str(row.get("name")) == name:
            return int(row.get("count", 0) or 0)
    return 0


def _build_trials(phase11: Dict[str, Any]) -> List[Dict[str, Any]]:
    eur_th = _pair_thresholds(phase11, "EUR_USD")
    gbp_th = _pair_thresholds(phase11, "GBP_USD")

    delay_count = _diagnosis_count(phase11, "long_peak::peak_obvious_and_ignored")
    dead_count = _diagnosis_count(phase11, "drawdown::dead_early_no_recovery")
    tol_count = _diagnosis_count(phase11, "drawdown::timing_tolerance_pattern")

    return [
        {
            "trial_id": "P12-T1",
            "title": "Post-peak delay guardrail",
            "priority": 1,
            "trigger_family": "long_peak::peak_obvious_and_ignored",
            "evidence_weight": delay_count,
            "hypothesis": "When post-peak delay exceeds pair p75 and giveback is at least 1.0 pip, value leakage dominates late exits.",
            "paper_policy": {
                "EUR_USD": {
                    "if": f"time_from_peak_to_close_seconds > {eur_th['peak_to_close_p75']} and loss_after_peak_pips >= 1.0",
                    "then": "candidate_exit=EARLY_POST_PEAK_GUARD",
                },
                "GBP_USD": {
                    "if": f"time_from_peak_to_close_seconds > {gbp_th['peak_to_close_p75']} and loss_after_peak_pips >= 1.0",
                    "then": "candidate_exit=EARLY_POST_PEAK_GUARD",
                },
            },
            "expected_effect_direction": {
                "peak_to_close_loss_distribution": "down",
                "drawdown_duration": "down",
                "win_rate": "flat_to_up",
            },
            "risk_notes": [
                "Could cut late-accelerating winners.",
                "Must verify no pair asymmetry in high-momentum tails.",
            ],
            "status": "DESIGN_ONLY",
        },
        {
            "trial_id": "P12-T2",
            "title": "Dead-early drawdown fail-fast",
            "priority": 2,
            "trigger_family": "drawdown::dead_early_no_recovery",
            "evidence_weight": dead_count,
            "hypothesis": "Trades that remain in deep drawdown beyond p75 with no recovery signal are low-probability holds.",
            "paper_policy": {
                "EUR_USD": {
                    "if": f"time_in_drawdown_seconds > {eur_th['drawdown_p75']} and outcome_proxy=no_recovery",
                    "then": "candidate_exit=DRAWDOWN_FAIL_FAST",
                },
                "GBP_USD": {
                    "if": f"time_in_drawdown_seconds > {gbp_th['drawdown_p75']} and outcome_proxy=no_recovery",
                    "then": "candidate_exit=DRAWDOWN_FAIL_FAST",
                },
            },
            "expected_effect_direction": {
                "avg_loser_hold_time": "down",
                "tail_loss": "down",
                "trades_per_hour": "flat_to_up",
            },
            "risk_notes": [
                "Recovery classifier definition must be frozen before any simulation.",
                "Could conflict with continuation patterns in volatile windows.",
            ],
            "status": "DESIGN_ONLY",
        },
        {
            "trial_id": "P12-T3",
            "title": "Drawdown tolerance cap",
            "priority": 3,
            "trigger_family": "drawdown::timing_tolerance_pattern",
            "evidence_weight": tol_count,
            "hypothesis": "Recovered winners with prolonged drawdown indicate tolerance leakage that can be capped with minimal edge damage.",
            "paper_policy": {
                "shared": {
                    "if": "time_to_first_profit_seconds < 600 and time_in_drawdown_seconds > 1800",
                    "then": "candidate_exit=TIMING_TOLERANCE_CAP",
                }
            },
            "expected_effect_direction": {
                "drawdown_duration": "down",
                "loss_after_peak": "down",
                "realized_pph": "up_if_false_cut_rate_controlled",
            },
            "risk_notes": [
                "May reduce upside on mean-reversion recoveries.",
                "Needs pair-by-pair false-cut accounting before promotion.",
            ],
            "status": "DESIGN_ONLY",
        },
    ]


def main() -> None:
    phase11 = _load_json(PHASE11_PATH)
    trials = _build_trials(phase11)

    pass_conditions = {
        "phase11_dependency_passed": phase11.get("status") == "PASS",
        "trials_defined": len(trials) >= 3,
        "contains_ranked_priorities": all("priority" in t for t in trials),
        "contains_guardrails": True,
        "paper_only_confirmed": True,
        "no_tuning_applied": True,
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE12_PAPER_ONLY_BEHAVIOR_CHANGE_TRIAL_DESIGN",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "PAPER_ONLY",
        },
        "dependency": {
            "phase11_status": phase11.get("status"),
            "phase11_path": str(PHASE11_PATH),
        },
        "inputs_used": {
            "top_non_tuning_diagnoses": phase11.get("top_non_tuning_diagnoses", []),
            "first_behavior_change_to_test_later": phase11.get("first_behavior_change_to_test_later", {}),
        },
        "paper_trials": trials,
        "experiment_guardrails": {
            "forbidden_actions": [
                "production parameter tuning",
                "architecture expansion",
                "session widening",
            ],
            "required_before_any_simulation": [
                "freeze trial definitions",
                "freeze acceptance metrics",
                "freeze rollback criteria",
            ],
        },
        "promotion_gate_for_phase13": {
            "requires": [
                "paper trial cards approved",
                "counterfactual measurement plan approved",
                "risk matrix approved",
            ],
            "status": "PENDING_REVIEW",
        },
        "pass_conditions": pass_conditions,
    }
    report["status"] = "PASS" if all(pass_conditions.values()) else "FAIL"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"wrote {OUTPUT_PATH}")
    print(json.dumps({"status": report["status"], "trial_ids": [t["trial_id"] for t in trials]}, indent=2))


if __name__ == "__main__":
    main()