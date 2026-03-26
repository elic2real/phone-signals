#!/usr/bin/env python3
"""Build MVP Phase 13 counterfactual simulation plan (no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PHASE12_PATH = Path("control/mvp_phase12_paper_only_behavior_change_trial_design.json")
PHASE11_PATH = Path("control/mvp_phase11_exit_timing_and_drawdown_pattern_decomposition_no_tuning.json")
OUTPUT_PATH = Path("control/mvp_phase13_counterfactual_simulation_plan_no_tuning.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _pair_baseline_snapshot(phase11: Dict[str, Any], pair: str) -> Dict[str, Any]:
    decomp = phase11.get("by_pair_decomposition", {}).get(pair, {})
    return {
        "sample_count": int(decomp.get("sample_count", 0) or 0),
        "peak_to_close_loss_distribution": decomp.get("peak_to_close_loss_distribution", {}),
        "drawdown_duration_buckets": decomp.get("drawdown_duration_buckets", {}),
        "thresholds": decomp.get("thresholds", {}),
    }


def _trial_cells(trial: Dict[str, Any]) -> List[Dict[str, Any]]:
    trial_id = str(trial.get("trial_id", ""))
    priority = int(trial.get("priority", 0) or 0)
    policy = trial.get("paper_policy", {})

    cells: List[Dict[str, Any]] = []
    for pair in ["EUR_USD", "GBP_USD"]:
        if pair in policy:
            rule = policy[pair]
        else:
            rule = policy.get("shared", {})
        cells.append(
            {
                "cell_id": f"{trial_id}::{pair}",
                "trial_id": trial_id,
                "pair": pair,
                "priority": priority,
                "counterfactual_rule": rule,
                "evaluation_mode": "OFFLINE_LABEL_ONLY",
                "expected_effect_direction": trial.get("expected_effect_direction", {}),
            }
        )
    return cells


def main() -> None:
    phase12 = _load_json(PHASE12_PATH)
    phase11 = _load_json(PHASE11_PATH)

    trials = phase12.get("paper_trials", [])
    ranked_trials = sorted(trials, key=lambda t: int(t.get("priority", 999) or 999))

    simulation_matrix: List[Dict[str, Any]] = []
    for trial in ranked_trials:
        simulation_matrix.extend(_trial_cells(trial))

    acceptance_protocol = {
        "lock_first_requirements": {
            "scope_lock": {
                "pairs": ["EUR_USD", "GBP_USD"],
                "session": "LONDON",
                "tuning": "NONE",
                "mode": "COUNTERFACTUAL_SIMULATION_ONLY",
            },
            "frozen_inputs": [
                str(PHASE12_PATH),
                str(PHASE11_PATH),
                "entry_v23_policy_guarded_active.json",
            ],
            "forbidden": [
                "policy parameter edits",
                "live runtime behavior changes",
                "architecture expansion",
            ],
        },
        "acceptance_metrics": {
            "primary": [
                "peak_to_close_loss_distribution_delta",
                "drawdown_duration_bucket_shift",
                "false_cut_rate_on_winners",
            ],
            "secondary": [
                "estimated_realized_pph_direction",
                "trade_life_seconds_change",
            ],
            "must_report_by_pair": True,
        },
        "promotion_thresholds": {
            "requires_positive_primary_metrics": 2,
            "max_false_cut_rate_on_winners": 0.15,
            "requires_no_scope_regression": True,
        },
        "rollback_criteria": {
            "false_cut_rate_on_winners_gt": 0.15,
            "pair_divergence_alert": True,
            "unexpected_tail_loss_increase": True,
        },
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE13_COUNTERFACTUAL_SIMULATION_PLAN_NO_TUNING",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "COUNTERFACTUAL_SIMULATION_ONLY",
        },
        "dependency": {
            "phase12_status": phase12.get("status"),
            "phase12_path": str(PHASE12_PATH),
            "phase11_status": phase11.get("status"),
            "phase11_path": str(PHASE11_PATH),
        },
        "baseline_snapshot": {
            "EUR_USD": _pair_baseline_snapshot(phase11, "EUR_USD"),
            "GBP_USD": _pair_baseline_snapshot(phase11, "GBP_USD"),
        },
        "trial_priority_order": [str(t.get("trial_id", "")) for t in ranked_trials],
        "simulation_matrix": simulation_matrix,
        "execution_stages": [
            {
                "stage": 1,
                "name": "single_trial_isolation",
                "description": "Run each trial independently per pair with no interaction effects.",
            },
            {
                "stage": 2,
                "name": "ordered_additive_stack",
                "description": "Apply trials in priority order to measure additive impact and interference.",
            },
            {
                "stage": 3,
                "name": "stress_slices",
                "description": "Re-evaluate on high drawdown and long-peak slices for brittleness checks.",
            },
        ],
        "acceptance_protocol": acceptance_protocol,
        "pass_conditions": {
            "phase12_dependency_passed": phase12.get("status") == "PASS",
            "phase11_dependency_passed": phase11.get("status") == "PASS",
            "simulation_matrix_present": len(simulation_matrix) > 0,
            "acceptance_protocol_present": True,
            "rollback_criteria_present": True,
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
                "trial_priority_order": report["trial_priority_order"],
                "matrix_cells": len(simulation_matrix),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()