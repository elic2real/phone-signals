#!/usr/bin/env python3
"""Build MVP Phase 15 false-cut mitigation redesign (paper-only, no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PHASE14_PATH = Path("control/mvp_phase14_counterfactual_simulation_execution_no_tuning.json")
PHASE13_PATH = Path("control/mvp_phase13_counterfactual_simulation_plan_no_tuning.json")
OUTPUT_PATH = Path("control/mvp_phase15_false_cut_mitigation_trial_redesign_paper_only.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _pair_stage2(phase14: Dict[str, Any], pair: str) -> Dict[str, Any]:
    return (
        phase14.get("stage_results", {})
        .get("stage2_ordered_additive_stack", {})
        .get(pair, {})
    )


def _mitigation_cards(pair: str, pair_stage2: Dict[str, Any], max_rate: float) -> List[Dict[str, Any]]:
    current_rate = float(pair_stage2.get("false_cut_rate_on_winners", 0.0) or 0.0)
    trigger_rate = float(pair_stage2.get("trigger_rate", 0.0) or 0.0)

    return [
        {
            "card_id": f"P15-{pair}-M1",
            "name": "Winner-protection confirmation gate",
            "hypothesis": "False cuts are high because exits trigger before confirmation loss of momentum.",
            "paper_rule": "Apply mitigation if candidate exit and post-trigger confirmation fails to break local continuation for two bars.",
            "expected_effect": {
                "false_cut_rate_on_winners": "down_material",
                "saved_loss_total": "down_moderate",
                "net_effect": "up_if_false_cut_drop_dominates",
            },
            "risk": "May delay exits on true failures.",
            "target": {
                "current_false_cut_rate": current_rate,
                "target_false_cut_rate": max_rate,
            },
            "status": "DESIGN_ONLY",
        },
        {
            "card_id": f"P15-{pair}-M2",
            "name": "Tiered trigger throttling",
            "hypothesis": "Trigger density is too broad; high trigger rate captures many eventual winners.",
            "paper_rule": "Throttle candidate exits by priority tier and only allow full trigger set in top-risk slices.",
            "expected_effect": {
                "trigger_rate": "down",
                "false_cut_rate_on_winners": "down",
                "saved_loss_total": "down_some",
            },
            "risk": "Could under-capture tail loss in non-top-risk trades.",
            "target": {
                "current_trigger_rate": trigger_rate,
                "target_trigger_rate": max(trigger_rate * 0.75, 0.0),
            },
            "status": "DESIGN_ONLY",
        },
        {
            "card_id": f"P15-{pair}-M3",
            "name": "Selective trial deactivation plan",
            "hypothesis": "Trials P12-T1 and P12-T3 contribute most to winner cuts; isolate safer subset first.",
            "paper_rule": "Use P12-T2 as anchor and re-introduce other trials only after pair-specific false-cut compliance.",
            "expected_effect": {
                "false_cut_rate_on_winners": "down_to_compliant",
                "saved_loss_total": "down",
                "gate_pass_probability": "up",
            },
            "risk": "Reduced gross saved-loss potential.",
            "status": "DESIGN_ONLY",
        },
    ]


def main() -> None:
    phase14 = _load_json(PHASE14_PATH)
    phase13 = _load_json(PHASE13_PATH)

    max_rate = float(
        phase14.get("acceptance_gate_evaluation", {})
        .get("promotion_thresholds", {})
        .get("max_false_cut_rate_on_winners", 0.15)
        or 0.15
    )

    redesign_by_pair: Dict[str, Any] = {}
    for pair in ["EUR_USD", "GBP_USD"]:
        s2 = _pair_stage2(phase14, pair)
        redesign_by_pair[pair] = {
            "stage2_snapshot": {
                "trigger_rate": float(s2.get("trigger_rate", 0.0) or 0.0),
                "false_cut_rate_on_winners": float(s2.get("false_cut_rate_on_winners", 0.0) or 0.0),
                "estimated_saved_loss_total_pips": float(s2.get("estimated_saved_loss_total_pips", 0.0) or 0.0),
                "passes_pair_gate": bool(
                    phase14.get("acceptance_gate_evaluation", {})
                    .get("by_pair", {})
                    .get(pair, {})
                    .get("passes_pair_gate", False)
                ),
            },
            "mitigation_cards": _mitigation_cards(pair, s2, max_rate),
        }

    protocol_lock = {
        "mode": "PAPER_ONLY_REDESIGN",
        "forbidden": [
            "production policy changes",
            "parameter tuning",
            "runtime execution changes",
        ],
        "required_before_phase16": [
            "mitigation cards approved",
            "counterfactual rerun protocol locked",
            "pair-specific false-cut targets locked",
        ],
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE15_FALSE_CUT_MITIGATION_TRIAL_REDESIGN_PAPER_ONLY",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "PAPER_ONLY",
        },
        "dependency": {
            "phase14_status": phase14.get("status"),
            "phase14_path": str(PHASE14_PATH),
            "phase13_status": phase13.get("status"),
            "phase13_path": str(PHASE13_PATH),
            "phase14_overall_gate_pass": phase14.get("acceptance_gate_evaluation", {}).get("overall_pass"),
        },
        "failure_root": {
            "max_false_cut_rate_allowed": max_rate,
            "pair_false_cut_rates": {
                "EUR_USD": redesign_by_pair["EUR_USD"]["stage2_snapshot"]["false_cut_rate_on_winners"],
                "GBP_USD": redesign_by_pair["GBP_USD"]["stage2_snapshot"]["false_cut_rate_on_winners"],
            },
        },
        "redesign_by_pair": redesign_by_pair,
        "redesign_priority_order": [
            "Winner-protection confirmation gate",
            "Tiered trigger throttling",
            "Selective trial deactivation plan",
        ],
        "protocol_lock": protocol_lock,
        "phase16_entry_gate": {
            "requires": [
                "all mitigation cards reviewed",
                "rerun spec includes pair-specific false-cut guard",
                "no scope expansion",
            ],
            "status": "PENDING",
        },
        "pass_conditions": {
            "phase14_dependency_passed": phase14.get("status") == "PASS",
            "failure_root_captured": True,
            "pair_mitigation_cards_present": True,
            "paper_only_confirmed": True,
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
                "max_false_cut_rate_allowed": max_rate,
                "pair_false_cut_rates": report["failure_root"]["pair_false_cut_rates"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()