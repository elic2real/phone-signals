#!/usr/bin/env python3
"""Build MVP Phase 16 mitigation-guarded counterfactual rerun plan (no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PHASE15_PATH = Path("control/mvp_phase15_false_cut_mitigation_trial_redesign_paper_only.json")
PHASE14_PATH = Path("control/mvp_phase14_counterfactual_simulation_execution_no_tuning.json")
OUTPUT_PATH = Path("control/mvp_phase16_counterfactual_rerun_plan_with_mitigation_guards.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _pair_snapshot(phase15: Dict[str, Any], pair: str) -> Dict[str, Any]:
    return (
        phase15.get("redesign_by_pair", {})
        .get(pair, {})
        .get("stage2_snapshot", {})
    )


def _pair_mitigation_cards(phase15: Dict[str, Any], pair: str) -> List[Dict[str, Any]]:
    return (
        phase15.get("redesign_by_pair", {})
        .get(pair, {})
        .get("mitigation_cards", [])
    )


def _rerun_plan_for_pair(pair: str, phase15: Dict[str, Any], max_false_cut: float) -> Dict[str, Any]:
    snap = _pair_snapshot(phase15, pair)
    cards = _pair_mitigation_cards(phase15, pair)

    ordered_cards = sorted(cards, key=lambda c: str(c.get("card_id", "")))
    m1 = next((c for c in ordered_cards if str(c.get("card_id", "")).endswith("-M1")), None)
    m2 = next((c for c in ordered_cards if str(c.get("card_id", "")).endswith("-M2")), None)
    m3 = next((c for c in ordered_cards if str(c.get("card_id", "")).endswith("-M3")), None)

    variants = []
    variants.append(
        {
            "variant_id": f"P16-{pair}-V0",
            "name": "control_replay",
            "description": "Re-run prior stack as locked baseline comparator.",
            "active_guards": [],
            "expected": {
                "false_cut_rate": float(snap.get("false_cut_rate_on_winners", 0.0) or 0.0),
                "trigger_rate": float(snap.get("trigger_rate", 0.0) or 0.0),
            },
        }
    )
    if m1:
        variants.append(
            {
                "variant_id": f"P16-{pair}-V1",
                "name": "m1_winner_confirmation_only",
                "description": "Apply winner-protection confirmation gate only.",
                "active_guards": [m1.get("card_id")],
                "expected": {
                    "false_cut_rate": "down_material",
                    "saved_loss": "down_moderate",
                },
            }
        )
    if m1 and m2:
        variants.append(
            {
                "variant_id": f"P16-{pair}-V2",
                "name": "m1_plus_m2",
                "description": "Apply confirmation gate and trigger throttling.",
                "active_guards": [m1.get("card_id"), m2.get("card_id")],
                "expected": {
                    "false_cut_rate": "down",
                    "trigger_rate": "down",
                    "saved_loss": "balanced",
                },
            }
        )
    if m3:
        variants.append(
            {
                "variant_id": f"P16-{pair}-V3",
                "name": "m3_anchor_mode",
                "description": "Use selective deactivation anchor mode (P12-T2-first).",
                "active_guards": [m3.get("card_id")],
                "expected": {
                    "false_cut_rate": "down_to_compliant",
                    "saved_loss": "down",
                    "gate_pass_probability": "up",
                },
            }
        )

    guard_gate = {
        "target_false_cut_rate_max": max_false_cut,
        "primary_positive_metrics_required": 2,
        "must_not_regress_scope": True,
    }

    return {
        "pair": pair,
        "current_snapshot": snap,
        "variants": variants,
        "guard_gate": guard_gate,
        "promotion_rule": "Promote only variants with false_cut_rate_on_winners <= target and primary_positive_metrics >= 2.",
    }


def main() -> None:
    phase15 = _load_json(PHASE15_PATH)
    phase14 = _load_json(PHASE14_PATH)

    max_false_cut = float(
        phase15.get("failure_root", {})
        .get("max_false_cut_rate_allowed", 0.15)
        or 0.15
    )

    rerun_plan = {
        "EUR_USD": _rerun_plan_for_pair("EUR_USD", phase15, max_false_cut),
        "GBP_USD": _rerun_plan_for_pair("GBP_USD", phase15, max_false_cut),
    }

    staged_protocol = [
        {
            "stage": 1,
            "name": "control_replay",
            "objective": "Verify reproducibility versus Phase 14 gate-fail baseline.",
        },
        {
            "stage": 2,
            "name": "single_guard_trials",
            "objective": "Measure isolated effect of each mitigation guard.",
        },
        {
            "stage": 3,
            "name": "guard_combination_trials",
            "objective": "Evaluate additive mitigation bundles for compliance.",
        },
        {
            "stage": 4,
            "name": "adjudication_pack",
            "objective": "Prepare promote/hold decision package with pair-level evidence.",
        },
    ]

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE16_COUNTERFACTUAL_RERUN_PLAN_WITH_MITIGATION_GUARDS",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "COUNTERFACTUAL_RERUN_PLANNING_ONLY",
        },
        "dependency": {
            "phase15_status": phase15.get("status"),
            "phase15_path": str(PHASE15_PATH),
            "phase14_status": phase14.get("status"),
            "phase14_path": str(PHASE14_PATH),
        },
        "failure_context": {
            "phase14_gate_overall_pass": phase14.get("acceptance_gate_evaluation", {}).get("overall_pass"),
            "max_false_cut_rate_allowed": max_false_cut,
            "pair_false_cut_rates": phase15.get("failure_root", {}).get("pair_false_cut_rates", {}),
        },
        "mitigation_guard_rerun_plan": rerun_plan,
        "staged_protocol": staged_protocol,
        "execution_lock_requirements": {
            "forbidden": [
                "live production behavior changes",
                "parameter tuning",
                "scope expansion",
            ],
            "required_before_phase17_execution": [
                "variant matrix approval",
                "pair gate thresholds confirmed",
                "rollback criteria frozen",
            ],
        },
        "phase17_entry_gate": {
            "requires": [
                "approved rerun variants per pair",
                "acceptance and rollback metrics locked",
                "no doctrine violations",
            ],
            "status": "PENDING",
        },
        "pass_conditions": {
            "phase15_dependency_passed": phase15.get("status") == "PASS",
            "phase14_dependency_passed": phase14.get("status") == "PASS",
            "pair_rerun_variants_present": True,
            "guard_gate_present": True,
            "planning_only_mode_confirmed": True,
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
                "variant_counts": {
                    "EUR_USD": len(rerun_plan["EUR_USD"]["variants"]),
                    "GBP_USD": len(rerun_plan["GBP_USD"]["variants"]),
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()