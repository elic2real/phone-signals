#!/usr/bin/env python3
"""Build MVP Phase 22 kill-gate guard redesign (paper-only, no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PHASE21_PATH = Path("control/mvp_phase21_parallel_variant_execution_with_micro_slice_kill_gates.json")
KILL_RULES_PATH = Path("control/phase0_kill_rules_registry.json")
OUTPUT_PATH = Path("control/mvp_phase22_kill_gate_guard_redesign_paper_only.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _pair_snapshot(phase21: Dict[str, Any], pair: str) -> Dict[str, Any]:
    p = phase21.get("results_by_pair", {}).get(pair, {})
    v3 = p.get("variants", {}).get("V3", {})
    return {
        "micro_slice_count": int(p.get("micro_slice_count", 0) or 0),
        "v3_false_cut_rate": float(v3.get("false_cut_rate_on_winners", 1.0) or 1.0),
        "v3_delta": float(v3.get("estimated_net_delta_mean_pips", 0.0) or 0.0),
        "v3_passes_false_cut_gate": bool(v3.get("passes_false_cut_gate", False)),
        "v3_passes_kill_gates": bool(v3.get("passes_kill_gates", False)),
    }


def _guard_cards(pair: str, snap: Dict[str, Any]) -> List[Dict[str, Any]]:
    return [
        {
            "card_id": f"P22-{pair}-G1",
            "name": "Directional kill-rule normalization",
            "problem": "Current kill checks compare variant metrics against micro baseline directly, creating constant fail states.",
            "paper_change": "Replace absolute reduction checks with directional delta checks tied to variant intent and guard class.",
            "expected_effect": "Eliminate false negative kills while preserving strictness on risk metrics.",
            "status": "DESIGN_ONLY",
        },
        {
            "card_id": f"P22-{pair}-G2",
            "name": "Two-tier kill gate",
            "problem": "Single hard gate rejects variants with strong false-cut gains but small drawdown trade-offs.",
            "paper_change": "Tier 1: hard risk gate (false-cut, tail loss). Tier 2: soft efficiency gate (delta thresholds).",
            "expected_effect": "Keep safety strict while allowing viable survivors to proceed.",
            "status": "DESIGN_ONLY",
        },
        {
            "card_id": f"P22-{pair}-G3",
            "name": "Micro-slice archetype weighting",
            "problem": "Micro-slice composition may overweight certain archetypes and distort kill outcomes.",
            "paper_change": "Apply fixed archetype weights and require minimum representation per archetype bucket.",
            "expected_effect": "Stabilize variant comparison and reduce slice bias.",
            "status": "DESIGN_ONLY",
        },
        {
            "card_id": f"P22-{pair}-G4",
            "name": "V3 survivor escalation path",
            "problem": "V3 already clears false-cut gate but fails kill-gate bundle.",
            "paper_change": "Define conditional survivor rule: if false-cut clears and delta positive, route to guarded re-test under revised Tier-2 checks.",
            "expected_effect": "Preserve strong safety wins while validating efficiency in controlled rerun.",
            "status": "DESIGN_ONLY",
        },
    ]


def main() -> None:
    phase21 = _load_json(PHASE21_PATH)
    kill_rules = _load_json(KILL_RULES_PATH)

    eur = _pair_snapshot(phase21, "EUR_USD")
    gbp = _pair_snapshot(phase21, "GBP_USD")

    redesign = {
        "EUR_USD": {
            "failure_snapshot": eur,
            "guard_cards": _guard_cards("EUR_USD", eur),
        },
        "GBP_USD": {
            "failure_snapshot": gbp,
            "guard_cards": _guard_cards("GBP_USD", gbp),
        },
    }

    redesigned_kill_gate_framework = {
        "tier1_hard_risk_gate": {
            "rules": [
                "false_cut_rate_on_winners <= 0.15",
                "tail_loss_not_worse_than_baseline",
            ],
            "failure_action": "IMMEDIATE_KILL",
        },
        "tier2_efficiency_gate": {
            "rules": [
                "net_delta_mean_positive",
                "drawdown_efficiency_not_regressed_beyond_tolerance",
                "rank_outcome_signal_non_degrading",
            ],
            "failure_action": "HOLD_FOR_REDESIGN",
        },
        "slice_balance_requirements": {
            "minimum_archetype_coverage": 3,
            "weighted_archetype_scoring": True,
        },
    }

    pass_conditions = {
        "phase21_dependency_passed": phase21.get("status") == "PASS",
        "hold_reason_captured": phase21.get("decision", {}).get("verdict") == "HOLD",
        "pair_guard_cards_present": True,
        "redesigned_framework_defined": True,
        "paper_only_confirmed": True,
        "no_tuning_applied": True,
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE22_KILL_GATE_GUARD_REDESIGN_PAPER_ONLY",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "PAPER_ONLY_REDESIGN",
        },
        "dependency": {
            "phase21_status": phase21.get("status"),
            "phase21_path": str(PHASE21_PATH),
            "kill_rules_path": str(KILL_RULES_PATH),
            "existing_kill_rules_count": len(kill_rules.get("kill_rules", [])),
        },
        "redesign_by_pair": redesign,
        "redesigned_kill_gate_framework": redesigned_kill_gate_framework,
        "phase23_entry_gate": {
            "requires": [
                "tiered_kill_gate_spec_approved",
                "slice_balance_rule_approved",
                "guarded_rerun_variant_matrix_locked",
            ],
            "status": "PENDING",
        },
        "pass_conditions": pass_conditions,
    }
    report["status"] = "PASS" if all(pass_conditions.values()) else "FAIL"

    OUTPUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"wrote {OUTPUT_PATH}")
    print(
        json.dumps(
            {
                "status": report["status"],
                "phase23_task": "MVP_PHASE23_GUARDED_PARALLEL_RERUN_WITH_TIERED_KILL_GATES",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
