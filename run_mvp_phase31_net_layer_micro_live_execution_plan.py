#!/usr/bin/env python3
"""Execute MVP Phase 31: net-layer micro-live execution plan (no tuning, no reruns)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

PHASE30_PATH = Path("control/mvp_phase30_net_layer_activation_handoff.json")
ENTRY_POLICY_PATH = Path("entry_v23_policy_guarded_active.json")
OUTPUT_PATH = Path("control/mvp_phase31_net_layer_micro_live_execution_plan.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    phase30 = _load_json(PHASE30_PATH)
    entry_policy = _load_json(ENTRY_POLICY_PATH)

    handoff = phase30.get("net_layer_handoff", {})
    contract = handoff.get("entry_contract", {})
    evidence = handoff.get("evidence_snapshot", {})
    gates = evidence.get("observed_live_gates", {})
    scope_lock = phase30.get("scope_lock", {})

    include_pairs = (
        entry_policy.get("entry_filters", {}).get("include_pairs", [])
    )
    include_sessions = (
        entry_policy.get("entry_filters", {}).get("include_sessions", [])
    )

    micro_live_execution_plan = {
        "plan_id": "NET_LAYER_MICRO_LIVE_PLAN_V1",
        "mode": "NET_LAYER_MICRO_LIVE_EXECUTION",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
        },
        "traffic_staging": [
            {
                "wave": "WAVE_1",
                "duration_minutes": 120,
                "risk_cap_fraction": 0.10,
                "requires_all_previous_wave_pass": True,
            },
            {
                "wave": "WAVE_2",
                "duration_minutes": 120,
                "risk_cap_fraction": 0.20,
                "requires_all_previous_wave_pass": True,
            },
            {
                "wave": "WAVE_3",
                "duration_minutes": 120,
                "risk_cap_fraction": 0.30,
                "requires_all_previous_wave_pass": True,
            },
        ],
        "entry_contract": {
            "pairs": contract.get("accepted_pairs", []),
            "sessions": contract.get("accepted_sessions", []),
            "max_false_cut_rate": float(contract.get("max_false_cut_rate", 0.10) or 0.10),
            "max_weighted_relative_spread": float(contract.get("max_weighted_relative_spread", 0.50) or 0.50),
            "min_weighted_delta_pph": float(contract.get("min_weighted_delta_pph", 0.0) or 0.0),
        },
        "abort_rules": [
            "If false_cut_rate_on_winners exceeds 0.10, trigger hard stop and rollback.",
            "If weighted_intervention_relative_spread exceeds 0.50, stop wave progression.",
            "If any critical incident occurs (close_404_count > 0 or ownership drift), rollback immediately.",
            "If scope lock is violated (pair/session/tuning), stop and invalidate run.",
        ],
        "rollback_protocol": {
            "kill_switch": "ARMED",
            "rollback_target": "PHASE30_NET_LAYER_PREP_ONLY_LOCK",
            "max_rollback_time_seconds": 30,
            "required_checks_after_rollback": [
                "position_flatten_confirmed",
                "guardrail_rearmed",
                "scope_lock_restored",
            ],
        },
        "observability_contract": {
            "required_counters": [
                "false_cut_rate_on_winners",
                "weighted_intervention_relative_spread",
                "weighted_delta_pph",
                "hard_stop_trigger_count",
                "incident_count",
            ],
            "audit_interval_seconds": 60,
            "final_soak_retrospective_required": True,
        },
        "promotion_policy": {
            "allow_micro_live_execution": True,
            "allow_full_live_promotion": False,
            "full_live_requires": [
                "phase31_execution_proof_pass",
                "phase31_incident_audit_pass",
                "explicit_signoff_task_complete",
            ],
        },
    }

    pass_conditions = {
        "phase30_dependency_passed": phase30.get("status") == "PASS",
        "phase30_verdict_promote": phase30.get("decision", {}).get("verdict") == "PROMOTE",
        "scope_lock_matches_entry_policy": include_pairs == ["EUR_USD", "GBP_USD"] and include_sessions == ["LONDON"],
        "observed_false_cut_within_contract": float(gates.get("max_false_cut_rate", 1.0) or 1.0) <= float(contract.get("max_false_cut_rate", 0.10) or 0.10),
        "observed_spread_within_contract": float(gates.get("weighted_relative_spread", 1.0) or 1.0) <= float(contract.get("max_weighted_relative_spread", 0.50) or 0.50),
        "observed_delta_within_contract": float(gates.get("min_weighted_delta_pph", -1.0) or -1.0) >= float(contract.get("min_weighted_delta_pph", 0.0) or 0.0),
        "no_tuning_applied": scope_lock.get("tuning") == "NONE",
        "full_live_still_blocked": True,
    }
    overall_pass = all(pass_conditions.values())

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE31_NET_LAYER_MICRO_LIVE_EXECUTION_PLAN",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "NET_LAYER_MICRO_LIVE_PLAN_ONLY",
        },
        "dependency": {
            "phase30_status": phase30.get("status"),
            "phase30_path": str(PHASE30_PATH),
            "entry_policy_path": str(ENTRY_POLICY_PATH),
        },
        "micro_live_execution_plan": micro_live_execution_plan,
        "pass_conditions": pass_conditions,
        "decision": {
            "verdict": "PROMOTE" if overall_pass else "HOLD",
            "overall_pass": overall_pass,
            "reason": (
                "Net-layer micro-live execution plan is complete with hard guardrails and rollback controls."
                if overall_pass
                else "Net-layer micro-live execution plan conditions failed; hold until contract and guardrails are satisfied."
            ),
            "release_action": "ALLOW_NET_LAYER_MICRO_LIVE_EXECUTION_UNDER_KILL_SWITCH" if overall_pass else "HOLD_PREP_ONLY",
            "live_promotion_allowed": False,
        },
        "next_recommended_task": (
            "MVP_PHASE32_NET_LAYER_MICRO_LIVE_EXECUTION_AND_INCIDENT_AUDIT"
            if overall_pass
            else "MVP_PHASE31B_NET_LAYER_PLAN_REMEDIATION"
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
