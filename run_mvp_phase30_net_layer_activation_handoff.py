#!/usr/bin/env python3
"""Execute MVP Phase 30: Net Layer activation handoff (no new reruns)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

PHASE29_PATH = Path("control/mvp_phase29_shadow_soak_retrospective_and_live_gating_decision.json")
OUTPUT_PATH = Path("control/mvp_phase30_net_layer_activation_handoff.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    phase29 = _load_json(PHASE29_PATH)

    live_gate = phase29.get("live_gating_decision", {})
    gate_results = phase29.get("live_entry_gates", {}).get("gate_results", {})
    observed = phase29.get("live_entry_gates", {}).get("observed", {})

    pass_conditions = {
        "phase29_dependency_passed": phase29.get("status") == "PASS",
        "phase29_verdict_promote": phase29.get("decision", {}).get("verdict") == "PROMOTE",
        "micro_live_pilot_allowed": bool(live_gate.get("micro_live_pilot_allowed", False)),
        "live_promotion_still_blocked": bool(live_gate.get("live_promotion_allowed", False)) is False,
        "critical_live_entry_gates_all_pass": all(bool(v) for v in gate_results.values()) if gate_results else False,
        "false_cut_within_cap": float(observed.get("max_false_cut_rate", 1.0) or 1.0) <= 0.10,
        "stability_spread_within_cap": float(observed.get("weighted_relative_spread", 1.0) or 1.0) <= 0.50,
        "no_tuning_applied": True,
    }
    overall_pass = all(pass_conditions.values())

    net_layer_packet = {
        "activation_mode": "NET_LAYER_PREP_ONLY",
        "policy": {
            "scope_lock": phase29.get("scope_lock", {}),
            "execution_window": "MICRO_LIVE_PILOT_PREP",
            "hard_guards": [
                "hard kill-switch must stay enabled",
                "instant rollback must remain armed",
                "live promotion remains blocked",
                "no tuning allowed in net-layer prep",
            ],
        },
        "entry_contract": {
            "accepted_pairs": ["EUR_USD", "GBP_USD"],
            "accepted_sessions": ["LONDON"],
            "max_false_cut_rate": 0.10,
            "max_weighted_relative_spread": 0.50,
            "min_weighted_delta_pph": 0.0,
        },
        "evidence_snapshot": {
            "phase29_task_id": phase29.get("task_id"),
            "soak_total_hours": phase29.get("retrospective", {}).get("soak_total_hours"),
            "hard_stop_trigger_count": phase29.get("retrospective", {}).get("hard_stop_trigger_count"),
            "incident_count": phase29.get("retrospective", {}).get("incident_count"),
            "observed_live_gates": observed,
        },
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE30_NET_LAYER_ACTIVATION_HANDOFF",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "NET_LAYER_PREP_ONLY",
        },
        "dependency": {
            "phase29_status": phase29.get("status"),
            "phase29_path": str(PHASE29_PATH),
        },
        "net_layer_handoff": net_layer_packet,
        "pass_conditions": pass_conditions,
        "decision": {
            "verdict": "PROMOTE" if overall_pass else "HOLD",
            "overall_pass": overall_pass,
            "reason": (
                "Net Layer handoff packet is activated from Phase29 evidence with strict prep-only live guardrails."
                if overall_pass
                else "Net Layer handoff blocked because one or more safety dependencies failed."
            ),
            "release_action": "NET_LAYER_PREP_ONLY",
            "live_promotion_allowed": False,
        },
        "next_recommended_task": (
            "MVP_PHASE31_NET_LAYER_MICRO_LIVE_EXECUTION_PLAN"
            if overall_pass
            else "MVP_PHASE30B_NET_LAYER_BLOCKER_REMEDIATION"
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
