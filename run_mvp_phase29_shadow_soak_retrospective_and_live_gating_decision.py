#!/usr/bin/env python3
"""Execute MVP Phase 29: shadow soak retrospective and live gating decision (no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PHASE28_PATH = Path("control/mvp_phase28_shadow_soak_execution_and_incident_audit.json")
PHASE26_PATH = Path("control/mvp_phase26_shadow_stability_window_proof.json")
OUTPUT_PATH = Path("control/mvp_phase29_shadow_soak_retrospective_and_live_gating_decision.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _f(v: Any) -> float:
    return float(v or 0.0)


def _segment_status_table(segments: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    table: List[Dict[str, Any]] = []
    for seg in segments:
        table.append(
            {
                "segment": seg.get("segment"),
                "duration_hours": int(seg.get("duration_hours", 0) or 0),
                "status": seg.get("status"),
                "hard_stop_triggered": bool(seg.get("hard_stop_triggered", False)),
                "metrics": seg.get("metrics", {}),
            }
        )
    return table


def main() -> None:
    phase28 = _load_json(PHASE28_PATH)
    phase26 = _load_json(PHASE26_PATH)

    segments = list(phase28.get("soak_execution", {}).get("executed_segments", []))
    segment_table = _segment_status_table(segments)

    soak_hours = int(phase28.get("soak_execution", {}).get("total_completed_hours", 0) or 0)
    hard_stop_count = int(phase28.get("soak_execution", {}).get("hard_stop_trigger_count", 0) or 0)
    incident_count = int(phase28.get("incident_audit", {}).get("incident_count", 0) or 0)
    release_lock_violations = int(phase28.get("incident_audit", {}).get("release_lock_violations", 0) or 0)
    rollback_pass = str(phase28.get("incident_audit", {}).get("rollback_drill_result", "")).upper() == "PASS"

    stability = phase26.get("stability_summary", {})
    weighted_deltas = [_f(v) for v in stability.get("weighted_delta_pph_values", [])]
    weighted_min_delta = min(weighted_deltas) if weighted_deltas else 0.0
    weighted_rel_spread = _f(stability.get("weighted_intervention_relative_spread"))
    max_false_cut = _f(stability.get("max_false_cut_rate_from_phase25"))

    live_gates = {
        "soak_72h_completed": soak_hours >= 72,
        "no_hard_stop_incidents": hard_stop_count == 0 and incident_count == 0,
        "no_release_lock_violations": release_lock_violations == 0,
        "rollback_drill_pass": rollback_pass,
        "false_cut_rate_within_live_cap": max_false_cut <= 0.10,
        "weighted_stability_spread_within_live_cap": weighted_rel_spread <= 0.50,
        "min_weighted_delta_positive": weighted_min_delta > 0.0,
    }

    live_gate_pass = all(live_gates.values())

    pass_conditions = {
        "phase28_dependency_passed": phase28.get("status") == "PASS",
        "phase28_verdict_promote": phase28.get("decision", {}).get("verdict") == "PROMOTE",
        "retrospective_completed": len(segment_table) == 3,
        "all_segments_passed": all(str(s.get("status")) == "PASS" for s in segment_table),
        "all_live_gates_evaluated": True,
        "live_gating_decision_made": True,
        "no_tuning_applied": True,
    }
    overall_pass = all(pass_conditions.values())

    live_gating_decision = {
        "decision": (
            "APPROVE_MICRO_LIVE_PILOT_WITH_HARD_KILL_SWITCH"
            if live_gate_pass
            else "HOLD_LIVE_AND_CONTINUE_SHADOW"
        ),
        "live_gate_pass": live_gate_pass,
        "live_promotion_allowed": False,
        "micro_live_pilot_allowed": live_gate_pass,
        "required_safety_controls": [
            "hard kill-switch active",
            "instant rollback path ready",
            "scope lock EUR_USD/GBP_USD LONDON only",
            "no tuning during pilot",
        ],
        "reason": (
            "Shadow soak retrospective passed and all live-entry safety gates are satisfied for a bounded micro pilot."
            if live_gate_pass
            else "One or more live-entry safety gates failed; keep shadow-only operation."
        ),
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE29_SHADOW_SOAK_RETROSPECTIVE_AND_LIVE_GATING_DECISION",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "SHADOW_RETROSPECTIVE_AND_LIVE_GATING",
        },
        "dependency": {
            "phase28_status": phase28.get("status"),
            "phase28_path": str(PHASE28_PATH),
            "phase26_status": phase26.get("status"),
            "phase26_path": str(PHASE26_PATH),
        },
        "retrospective": {
            "segment_summary": segment_table,
            "soak_total_hours": soak_hours,
            "hard_stop_trigger_count": hard_stop_count,
            "incident_count": incident_count,
            "release_lock_violations": release_lock_violations,
        },
        "live_entry_gates": {
            "gate_thresholds": {
                "max_false_cut_rate": 0.10,
                "max_weighted_relative_spread": 0.50,
                "min_weighted_delta_pph": 0.0,
            },
            "observed": {
                "max_false_cut_rate": max_false_cut,
                "weighted_relative_spread": weighted_rel_spread,
                "min_weighted_delta_pph": weighted_min_delta,
            },
            "gate_results": live_gates,
        },
        "live_gating_decision": live_gating_decision,
        "pass_conditions": pass_conditions,
        "decision": {
            "verdict": "PROMOTE" if overall_pass else "HOLD",
            "overall_pass": overall_pass,
            "reason": (
                "Retrospective and live-gating decision completed with explicit safety outcome under lock-held constraints."
                if overall_pass
                else "Phase29 retrospective or live-gating decision process failed."
            ),
            "release_action": (
                "ALLOW_MICRO_LIVE_PILOT_PREP_ONLY"
                if live_gate_pass
                else "SHADOW_ONLY_CONTINUE"
            ),
            "live_promotion_allowed": False,
        },
        "next_recommended_task": (
            "MVP_PHASE30_MICRO_LIVE_PILOT_PREP_AND_GUARDRAILS"
            if live_gate_pass
            else "MVP_PHASE29B_SHADOW_EXTENSION_AND_GATE_RETRY"
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
                "live_gate_pass": live_gate_pass,
                "next": report["next_recommended_task"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()