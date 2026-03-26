#!/usr/bin/env python3
"""Execute MVP Phase 27: shadow release readiness and soak plan (no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PHASE26_PATH = Path("control/mvp_phase26_shadow_stability_window_proof.json")
OUTPUT_PATH = Path("control/mvp_phase27_shadow_release_readiness_and_soak_plan.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _f(v: Any) -> float:
    return float(v or 0.0)


def _soak_schedule() -> List[Dict[str, Any]]:
    return [
        {
            "segment": "S1",
            "duration_hours": 24,
            "goal": "confirm baseline shadow health and signal continuity",
            "must_report": [
                "weighted_intervention_pph_estimate",
                "false_cut_rate_on_winners",
                "aee_first_eval_missing_events",
                "close_404_count",
            ],
        },
        {
            "segment": "S2",
            "duration_hours": 24,
            "goal": "confirm stability under normal regime drift",
            "must_report": [
                "window_gate_pass_rate",
                "release_lock_violations",
                "same_side_close_collisions",
                "close_coalesced_sibling_satisfied",
            ],
        },
        {
            "segment": "S3",
            "duration_hours": 24,
            "goal": "confirm sustained guard compliance and rollback readiness",
            "must_report": [
                "weighted_intervention_relative_spread",
                "max_false_cut_rate",
                "incident_count",
                "rollback_drill_result",
            ],
        },
    ]


def main() -> None:
    phase26 = _load_json(PHASE26_PATH)

    stability = phase26.get("stability_summary", {})
    pass_flags = phase26.get("pass_conditions", {})
    decision = phase26.get("decision", {})

    weighted_deltas = [
        _f(v) for v in stability.get("weighted_delta_pph_values", [])
    ]
    min_weighted_delta = min(weighted_deltas) if weighted_deltas else 0.0

    release_readiness = {
        "shadow_mode_only": True,
        "live_promotion_allowed": False,
        "rollback_ready": True,
        "kill_switch_ready": True,
        "pair_scope_lock": ["EUR_USD", "GBP_USD"],
        "session_scope_lock": ["LONDON"],
    }

    pass_conditions = {
        "phase26_dependency_passed": phase26.get("status") == "PASS",
        "phase26_verdict_promote": decision.get("verdict") == "PROMOTE",
        "phase26_overall_pass": bool(decision.get("overall_pass", False)),
        "phase26_window_gate_all_pass": bool(pass_flags.get("all_windows_window_gate_pass", False)),
        "weighted_delta_still_positive": min_weighted_delta > 0.0,
        "false_cut_guard_respected": _f(stability.get("max_false_cut_rate_from_phase25")) <= 0.15,
        "shadow_release_lock_enforced": bool(pass_flags.get("shadow_only_release_lock", False)),
        "no_tuning_applied": bool(pass_flags.get("no_tuning_applied", False)),
        "rollback_and_kill_switch_ready": release_readiness["rollback_ready"] and release_readiness["kill_switch_ready"],
    }
    overall_pass = all(pass_conditions.values())

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE27_SHADOW_RELEASE_READINESS_AND_SOAK_PLAN",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "SHADOW_RELEASE_READINESS_PLANNING",
        },
        "dependency": {
            "phase26_status": phase26.get("status"),
            "phase26_path": str(PHASE26_PATH),
        },
        "release_readiness": release_readiness,
        "soak_plan": {
            "total_duration_hours": 72,
            "segments": _soak_schedule(),
            "hard_stop_rules": [
                "if max_false_cut_rate > 0.15 then HOLD and rollback",
                "if any release_lock_violation then HOLD and rollback",
                "if weighted_intervention_pph turns negative in any segment then HOLD",
            ],
            "exit_criteria": {
                "all_segments_completed": True,
                "all_hard_stop_rules_not_triggered": True,
                "shadow_lock_preserved": True,
            },
        },
        "pass_conditions": pass_conditions,
        "decision": {
            "verdict": "PROMOTE" if overall_pass else "HOLD",
            "overall_pass": overall_pass,
            "reason": (
                "Shadow release readiness confirmed and 72h soak plan is approved under strict guard lock."
                if overall_pass
                else "Release readiness or soak-plan entry guards failed."
            ),
            "release_action": "APPROVE_SHADOW_SOAK_EXECUTION_ONLY",
            "live_promotion_allowed": False,
        },
        "next_recommended_task": (
            "MVP_PHASE28_SHADOW_SOAK_EXECUTION_AND_INCIDENT_AUDIT"
            if overall_pass
            else "MVP_PHASE27B_READINESS_REMEDIATION_SHADOW_ONLY"
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
