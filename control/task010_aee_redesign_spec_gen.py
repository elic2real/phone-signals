#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
CONTROL = ROOT / "control"

FAMILIES = [
    "EXPANSION_BREAKOUT",
    "RECLAIM_CONTINUATION",
    "PULLBACK_CONTINUATION",
    "RANGE_ESCAPE",
    "OTHER",
]

STATES = [
    "INIT",
    "PROBING",
    "CONFIRMING",
    "EXPANDING",
    "EXHAUSTING",
    "FAILING",
    "EXIT",
]


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _family_damage_map(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        fam = str(r.get("family", ""))
        branches = list(r.get("exit_branches", []))
        branches.sort(key=lambda x: float(x.get("branch_removal_delta_pph", 0.0)), reverse=True)
        out[fam] = branches
    return out


def _family_behavior_profile(family: str) -> dict[str, Any]:
    if family == "EXPANSION_BREAKOUT":
        return {
            "expected_early_drawdown": "low",
            "expected_time_to_green": "fast",
            "expected_confirmation_time": "short",
            "expected_stall_profile": "low stall tolerance",
            "expected_giveback_profile": "tight giveback tolerance",
        }
    if family == "PULLBACK_CONTINUATION":
        return {
            "expected_early_drawdown": "moderate",
            "expected_time_to_green": "medium",
            "expected_confirmation_time": "medium",
            "expected_stall_profile": "moderate stall tolerance",
            "expected_giveback_profile": "moderate giveback tolerance",
        }
    if family == "RECLAIM_CONTINUATION":
        return {
            "expected_early_drawdown": "moderate_to_high",
            "expected_time_to_green": "delayed",
            "expected_confirmation_time": "longer",
            "expected_stall_profile": "higher stall tolerance during reclaim",
            "expected_giveback_profile": "structured giveback tolerance",
        }
    if family == "RANGE_ESCAPE":
        return {
            "expected_early_drawdown": "moderate",
            "expected_time_to_green": "medium",
            "expected_confirmation_time": "medium",
            "expected_stall_profile": "contextual stall tolerance",
            "expected_giveback_profile": "moderate giveback tolerance",
        }
    return {
        "expected_early_drawdown": "unknown_mixed",
        "expected_time_to_green": "mixed",
        "expected_confirmation_time": "mixed",
        "expected_stall_profile": "mixed",
        "expected_giveback_profile": "mixed",
    }


def main() -> None:
    family_sim = _load_json(CONTROL / "aee_family_exit_policy_simulation_report.json")
    branch_rank = _load_json(CONTROL / "aee_exit_branch_damage_rank.json")
    recommendations = _load_json(CONTROL / "aee_family_branch_recommendation.json")

    damage_map = _family_damage_map(list(family_sim.get("rows", [])))
    rec_rows = {str(r.get("family")): r for r in recommendations.get("rows", [])}

    v3_spec = {
        "generated_at": _iso_now(),
        "task_id": "TASK-010",
        "name": "AEE_FAMILY_STATE_MACHINE_V3",
        "states": STATES,
        "family_specs": {},
    }

    transition_rows = []
    survival_rows = []
    impl_rows = []

    top_global_damage = list(branch_rank.get("rows", []))[:5]

    for fam in FAMILIES:
        profile = _family_behavior_profile(fam)
        fam_damage = damage_map.get(fam, [])
        primary = fam_damage[0] if fam_damage else None
        secondary = fam_damage[1] if len(fam_damage) > 1 else None

        v3_spec["family_specs"][fam] = {
            **profile,
            "promotion_criteria": {
                "INIT_to_PROBING": "entry intent remains valid under family-expected adverse behavior",
                "PROBING_to_CONFIRMING": "directional follow-through matches family confirmation profile",
                "CONFIRMING_to_EXPANDING": "progress and energy confirm continuation",
                "EXPANDING_to_EXHAUSTING": "deceleration or giveback exceeds family expansion expectations",
                "EXHAUSTING_to_FAILING": "recovery attempts fail against family-specific invalidation rules",
                "FAILING_to_EXIT": "family invalidation confirmed",
            },
            "invalidation_conditions": {
                "generic": "pattern no longer behaves like family definition",
                "primary_destructive_branch_from_task009": primary,
                "secondary_destructive_branch_from_task009": secondary,
            },
        }

        transition_rows.extend(
            [
                {
                    "family": fam,
                    "from_state": "INIT",
                    "to_state": "PROBING",
                    "trigger": "entry accepted and initial path observed",
                },
                {
                    "family": fam,
                    "from_state": "PROBING",
                    "to_state": "CONFIRMING",
                    "trigger": "family-specific confirmation behavior present",
                },
                {
                    "family": fam,
                    "from_state": "CONFIRMING",
                    "to_state": "EXPANDING",
                    "trigger": "continuation confirmed with acceptable giveback",
                },
                {
                    "family": fam,
                    "from_state": "EXPANDING",
                    "to_state": "EXHAUSTING",
                    "trigger": "momentum decay or stall beyond family profile",
                },
                {
                    "family": fam,
                    "from_state": "EXHAUSTING",
                    "to_state": "FAILING",
                    "trigger": "recovery attempts do not restore family-valid behavior",
                },
                {
                    "family": fam,
                    "from_state": "FAILING",
                    "to_state": "EXIT",
                    "trigger": "family invalidation confirmed",
                },
            ]
        )

        rec = rec_rows.get(fam, {})
        primary_obj = rec.get("primary_destructive_branch")
        secondary_obj = rec.get("secondary_destructive_branch")

        for obj in [primary_obj, secondary_obj]:
            if not obj:
                continue
            branch_name = str(obj.get("exit_branch", "UNKNOWN"))
            survival_rows.append(
                {
                    "family": fam,
                    "branch": branch_name,
                    "treatment": "REMOVED" if "FAST_FAILURE" in branch_name else "FAMILY_OVERRIDE",
                    "reason": "high branch_removal_delta_pph in TASK-009",
                }
            )

        protective = rec.get("neutral_or_protective_branches", [])
        for obj in protective[:2]:
            branch_name = str(obj.get("exit_branch", "UNKNOWN"))
            survival_rows.append(
                {
                    "family": fam,
                    "branch": branch_name,
                    "treatment": "RETAINED",
                    "reason": "low or negative branch_removal_delta_pph",
                }
            )

        impl_rows.append(
            {
                "family": fam,
                "current_code_reference": "aee_state_machine_v2.py::_eval_trade_baseline",
                "required_changes": [
                    "replace generic fast-failure trigger with family-specific INIT/PROBING expectations",
                    "apply family-specific stall and giveback policies",
                    "route invalidation through family behavior contracts rather than universal early-failure rules",
                ],
            }
        )

    # Global branches not explicitly mapped become diagnostic-only by default.
    mapped = {(r["family"], r["branch"]) for r in survival_rows}
    for fam in FAMILIES:
        for row in top_global_damage:
            branch = str(row.get("exit_branch", "UNKNOWN"))
            key = (fam, branch)
            if key in mapped:
                continue
            survival_rows.append(
                {
                    "family": fam,
                    "branch": branch,
                    "treatment": "DIAGNOSTIC_ONLY",
                    "reason": "not in family top destructive set; keep for observability",
                }
            )

    transition_table = {
        "generated_at": _iso_now(),
        "task_id": "TASK-010",
        "rows": transition_rows,
    }
    survival_map = {
        "generated_at": _iso_now(),
        "task_id": "TASK-010",
        "rows": survival_rows,
    }
    implementation_plan = {
        "generated_at": _iso_now(),
        "task_id": "TASK-010",
        "dependencies": [
            "TASK-009",
            "TASK-014"
        ],
        "rows": impl_rows,
        "global_steps": [
            "Introduce family router before early-failure ladder in aee_state_machine_v2.py",
            "Implement state progression INIT->PROBING->CONFIRMING->EXPANDING->EXHAUSTING->FAILING->EXIT per family",
            "Demote non-surviving generic branches to diagnostics-only logging",
            "Add per-family validation outputs against TASK-009 branch-damage recommendations",
        ],
    }

    (CONTROL / "aee_family_state_machine_v3_spec.json").write_text(json.dumps(v3_spec, indent=2) + "\n", encoding="utf-8")
    (CONTROL / "aee_family_state_transition_table.json").write_text(json.dumps(transition_table, indent=2) + "\n", encoding="utf-8")
    (CONTROL / "aee_branch_survival_map.json").write_text(json.dumps(survival_map, indent=2) + "\n", encoding="utf-8")
    (CONTROL / "aee_family_implementation_plan.json").write_text(json.dumps(implementation_plan, indent=2) + "\n", encoding="utf-8")

    print("wrote control/aee_family_state_machine_v3_spec.json")
    print("wrote control/aee_family_state_transition_table.json")
    print("wrote control/aee_branch_survival_map.json")
    print("wrote control/aee_family_implementation_plan.json")


if __name__ == "__main__":
    main()
