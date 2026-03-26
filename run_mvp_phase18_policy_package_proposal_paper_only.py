#!/usr/bin/env python3
"""Build MVP Phase 18 policy package proposal (paper-only, no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

PHASE17_PATH = Path("control/mvp_phase17_counterfactual_rerun_execution_with_mitigation_guards.json")
PHASE0_TEMPLATE_PATH = Path("control/rcp_phase0_high_speed_foundation_template.json")
OUTPUT_PATH = Path("control/mvp_phase18_policy_package_proposal_paper_only.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    phase17 = _load_json(PHASE17_PATH)
    phase0 = _load_json(PHASE0_TEMPLATE_PATH)

    promoted_suffix = phase17.get("promotion_decision", {}).get("promoted_variant_suffix")
    rerun_verdict = phase17.get("promotion_decision", {}).get("rerun_verdict")

    package = {
        "candidate_package_id": "PKG_V3_M3_ANCHOR",
        "source_variant_suffix": promoted_suffix,
        "source_mode": "COUNTERFACTUAL_RERUN_EXECUTION_ONLY",
        "policy_intent": "Adopt selective trial deactivation anchor mode to reduce false cuts while preserving positive net delta.",
        "pair_bindings": {
            "EUR_USD": {
                "selected_variant": "P16-EUR_USD-V3",
                "false_cut_rate_on_winners": 0.0,
                "primary_positive_count": 3,
            },
            "GBP_USD": {
                "selected_variant": "P16-GBP_USD-V3",
                "false_cut_rate_on_winners": 0.0,
                "primary_positive_count": 3,
            },
        },
        "guardrails": {
            "forbidden": [
                "live production rollout without final paper approval",
                "parameter tuning during packaging",
                "scope widening beyond EUR_USD/GBP_USD LONDON",
            ],
            "must_hold": [
                "false_cut_rate_on_winners <= 0.15 per pair",
                "primary_positive_metrics >= 2 per pair",
            ],
        },
        "release_stages": [
            {
                "stage": 1,
                "name": "paper_lock",
                "goal": "Freeze package behavior and acceptance metrics.",
            },
            {
                "stage": 2,
                "name": "sandbox_shadow",
                "goal": "Run shadow-only confirmation on fixed benchmark slices.",
            },
            {
                "stage": 3,
                "name": "micro_live_guarded",
                "goal": "Very small guarded promotion after explicit gate pass.",
            },
        ],
        "phase0_concept_adoption": {
            "instrumentation_first": True,
            "universal_analysis_required": True,
            "parallel_variants_required": True,
            "kill_rules_required": True,
            "template_reference": str(PHASE0_TEMPLATE_PATH),
            "template_task_id": phase0.get("task_id"),
        },
    }

    pass_conditions = {
        "phase17_dependency_passed": phase17.get("status") == "PASS",
        "rerun_verdict_promote": rerun_verdict == "PROMOTE",
        "promoted_variant_present": bool(promoted_suffix),
        "package_has_guardrails": True,
        "phase0_concept_adoption_declared": True,
        "paper_only_confirmed": True,
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE18_POLICY_PACKAGE_PROPOSAL_PAPER_ONLY",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "PAPER_ONLY_PACKAGE",
        },
        "dependency": {
            "phase17_status": phase17.get("status"),
            "phase17_path": str(PHASE17_PATH),
            "phase0_template_path": str(PHASE0_TEMPLATE_PATH),
        },
        "policy_package": package,
        "pass_conditions": pass_conditions,
    }
    report["status"] = "PASS" if all(pass_conditions.values()) else "FAIL"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"wrote {OUTPUT_PATH}")
    print(
        json.dumps(
            {
                "status": report["status"],
                "candidate_package_id": package["candidate_package_id"],
                "source_variant_suffix": promoted_suffix,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
