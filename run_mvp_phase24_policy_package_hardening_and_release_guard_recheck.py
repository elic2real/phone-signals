#!/usr/bin/env python3
"""Run MVP Phase 24 policy package hardening and release guard recheck (no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

PHASE23_PATH = Path("control/mvp_phase23_guarded_parallel_rerun_with_tiered_kill_gates.json")
PHASE18_PATH = Path("control/mvp_phase18_policy_package_proposal_paper_only.json")
OUTPUT_PATH = Path("control/mvp_phase24_policy_package_hardening_and_release_guard_recheck.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    phase23 = _load_json(PHASE23_PATH)
    phase18 = _load_json(PHASE18_PATH)

    decision = phase23.get("decision", {})
    promoted_variant = decision.get("promoted_variant")
    pair_results = phase23.get("results_by_pair", {})
    gate_cfg = phase23.get("gate_config", {})

    pair_guard_recheck: Dict[str, Any] = {}
    pair_selected_all_v3 = True
    pair_tiered_survival_all = True

    for pair in ["EUR_USD", "GBP_USD"]:
        selected_variant = (
            pair_results.get(pair, {})
            .get("selected_variant", {})
            .get("variant")
        )
        tiered_survives = bool(
            pair_results.get(pair, {})
            .get("variants", {})
            .get("V3", {})
            .get("passes_tiered_gates", False)
        )
        pair_selected_all_v3 = pair_selected_all_v3 and (selected_variant == "V3")
        pair_tiered_survival_all = pair_tiered_survival_all and tiered_survives

        pair_guard_recheck[pair] = {
            "selected_variant": selected_variant,
            "expected_variant": "V3",
            "v3_passes_tiered_gates": tiered_survives,
            "false_cut_rate_on_winners": (
                pair_results.get(pair, {})
                .get("variants", {})
                .get("V3", {})
                .get("false_cut_rate_on_winners")
            ),
            "tier1_max_false_cut_allowed": (
                gate_cfg.get("tier1", {})
                .get("max_false_cut_rate_on_winners")
            ),
            "weighted_delta": (
                pair_results.get(pair, {})
                .get("variants", {})
                .get("V3", {})
                .get("weighted_delta_by_family", {})
                .get("weighted_delta")
            ),
        }

    pass_conditions = {
        "phase23_dependency_passed": phase23.get("status") == "PASS",
        "phase23_verdict_promote": decision.get("verdict") == "PROMOTE",
        "phase23_promoted_variant_v3": promoted_variant == "V3",
        "pair_selected_variant_v3": pair_selected_all_v3,
        "pair_tiered_gate_survival": pair_tiered_survival_all,
        "cross_pair_survival_confirmed": bool(decision.get("overall_pass", False)),
        "no_tuning_applied": True,
        "scope_lock_preserved": phase23.get("scope_lock", {}).get("pairs") == ["EUR_USD", "GBP_USD"],
    }

    all_pass = all(pass_conditions.values())

    package_base = phase18.get("policy_package", {})
    hardened_package = {
        "candidate_package_id": "PKG_V3_M3_ANCHOR_HARDENED_P24",
        "base_package_id": package_base.get("candidate_package_id"),
        "source_phase23_variant": promoted_variant,
        "hardening_mode": "NO_TUNING_GUARD_LOCK",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
        },
        "lock_matrix": {
            "variant_lock": "V3",
            "tier1": gate_cfg.get("tier1", {}),
            "tier2": gate_cfg.get("tier2", {}),
            "slice_balance": gate_cfg.get("slice_balance", {}),
            "forbidden_changes": [
                "parameter tuning",
                "pair or session scope expansion",
                "live rollout before release-guard stage pass",
            ],
        },
        "release_guard_recheck": {
            "pair_checks": pair_guard_recheck,
            "cross_pair_survival": bool(decision.get("overall_pass", False)),
            "cross_pair_variant": promoted_variant,
            "release_action": "ALLOW_SHADOW_BENCHMARK_STAGE_ONLY",
            "live_promotion_allowed": False,
            "reason": "Package hardened and lock-held; proceed to benchmark shadow validation only.",
        },
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE24_POLICY_PACKAGE_HARDENING_AND_RELEASE_GUARD_RECHECK",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "PAPER_ONLY_HARDENING_AND_GUARD_RECHECK",
        },
        "dependency": {
            "phase23_status": phase23.get("status"),
            "phase23_path": str(PHASE23_PATH),
            "phase18_status": phase18.get("status"),
            "phase18_path": str(PHASE18_PATH),
        },
        "hardened_policy_package": hardened_package,
        "pass_conditions": pass_conditions,
        "decision": {
            "verdict": "PROMOTE" if all_pass else "HOLD",
            "overall_pass": all_pass,
            "reason": (
                "Policy package hardened with lock-held release guard recheck; ready for shadow benchmark stage."
                if all_pass
                else "Phase24 guard recheck failed; hold until dependency/survival conditions are restored."
            ),
        },
        "next_recommended_task": (
            "MVP_PHASE25_SHADOW_BENCHMARK_EXECUTION_AND_DRIFT_PROOF"
            if all_pass
            else "MVP_PHASE24B_GUARD_RECHECK_REMEDIATION_PAPER_ONLY"
        ),
    }
    report["status"] = "PASS" if all_pass else "FAIL"

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
