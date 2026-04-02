from __future__ import annotations

import json
import random
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List

WORKSPACE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKSPACE))

try:
    from github_repo import tick_generator as synthetic_ticks  # noqa: E402
except ImportError:
    import tick_generator as synthetic_ticks  # type: ignore[no-redef]  # noqa: E402
from tools.run_v2_entry_stack import _matching_survivor_routes, make_config, read_json, write_json  # noqa: E402
from tools.v2_phase1_physics_engine import build_phase1_stack, sanitize_ticks  # noqa: E402
from tools.v2_phase2_cluster_engine import assign_profile_to_cluster  # noqa: E402
from tools.v2_phase3_context_engine import build_context_snapshot  # noqa: E402
from tools.v2_phase4_trigger_engine import build_trigger_candidate  # noqa: E402
from tools.v2_phase5_evaluation_engine import evaluate_candidate  # noqa: E402
from tools.v2_tier1_truth_kernel import build_truth_kernel  # noqa: E402


CONTROL_DIR = WORKSPACE / "control" / "v2_engine"
PHASE2_DIR = CONTROL_DIR / "phase2"
TIER3_DIR = CONTROL_DIR / "tier3"

TRACK1_DOCTRINES = [
    "EXPANSION_RELEASE_LONG",
    "EXPANSION_RELEASE_SHORT",
    "PRESSURE_DRIVE_LONG",
    "PRESSURE_DRIVE_SHORT",
    "FAILED_PUSH_SHORT_REVERSAL_SCALP",
    "COILED_TRANSITION_SHORT",
]

GENERIC_ROUTE_MODES = {
    "GENERAL_CAPTURE",
}


def scenario_ticks(name: str, seed: int) -> List[Dict[str, Any]]:
    random.seed(seed)
    scenario_name = str(name or "").strip()
    legacy_aliases = {
        "chop_mean_reversion": "low_energy_range",
    }
    scenario_name = legacy_aliases.get(scenario_name, scenario_name)
    return synthetic_ticks.SCENARIO_REGISTRY[scenario_name]()


def _round(value: float) -> float:
    return round(float(value), 6)


def _annotate_truth_kernels(profiles: List[Dict[str, Any]], tier0_handoff_rows: List[Dict[str, Any]] | None = None) -> List[Dict[str, Any]]:
    handoff_by_profile_id = {
        str(row.get("profile_id", "") or ""): row
        for row in list(tier0_handoff_rows or [])
    }
    annotated: List[Dict[str, Any]] = []
    for profile in profiles:
        kernel = build_truth_kernel(
            profile,
            profiles,
            handoff_by_profile_id.get(str(profile.get("profile_id", "") or "")),
        )
        annotated.append(
            {
                **profile,
                "truth_kernel": kernel,
                "truth_kernel_signature": str(kernel.get("kernel_signature", "") or ""),
            }
        )
    return annotated


def _identity_binding_status(route_modes: List[str], survivor_rule_count: int) -> str:
    if survivor_rule_count <= 0:
        return "UNBOUND_NO_SURVIVOR_ROUTE"
    distinct_modes = {str(mode or "").upper() for mode in route_modes if str(mode or "").strip()}
    if not distinct_modes:
        return "UNBOUND_NO_SURVIVOR_ROUTE"
    if distinct_modes.issubset(GENERIC_ROUTE_MODES):
        return "GENERIC_SHARED_ONLY"
    return "DOCTRINE_LOCAL_IDENTITY_BOUND"


def _phase1_residue_status(doctrine_id: str, cluster_report: Dict[str, Any]) -> str:
    extraction_summary = cluster_report.get("tier1_extraction_summary", {}) or {}
    uncaptured = dict(extraction_summary.get("uncaptured_doctrine_states", {}) or extraction_summary.get("uncaptured_pattern_match_states", {}) or {})
    residue = int(uncaptured.get(doctrine_id, 0) or 0)
    return "RESIDUE_PRESENT" if residue > 0 else "CLEAR"


def _phase2_naming_status(cluster: Dict[str, Any], identity_binding_status: str) -> str:
    doctrine_id = str(cluster.get("doctrine_id", "") or "")
    doctrine_state = str(cluster.get("doctrine_state", cluster.get("pattern_match_state", "")) or "")
    if doctrine_id != doctrine_state:
        return "REQUIRES_RENAME_NORMALIZATION"
    if identity_binding_status == "GENERIC_SHARED_ONLY":
        return "REQUIRES_LOCAL_RULE_REBIND"
    return "NORMALIZED"


def _current_status(
    *,
    identity_binding_status: str,
    phase2_naming_status: str,
    candidate_count: int,
    ready_count: int,
    filled_count: int,
    abort_counter: Counter[str],
) -> str:
    if identity_binding_status == "UNBOUND_NO_SURVIVOR_ROUTE":
        return "RETIRE_AS_STRUCTURALLY_INVALID"
    if phase2_naming_status == "REQUIRES_RENAME_NORMALIZATION":
        return "REQUIRES_RENAME_NORMALIZATION"
    if candidate_count == 0:
        return "REQUIRES_LOCAL_RULE_REBIND"
    if ready_count > 0 or filled_count > 0:
        return "CLEANLY_INSTANTIATED"
    if abort_counter.get("zone_misaligned", 0) >= candidate_count:
        return "REQUIRES_LOCAL_RULE_REBIND"
    if abort_counter.get("insufficient_charge", 0) >= max(1, candidate_count // 2):
        return "STILL_ABORTS_ON_INSUFFICIENT_CHARGE"
    return "REQUIRES_LOCAL_RULE_REBIND"


def _next_action(current_status: str) -> str:
    if current_status == "CLEANLY_INSTANTIATED":
        return "PROMOTE_TO_TRACK2_OR_PHASE5_PROOF"
    if current_status == "REQUIRES_RENAME_NORMALIZATION":
        return "NORMALIZE_DOCTRINE_TO_RECOGNITION_GRAMMAR"
    if current_status == "REQUIRES_LOCAL_RULE_REBIND":
        return "REBINDS_ZONE_ROUTE_OR_ENTRY_SHAPE_LOCALLY"
    if current_status == "STILL_ABORTS_ON_INSUFFICIENT_CHARGE":
        return "LOWER_CHARGE_FLOOR_OR_RESHAPE_ENTRY_ENERGY_CONTRACT"
    return "REVIEW_FOR_RETIREMENT_OR_RECOMPOSITION"


def main() -> int:
    determinism = read_json(CONTROL_DIR / "v2_determinism_lock.json")
    config = make_config(determinism)
    base_seed = int(determinism["seed"])
    scenario_names = determinism["sampling_policy"]["fixed_phase1_scenarios"]
    cluster_report = read_json(PHASE2_DIR / "v2_phase2_cluster_report.json")
    clusters = list(cluster_report.get("clusters", []))
    cluster_lookup = {str(row.get("doctrine_id", "") or ""): row for row in clusters}

    scenario_state: Dict[str, Dict[str, Any]] = {}
    for idx, name in enumerate(scenario_names):
        raw_ticks = scenario_ticks(name, base_seed + idx)
        sanitized = sanitize_ticks(raw_ticks, config)
        ticks = [vars(tick) for tick in sanitized["ticks"]]
        phase1_stack = build_phase1_stack(sanitized["ticks"], config)
        profiles = [vars(profile) for profile in phase1_stack["profiles"]]
        scenario_state[name] = {
            "ticks": ticks,
            "profiles": _annotate_truth_kernels(profiles, phase1_stack["tier0_handoff_rows"]),
        }

    doctrine_rows: List[Dict[str, Any]] = []
    for doctrine_id in TRACK1_DOCTRINES:
        cluster = dict(cluster_lookup.get(doctrine_id, {}) or {})
        rules = list(cluster.get("tier1_expression_rules", []) or [])
        survivor_rules = [rule for rule in rules if bool(rule.get("survivor"))]
        route_modes = sorted({str(rule.get("route_operating_mode", "") or "") for rule in survivor_rules})
        identity_binding_status = _identity_binding_status(route_modes, len(survivor_rules))
        naming_status = _phase2_naming_status(cluster, identity_binding_status) if cluster else "REQUIRES_RENAME_NORMALIZATION"

        trigger_rows: List[Dict[str, Any]] = []
        filled_count = 0
        for scenario_name, state in scenario_state.items():
            profiles = state["profiles"]
            ticks = state["ticks"]
            profiles_by_anchor = {int(profile["anchor_index"]): profile for profile in profiles}
            for profile in profiles:
                cluster_id = assign_profile_to_cluster(profile, clusters)
                if cluster_id != doctrine_id:
                    continue
                matching_routes = _matching_survivor_routes(cluster, profile, {})
                if not matching_routes:
                    continue
                selected_route = matching_routes[0]
                context = build_context_snapshot(profile=profile, scenario_profiles=profiles, cluster=cluster)
                candidate = build_trigger_candidate(
                    profile=profile,
                    context=context,
                    cluster=cluster,
                    ticks=ticks,
                    profiles_by_anchor=profiles_by_anchor,
                    selected_route=selected_route,
                )
                trigger_rows.append({"scenario": scenario_name, **candidate})
                evaluation = evaluate_candidate(
                    candidate=candidate,
                    ticks=ticks,
                    commission_pips=float(config.commission_pips),
                    slippage_pips=float(config.slippage_pips),
                )
                if str(evaluation.get("status", "") or "").upper() == "FILLED":
                    filled_count += 1

        abort_counter = Counter(
            str(row.get("reason", "UNKNOWN") or "UNKNOWN")
            for row in trigger_rows
            if str(row.get("status", "") or "").upper() == "ABORTED"
        )
        ready_count = sum(1 for row in trigger_rows if str(row.get("status", "") or "").upper() == "READY")
        current_status = _current_status(
            identity_binding_status=identity_binding_status,
            phase2_naming_status=naming_status,
            candidate_count=len(trigger_rows),
            ready_count=ready_count,
            filled_count=filled_count,
            abort_counter=abort_counter,
        )

        doctrine_rows.append(
            {
                "doctrine_id": doctrine_id,
                "prior_failure_class": "INSTANTIATION",
                "current_status": current_status,
                "identity_binding_status": identity_binding_status,
                "phase1_residue_status": _phase1_residue_status(doctrine_id, cluster_report),
                "phase2_naming_status": naming_status,
                "phase4_abort_status": {
                    "candidate_count": len(trigger_rows),
                    "ready_count": ready_count,
                    "filled_count": filled_count,
                    "abort_reason_counts": dict(abort_counter),
                },
                "route_operating_modes": route_modes,
                "survivor_rule_count": len(survivor_rules),
                "runtime_status": str(cluster.get("doctrine_runtime_status", "MISSING") or "MISSING"),
                "next_action": _next_action(current_status),
            }
        )

    report = {
        "artifact_id": "V2_TRACK1_INSTANTIATION_PASS_REPORT",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "doctrine_count": len(doctrine_rows),
        "doctrines": doctrine_rows,
        "status_counts": dict(Counter(str(row["current_status"]) for row in doctrine_rows)),
        "notes": [
            "This report replays the Track 1 doctrines directly, including frozen research-only doctrines, to separate identity failures from runtime filtering.",
            "CLEANLY_INSTANTIATED means the doctrine now produces ready or filled trigger candidates under deterministic replay.",
            "REQUIRES_LOCAL_RULE_REBIND means the doctrine has a route identity but still fails on local zone or entry-shape binding.",
        ],
    }
    output_path = TIER3_DIR / "v2_track1_instantiation_pass_report.json"
    write_json(output_path, report)
    print(
        json.dumps(
            {
                "status": "PASS",
                "report": str(output_path),
                "status_counts": report["status_counts"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
