from __future__ import annotations

import argparse
import os
import json
import random
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Dict, List

WORKSPACE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(WORKSPACE))

try:
    from github_repo import tick_generator as synthetic_ticks  # noqa: E402
except ImportError:
    import tick_generator as synthetic_ticks  # type: ignore[no-redef]  # noqa: E402
from tools.v2_phase1_physics_engine import Phase1Config, build_phase1_stack, sanitize_ticks, summarize_profiles  # noqa: E402
from tools.v2_phase2_cluster_engine import assign_profile_to_cluster, fit_phase2_clusters  # noqa: E402
from tools.v2_phase2_extraction_engine import _route_matches_profile, evaluate_phase2_doctrines  # noqa: E402
from tools.v2_phase3_context_engine import build_context_snapshot  # noqa: E402
from tools.v2_phase4_trigger_engine import build_trigger_candidate, get_doctrine_option_lock_plan, get_track2_pocket_whitelist_plan  # noqa: E402
from tools.v2_phase5_evaluation_engine import evaluate_candidate, summarize_result_groups, summarize_strategy_results  # noqa: E402
from tools.v2_tier1_truth_kernel import build_truth_kernel  # noqa: E402


CONTROL_DIR = WORKSPACE / "control" / "v2_engine"
PHASE1_DIR = CONTROL_DIR / "phase1"
PHASE2_DIR = CONTROL_DIR / "phase2"
PHASE3_DIR = CONTROL_DIR / "phase3"
PHASE4_DIR = CONTROL_DIR / "phase4"
PHASE5_DIR = CONTROL_DIR / "phase5"


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _load_blitz_config() -> Dict[str, Any]:
    path = str(os.environ.get("V2_BLITZ_CONFIG", "") or "").strip()
    if not path:
        return {}
    config_path = Path(path)
    if not config_path.exists():
        return {}
    return read_json(config_path)


def make_config(lock: Dict[str, Any]) -> Phase1Config:
    cfg = lock["config_lock"]
    return Phase1Config(**cfg)


def sample_profiles(rows: List[Dict[str, Any]], stride: int, limit: int) -> List[Dict[str, Any]]:
    sampled: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        if idx % max(stride, 1) == 0:
            sampled.append(row)
        if len(sampled) >= limit:
            break
    return sampled


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


def scenario_ticks(name: str, seed: int) -> List[Dict[str, Any]]:
    scenario_name = str(name or "").strip()
    legacy_aliases = {
        "chop_mean_reversion": "low_energy_range",
    }
    scenario_name = legacy_aliases.get(scenario_name, scenario_name)
    state = random.getstate()
    try:
        random.seed(seed)
        return synthetic_ticks.SCENARIO_REGISTRY[scenario_name]()
    finally:
        random.setstate(state)


def _route_blitz_bias(cluster: Dict[str, Any], rule: Dict[str, Any], blitz_config: Dict[str, Any]) -> float:
    route_cfg = (
        blitz_config.get("route_selection", {})
        .get(str(cluster.get("doctrine_id", "") or ""), {})
    )
    route_mode = str(rule.get("route_operating_mode", "") or "")
    variant_id = str(rule.get("selected_variant_id", rule.get("selected_variant", {}).get("variant_id", "")) or "")
    return float(route_cfg.get("mode_bias", {}).get(route_mode, 0.0) or 0.0) + float(
        route_cfg.get("variant_bias", {}).get(variant_id, 0.0) or 0.0
    )


def _matching_survivor_routes(cluster: Dict[str, Any], profile: Dict[str, Any], blitz_config: Dict[str, Any] | None = None) -> List[Dict[str, Any]]:
    expression_id = str(profile.get("distance_family_id", "") or "")
    blitz_config = blitz_config or {}
    matching_routes = [
        rule
        for rule in cluster.get("tier1_expression_rules", [])
        if bool(rule.get("survivor"))
        and str(rule.get("expression_id", "") or "") == expression_id
        and _route_matches_profile(profile, rule)
    ]
    return sorted(
        matching_routes,
        key=lambda rule: (
            -int(rule.get("specificity", 0) or 0),
            -_route_blitz_bias(cluster, rule, blitz_config),
            -float(rule.get("expectancy_pips", 0.0) or 0.0),
            -float(rule.get("net_pnl_pips", 0.0) or 0.0),
            -int(rule.get("trade_count", 0) or 0),
            str(rule.get("route_id", "") or ""),
        ),
    )


def _doctrine_operating_tier(doctrine_id: str) -> str:
    doctrine_id = str(doctrine_id or "").upper()
    backbone = {
        "TRANSITION_RELEASE_SHORT_STANDARD",
        "FLOW_DRIFT_SHORT",
        "FLOW_DRIFT_LONG",
        "COMPRESSION_PRESSURE_LIFT_LONG",
    }
    fragile = {
        "OSCILLATION_EDGE_SHORT_SCALP",
        "OSCILLATION_EDGE_LONG_SCALP",
        "OSCILLATION_PRESSURE_BUILD_LONG",
        "PRESSURE_DRIVE_SHORT",
        "FAILED_PUSH_SHORT_REVERSAL_SCALP",
    }
    retired = {
        "COILED_TRANSITION_SHORT",
        "FAILED_PUSH_LONG_REVERSAL_SCALP",
    }
    frozen = {
        "TRANSITION_RELEASE_LONG_STANDARD",
        "OSCILLATION_PRESSURE_BUILD_SHORT",
        "COMPRESSION_PRESSURE_DROP_SHORT",
    }
    if doctrine_id in backbone:
        return "BACKBONE"
    if doctrine_id in fragile:
        return "FRAGILE"
    if doctrine_id in retired:
        return "RETIRED"
    if doctrine_id in frozen:
        return "FROZEN"
    return "UNCLASSIFIED"


def _doctrine_runtime_contract(doctrine_id: str) -> Dict[str, Any]:
    doctrine_id = str(doctrine_id or "").upper()
    contract: Dict[str, Any] = {
        "required_zone": "ANY",
        "trigger_demand": "GENERIC_DIRECTIONAL_CONFIRMATION",
        "allowed_route_classes": [],
        "invalid_route_classes": [],
        "extraction_intent": "GENERAL_CAPTURE",
    }
    if "TRANSITION_RELEASE_SHORT" in doctrine_id:
        contract.update(
            {
                "required_zone": "NEAR_FLOOR",
                "trigger_demand": "IGNITION_RELEASE_OR_RELEASE_CONFIRM",
                "allowed_route_classes": ["TRANSITION_FRONT_RUN", "TRANSITION_CONFIRM", "CAPTURE_SCALP", "CAPTURE_NEAR"],
                "invalid_route_classes": [],
                "extraction_intent": "FAST_RELEASE_THEN_SELECTIVE_EXTENSION",
            }
        )
    elif "TRANSITION_RELEASE_LONG" in doctrine_id:
        contract.update(
            {
                "required_zone": "NEAR_CEILING",
                "trigger_demand": "LONG_RELEASE_CONFIRMATION",
                "allowed_route_classes": ["CAPTURE_BALANCED", "CAPTURE_FULL", "TRANSITION_LATE"],
                "invalid_route_classes": ["TRANSITION_RUNNER", "IGNITION_CAPTURE_EXTENSION", "IGNITION_CAPTURE_NEAR"],
                "extraction_intent": "LONG_RELEASE_CAPTURE_UNDER_REWRITE",
            }
        )
    elif "FLOW_DRIFT_SHORT" in doctrine_id:
        contract.update(
            {
                "required_zone": "MID_ZONE",
                "trigger_demand": "DRIFT_REACCEL_OR_DRIFT_CONFIRM",
                "allowed_route_classes": ["DRIFT_REACCEL", "DRIFT_CONFIRM", "CAPTURE_SCALP", "CAPTURE_NEAR"],
                "invalid_route_classes": ["DRIFT_SCALP_ON_DORMANT_OR_PATTERN_HARVEST"],
                "extraction_intent": "MID_DRIFT_REACCEL_CAPTURE",
            }
        )
    elif "FLOW_DRIFT_LONG" in doctrine_id:
        contract.update(
            {
                "required_zone": "MID_ZONE",
                "trigger_demand": "LONG_DRIFT_REACCEL_OR_IGNITION_ACCEPTANCE",
                "allowed_route_classes": ["DRIFT_REACCEL", "DRIFT_CONFIRM", "CAPTURE_BALANCED", "CAPTURE_EXTENSION"],
                "invalid_route_classes": ["DRIFT_RIDE"],
                "extraction_intent": "UNDER_ARMED_LONG_DRIFT_CAPTURE",
            }
        )
    elif "COMPRESSION_PRESSURE_LIFT" in doctrine_id:
        contract.update(
            {
                "required_zone": "NEAR_CEILING",
                "trigger_demand": "COMPRESSION_PRESSURE_PERSISTENCE",
                "allowed_route_classes": ["CAPTURE_FULL", "COMPRESSION_SWING", "CAPTURE_EXTENSION", "PRESSURE_EXTENSION"],
                "invalid_route_classes": [],
                "extraction_intent": "HIGH_VOLUME_PRESSURE_LIFT_CAPTURE",
            }
        )
    elif "COMPRESSION_PRESSURE_DROP" in doctrine_id:
        contract.update(
            {
                "required_zone": "NEAR_FLOOR",
                "trigger_demand": "COMPRESSION_PRESSURE_PERSISTENCE",
                "allowed_route_classes": ["CAPTURE_FULL", "CAPTURE_BALANCED", "CAPTURE_EXTENSION"],
                "invalid_route_classes": ["CAPTURE_EXTENSION_ON_DRIFT", "CAPTURE_EXTENSION_ON_PATTERN_HARVEST"],
                "extraction_intent": "SHORT_PRESSURE_DROP_CAPTURE_UNDER_REVIEW",
            }
        )
    elif "OSCILLATION_EDGE_SHORT" in doctrine_id:
        contract.update(
            {
                "required_zone": "NEAR_CEILING",
                "trigger_demand": "HARD_REJECTION_CONFIRMATION",
                "allowed_route_classes": ["EDGE_SCALP", "EDGE_CONFIRM", "EDGE_RELOAD", "CAPTURE_NEAR"],
                "invalid_route_classes": ["BROAD_EXTENSION_BEHAVIOR"],
                "extraction_intent": "FAST_OSCILLATION_REJECTION_SCALP",
            }
        )
    elif "OSCILLATION_EDGE_LONG" in doctrine_id:
        contract.update(
            {
                "required_zone": "NEAR_FLOOR",
                "trigger_demand": "FLOOR_REBOUND_CONFIRMATION",
                "allowed_route_classes": ["EDGE_SCALP", "EDGE_CONFIRM", "EDGE_RELOAD", "CAPTURE_BALANCED"],
                "invalid_route_classes": ["CEILING_LONG_ROUTE_FAMILIES"],
                "extraction_intent": "SELECTIVE_OSCILLATION_REBOUND_SCALP",
            }
        )
    elif "OSCILLATION_PRESSURE_BUILD_LONG" in doctrine_id:
        contract.update(
            {
                "required_zone": "NEAR_CEILING_OR_MID_ZONE",
                "trigger_demand": "PRESSURE_PERSISTENCE_PLUS_PATH_PROOF",
                "allowed_route_classes": ["CAPTURE_FULL", "CAPTURE_BALANCED", "CAPTURE_NEAR", "PRESSURE_BUILD_DRIVE"],
                "invalid_route_classes": ["PRESSURE_SWING_ON_PRESSURED_OR_DRIFT_OR_PATTERN_HARVEST", "PRESSURED_CAPTURE_EXTENSION"],
                "extraction_intent": "FRAGILE_PRESSURE_BUILD_CAPTURE",
            }
        )
    elif "OSCILLATION_PRESSURE_BUILD_SHORT" in doctrine_id:
        contract.update(
            {
                "required_zone": "NEAR_FLOOR_OR_MID_ZONE",
                "trigger_demand": "PRESSURE_PERSISTENCE_PLUS_PATH_PROOF",
                "allowed_route_classes": ["CAPTURE_FULL", "CAPTURE_BALANCED", "CAPTURE_EXTENSION"],
                "invalid_route_classes": ["OVER_BROAD_SWING_AND_EXTENSION_BEHAVIOR"],
                "extraction_intent": "SHORT_PRESSURE_BUILD_UNDER_REWRITE",
            }
        )
    elif "FAILED_PUSH_" in doctrine_id:
        contract.update(
            {
                "required_zone": "MID_ZONE",
                "trigger_demand": "REVERSAL_CONFIRMATION",
                "allowed_route_classes": ["REVERSAL_SNAP", "REVERSAL_CONFIRM", "CAPTURE_BALANCED"],
                "invalid_route_classes": ["MIXED_FAMILY_SURFACES"],
                "extraction_intent": "FAILED_PUSH_REVERSAL_SCALP_CAPTURE",
            }
        )
    elif "PRESSURE_DRIVE" in doctrine_id:
        contract.update(
            {
                "required_zone": "MID_ZONE",
                "trigger_demand": "PRESSURE_CONTINUATION_CONFIRMATION",
                "allowed_route_classes": ["PRESSURE_BREAK", "PRESSURE_HOLD", "PRESSURE_FRONT_RUN"],
                "invalid_route_classes": ["OVER_BROAD_MID_ZONE_ASSIGNMENT"],
                "extraction_intent": "PRESSURE_DRIVE_EDGE_TO_MID_CONTINUATION",
            }
        )
    return contract


def _phase5_gate_passed(row: Dict[str, Any], gate: Dict[str, Any]) -> bool:
    return (
        int(row.get("trade_count", 0) or 0) >= int(gate["minimum_trade_count"])
        and float(row.get("win_rate", 0.0) or 0.0) >= float(gate["minimum_win_rate"])
        and float(row.get("expectancy_pips", 0.0) or 0.0) >= float(gate["minimum_expectancy_pips"])
        and float(row.get("net_pnl_pips", 0.0) or 0.0) >= float(gate["minimum_net_pnl_pips"])
    )


def _phase2_survivor_gate_passed(
    row: Dict[str, Any],
    extraction_summary: Dict[str, Any],
    selected_expression_ids: List[str],
    gate: Dict[str, Any],
    blitz_config: Dict[str, Any],
) -> bool:
    doctrine_id = str(row.get("doctrine_id", "") or "")
    instantiation_lane_doctrines = {
        "COMPRESSION_RELEASE_LONG",
        "COMPRESSION_RELEASE_SHORT",
        "PRESSURE_DRIVE_LONG",
        "PRESSURE_DRIVE_SHORT",
        "EXPANSION_RELEASE_LONG",
        "EXPANSION_RELEASE_SHORT",
        "COILED_COMPRESSION_LONG",
        "COILED_COMPRESSION_SHORT",
        "COILED_TRANSITION_SHORT",
    }
    default_pass = (
        int(row["cluster_size"]) >= int(gate["minimum_cluster_size"])
        and int(extraction_summary["trade_count"]) >= int(gate["minimum_trade_count"])
        and float(extraction_summary["win_rate"]) >= float(gate["minimum_win_rate"])
        and float(extraction_summary["expectancy_pips"]) >= float(gate["minimum_expectancy_pips"])
        and float(extraction_summary["net_pnl_pips"]) >= float(gate["minimum_net_pnl_pips"])
        and len(selected_expression_ids) > 0
    )
    override = blitz_config.get("phase2_survivor_override", {}).get(doctrine_id, {})
    if not override:
        return default_pass

    if int(row["cluster_size"]) < int(override.get("minimum_cluster_size", gate["minimum_cluster_size"])):
        return False
    if int(extraction_summary["trade_count"]) < int(override.get("minimum_trade_count", gate["minimum_trade_count"])):
        return False
    if len(selected_expression_ids) < int(override.get("minimum_selected_expression_count", 1)):
        return False

    positive_routes = [
        rule
        for rule in row.get("tier1_expression_rules", [])
        if bool(rule.get("survivor"))
        and float(rule.get("expectancy_pips", 0.0) or 0.0) > float(override.get("minimum_positive_route_expectancy_pips", 0.0))
        and int(rule.get("trade_count", 0) or 0) >= int(override.get("minimum_positive_route_trade_count", 1))
    ]
    if int(override.get("minimum_positive_route_count", 0) or 0) > len(positive_routes):
        return False

    if doctrine_id in instantiation_lane_doctrines:
        instantiation_routes = [
            rule
            for rule in row.get("tier1_expression_rules", [])
            if bool(rule.get("survivor"))
            and bool(rule.get("instantiation_lane"))
            and float(rule.get("expectancy_pips", 0.0) or 0.0) > 0.0
            and float(rule.get("net_pnl_pips", 0.0) or 0.0) > 0.0
        ]
        relaxed_instantiation_pass = (
            int(row["cluster_size"]) >= max(3, int(gate["minimum_cluster_size"]) - 1)
            and int(extraction_summary["trade_count"]) >= int(gate["minimum_trade_count"])
            and float(extraction_summary["expectancy_pips"]) >= 0.01
            and float(extraction_summary["net_pnl_pips"]) >= 0.01
            and float(extraction_summary["win_rate"]) >= 0.33
            and len(selected_expression_ids) > 0
            and (len(positive_routes) > 0 or len(instantiation_routes) > 0)
        )
        if relaxed_instantiation_pass:
            return True

    return default_pass or bool(override.get("allow_relaxed_fragile_scalp_lane")) and bool(positive_routes)


def _profile_allowed_by_blitz_filter(
    *,
    profile: Dict[str, Any],
    cluster: Dict[str, Any],
    context: Dict[str, Any],
    blitz_config: Dict[str, Any],
) -> bool:
    doctrine_id = str(cluster.get("doctrine_id", "") or "")
    doctrine_filter = blitz_config.get("regime_filter", {}).get(doctrine_id, {})
    if not doctrine_filter:
        return True
    if str(context.get("regime_state", "") or "") in set(doctrine_filter.get("exclude_regime_states", [])):
        return False
    volatility_percentile = float(context.get("volatility_percentile", 0.0) or 0.0)
    if volatility_percentile > float(doctrine_filter.get("max_volatility_percentile", 1.0) or 1.0):
        return False
    velocity_now = abs(float(profile.get("velocity_pips_per_sec", 0.0) or 0.0))
    if velocity_now > float(doctrine_filter.get("max_anchor_velocity_pips_per_sec", 1e9) or 1e9):
        return False
    return True


def _enrich_phase5_summaries(rows: List[Dict[str, Any]], gate: Dict[str, Any]) -> List[Dict[str, Any]]:
    enriched: List[Dict[str, Any]] = []
    for row in rows:
        gate_passed = _phase5_gate_passed(row, gate)
        enriched.append(
            {
                **row,
                "base_viable": bool(row.get("viable")),
                "viable": gate_passed,
                "phase5_gate_passed": gate_passed,
            }
        )
    return enriched


def _build_registry_promotion_review(
    strategy_rows: List[Dict[str, Any]],
    variant_rows: List[Dict[str, Any]],
    selected_strategy_ids: List[str],
) -> Dict[str, Any]:
    selected = set(selected_strategy_ids)
    review_rows: List[Dict[str, Any]] = []
    for row in strategy_rows:
        strategy_id = str(row.get("strategy_id", "") or "")
        if not strategy_id or strategy_id in selected or not bool(row.get("phase5_gate_passed")):
            continue
        passing_variants = [
            variant
            for variant in variant_rows
            if str(variant.get("strategy_id", "") or "") == strategy_id and bool(variant.get("phase5_gate_passed"))
        ]
        passing_variants.sort(
            key=lambda variant: (
                -float(variant.get("net_pnl_pips", 0.0) or 0.0),
                -int(variant.get("trade_count", 0) or 0),
                str(variant.get("tier1_route_id", "") or ""),
            )
        )
        best_variant = passing_variants[0] if passing_variants else None
        route_id = str((best_variant or {}).get("tier1_route_id", "") or "")
        variant_id = str((best_variant or {}).get("tier1_variant_id", "") or "")
        best_trade_count = int((best_variant or {}).get("trade_count", 0) or 0)
        concentrated_window_share = round(best_trade_count / max(int(row.get("trade_count", 0) or 0), 1), 6)
        clean_route_identity = bool(best_variant) and "GENERAL" not in route_id and variant_id not in {
            "CAPTURE_FULL",
            "CAPTURE_BALANCED",
            "CAPTURE_EXTENSION",
        }
        isolated_window = concentrated_window_share >= 0.7
        meaningful_sample = best_trade_count >= 5
        promotion_ready = clean_route_identity and isolated_window and meaningful_sample
        review_rows.append(
            {
                "strategy_id": strategy_id,
                "current_registry_status": "NOT_SELECTED",
                "clean_route_identity": clean_route_identity,
                "isolated_window": isolated_window,
                "meaningful_sample": meaningful_sample,
                "promotion_ready": promotion_ready,
                "best_passing_variant": best_variant,
                "concentrated_window_share": concentrated_window_share,
                "next_action": (
                    "PROMOTE_TO_ENTRY_REGISTRY"
                    if promotion_ready
                    else (
                        "TRACK_2_ISOLATE_WINDOW"
                        if passing_variants
                        else "DO_NOT_PROMOTE"
                    )
                ),
            }
        )
    return {
        "artifact_id": "V2_REGISTRY_PROMOTION_REVIEW",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "selected_strategy_ids": selected_strategy_ids,
        "review": review_rows,
    }


def _build_phase4_variant_fire_report(trigger_rows: List[Dict[str, Any]], performance_gate: Dict[str, Any]) -> Dict[str, Any]:
    grouped: Dict[tuple[str, str, str], List[Dict[str, Any]]] = {}
    for row in trigger_rows:
        key = (
            str(row.get("strategy_id", "") or ""),
            str(row.get("tier1_variant_id", "") or ""),
            str(row.get("tier1_route_id", "") or ""),
        )
        if not any(key):
            continue
        grouped.setdefault(key, []).append(row)

    variant_rows: List[Dict[str, Any]] = []
    for (strategy_id, variant_id, route_id), rows in grouped.items():
        ready_count = sum(1 for row in rows if row.get("status") == "READY")
        aborted_rows = [row for row in rows if row.get("status") == "ABORTED"]
        invalid_rows = [row for row in rows if row.get("status") == "INVALID"]
        abort_reason_counts: Dict[str, int] = {}
        for row in aborted_rows:
            reason = str(row.get("reason", "UNKNOWN") or "UNKNOWN")
            abort_reason_counts[reason] = abort_reason_counts.get(reason, 0) + 1
        invalid_reason_counts: Dict[str, int] = {}
        for row in invalid_rows:
            reason = str(row.get("reason", "UNKNOWN") or "UNKNOWN")
            invalid_reason_counts[reason] = invalid_reason_counts.get(reason, 0) + 1
        variant_rows.append(
            {
                "strategy_id": strategy_id,
                "tier1_variant_id": variant_id,
                "tier1_route_id": route_id,
                "result_count": len(rows),
                "ready_count": ready_count,
                "aborted_count": len(aborted_rows),
                "invalid_count": len(invalid_rows),
                "ready_share": round(ready_count / max(len(rows), 1), 6),
                "abort_reason_counts": abort_reason_counts,
                "invalid_reason_counts": invalid_reason_counts,
            }
        )

    variant_rows.sort(
        key=lambda row: (
            row["strategy_id"],
            -float(row["ready_share"]),
            -int(row["ready_count"]),
            int(row["aborted_count"]),
            row["tier1_variant_id"],
            row["tier1_route_id"],
        )
    )
    return {
        "artifact_id": "V2_PHASE4_VARIANT_FIRE_REPORT",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "performance_gate": performance_gate,
        "variant_routes": variant_rows,
    }


def _build_phase4_adjustment_option_report(trigger_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    grouped: Dict[tuple[str, str, str], List[Dict[str, Any]]] = {}
    ladder_lookup: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
    for row in trigger_rows:
        strategy_id = str(row.get("strategy_id", "") or "")
        route_mode = str(row.get("tier1_route_operating_mode", "") or "")
        option_id = str(row.get("selected_adjustment_option_id", "") or "")
        if strategy_id and route_mode and option_id:
            grouped.setdefault((strategy_id, route_mode, option_id), []).append(row)
        ladder_key = (strategy_id, route_mode)
        ladder = row.get("adjustment_option_ladder")
        if strategy_id and route_mode and isinstance(ladder, list) and ladder_key not in ladder_lookup:
            ladder_lookup[ladder_key] = ladder

    rows: List[Dict[str, Any]] = []
    for (strategy_id, route_mode, option_id), items in grouped.items():
        ready_count = sum(1 for row in items if row.get("status") == "READY")
        rows.append(
            {
                "strategy_id": strategy_id,
                "route_operating_mode": route_mode,
                "selected_adjustment_option_id": option_id,
                "result_count": len(items),
                "ready_count": ready_count,
                "aborted_count": sum(1 for row in items if row.get("status") == "ABORTED"),
                "ready_share": round(ready_count / max(len(items), 1), 6),
            }
        )
    rows.sort(key=lambda row: (row["strategy_id"], row["route_operating_mode"], -row["ready_share"], row["selected_adjustment_option_id"]))

    ladders = [
        {
            "strategy_id": strategy_id,
            "route_operating_mode": route_mode,
            "option_ladder": ladder,
        }
        for (strategy_id, route_mode), ladder in sorted(ladder_lookup.items())
    ]
    return {
        "artifact_id": "V2_PHASE4_ADJUSTMENT_OPTION_REPORT",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "option_routes": rows,
        "option_ladders": ladders,
    }


def _build_strategy_baseline_comparison(
    baseline_rows: Dict[str, Dict[str, Any]],
    current_rows: Dict[str, Dict[str, Any]],
    baseline_report: str,
    current_report: str,
) -> Dict[str, Any]:
    strategy_ids = sorted(set(baseline_rows) | set(current_rows))
    comparisons: List[Dict[str, Any]] = []
    for strategy_id in strategy_ids:
        before = baseline_rows.get(strategy_id, {})
        after = current_rows.get(strategy_id, {})
        comparisons.append(
            {
                "strategy_id": strategy_id,
                "before": before,
                "after": after,
                "delta": {
                    "trade_count": int(after.get("trade_count", 0) or 0) - int(before.get("trade_count", 0) or 0),
                    "aborted_count": int(after.get("aborted_count", 0) or 0) - int(before.get("aborted_count", 0) or 0),
                    "win_rate": round(float(after.get("win_rate", 0.0) or 0.0) - float(before.get("win_rate", 0.0) or 0.0), 6),
                    "expectancy_pips": round(float(after.get("expectancy_pips", 0.0) or 0.0) - float(before.get("expectancy_pips", 0.0) or 0.0), 6),
                    "net_pnl_pips": round(float(after.get("net_pnl_pips", 0.0) or 0.0) - float(before.get("net_pnl_pips", 0.0) or 0.0), 6),
                    "phase5_gate_passed": bool(after.get("phase5_gate_passed")) != bool(before.get("phase5_gate_passed")),
                },
            }
        )
    return {
        "artifact_id": "V2_PHASE5_STRATEGY_BEFORE_AFTER_COMPARISON",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "baseline_report": baseline_report,
        "current_report": current_report,
        "strategy_comparisons": comparisons,
    }


def _build_phase4_option_lock_plan() -> Dict[str, Any]:
    plan = get_doctrine_option_lock_plan()
    return {
        "artifact_id": "V2_PHASE4_OPTION_LOCK_PLAN",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "policy": "MAX_2_LIVE_PLUS_MAX_1_PROBATIONARY_PER_DOCTRINE",
        "doctrines": [
            {
                "doctrine_id": doctrine_id,
                "live_options": spec.get("live", []),
                "probationary_options": spec.get("probationary", []),
                "notes": spec.get("notes", []),
            }
            for doctrine_id, spec in sorted(plan.items())
        ],
    }


def _build_phase4_option_prune_report(adjustment_report: Dict[str, Any]) -> Dict[str, Any]:
    plan = get_doctrine_option_lock_plan()
    observed_lookup = {
        (
            str(row.get("strategy_id", "") or ""),
            str(row.get("route_operating_mode", "") or ""),
            str(row.get("selected_adjustment_option_id", "") or ""),
        ): row
        for row in adjustment_report.get("option_routes", [])
    }
    rows: List[Dict[str, Any]] = []
    for doctrine_id, ladder_spec in plan.items():
        live_keys = {
            (str(item.get("route_operating_mode", "") or ""), str(item.get("option_id", "") or ""))
            for item in ladder_spec.get("live", [])
        }
        probation_keys = {
            (str(item.get("route_operating_mode", "") or ""), str(item.get("option_id", "") or ""))
            for item in ladder_spec.get("probationary", [])
        }
        ladder_rows = [
            row
            for row in adjustment_report.get("option_ladders", [])
            if str(row.get("strategy_id", "") or "") == doctrine_id
        ]
        for ladder_row in ladder_rows:
            route_mode = str(ladder_row.get("route_operating_mode", "") or "")
            for option in ladder_row.get("option_ladder", []):
                option_id = str(option.get("option_id", "") or "")
                key = (doctrine_id, route_mode, option_id)
                if (route_mode, option_id) in live_keys:
                    status = "LIVE"
                elif (route_mode, option_id) in probation_keys:
                    status = "PROBATIONARY"
                else:
                    status = "PRUNED"
                observed = observed_lookup.get(key, {})
                rows.append(
                    {
                        "doctrine_id": doctrine_id,
                        "route_operating_mode": route_mode,
                        "option_id": option_id,
                        "status": status,
                        "ready_count": int(observed.get("ready_count", 0) or 0),
                        "aborted_count": int(observed.get("aborted_count", 0) or 0),
                        "ready_share": float(observed.get("ready_share", 0.0) or 0.0),
                    }
                )
    rows.sort(key=lambda row: (row["doctrine_id"], row["route_operating_mode"], row["status"], row["option_id"]))
    return {
        "artifact_id": "V2_PHASE4_OPTION_PRUNE_REPORT",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "rows": rows,
    }


def _build_phase5_doctrine_option_rerun_summary(
    comparison_report: Dict[str, Any],
    lock_plan_report: Dict[str, Any],
) -> Dict[str, Any]:
    lock_lookup = {row["doctrine_id"]: row for row in lock_plan_report.get("doctrines", [])}
    rows: List[Dict[str, Any]] = []
    for comparison in comparison_report.get("strategy_comparisons", []):
        strategy_id = str(comparison.get("strategy_id", "") or "")
        rows.append(
            {
                "strategy_id": strategy_id,
                "live_options": lock_lookup.get(strategy_id, {}).get("live_options", []),
                "probationary_options": lock_lookup.get(strategy_id, {}).get("probationary_options", []),
                "delta": comparison.get("delta", {}),
                "before_phase5_gate_passed": bool(comparison.get("before", {}).get("phase5_gate_passed")),
                "after_phase5_gate_passed": bool(comparison.get("after", {}).get("phase5_gate_passed")),
            }
        )
    rows.sort(key=lambda row: row["strategy_id"])
    return {
        "artifact_id": "V2_PHASE5_DOCTRINE_OPTION_RERUN_SUMMARY",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "rows": rows,
    }


def _build_phase4_upstream_leak_trace(cluster_report: Dict[str, Any], context_report: Dict[str, Any], trigger_report: Dict[str, Any]) -> Dict[str, Any]:
    doctrine_id = "OSCILLATION_EDGE_LONG_SCALP"
    cluster = next((row for row in cluster_report.get("clusters", []) if str(row.get("cluster_id", "") or "") == doctrine_id), {})
    active_clusters = {str(row.get("cluster_id", "") or "") for row in cluster_report.get("active_clusters", [])}
    context_rows = [row for row in context_report.get("rows", []) if str(row.get("strategy_id", row.get("cluster_id", "")) or "") == doctrine_id]
    trigger_rows = [row for row in trigger_report.get("rows", []) if str(row.get("strategy_id", "") or "") == doctrine_id]
    extraction_summary = cluster.get("tier1_extraction_summary", {})
    failed_gates = []
    performance_gate = cluster_report.get("performance_gate", {})
    if cluster:
        if int(cluster.get("cluster_size", 0) or 0) < int(performance_gate.get("minimum_cluster_size", 0) or 0):
            failed_gates.append("minimum_cluster_size")
        if int(extraction_summary.get("trade_count", 0) or 0) < int(performance_gate.get("minimum_trade_count", 0) or 0):
            failed_gates.append("minimum_trade_count")
        if float(extraction_summary.get("win_rate", 0.0) or 0.0) < float(performance_gate.get("minimum_win_rate", 0.0) or 0.0):
            failed_gates.append("minimum_win_rate")
        if float(extraction_summary.get("expectancy_pips", 0.0) or 0.0) < float(performance_gate.get("minimum_expectancy_pips", 0.0) or 0.0):
            failed_gates.append("minimum_expectancy_pips")
        if float(extraction_summary.get("net_pnl_pips", 0.0) or 0.0) < float(performance_gate.get("minimum_net_pnl_pips", 0.0) or 0.0):
            failed_gates.append("minimum_net_pnl_pips")
        if not cluster.get("tier1_selected_expression_ids"):
            failed_gates.append("selected_expression_ids")

    return {
        "artifact_id": "V2_PHASE4_UPSTREAM_LEAK_TRACE",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "doctrine_id": doctrine_id,
        "phase2_cluster_present": bool(cluster),
        "phase2_active_cluster": doctrine_id in active_clusters,
        "phase2_tier1_survivor": bool(cluster.get("tier1_survivor")),
        "phase2_runtime_status": cluster.get("doctrine_runtime_status"),
        "phase2_failed_gates": failed_gates,
        "phase2_extraction_summary": extraction_summary,
        "phase3_context_row_count": len(context_rows),
        "phase4_trigger_row_count": len(trigger_rows),
        "phase4_trigger_status_counts": {
            "READY": sum(1 for row in trigger_rows if row.get("status") == "READY"),
            "ABORTED": sum(1 for row in trigger_rows if row.get("status") == "ABORTED"),
            "INVALID": sum(1 for row in trigger_rows if row.get("status") == "INVALID"),
        },
        "diagnosis": "Filtered out at Phase 2 before Phase 3/4 propagation." if cluster and doctrine_id not in active_clusters else "Leak location unresolved within current reports.",
    }


def _build_phase5_backbone_profit_leak_report(
    comparison_report: Dict[str, Any],
    variant_viability_report: Dict[str, Any],
) -> Dict[str, Any]:
    backbone = {
        "TRANSITION_RELEASE_SHORT_STANDARD",
        "FLOW_DRIFT_SHORT",
        "FLOW_DRIFT_LONG",
        "COMPRESSION_PRESSURE_LIFT_LONG",
    }
    comparisons = {
        str(row.get("strategy_id", "") or ""): row
        for row in comparison_report.get("strategy_comparisons", [])
        if str(row.get("strategy_id", "") or "") in backbone
    }
    variant_rows = [
        row for row in variant_viability_report.get("variant_strategies", [])
        if str(row.get("strategy_id", "") or "") in backbone
    ]
    by_strategy: Dict[str, List[Dict[str, Any]]] = {}
    for row in variant_rows:
        by_strategy.setdefault(str(row.get("strategy_id", "") or ""), []).append(row)

    rows: List[Dict[str, Any]] = []
    for strategy_id in sorted(backbone):
        comparison = comparisons.get(strategy_id, {})
        before = comparison.get("before", {})
        after = comparison.get("after", {})
        delta = comparison.get("delta", {})
        after_variants = sorted(
            by_strategy.get(strategy_id, []),
            key=lambda row: (
                -float(row.get("expectancy_pips", 0.0) or 0.0),
                -float(row.get("net_pnl_pips", 0.0) or 0.0),
                -int(row.get("trade_count", 0) or 0),
                str(row.get("tier1_variant_id", "") or ""),
            ),
        )
        before_avg = round(float(before.get("net_pnl_pips", 0.0) or 0.0) / max(int(before.get("trade_count", 0) or 0), 1), 6)
        after_avg = round(float(after.get("net_pnl_pips", 0.0) or 0.0) / max(int(after.get("trade_count", 0) or 0), 1), 6)
        leak_tags: List[str] = []
        if int(delta.get("trade_count", 0) or 0) < 0:
            leak_tags.append("COUNT_LOSS")
        if float(delta.get("net_pnl_pips", 0.0) or 0.0) < 0:
            leak_tags.append("NET_PNL_LOSS")
        if after_avg < before_avg:
            leak_tags.append("AVG_TRADE_VALUE_LOSS")
        if int(after.get("aborted_count", 0) or 0) > int(before.get("aborted_count", 0) or 0):
            leak_tags.append("ABORT_PRESSURE_UP")

        rows.append(
            {
                "strategy_id": strategy_id,
                "before": {
                    "trade_count": int(before.get("trade_count", 0) or 0),
                    "aborted_count": int(before.get("aborted_count", 0) or 0),
                    "expectancy_pips": float(before.get("expectancy_pips", 0.0) or 0.0),
                    "net_pnl_pips": float(before.get("net_pnl_pips", 0.0) or 0.0),
                    "avg_trade_value_pips": before_avg,
                    "target_distance_buckets": before.get("target_distance_buckets", []),
                    "abort_reason_counts": before.get("abort_reason_counts", {}),
                },
                "after": {
                    "trade_count": int(after.get("trade_count", 0) or 0),
                    "aborted_count": int(after.get("aborted_count", 0) or 0),
                    "expectancy_pips": float(after.get("expectancy_pips", 0.0) or 0.0),
                    "net_pnl_pips": float(after.get("net_pnl_pips", 0.0) or 0.0),
                    "avg_trade_value_pips": after_avg,
                    "target_distance_buckets": after.get("target_distance_buckets", []),
                    "abort_reason_counts": after.get("abort_reason_counts", {}),
                },
                "delta": delta,
                "profit_leak_tags": leak_tags,
                "after_top_variant_routes": [
                    {
                        "tier1_variant_id": row.get("tier1_variant_id"),
                        "tier1_route_id": row.get("tier1_route_id"),
                        "trade_count": row.get("trade_count"),
                        "aborted_count": row.get("aborted_count"),
                        "expectancy_pips": row.get("expectancy_pips"),
                        "net_pnl_pips": row.get("net_pnl_pips"),
                        "phase5_gate_passed": row.get("phase5_gate_passed"),
                    }
                    for row in after_variants[:5]
                ],
            }
        )

    return {
        "artifact_id": "V2_PHASE5_BACKBONE_PROFIT_LEAK_REPORT",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "rows": rows,
    }


def _build_phase4_backbone_lock_correction_plan(
    profit_leak_report: Dict[str, Any],
    option_lock_plan: Dict[str, Any],
) -> Dict[str, Any]:
    lock_lookup = {row["doctrine_id"]: row for row in option_lock_plan.get("doctrines", [])}
    rows: List[Dict[str, Any]] = []
    for leak_row in profit_leak_report.get("rows", []):
        strategy_id = str(leak_row.get("strategy_id", "") or "")
        tags = set(leak_row.get("profit_leak_tags", []))
        adjustments: List[str] = []
        if "COUNT_LOSS" in tags:
            adjustments.append("Restore one higher-throughput route sibling inside the current live shortlist only.")
        if "AVG_TRADE_VALUE_LOSS" in tags or "NET_PNL_LOSS" in tags:
            adjustments.append("Remove the weakest probationary extension/capture behavior before broadening anything.")
        if "ABORT_PRESSURE_UP" in tags:
            adjustments.append("Reduce kill-switch decay pressure by restoring the prior profitable confirmation/TTL shape selectively.")
        if not adjustments:
            adjustments.append("Keep current lock set unchanged and observe one more rerun.")
        rows.append(
            {
                "strategy_id": strategy_id,
                "current_live_options": lock_lookup.get(strategy_id, {}).get("live_options", []),
                "current_probationary_options": lock_lookup.get(strategy_id, {}).get("probationary_options", []),
                "profit_leak_tags": leak_row.get("profit_leak_tags", []),
                "recommended_corrections": adjustments,
            }
        )
    rows.sort(key=lambda row: row["strategy_id"])
    return {
        "artifact_id": "V2_PHASE4_BACKBONE_LOCK_CORRECTION_PLAN",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "rows": rows,
    }


def _build_phase2_survivor_gate_trace(cluster_report: Dict[str, Any]) -> Dict[str, Any]:
    doctrine_id = "OSCILLATION_EDGE_LONG_SCALP"
    cluster = next((row for row in cluster_report.get("clusters", []) if str(row.get("cluster_id", "") or "") == doctrine_id), {})
    extraction_summary = cluster.get("tier1_extraction_summary", {})
    gate = cluster_report.get("performance_gate", {})
    actual = {
        "cluster_size": int(cluster.get("cluster_size", 0) or 0),
        "trade_count": int(extraction_summary.get("trade_count", 0) or 0),
        "win_rate": float(extraction_summary.get("win_rate", 0.0) or 0.0),
        "expectancy_pips": float(extraction_summary.get("expectancy_pips", 0.0) or 0.0),
        "net_pnl_pips": float(extraction_summary.get("net_pnl_pips", 0.0) or 0.0),
        "selected_expression_count": len(cluster.get("tier1_selected_expression_ids", [])),
    }
    thresholds = {
        "cluster_size": int(gate.get("minimum_cluster_size", 0) or 0),
        "trade_count": int(gate.get("minimum_trade_count", 0) or 0),
        "win_rate": float(gate.get("minimum_win_rate", 0.0) or 0.0),
        "expectancy_pips": float(gate.get("minimum_expectancy_pips", 0.0) or 0.0),
        "net_pnl_pips": float(gate.get("minimum_net_pnl_pips", 0.0) or 0.0),
        "selected_expression_count": 1,
    }
    return {
        "artifact_id": "V2_PHASE2_SURVIVOR_GATE_TRACE",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "doctrine_id": doctrine_id,
        "actual": actual,
        "thresholds": thresholds,
        "gate_results": {
            "cluster_size": actual["cluster_size"] >= thresholds["cluster_size"],
            "trade_count": actual["trade_count"] >= thresholds["trade_count"],
            "win_rate": actual["win_rate"] >= thresholds["win_rate"],
            "expectancy_pips": actual["expectancy_pips"] >= thresholds["expectancy_pips"],
            "net_pnl_pips": actual["net_pnl_pips"] >= thresholds["net_pnl_pips"],
            "selected_expression_count": actual["selected_expression_count"] >= thresholds["selected_expression_count"],
        },
        "diagnosis": "Low-count scalp family fails generic survivor economics despite sufficient cluster size and trade count.",
    }


def _build_phase2_edge_long_scalp_admission_adjustment_plan(trace_report: Dict[str, Any]) -> Dict[str, Any]:
    actual = trace_report.get("actual", {})
    return {
        "artifact_id": "V2_PHASE2_EDGE_LONG_SCALP_ADMISSION_ADJUSTMENT_PLAN",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "doctrine_id": "OSCILLATION_EDGE_LONG_SCALP",
        "current_problem": "Fails generic Phase 2 survivor economics before Phase 3/4 propagation.",
        "recommended_order": [
            "Create a fragile-scalp survivor lane with doctrine-tier-aware economics instead of global doctrine floors.",
            "Require route-level evidence concentration before relaxed admission is allowed.",
            "Do not change Phase 4 until the doctrine can re-enter active clusters.",
        ],
        "candidate_admission_options": [
            {
                "option_id": "SCALP_PROBATION_LANE",
                "description": "Allow fragile scalp doctrines to survive with lower aggregate win/expectancy floors if at least one route family is positive and cluster size is sufficient.",
                "trigger_conditions": {
                    "doctrine_operating_tier": "FRAGILE",
                    "cluster_size_min": max(12, int(actual.get('cluster_size', 0) * 0.1)),
                    "route_level_positive_edge_required": True,
                },
            },
            {
                "option_id": "ROUTE_DOMINANCE_ADMISSION",
                "description": "Admit only if one or two route families dominate with positive expectancy, even when doctrine-wide aggregate is slightly negative.",
                "trigger_conditions": {
                    "positive_route_share_required": 0.25,
                    "max_live_routes": 2,
                },
            },
            {
                "option_id": "NO_CHANGE_RESEARCH_ONLY",
                "description": "Keep current survivor gate unchanged and leave the doctrine outside active runtime until Phase 2 route identity improves.",
                "trigger_conditions": {
                    "requires_no_runtime_change": True,
                },
            },
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run V2 entry stack with optional focused strategy rerun.")
    parser.add_argument("--focus-strategy", default="", help="Only evaluate one previously cleared strategy id for fast baseline comparison.")
    parser.add_argument(
        "--baseline-report",
        default=str(PHASE5_DIR / "v2_phase5_evaluation_report.json"),
        help="Previous evaluation report used for baseline comparison.",
    )
    args = parser.parse_args()

    determinism = read_json(CONTROL_DIR / "v2_determinism_lock.json")
    phase_contracts = read_json(CONTROL_DIR / "v2_phase_contracts.json")
    performance_gate = read_json(CONTROL_DIR / "v2_performance_gate.json")
    blitz_config = _load_blitz_config()
    config = make_config(determinism)
    base_seed = int(determinism["seed"])
    scenario_names = determinism["sampling_policy"]["fixed_phase1_scenarios"]
    profile_limit = int(determinism["sampling_policy"]["fixed_max_profiles_per_scenario"])
    profile_stride = int(determinism["sampling_policy"]["fixed_profile_stride"])

    scenario_state: Dict[str, Dict[str, Any]] = {}
    all_profiles: List[Dict[str, Any]] = []
    phase1_scenarios: List[Dict[str, Any]] = []

    for idx, name in enumerate(scenario_names):
        raw_ticks = scenario_ticks(name, base_seed + idx)
        sanitized = sanitize_ticks(raw_ticks, config)
        sanitized_ticks = [vars(tick) for tick in sanitized["ticks"]]
        phase1_stack = build_phase1_stack(sanitized["ticks"], config)
        phase1_profiles = [vars(profile) for profile in phase1_stack["profiles"]]
        profiles = _annotate_truth_kernels(phase1_profiles, phase1_stack["tier0_handoff_rows"])
        sampled_profiles = sample_profiles(profiles, profile_stride, profile_limit)
        scenario_state[name] = {
            "raw_ticks": raw_ticks,
            "sanitized_summary": sanitized["summary"],
            "ticks": sanitized_ticks,
            "profiles": profiles,
        }
        all_profiles.extend(profiles)
        phase1_summary = summarize_profiles(list(phase1_stack["profiles"]))
        phase1_scenarios.append(
            {
                "scenario": name,
                "seed": base_seed + idx,
                "tick_input_count": len(raw_ticks),
                "tick_kept_count": int(sanitized["summary"].get("kept_count", 0)),
                "tick_dropped_count": int(sanitized["summary"].get("dropped_count", 0)),
                "drop_reasons": sanitized["summary"].get("drop_reasons", {}),
                "sampled_profile_count": len(sampled_profiles),
                **phase1_summary,
            }
        )

    phase1_report = {
        "artifact_id": "V2_PHASE1_SCENARIO_SUMMARY",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "boundary_lock_ref": str(CONTROL_DIR / "v2_boundary_lock.json"),
        "sampling_policy": determinism["sampling_policy"],
        "scenarios": phase1_scenarios,
    }
    write_json(PHASE1_DIR / "v2_phase1_scenario_summary.json", phase1_report)

    phase2 = fit_phase2_clusters(all_profiles, base_seed)
    all_clusters = phase2["clusters"]
    phase2_extraction = evaluate_phase2_doctrines(
        scenario_state=scenario_state,
        doctrines=all_clusters,
        commission_pips=float(config.commission_pips),
        slippage_pips=float(config.slippage_pips),
    )
    extraction_lookup = {
        row["strategy_id"]: row for row in phase2_extraction["strategy_summaries"]
    }
    enriched_clusters = []
    for row in all_clusters:
        extraction_summary = extraction_lookup.get(
            row["doctrine_id"],
            {
                "strategy_id": row["doctrine_id"],
                "trade_count": 0,
                "aborted_count": 0,
                "distance_expression_count": 0,
                "target_distance_bucket_count": 0,
                "distance_expression_ids": [],
                "target_distance_buckets": [],
                "win_rate": 0.0,
                "expectancy_pips": 0.0,
                "net_pnl_pips": 0.0,
                "viable": False,
            },
        )
        expression_rules = phase2_extraction.get("expression_rule_summary", {}).get(row["doctrine_id"], [])
        selected_expression_ids = [
            rule["expression_id"]
            for rule in expression_rules
            if bool(rule.get("survivor"))
        ]
        operating_tier = _doctrine_operating_tier(str(row["doctrine_id"]))
        runtime_status = (
            "RETIRED_DISABLED"
            if operating_tier == "RETIRED"
            else "FROZEN_RESEARCH_ONLY"
            if operating_tier == "FROZEN"
            else "ACTIVE_RUNTIME"
        )
        tier1_survivor = _phase2_survivor_gate_passed(
            row,
            extraction_summary,
            selected_expression_ids,
            performance_gate["phase2"],
            blitz_config,
        )
        enriched_clusters.append(
            {
                **row,
                "doctrine_operating_tier": operating_tier,
                "doctrine_runtime_status": runtime_status,
                "doctrine_runtime_contract": _doctrine_runtime_contract(str(row["doctrine_id"])),
                "tier1_extraction_summary": extraction_summary,
                "tier1_expression_rules": expression_rules,
                "tier1_selected_expression_ids": selected_expression_ids,
                "tier1_survivor": tier1_survivor,
            }
        )

    clusters = [
        row
        for row in enriched_clusters
        if bool(row.get("tier1_survivor")) and str(row.get("doctrine_runtime_status", "")) == "ACTIVE_RUNTIME"
    ]
    cluster_lookup = {row["cluster_id"]: row for row in clusters}

    total_represented = sum(int(row.get("cluster_size", 0)) for row in enriched_clusters)
    active_represented = sum(int(row.get("cluster_size", 0)) for row in clusters)
    frozen_survivors = [
        row for row in enriched_clusters if bool(row.get("tier1_survivor")) and str(row.get("doctrine_runtime_status", "")) == "FROZEN_RESEARCH_ONLY"
    ]

    cluster_report = {
        "artifact_id": "V2_PHASE2_CLUSTER_REPORT",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "deterministic_seed": base_seed,
        "cluster_count": len(enriched_clusters),
        "doctrine_count": len(enriched_clusters),
        "active_cluster_count": len(clusters),
        "filtered_out_cluster_count": len(enriched_clusters) - len(clusters),
        "coverage": {
            **phase2.get("coverage", {}),
            "active_represented_count": active_represented,
            "active_represented_share": round(active_represented / max(total_represented, 1), 6),
        },
        "tier1_extraction_summary": {
            "raw_opportunity_count": phase2_extraction["raw_opportunity_count"],
            "assignment_count": phase2_extraction["assignment_count"],
            "candidate_count": phase2_extraction["candidate_count"],
            "result_count": phase2_extraction["result_count"],
            "captured_profile_count": phase2_extraction["captured_profile_count"],
            "captured_profile_share": phase2_extraction["captured_profile_share"],
            "uncaptured_profile_count": phase2_extraction["uncaptured_profile_count"],
            "uncaptured_profile_share": phase2_extraction["uncaptured_profile_share"],
            "survivor_count": len(clusters),
            "survivor_ids": [row["doctrine_id"] for row in clusters],
            "frozen_survivor_count": len(frozen_survivors),
            "frozen_survivor_ids": [row["doctrine_id"] for row in frozen_survivors],
            "uncaptured_doctrine_states": phase2_extraction.get("uncaptured_doctrine_states", phase2_extraction["uncaptured_pattern_match_states"]),
            "uncaptured_pattern_match_states": phase2_extraction["uncaptured_pattern_match_states"],
            "uncaptured_distance_families": phase2_extraction["uncaptured_distance_families"],
            "uncaptured_extraction_signatures": phase2_extraction["uncaptured_extraction_signatures"],
        },
        "clusters": enriched_clusters,
        "doctrines": enriched_clusters,
        "active_clusters": clusters,
        "active_doctrines": clusters,
        "performance_gate": performance_gate["phase2"],
        "forbidden_feature_confirmation": [
            "asset_name_excluded",
            "pair_excluded",
            "session_excluded",
            "regime_excluded",
            "pnl_excluded",
        ],
        "notes": [
            "Phase 2 now compiles doctrine candidates from the full Tier 0 raw opportunity surface.",
            "Representatives preserve multi-distance expressions instead of collapsing each episode to one outcome.",
            "One doctrine may own multiple target distance buckets and distance families.",
            "Tier 1 now runs a direct unrestricted extraction simulation before any doctrine is allowed into Tier 2.",
            "Assignment still requires doctrine support-contract agreement, not pattern-state name match alone.",
            "Doctrine operating tiers now distinguish backbone, fragile, and frozen research-only doctrines.",
        ],
    }
    write_json(PHASE2_DIR / "v2_phase2_cluster_report.json", cluster_report)
    write_json(
        PHASE2_DIR / "phase2_doctrine_operating_model.json",
        {
            "artifact_id": "V2_PHASE2_DOCTRINE_OPERATING_MODEL",
            "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "doctrines": [
                {
                    "doctrine_id": row["doctrine_id"],
                    "doctrine_operating_tier": row["doctrine_operating_tier"],
                    "doctrine_runtime_status": row["doctrine_runtime_status"],
                    "doctrine_runtime_contract": row["doctrine_runtime_contract"],
                    "tier1_survivor": row["tier1_survivor"],
                }
                for row in enriched_clusters
            ],
        },
    )

    context_rows: List[Dict[str, Any]] = []
    trigger_rows: List[Dict[str, Any]] = []
    evaluation_rows: List[Dict[str, Any]] = []
    rr_rows: List[float] = []
    focus_strategy = str(args.focus_strategy or "").strip().upper()

    for scenario_name, state in scenario_state.items():
        profiles = state["profiles"]
        ticks = state["ticks"]
        profiles_by_anchor = {int(profile["anchor_index"]): profile for profile in profiles}
        fired_episode_keys: set[tuple[str, str]] = set()
        for profile in profiles:
            cluster_id = assign_profile_to_cluster(profile, clusters)
            if not cluster_id:
                continue
            cluster = cluster_lookup[cluster_id]
            matching_routes = _matching_survivor_routes(cluster, profile, blitz_config)
            if not matching_routes:
                continue
            chosen_route = matching_routes[0]
            context = build_context_snapshot(profile=profile, scenario_profiles=profiles, cluster=cluster)
            if not _profile_allowed_by_blitz_filter(profile=profile, cluster=cluster, context=context, blitz_config=blitz_config):
                continue
            rr_rows.append(
                float(context["projection_axis"]["expected_target_distance_pips"])
                / max(float(context["projection_axis"]["stop_distance_pips"]), 1e-9)
            )
            context_rows.append(
                {
                    "scenario": scenario_name,
                    "profile_id": profile["profile_id"],
                    "cluster_id": cluster_id,
                    "tier1_route_id": chosen_route["route_id"],
                    "tier1_segment_type": chosen_route["segment_type"],
                    "tier1_segment_value": chosen_route["segment_value"],
                    "tier1_variant_id": chosen_route["selected_variant_id"],
                    **context,
                }
            )
            candidate = build_trigger_candidate(
                profile=profile,
                context=context,
                cluster=cluster,
                ticks=ticks,
                profiles_by_anchor=profiles_by_anchor,
                selected_route=chosen_route,
            )
            if candidate.get("status") == "READY":
                episode_key = str(
                    profile.get("opportunity_episode_id")
                    or profile.get("overlap_group_id")
                    or profile.get("profile_id")
                    or "UNKNOWN_EPISODE"
                )
                idempotency_key = (str(candidate.get("strategy_id", "") or ""), episode_key)
                if idempotency_key in fired_episode_keys:
                    candidate = {
                        **candidate,
                        "status": "INVALID",
                        "reason": "duplicate_trigger_suppressed",
                    }
                else:
                    fired_episode_keys.add(idempotency_key)
            if focus_strategy and candidate.get("strategy_id") != focus_strategy:
                continue
            trigger_rows.append(
                {
                    "scenario": scenario_name,
                    "tier1_route_id": chosen_route["route_id"],
                    "tier1_segment_type": chosen_route["segment_type"],
                    "tier1_segment_value": chosen_route["segment_value"],
                    "tier1_variant_id": chosen_route["selected_variant_id"],
                    **candidate,
                }
            )
            evaluation_rows.append(
                {
                    "scenario": scenario_name,
                    "tier1_route_id": chosen_route["route_id"],
                    "tier1_segment_type": chosen_route["segment_type"],
                    "tier1_segment_value": chosen_route["segment_value"],
                    "tier1_variant_id": chosen_route["selected_variant_id"],
                    **evaluate_candidate(
                        candidate=candidate,
                        ticks=ticks,
                        commission_pips=float(config.commission_pips),
                        slippage_pips=float(config.slippage_pips),
                    ),
                }
            )

    context_report = {
        "artifact_id": "V2_PHASE3_CONTEXT_REPORT",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "row_count": len(context_rows),
        "median_target_to_stop_ratio": round(sorted(rr_rows)[len(rr_rows) // 2], 6) if rr_rows else 0.0,
        "performance_gate": performance_gate["phase3"],
        "rows": context_rows,
    }
    write_json(PHASE3_DIR / "v2_phase3_context_report.json", context_report)

    trigger_report = {
        "artifact_id": "V2_PHASE4_TRIGGER_REPORT",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "candidate_count": len(trigger_rows),
        "ready_count": sum(1 for row in trigger_rows if row.get("status") == "READY"),
        "aborted_count": sum(1 for row in trigger_rows if row.get("status") == "ABORTED"),
        "invalid_count": sum(1 for row in trigger_rows if row.get("status") == "INVALID"),
        "abort_ratio": round(
            sum(1 for row in trigger_rows if row.get("status") == "ABORTED") / max(len(trigger_rows), 1),
            6,
        ),
        "performance_gate": performance_gate["phase4"],
        "rows": trigger_rows,
    }
    write_json(PHASE4_DIR / "v2_phase4_trigger_report.json", trigger_report)

    strategy_summaries = _enrich_phase5_summaries(
        summarize_strategy_results(evaluation_rows),
        performance_gate["phase5"],
    )
    variant_strategy_summaries = _enrich_phase5_summaries(
        summarize_result_groups(
            evaluation_rows,
            group_fields=["strategy_id", "tier1_variant_id", "tier1_route_id"],
        ),
        performance_gate["phase5"],
    )
    # Keep every strategy that clears the Phase 5 profitability gate so Tier 6
    # reflects the full profitable entry set rather than only the top few rows.
    top_viable = [
        row
        for row in strategy_summaries
        if row["phase5_gate_passed"]
    ]
    if not top_viable:
        top_viable = strategy_summaries[:1]
    promotion_review = _build_registry_promotion_review(
        strategy_summaries,
        variant_strategy_summaries,
        [str(row.get("strategy_id", "") or "") for row in top_viable],
    )
    promoted_extensions = [
        row["best_passing_variant"]
        for row in promotion_review["review"]
        if bool(row.get("promotion_ready")) and row.get("best_passing_variant")
    ]
    selected_strategy_ids = {str(row.get("strategy_id", "") or "") for row in top_viable}
    for promoted in promoted_extensions:
        promoted_strategy_id = str(promoted.get("strategy_id", "") or "")
        if promoted_strategy_id in selected_strategy_ids:
            continue
        strategy_row = next(
            (row for row in strategy_summaries if str(row.get("strategy_id", "") or "") == promoted_strategy_id),
            None,
        )
        if strategy_row:
            top_viable.append(strategy_row)
            selected_strategy_ids.add(promoted_strategy_id)
    promotion_review = _build_registry_promotion_review(
        strategy_summaries,
        variant_strategy_summaries,
        [str(row.get("strategy_id", "") or "") for row in top_viable],
    )

    stage_gates = {
        "phase2_cluster_floor": len(clusters) > 0,
        "phase3_projection_floor": (context_report["median_target_to_stop_ratio"] >= float(performance_gate["phase3"]["minimum_expected_target_to_stop_ratio"])),
        "phase4_ready_floor": trigger_report["ready_count"] >= int(performance_gate["phase4"]["minimum_ready_candidates"]),
        "phase4_abort_floor": trigger_report["abort_ratio"] <= float(performance_gate["phase4"]["maximum_abort_ratio"]),
        "phase5_strategy_floor": any(
            row["phase5_gate_passed"]
            for row in strategy_summaries
        ),
    }

    evaluation_report = {
        "artifact_id": "V2_PHASE5_EVALUATION_REPORT",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "one_way_firewall": True,
        "focus_strategy": focus_strategy or None,
        "result_count": len(evaluation_rows),
        "performance_gate": performance_gate["phase5"],
        "stage_gates": stage_gates,
        "strategies": strategy_summaries,
        "variant_strategies": variant_strategy_summaries,
        "selected_strategy_count": len(top_viable),
        "selected_strategies": top_viable,
    }
    report_name = "v2_phase5_evaluation_report.json" if not focus_strategy else f"v2_phase5_evaluation_report_{focus_strategy.lower()}.json"
    write_json(PHASE5_DIR / report_name, evaluation_report)
    write_json(
        PHASE5_DIR / ("phase5_evaluation_rows.json" if not focus_strategy else f"phase5_evaluation_rows_{focus_strategy.lower()}.json"),
        {
            "artifact_id": "V2_PHASE5_EVALUATION_ROWS",
            "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "focus_strategy": focus_strategy or None,
            "rows": evaluation_rows,
        },
    )
    write_json(
        PHASE4_DIR / "phase4_variant_fire_report.json",
        _build_phase4_variant_fire_report(trigger_rows, performance_gate["phase4"]),
    )
    write_json(
        PHASE4_DIR / "phase4_adjustment_option_report.json",
        _build_phase4_adjustment_option_report(trigger_rows),
    )
    option_lock_plan = _build_phase4_option_lock_plan()
    write_json(PHASE4_DIR / "phase4_option_lock_plan.json", option_lock_plan)
    option_prune_report = _build_phase4_option_prune_report(
        _build_phase4_adjustment_option_report(trigger_rows),
    )
    write_json(PHASE4_DIR / "phase4_option_prune_report.json", option_prune_report)
    write_json(
        PHASE4_DIR / "phase4_track2_pocket_whitelist_plan.json",
        {
            "artifact_id": "V2_PHASE4_TRACK2_POCKET_WHITELIST_PLAN",
            "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "doctrines": get_track2_pocket_whitelist_plan(),
        },
    )
    variant_viability_report = {
        "artifact_id": "V2_PHASE5_VARIANT_VIABILITY_REPORT",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "performance_gate": performance_gate["phase5"],
        "variant_strategies": variant_strategy_summaries,
    }
    write_json(PHASE5_DIR / "phase5_variant_viability_report.json", variant_viability_report)
    write_json(PHASE5_DIR / "v2_registry_promotion_review.json", promotion_review)

    strategy_registry = {
        "artifact_id": "V2_ENTRY_STRATEGY_REGISTRY",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "status": "READY" if top_viable and all(stage_gates.values()) else "INSUFFICIENT",
        "strategies": top_viable,
        "minimum_supported_strategies": 1,
        "target_supported_strategies": len(top_viable),
        "performance_gate_ref": str(CONTROL_DIR / "v2_performance_gate.json"),
    }
    write_json(CONTROL_DIR / "v2_entry_strategy_registry.json", strategy_registry)

    baseline_comparison: Dict[str, Any] | None = None
    strategy_baseline_comparison: Dict[str, Any] | None = None
    baseline_path = Path(str(args.baseline_report))
    if baseline_path.exists():
        baseline_obj = read_json(baseline_path)
        baseline_rows = {row.get("strategy_id"): row for row in baseline_obj.get("strategies", [])}
        current_rows = {row.get("strategy_id"): row for row in strategy_summaries}
        strategy_baseline_comparison = _build_strategy_baseline_comparison(
            baseline_rows,
            current_rows,
            str(baseline_path),
            str(PHASE5_DIR / report_name),
        )
        write_json(PHASE5_DIR / "v2_phase5_strategy_before_after_comparison.json", strategy_baseline_comparison)
        profit_leak_report = _build_phase5_backbone_profit_leak_report(
            strategy_baseline_comparison,
            variant_viability_report,
        )
        write_json(PHASE5_DIR / "phase5_backbone_profit_leak_report.json", profit_leak_report)
        write_json(
            PHASE4_DIR / "phase4_backbone_lock_correction_plan.json",
            _build_phase4_backbone_lock_correction_plan(profit_leak_report, option_lock_plan),
        )
        write_json(
            PHASE5_DIR / "phase5_doctrine_option_rerun_summary.json",
            _build_phase5_doctrine_option_rerun_summary(strategy_baseline_comparison, option_lock_plan),
        )
        if focus_strategy and focus_strategy in current_rows:
            before = baseline_rows.get(focus_strategy, {})
            after = current_rows[focus_strategy]
            baseline_comparison = {
                "artifact_id": "V2_FOCUSED_BASELINE_COMPARISON",
                "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "strategy_id": focus_strategy,
                "baseline_report": str(baseline_path),
                "current_report": str(PHASE5_DIR / report_name),
                "before": before,
                "after": after,
                "delta": {
                    "trade_count": int(after.get("trade_count", 0)) - int(before.get("trade_count", 0) or 0),
                    "win_rate": round(float(after.get("win_rate", 0.0)) - float(before.get("win_rate", 0.0) or 0.0), 6),
                    "expectancy_pips": round(float(after.get("expectancy_pips", 0.0)) - float(before.get("expectancy_pips", 0.0) or 0.0), 6),
                    "net_pnl_pips": round(float(after.get("net_pnl_pips", 0.0)) - float(before.get("net_pnl_pips", 0.0) or 0.0), 6),
                },
            }
            write_json(PHASE5_DIR / f"v2_focus_baseline_comparison_{focus_strategy.lower()}.json", baseline_comparison)

    write_json(
        PHASE4_DIR / "phase4_upstream_leak_trace_oscillation_edge_long_scalp.json",
        _build_phase4_upstream_leak_trace(cluster_report, context_report, trigger_report),
    )
    phase2_gate_trace = _build_phase2_survivor_gate_trace(cluster_report)
    write_json(
        PHASE2_DIR / "phase2_survivor_gate_trace_oscillation_edge_long_scalp.json",
        phase2_gate_trace,
    )
    write_json(
        PHASE2_DIR / "phase2_edge_long_scalp_admission_adjustment_plan.json",
        _build_phase2_edge_long_scalp_admission_adjustment_plan(phase2_gate_trace),
    )

    canonical_stack = {
        "artifact_id": "V2_CANONICAL_STACK",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "canonical_entry_runner": str(WORKSPACE / "tools" / "run_v2_entry_stack.py"),
        "canonical_phase1_runner": str(WORKSPACE / "tools" / "run_v2_phase1.py"),
        "canonical_entry_strategy_registry": str(CONTROL_DIR / "v2_entry_strategy_registry.json"),
        "canonical_entry_registry_module": str(WORKSPACE / "engine" / "entry" / "doctrine_entry_registry.py"),
        "canonical_entry_packet_builder": str(WORKSPACE / "engine" / "entry" / "entry_packet_builder.py"),
        "canonical_trade_gateway": str(WORKSPACE / "engine" / "in_between" / "trade_gateway_engine.py"),
        "canonical_aee_architecture": str(WORKSPACE / "aee_3tier_architecture.py"),
        "canonical_aee_integration": str(WORKSPACE / "aee_3tier_integration.py"),
        "canonical_aee_runtime_manager": str(WORKSPACE / "engine" / "aee" / "runtime_manager.py"),
        "legacy_policy": "RETIRED_NOT_DELETED",
    }
    write_json(CONTROL_DIR / "v2_canonical_stack.json", canonical_stack)

    entry_to_aee_contract = {
        "artifact_id": "V2_ENTRY_TO_AEE_CONTRACT",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "canonical_entry_output": str(CONTROL_DIR / "v2_entry_strategy_registry.json"),
        "canonical_entry_stack": str(CONTROL_DIR / "v2_canonical_stack.json"),
        "canonical_gateway_engine": str(WORKSPACE / "engine" / "in_between" / "trade_gateway_engine.py"),
        "canonical_packet_contract": str(WORKSPACE / "entry_inbetween_aee_contract.json"),
        "canonical_aee_architecture": str(WORKSPACE / "aee_3tier_architecture.py"),
        "canonical_aee_integration": str(WORKSPACE / "aee_3tier_integration.py"),
        "canonical_aee_runtime_manager": str(WORKSPACE / "engine" / "aee" / "runtime_manager.py"),
        "chain_status": "LOCKED",
        "required_identity_fields": [
            "doctrine_family_id",
            "doctrine_variant_id",
            "entry_pocket_id",
            "target_bucket",
            "target_profile",
            "horizon_class",
            "trade_role",
            "friction_profile",
        ],
        "notes": [
            "Legacy entry research surfaces are not canonical execution surfaces.",
            "AEE should consume only V2 canonical outputs once resumed.",
            "Gateway owns canonical trade identity and packet shaping.",
        ],
    }
    write_json(CONTROL_DIR / "v2_entry_to_aee_contract.json", entry_to_aee_contract)

    legacy_registry = {
        "artifact_id": "V2_LEGACY_SURFACE_REGISTRY",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "policy": "RETIRED_NOT_DELETED",
        "surfaces": [
            {
                "path": str(WORKSPACE / "control" / "doctrine_surface"),
                "status": "LEGACY_RESEARCH_ONLY",
            },
            {
                "path": str(WORKSPACE / "control" / "entry_diagnostics"),
                "status": "LEGACY_RESEARCH_ONLY",
            },
            {
                "path": str(WORKSPACE / "pattern_recognition"),
                "status": "LEGACY_RUNTIME_COMPONENTS_NOT_CANONICAL_FOR_V2",
            },
            {
                "path": str(WORKSPACE / "phone_bot.py"),
                "status": "NON_CANONICAL_RUNTIME_SURFACE",
            },
        ],
    }
    write_json(CONTROL_DIR / "v2_legacy_surface_registry.json", legacy_registry)

    summary = {
        "status": "PASS" if top_viable and all(stage_gates.values()) else "FAIL",
        "focus_strategy": focus_strategy or None,
        "selected_strategy_count": len(top_viable),
        "selected_strategy_ids": [row["strategy_id"] for row in top_viable],
        "stage_gates": stage_gates,
        "report": str(PHASE5_DIR / report_name),
        "strategy_before_after_comparison": (
            str(PHASE5_DIR / "v2_phase5_strategy_before_after_comparison.json")
            if strategy_baseline_comparison
            else None
        ),
        "baseline_comparison": (
            str(PHASE5_DIR / f"v2_focus_baseline_comparison_{focus_strategy.lower()}.json")
            if baseline_comparison
            else None
        ),
        "phase_contract_ref": str(CONTROL_DIR / "v2_phase_contracts.json"),
    }
    print(json.dumps(summary, indent=2))
    return 0 if top_viable and all(stage_gates.values()) else 1


if __name__ == "__main__":
    raise SystemExit(main())
