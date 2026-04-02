from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any, Dict, List

from tools.v2_phase2_cluster_engine import _doctrine_state, assign_profile_to_cluster
from tools.v2_phase5_evaluation_engine import evaluate_candidate, summarize_strategy_results


INSTANTIATION_LANE_DOCTRINES = {
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


def _is_raw_opportunity(profile: Dict[str, Any]) -> bool:
    return bool(
        profile.get("pattern_qualified_opportunity")
        or profile.get("conservative_opportunity")
        or profile.get("aggressive_path_opportunity")
    )


def _ttl_ticks(profile: Dict[str, Any]) -> int:
    bucket = str(profile.get("target_distance_bucket", "") or "").upper()
    energy = str(profile.get("energy_state", "") or "").upper()
    lifecycle = str(profile.get("lifecycle_stage", "") or "").upper()
    base = {
        "MICRO": 6,
        "SMALL": 10,
        "MEDIUM": 16,
        "LARGE": 24,
        "EXTENDED": 36,
    }.get(bucket, 12)
    energy_multiplier = {
        "IGNITION": 0.75,
        "DRIVE": 0.9,
        "DRIFT": 1.1,
        "DORMANT": 1.35,
    }.get(energy, 1.0)
    lifecycle_multiplier = {
        "RELEASE": 0.9,
        "EXPLOIT": 1.0,
        "PATTERN_HARVEST": 1.05,
        "HARVEST": 1.0,
        "LATE": 0.75,
        "WATCH": 0.8,
        "SCAN": 0.8,
        "SETUP": 1.15,
    }.get(lifecycle, 1.0)
    return max(3, int(round(base * energy_multiplier * lifecycle_multiplier)))


def _base_variant_specs() -> List[Dict[str, Any]]:
    return [
        {"variant_id": "CAPTURE_SCALP", "target_multiplier": 0.35, "stop_multiplier": 0.18, "ttl_multiplier": 0.45, "entry_offset_ticks": 0},
        {"variant_id": "CAPTURE_SNAP", "target_multiplier": 0.5, "stop_multiplier": 0.25, "ttl_multiplier": 0.6, "entry_offset_ticks": 0},
        {"variant_id": "CAPTURE_NEAR", "target_multiplier": 0.6, "stop_multiplier": 0.45, "ttl_multiplier": 0.75, "entry_offset_ticks": 0},
        {"variant_id": "CAPTURE_BALANCED", "target_multiplier": 0.8, "stop_multiplier": 0.6, "ttl_multiplier": 1.0, "entry_offset_ticks": 0},
        {"variant_id": "CAPTURE_FULL", "target_multiplier": 1.0, "stop_multiplier": 0.75, "ttl_multiplier": 1.0, "entry_offset_ticks": 0},
        {"variant_id": "CAPTURE_EXTENSION", "target_multiplier": 1.15, "stop_multiplier": 0.7, "ttl_multiplier": 1.25, "entry_offset_ticks": 0},
    ]


def _variant_specs_for_doctrine(doctrine: Dict[str, Any]) -> List[Dict[str, Any]]:
    doctrine_state = str(
        doctrine.get("doctrine_state", doctrine.get("doctrine_id", doctrine.get("pattern_match_state", ""))) or ""
    ).upper()
    variants = list(_base_variant_specs())
    if "RAW_COMPRESSION_CEILING_REJECTION_SHORT" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "REJECTION_SCALP", "target_multiplier": 0.45, "stop_multiplier": 0.18, "ttl_multiplier": 0.4, "entry_offset_ticks": 0},
                {"variant_id": "REJECTION_CONFIRM", "target_multiplier": 0.75, "stop_multiplier": 0.28, "ttl_multiplier": 0.65, "entry_offset_ticks": 1},
            ]
        )
    if "TRANSITION_RELEASE" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "TRANSITION_FRONT_RUN", "target_multiplier": 0.55, "stop_multiplier": 0.2, "ttl_multiplier": 0.45, "entry_offset_ticks": 0},
                {"variant_id": "TRANSITION_CONFIRM", "target_multiplier": 0.7, "stop_multiplier": 0.28, "ttl_multiplier": 0.65, "entry_offset_ticks": 1},
                {"variant_id": "TRANSITION_LATE", "target_multiplier": 0.9, "stop_multiplier": 0.35, "ttl_multiplier": 0.9, "entry_offset_ticks": 2},
                {"variant_id": "TRANSITION_RUNNER", "target_multiplier": 1.25, "stop_multiplier": 0.42, "ttl_multiplier": 1.3, "entry_offset_ticks": 1},
            ]
        )
    if "OSCILLATION_EDGE" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "EDGE_SCALP", "target_multiplier": 0.4, "stop_multiplier": 0.16, "ttl_multiplier": 0.35, "entry_offset_ticks": 0},
                {"variant_id": "EDGE_CONFIRM", "target_multiplier": 0.55, "stop_multiplier": 0.22, "ttl_multiplier": 0.5, "entry_offset_ticks": 1},
                {"variant_id": "EDGE_RELOAD", "target_multiplier": 0.75, "stop_multiplier": 0.26, "ttl_multiplier": 0.7, "entry_offset_ticks": 2},
            ]
        )
    if "OSCILLATION_PRESSURE_BUILD" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "PRESSURE_BUILD_SNAP", "target_multiplier": 0.6, "stop_multiplier": 0.2, "ttl_multiplier": 0.55, "entry_offset_ticks": 0},
                {"variant_id": "PRESSURE_BUILD_DRIVE", "target_multiplier": 0.95, "stop_multiplier": 0.32, "ttl_multiplier": 0.9, "entry_offset_ticks": 1},
            ]
        )
    if "FAILED_BREAK_LONG_RECLAIM" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "RECLAIM_CONFIRM", "target_multiplier": 0.8, "stop_multiplier": 0.3, "ttl_multiplier": 0.8, "entry_offset_ticks": 1},
                {"variant_id": "RECLAIM_HOLD", "target_multiplier": 1.0, "stop_multiplier": 0.35, "ttl_multiplier": 1.0, "entry_offset_ticks": 1},
            ]
        )
    if "BALANCED_SURFACE_LONG_PUSH" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "BALANCE_BREAK", "target_multiplier": 1.0, "stop_multiplier": 0.32, "ttl_multiplier": 0.85, "entry_offset_ticks": 0},
                {"variant_id": "BALANCE_RETEST", "target_multiplier": 1.15, "stop_multiplier": 0.28, "ttl_multiplier": 1.0, "entry_offset_ticks": 1},
            ]
        )
    if "FLOW_DRIFT" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "DRIFT_RIDE", "target_multiplier": 0.7, "stop_multiplier": 0.24, "ttl_multiplier": 0.8, "entry_offset_ticks": 1},
                {"variant_id": "DRIFT_REACCEL", "target_multiplier": 1.0, "stop_multiplier": 0.3, "ttl_multiplier": 1.0, "entry_offset_ticks": 0},
            ]
        )
    if "PRESSURE_DRIVE" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "PRESSURE_BREAK", "target_multiplier": 0.7, "stop_multiplier": 0.24, "ttl_multiplier": 0.7, "entry_offset_ticks": 0},
                {"variant_id": "PRESSURE_HOLD", "target_multiplier": 1.0, "stop_multiplier": 0.35, "ttl_multiplier": 1.0, "entry_offset_ticks": 1},
            ]
        )
    if "COILED_COMPRESSION" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "COIL_BREAK", "target_multiplier": 0.85, "stop_multiplier": 0.24, "ttl_multiplier": 0.7, "entry_offset_ticks": 0},
                {"variant_id": "COIL_EXPAND", "target_multiplier": 1.1, "stop_multiplier": 0.32, "ttl_multiplier": 1.0, "entry_offset_ticks": 1},
            ]
        )
    if "COMPRESSION_PRESSURE_LIFT" in doctrine_state or "COMPRESSION_PRESSURE_DROP" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "PRESSURE_RELEASE", "target_multiplier": 0.8, "stop_multiplier": 0.26, "ttl_multiplier": 0.75, "entry_offset_ticks": 0},
                {"variant_id": "PRESSURE_EXTENSION", "target_multiplier": 1.15, "stop_multiplier": 0.34, "ttl_multiplier": 1.05, "entry_offset_ticks": 1},
            ]
        )
    if "COMPRESSION_RELEASE" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "RELEASE_DIRECT", "target_multiplier": 0.9, "stop_multiplier": 0.26, "ttl_multiplier": 0.8, "entry_offset_ticks": 0},
                {"variant_id": "RELEASE_RUN", "target_multiplier": 1.2, "stop_multiplier": 0.36, "ttl_multiplier": 1.15, "entry_offset_ticks": 1},
            ]
        )
    if "COILED_TRANSITION" in doctrine_state or "EXPANSION_RELEASE" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "TREND_LAUNCH", "target_multiplier": 0.95, "stop_multiplier": 0.28, "ttl_multiplier": 0.85, "entry_offset_ticks": 0},
                {"variant_id": "TREND_HOLD", "target_multiplier": 1.25, "stop_multiplier": 0.38, "ttl_multiplier": 1.2, "entry_offset_ticks": 1},
            ]
        )
    if "FAILED_PUSH_" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "REVERSAL_SNAP", "target_multiplier": 0.5, "stop_multiplier": 0.18, "ttl_multiplier": 0.45, "entry_offset_ticks": 0},
                {"variant_id": "REVERSAL_CONFIRM", "target_multiplier": 0.7, "stop_multiplier": 0.24, "ttl_multiplier": 0.6, "entry_offset_ticks": 1},
            ]
        )
    if "COMPRESSION_FLOOR_BREAKDOWN" in doctrine_state or "COMPRESSION_CEILING_BREAKOUT" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "BREAKOUT_DIRECT", "target_multiplier": 0.85, "stop_multiplier": 0.26, "ttl_multiplier": 0.75, "entry_offset_ticks": 0},
                {"variant_id": "BREAKOUT_HOLD", "target_multiplier": 1.1, "stop_multiplier": 0.34, "ttl_multiplier": 1.0, "entry_offset_ticks": 1},
            ]
        )
    if "OSCILLATION_PRESSURE_BUILD" in doctrine_state or "PRESSURE_DRIVE" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "PRESSURE_CONFIRM", "target_multiplier": 0.75, "stop_multiplier": 0.3, "ttl_multiplier": 0.7, "entry_offset_ticks": 1},
                {"variant_id": "PRESSURE_SWING", "target_multiplier": 1.25, "stop_multiplier": 0.45, "ttl_multiplier": 1.1, "entry_offset_ticks": 1},
                {"variant_id": "PRESSURE_FRONT_RUN", "target_multiplier": 0.5, "stop_multiplier": 0.18, "ttl_multiplier": 0.45, "entry_offset_ticks": 0},
            ]
        )
    if "COMPRESSION" in doctrine_state or "COILED" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "COMPRESSION_BURST", "target_multiplier": 0.65, "stop_multiplier": 0.22, "ttl_multiplier": 0.5, "entry_offset_ticks": 1},
                {"variant_id": "COMPRESSION_SWING", "target_multiplier": 1.2, "stop_multiplier": 0.4, "ttl_multiplier": 1.2, "entry_offset_ticks": 1},
                {"variant_id": "COMPRESSION_BREAK", "target_multiplier": 0.9, "stop_multiplier": 0.28, "ttl_multiplier": 0.8, "entry_offset_ticks": 0},
            ]
        )
    if "FLOW_DRIFT" in doctrine_state or "RAW_BALANCED" in doctrine_state:
        variants.extend(
            [
                {"variant_id": "DRIFT_STEP", "target_multiplier": 0.55, "stop_multiplier": 0.2, "ttl_multiplier": 0.65, "entry_offset_ticks": 1},
                {"variant_id": "DRIFT_CONFIRM", "target_multiplier": 0.8, "stop_multiplier": 0.32, "ttl_multiplier": 0.85, "entry_offset_ticks": 1},
                {"variant_id": "DRIFT_SCALP", "target_multiplier": 0.4, "stop_multiplier": 0.16, "ttl_multiplier": 0.5, "entry_offset_ticks": 0},
            ]
        )
    deduped: Dict[str, Dict[str, Any]] = {}
    for variant in variants:
        deduped[str(variant["variant_id"])] = variant
    return list(deduped.values())


def _expression_compatible_with_doctrine(doctrine_id: str, expression_id: str) -> bool:
    doctrine_id = str(doctrine_id or "").upper()
    expression_id = str(expression_id or "").upper()
    if "OSCILLATION_EDGE_LONG" in doctrine_id:
        return "NEAR_FLOOR" in expression_id and "FLOOR_REBOUND|LONG" in expression_id
    if "OSCILLATION_EDGE_SHORT" in doctrine_id:
        return "NEAR_CEILING" in expression_id and "CEILING_REJECTION|SHORT" in expression_id
    return True


def _route_operating_mode(
    *,
    doctrine_id: str,
    expression_id: str,
    segment_type: str,
    segment_value: str,
    variant: Dict[str, Any],
) -> str:
    doctrine_id = str(doctrine_id or "").upper()
    expression_id = str(expression_id or "").upper()
    segment_type = str(segment_type or "BASE").upper()
    segment_value = str(segment_value or "ALL").upper()
    variant_id = str(variant.get("variant_id", "") or "").upper()

    if "TRANSITION_RELEASE_SHORT" in doctrine_id:
        if variant_id in {"TRANSITION_FRONT_RUN", "CAPTURE_SCALP", "CAPTURE_SNAP"} or (
            segment_type == "ENERGY" and segment_value == "IGNITION"
        ) or (segment_type == "LIFECYCLE" and segment_value == "RELEASE"):
            return "IGNITION_RELEASE_FAST"
        if variant_id in {"TRANSITION_RUNNER", "CAPTURE_EXTENSION"}:
            return "RELEASE_EXTENSION"
        return "RELEASE_CONFIRM"
    if "TRANSITION_RELEASE_LONG" in doctrine_id:
        if segment_type == "ENERGY" and segment_value == "IGNITION":
            return "LONG_RELEASE_IGNITION"
        return "LONG_RELEASE_CONFIRM"
    if "FLOW_DRIFT_SHORT" in doctrine_id:
        if variant_id in {"DRIFT_REACCEL", "CAPTURE_EXTENSION"}:
            return "DRIFT_REACCEL"
        if variant_id in {"DRIFT_CONFIRM", "CAPTURE_BALANCED", "CAPTURE_FULL"}:
            return "DRIFT_CONFIRM"
        return "DRIFT_HARVEST"
    if "FLOW_DRIFT_LONG" in doctrine_id:
        if variant_id in {"DRIFT_REACCEL", "CAPTURE_EXTENSION", "CAPTURE_FULL"}:
            return "LONG_DRIFT_REACCEL"
        if segment_type == "ENERGY" and segment_value == "IGNITION":
            return "LONG_DRIFT_IGNITION"
        return "LONG_DRIFT_CONFIRM"
    if "COMPRESSION_PRESSURE_LIFT_LONG" in doctrine_id:
        if variant_id == "COMPRESSION_SWING":
            return "COMPRESSION_SWING"
        if variant_id in {"PRESSURE_EXTENSION", "CAPTURE_EXTENSION"}:
            return "PRESSURE_EXTENSION_QUALIFIED"
        return "PRESSURE_LIFT_CAPTURE"
    if "COMPRESSION_PRESSURE_DROP_SHORT" in doctrine_id:
        if variant_id in {"CAPTURE_EXTENSION", "PRESSURE_EXTENSION"}:
            return "PRESSURE_DROP_EXTENSION"
        return "PRESSURE_DROP_CAPTURE"
    if "OSCILLATION_EDGE_SHORT" in doctrine_id:
        if "EXTENDED" in expression_id or variant_id in {"CAPTURE_EXTENSION", "CAPTURE_FULL"}:
            return "SOFT_REJECTION"
        return "HARD_REJECTION"
    if "OSCILLATION_EDGE_LONG" in doctrine_id:
        if variant_id in {"EDGE_CONFIRM", "EDGE_RELOAD"}:
            return "HARD_REBOUND"
        return "SOFT_REBOUND"
    if "OSCILLATION_PRESSURE_BUILD_LONG" in doctrine_id:
        if variant_id in {"PRESSURE_SWING", "PRESSURE_BUILD_DRIVE"}:
            return "PRESSURE_BUILD_SWING"
        return "PRESSURE_BUILD_CAPTURE"
    if "OSCILLATION_PRESSURE_BUILD_SHORT" in doctrine_id:
        if variant_id in {"PRESSURE_SWING", "PRESSURE_BUILD_DRIVE"}:
            return "PRESSURE_BUILD_SWING"
        return "PRESSURE_BUILD_CAPTURE"
    if "FAILED_PUSH_" in doctrine_id:
        if "NEAR_FLOOR" in expression_id or "NEAR_CEILING" in expression_id:
            return "FAILED_PUSH_REJECTION"
        return "FAILED_PUSH_RECOVERY"
    if "PRESSURE_DRIVE" in doctrine_id:
        if "NEAR_FLOOR" in expression_id or "NEAR_CEILING" in expression_id:
            return "PRESSURE_DRIVE_EDGE"
        if variant_id in {"PRESSURE_HOLD", "CAPTURE_EXTENSION", "CAPTURE_FULL"}:
            return "PRESSURE_DRIVE_HOLD"
        return "PRESSURE_DRIVE_MID"
    if "COMPRESSION_RELEASE" in doctrine_id:
        if variant_id in {"RELEASE_RUN", "CAPTURE_EXTENSION", "COMPRESSION_SWING"}:
            return "COMPRESSION_RELEASE_RUN"
        if variant_id in {"RELEASE_DIRECT", "COMPRESSION_BREAK", "CAPTURE_SNAP", "CAPTURE_SCALP"}:
            return "COMPRESSION_RELEASE_DIRECT"
        return "COMPRESSION_RELEASE_CONFIRM"
    if "COILED_COMPRESSION" in doctrine_id:
        if variant_id in {"COIL_EXPAND", "COMPRESSION_SWING", "CAPTURE_EXTENSION", "CAPTURE_FULL"}:
            return "COIL_EXPAND"
        return "COIL_BREAK"
    if "COILED_TRANSITION" in doctrine_id:
        if variant_id in {"TREND_HOLD", "CAPTURE_EXTENSION", "CAPTURE_FULL"}:
            return "COILED_TRANSITION_HOLD"
        return "COILED_TRANSITION_BREAK"
    if "EXPANSION_RELEASE" in doctrine_id:
        if variant_id in {"TREND_HOLD", "CAPTURE_EXTENSION", "CAPTURE_FULL"}:
            return "EXPANSION_RELEASE_HOLD"
        return "EXPANSION_RELEASE_LAUNCH"
    return "GENERAL_CAPTURE"


def _expression_survivor(
    *,
    doctrine_id: str,
    summary: Dict[str, Any],
    segment_rows: List[tuple[str, Dict[str, Any]]],
) -> bool:
    trade_count = int(summary.get("trade_count", 0) or 0)
    expectancy = float(summary.get("expectancy_pips", 0.0) or 0.0)
    net_pnl = float(summary.get("net_pnl_pips", 0.0) or 0.0)
    win_rate = float(summary.get("win_rate", 0.0) or 0.0)
    doctrine_id = str(doctrine_id or "").upper()

    if trade_count >= 2 and expectancy > 0.0 and net_pnl > 0.0:
        return True

    # Track 1 doctrines need a softer first-pass lane so a promising route can
    # instantiate before the doctrine is judged by harsher downstream economics.
    if doctrine_id in INSTANTIATION_LANE_DOCTRINES:
        return (
            trade_count >= 1
            and expectancy > 0.0
            and net_pnl > 0.0
            and win_rate >= 0.33
            and len(segment_rows) >= 1
        )

    return False


def _variant_allowed_for_route(
    *,
    doctrine_id: str,
    expression_id: str,
    segment_type: str,
    segment_value: str,
    variant: Dict[str, Any],
) -> bool:
    doctrine_id = str(doctrine_id or "").upper()
    expression_id = str(expression_id or "").upper()
    segment_type = str(segment_type or "BASE").upper()
    segment_value = str(segment_value or "ALL").upper()
    variant_id = str(variant.get("variant_id", "") or "").upper()

    if not _expression_compatible_with_doctrine(doctrine_id, expression_id):
        return False

    if "OSCILLATION_PRESSURE_BUILD" in doctrine_id:
        if variant_id == "PRESSURE_SWING":
            if segment_type == "PRECURSOR" and segment_value == "PRESSURED":
                return False
            if segment_type == "LIFECYCLE" and segment_value == "PATTERN_HARVEST":
                return False
            if segment_type == "ENERGY" and segment_value in {"DRIFT", "DORMANT"}:
                return False
        if variant_id == "CAPTURE_EXTENSION":
            if segment_type == "LIFECYCLE" and segment_value == "PATTERN_HARVEST":
                return False
            if "LONG" in doctrine_id and segment_type == "PRECURSOR" and segment_value == "PRESSURED":
                return False
        if "LONG" in doctrine_id and variant_id in {"CAPTURE_FULL", "CAPTURE_BALANCED"}:
            if segment_type == "PRECURSOR" and segment_value == "PRESSURED":
                return False

    if "TRANSITION_RELEASE_LONG" in doctrine_id:
        if variant_id == "TRANSITION_RUNNER":
            return False
        if variant_id == "CAPTURE_EXTENSION" and (
            (segment_type == "ENERGY" and segment_value == "IGNITION")
            or (segment_type == "PRECURSOR" and segment_value == "PRESSURED")
        ):
            return False
        if variant_id == "CAPTURE_NEAR" and segment_type == "ENERGY" and segment_value == "IGNITION":
            return False

    if "COMPRESSION_PRESSURE_DROP_SHORT" in doctrine_id:
        if variant_id == "CAPTURE_EXTENSION" and (
            (segment_type == "ENERGY" and segment_value == "DRIFT")
            or (segment_type == "LIFECYCLE" and segment_value == "PATTERN_HARVEST")
        ):
            return False
        if variant_id == "CAPTURE_FULL" and segment_type == "ENERGY" and segment_value == "DORMANT":
            return False

    if "FLOW_DRIFT_SHORT" in doctrine_id and variant_id == "DRIFT_SCALP":
        if segment_type in {"ENERGY", "LIFECYCLE"} and segment_value in {"DORMANT", "PATTERN_HARVEST"}:
            return False
    if "FLOW_DRIFT_SHORT" in doctrine_id and variant_id == "CAPTURE_EXTENSION":
        if segment_type == "ENERGY" and segment_value == "DORMANT":
            return False

    if "FLOW_DRIFT_LONG" in doctrine_id and variant_id == "DRIFT_RIDE":
        return False
    if "FLOW_DRIFT_LONG" in doctrine_id and variant_id == "CAPTURE_SCALP":
        return False
    if "FLOW_DRIFT_LONG" in doctrine_id and variant_id == "CAPTURE_SNAP":
        if segment_type != "ENERGY" or segment_value != "IGNITION":
            return False

    if "COMPRESSION_PRESSURE_LIFT_LONG" in doctrine_id and variant_id == "PRESSURE_EXTENSION":
        if segment_type == "LIFECYCLE" and segment_value == "PATTERN_HARVEST":
            return False
        if segment_type == "PRECURSOR" and segment_value != "BALANCED":
            return False

    if "OSCILLATION_EDGE_SHORT" in doctrine_id and variant_id in {"CAPTURE_EXTENSION", "CAPTURE_FULL"}:
        return False

    if "OSCILLATION_EDGE_LONG" in doctrine_id and variant_id in {"CAPTURE_EXTENSION", "CAPTURE_FULL"}:
        return False
    if "OSCILLATION_EDGE_LONG" in doctrine_id and variant_id == "CAPTURE_BALANCED":
        if segment_type == "LIFECYCLE" and segment_value == "PATTERN_HARVEST":
            return False
        if segment_type == "ENERGY" and segment_value == "DORMANT":
            return False

    return True


def _segment_routes(rows: List[tuple[str, Dict[str, Any]]]) -> List[Dict[str, Any]]:
    groups: Dict[tuple[str, str], List[tuple[str, Dict[str, Any]]]] = {("BASE", "ALL"): rows}
    segment_extractors = {
        "ENERGY": lambda profile: str(profile.get("energy_state", "") or "UNKNOWN"),
        "LIFECYCLE": lambda profile: str(profile.get("lifecycle_stage", "") or "UNKNOWN"),
        "PRECURSOR": lambda profile: str(profile.get("precursor_state", "") or "UNKNOWN"),
        "EPISODE_POSITION": lambda profile: str(profile.get("episode_position", "") or "UNKNOWN"),
    }
    minimum_segment_size = 6
    for segment_type, getter in segment_extractors.items():
        bucketed: Dict[str, List[tuple[str, Dict[str, Any]]]] = defaultdict(list)
        for scenario_name, profile in rows:
            bucketed[getter(profile)].append((scenario_name, profile))
        for segment_value, bucket_rows in bucketed.items():
            if minimum_segment_size <= len(bucket_rows) < len(rows):
                groups[(segment_type, segment_value)] = bucket_rows
    return [
        {
            "segment_type": segment_type,
            "segment_value": segment_value,
            "rows": grouped_rows,
            "specificity": 0 if segment_type == "BASE" else 1,
        }
        for (segment_type, segment_value), grouped_rows in groups.items()
    ]


def _route_matches_profile(profile: Dict[str, Any], route: Dict[str, Any]) -> bool:
    segment_type = str(route.get("segment_type", "BASE") or "BASE")
    segment_value = str(route.get("segment_value", "ALL") or "ALL")
    if segment_type == "BASE":
        return True
    profile_value_map = {
        "ENERGY": str(profile.get("energy_state", "") or "UNKNOWN"),
        "LIFECYCLE": str(profile.get("lifecycle_stage", "") or "UNKNOWN"),
        "PRECURSOR": str(profile.get("precursor_state", "") or "UNKNOWN"),
        "EPISODE_POSITION": str(profile.get("episode_position", "") or "UNKNOWN"),
    }
    return profile_value_map.get(segment_type) == segment_value


def _variant_profile_score(profile: Dict[str, Any], route: Dict[str, Any]) -> float:
    variant = route["selected_variant"]
    score = 0.0
    bucket = str(profile.get("target_distance_bucket", "") or "").upper()
    energy = str(profile.get("energy_state", "") or "").upper()
    lifecycle = str(profile.get("lifecycle_stage", "") or "").upper()
    variant_id = str(variant.get("variant_id", "") or "").upper()
    target_multiplier = float(variant.get("target_multiplier", 1.0) or 1.0)
    ttl_multiplier = float(variant.get("ttl_multiplier", 1.0) or 1.0)
    entry_offset = int(variant.get("entry_offset_ticks", 0) or 0)
    operating_mode = str(route.get("route_operating_mode", "") or "")
    precursor = str(profile.get("precursor_state", "") or "").upper()

    if bucket in {"MICRO", "SMALL"}:
        score += 1.5 if target_multiplier <= 0.65 else 0.0
        score += 1.0 if entry_offset <= 1 else 0.0
    if bucket in {"LARGE", "EXTENDED"}:
        score += 1.5 if target_multiplier >= 0.8 else 0.0
        score += 1.0 if ttl_multiplier >= 0.9 else 0.0
    if energy == "IGNITION":
        score += 1.0 if entry_offset == 0 else 0.0
        score += 0.5 if ttl_multiplier <= 1.0 else 0.0
    if energy in {"DRIFT", "DORMANT"}:
        score += 1.0 if entry_offset >= 1 else 0.0
        score += 0.5 if ttl_multiplier >= 0.7 else 0.0
    if lifecycle in {"RUN_EXTENSION", "LATE"}:
        score += 1.0 if target_multiplier >= 0.9 else 0.0
    if lifecycle == "RELEASE":
        score += 1.0 if entry_offset == 0 else 0.0

    if "CONFIRM" in variant_id and energy in {"DRIFT", "DORMANT"}:
        score += 0.75
    if "SCALP" in variant_id and bucket in {"MICRO", "SMALL"}:
        score += 0.75
    if "RUNNER" in variant_id and bucket in {"LARGE", "EXTENDED"}:
        score += 0.75
    if "BREAK" in variant_id and energy in {"IGNITION", "DRIVE"}:
        score += 0.75
    if operating_mode == "IGNITION_RELEASE_FAST" and energy == "IGNITION":
        score += 1.5
    if operating_mode == "RELEASE_CONFIRM" and energy in {"DRIFT", "DORMANT"}:
        score += 0.75
    if operating_mode == "DRIFT_REACCEL" and precursor == "PRESSURED":
        score += 1.25
    if operating_mode == "DRIFT_CONFIRM" and energy == "DORMANT":
        score += 0.75
    if operating_mode == "LONG_DRIFT_REACCEL":
        score += 1.2
    if operating_mode == "LONG_DRIFT_IGNITION" and energy == "IGNITION":
        score += 1.3
    if operating_mode == "COMPRESSION_SWING":
        score += 1.1 if bucket in {"LARGE", "EXTENDED"} else 0.4
    if operating_mode == "PRESSURE_LIFT_CAPTURE" and bucket in {"MICRO", "SMALL", "MEDIUM"}:
        score += 0.9
    if operating_mode == "HARD_REJECTION":
        score += 1.1
    if operating_mode == "HARD_REBOUND":
        score += 0.8
    if operating_mode == "PRESSURE_BUILD_CAPTURE":
        score += 0.5
    return score


def build_phase2_candidate(
    *,
    profile: Dict[str, Any],
    doctrine: Dict[str, Any],
    ticks: List[Dict[str, Any]],
    variant: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    if variant is None:
        variant = {"variant_id": "CAPTURE_FULL", "target_multiplier": 1.0, "stop_multiplier": 1.0, "ttl_multiplier": 1.0, "entry_offset_ticks": 0}
    anchor_index = int(profile.get("anchor_index", -1) or -1) + int(variant.get("entry_offset_ticks", 0) or 0)
    direction = str(profile.get("direction_group", "") or "").upper()
    if anchor_index < 0 or anchor_index >= len(ticks):
        return {"status": "ABORTED", "reason": "anchor_out_of_range", "strategy_id": doctrine["doctrine_id"]}
    if direction not in {"LONG", "SHORT"}:
        return {"status": "ABORTED", "reason": "direction_missing", "strategy_id": doctrine["doctrine_id"]}

    doctrine_id = str(doctrine.get("doctrine_id", "") or "")
    target_distance = float(profile.get("discovered_distance_pips", 0.0) or 0.0)
    if target_distance <= 0.0:
        return {"status": "ABORTED", "reason": "distance_missing", "strategy_id": doctrine["doctrine_id"]}

    tick = ticks[anchor_index]
    pip_size = 0.01 if "JPY" in str(profile.get("profile_id", "")).upper() else 0.0001
    entry_price = float(tick["ask"] if direction == "LONG" else tick["bid"])
    friction = float(profile.get("friction_threshold_pips", 0.0) or 0.0)
    path_budget = float(profile.get("path_discovery_pips", 0.0) or 0.0)
    boundary_width = float(profile.get("boundary_width_pips", 0.0) or 0.0)
    target_distance = max(friction, target_distance * float(variant["target_multiplier"]))
    stop_distance = max(friction, float(profile.get("discovered_distance_pips", 0.0) or 0.0) * float(variant["stop_multiplier"]))
    if "OSCILLATION_EDGE" in doctrine_id:
        target_distance = max(friction, min(target_distance, max(path_budget * 0.7, friction * 1.5)))
        stop_distance = max(friction, min(stop_distance, friction * 2.2))
    elif "RAW_COMPRESSION_CEILING_REJECTION_SHORT" in doctrine_id:
        target_distance = max(friction, min(max(path_budget, friction * 2.0), target_distance))
        stop_distance = max(friction, min(stop_distance, friction * 2.4))
    elif "FAILED_BREAK_LONG_RECLAIM" in doctrine_id:
        target_distance = max(friction * 1.5, min(max(boundary_width * 0.6, friction * 2.0), target_distance * 1.1))
        stop_distance = max(friction, min(stop_distance, friction * 2.6))
    elif "BALANCED_SURFACE_LONG_PUSH" in doctrine_id:
        target_distance = max(friction * 2.0, max(target_distance, boundary_width * 0.8))
        stop_distance = max(friction, min(stop_distance, max(boundary_width * 0.45, friction * 2.2)))
    elif "TRANSITION_RELEASE" in doctrine_id:
        target_distance = max(friction * 2.0, max(target_distance, path_budget * 0.9))
        stop_distance = max(friction, min(stop_distance, max(boundary_width * 0.4, friction * 2.0)))
    elif "OSCILLATION_PRESSURE_BUILD" in doctrine_id:
        target_distance = max(friction * 1.5, min(max(path_budget * 0.9, friction * 2.0), max(target_distance, boundary_width * 0.5)))
        stop_distance = max(friction, min(stop_distance, max(boundary_width * 0.35, friction * 2.0)))
    elif "FLOW_DRIFT" in doctrine_id:
        target_distance = max(friction * 1.2, min(max(path_budget * 0.55, friction * 1.5), target_distance * 0.85))
        stop_distance = max(friction, min(stop_distance, max(boundary_width * 0.22, friction * 1.35)))
    elif "PRESSURE_DRIVE" in doctrine_id:
        target_distance = max(friction * 1.35, min(max(path_budget * 0.7, friction * 1.7), target_distance * 0.9))
        stop_distance = max(friction, min(stop_distance, max(boundary_width * 0.28, friction * 1.5)))
    elif "COILED_COMPRESSION" in doctrine_id:
        target_distance = max(friction * 1.4, max(target_distance, boundary_width * 0.55))
        stop_distance = max(friction, min(stop_distance, max(boundary_width * 0.3, friction * 1.7)))
    elif "COMPRESSION_PRESSURE_LIFT" in doctrine_id or "COMPRESSION_PRESSURE_DROP" in doctrine_id:
        target_distance = max(friction * 1.7, max(target_distance, boundary_width * 0.65))
        stop_distance = max(friction, min(stop_distance, max(boundary_width * 0.35, friction * 2.0)))
    elif "COMPRESSION_RELEASE" in doctrine_id:
        target_distance = max(friction * 1.8, max(target_distance, path_budget * 0.95))
        stop_distance = max(friction, min(stop_distance, max(boundary_width * 0.32, friction * 2.1)))
    elif "COILED_TRANSITION" in doctrine_id:
        route_mode = _route_operating_mode(
            doctrine_id=doctrine_id,
            expression_id=str(profile.get("distance_family_id", "") or ""),
            segment_type="BASE",
            segment_value="ALL",
            variant=variant,
        )
        if route_mode == "COILED_TRANSITION_BREAK":
            target_distance = max(friction * 1.45, min(max(path_budget * 0.72, friction * 1.7), target_distance * 0.92))
            stop_distance = max(friction, min(stop_distance, max(boundary_width * 0.24, friction * 1.45)))
        else:
            target_distance = max(friction * 1.65, min(max(path_budget * 0.84, friction * 1.9), target_distance))
            stop_distance = max(friction, min(stop_distance, max(boundary_width * 0.28, friction * 1.62)))
    elif "EXPANSION_RELEASE" in doctrine_id:
        target_distance = max(friction * 2.0, max(target_distance, path_budget))
        stop_distance = max(friction, min(stop_distance, max(boundary_width * 0.4, friction * 2.2)))
    target_price = entry_price + target_distance * pip_size if direction == "LONG" else entry_price - target_distance * pip_size
    stop_price = entry_price - stop_distance * pip_size if direction == "LONG" else entry_price + stop_distance * pip_size
    ttl_end = min(len(ticks) - 1, anchor_index + max(3, int(round(_ttl_ticks(profile) * float(variant["ttl_multiplier"])))))

    return {
        "status": "READY",
        "strategy_id": doctrine["doctrine_id"],
        "doctrine_id": doctrine["doctrine_id"],
        "cluster_id": doctrine["doctrine_id"],
        "direction": direction,
        "profile_id": profile["profile_id"],
        "distance_expression_id": str(profile.get("distance_family_id", "") or ""),
        "target_distance_bucket": str(profile.get("target_distance_bucket", "") or ""),
        "anchor_index": anchor_index,
        "entry_price": round(entry_price, 6),
        "target_price": round(target_price, 6),
        "stop_price": round(stop_price, 6),
        "ttl_end_index": ttl_end,
        "ttl_ticks": ttl_end - anchor_index,
        "tier1_variant_id": str(variant["variant_id"]),
        "tier1_entry_offset_ticks": int(variant.get("entry_offset_ticks", 0) or 0),
        "phase": "PHASE_2",
    }


def _pick_expression_rules(
    *,
    scenario_state: Dict[str, Dict[str, Any]],
    doctrines: List[Dict[str, Any]],
    commission_pips: float,
    slippage_pips: float,
) -> Dict[str, Dict[str, Any]]:
    assignments: Dict[str, List[tuple[str, Dict[str, Any]]]] = defaultdict(list)

    for scenario_name, state in scenario_state.items():
        for profile in state["profiles"]:
            if not _is_raw_opportunity(profile):
                continue
            doctrine_id = assign_profile_to_cluster(profile, doctrines)
            if not doctrine_id:
                continue
            assignments[doctrine_id].append((scenario_name, profile))

    selected_rules: Dict[str, Dict[str, Any]] = {}
    for doctrine in doctrines:
        doctrine_id = str(doctrine["doctrine_id"])
        expression_groups: Dict[str, List[tuple[str, Dict[str, Any]]]] = defaultdict(list)
        for scenario_name, profile in assignments.get(doctrine_id, []):
            expression_id = str(profile.get("distance_family_id", "") or "")
            if not _expression_compatible_with_doctrine(doctrine_id, expression_id):
                continue
            expression_groups[expression_id].append((scenario_name, profile))

        doctrine_rules: Dict[str, List[Dict[str, Any]]] = {}
        variants = _variant_specs_for_doctrine(doctrine)
        for expression_id, rows in expression_groups.items():
            survivor_routes: List[Dict[str, Any]] = []
            segment_routes = _segment_routes(rows)
            for segment in segment_routes:
                segment_rows = segment["rows"]
                for variant in variants:
                    if not _variant_allowed_for_route(
                        doctrine_id=doctrine_id,
                        expression_id=expression_id,
                        segment_type=segment["segment_type"],
                        segment_value=segment["segment_value"],
                        variant=variant,
                    ):
                        continue
                    results: List[Dict[str, Any]] = []
                    for scenario_name, profile in segment_rows:
                        ticks = scenario_state[scenario_name]["ticks"]
                        candidate = build_phase2_candidate(
                            profile=profile,
                            doctrine=doctrine,
                            ticks=ticks,
                            variant=variant,
                        )
                        results.append(
                            evaluate_candidate(
                                candidate=candidate,
                                ticks=ticks,
                                commission_pips=commission_pips,
                                slippage_pips=slippage_pips,
                            )
                        )
                    if not results:
                        continue
                    summary = summarize_strategy_results(results)[0]
                    summary = {
                        **summary,
                        "variant_id": variant["variant_id"],
                    }
                    expression_survivor = _expression_survivor(
                        doctrine_id=doctrine_id,
                        summary=summary,
                        segment_rows=segment_rows,
                    )
                    if not expression_survivor:
                        continue
                    route_operating_mode = _route_operating_mode(
                        doctrine_id=doctrine_id,
                        expression_id=expression_id,
                        segment_type=segment["segment_type"],
                        segment_value=segment["segment_value"],
                        variant=variant,
                    )
                    survivor_routes.append(
                        {
                            "expression_id": expression_id,
                            "route_id": f"{expression_id}|{segment['segment_type']}|{segment['segment_value']}|{variant['variant_id']}",
                            "segment_type": segment["segment_type"],
                            "segment_value": segment["segment_value"],
                            "specificity": segment["specificity"],
                            "route_operating_mode": route_operating_mode,
                            "selected_variant": variant,
                            "summary": summary,
                            "survivor": True,
                            "instantiation_lane": bool(
                                str(doctrine_id or "").upper() in INSTANTIATION_LANE_DOCTRINES
                                and int(summary.get("trade_count", 0) or 0) == 1
                            ),
                        }
                    )

            survivor_routes.sort(
                key=lambda row: (
                    -int(row["specificity"]),
                    -(float(row["summary"]["expectancy_pips"])),
                    -(float(row["summary"]["net_pnl_pips"])),
                    -(int(row["summary"]["trade_count"])),
                    str(row["route_id"]),
                )
            )
            doctrine_rules[expression_id] = survivor_routes[:8]
        selected_rules[doctrine_id] = doctrine_rules
    return selected_rules


def evaluate_phase2_doctrines(
    *,
    scenario_state: Dict[str, Dict[str, Any]],
    doctrines: List[Dict[str, Any]],
    commission_pips: float,
    slippage_pips: float,
) -> Dict[str, Any]:
    selected_rules = _pick_expression_rules(
        scenario_state=scenario_state,
        doctrines=doctrines,
        commission_pips=commission_pips,
        slippage_pips=slippage_pips,
    )
    result_rows: List[Dict[str, Any]] = []
    candidates: List[Dict[str, Any]] = []
    assignment_count = 0
    raw_opportunity_count = 0
    captured_profile_ids: set[str] = set()
    uncaptured_profiles: List[Dict[str, Any]] = []

    for scenario_name, state in scenario_state.items():
        ticks = state["ticks"]
        profiles = state["profiles"]
        for profile in profiles:
            if not _is_raw_opportunity(profile):
                continue
            raw_opportunity_count += 1
            doctrine_id = assign_profile_to_cluster(profile, doctrines)
            if not doctrine_id:
                uncaptured_profiles.append(profile)
                continue
            doctrine = next(row for row in doctrines if row["doctrine_id"] == doctrine_id)
            expression_id = str(profile.get("distance_family_id", "") or "")
            expression_routes = selected_rules.get(doctrine_id, {}).get(expression_id, [])
            matching_routes = [
                route
                for route in expression_routes
                if bool(route.get("survivor")) and _route_matches_profile(profile, route)
            ]
            if not matching_routes:
                uncaptured_profiles.append(profile)
                continue
            chosen_route = max(
                matching_routes,
                key=lambda route: (
                    int(route.get("specificity", 0)),
                    _variant_profile_score(profile, route),
                    float(route["summary"]["expectancy_pips"]),
                    float(route["summary"]["net_pnl_pips"]),
                    int(route["summary"]["trade_count"]),
                    str(route["route_id"]),
                ),
            )
            candidate = build_phase2_candidate(
                profile=profile,
                doctrine=doctrine,
                ticks=ticks,
                variant=chosen_route["selected_variant"],
            )
            candidates.append({"scenario": scenario_name, **candidate})
            assignment_count += 1
            captured_profile_ids.add(f"{scenario_name}|{profile['profile_id']}")
            result_rows.append(
                {
                    "scenario": scenario_name,
                    **evaluate_candidate(
                        candidate=candidate,
                        ticks=ticks,
                        commission_pips=commission_pips,
                        slippage_pips=slippage_pips,
                    ),
                }
            )

    doctrine_summaries = summarize_strategy_results(result_rows)
    doctrine_lookup = {row["strategy_id"]: row for row in doctrine_summaries}
    expression_rule_summary: Dict[str, List[Dict[str, Any]]] = {}
    for doctrine_id, expression_rules in selected_rules.items():
        expression_rule_summary[doctrine_id] = [
            {
                "expression_id": rule["expression_id"],
                "route_id": rule["route_id"],
                "survivor": rule["survivor"],
                "specificity": rule["specificity"],
                "segment_type": rule["segment_type"],
                "segment_value": rule["segment_value"],
                "route_operating_mode": rule.get("route_operating_mode", "GENERAL_CAPTURE"),
                "selected_variant": rule["selected_variant"],
                "selected_variant_id": rule["selected_variant"]["variant_id"],
                "instantiation_lane": bool(rule.get("instantiation_lane")),
                "trade_count": rule["summary"]["trade_count"],
                "win_rate": rule["summary"]["win_rate"],
                "expectancy_pips": rule["summary"]["expectancy_pips"],
                "net_pnl_pips": rule["summary"]["net_pnl_pips"],
            }
            for rule in sorted(
                [route for routes in expression_rules.values() for route in routes],
                key=lambda row: (
                    not row["survivor"],
                    -int(row["specificity"]),
                    -(row["summary"]["expectancy_pips"]),
                    -(row["summary"]["net_pnl_pips"]),
                    row["route_id"],
                ),
            )
        ]
    uncaptured_doctrine_counts = Counter(_doctrine_state(row)[0] for row in uncaptured_profiles)
    uncaptured_distance_counts = Counter(str(row.get("distance_family_id", "") or "") for row in uncaptured_profiles)
    uncaptured_signature_counts = Counter(str(row.get("extraction_signature", "") or "") for row in uncaptured_profiles)
    return {
        "raw_opportunity_count": raw_opportunity_count,
        "assignment_count": assignment_count,
        "candidate_count": len(candidates),
        "result_count": len(result_rows),
        "captured_profile_count": len(captured_profile_ids),
        "captured_profile_share": round(len(captured_profile_ids) / max(raw_opportunity_count, 1), 6),
        "uncaptured_profile_count": len(uncaptured_profiles),
        "uncaptured_profile_share": round(len(uncaptured_profiles) / max(raw_opportunity_count, 1), 6),
        "uncaptured_doctrine_states": dict(uncaptured_doctrine_counts.most_common(20)),
        "uncaptured_pattern_match_states": dict(uncaptured_doctrine_counts.most_common(20)),
        "uncaptured_distance_families": dict(uncaptured_distance_counts.most_common(20)),
        "uncaptured_extraction_signatures": dict(uncaptured_signature_counts.most_common(20)),
        "candidates": candidates,
        "results": result_rows,
        "strategy_summaries": doctrine_summaries,
        "strategy_summary_lookup": doctrine_lookup,
        "expression_rule_summary": expression_rule_summary,
    }
