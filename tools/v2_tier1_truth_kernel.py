from __future__ import annotations

import math
from typing import Any, Dict, List


def _round(value: Any, digits: int = 6) -> float:
    try:
        out = float(value or 0.0)
    except Exception:
        return 0.0
    if math.isnan(out) or math.isinf(out):
        return 0.0
    return round(out, digits)


def _band(value: float, *, low: float, high: float, labels: tuple[str, str, str]) -> str:
    if value <= low:
        return labels[0]
    if value >= high:
        return labels[2]
    return labels[1]


def _direction_sign(value: str) -> float:
    value = str(value or "").upper()
    if value in {"LONG", "UP"}:
        return 1.0
    if value in {"SHORT", "DOWN"}:
        return -1.0
    return 0.0


def _macro_bias_kernel(profile: Dict[str, Any], scenario_profiles: List[Dict[str, Any]] | None) -> str:
    if not scenario_profiles:
        return "UNSPECIFIED"
    anchor = int(profile.get("anchor_index", 0) or 0)
    window = scenario_profiles[max(0, anchor - 48) : anchor + 1]
    if len(window) < 8:
        return "UNSPECIFIED"
    directional_energy = 0.0
    absolute_energy = 0.0
    for row in window:
        sign = _direction_sign(row.get("vector_bias", ""))
        magnitude = abs(_round(row.get("velocity_pips_per_sec", 0.0))) + max(0.0, _round(row.get("impulse_ratio", 0.0)) * 0.25)
        directional_energy += sign * magnitude
        absolute_energy += magnitude
    normalized = directional_energy / max(absolute_energy, 1e-9)
    if normalized >= 0.2:
        return "BULLISH"
    if normalized <= -0.2:
        return "BEARISH"
    return "NEUTRAL"


def _htf_zone_kernel(profile: Dict[str, Any]) -> str:
    direction = str(profile.get("direction_group", "UNKNOWN") or "UNKNOWN").upper()
    zone_state = str(profile.get("zone_state", "UNKNOWN") or "UNKNOWN").upper()
    long_level = str(profile.get("long_level_type", "UNKNOWN") or "UNKNOWN").upper()
    short_level = str(profile.get("short_level_type", "UNKNOWN") or "UNKNOWN").upper()
    long_quality = _round(profile.get("long_retest_quality", 0.0))
    short_quality = _round(profile.get("short_retest_quality", 0.0))

    if direction == "LONG" and zone_state == "NEAR_FLOOR" and long_quality >= 0.8:
        return "HTF_SUPPORT"
    if direction == "SHORT" and zone_state == "NEAR_CEILING" and short_quality >= 0.8:
        return "HTF_RESISTANCE"
    if long_level in {"ROUND_NUMBER", "SESSION_OPEN"} or short_level in {"ROUND_NUMBER", "SESSION_OPEN"}:
        return "REFERENCE_LEVEL"
    if zone_state == "MID_ZONE":
        return "MID_VOID"
    return "EDGE_REFERENCE"


def _liquidity_map_kernel(profile: Dict[str, Any]) -> str:
    book_toxicity = _round(profile.get("book_toxicity_proxy", 0.0))
    rejection_velocity = _round(profile.get("rejection_velocity", 0.0))
    order_flow = _round(profile.get("order_flow_imbalance", 0.0))
    zone_state = str(profile.get("zone_state", "UNKNOWN") or "UNKNOWN").upper()

    if book_toxicity >= 1.8:
        return "TOXIC_THIN"
    if zone_state in {"NEAR_CEILING", "NEAR_FLOOR"} and rejection_velocity >= 0.8:
        return "SWEEP_READY"
    if abs(order_flow) >= 0.35 and rejection_velocity <= 0.35:
        return "FLOW_IMBALANCED"
    return "BALANCED_BOOK"


def build_truth_kernel(profile: Dict[str, Any], scenario_profiles: List[Dict[str, Any]] | None = None) -> Dict[str, Any]:
    direction_alignment = _round(profile.get("direction_alignment_score", 0.0))
    book_toxicity = _round(profile.get("book_toxicity_proxy", 0.0))
    displacement = _round(profile.get("displacement_score", 0.0))
    session_elapsed = _round(profile.get("session_elapsed_pct", 0.0))
    order_flow = _round(profile.get("order_flow_imbalance", 0.0))
    rejection_velocity = _round(profile.get("rejection_velocity", 0.0))
    compression_energy = _round(profile.get("compression_energy_score", 0.0))

    kernel = {
        "profile_id": str(profile.get("profile_id", "") or ""),
        "movement_kernel": {
            "movement_state": str(profile.get("movement_state", "UNKNOWN") or "UNKNOWN"),
            "cost_covering_state": str(profile.get("cost_covering_state", "UNKNOWN") or "UNKNOWN"),
            "path_covering_state": str(profile.get("path_covering_state", "UNKNOWN") or "UNKNOWN"),
            "extractable": bool(profile.get("extractable")),
            "displacement_band": _band(displacement, low=1.0, high=2.0, labels=("WEAK", "WORKABLE", "STRONG")),
            "target_distance_bucket": str(profile.get("target_distance_bucket", "UNKNOWN") or "UNKNOWN"),
        },
        "structure_kernel": {
            "surface_type": str(profile.get("surface_type", "UNKNOWN") or "UNKNOWN"),
            "market_pattern_state": str(profile.get("market_pattern_state", "UNKNOWN") or "UNKNOWN"),
            "zone_state": str(profile.get("zone_state", "UNKNOWN") or "UNKNOWN"),
            "topology_family_id": str(profile.get("topology_family_id", "UNKNOWN") or "UNKNOWN"),
            "location_relation_id": str(profile.get("location_relation_id", "UNKNOWN") or "UNKNOWN"),
            "level_type_long": str(profile.get("long_level_type", "UNKNOWN") or "UNKNOWN"),
            "level_type_short": str(profile.get("short_level_type", "UNKNOWN") or "UNKNOWN"),
        },
        "direction_kernel": {
            "direction_group": str(profile.get("direction_group", "UNKNOWN") or "UNKNOWN"),
            "vector_bias": str(profile.get("vector_bias", "UNKNOWN") or "UNKNOWN"),
            "order_flow_band": str(profile.get("order_flow_band", "UNKNOWN") or "UNKNOWN"),
            "direction_alignment_band": _band(direction_alignment, low=0.1, high=0.55, labels=("MISALIGNED", "PARTIAL", "ALIGNED")),
            "structural_room_band": _band(
                max(
                    _round(profile.get("long_space_above_pips", 0.0)),
                    _round(profile.get("short_space_below_pips", 0.0)),
                ),
                low=3.0,
                high=8.0,
                labels=("TIGHT", "WORKABLE", "OPEN"),
            ),
        },
        "precursor_kernel": {
            "precursor_state": str(profile.get("precursor_state", "UNKNOWN") or "UNKNOWN"),
            "precursor_family_id": str(profile.get("precursor_family_id", "UNKNOWN") or "UNKNOWN"),
            "lifecycle_stage": str(profile.get("lifecycle_stage", "UNKNOWN") or "UNKNOWN"),
            "pressure_band": _band(abs(_round(profile.get("precursor_pressure_score", 0.0))), low=0.2, high=0.6, labels=("LIGHT", "MODERATE", "HEAVY")),
        },
        "energy_kernel": {
            "energy_state": str(profile.get("energy_state", "UNKNOWN") or "UNKNOWN"),
            "energy_family_id": str(profile.get("energy_family_id", "UNKNOWN") or "UNKNOWN"),
            "impulse_band": _band(_round(profile.get("impulse_ratio", 0.0)), low=0.4, high=1.0, labels=("FADE", "BUILD", "SURGE")),
            "compression_energy_band": _band(compression_energy, low=0.2, high=0.55, labels=("LOW", "MEDIUM", "HIGH")),
            "rejection_velocity_band": _band(rejection_velocity, low=0.35, high=0.8, labels=("SOFT", "WORKABLE", "HARD")),
        },
        "quality_kernel": {
            "book_quality_band": _band(book_toxicity, low=1.05, high=1.6, labels=("CLEAN", "WORKABLE", "TOXIC")),
            "session_progress_band": _band(session_elapsed, low=0.25, high=0.8, labels=("OPENING", "MID", "LATE")),
            "order_flow_alignment_band": _band(abs(order_flow), low=0.1, high=0.25, labels=("WEAK", "WORKABLE", "STRONG")),
            "opportunity_confidence_tier": str(profile.get("opportunity_confidence_tier", "UNKNOWN") or "UNKNOWN"),
        },
        "event_kernel": {
            "live_recognition_state": str(profile.get("live_recognition_state", "UNKNOWN") or "UNKNOWN"),
            "pattern_match_state": str(profile.get("pattern_match_state", "UNKNOWN") or "UNKNOWN"),
            "trigger_state": str(profile.get("trigger_state", "UNKNOWN") or "UNKNOWN"),
        },
        "global_slots": {
            "macro_bias_kernel": _macro_bias_kernel(profile, scenario_profiles),
            "htf_zone_kernel": _htf_zone_kernel(profile),
            "liquidity_map_kernel": _liquidity_map_kernel(profile),
        },
    }

    kernel["kernel_signature"] = "|".join(
        [
            kernel["structure_kernel"]["surface_type"],
            kernel["structure_kernel"]["zone_state"],
            kernel["direction_kernel"]["direction_group"],
            kernel["precursor_kernel"]["precursor_state"],
            kernel["energy_kernel"]["energy_state"],
            kernel["event_kernel"]["trigger_state"],
        ]
    )
    return kernel
