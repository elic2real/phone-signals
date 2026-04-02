from __future__ import annotations

import math
from typing import Any, Dict, List


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _ffloat(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value or 0.0)
    except Exception:
        return default
    if math.isnan(out) or math.isinf(out):
        return default
    return out


def _truth_kernel(profile: Dict[str, Any]) -> Dict[str, Any]:
    return dict(profile.get("truth_kernel", {}) or {})


def _global_slot(profile: Dict[str, Any], field: str, default: str = "UNSPECIFIED") -> str:
    kernel = _truth_kernel(profile)
    return str(((kernel.get("global_slots", {}) or {}).get(field, default) or default))


def _doctrine_projection_modifiers(doctrine_id: str) -> Dict[str, float]:
    doctrine_id = str(doctrine_id or "")
    modifiers = {
        "target_multiplier": 1.0,
        "stop_multiplier": 1.0,
        "ttl_multiplier": 1.0,
    }
    if "TRANSITION_RELEASE_SHORT" in doctrine_id:
        modifiers.update({"target_multiplier": 1.16, "stop_multiplier": 0.92, "ttl_multiplier": 0.8})
    elif "TRANSITION_RELEASE_LONG" in doctrine_id:
        modifiers.update({"target_multiplier": 1.08, "stop_multiplier": 0.97, "ttl_multiplier": 0.84})
    elif "COMPRESSION_PRESSURE_LIFT" in doctrine_id:
        modifiers.update({"target_multiplier": 1.12, "stop_multiplier": 0.9, "ttl_multiplier": 1.05})
    elif "COMPRESSION_PRESSURE_DROP" in doctrine_id:
        modifiers.update({"target_multiplier": 0.98, "stop_multiplier": 1.02, "ttl_multiplier": 0.96})
    elif "OSCILLATION_PRESSURE_BUILD_LONG" in doctrine_id:
        modifiers.update({"target_multiplier": 0.96, "stop_multiplier": 0.94, "ttl_multiplier": 0.96})
    elif "OSCILLATION_PRESSURE_BUILD_SHORT" in doctrine_id:
        modifiers.update({"target_multiplier": 0.86, "stop_multiplier": 1.04, "ttl_multiplier": 0.9})
    elif "OSCILLATION_EDGE_LONG" in doctrine_id:
        modifiers.update({"target_multiplier": 0.82, "stop_multiplier": 0.88, "ttl_multiplier": 0.82})
    elif "OSCILLATION_EDGE_SHORT" in doctrine_id:
        modifiers.update({"target_multiplier": 0.84, "stop_multiplier": 0.9, "ttl_multiplier": 0.84})
    elif "FLOW_DRIFT_SHORT" in doctrine_id:
        modifiers.update({"target_multiplier": 0.96, "stop_multiplier": 0.92, "ttl_multiplier": 0.94})
    elif "FLOW_DRIFT_LONG" in doctrine_id:
        modifiers.update({"target_multiplier": 0.92, "stop_multiplier": 0.94, "ttl_multiplier": 0.98})
    elif "FAILED_PUSH_" in doctrine_id:
        modifiers.update({"target_multiplier": 0.74, "stop_multiplier": 0.86, "ttl_multiplier": 0.72})
    elif "PRESSURE_DRIVE" in doctrine_id:
        modifiers.update({"target_multiplier": 0.92, "stop_multiplier": 0.94, "ttl_multiplier": 0.88})
    return modifiers


def _regime_projection_modifiers(regime_state: str) -> Dict[str, float]:
    if regime_state == "EXPANSION":
        return {"target_multiplier": 1.06, "stop_multiplier": 0.96, "ttl_multiplier": 0.9}
    if regime_state == "COMPRESSION":
        return {"target_multiplier": 0.94, "stop_multiplier": 0.98, "ttl_multiplier": 1.08}
    return {"target_multiplier": 1.0, "stop_multiplier": 1.0, "ttl_multiplier": 1.0}


def _session_projection_modifiers(session_state: str) -> Dict[str, float]:
    if session_state == "INJECTING":
        return {"target_multiplier": 1.04, "stop_multiplier": 0.97, "ttl_multiplier": 0.92}
    if session_state == "BLEEDING":
        return {"target_multiplier": 0.94, "stop_multiplier": 1.01, "ttl_multiplier": 0.9}
    return {"target_multiplier": 1.0, "stop_multiplier": 1.0, "ttl_multiplier": 1.0}


def _macro_bias_projection_modifiers(macro_bias_kernel: str, direction_group: str) -> Dict[str, float]:
    macro_bias_kernel = str(macro_bias_kernel or "UNSPECIFIED").upper()
    direction_group = str(direction_group or "UNKNOWN").upper()
    if macro_bias_kernel == "UNSPECIFIED" or macro_bias_kernel == "NEUTRAL" or direction_group not in {"LONG", "SHORT"}:
        return {"target_multiplier": 1.0, "stop_multiplier": 1.0, "ttl_multiplier": 1.0}
    aligned = (
        (macro_bias_kernel == "BULLISH" and direction_group == "LONG")
        or (macro_bias_kernel == "BEARISH" and direction_group == "SHORT")
    )
    if aligned:
        return {"target_multiplier": 1.08, "stop_multiplier": 0.96, "ttl_multiplier": 0.94}
    return {"target_multiplier": 0.86, "stop_multiplier": 1.08, "ttl_multiplier": 0.82}


def _htf_zone_projection_modifiers(htf_zone_kernel: str, direction_group: str) -> Dict[str, float]:
    htf_zone_kernel = str(htf_zone_kernel or "UNSPECIFIED").upper()
    direction_group = str(direction_group or "UNKNOWN").upper()
    if htf_zone_kernel == "HTF_SUPPORT" and direction_group == "LONG":
        return {"target_multiplier": 1.08, "stop_multiplier": 0.95, "ttl_multiplier": 1.02}
    if htf_zone_kernel == "HTF_RESISTANCE" and direction_group == "SHORT":
        return {"target_multiplier": 1.08, "stop_multiplier": 0.95, "ttl_multiplier": 1.02}
    if htf_zone_kernel == "REFERENCE_LEVEL":
        return {"target_multiplier": 1.03, "stop_multiplier": 0.98, "ttl_multiplier": 0.98}
    if htf_zone_kernel == "MID_VOID":
        return {"target_multiplier": 0.9, "stop_multiplier": 1.04, "ttl_multiplier": 0.88}
    return {"target_multiplier": 1.0, "stop_multiplier": 1.0, "ttl_multiplier": 1.0}


def _liquidity_projection_modifiers(liquidity_map_kernel: str) -> Dict[str, float]:
    liquidity_map_kernel = str(liquidity_map_kernel or "UNSPECIFIED").upper()
    if liquidity_map_kernel == "SWEEP_READY":
        return {"target_multiplier": 1.08, "stop_multiplier": 0.96, "ttl_multiplier": 0.9}
    if liquidity_map_kernel == "FLOW_IMBALANCED":
        return {"target_multiplier": 1.04, "stop_multiplier": 0.98, "ttl_multiplier": 0.94}
    if liquidity_map_kernel == "TOXIC_THIN":
        return {"target_multiplier": 0.84, "stop_multiplier": 1.08, "ttl_multiplier": 0.8}
    return {"target_multiplier": 1.0, "stop_multiplier": 1.0, "ttl_multiplier": 1.0}


def _combine_projection_layers(
    base_target_distance_pips: float,
    base_stop_distance_pips: float,
    base_ttl_sec: float,
    doctrine_modifiers: Dict[str, float],
    regime_modifiers: Dict[str, float],
    session_modifiers: Dict[str, float],
    macro_bias_modifiers: Dict[str, float],
    htf_zone_modifiers: Dict[str, float],
    liquidity_modifiers: Dict[str, float],
    friction: float,
) -> Dict[str, float]:
    target_distance_pips = base_target_distance_pips
    stop_distance_pips = base_stop_distance_pips
    ttl_sec = base_ttl_sec
    for layer in (
        doctrine_modifiers,
        regime_modifiers,
        session_modifiers,
        macro_bias_modifiers,
        htf_zone_modifiers,
        liquidity_modifiers,
    ):
        target_distance_pips *= float(layer["target_multiplier"])
        stop_distance_pips *= float(layer["stop_multiplier"])
        ttl_sec *= float(layer["ttl_multiplier"])
    target_distance_pips = max(friction * 1.35, target_distance_pips)
    stop_distance_pips = max(friction * 1.15, stop_distance_pips)
    ttl_sec = max(6.0, ttl_sec)
    return {
        "expected_target_distance_pips": round(target_distance_pips, 6),
        "expected_ttl_sec": round(ttl_sec, 6),
        "stop_distance_pips": round(stop_distance_pips, 6),
    }


def build_context_snapshot(
    *,
    profile: Dict[str, Any],
    scenario_profiles: List[Dict[str, Any]],
    cluster: Dict[str, Any],
) -> Dict[str, Any]:
    velocity_values = sorted(abs(_ffloat(row.get("velocity_pips_per_sec", 0.0), 0.0)) for row in scenario_profiles)
    anchor_velocity = abs(_ffloat(profile.get("velocity_pips_per_sec", 0.0), 0.0))
    percentile_index = sum(1 for value in velocity_values if value <= anchor_velocity)
    volatility_percentile = percentile_index / max(len(velocity_values), 1)

    compression_ratio = _ffloat(profile.get("compression_ratio", 0.0), 0.0)
    if volatility_percentile >= 0.67 and compression_ratio <= 0.60:
        regime_state = "EXPANSION"
    elif volatility_percentile <= 0.33 and compression_ratio >= 0.60:
        regime_state = "COMPRESSION"
    else:
        regime_state = "BALANCED"

    local_window = scenario_profiles[max(0, int(profile["anchor_index"]) - 5) : int(profile["anchor_index"]) + 1]
    local_activity = sum(abs(_ffloat(row.get("velocity_pips_per_sec", 0.0), 0.0)) for row in local_window) / max(len(local_window), 1)
    baseline_activity = sum(abs(_ffloat(row.get("velocity_pips_per_sec", 0.0), 0.0)) for row in scenario_profiles) / max(len(scenario_profiles), 1)
    activity_ratio = local_activity / max(baseline_activity, 1e-9)
    if activity_ratio >= 1.25:
        session_state = "INJECTING"
    elif activity_ratio <= 0.8:
        session_state = "BLEEDING"
    else:
        session_state = "STABLE"

    instrument = str(profile["profile_id"]).split(":")[0]
    average_boundary_width = sum(_ffloat(row.get("boundary_width_pips", 0.0), 0.0) for row in scenario_profiles) / max(len(scenario_profiles), 1)
    activity_factor = _clamp(baseline_activity / 0.35, 0.85, 1.75)
    spatial_factor = _clamp(average_boundary_width / 7.5, 0.85, 1.6)
    instrument_multiplier = round(_clamp((activity_factor * 0.6) + (spatial_factor * 0.4), 0.85, 1.8), 6)
    energy_floor = max(0.2, _ffloat(cluster.get("average_abs_velocity", 0.0), 0.0) * 0.6 * instrument_multiplier)
    direction_group = str(profile.get("direction_group", "UNKNOWN") or "UNKNOWN")
    macro_bias_kernel = _global_slot(profile, "macro_bias_kernel")
    htf_zone_kernel = _global_slot(profile, "htf_zone_kernel")
    liquidity_map_kernel = _global_slot(profile, "liquidity_map_kernel")

    boundary_width = _ffloat(profile.get("boundary_width_pips", 0.0), 0.0)
    friction = _ffloat(profile.get("friction_threshold_pips", 0.0), 0.0)
    energy_state = str(profile.get("energy_state", "DORMANT") or "DORMANT")
    ttl_map = {
        "IGNITION": 8.0,
        "DRIVE": 14.0,
        "DRIFT": 22.0,
        "DORMANT": 30.0,
    }
    base_ttl_sec = ttl_map.get(energy_state, 20.0) * instrument_multiplier
    base_target_distance_pips = max(
        friction * 1.5,
        min(boundary_width * 0.45, max(anchor_velocity * base_ttl_sec * 0.35, friction * 1.8)),
    )
    base_stop_distance_pips = max(
        friction * 1.25,
        min(boundary_width * 0.32, max(anchor_velocity * base_ttl_sec * 0.2, friction * 1.4)),
    )
    doctrine_modifiers = _doctrine_projection_modifiers(str(cluster.get("doctrine_id", "") or ""))
    regime_modifiers = _regime_projection_modifiers(regime_state)
    session_modifiers = _session_projection_modifiers(session_state)
    macro_bias_modifiers = _macro_bias_projection_modifiers(macro_bias_kernel, direction_group)
    htf_zone_modifiers = _htf_zone_projection_modifiers(htf_zone_kernel, direction_group)
    liquidity_modifiers = _liquidity_projection_modifiers(liquidity_map_kernel)
    projection_axis = _combine_projection_layers(
        base_target_distance_pips=base_target_distance_pips,
        base_stop_distance_pips=base_stop_distance_pips,
        base_ttl_sec=base_ttl_sec,
        doctrine_modifiers=doctrine_modifiers,
        regime_modifiers=regime_modifiers,
        session_modifiers=session_modifiers,
        macro_bias_modifiers=macro_bias_modifiers,
        htf_zone_modifiers=htf_zone_modifiers,
        liquidity_modifiers=liquidity_modifiers,
        friction=friction,
    )

    return {
        "regime_state": regime_state,
        "volatility_percentile": round(volatility_percentile, 6),
        "session_state": session_state,
        "session_activity_ratio": round(activity_ratio, 6),
        "instrument": instrument,
        "instrument_multiplier": round(instrument_multiplier, 6),
        "macro_bias_kernel": macro_bias_kernel,
        "htf_zone_kernel": htf_zone_kernel,
        "liquidity_map_kernel": liquidity_map_kernel,
        "macro_bias_alignment": (
            "ALIGNED"
            if (macro_bias_kernel == "BULLISH" and direction_group == "LONG") or (macro_bias_kernel == "BEARISH" and direction_group == "SHORT")
            else "COUNTER"
            if macro_bias_kernel in {"BULLISH", "BEARISH"} and direction_group in {"LONG", "SHORT"}
            else "NEUTRAL"
        ),
        "projection_axis": projection_axis,
        "projection_layers": {
            "base": {
                "expected_target_distance_pips": round(base_target_distance_pips, 6),
                "expected_ttl_sec": round(base_ttl_sec, 6),
                "stop_distance_pips": round(base_stop_distance_pips, 6),
            },
            "doctrine": doctrine_modifiers,
            "regime": regime_modifiers,
            "session": session_modifiers,
            "global_truth": {
                "macro_bias": macro_bias_modifiers,
                "htf_zone": htf_zone_modifiers,
                "liquidity_map": liquidity_modifiers,
            },
        },
        "energy_floor": round(energy_floor, 6),
    }
