from __future__ import annotations

import math
import statistics
from dataclasses import asdict, dataclass
from typing import Any, Dict, Iterable, List


def _ffloat(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
    except Exception:
        return default
    if math.isnan(out) or math.isinf(out):
        return default
    return out


def infer_pip_size(instrument: str) -> float:
    return 0.01 if "JPY" in str(instrument or "").upper() else 0.0001


def _median(values: Iterable[float], default: float = 0.0) -> float:
    clean: List[float] = []
    for value in values:
        coerced = _ffloat(value, float("nan"))
        if math.isnan(coerced) or math.isinf(coerced):
            continue
        clean.append(coerced)
    if not clean:
        return default
    return float(statistics.median(clean))


@dataclass
class Phase1Config:
    spread_anomaly_multiplier: float = 3.0
    jump_sigma_multiplier: float = 6.0
    displacement_window: int = 8
    compression_short_window: int = 12
    compression_long_window: int = 48
    velocity_window: int = 5
    acceleration_window: int = 3
    commission_pips: float = 0.1
    slippage_pips: float = 0.05
    epsilon: float = 1e-9


@dataclass
class SanitizedTick:
    instrument: str
    ts: float
    bid: float
    ask: float
    mid: float
    spread_pips: float
    index: int
    data_confidence: str


@dataclass
class Phase1Profile:
    profile_id: str
    anchor_index: int
    gross_movement_pips: float
    displacement_score: float
    net_displacement_pips: float
    friction_threshold_pips: float
    usable_available_pips: float
    path_discovery_pips: float
    extractable: bool
    conservative_opportunity: bool
    aggressive_path_opportunity: bool
    pattern_qualified_opportunity: bool
    movement_state: str
    cost_covering_state: str
    path_covering_state: str
    opportunity_confidence_tier: str
    distance_to_floor_pips: float
    distance_to_ceiling_pips: float
    compression_ratio: float
    boundary_width_pips: float
    zone_state: str
    time_in_zone_sec: float
    velocity_pips_per_sec: float
    acceleration_pips_per_sec2: float
    energy_state: str
    tick_cadence_sec: float
    velocity_pips_per_bar: float
    acceleration_pips_per_bar: float
    impulse_ratio: float
    vector_bias: str
    precursor_state: str
    precursor_pressure_score: float
    precursor_width_pips: float
    precursor_duration_bars: int
    market_pattern_state: str
    surface_type: str
    pattern_match_state: str
    live_recognition_state: str
    trigger_state: str
    direction_group: str
    distance_mode: str
    discovered_distance_pips: float
    target_distance_bucket: str
    extraction_signature: str
    doctrine_family_id: str
    precursor_family_id: str
    topology_family_id: str
    location_relation_id: str
    distance_family_id: str
    energy_family_id: str
    order_flow_imbalance: float
    order_flow_band: str
    compression_energy_score: float
    rejection_velocity: float
    book_toxicity_proxy: float
    session_elapsed_pct: float
    direction_alignment_score: float
    long_retest_quality: float
    long_seller_exhaustion: bool
    long_ofi_alignment: bool
    long_level_type: str
    long_space_above_pips: float
    short_retest_quality: float
    short_buyer_exhaustion: bool
    short_ofi_alignment: bool
    short_level_type: str
    short_space_below_pips: float
    payload_status: str
    lifecycle_stage: str
    parent_pattern_id: str
    parent_episode_id: str
    opportunity_episode_id: str
    overlap_group_id: str
    episode_position: str
    data_confidence: str


def sanitize_ticks(raw_ticks: List[Dict[str, Any]], config: Phase1Config) -> Dict[str, Any]:
    if not raw_ticks:
        return {"ticks": [], "summary": {"input_count": 0, "kept_count": 0, "dropped_count": 0, "drop_reasons": {}}}

    enriched: List[Dict[str, Any]] = []
    for idx, row in enumerate(raw_ticks):
        bid = _ffloat(row.get("bid"), float("nan"))
        ask = _ffloat(row.get("ask"), float("nan"))
        ts = _ffloat(row.get("ts"), float("nan"))
        instrument = str(row.get("instrument", "") or "UNKNOWN")
        if not math.isfinite(bid) or not math.isfinite(ask) or not math.isfinite(ts):
            enriched.append({"drop_reason": "invalid_numeric", "index": idx})
            continue
        if ask <= 0.0 or bid <= 0.0 or ask <= bid:
            enriched.append({"drop_reason": "invalid_quote", "index": idx})
            continue
        mid = _ffloat(row.get("mid"), (bid + ask) / 2.0)
        spread_pips = (ask - bid) / infer_pip_size(instrument)
        enriched.append(
            {
                "instrument": instrument,
                "ts": ts,
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spread_pips": spread_pips,
                "volume": row.get("volume"),
                "data_confidence": str(row.get("data_confidence") or "DIRECT_QUOTE"),
                "index": idx,
            }
        )

    valid = [row for row in enriched if "drop_reason" not in row]
    median_spread = _median((row["spread_pips"] for row in valid), default=0.0)
    mid_changes: List[float] = []
    prev_mid: float | None = None
    for row in valid:
        if prev_mid is not None:
            mid_changes.append(abs(row["mid"] - prev_mid))
        prev_mid = row["mid"]
    median_mid_change = _median(mid_changes, default=0.0)

    kept: List[SanitizedTick] = []
    drop_reasons: Dict[str, int] = {}
    last_ts: float | None = None
    last_mid: float | None = None
    for row in enriched:
        reason = row.get("drop_reason")
        if reason is None:
            if last_ts is not None and row["ts"] <= last_ts:
                reason = "non_monotonic_timestamp"
            elif median_spread > 0.0 and row["spread_pips"] > median_spread * config.spread_anomaly_multiplier:
                reason = "spread_anomaly"
            elif last_mid is not None:
                jump = abs(row["mid"] - last_mid)
                jump_limit = max(config.epsilon, median_mid_change * config.jump_sigma_multiplier)
                volume = row.get("volume")
                zero_volume = volume is not None and _ffloat(volume, 0.0) <= 0.0
                if jump > jump_limit and (zero_volume or row["spread_pips"] > median_spread * 1.5):
                    reason = "synthetic_jump"
        if reason is not None:
            drop_reasons[reason] = drop_reasons.get(reason, 0) + 1
            continue

        tick = SanitizedTick(
            instrument=row["instrument"],
            ts=row["ts"],
            bid=row["bid"],
            ask=row["ask"],
            mid=row["mid"],
            spread_pips=row["spread_pips"],
            index=row["index"],
            data_confidence=row["data_confidence"],
        )
        kept.append(tick)
        last_ts = tick.ts
        last_mid = tick.mid

    return {
        "ticks": kept,
        "summary": {
            "input_count": len(raw_ticks),
            "kept_count": len(kept),
            "dropped_count": len(raw_ticks) - len(kept),
            "median_spread_pips": median_spread,
            "median_mid_change": median_mid_change,
            "drop_reasons": drop_reasons,
        },
    }


def _window_slice(items: List[SanitizedTick], end_index: int, window: int) -> List[SanitizedTick]:
    start = max(0, end_index - window + 1)
    return items[start : end_index + 1]


def _zone_state(distance_floor: float, distance_ceiling: float, width_pips: float) -> str:
    if width_pips <= 0.0:
        return "UNDEFINED"
    floor_ratio = distance_floor / max(width_pips, 1e-9)
    ceiling_ratio = distance_ceiling / max(width_pips, 1e-9)
    if floor_ratio <= 0.2:
        return "NEAR_FLOOR"
    if ceiling_ratio <= 0.2:
        return "NEAR_CEILING"
    return "MID_ZONE"


def _classify_market_pattern(*, compression_ratio: float, zone_state: str, velocity: float, acceleration: float) -> str:
    abs_velocity = abs(velocity)
    abs_acceleration = abs(acceleration)
    if compression_ratio <= 0.30:
        return "COMPRESSION"
    if compression_ratio >= 0.80 and abs_velocity >= 1.0:
        return "EXPANSION"
    if zone_state in {"NEAR_FLOOR", "NEAR_CEILING"} and abs_velocity <= 1.0:
        return "OSCILLATION"
    if abs_acceleration >= 0.75:
        return "TRANSITION"
    return "BALANCED"


def _precursor_snapshot(pre_window: List[SanitizedTick], pip_size: float, epsilon: float) -> tuple[str, float, float, int]:
    if len(pre_window) < 3:
        return "UNDEFINED", 0.0, 0.0, len(pre_window)
    total_move = 0.0
    signed_move = 0.0
    for idx in range(1, len(pre_window)):
        change = (pre_window[idx].mid - pre_window[idx - 1].mid) / pip_size
        total_move += abs(change)
        signed_move += change
    directional_pressure = signed_move / max(total_move, epsilon)
    pre_width = (max(t.mid for t in pre_window) - min(t.mid for t in pre_window)) / pip_size
    if pre_width <= 1.5:
        precursor_state = "COILED"
    elif abs(directional_pressure) >= 0.55:
        precursor_state = "PRESSURED"
    else:
        precursor_state = "BALANCED"
    return precursor_state, round(directional_pressure, 6), round(pre_width, 6), len(pre_window)


def _match_pattern(
    *,
    precursor_state: str,
    market_pattern_state: str,
    zone_state: str,
    vector_bias: str,
    location_relation_id: str,
    energy_state: str,
) -> tuple[str, str]:
    if market_pattern_state == "OSCILLATION" and zone_state == "NEAR_CEILING" and vector_bias == "DOWN":
        return "OSCILLATION_EDGE_SHORT", "RECOGNIZE_OSCILLATION_SHORT_EDGE"
    if market_pattern_state == "OSCILLATION" and zone_state == "NEAR_FLOOR" and vector_bias == "UP":
        return "OSCILLATION_EDGE_LONG", "RECOGNIZE_OSCILLATION_LONG_EDGE"
    if (
        market_pattern_state == "OSCILLATION"
        and location_relation_id == "MID_BALANCE"
        and precursor_state == "COILED"
        and energy_state in {"DRIFT", "DORMANT"}
    ):
        side = "LONG" if vector_bias == "UP" else "SHORT" if vector_bias == "DOWN" else "NEUTRAL"
        return f"OSCILLATION_ROTATION_{side}", f"RECOGNIZE_OSCILLATION_ROTATION_{side}"
    if market_pattern_state == "OSCILLATION" and location_relation_id == "CEILING_PRESSURE" and vector_bias == "UP":
        return "OSCILLATION_PRESSURE_BUILD_LONG", "RECOGNIZE_OSCILLATION_PRESSURE_LONG"
    if market_pattern_state == "OSCILLATION" and location_relation_id == "FLOOR_PRESSURE" and vector_bias == "DOWN":
        return "OSCILLATION_PRESSURE_BUILD_SHORT", "RECOGNIZE_OSCILLATION_PRESSURE_SHORT"
    if market_pattern_state == "COMPRESSION" and precursor_state == "COILED":
        side = "LONG" if vector_bias == "UP" else "SHORT" if vector_bias == "DOWN" else "NEUTRAL"
        return f"COILED_COMPRESSION_{side}", f"RECOGNIZE_COMPRESSION_{side}"
    if market_pattern_state == "COMPRESSION" and energy_state in {"IGNITION", "DRIVE"}:
        side = "LONG" if vector_bias == "UP" else "SHORT" if vector_bias == "DOWN" else "NEUTRAL"
        return f"COMPRESSION_RELEASE_{side}", f"RECOGNIZE_COMPRESSION_RELEASE_{side}"
    if market_pattern_state == "COMPRESSION" and location_relation_id == "CEILING_PRESSURE" and vector_bias == "UP":
        return "COMPRESSION_PRESSURE_LIFT_LONG", "RECOGNIZE_COMPRESSION_LIFT_LONG"
    if market_pattern_state == "COMPRESSION" and location_relation_id == "FLOOR_PRESSURE" and vector_bias == "DOWN":
        return "COMPRESSION_PRESSURE_DROP_SHORT", "RECOGNIZE_COMPRESSION_DROP_SHORT"
    if market_pattern_state == "EXPANSION" and precursor_state in {"COILED", "PRESSURED"}:
        side = "LONG" if vector_bias == "UP" else "SHORT" if vector_bias == "DOWN" else "NEUTRAL"
        return f"EXPANSION_RELEASE_{side}", f"RECOGNIZE_EXPANSION_{side}"
    if market_pattern_state == "TRANSITION" and precursor_state in {"PRESSURED", "BALANCED"}:
        side = "LONG" if vector_bias == "UP" else "SHORT" if vector_bias == "DOWN" else "NEUTRAL"
        return f"TRANSITION_RELEASE_{side}", f"RECOGNIZE_TRANSITION_{side}"
    if market_pattern_state == "TRANSITION" and precursor_state == "COILED":
        side = "LONG" if vector_bias == "UP" else "SHORT" if vector_bias == "DOWN" else "NEUTRAL"
        return f"COILED_TRANSITION_{side}", f"RECOGNIZE_COILED_TRANSITION_{side}"
    if market_pattern_state == "BALANCED" and precursor_state == "PRESSURED":
        side = "LONG" if vector_bias == "UP" else "SHORT" if vector_bias == "DOWN" else "NEUTRAL"
        return f"PRESSURE_DRIVE_{side}", f"RECOGNIZE_PRESSURE_{side}"
    if market_pattern_state == "BALANCED" and location_relation_id == "MID_DRIFT" and energy_state in {"DRIFT", "DRIVE", "IGNITION"}:
        side = "LONG" if vector_bias == "UP" else "SHORT" if vector_bias == "DOWN" else "NEUTRAL"
        return f"FLOW_DRIFT_{side}", f"RECOGNIZE_FLOW_{side}"
    if market_pattern_state == "BALANCED" and location_relation_id == "MID_BALANCE" and energy_state in {"DRIFT", "DRIVE"}:
        side = "LONG" if vector_bias == "UP" else "SHORT" if vector_bias == "DOWN" else "NEUTRAL"
        return f"BALANCED_ROTATION_{side}", f"RECOGNIZE_BALANCED_ROTATION_{side}"
    if vector_bias == "FLAT" and market_pattern_state in {"OSCILLATION", "COMPRESSION", "EXPANSION", "TRANSITION"}:
        return "INTENTIONAL_REJECT_NO_DIRECTION", "RECOGNIZE_INTENTIONAL_REJECT_NO_DIRECTION"
    return "UNMATCHED", "RECOGNIZE_NONE"


def _compression_band(compression_ratio: float) -> str:
    if compression_ratio <= 0.20:
        return "TIGHT"
    if compression_ratio <= 0.40:
        return "COMPRESSED"
    if compression_ratio <= 0.70:
        return "BALANCED"
    return "OPEN"


def _pressure_band(precursor_pressure_score: float) -> str:
    magnitude = abs(precursor_pressure_score)
    if magnitude < 0.20:
        return "LIGHT"
    if magnitude < 0.55:
        return "MODERATE"
    if magnitude < 0.80:
        return "HEAVY"
    return "EXTREME"


def _surface_type(market_pattern_state: str) -> str:
    mapping = {
        "COMPRESSION": "COMPRESSION",
        "BALANCED": "BALANCED",
        "OSCILLATION": "OSCILLATION",
        "TRANSITION": "TRENDING",
        "EXPANSION": "TRENDING",
    }
    return mapping.get(market_pattern_state, "BALANCED")


def _ofi_proxy(pre_window: List[SanitizedTick], pip_size: float, epsilon: float) -> tuple[float, str]:
    if len(pre_window) < 3:
        return 0.0, "BALANCED"
    signed_flow = 0.0
    total_flow = 0.0
    for idx in range(1, len(pre_window)):
        change = (pre_window[idx].mid - pre_window[idx - 1].mid) / pip_size
        signed_flow += change
        total_flow += abs(change)
    ofi = max(-1.0, min(1.0, signed_flow / max(total_flow, epsilon)))
    if ofi > 0.35:
        band = "STRONG_BUY"
    elif ofi > 0.10:
        band = "MILD_BUY"
    elif ofi < -0.35:
        band = "STRONG_SELL"
    elif ofi < -0.10:
        band = "MILD_SELL"
    else:
        band = "BALANCED"
    return round(ofi, 6), band


def _compression_energy_score(
    short_window: List[SanitizedTick],
    long_window: List[SanitizedTick],
    pip_size: float,
    epsilon: float,
) -> float:
    if len(short_window) < 3 or len(long_window) < 6:
        return 0.0
    long_high = max(t.mid for t in long_window)
    long_low = min(t.mid for t in long_window)
    range_width_pips = (long_high - long_low) / pip_size
    bar_count_inside = sum(1 for t in short_window if long_low <= t.mid <= long_high)
    atr_proxy = sum(
        abs((long_window[idx].mid - long_window[idx - 1].mid) / pip_size)
        for idx in range(1, len(long_window))
    ) / max(len(long_window) - 1, 1)
    normalized_width = min(1.0, range_width_pips / max(atr_proxy, epsilon))
    ces = (bar_count_inside / max(len(short_window), 1)) * (1.0 - normalized_width)
    return round(max(0.0, min(1.0, ces)), 6)


def _rejection_velocity(
    *,
    test_window: List[SanitizedTick],
    current_mid: float,
    zone_state: str,
    tick_cadence_sec: float,
    pip_size: float,
) -> float:
    if not test_window:
        return 0.0
    if zone_state == "NEAR_CEILING":
        test_extreme = max(t.mid for t in test_window)
        retreat_pips = max(0.0, (test_extreme - current_mid) / pip_size)
    elif zone_state == "NEAR_FLOOR":
        test_extreme = min(t.mid for t in test_window)
        retreat_pips = max(0.0, (current_mid - test_extreme) / pip_size)
    else:
        test_extreme = test_window[-1].mid
        retreat_pips = abs((current_mid - test_extreme) / pip_size)
    minutes = max(tick_cadence_sec / 60.0, 1e-9)
    return round(retreat_pips / minutes, 6)


def _book_toxicity_proxy(spread_pips: float, long_window: List[SanitizedTick]) -> float:
    median_spread = _median((tick.spread_pips for tick in long_window), default=spread_pips)
    return round(spread_pips / max(median_spread, 1e-9), 6)


def _distance_mode(distance_to_reference_pips: float) -> str:
    if distance_to_reference_pips <= 3.0:
        return "SCALP_DISTANCE"
    if distance_to_reference_pips <= 8.0:
        return "STANDARD_DISTANCE"
    if distance_to_reference_pips <= 15.0:
        return "EXTENDED_DISTANCE"
    return "FAR_DISTANCE"


def _session_elapsed_pct(current_index: int, total_count: int) -> float:
    if total_count <= 1:
        return 0.0
    return round(current_index / max(total_count - 1, 1), 6)


def _level_type(
    *,
    zone_state: str,
    current_mid: float,
    opening_mid: float,
    pip_size: float,
) -> str:
    round_anchor = round(current_mid / 0.005) * 0.005
    if abs(current_mid - round_anchor) / pip_size <= 2.0:
        return "ROUND_NUMBER"
    if abs(current_mid - opening_mid) / pip_size <= 2.0:
        return "SESSION_OPEN"
    if zone_state == "NEAR_CEILING":
        return "PRIOR_HIGH"
    if zone_state == "NEAR_FLOOR":
        return "PRIOR_LOW"
    return "VWAP"


def _directional_feature_pack(
    *,
    direction_group: str,
    zone_state: str,
    vector_bias: str,
    distance_floor: float,
    distance_ceiling: float,
    opening_mid: float,
    current_mid: float,
    pip_size: float,
    ofi: float,
    recent_bar_moves: List[float],
) -> Dict[str, Any]:
    opposite_moves = [abs(move) for move in recent_bar_moves[-3:] if move]
    decaying_opposite = len(opposite_moves) >= 2 and opposite_moves[-1] <= opposite_moves[-2]
    long_level_type = _level_type(
        zone_state=zone_state,
        current_mid=current_mid,
        opening_mid=opening_mid,
        pip_size=pip_size,
    )
    short_level_type = long_level_type
    long_retest_quality = 1.0 if zone_state == "NEAR_FLOOR" and vector_bias == "UP" else 0.45 if vector_bias == "UP" else 0.0
    short_retest_quality = 1.0 if zone_state == "NEAR_CEILING" and vector_bias == "DOWN" else 0.45 if vector_bias == "DOWN" else 0.0
    return {
        "long_retest_quality": round(long_retest_quality, 6),
        "long_seller_exhaustion": bool(direction_group == "LONG" and decaying_opposite),
        "long_ofi_alignment": bool(ofi > 0.10),
        "long_level_type": long_level_type,
        "long_space_above_pips": round(distance_ceiling, 6),
        "short_retest_quality": round(short_retest_quality, 6),
        "short_buyer_exhaustion": bool(direction_group == "SHORT" and decaying_opposite),
        "short_ofi_alignment": bool(ofi < -0.10),
        "short_level_type": short_level_type,
        "short_space_below_pips": round(distance_floor, 6),
    }


def _trend_pressure_continuation_match(
    *,
    direction: str,
    surface_type: str,
    location_relation_id: str,
    vector_bias: str,
    precursor_state: str,
    energy_state: str,
    ofi: float,
    distance_mode: str,
    space_pips: float,
    ofi_aligned: bool,
    session_elapsed_pct: float,
) -> bool:
    if surface_type != "TRENDING":
        return False
    expected_location = "CEILING_PRESSURE" if direction == "LONG" else "FLOOR_PRESSURE"
    expected_bias = "UP" if direction == "LONG" else "DOWN"
    if location_relation_id != expected_location or vector_bias != expected_bias:
        return False
    if distance_mode not in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}:
        return False

    directional_ofi = ofi > 0.10 if direction == "LONG" else ofi < -0.10
    strong_ofi = ofi > 0.20 if direction == "LONG" else ofi < -0.20
    pressure_memory = precursor_state == "PRESSURED"
    ignition_flow = energy_state in {"IGNITION", "DRIVE"} and directional_ofi
    structural_room = space_pips > 4.0
    layered_continuation = pressure_memory or strong_ofi or ignition_flow
    late_override = pressure_memory and strong_ofi
    session_open = session_elapsed_pct < 0.80 or (session_elapsed_pct < 0.90 and late_override)
    return bool(ofi_aligned and layered_continuation and session_open and (structural_room or layered_continuation))


def _compression_pressure_release_match(
    *,
    direction: str,
    surface_type: str,
    location_relation_id: str,
    vector_bias: str,
    precursor_state: str,
    energy_state: str,
    ofi: float,
    ces: float,
    distance_mode: str,
    ofi_aligned: bool,
) -> bool:
    if surface_type != "COMPRESSION":
        return False
    expected_location = "CEILING_PRESSURE" if direction == "LONG" else "FLOOR_PRESSURE"
    expected_bias = "UP" if direction == "LONG" else "DOWN"
    if location_relation_id != expected_location or vector_bias != expected_bias:
        return False
    if distance_mode not in {"SCALP_DISTANCE", "STANDARD_DISTANCE"}:
        return False

    strong_ofi = ofi > 0.20 if direction == "LONG" else ofi < -0.20
    directional_ofi = ofi > 0.05 if direction == "LONG" else ofi < -0.05
    compression_memory = ces > 0.35 or precursor_state in {"BALANCED", "PRESSURED", "COILED"}
    pressure_confirmation = bool(ofi_aligned or strong_ofi or directional_ofi)
    release_confirmation = (
        energy_state in {"IGNITION", "DRIVE"}
        or precursor_state == "PRESSURED"
        or (distance_mode == "SCALP_DISTANCE" and energy_state in {"DRIFT", "DORMANT"} and strong_ofi)
    )
    return bool(compression_memory and pressure_confirmation and release_confirmation)


def _trigger_state(
    *,
    pattern_match_state: str,
    surface_type: str,
    market_pattern_state: str,
    zone_state: str,
    vector_bias: str,
    location_relation_id: str,
    precursor_state: str,
    energy_state: str,
    ofi: float,
    ces: float,
    rejection_velocity: float,
    distance_mode: str,
    long_space_above_pips: float,
    short_space_below_pips: float,
    long_ofi_alignment: bool,
    short_ofi_alignment: bool,
    long_seller_exhaustion: bool,
    short_buyer_exhaustion: bool,
    session_elapsed_pct: float,
) -> str:
    if surface_type == "COMPRESSION" and zone_state == "NEAR_CEILING" and vector_bias == "DOWN" and ces > 0.55 and rejection_velocity > 0.8:
        return "FAILED_BREAK"
    if surface_type == "COMPRESSION" and zone_state == "NEAR_FLOOR" and vector_bias == "UP" and ces > 0.50 and rejection_velocity > 0.7:
        return "RECLAIM"
    if _compression_pressure_release_match(
        direction="LONG",
        surface_type=surface_type,
        location_relation_id=location_relation_id,
        vector_bias=vector_bias,
        precursor_state=precursor_state,
        energy_state=energy_state,
        ofi=ofi,
        ces=ces,
        distance_mode=distance_mode,
        ofi_aligned=long_ofi_alignment,
    ):
        return "COMPRESSION_PRESSURE_PUSH"
    if _compression_pressure_release_match(
        direction="SHORT",
        surface_type=surface_type,
        location_relation_id=location_relation_id,
        vector_bias=vector_bias,
        precursor_state=precursor_state,
        energy_state=energy_state,
        ofi=ofi,
        ces=ces,
        distance_mode=distance_mode,
        ofi_aligned=short_ofi_alignment,
    ):
        return "COMPRESSION_PRESSURE_DROP_CONFIRM"
    if surface_type == "COMPRESSION" and energy_state in {"IGNITION", "DRIVE"} and abs(ofi) > 0.10:
        return "ONE_BAR_CONFIRM"
    if surface_type == "OSCILLATION" and zone_state in {"NEAR_CEILING", "NEAR_FLOOR"} and distance_mode == "SCALP_DISTANCE" and rejection_velocity > 0.6:
        return "RETEST_HOLD"
    if surface_type == "OSCILLATION" and location_relation_id == "CEILING_PRESSURE" and vector_bias == "UP" and ofi > 0.0:
        return "CONTINUATION_PUSH"
    if surface_type == "OSCILLATION" and location_relation_id == "FLOOR_PRESSURE" and vector_bias == "DOWN" and ofi < 0.0:
        return "CONTINUATION_PUSH"
    if surface_type == "BALANCED" and vector_bias == "UP" and ces > 0.55 and ofi > 0.20:
        return "ONE_BAR_CONFIRM"
    if surface_type == "BALANCED" and vector_bias == "UP" and long_seller_exhaustion and ofi > 0.0:
        return "RECLAIM"
    if surface_type == "BALANCED" and vector_bias == "DOWN" and short_buyer_exhaustion and ofi < 0.0:
        return "FAILED_PUSH_REVERSE"
    if _trend_pressure_continuation_match(
        direction="LONG",
        surface_type=surface_type,
        location_relation_id=location_relation_id,
        vector_bias=vector_bias,
        precursor_state=precursor_state,
        energy_state=energy_state,
        ofi=ofi,
        distance_mode=distance_mode,
        space_pips=long_space_above_pips,
        ofi_aligned=long_ofi_alignment,
        session_elapsed_pct=session_elapsed_pct,
    ):
        return "TREND_PRESSURE_CONTINUATION_LONG"
    if _trend_pressure_continuation_match(
        direction="SHORT",
        surface_type=surface_type,
        location_relation_id=location_relation_id,
        vector_bias=vector_bias,
        precursor_state=precursor_state,
        energy_state=energy_state,
        ofi=ofi,
        distance_mode=distance_mode,
        space_pips=short_space_below_pips,
        ofi_aligned=short_ofi_alignment,
        session_elapsed_pct=session_elapsed_pct,
    ):
        return "TREND_PRESSURE_CONTINUATION_SHORT"
    if (
        surface_type == "TRENDING"
        and location_relation_id == "MID_DRIFT"
        and vector_bias == "UP"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and energy_state in {"DRIFT", "DRIVE", "IGNITION"}
        and ofi > 0.05
        and session_elapsed_pct < 0.85
    ):
        return "TREND_DRIFT_CONTINUATION_LONG"
    if (
        surface_type == "TRENDING"
        and location_relation_id == "MID_DRIFT"
        and vector_bias == "DOWN"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and energy_state in {"DRIFT", "DRIVE", "IGNITION", "DORMANT"}
        and ofi < -0.05
        and session_elapsed_pct < 0.85
    ):
        return "TREND_DRIFT_CONTINUATION_SHORT"
    if market_pattern_state in {"TRANSITION", "EXPANSION"} and energy_state in {"IGNITION", "DRIVE"}:
        if vector_bias == "DOWN" and ofi < -0.20 and short_space_below_pips > 6.0 and session_elapsed_pct < 0.75:
            return "CONTINUATION_PUSH"
        if vector_bias == "UP" and ofi > 0.20 and long_space_above_pips > 6.0 and session_elapsed_pct < 0.75:
            return "CONTINUATION_PUSH"
    if market_pattern_state in {"TRANSITION", "EXPANSION"} and precursor_state == "COILED" and abs(ofi) > 0.05:
        return "CONTINUATION_PUSH"
    if vector_bias == "DOWN" and short_ofi_alignment and location_relation_id in {"CEILING_REJECTION", "MID_DRIFT"} and rejection_velocity > 0.5:
        return "FAILED_PUSH_REVERSE"
    if vector_bias == "UP" and long_ofi_alignment and location_relation_id in {"FLOOR_REBOUND", "MID_DRIFT"} and (long_seller_exhaustion or rejection_velocity > 0.5):
        return "FAILED_PUSH_REVERSE"
    if (
        pattern_match_state in {"TRANSITION_RELEASE_LONG", "TRANSITION_RELEASE_SHORT"}
        and market_pattern_state == "TRANSITION"
        and energy_state in {"IGNITION", "DRIVE"}
        and location_relation_id in {"MID_DRIFT", "FLOOR_PRESSURE", "CEILING_PRESSURE"}
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and session_elapsed_pct < 0.85
    ):
        return "CONTINUATION_PUSH"
    if (
        pattern_match_state in {"FLOW_DRIFT_LONG", "FLOW_DRIFT_SHORT"}
        and location_relation_id == "MID_DRIFT"
        and energy_state in {"DRIFT", "DRIVE", "IGNITION"}
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and session_elapsed_pct < 0.90
    ):
        return "TREND_DRIFT_CONTINUATION_LONG" if vector_bias == "UP" else "TREND_DRIFT_CONTINUATION_SHORT" if vector_bias == "DOWN" else "NO_CONFIDENT_TRIGGER"
    if (
        pattern_match_state == "COMPRESSION_PRESSURE_LIFT_LONG"
        and location_relation_id == "CEILING_PRESSURE"
        and vector_bias == "UP"
        and energy_state in {"DRIFT", "DRIVE", "IGNITION"}
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and (ces > 0.10 or precursor_state in {"BALANCED", "PRESSURED", "COILED"})
    ):
        return "COMPRESSION_PRESSURE_PUSH"
    if (
        pattern_match_state == "COMPRESSION_PRESSURE_DROP_SHORT"
        and location_relation_id == "FLOOR_PRESSURE"
        and vector_bias == "DOWN"
        and energy_state in {"DRIFT", "DRIVE", "IGNITION"}
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and (ces > 0.10 or precursor_state in {"BALANCED", "PRESSURED", "COILED"})
    ):
        return "COMPRESSION_PRESSURE_DROP_CONFIRM"
    if (
        pattern_match_state in {"PRESSURE_DRIVE_LONG", "PRESSURE_DRIVE_SHORT"}
        and precursor_state == "PRESSURED"
        and location_relation_id == "MID_DRIFT"
        and energy_state in {"DRIFT", "DRIVE", "IGNITION"}
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and session_elapsed_pct < 0.90
    ):
        return "TREND_PRESSURE_CONTINUATION_LONG" if vector_bias == "UP" else "TREND_PRESSURE_CONTINUATION_SHORT" if vector_bias == "DOWN" else "NO_CONFIDENT_TRIGGER"
    if (
        pattern_match_state in {"COILED_COMPRESSION_LONG", "COILED_COMPRESSION_SHORT"}
        and precursor_state == "COILED"
        and market_pattern_state == "COMPRESSION"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE"}
        and session_elapsed_pct < 0.90
        and zone_state in {"NEAR_CEILING", "NEAR_FLOOR", "MID_ZONE"}
    ):
        return "ONE_BAR_CONFIRM"
    if (
        market_pattern_state == "EXPANSION"
        and energy_state in {"IGNITION", "DRIVE"}
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and session_elapsed_pct < 0.85
        and vector_bias in {"UP", "DOWN"}
    ):
        return "CONTINUATION_PUSH"
    return "NO_CONFIDENT_TRIGGER"


def _direction_alignment_score(direction_group: str, vector_bias: str, ofi: float, trigger_state: str) -> float:
    if direction_group not in {"LONG", "SHORT"}:
        return -1.0
    direction_sign = 1.0 if direction_group == "LONG" else -1.0
    vector_sign = 1.0 if vector_bias == "UP" else -1.0 if vector_bias == "DOWN" else 0.0
    ofi_sign = 1.0 if ofi > 0.10 else -1.0 if ofi < -0.10 else 0.0
    long_triggers = {"RECLAIM", "ONE_BAR_CONFIRM", "CONTINUATION_PUSH", "TREND_PRESSURE_CONTINUATION_LONG", "COMPRESSION_PRESSURE_PUSH", "TREND_DRIFT_CONTINUATION_LONG"}
    short_triggers = {"FAILED_BREAK", "FAILED_PUSH_REVERSE", "CONTINUATION_PUSH", "RETEST_HOLD", "TREND_PRESSURE_CONTINUATION_SHORT", "COMPRESSION_PRESSURE_DROP_CONFIRM", "TREND_DRIFT_CONTINUATION_SHORT"}
    trigger_sign = 1.0 if trigger_state in long_triggers and direction_group == "LONG" else -1.0 if trigger_state in short_triggers and direction_group == "SHORT" else 0.0
    score = (direction_sign * vector_sign + direction_sign * ofi_sign + direction_sign * trigger_sign) / 3.0
    return round(max(-1.0, min(1.0, score)), 6)


def _doctrine_family_id(
    *,
    surface_type: str,
    trigger_state: str,
    distance_mode: str,
    direction_group: str,
    location_relation_id: str,
    precursor_state: str,
    energy_state: str,
    ofi: float,
    ces: float,
    rejection_velocity: float,
    btp: float,
    session_elapsed_pct: float,
    long_space_above_pips: float,
    short_space_below_pips: float,
    long_ofi_alignment: bool,
    short_ofi_alignment: bool,
    long_level_type: str,
    short_level_type: str,
    long_seller_exhaustion: bool,
    short_buyer_exhaustion: bool,
) -> str:
    if distance_mode == "FAR_DISTANCE" or direction_group not in {"LONG", "SHORT"} or trigger_state == "NO_CONFIDENT_TRIGGER":
        return "NO_DOCTRINE_MATCH"
    if btp > 2.0:
        return "DEFERRED_TOXIC_BOOK"
    if (
        surface_type == "COMPRESSION"
        and trigger_state == "FAILED_BREAK"
        and direction_group == "SHORT"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE"}
        and short_ofi_alignment
        and rejection_velocity > 0.8
        and btp < 1.5
    ):
        suffix = "SCALP" if distance_mode == "SCALP_DISTANCE" else "STANDARD"
        return f"RAW_COMPRESSION_CEILING_REJECTION_SHORT_{suffix}"
    if (
        surface_type == "OSCILLATION"
        and direction_group == "LONG"
        and location_relation_id == "CEILING_PRESSURE"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and ofi > 0.0
        and btp < 1.6
    ):
        return "OSCILLATION_PRESSURE_BUILD_LONG"
    if (
        surface_type == "OSCILLATION"
        and direction_group == "SHORT"
        and location_relation_id == "FLOOR_PRESSURE"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and ofi < 0.0
        and btp < 1.6
    ):
        return "OSCILLATION_PRESSURE_BUILD_SHORT"
    if (
        trigger_state == "CONTINUATION_PUSH"
        and direction_group == "SHORT"
        and distance_mode in {"STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and ofi < -0.20
        and short_space_below_pips > 6.0
        and btp < 1.3
        and session_elapsed_pct < 0.75
    ):
        suffix = "EXTENDED" if distance_mode == "EXTENDED_DISTANCE" else "STANDARD"
        return f"TRANSITION_RELEASE_SHORT_{suffix}"
    if (
        trigger_state == "TREND_PRESSURE_CONTINUATION_SHORT"
        and direction_group == "SHORT"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and (ofi < -0.10 or short_ofi_alignment)
        and (short_space_below_pips > 4.0 or precursor_state == "PRESSURED" or ofi < -0.20)
        and btp < 1.4
        and (session_elapsed_pct < 0.80 or (session_elapsed_pct < 0.90 and precursor_state == "PRESSURED" and ofi < -0.20))
    ):
        suffix = "EXTENDED" if distance_mode == "EXTENDED_DISTANCE" else "STANDARD"
        return f"TRANSITION_RELEASE_SHORT_{suffix}"
    if (
        trigger_state == "COMPRESSION_PRESSURE_PUSH"
        and direction_group == "LONG"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and (ofi > 0.10 or long_ofi_alignment)
        and (ces > 0.20 or precursor_state in {"BALANCED", "PRESSURED", "COILED"})
        and btp < 1.5
    ):
        return "COMPRESSION_PRESSURE_LIFT_LONG"
    if (
        trigger_state == "COMPRESSION_PRESSURE_DROP_CONFIRM"
        and direction_group == "SHORT"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and (ofi < -0.10 or short_ofi_alignment)
        and (ces > 0.20 or precursor_state in {"BALANCED", "PRESSURED", "COILED"})
        and btp < 1.5
    ):
        return "COMPRESSION_PRESSURE_DROP_SHORT"
    if (
        surface_type == "COMPRESSION"
        and direction_group == "LONG"
        and location_relation_id == "CEILING_PRESSURE"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and ofi > 0.10
        and ces > 0.35
        and btp < 1.5
    ):
        return "COMPRESSION_PRESSURE_LIFT_LONG"
    if (
        surface_type == "COMPRESSION"
        and direction_group == "SHORT"
        and location_relation_id == "FLOOR_PRESSURE"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and ofi < -0.10
        and ces > 0.35
        and btp < 1.5
    ):
        return "COMPRESSION_PRESSURE_DROP_SHORT"
    if (
        surface_type == "COMPRESSION"
        and direction_group == "LONG"
        and energy_state in {"IGNITION", "DRIVE"}
        and distance_mode in {"STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and ofi > 0.10
        and btp < 1.5
    ):
        return "COMPRESSION_RELEASE_LONG"
    if (
        surface_type == "COMPRESSION"
        and direction_group == "SHORT"
        and energy_state in {"IGNITION", "DRIVE"}
        and distance_mode in {"STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and ofi < -0.10
        and btp < 1.5
    ):
        return "COMPRESSION_RELEASE_SHORT"
    if (
        surface_type == "COMPRESSION"
        and precursor_state == "COILED"
        and direction_group == "LONG"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE"}
        and btp < 1.6
    ):
        return "COILED_COMPRESSION_LONG"
    if (
        surface_type == "COMPRESSION"
        and precursor_state == "COILED"
        and direction_group == "SHORT"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE"}
        and btp < 1.6
    ):
        return "COILED_COMPRESSION_SHORT"
    if (
        surface_type == "OSCILLATION"
        and trigger_state == "RETEST_HOLD"
        and direction_group == "SHORT"
        and distance_mode == "SCALP_DISTANCE"
        and rejection_velocity > 0.6
        and btp < 1.4
    ):
        return "OSCILLATION_EDGE_SHORT_SCALP"
    if (
        trigger_state == "RECLAIM"
        and direction_group == "LONG"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE"}
        and ofi > 0.10
        and long_seller_exhaustion
        and long_level_type in {"PRIOR_LOW", "ROUND_NUMBER", "SESSION_OPEN"}
        and long_space_above_pips > 5.0
        and btp < 1.4
    ):
        suffix = "SCALP" if distance_mode == "SCALP_DISTANCE" else "STANDARD"
        return f"FAILED_BREAK_LONG_RECLAIM_{suffix}"
    if (
        surface_type == "BALANCED"
        and trigger_state == "ONE_BAR_CONFIRM"
        and direction_group == "LONG"
        and distance_mode == "STANDARD_DISTANCE"
        and ofi > 0.20
        and ces > 0.55
        and long_space_above_pips > 7.0
        and btp < 1.3
        and session_elapsed_pct < 0.70
    ):
        return "BALANCED_SURFACE_LONG_PUSH_STANDARD"
    if (
        surface_type in {"BALANCED", "TRENDING"}
        and direction_group == "LONG"
        and location_relation_id == "MID_DRIFT"
        and energy_state in {"DRIFT", "DRIVE", "IGNITION"}
        and ofi > 0.0
        and btp < 1.6
    ):
        return "FLOW_DRIFT_LONG"
    if (
        trigger_state == "TREND_DRIFT_CONTINUATION_LONG"
        and direction_group == "LONG"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and ofi > 0.05
        and btp < 1.5
    ):
        return "FLOW_DRIFT_LONG"
    if (
        surface_type in {"BALANCED", "TRENDING"}
        and direction_group == "SHORT"
        and location_relation_id == "MID_DRIFT"
        and energy_state in {"DRIFT", "DRIVE", "IGNITION"}
        and ofi < 0.0
        and btp < 1.6
    ):
        return "FLOW_DRIFT_SHORT"
    if (
        trigger_state == "TREND_DRIFT_CONTINUATION_SHORT"
        and direction_group == "SHORT"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and ofi < -0.05
        and btp < 1.5
    ):
        return "FLOW_DRIFT_SHORT"
    if (
        surface_type == "BALANCED"
        and precursor_state == "PRESSURED"
        and direction_group == "LONG"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and ofi > 0.0
        and btp < 1.6
    ):
        return "PRESSURE_DRIVE_LONG"
    if (
        surface_type == "BALANCED"
        and precursor_state == "PRESSURED"
        and direction_group == "SHORT"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and ofi < 0.0
        and btp < 1.6
    ):
        return "PRESSURE_DRIVE_SHORT"
    if (
        surface_type == "OSCILLATION"
        and trigger_state == "RETEST_HOLD"
        and direction_group == "LONG"
        and distance_mode == "SCALP_DISTANCE"
        and rejection_velocity > 0.6
        and btp < 1.4
    ):
        return "OSCILLATION_EDGE_LONG_SCALP"
    if (
        trigger_state == "CONTINUATION_PUSH"
        and direction_group == "LONG"
        and distance_mode == "STANDARD_DISTANCE"
        and ofi > 0.20
        and long_space_above_pips > 6.0
        and btp < 1.3
        and session_elapsed_pct < 0.75
    ):
        return "TRANSITION_RELEASE_LONG_STANDARD"
    if (
        trigger_state == "TREND_PRESSURE_CONTINUATION_LONG"
        and direction_group == "LONG"
        and distance_mode in {"SCALP_DISTANCE", "STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and (ofi > 0.10 or long_ofi_alignment)
        and (long_space_above_pips > 4.0 or precursor_state == "PRESSURED" or ofi > 0.20)
        and btp < 1.4
        and (session_elapsed_pct < 0.80 or (session_elapsed_pct < 0.90 and precursor_state == "PRESSURED" and ofi > 0.20))
    ):
        return "TRANSITION_RELEASE_LONG_STANDARD"
    if (
        surface_type == "TRENDING"
        and precursor_state == "COILED"
        and direction_group == "LONG"
        and distance_mode in {"STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and ofi > 0.05
        and btp < 1.5
    ):
        return "COILED_TRANSITION_LONG"
    if (
        surface_type == "TRENDING"
        and precursor_state == "COILED"
        and direction_group == "SHORT"
        and distance_mode in {"STANDARD_DISTANCE", "EXTENDED_DISTANCE"}
        and ofi < -0.05
        and btp < 1.5
    ):
        return "COILED_TRANSITION_SHORT"
    if (
        surface_type == "TRENDING"
        and direction_group == "LONG"
        and energy_state in {"IGNITION", "DRIVE"}
        and ofi > 0.10
        and btp < 1.5
    ):
        return "EXPANSION_RELEASE_LONG"
    if (
        surface_type == "TRENDING"
        and direction_group == "SHORT"
        and energy_state in {"IGNITION", "DRIVE"}
        and ofi < -0.10
        and btp < 1.5
    ):
        return "EXPANSION_RELEASE_SHORT"
    if (
        trigger_state == "FAILED_PUSH_REVERSE"
        and direction_group == "SHORT"
        and distance_mode == "SCALP_DISTANCE"
        and ofi < -0.10
        and btp < 1.5
    ):
        return "FAILED_PUSH_SHORT_REVERSAL_SCALP"
    if (
        trigger_state == "FAILED_PUSH_REVERSE"
        and direction_group == "LONG"
        and distance_mode == "SCALP_DISTANCE"
        and ofi > 0.10
        and btp < 1.5
    ):
        return "FAILED_PUSH_LONG_REVERSAL_SCALP"
    if (
        surface_type == "COMPRESSION"
        and trigger_state == "ONE_BAR_CONFIRM"
        and direction_group == "SHORT"
        and distance_mode == "STANDARD_DISTANCE"
        and ofi < -0.20
        and ces > 0.55
    ):
        return "COMPRESSION_FLOOR_BREAKDOWN_SHORT_STANDARD"
    if (
        surface_type == "COMPRESSION"
        and trigger_state == "ONE_BAR_CONFIRM"
        and direction_group == "LONG"
        and distance_mode == "STANDARD_DISTANCE"
        and ofi > 0.20
        and ces > 0.55
    ):
        return "COMPRESSION_CEILING_BREAKOUT_LONG_STANDARD"
    return "NO_DOCTRINE_MATCH"


def _precursor_family_id(
    precursor_state: str,
    precursor_pressure_score: float,
    precursor_width_pips: float,
    precursor_duration_bars: int,
) -> str:
    width_band = "TIGHT" if precursor_width_pips <= 1.5 else "WIDE"
    duration_band = "SHORT" if precursor_duration_bars <= 4 else "LONG"
    pressure_band = _pressure_band(precursor_pressure_score)
    return f"{precursor_state}|{pressure_band}|{width_band}|{duration_band}"


def _topology_family_id(
    market_pattern_state: str,
    zone_state: str,
    compression_ratio: float,
    direction_group: str,
) -> str:
    return f"{market_pattern_state}|{zone_state}|{_compression_band(compression_ratio)}|{direction_group}"


def _location_relation_id(
    zone_state: str,
    vector_bias: str,
    distance_floor: float,
    distance_ceiling: float,
    width_pips: float,
) -> str:
    if width_pips <= 0.0:
        return "UNDEFINED"
    if zone_state == "NEAR_FLOOR":
        return "FLOOR_REBOUND" if vector_bias == "UP" else "FLOOR_PRESSURE"
    if zone_state == "NEAR_CEILING":
        return "CEILING_REJECTION" if vector_bias == "DOWN" else "CEILING_PRESSURE"
    midpoint_delta = abs(distance_floor - distance_ceiling) / max(width_pips, 1e-9)
    if midpoint_delta <= 0.10:
        return "MID_BALANCE"
    return "MID_DRIFT"


def _distance_family_id(
    direction_group: str,
    topology_family_id: str,
    location_relation_id: str,
    target_distance_bucket: str,
) -> str:
    return f"{topology_family_id}|{location_relation_id}|{direction_group}|{target_distance_bucket}"


def _energy_family_id(
    energy_state: str,
    impulse_ratio: float,
    acceleration_per_bar: float,
    friction_threshold_pips: float,
) -> str:
    acceleration_ratio = abs(acceleration_per_bar) / max(friction_threshold_pips, 1e-9)
    if impulse_ratio >= 2.5:
        impulse_band = "SURGE"
    elif impulse_ratio >= 1.5:
        impulse_band = "STRONG"
    elif impulse_ratio >= 0.75:
        impulse_band = "BUILD"
    else:
        impulse_band = "FADE"
    accel_band = "SHARP" if acceleration_ratio >= 0.75 else "SMOOTH"
    return f"{energy_state}|{impulse_band}|{accel_band}"


def _lifecycle_stage(
    *,
    raw_opportunity: bool,
    pattern_match_state: str,
    energy_state: str,
    precursor_state: str,
    zone_state: str,
    time_in_zone_sec: float,
    tick_cadence_sec: float,
    target_distance_bucket: str,
) -> str:
    age_bars = time_in_zone_sec / max(tick_cadence_sec, 1e-9)
    if not raw_opportunity:
        if precursor_state in {"COILED", "PRESSURED"}:
            return "SETUP"
        if zone_state in {"NEAR_FLOOR", "NEAR_CEILING"}:
            return "WATCH"
        return "SCAN"
    if energy_state == "IGNITION":
        return "RELEASE"
    if energy_state == "DRIVE":
        return "EXPLOIT"
    if target_distance_bucket in {"LARGE", "EXTENDED"} and age_bars <= 8:
        return "RUN_EXTENSION"
    if pattern_match_state != "UNMATCHED":
        return "PATTERN_HARVEST"
    if age_bars >= 24:
        return "LATE"
    return "HARVEST"


def _profile_instrument(profile: Phase1Profile) -> str:
    return str(profile.profile_id).split(":", 1)[0]


def _assign_episode_structure(profiles: List[Phase1Profile]) -> None:
    pattern_seq: Dict[str, int] = {}
    raw_episode_seq: Dict[str, int] = {}
    opportunity_seq: Dict[str, int] = {}

    prev_instrument: str | None = None
    prev_pattern_key: str | None = None
    prev_raw_episode_key: str | None = None
    prev_opportunity_key: str | None = None
    current_pattern_id = "NO_PATTERN"
    current_parent_episode_id = "NO_PARENT_EPISODE"
    current_opportunity_episode_id = "NO_OPPORTUNITY_EPISODE"

    for profile in profiles:
        instrument = _profile_instrument(profile)
        if instrument != prev_instrument:
            prev_pattern_key = None
            prev_raw_episode_key = None
            prev_opportunity_key = None
            current_pattern_id = "NO_PATTERN"
            current_parent_episode_id = "NO_PARENT_EPISODE"
            current_opportunity_episode_id = "NO_OPPORTUNITY_EPISODE"
            prev_instrument = instrument

        pattern_key = f"{instrument}|{profile.topology_family_id}"
        if pattern_key != prev_pattern_key:
            pattern_seq[instrument] = pattern_seq.get(instrument, 0) + 1
            current_pattern_id = f"{instrument}:PATTERN:{pattern_seq[instrument]}"
            prev_pattern_key = pattern_key
            prev_raw_episode_key = None
            prev_opportunity_key = None

        profile.parent_pattern_id = current_pattern_id

        if profile.extractable:
            raw_episode_key = f"{current_pattern_id}|{profile.direction_group}|RAW"
            if raw_episode_key != prev_raw_episode_key:
                raw_episode_seq[instrument] = raw_episode_seq.get(instrument, 0) + 1
                current_parent_episode_id = f"{instrument}:RAW_EP:{raw_episode_seq[instrument]}"
                prev_raw_episode_key = raw_episode_key
                prev_opportunity_key = None
            profile.parent_episode_id = current_parent_episode_id

            opportunity_key = f"{current_parent_episode_id}|{profile.distance_family_id}|{profile.energy_family_id}"
            if opportunity_key != prev_opportunity_key:
                opportunity_seq[instrument] = opportunity_seq.get(instrument, 0) + 1
                current_opportunity_episode_id = f"{instrument}:OPP_EP:{opportunity_seq[instrument]}"
                prev_opportunity_key = opportunity_key
            profile.opportunity_episode_id = current_opportunity_episode_id
        else:
            profile.parent_episode_id = "NO_PARENT_EPISODE"
            profile.opportunity_episode_id = "NO_OPPORTUNITY_EPISODE"
            prev_raw_episode_key = None
            prev_opportunity_key = None

        profile.overlap_group_id = f"{instrument}|{profile.distance_family_id}"

    positions: Dict[str, int] = {}
    lengths: Dict[str, int] = {}
    for profile in profiles:
        if profile.opportunity_episode_id != "NO_OPPORTUNITY_EPISODE":
            lengths[profile.opportunity_episode_id] = lengths.get(profile.opportunity_episode_id, 0) + 1
    for profile in profiles:
        if profile.opportunity_episode_id == "NO_OPPORTUNITY_EPISODE":
            profile.episode_position = "NONE"
            continue
        episode_id = profile.opportunity_episode_id
        positions[episode_id] = positions.get(episode_id, 0) + 1
        length = lengths[episode_id]
        pos = positions[episode_id]
        if length <= 1:
            profile.episode_position = "SINGLE"
        elif pos <= max(1, length // 3):
            profile.episode_position = "EARLY"
        elif pos >= max(2, length - (length // 3)):
            profile.episode_position = "LATE"
        else:
            profile.episode_position = "MID"


def _direction_group(vector_bias: str) -> str:
    if vector_bias == "UP":
        return "LONG"
    if vector_bias == "DOWN":
        return "SHORT"
    return "NEUTRAL"


def _discover_distance_pips(
    *,
    net_displacement_pips: float,
    friction_threshold_pips: float,
    usable_available_pips: float,
) -> float:
    discovered = max(usable_available_pips, net_displacement_pips - friction_threshold_pips)
    return max(0.0, discovered)


def _path_discovery_pips(
    *,
    gross_movement_pips: float,
    friction_threshold_pips: float,
    boundary_width_pips: float,
    market_pattern_state: str,
) -> float:
    if market_pattern_state not in {"OSCILLATION", "COMPRESSION", "BALANCED", "TRANSITION"}:
        return 0.0
    path_budget = max(0.0, gross_movement_pips - friction_threshold_pips)
    return max(0.0, min(boundary_width_pips, path_budget))


def _target_distance_bucket(discovered_distance_pips: float, friction_threshold_pips: float) -> str:
    if discovered_distance_pips <= 0.0:
        return "NON_COVERING"
    unit = max(friction_threshold_pips, 1e-9)
    multiple = discovered_distance_pips / unit
    if multiple < 1.0:
        return "MICRO"
    if multiple < 2.0:
        return "SMALL"
    if multiple < 4.0:
        return "MEDIUM"
    if multiple < 8.0:
        return "LARGE"
    return "EXTENDED"


def _extraction_signature(
    *,
    market_pattern_state: str,
    pattern_match_state: str,
    direction_group: str,
    target_distance_bucket: str,
) -> str:
    if pattern_match_state != "UNMATCHED":
        return f"{pattern_match_state}|{direction_group}|{target_distance_bucket}"
    return f"{market_pattern_state}|{direction_group}|{target_distance_bucket}"


def _discover_move_event(
    *,
    profile_id: str,
    instrument: str,
    anchor_index: int,
    movement_state: str,
    direction_group: str,
    vector_bias: str,
    gross_movement_pips: float,
    net_displacement_pips: float,
    displacement_score: float,
    tick_cadence_sec: float,
    velocity_pips_per_sec: float,
    acceleration_pips_per_sec2: float,
    discovered_distance_pips: float,
    target_distance_bucket: str,
    data_confidence: str,
) -> Dict[str, Any]:
    return {
        "profile_id": profile_id,
        "instrument": instrument,
        "anchor_index": anchor_index,
        "movement_state": movement_state,
        "direction_group": direction_group,
        "vector_bias": vector_bias,
        "gross_movement_pips": round(gross_movement_pips, 6),
        "net_displacement_pips": round(net_displacement_pips, 6),
        "displacement_score": round(displacement_score, 6),
        "tick_cadence_sec": round(tick_cadence_sec, 6),
        "velocity_pips_per_sec": round(velocity_pips_per_sec, 6),
        "acceleration_pips_per_sec2": round(acceleration_pips_per_sec2, 6),
        "discovered_distance_pips": round(discovered_distance_pips, 6),
        "target_distance_bucket": target_distance_bucket,
        "data_confidence": data_confidence,
    }


def _evaluate_move_economics(
    *,
    friction_threshold_pips: float,
    usable_available_pips: float,
    path_discovery_pips: float,
    cost_covering_state: str,
    path_covering_state: str,
    conservative_opportunity: bool,
    aggressive_path_opportunity: bool,
    raw_opportunity: bool,
    opportunity_confidence_tier: str,
) -> Dict[str, Any]:
    return {
        "friction_threshold_pips": round(friction_threshold_pips, 6),
        "usable_available_pips": round(usable_available_pips, 6),
        "path_discovery_pips": round(path_discovery_pips, 6),
        "cost_covering_state": cost_covering_state,
        "path_covering_state": path_covering_state,
        "conservative_opportunity": conservative_opportunity,
        "aggressive_path_opportunity": aggressive_path_opportunity,
        "raw_opportunity": raw_opportunity,
        "opportunity_confidence_tier": opportunity_confidence_tier,
    }


def _measure_precursor_context(
    *,
    precursor_state: str,
    precursor_pressure_score: float,
    precursor_width_pips: float,
    precursor_duration_bars: int,
    order_flow_imbalance: float,
    order_flow_band: str,
    compression_energy_score: float,
    rejection_velocity: float,
    book_toxicity_proxy: float,
    direction_alignment_score: float,
) -> Dict[str, Any]:
    return {
        "precursor_state": precursor_state,
        "precursor_pressure_score": round(precursor_pressure_score, 6),
        "precursor_width_pips": round(precursor_width_pips, 6),
        "precursor_duration_bars": precursor_duration_bars,
        "order_flow_imbalance": round(order_flow_imbalance, 6),
        "order_flow_band": order_flow_band,
        "compression_energy_score": round(compression_energy_score, 6),
        "rejection_velocity": round(rejection_velocity, 6),
        "book_toxicity_proxy": round(book_toxicity_proxy, 6),
        "direction_alignment_score": round(direction_alignment_score, 6),
    }


def _build_independent_market_map(
    *,
    profile_id: str,
    mapping_id: str,
    market_pattern_state: str,
    surface_type: str,
    zone_state: str,
    location_relation_id: str,
    compression_ratio: float,
    boundary_width_pips: float,
    distance_to_floor_pips: float,
    distance_to_ceiling_pips: float,
    time_in_zone_sec: float,
    session_elapsed_pct: float,
    topology_family_id: str,
    distance_family_id: str,
    energy_family_id: str,
    precursor_family_id: str,
    lifecycle_stage: str,
) -> Dict[str, Any]:
    return {
        "profile_id": profile_id,
        "mapping_id": mapping_id,
        "market_pattern_state": market_pattern_state,
        "surface_type": surface_type,
        "zone_state": zone_state,
        "location_relation_id": location_relation_id,
        "compression_ratio": round(compression_ratio, 6),
        "boundary_width_pips": round(boundary_width_pips, 6),
        "distance_to_floor_pips": round(distance_to_floor_pips, 6),
        "distance_to_ceiling_pips": round(distance_to_ceiling_pips, 6),
        "time_in_zone_sec": round(time_in_zone_sec, 6),
        "session_elapsed_pct": round(session_elapsed_pct, 6),
        "topology_family_id": topology_family_id,
        "distance_family_id": distance_family_id,
        "energy_family_id": energy_family_id,
        "precursor_family_id": precursor_family_id,
        "lifecycle_stage": lifecycle_stage,
    }


def _fit_opportunity_to_market_map(
    *,
    profile_id: str,
    mapping_id: str,
    direction_group: str,
    distance_mode: str,
    target_distance_bucket: str,
    raw_opportunity: bool,
    conservative_opportunity: bool,
    aggressive_path_opportunity: bool,
    location_relation_id: str,
    market_pattern_state: str,
    surface_type: str,
    zone_state: str,
) -> Dict[str, Any]:
    if not raw_opportunity:
        fit_state = "NO_FIT"
    elif conservative_opportunity:
        fit_state = "STRUCTURED_FIT"
    elif aggressive_path_opportunity:
        fit_state = "PATH_FIT"
    else:
        fit_state = "THIN_FIT"
    return {
        "profile_id": profile_id,
        "mapping_id": mapping_id,
        "fit_state": fit_state,
        "direction_group": direction_group,
        "distance_mode": distance_mode,
        "target_distance_bucket": target_distance_bucket,
        "raw_opportunity": raw_opportunity,
        "conservative_opportunity": conservative_opportunity,
        "aggressive_path_opportunity": aggressive_path_opportunity,
        "location_relation_id": location_relation_id,
        "market_pattern_state": market_pattern_state,
        "surface_type": surface_type,
        "zone_state": zone_state,
    }


def _compile_tier0_handoff(
    *,
    profile_id: str,
    mapping_id: str,
    movement_row: Dict[str, Any],
    economics_row: Dict[str, Any],
    precursor_row: Dict[str, Any],
    mapping_row: Dict[str, Any],
    fit_row: Dict[str, Any],
    directional_pack: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "profile_id": profile_id,
        "mapping_id": mapping_id,
        "direction_group": movement_row["direction_group"],
        "discovered_distance_pips": movement_row["discovered_distance_pips"],
        "target_distance_bucket": movement_row["target_distance_bucket"],
        "movement_state": movement_row["movement_state"],
        "cost_covering_state": economics_row["cost_covering_state"],
        "path_covering_state": economics_row["path_covering_state"],
        "raw_opportunity": economics_row["raw_opportunity"],
        "opportunity_confidence_tier": economics_row["opportunity_confidence_tier"],
        "precursor_state": precursor_row["precursor_state"],
        "precursor_pressure_score": precursor_row["precursor_pressure_score"],
        "order_flow_imbalance": precursor_row["order_flow_imbalance"],
        "compression_energy_score": precursor_row["compression_energy_score"],
        "rejection_velocity": precursor_row["rejection_velocity"],
        "book_toxicity_proxy": precursor_row["book_toxicity_proxy"],
        "direction_alignment_score": precursor_row["direction_alignment_score"],
        "market_pattern_state": mapping_row["market_pattern_state"],
        "surface_type": mapping_row["surface_type"],
        "zone_state": mapping_row["zone_state"],
        "location_relation_id": mapping_row["location_relation_id"],
        "distance_mode": fit_row["distance_mode"],
        "fit_state": fit_row["fit_state"],
        "session_elapsed_pct": mapping_row["session_elapsed_pct"],
        "long_level_type": str(directional_pack["long_level_type"]),
        "short_level_type": str(directional_pack["short_level_type"]),
        "long_space_above_pips": float(directional_pack["long_space_above_pips"]),
        "short_space_below_pips": float(directional_pack["short_space_below_pips"]),
        "long_ofi_alignment": bool(directional_pack["long_ofi_alignment"]),
        "short_ofi_alignment": bool(directional_pack["short_ofi_alignment"]),
        "data_confidence": movement_row["data_confidence"],
    }


def build_phase1_stack(ticks: List[SanitizedTick], config: Phase1Config) -> Dict[str, Any]:
    profiles: List[Phase1Profile] = []
    event_discovery_rows: List[Dict[str, Any]] = []
    market_mapping_rows: List[Dict[str, Any]] = []
    opportunity_fit_rows: List[Dict[str, Any]] = []
    tier0_handoff_rows: List[Dict[str, Any]] = []
    if len(ticks) < max(config.compression_long_window, config.displacement_window, config.velocity_window) + 1:
        return {
            "profiles": profiles,
            "event_discovery_rows": event_discovery_rows,
            "market_mapping_rows": market_mapping_rows,
            "opportunity_fit_rows": opportunity_fit_rows,
            "tier0_handoff_rows": tier0_handoff_rows,
        }

    for idx in range(len(ticks)):
        short_window = _window_slice(ticks, idx, config.compression_short_window)
        long_window = _window_slice(ticks, idx, config.compression_long_window)
        disp_window = _window_slice(ticks, idx, config.displacement_window)
        vel_window = _window_slice(ticks, idx, config.velocity_window)
        pre_window = _window_slice(ticks, max(idx - 1, 0), config.displacement_window)

        if len(long_window) < 4 or len(disp_window) < 2 or len(vel_window) < 2:
            continue

        current = ticks[idx]
        profile_id = f"{current.instrument}:{idx}"
        mapping_id = f"{current.instrument}:MAP:{idx}"
        pip_size = infer_pip_size(current.instrument)

        floor_mid = min(t.mid for t in long_window)
        ceiling_mid = max(t.mid for t in long_window)
        width_pips = (ceiling_mid - floor_mid) / pip_size
        short_width_pips = (max(t.mid for t in short_window) - min(t.mid for t in short_window)) / pip_size
        compression_ratio = short_width_pips / max(width_pips, config.epsilon)

        current_mid = current.mid
        distance_floor = max(0.0, (current_mid - floor_mid) / pip_size)
        distance_ceiling = max(0.0, (ceiling_mid - current_mid) / pip_size)
        zone_state = _zone_state(distance_floor, distance_ceiling, width_pips)

        start_tick = disp_window[0]
        net_displacement_pips = abs(current_mid - start_tick.mid) / pip_size
        gross_movement_pips = sum(
            abs((disp_window[pos].mid - disp_window[pos - 1].mid) / pip_size)
            for pos in range(1, len(disp_window))
        )
        friction_threshold_pips = current.spread_pips + config.commission_pips + config.slippage_pips
        displacement_score = net_displacement_pips / max(friction_threshold_pips, config.epsilon)
        usable_available_pips = max(
            0.0,
            min(max(distance_floor, distance_ceiling), net_displacement_pips - friction_threshold_pips),
        )
        movement_state = "MOVE_DETECTED" if gross_movement_pips > friction_threshold_pips * 0.5 else "NOISE"
        cost_covering_state = "COST_COVERING" if net_displacement_pips > friction_threshold_pips else "NON_COVERING"

        velocities: List[float] = []
        bar_moves: List[float] = []
        cadence_values: List[float] = []
        for pos in range(1, len(vel_window)):
            now_tick = vel_window[pos]
            prev_tick = vel_window[pos - 1]
            dt = max(now_tick.ts - prev_tick.ts, config.epsilon)
            dp = (now_tick.mid - prev_tick.mid) / pip_size
            velocities.append(dp / dt)
            bar_moves.append(dp)
            cadence_values.append(dt)
        if not velocities:
            continue
        velocity = velocities[-1]
        velocity_per_bar = bar_moves[-1]
        tick_cadence_sec = _median(cadence_values, default=1.0)
        if len(velocities) >= 2:
            prev_velocity = velocities[-2]
            dt_acc = max(vel_window[-1].ts - vel_window[-2].ts, config.epsilon)
            acceleration = (velocity - prev_velocity) / dt_acc
            acceleration_per_bar = bar_moves[-1] - bar_moves[-2]
        else:
            acceleration = 0.0
            acceleration_per_bar = 0.0

        impulse_ratio = abs(velocity_per_bar) / max(friction_threshold_pips, config.epsilon)
        acceleration_ratio = abs(acceleration_per_bar) / max(friction_threshold_pips, config.epsilon)
        if impulse_ratio >= 2.0 or (impulse_ratio >= 1.2 and acceleration_ratio >= 0.5):
            energy_state = "IGNITION"
        elif impulse_ratio >= 1.0:
            energy_state = "DRIVE"
        elif impulse_ratio >= 0.35:
            energy_state = "DRIFT"
        else:
            energy_state = "DORMANT"

        vector_bias = "UP" if velocity > config.epsilon else ("DOWN" if velocity < -config.epsilon else "FLAT")
        time_in_zone_sec = max(current.ts - long_window[0].ts, 0.0)
        ofi, ofi_band = _ofi_proxy(pre_window, pip_size, config.epsilon)
        ces = _compression_energy_score(short_window, long_window, pip_size, config.epsilon)
        rv = _rejection_velocity(
            test_window=short_window,
            current_mid=current_mid,
            zone_state=zone_state,
            tick_cadence_sec=tick_cadence_sec,
            pip_size=pip_size,
        )
        btp = _book_toxicity_proxy(current.spread_pips, long_window)
        session_elapsed = _session_elapsed_pct(idx, len(ticks))
        precursor_state, precursor_pressure_score, precursor_width_pips, precursor_duration_bars = _precursor_snapshot(
            pre_window,
            pip_size,
            config.epsilon,
        )
        market_pattern_state = _classify_market_pattern(
            compression_ratio=compression_ratio,
            zone_state=zone_state,
            velocity=velocity,
            acceleration=acceleration,
        )
        path_discovery_pips = _path_discovery_pips(
            gross_movement_pips=gross_movement_pips,
            friction_threshold_pips=friction_threshold_pips,
            boundary_width_pips=width_pips,
            market_pattern_state=market_pattern_state,
        )
        path_covering_state = "PATH_COVERING" if path_discovery_pips > 0.0 else "PATH_THIN"
        location_relation_id = _location_relation_id(
            zone_state,
            vector_bias,
            distance_floor,
            distance_ceiling,
            width_pips,
        )
        pattern_match_state, live_recognition_state = _match_pattern(
            precursor_state=precursor_state,
            market_pattern_state=market_pattern_state,
            zone_state=zone_state,
            vector_bias=vector_bias,
            location_relation_id=location_relation_id,
            energy_state=energy_state,
        )
        direction_group = _direction_group(vector_bias)
        surface_type = _surface_type(market_pattern_state)
        directional_pack = _directional_feature_pack(
            direction_group=direction_group,
            zone_state=zone_state,
            vector_bias=vector_bias,
            distance_floor=distance_floor,
            distance_ceiling=distance_ceiling,
            opening_mid=ticks[0].mid,
            current_mid=current_mid,
            pip_size=pip_size,
            ofi=ofi,
            recent_bar_moves=bar_moves,
        )
        nearest_reference_distance = min(distance_floor, distance_ceiling)
        distance_mode = _distance_mode(nearest_reference_distance)
        trigger_state = _trigger_state(
            pattern_match_state=pattern_match_state,
            surface_type=surface_type,
            market_pattern_state=market_pattern_state,
            zone_state=zone_state,
            vector_bias=vector_bias,
            location_relation_id=location_relation_id,
            precursor_state=precursor_state,
            energy_state=energy_state,
            ofi=ofi,
            ces=ces,
            rejection_velocity=rv,
            distance_mode=distance_mode,
            long_space_above_pips=float(directional_pack["long_space_above_pips"]),
            short_space_below_pips=float(directional_pack["short_space_below_pips"]),
            long_ofi_alignment=bool(directional_pack["long_ofi_alignment"]),
            short_ofi_alignment=bool(directional_pack["short_ofi_alignment"]),
            long_seller_exhaustion=bool(directional_pack["long_seller_exhaustion"]),
            short_buyer_exhaustion=bool(directional_pack["short_buyer_exhaustion"]),
            session_elapsed_pct=session_elapsed,
        )
        direction_alignment = _direction_alignment_score(direction_group, vector_bias, ofi, trigger_state)
        doctrine_family_id = _doctrine_family_id(
            surface_type=surface_type,
            trigger_state=trigger_state,
            distance_mode=distance_mode,
            direction_group=direction_group,
            location_relation_id=location_relation_id,
            precursor_state=precursor_state,
            energy_state=energy_state,
            ofi=ofi,
            ces=ces,
            rejection_velocity=rv,
            btp=btp,
            session_elapsed_pct=session_elapsed,
            long_space_above_pips=float(directional_pack["long_space_above_pips"]),
            short_space_below_pips=float(directional_pack["short_space_below_pips"]),
            long_ofi_alignment=bool(directional_pack["long_ofi_alignment"]),
            short_ofi_alignment=bool(directional_pack["short_ofi_alignment"]),
            long_level_type=str(directional_pack["long_level_type"]),
            short_level_type=str(directional_pack["short_level_type"]),
            long_seller_exhaustion=bool(directional_pack["long_seller_exhaustion"]),
            short_buyer_exhaustion=bool(directional_pack["short_buyer_exhaustion"]),
        )
        discovered_distance_pips = _discover_distance_pips(
            net_displacement_pips=net_displacement_pips,
            friction_threshold_pips=friction_threshold_pips,
            usable_available_pips=usable_available_pips,
        )
        discovered_distance_pips = max(discovered_distance_pips, path_discovery_pips)
        target_distance_bucket = _target_distance_bucket(discovered_distance_pips, friction_threshold_pips)
        extraction_signature = _extraction_signature(
            market_pattern_state=market_pattern_state,
            pattern_match_state=doctrine_family_id if doctrine_family_id not in {"NO_DOCTRINE_MATCH", "DEFERRED_TOXIC_BOOK"} else pattern_match_state,
            direction_group=direction_group,
            target_distance_bucket=target_distance_bucket,
        )
        precursor_family_id = _precursor_family_id(
            precursor_state,
            precursor_pressure_score,
            precursor_width_pips,
            precursor_duration_bars,
        )
        topology_family_id = _topology_family_id(
            market_pattern_state,
            zone_state,
            compression_ratio,
            direction_group,
        )
        distance_family_id = _distance_family_id(
            direction_group,
            topology_family_id,
            location_relation_id,
            target_distance_bucket,
        )
        energy_family_id = _energy_family_id(
            energy_state,
            impulse_ratio,
            acceleration_per_bar,
            friction_threshold_pips,
        )
        conservative_opportunity = usable_available_pips > 0.0 and cost_covering_state == "COST_COVERING"
        raw_opportunity = conservative_opportunity or (
            path_discovery_pips > friction_threshold_pips * 0.75 and movement_state == "MOVE_DETECTED"
        )
        aggressive_path_opportunity = raw_opportunity and not conservative_opportunity
        payload_status = "READY"
        if direction_group not in {"LONG", "SHORT"} or trigger_state == "NO_CONFIDENT_TRIGGER":
            payload_status = "REJECT_NO_TRIGGER"
        elif distance_mode == "FAR_DISTANCE":
            payload_status = "REJECT_FAR_DISTANCE"
        elif btp > 2.0:
            payload_status = "DEFER_TOXIC_BOOK"
        elif doctrine_family_id == "NO_DOCTRINE_MATCH":
            payload_status = "REJECT_NO_DOCTRINE_MATCH"
        elif doctrine_family_id == "DEFERRED_TOXIC_BOOK":
            payload_status = "DEFER_TOXIC_BOOK"
        pattern_qualified_opportunity = raw_opportunity and payload_status == "READY"
        if pattern_qualified_opportunity and conservative_opportunity:
            opportunity_confidence_tier = "QUALIFIED_CONSERVATIVE"
        elif pattern_qualified_opportunity and aggressive_path_opportunity:
            opportunity_confidence_tier = "QUALIFIED_PATH"
        elif conservative_opportunity:
            opportunity_confidence_tier = "CONSERVATIVE"
        elif aggressive_path_opportunity:
            opportunity_confidence_tier = "AGGRESSIVE_PATH"
        else:
            opportunity_confidence_tier = "NON_OPPORTUNITY"
        lifecycle_stage = _lifecycle_stage(
            raw_opportunity=raw_opportunity,
            pattern_match_state=pattern_match_state,
            energy_state=energy_state,
            precursor_state=precursor_state,
            zone_state=zone_state,
            time_in_zone_sec=time_in_zone_sec,
            tick_cadence_sec=tick_cadence_sec,
            target_distance_bucket=target_distance_bucket,
        )
        movement_row = _discover_move_event(
            profile_id=profile_id,
            instrument=current.instrument,
            anchor_index=idx,
            movement_state=movement_state,
            direction_group=direction_group,
            vector_bias=vector_bias,
            gross_movement_pips=gross_movement_pips,
            net_displacement_pips=net_displacement_pips,
            displacement_score=displacement_score,
            tick_cadence_sec=tick_cadence_sec,
            velocity_pips_per_sec=velocity,
            acceleration_pips_per_sec2=acceleration,
            discovered_distance_pips=discovered_distance_pips,
            target_distance_bucket=target_distance_bucket,
            data_confidence=current.data_confidence,
        )
        economics_row = _evaluate_move_economics(
            friction_threshold_pips=friction_threshold_pips,
            usable_available_pips=usable_available_pips,
            path_discovery_pips=path_discovery_pips,
            cost_covering_state=cost_covering_state,
            path_covering_state=path_covering_state,
            conservative_opportunity=conservative_opportunity,
            aggressive_path_opportunity=aggressive_path_opportunity,
            raw_opportunity=raw_opportunity,
            opportunity_confidence_tier=opportunity_confidence_tier,
        )
        precursor_row = _measure_precursor_context(
            precursor_state=precursor_state,
            precursor_pressure_score=precursor_pressure_score,
            precursor_width_pips=precursor_width_pips,
            precursor_duration_bars=precursor_duration_bars,
            order_flow_imbalance=ofi,
            order_flow_band=ofi_band,
            compression_energy_score=ces,
            rejection_velocity=rv,
            book_toxicity_proxy=btp,
            direction_alignment_score=direction_alignment,
        )
        mapping_row = _build_independent_market_map(
            profile_id=profile_id,
            mapping_id=mapping_id,
            market_pattern_state=market_pattern_state,
            surface_type=surface_type,
            zone_state=zone_state,
            location_relation_id=location_relation_id,
            compression_ratio=compression_ratio,
            boundary_width_pips=width_pips,
            distance_to_floor_pips=distance_floor,
            distance_to_ceiling_pips=distance_ceiling,
            time_in_zone_sec=time_in_zone_sec,
            session_elapsed_pct=session_elapsed,
            topology_family_id=topology_family_id,
            distance_family_id=distance_family_id,
            energy_family_id=energy_family_id,
            precursor_family_id=precursor_family_id,
            lifecycle_stage=lifecycle_stage,
        )
        fit_row = _fit_opportunity_to_market_map(
            profile_id=profile_id,
            mapping_id=mapping_id,
            direction_group=direction_group,
            distance_mode=distance_mode,
            target_distance_bucket=target_distance_bucket,
            raw_opportunity=raw_opportunity,
            conservative_opportunity=conservative_opportunity,
            aggressive_path_opportunity=aggressive_path_opportunity,
            location_relation_id=location_relation_id,
            market_pattern_state=market_pattern_state,
            surface_type=surface_type,
            zone_state=zone_state,
        )
        tier0_handoff_row = _compile_tier0_handoff(
            profile_id=profile_id,
            mapping_id=mapping_id,
            movement_row=movement_row,
            economics_row=economics_row,
            precursor_row=precursor_row,
            mapping_row=mapping_row,
            fit_row=fit_row,
            directional_pack=directional_pack,
        )
        event_discovery_rows.append({**movement_row, **economics_row, **precursor_row})
        market_mapping_rows.append(mapping_row)
        opportunity_fit_rows.append(fit_row)
        tier0_handoff_rows.append(tier0_handoff_row)
        profiles.append(
            Phase1Profile(
                profile_id=profile_id,
                anchor_index=idx,
                gross_movement_pips=round(gross_movement_pips, 6),
                displacement_score=round(displacement_score, 6),
                net_displacement_pips=round(net_displacement_pips, 6),
                friction_threshold_pips=round(friction_threshold_pips, 6),
                usable_available_pips=round(usable_available_pips, 6),
                path_discovery_pips=round(path_discovery_pips, 6),
                extractable=raw_opportunity,
                conservative_opportunity=conservative_opportunity,
                aggressive_path_opportunity=aggressive_path_opportunity,
                pattern_qualified_opportunity=pattern_qualified_opportunity,
                movement_state=movement_state,
                cost_covering_state=cost_covering_state,
                path_covering_state=path_covering_state,
                opportunity_confidence_tier=opportunity_confidence_tier,
                distance_to_floor_pips=round(distance_floor, 6),
                distance_to_ceiling_pips=round(distance_ceiling, 6),
                compression_ratio=round(compression_ratio, 6),
                boundary_width_pips=round(width_pips, 6),
                zone_state=zone_state,
                time_in_zone_sec=round(time_in_zone_sec, 6),
                velocity_pips_per_sec=round(velocity, 6),
                acceleration_pips_per_sec2=round(acceleration, 6),
                energy_state=energy_state,
                tick_cadence_sec=round(tick_cadence_sec, 6),
                velocity_pips_per_bar=round(velocity_per_bar, 6),
                acceleration_pips_per_bar=round(acceleration_per_bar, 6),
                impulse_ratio=round(impulse_ratio, 6),
                vector_bias=vector_bias,
                precursor_state=precursor_state,
                precursor_pressure_score=precursor_pressure_score,
                precursor_width_pips=precursor_width_pips,
                precursor_duration_bars=precursor_duration_bars,
                market_pattern_state=market_pattern_state,
                surface_type=surface_type,
                pattern_match_state=doctrine_family_id if doctrine_family_id not in {"NO_DOCTRINE_MATCH", "DEFERRED_TOXIC_BOOK"} else pattern_match_state,
                live_recognition_state=live_recognition_state,
                trigger_state=trigger_state,
                direction_group=direction_group,
                distance_mode=distance_mode,
                discovered_distance_pips=round(discovered_distance_pips, 6),
                target_distance_bucket=target_distance_bucket,
                extraction_signature=extraction_signature,
                doctrine_family_id=doctrine_family_id,
                precursor_family_id=precursor_family_id,
                topology_family_id=topology_family_id,
                location_relation_id=location_relation_id,
                distance_family_id=distance_family_id,
                energy_family_id=energy_family_id,
                order_flow_imbalance=ofi,
                order_flow_band=ofi_band,
                compression_energy_score=ces,
                rejection_velocity=rv,
                book_toxicity_proxy=btp,
                session_elapsed_pct=session_elapsed,
                direction_alignment_score=direction_alignment,
                long_retest_quality=float(directional_pack["long_retest_quality"]),
                long_seller_exhaustion=bool(directional_pack["long_seller_exhaustion"]),
                long_ofi_alignment=bool(directional_pack["long_ofi_alignment"]),
                long_level_type=str(directional_pack["long_level_type"]),
                long_space_above_pips=float(directional_pack["long_space_above_pips"]),
                short_retest_quality=float(directional_pack["short_retest_quality"]),
                short_buyer_exhaustion=bool(directional_pack["short_buyer_exhaustion"]),
                short_ofi_alignment=bool(directional_pack["short_ofi_alignment"]),
                short_level_type=str(directional_pack["short_level_type"]),
                short_space_below_pips=float(directional_pack["short_space_below_pips"]),
                payload_status=payload_status,
                lifecycle_stage=lifecycle_stage,
                parent_pattern_id="NO_PATTERN",
                parent_episode_id="NO_PARENT_EPISODE",
                opportunity_episode_id="NO_OPPORTUNITY_EPISODE",
                overlap_group_id="NO_OVERLAP_GROUP",
                episode_position="NONE",
                data_confidence=current.data_confidence,
            )
        )
    _assign_episode_structure(profiles)
    return {
        "profiles": profiles,
        "event_discovery_rows": event_discovery_rows,
        "market_mapping_rows": market_mapping_rows,
        "opportunity_fit_rows": opportunity_fit_rows,
        "tier0_handoff_rows": tier0_handoff_rows,
    }


def build_phase1_profiles(ticks: List[SanitizedTick], config: Phase1Config) -> List[Phase1Profile]:
    return list(build_phase1_stack(ticks, config)["profiles"])


def summarize_profiles(profiles: List[Phase1Profile]) -> Dict[str, Any]:
    if not profiles:
        return {
            "profile_count": 0,
            "extractable_count": 0,
            "conservative_opportunity_count": 0,
            "aggressive_path_opportunity_count": 0,
            "pattern_qualified_opportunity_count": 0,
            "movement_detected_count": 0,
            "cost_covering_count": 0,
            "energy_states": {},
            "zone_states": {},
            "market_pattern_states": {},
            "surface_types": {},
            "pattern_match_states": {},
            "trigger_states": {},
            "direction_groups": {},
            "distance_modes": {},
            "target_distance_buckets": {},
            "extraction_signatures": {},
            "doctrine_family_ids": {},
            "precursor_family_ids": {},
            "topology_family_ids": {},
            "location_relation_ids": {},
            "distance_family_ids": {},
            "energy_family_ids": {},
            "payload_status": {},
            "lifecycle_stages": {},
            "data_confidence": {},
            "opportunity_confidence_tiers": {},
            "compression_present": False,
            "expansion_present": False,
        }

    energy_states: Dict[str, int] = {}
    zone_states: Dict[str, int] = {}
    market_pattern_states: Dict[str, int] = {}
    surface_types: Dict[str, int] = {}
    pattern_match_states: Dict[str, int] = {}
    trigger_states: Dict[str, int] = {}
    direction_groups: Dict[str, int] = {}
    distance_modes: Dict[str, int] = {}
    target_distance_buckets: Dict[str, int] = {}
    extraction_signatures: Dict[str, int] = {}
    doctrine_family_ids: Dict[str, int] = {}
    precursor_family_ids: Dict[str, int] = {}
    topology_family_ids: Dict[str, int] = {}
    location_relation_ids: Dict[str, int] = {}
    distance_family_ids: Dict[str, int] = {}
    energy_family_ids: Dict[str, int] = {}
    payload_status: Dict[str, int] = {}
    lifecycle_stages: Dict[str, int] = {}
    data_confidence: Dict[str, int] = {}
    opportunity_confidence_tiers: Dict[str, int] = {}
    compression_present = False
    expansion_present = False
    for profile in profiles:
        energy_states[profile.energy_state] = energy_states.get(profile.energy_state, 0) + 1
        zone_states[profile.zone_state] = zone_states.get(profile.zone_state, 0) + 1
        market_pattern_states[profile.market_pattern_state] = market_pattern_states.get(profile.market_pattern_state, 0) + 1
        surface_types[profile.surface_type] = surface_types.get(profile.surface_type, 0) + 1
        pattern_match_states[profile.pattern_match_state] = pattern_match_states.get(profile.pattern_match_state, 0) + 1
        trigger_states[profile.trigger_state] = trigger_states.get(profile.trigger_state, 0) + 1
        direction_groups[profile.direction_group] = direction_groups.get(profile.direction_group, 0) + 1
        distance_modes[profile.distance_mode] = distance_modes.get(profile.distance_mode, 0) + 1
        target_distance_buckets[profile.target_distance_bucket] = (
            target_distance_buckets.get(profile.target_distance_bucket, 0) + 1
        )
        extraction_signatures[profile.extraction_signature] = extraction_signatures.get(profile.extraction_signature, 0) + 1
        doctrine_family_ids[profile.doctrine_family_id] = doctrine_family_ids.get(profile.doctrine_family_id, 0) + 1
        precursor_family_ids[profile.precursor_family_id] = precursor_family_ids.get(profile.precursor_family_id, 0) + 1
        topology_family_ids[profile.topology_family_id] = topology_family_ids.get(profile.topology_family_id, 0) + 1
        location_relation_ids[profile.location_relation_id] = location_relation_ids.get(profile.location_relation_id, 0) + 1
        distance_family_ids[profile.distance_family_id] = distance_family_ids.get(profile.distance_family_id, 0) + 1
        energy_family_ids[profile.energy_family_id] = energy_family_ids.get(profile.energy_family_id, 0) + 1
        payload_status[profile.payload_status] = payload_status.get(profile.payload_status, 0) + 1
        lifecycle_stages[profile.lifecycle_stage] = lifecycle_stages.get(profile.lifecycle_stage, 0) + 1
        data_confidence[profile.data_confidence] = data_confidence.get(profile.data_confidence, 0) + 1
        opportunity_confidence_tiers[profile.opportunity_confidence_tier] = (
            opportunity_confidence_tiers.get(profile.opportunity_confidence_tier, 0) + 1
        )
        if profile.compression_ratio <= 0.35:
            compression_present = True
        if profile.compression_ratio >= 0.75:
            expansion_present = True

    return {
        "profile_count": len(profiles),
        "extractable_count": sum(1 for p in profiles if p.extractable),
        "conservative_opportunity_count": sum(1 for p in profiles if p.conservative_opportunity),
        "aggressive_path_opportunity_count": sum(1 for p in profiles if p.aggressive_path_opportunity),
        "pattern_qualified_opportunity_count": sum(1 for p in profiles if p.pattern_qualified_opportunity),
        "movement_detected_count": sum(1 for p in profiles if p.movement_state == "MOVE_DETECTED"),
        "cost_covering_count": sum(1 for p in profiles if p.cost_covering_state == "COST_COVERING"),
        "energy_states": energy_states,
        "zone_states": zone_states,
        "market_pattern_states": market_pattern_states,
        "surface_types": surface_types,
        "pattern_match_states": pattern_match_states,
        "trigger_states": trigger_states,
        "direction_groups": direction_groups,
        "distance_modes": distance_modes,
        "target_distance_buckets": target_distance_buckets,
        "extraction_signatures": extraction_signatures,
        "doctrine_family_ids": doctrine_family_ids,
        "precursor_family_ids": precursor_family_ids,
        "topology_family_ids": topology_family_ids,
        "location_relation_ids": location_relation_ids,
        "distance_family_ids": distance_family_ids,
        "energy_family_ids": energy_family_ids,
        "payload_status": payload_status,
        "lifecycle_stages": lifecycle_stages,
        "data_confidence": data_confidence,
        "opportunity_confidence_tiers": opportunity_confidence_tiers,
        "compression_present": compression_present,
        "expansion_present": expansion_present,
    }


def run_phase1(raw_ticks: List[Dict[str, Any]], config: Phase1Config) -> Dict[str, Any]:
    sanitized = sanitize_ticks(raw_ticks, config)
    phase1_stack = build_phase1_stack(sanitized["ticks"], config)
    profiles = list(phase1_stack["profiles"])
    return {
        "config": asdict(config),
        "sanitizer": sanitized["summary"],
        "profiles": [asdict(profile) for profile in profiles],
        "tier0": {
            "event_discovery_rows": list(phase1_stack["event_discovery_rows"]),
            "market_mapping_rows": list(phase1_stack["market_mapping_rows"]),
            "opportunity_fit_rows": list(phase1_stack["opportunity_fit_rows"]),
            "tier0_handoff_rows": list(phase1_stack["tier0_handoff_rows"]),
        },
        "summary": summarize_profiles(profiles),
    }
