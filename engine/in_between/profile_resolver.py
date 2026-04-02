from __future__ import annotations

from typing import Any, Dict, List


def _clean_text(value: Any, default: str = "UNKNOWN") -> str:
    text = str(value or "").strip().upper()
    return text or default


def _target_buckets(payload: Dict[str, Any]) -> List[str]:
    return [_clean_text(value) for value in list(payload.get("target_buckets", payload.get("target_distance_buckets", [])) or [])]


def resolve_trade_role(payload: Dict[str, Any]) -> str:
    buckets = set(_target_buckets(payload))
    if "EXTENDED" in buckets:
        return "RUNNER"
    if "LARGE" in buckets or "MEDIUM" in buckets:
        return "HYBRID"
    return "HARVEST"


def resolve_horizon_class(payload: Dict[str, Any]) -> str:
    buckets = set(_target_buckets(payload))
    if "EXTENDED" in buckets:
        return "LONG"
    if "LARGE" in buckets or "MEDIUM" in buckets:
        return "MEDIUM"
    return "SHORT"


def resolve_target_profile(payload: Dict[str, Any]) -> str:
    buckets = _target_buckets(payload)
    return buckets[0] if buckets else "SMALL"


def resolve_timeout_profile(payload: Dict[str, Any]) -> str:
    role = resolve_trade_role(payload)
    if role == "RUNNER":
        return "WIDE_TTL"
    if role == "HYBRID":
        return "BALANCED_TTL"
    return "TIGHT_TTL"


def resolve_expected_progress_profile(payload: Dict[str, Any]) -> str:
    horizon = resolve_horizon_class(payload)
    if horizon == "LONG":
        return "MULTI_STAGE_EXTENSION"
    if horizon == "MEDIUM":
        return "BALANCED_CONTINUATION"
    return "FAST_CAPTURE"


def resolve_invalidation_type(payload: Dict[str, Any]) -> str:
    zone_state = _clean_text(payload.get("zone_state", "MID_ZONE"))
    if "FLOOR" in zone_state or "CEILING" in zone_state:
        return "ZONE_FAILURE"
    return "CHARGE_OR_DECAY_FAILURE"


def resolve_management_profile_id(payload: Dict[str, Any]) -> str:
    role = resolve_trade_role(payload)
    direction = _clean_text(payload.get("direction", "UNKNOWN"))
    family = _clean_text(payload.get("doctrine_family_id", "UNKNOWN"))
    return f"{family}|{direction}|{role}"

