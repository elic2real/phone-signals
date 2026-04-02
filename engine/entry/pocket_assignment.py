from __future__ import annotations

from typing import Any, Dict, List


def _clean_text(value: Any, default: str = "UNKNOWN") -> str:
    text = str(value or "").strip().upper()
    return text or default


def _split_expression(expression_id: str) -> List[str]:
    return [segment.strip().upper() for segment in str(expression_id or "").split("|") if segment.strip()]


def infer_family_from_strategy_id(strategy_id: str) -> str:
    text = _clean_text(strategy_id)
    if any(token in text for token in ("FLOW", "TRANSITION", "COMPRESSION", "EXPANSION", "COILED")):
        return "continuation"
    if any(token in text for token in ("FAILED_PUSH", "REVERSAL")):
        return "reversal"
    if "OSCILLATION" in text:
        return "oscillation"
    if "PRESSURE" in text:
        return "pressure"
    return "unknown"


def infer_direction(payload: Dict[str, Any]) -> str:
    for key in ("direction", "dir", "side"):
        value = _clean_text(payload.get(key, ""))
        if value in {"LONG", "SHORT"}:
            return value
    strategy_id = _clean_text(payload.get("strategy_id", ""))
    if strategy_id.endswith("_LONG"):
        return "LONG"
    if strategy_id.endswith("_SHORT"):
        return "SHORT"
    return "UNKNOWN"


def assign_entry_pocket(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Derive doctrine-local pocket metadata from the richest available entry fields."""
    expression_ids = list(payload.get("distance_expression_ids", []) or [])
    anchor_expression = _clean_text(expression_ids[0] if expression_ids else payload.get("distance_expression_id", ""))
    segments = _split_expression(anchor_expression)
    regime_state = segments[0] if len(segments) > 0 else _clean_text(payload.get("regime_state", "MIXED"))
    zone_state = segments[1] if len(segments) > 1 else _clean_text(payload.get("zone_state", "MID_ZONE"))
    window_state = segments[2] if len(segments) > 2 else _clean_text(payload.get("window_state", "SYSTEM"))
    path_state = segments[4] if len(segments) > 4 else _clean_text(payload.get("path_state", "GENERIC"))
    direction = infer_direction(payload)
    strategy_id = _clean_text(payload.get("strategy_id", "UNNAMED_STRATEGY"))
    pocket_id = "|".join([strategy_id, regime_state, window_state, zone_state, direction])
    return {
        "entry_pocket_id": pocket_id,
        "anchor_expression_id": anchor_expression,
        "regime_state": regime_state,
        "zone_state": zone_state,
        "window_state": window_state,
        "path_state": path_state,
        "direction": direction,
        "doctrine_family_id": infer_family_from_strategy_id(strategy_id),
    }

