from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Dict, List

from engine.entry.doctrine_entry_registry import doctrine_identity, load_selected_entry_strategies
from engine.entry.pocket_assignment import assign_entry_pocket


def build_entry_packet_template(strategy_row: Dict[str, Any]) -> Dict[str, Any]:
    pocket = assign_entry_pocket(strategy_row)
    target_buckets = [str(value) for value in list(strategy_row.get("target_distance_buckets", []) or [])]
    return {
        "packet_version": "entry_packet_template_v1",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "source": "v2_entry_strategy_registry",
        "strategy_id": str(strategy_row.get("strategy_id", "") or ""),
        "doctrine_family_id": pocket["doctrine_family_id"],
        "doctrine_variant_id": str(strategy_row.get("strategy_id", "") or ""),
        "entry_pocket_id": pocket["entry_pocket_id"],
        "direction": pocket["direction"],
        "regime_state": pocket["regime_state"],
        "window_state": pocket["window_state"],
        "zone_state": pocket["zone_state"],
        "path_state": pocket["path_state"],
        "target_buckets": target_buckets,
        "expectancy_pips": float(strategy_row.get("expectancy_pips", 0.0) or 0.0),
        "net_pnl_pips": float(strategy_row.get("net_pnl_pips", 0.0) or 0.0),
        "win_rate": float(strategy_row.get("win_rate", 0.0) or 0.0),
        "trade_count": int(strategy_row.get("trade_count", 0) or 0),
        "scenario_count": int(strategy_row.get("scenario_count", 0) or 0),
        "anchor_expression_id": pocket["anchor_expression_id"],
    }


def build_selected_entry_packet_templates() -> List[Dict[str, Any]]:
    return [build_entry_packet_template(row) for row in load_selected_entry_strategies()]


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clean_text(value: Any, default: str = "") -> str:
    text = str(value or "").strip().upper()
    return text or default


def build_canonical_entry_packet(
    payload: Dict[str, Any],
    *,
    doctrine_id: str,
    reject_reason: str = "",
    doctrine_match_result: str = "MATCHED",
    payload_validated: bool = True,
) -> Dict[str, Any]:
    pocket = assign_entry_pocket(payload)
    identity = doctrine_identity(doctrine_id)
    energy = dict(payload.get("energy_snapshot", {}) or {})
    return {
        "packet_version": "canonical_entry_packet_v1",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "source": str(payload.get("source", "gateway") or "gateway"),
        "strategy_id": str(payload.get("strategy_id", payload.get("setup", doctrine_id)) or doctrine_id),
        "doctrine_id": identity["doctrine_id"],
        "doctrine_family": identity["doctrine_family"],
        "variant": identity["variant"],
        "doctrine_family_id": identity["doctrine_family"],
        "doctrine_variant_id": identity["doctrine_id"],
        "entry_pocket_id": str(payload.get("entry_pocket_id", pocket["entry_pocket_id"]) or pocket["entry_pocket_id"]),
        "direction": _clean_text(payload.get("direction", pocket["direction"]), "UNKNOWN"),
        "surface_type": _clean_text(payload.get("surface_type")),
        "trigger_state": _clean_text(payload.get("trigger_state")),
        "distance_mode": _clean_text(payload.get("distance_mode")),
        "level_type": _clean_text(payload.get("level_type")),
        "pair": str(payload.get("pair", payload.get("instrument", "UNKNOWN")) or "UNKNOWN"),
        "session_label": _clean_text(payload.get("session_label", payload.get("session", "SYSTEM")), "SYSTEM"),
        "regime_state": _clean_text(payload.get("regime_state", pocket["regime_state"]), "UNKNOWN"),
        "window_state": _clean_text(payload.get("window_state", pocket["window_state"]), "UNKNOWN"),
        "zone_state": _clean_text(payload.get("zone_state", pocket["zone_state"]), "UNKNOWN"),
        "path_state": _clean_text(payload.get("path_state", pocket["path_state"]), "UNKNOWN"),
        "anchor_expression_id": str(payload.get("anchor_expression_id", pocket["anchor_expression_id"]) or pocket["anchor_expression_id"]),
        "sl_rule": str(payload.get("sl_rule", "") or ""),
        "tp_rule": str(payload.get("tp_rule", "") or ""),
        "runner_eligible": bool(payload.get("runner_eligible", False)),
        "partial_close_eligible": bool(payload.get("partial_close_eligible", False)),
        "target_profile": str(payload.get("target_profile", "") or ""),
        "trade_role": str(payload.get("trade_role", "") or ""),
        "horizon_class": str(payload.get("horizon_class", "") or ""),
        "OFI": _safe_float(energy.get("OFI", payload.get("OFI"))),
        "CES": _safe_float(energy.get("CES", payload.get("CES"))),
        "RV": _safe_float(energy.get("RV", payload.get("RV"))),
        "BTP": _safe_float(energy.get("BTP", payload.get("BTP"))),
        "energy_snapshot": {
            "OFI": _safe_float(energy.get("OFI", payload.get("OFI"))),
            "CES": _safe_float(energy.get("CES", payload.get("CES"))),
            "RV": _safe_float(energy.get("RV", payload.get("RV"))),
            "BTP": _safe_float(energy.get("BTP", payload.get("BTP"))),
        },
        "session_elapsed_pct": _safe_float(payload.get("session_elapsed_pct", payload.get("session_pct"))),
        "bar_index": int(_safe_float(payload.get("bar_index"), 0.0)),
        "payload_validated": bool(payload_validated),
        "reject_reason": str(reject_reason or ""),
        "doctrine_match_result": str(doctrine_match_result or ""),
    }
