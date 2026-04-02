from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Dict

from engine.entry.pocket_assignment import assign_entry_pocket
from engine.in_between.profile_resolver import (
    resolve_expected_progress_profile,
    resolve_horizon_class,
    resolve_invalidation_type,
    resolve_management_profile_id,
    resolve_target_profile,
    resolve_timeout_profile,
    resolve_trade_role,
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _clean_text(value: Any, default: str = "UNKNOWN") -> str:
    text = str(value or "").strip().upper()
    return text or default


def infer_regime_from_metrics(metrics: Dict[str, Any] | None) -> str:
    metrics = metrics or {}
    velocity = _safe_float(metrics.get("velocity"), 0.0)
    pullback = _safe_float(metrics.get("pullback"), 0.0)
    speed = _safe_float(metrics.get("speed"), 0.0)
    cps = _safe_float(metrics.get("cps"), 0.5)
    if velocity >= 0.40 and pullback <= 0.20 and cps >= 0.55:
        return "TREND"
    if pullback >= 0.30 and velocity <= 0.20 and speed >= 0.25:
        return "MEAN_REVERSION"
    return "MIXED"


def normalize_trade_intent(payload: Dict[str, Any], metrics: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """Normalize entry or runtime trade intent into one canonical gateway packet."""
    metrics = metrics or {}
    pocket = assign_entry_pocket(payload)
    pair = _clean_text(payload.get("pair", payload.get("instrument", "UNKNOWN")))
    session_label = _clean_text(payload.get("session", payload.get("session_label", "SYSTEM")))
    regime_state = _clean_text(payload.get("regime_state", "")) if payload.get("regime_state") else infer_regime_from_metrics(metrics)
    target_profile = resolve_target_profile(payload)
    packet = {
        "packet_version": "trade_gateway_packet_v1",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "source": str(payload.get("source", "runtime_or_entry")),
        "entry": {
            "strategy_id": str(payload.get("strategy_id", payload.get("setup", payload.get("setup_resolved", ""))) or ""),
            "doctrine_family_id": _clean_text(payload.get("doctrine_family_id", pocket["doctrine_family_id"]), "UNKNOWN"),
            "doctrine_variant_id": _clean_text(payload.get("doctrine_variant_id", payload.get("strategy_id", "")), "UNKNOWN"),
            "pair": pair,
            "direction": _clean_text(payload.get("direction", pocket["direction"]), "UNKNOWN"),
            "session_label": session_label,
            "regime_state": regime_state,
            "entry_pocket_id": str(payload.get("entry_pocket_id", pocket["entry_pocket_id"]) or pocket["entry_pocket_id"]),
            "target_bucket": target_profile,
            "target_profile": target_profile,
            "horizon_class": resolve_horizon_class(payload),
            "trade_role": resolve_trade_role(payload),
            "expected_progress_profile": resolve_expected_progress_profile(payload),
            "invalidation_type": resolve_invalidation_type(payload),
            "timeout_profile": resolve_timeout_profile(payload),
            "management_profile_id": resolve_management_profile_id(
                {
                    **payload,
                    "direction": _clean_text(payload.get("direction", pocket["direction"]), "UNKNOWN"),
                    "doctrine_family_id": _clean_text(payload.get("doctrine_family_id", pocket["doctrine_family_id"]), "UNKNOWN"),
                }
            ),
            "zone_state": pocket["zone_state"],
            "window_state": pocket["window_state"],
            "path_state": pocket["path_state"],
            "anchor_expression_id": pocket["anchor_expression_id"],
        },
        "friction": {
            "spread_pips": _safe_float(payload.get("spread_pips", metrics.get("spread_pips")), 0.0),
            "commission_pips": _safe_float(payload.get("commission_pips"), 0.0),
            "slippage_pips": _safe_float(payload.get("slippage_pips"), 0.0),
            "expected_target_pips": _safe_float(payload.get("expected_target_pips", payload.get("expectancy_pips")), 0.0),
            "minimum_payoff_pips": _safe_float(payload.get("minimum_payoff_pips", payload.get("expectancy_pips")), 0.0),
        },
        "economics": {
            "win_rate": _safe_float(payload.get("win_rate"), 0.0),
            "trade_count": int(_safe_float(payload.get("trade_count"), 0.0)),
            "expectancy_pips": _safe_float(payload.get("expectancy_pips"), 0.0),
            "net_pnl_pips": _safe_float(payload.get("net_pnl_pips"), 0.0),
        },
        "aee": {
            "pocket_id": str(payload.get("pocket_id", f"{pair}__{_clean_text(payload.get('direction', pocket['direction']), 'UNKNOWN')}__{session_label}__{regime_state}") or ""),
            "management_profile_id": resolve_management_profile_id(
                {
                    **payload,
                    "direction": _clean_text(payload.get("direction", pocket["direction"]), "UNKNOWN"),
                    "doctrine_family_id": _clean_text(payload.get("doctrine_family_id", pocket["doctrine_family_id"]), "UNKNOWN"),
                }
            ),
        },
    }
    return packet

