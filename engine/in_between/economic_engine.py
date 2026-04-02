from __future__ import annotations

from typing import Any, Dict


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def evaluate_economic_viability(packet: Dict[str, Any], *, cost_multiplier: float = 1.10) -> Dict[str, Any]:
    """Pure economic gate for the canonical gateway packet."""
    friction = dict(packet.get("friction", {}) or {})
    spread_pips = max(0.0, _safe_float(friction.get("spread_pips"), 0.0))
    commission_pips = max(0.0, _safe_float(friction.get("commission_pips"), 0.0))
    slippage_pips = max(0.0, _safe_float(friction.get("slippage_pips"), 0.0))
    minimum_payoff = max(
        0.0,
        _safe_float(friction.get("minimum_payoff_pips"), 0.0),
        _safe_float(friction.get("expected_target_pips"), 0.0),
        _safe_float(packet.get("economics", {}).get("expectancy_pips"), 0.0),
    )
    round_trip_cost = spread_pips + commission_pips + slippage_pips
    required_payoff = round_trip_cost * float(cost_multiplier)
    net_edge = minimum_payoff - required_payoff
    viable = net_edge > 0.0
    return {
        "viable": viable,
        "reason": "FRICTION_OK" if viable else "FRICTION_NOT_COVERED",
        "round_trip_cost_pips": round(round_trip_cost, 6),
        "required_payoff_pips": round(required_payoff, 6),
        "minimum_payoff_pips": round(minimum_payoff, 6),
        "net_edge_pips": round(net_edge, 6),
    }

