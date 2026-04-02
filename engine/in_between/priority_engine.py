from __future__ import annotations

from typing import Any, Dict, Iterable, List


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def compute_priority_score(packet: Dict[str, Any]) -> float:
    economics = dict(packet.get("economics", {}) or {})
    viability = dict(packet.get("economic_gate", {}) or {})
    base = (
        _safe_float(economics.get("expectancy_pips"), 0.0) * 3.0
        + _safe_float(economics.get("win_rate"), 0.0) * 2.0
        + min(_safe_float(economics.get("trade_count"), 0.0), 25.0) * 0.1
    )
    return round(base + _safe_float(viability.get("net_edge_pips"), 0.0), 6)


def rank_trade_packets(packets: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    ranked = []
    for packet in packets:
        score = compute_priority_score(packet)
        ranked.append({**packet, "priority_score": score})
    return sorted(
        ranked,
        key=lambda row: (
            -float(row.get("priority_score", 0.0) or 0.0),
            -float(row.get("economics", {}).get("expectancy_pips", 0.0) or 0.0),
            -float(row.get("economics", {}).get("win_rate", 0.0) or 0.0),
            str(row.get("entry", {}).get("strategy_id", "") or ""),
        ),
    )

