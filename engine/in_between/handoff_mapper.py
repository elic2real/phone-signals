from __future__ import annotations

from typing import Any, Dict


def shape_aee_handoff(packet: Dict[str, Any]) -> Dict[str, Any]:
    entry = dict(packet.get("entry", {}) or {})
    friction = dict(packet.get("friction", {}) or {})
    economics = dict(packet.get("economics", {}) or {})
    aee = dict(packet.get("aee", {}) or {})
    return {
        "packet_version": packet.get("packet_version", "trade_gateway_packet_v1"),
        "pocket_id": aee.get("pocket_id"),
        "management_profile_id": aee.get("management_profile_id"),
        "doctrine_family_id": entry.get("doctrine_family_id"),
        "doctrine_variant_id": entry.get("doctrine_variant_id"),
        "strategy_id": entry.get("strategy_id"),
        "pair": entry.get("pair"),
        "direction": entry.get("direction"),
        "session_label": entry.get("session_label"),
        "regime_state": entry.get("regime_state"),
        "entry_pocket_id": entry.get("entry_pocket_id"),
        "target_bucket": entry.get("target_bucket"),
        "target_profile": entry.get("target_profile"),
        "trade_role": entry.get("trade_role"),
        "horizon_class": entry.get("horizon_class"),
        "timeout_profile": entry.get("timeout_profile"),
        "invalidation_type": entry.get("invalidation_type"),
        "expected_progress_profile": entry.get("expected_progress_profile"),
        "friction_profile": friction,
        "economics": economics,
    }

