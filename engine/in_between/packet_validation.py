from __future__ import annotations

from typing import Any, Dict, List


REQUIRED_ENTRY_FIELDS = [
    "strategy_id",
    "doctrine_family_id",
    "doctrine_variant_id",
    "pair",
    "direction",
    "session_label",
    "regime_state",
    "entry_pocket_id",
    "target_bucket",
    "target_profile",
    "horizon_class",
    "trade_role",
    "management_profile_id",
]


def validate_trade_packet(packet: Dict[str, Any]) -> Dict[str, Any]:
    entry = dict(packet.get("entry", {}) or {})
    missing: List[str] = [field for field in REQUIRED_ENTRY_FIELDS if not str(entry.get(field, "") or "").strip()]
    aee = dict(packet.get("aee", {}) or {})
    if not str(aee.get("pocket_id", "") or "").strip():
        missing.append("aee.pocket_id")
    return {
        "valid": not missing,
        "missing_fields": missing,
    }

