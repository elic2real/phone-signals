from __future__ import annotations

import json
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from aee_family_state_machine_v3 import get_directional_aee_params
from engine.entry.doctrine_entry_registry import (
    DISTANCE_MODES,
    SURFACE_TYPES,
    TRIGGER_STATES,
    validate_level_event_payload,
    match_doctrine,
    DOCTRINE_REGISTRY,
)
from engine.entry.entry_packet_builder import build_canonical_entry_packet
from engine.in_between.economic_engine import evaluate_economic_viability
from engine.in_between.handoff_mapper import shape_aee_handoff
from engine.in_between.intent_normalizer import normalize_trade_intent
from engine.in_between.packet_validation import validate_trade_packet
from engine.in_between.priority_engine import rank_trade_packets

WORKSPACE = Path(__file__).resolve().parents[2]
DIAGNOSTICS_DIR = WORKSPACE / "control" / "diagnostics"
PAYLOAD_REJECT_TAXONOMY_PATH = DIAGNOSTICS_DIR / "payload_reject_taxonomy.json"

REQUIRED_PAYLOAD_FIELDS = {
    "surface_type": SURFACE_TYPES,
    "trigger_state": TRIGGER_STATES,
    "distance_mode": DISTANCE_MODES,
    "direction": {"LONG", "SHORT"},
    "energy_snapshot": None,
}


class GatewayResult(StrEnum):
    HARD_REJECT = "HARD_REJECT"
    NO_DOCTRINE_MATCH = "NO_DOCTRINE_MATCH"
    MATCHED = "MATCHED"


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


def _empty_taxonomy() -> Dict[str, Any]:
    return {
        "schema_version": "1.0",
        "generated_at": "",
        "session_range": {"first_session": "", "last_session": "", "session_count": 0},
        "hard_reject": {"total": 0, "by_reason": {}},
        "no_doctrine_match": {"total": 0, "pct_of_valid_payloads": 0.0, "by_surface_trigger_combination": {}},
        "matched": {"total": 0, "pct_of_all_candidates": 0.0, "by_doctrine_id": {}},
        "totals": {
            "all_candidates": 0,
            "invalid_payload_rate": 0.0,
            "valid_unmatched_rate": 0.0,
            "match_rate": 0.0,
        },
        "highest_value_fixes": [],
    }


def _load_taxonomy() -> Dict[str, Any]:
    if not PAYLOAD_REJECT_TAXONOMY_PATH.exists():
        return _empty_taxonomy()
    try:
        return json.loads(PAYLOAD_REJECT_TAXONOMY_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _empty_taxonomy()


def compute_highest_value_fixes(taxonomy: Dict[str, Any]) -> List[Dict[str, Any]]:
    fixes: List[Dict[str, Any]] = []
    hard_rejects = dict(taxonomy.get("hard_reject", {}).get("by_reason", {}) or {})
    raw_fallback = int(dict(hard_rejects.get("RAW_FALLBACK", {}) or {}).get("count", 0) or 0)
    if raw_fallback > 0:
        fixes.append(
            {
                "priority": 1,
                "fix_type": "UPSTREAM_PAYLOAD",
                "reason": "RAW_FALLBACK",
                "volume": raw_fallback,
                "action": "Tier 0 must emit a valid trigger_state. RAW_FALLBACK is never executable.",
            }
        )
    combos = dict(taxonomy.get("no_doctrine_match", {}).get("by_surface_trigger_combination", {}) or {})
    for combo, count in combos.items():
        volume = int(count or 0)
        if volume <= 0:
            continue
        fixes.append(
            {
                "priority": 2,
                "fix_type": "MISSING_DOCTRINE",
                "reason": f"no doctrine for {combo}",
                "volume": volume,
                "action": f"Add coverage or loosen energy gates for {combo}.",
            }
        )
    return sorted(fixes, key=lambda row: (int(row.get("priority", 99)), -int(row.get("volume", 0))))


def _update_taxonomy(result: GatewayResult, reason: str, payload: Dict[str, Any], doctrine_id: str | None = None) -> None:
    DIAGNOSTICS_DIR.mkdir(parents=True, exist_ok=True)
    taxonomy = _load_taxonomy()
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    session_label = str(payload.get("session_label", payload.get("session", "")) or "")
    taxonomy["generated_at"] = now
    session_range = dict(taxonomy.get("session_range", {}) or {})
    if session_label and not session_range.get("first_session"):
        session_range["first_session"] = session_label
    if session_label:
        session_range["last_session"] = session_label
        seen_count = int(session_range.get("session_count", 0) or 0)
        session_range["session_count"] = max(seen_count, 1)
    taxonomy["session_range"] = session_range

    totals = dict(taxonomy.get("totals", {}) or {})
    totals["all_candidates"] = int(totals.get("all_candidates", 0) or 0) + 1
    taxonomy["totals"] = totals

    if result == GatewayResult.HARD_REJECT:
        bucket = dict(taxonomy.get("hard_reject", {}) or {})
        bucket["total"] = int(bucket.get("total", 0) or 0) + 1
        by_reason = dict(bucket.get("by_reason", {}) or {})
        reject_key = "RAW_FALLBACK" if reason.endswith("RAW_FALLBACK") or reason == "RAW_FALLBACK" else reason
        stats = dict(by_reason.get(reject_key, {}) or {})
        stats["count"] = int(stats.get("count", 0) or 0) + 1
        stats["pct_of_all_candidates"] = 0.0
        by_reason[reject_key] = stats
        bucket["by_reason"] = by_reason
        taxonomy["hard_reject"] = bucket
    elif result == GatewayResult.NO_DOCTRINE_MATCH:
        bucket = dict(taxonomy.get("no_doctrine_match", {}) or {})
        bucket["total"] = int(bucket.get("total", 0) or 0) + 1
        combo = "|".join(
            [
                _clean_text(payload.get("surface_type"), "UNKNOWN"),
                _clean_text(payload.get("trigger_state"), "UNKNOWN"),
                _clean_text(payload.get("distance_mode"), "UNKNOWN"),
                _clean_text(payload.get("direction"), "UNKNOWN"),
            ]
        )
        by_combo = dict(bucket.get("by_surface_trigger_combination", {}) or {})
        by_combo[combo] = int(by_combo.get(combo, 0) or 0) + 1
        bucket["by_surface_trigger_combination"] = by_combo
        taxonomy["no_doctrine_match"] = bucket
    else:
        bucket = dict(taxonomy.get("matched", {}) or {})
        bucket["total"] = int(bucket.get("total", 0) or 0) + 1
        by_doctrine = dict(bucket.get("by_doctrine_id", {}) or {})
        key = str(doctrine_id or "UNKNOWN")
        by_doctrine[key] = int(by_doctrine.get(key, 0) or 0) + 1
        bucket["by_doctrine_id"] = by_doctrine
        taxonomy["matched"] = bucket

    all_candidates = max(1, int(totals.get("all_candidates", 0) or 0))
    hard_total = int(dict(taxonomy.get("hard_reject", {}) or {}).get("total", 0) or 0)
    valid_payload_total = max(0, all_candidates - hard_total)
    unmatched_total = int(dict(taxonomy.get("no_doctrine_match", {}) or {}).get("total", 0) or 0)
    matched_total = int(dict(taxonomy.get("matched", {}) or {}).get("total", 0) or 0)
    taxonomy["totals"]["invalid_payload_rate"] = round(hard_total / all_candidates, 6)
    taxonomy["totals"]["valid_unmatched_rate"] = round(unmatched_total / max(1, valid_payload_total), 6)
    taxonomy["totals"]["match_rate"] = round(matched_total / all_candidates, 6)
    taxonomy["no_doctrine_match"]["pct_of_valid_payloads"] = round(unmatched_total / max(1, valid_payload_total), 6)
    taxonomy["matched"]["pct_of_all_candidates"] = round(matched_total / all_candidates, 6)
    taxonomy["highest_value_fixes"] = compute_highest_value_fixes(taxonomy)
    PAYLOAD_REJECT_TAXONOMY_PATH.write_text(json.dumps(taxonomy, indent=2), encoding="utf-8")


def validate_payload(payload: Dict[str, Any]) -> Tuple[bool, str]:
    if _clean_text(payload.get("distance_mode")) == "FAR_DISTANCE":
        return False, "FAR_DISTANCE"
    if _clean_text(payload.get("direction")) not in {"LONG", "SHORT"}:
        return False, f"INVALID_VALUE:direction:{payload.get('direction')}"
    if _clean_text(payload.get("trigger_state")) == "RAW_FALLBACK":
        return False, "RAW_FALLBACK"
    if not dict(payload.get("energy_snapshot", {}) or {}):
        return False, "MISSING_FIELD:energy_snapshot"
    for field, valid_values in REQUIRED_PAYLOAD_FIELDS.items():
        if field not in payload:
            return False, f"MISSING_FIELD:{field}"
        if valid_values is not None and _clean_text(payload.get(field)) not in valid_values:
            return False, f"INVALID_VALUE:{field}:{payload.get(field)}"
    level_event_valid, level_reason = validate_level_event_payload(payload)
    if not level_event_valid:
        return False, level_reason
    return True, "VALID"


def build_rejection_audit_row(payload: Dict[str, Any], reason: str) -> Dict[str, Any]:
    return {
        "ts": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "reject_reason": str(reason or ""),
        "payload_dump": payload,
        "was_fallback": _clean_text(payload.get("trigger_state")) == "RAW_FALLBACK",
    }


def process_payload(
    payload: Dict[str, Any],
    metrics: Dict[str, Any] | None = None,
) -> Tuple[GatewayResult, str, Dict[str, Any] | None]:
    valid, reject_reason = validate_payload(payload)
    if not valid:
        _update_taxonomy(GatewayResult.HARD_REJECT, reject_reason, payload)
        return GatewayResult.HARD_REJECT, reject_reason, None
    doctrine_id, match_result = match_doctrine(payload)
    if doctrine_id is None:
        reason = "no_registry_entry_or_gates_too_strict"
        _update_taxonomy(GatewayResult.NO_DOCTRINE_MATCH, reason, payload)
        return GatewayResult.NO_DOCTRINE_MATCH, reason, None
    packet = build_trade_gateway_packet(payload, metrics=metrics, doctrine_id=doctrine_id, doctrine_match_result=match_result)
    _update_taxonomy(GatewayResult.MATCHED, "ok", payload, doctrine_id=doctrine_id)
    return GatewayResult.MATCHED, "ok", packet


def build_trade_gateway_packet(
    payload: Dict[str, Any],
    metrics: Dict[str, Any] | None = None,
    *,
    doctrine_id: str | None = None,
    doctrine_match_result: str = "MATCHED",
) -> Dict[str, Any]:
    """Canonical owner of normalization, economic gate, priority, and AEE handoff shaping."""
    doctrine_id = str(doctrine_id or "")
    packet = normalize_trade_intent(payload, metrics)
    canonical_entry = (
        build_canonical_entry_packet(
            payload,
            doctrine_id=doctrine_id,
            reject_reason="",
            doctrine_match_result=doctrine_match_result,
            payload_validated=True,
        )
        if doctrine_id
        else None
    )
    validation = validate_trade_packet(packet)
    packet["validation"] = validation
    packet["economic_gate"] = evaluate_economic_viability(packet)
    packet["aee_handoff"] = shape_aee_handoff(packet)
    if canonical_entry:
        doctrine_spec = dict(DOCTRINE_REGISTRY.get(doctrine_id, {}) or {})
        canonical_entry["sl_rule"] = str(doctrine_spec.get("sl_rule", canonical_entry.get("sl_rule", "")) or "")
        canonical_entry["tp_rule"] = str(doctrine_spec.get("tp_rule", canonical_entry.get("tp_rule", "")) or "")
        canonical_entry["runner_eligible"] = bool(doctrine_spec.get("runner_logic", False))
        canonical_entry["partial_close_eligible"] = bool(doctrine_spec.get("partial_close_eligible", False))
        canonical_entry["aee_params"] = get_directional_aee_params(canonical_entry)
        packet["canonical_entry"] = canonical_entry
        packet["gateway_result"] = GatewayResult.MATCHED.value
    else:
        packet["gateway_result"] = GatewayResult.NO_DOCTRINE_MATCH.value
    return packet


def build_ranked_trade_gateway_packets(
    payloads: Iterable[Dict[str, Any]],
    metrics_by_strategy: Dict[str, Dict[str, Any]] | None = None,
) -> List[Dict[str, Any]]:
    metrics_by_strategy = metrics_by_strategy or {}
    packets = []
    for payload in payloads:
        strategy_id = str(payload.get("strategy_id", payload.get("setup", "")) or "")
        result, reason, packet = process_payload(payload, metrics_by_strategy.get(strategy_id))
        if packet is None:
            packet = {
                "packet_version": "trade_gateway_packet_v1",
                "gateway_result": result.value,
                "reject_reason": reason,
                "rejection_audit": build_rejection_audit_row(payload, reason),
                "source_payload": payload,
            }
        packets.append(packet)
    return rank_trade_packets(packets)
