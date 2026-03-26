#!/usr/bin/env python3
from __future__ import annotations

from typing import Any


CANONICAL_ENTRY_FIELDS = [
    "pair",
    "timestamp",
    "entry_time",
    "session_id",
    "session",
    "weekday",
    "quarter",
    "direction",
    "direction_assumed",
    "target_distance",
    "price",
    "static_pips",
    "static_R",
    "static_reason",
    "tp_hit_min",
    "sl_hit_min",
    "future_mfe_pips",
    "future_mae_pips",
    "trade_id",
]


def build_canonical_selected_entry(row: dict[str, Any], seq: int) -> dict[str, Any] | None:
    static_pips = float(row["static_pips"])
    target = float(row["target_distance"])
    if abs(static_pips) != target:
        return None
    direction = str(row.get("direction") or row.get("direction_assumed") or "")
    timestamp = str(row.get("entry_time") or row.get("timestamp") or "")
    return {
        **row,
        "trade_id": f"T{seq:06d}",
        "entry_time": timestamp,
        "timestamp": timestamp,
        "direction": direction,
        "direction_assumed": str(row.get("direction_assumed") or direction),
        "static_pips": static_pips,
        "static_R": round(static_pips / max(target, 1e-9), 6),
        "static_reason": "TP_HIT" if static_pips > 0 else "SL_HIT",
    }


def build_selected_entries_from_population(entry_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    for seq, row in enumerate(entry_rows, start=1):
        payload = build_canonical_selected_entry(row, seq)
        if payload is not None:
            selected.append(payload)
    return selected


def build_selected_entries_from_truth(
    truth_rows: list[dict[str, Any]],
    rule_applies,
    entry_rules: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    trade_seq = 0
    for row in truth_rows:
        for rule in entry_rules:
            if rule_applies(row, rule):
                trade_seq += 1
                payload = build_canonical_selected_entry(row, trade_seq)
                if payload is not None:
                    selected.append(payload)
                break
    return selected


def validate_canonical_entry_rows(rows: list[dict[str, Any]]) -> None:
    required = {
        "trade_id",
        "entry_time",
        "timestamp",
        "direction",
        "target_distance",
        "price",
        "static_pips",
        "static_R",
        "static_reason",
        "quarter",
        "session_id",
    }
    for idx, row in enumerate(rows):
        missing = sorted(k for k in required if k not in row or row[k] in (None, ""))
        if missing:
            raise KeyError(f"canonical entry row {idx} missing required fields: {missing}")
