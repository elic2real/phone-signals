#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "logs"
DB_PATH = ROOT / "phone_bot.db"

TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})")
TRADE_ID_RE = re.compile(r"'trade_id':\s*(\d+)")
DB_TRADE_ID_RE = re.compile(r"'db_trade_id':\s*(\d+)")
ENDPOINT_RE = re.compile(r"'endpoint':\s*'([^']+)'")
STATUS_RE = re.compile(r"'status':\s*([^,}]+)")


@dataclass
class TradeRow:
    trade_id: int
    pair: str
    direction: str
    state: str
    broker_trade_id: str
    note: str


def _parse_iso(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def _fmt_runtime_ts(line: str) -> str:
    m = TS_RE.search(line)
    return m.group(1) if m else ""


def _extract_trade_id(line: str) -> int | None:
    m = TRADE_ID_RE.search(line)
    if m:
        return int(m.group(1))
    m = DB_TRADE_ID_RE.search(line)
    if m:
        return int(m.group(1))
    return None


def _extract_status(line: str) -> str:
    m = STATUS_RE.search(line)
    if not m:
        return ""
    return m.group(1).strip().strip("'\"")


def _extract_endpoint(line: str) -> str:
    m = ENDPOINT_RE.search(line)
    return m.group(1).strip() if m else ""


def _decision_to_command(decision: str, direction: str) -> str:
    d = str(decision or "HOLD").upper()
    side = "long" if direction.upper() == "LONG" else "short"
    if d == "HOLD":
        return "NOOP"
    if d in {"TIGHTEN", "TIGHTEN_SL", "PROTECT", "PROTECTIVE_TIGHTEN"}:
        return "modify_stop_loss"
    if d in {"PARTIAL", "PARTIAL_CLOSE"}:
        return "close_partial_units"
    return f"close_position(side={side},units=ALL)"


def _load_latest_trades(limit: int) -> list[TradeRow]:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id AS trade_id, pair, dir AS direction, state, COALESCE(oanda_trade_id, '') AS broker_trade_id,
               COALESCE(note, '') AS note
        FROM trades
        ORDER BY id DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()
    return [
        TradeRow(
            trade_id=int(r["trade_id"]),
            pair=str(r["pair"] or ""),
            direction=str(r["direction"] or ""),
            state=str(r["state"] or ""),
            broker_trade_id=str(r["broker_trade_id"] or ""),
            note=str(r["note"] or ""),
        )
        for r in rows
    ]


def _load_trade_decisions(trade_ids: set[int]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {tid: {"aee_eval_time": "", "aee_decision": ""} for tid in trade_ids}
    p = LOGS / "trades.jsonl"
    if not p.exists():
        return out

    for raw in p.read_text(errors="ignore").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        if str(obj.get("kind")) != "AEE_PERIODIC_DECISION":
            continue
        try:
            tid = int(obj.get("trade_id"))
        except Exception:
            continue
        if tid not in out:
            continue

        ts = str(obj.get("ts_utc") or obj.get("ts") or "")
        decision = str(obj.get("decision") or "HOLD")

        # Prefer first actionable decision; otherwise first seen decision.
        cur_decision = out[tid]["aee_decision"]
        if not cur_decision:
            out[tid] = {"aee_eval_time": ts, "aee_decision": decision}
            continue
        if cur_decision.upper() == "HOLD" and decision.upper() != "HOLD":
            out[tid] = {"aee_eval_time": ts, "aee_decision": decision}

    return out


def _load_runtime_bridge(trade_ids: set[int]) -> dict[int, dict[str, Any]]:
    out: dict[int, dict[str, Any]] = {
        tid: {
            "translated_command": "",
            "command_send_time": "",
            "broker_endpoint_used": "",
            "broker_response_time": "",
            "broker_response_status": "",
            "response_line": "",
        }
        for tid in trade_ids
    }

    runtime_paths = sorted(LOGS.glob("runtime.log*"))
    for path in runtime_paths:
        for line in path.read_text(errors="ignore").splitlines():
            if not any(k in line for k in ("AEE_OANDA_TRANSPORT", "AEE_CLOSE_REQUEST", "EXIT_ATTEMPT", "AEE_CLOSE_RESPONSE", "EXIT_RESPONSE")):
                continue
            tid = _extract_trade_id(line)
            if tid is None or tid not in out:
                continue

            if "AEE_OANDA_TRANSPORT" in line:
                if not out[tid]["command_send_time"]:
                    out[tid]["command_send_time"] = _fmt_runtime_ts(line)
                ep = _extract_endpoint(line)
                if ep:
                    out[tid]["broker_endpoint_used"] = ep
            elif "AEE_CLOSE_REQUEST" in line or "EXIT_ATTEMPT" in line:
                if not out[tid]["command_send_time"]:
                    out[tid]["command_send_time"] = _fmt_runtime_ts(line)
                if "EXIT_ATTEMPT" in line and not out[tid]["translated_command"]:
                    if "'longUnits': 'ALL'" in line:
                        out[tid]["translated_command"] = "close_position(side=long,units=ALL)"
                    elif "'shortUnits': 'ALL'" in line:
                        out[tid]["translated_command"] = "close_position(side=short,units=ALL)"
                    else:
                        out[tid]["translated_command"] = "close_position"
            elif "AEE_CLOSE_RESPONSE" in line or "EXIT_RESPONSE" in line:
                if not out[tid]["broker_response_time"]:
                    out[tid]["broker_response_time"] = _fmt_runtime_ts(line)
                    out[tid]["broker_response_status"] = _extract_status(line)
                    out[tid]["response_line"] = line

    return out


def _load_state_transition_after_response(trade_ids: set[int], response_times: dict[int, str]) -> dict[int, str]:
    out = {tid: "" for tid in trade_ids}
    p = LOGS / "trades.jsonl"
    if not p.exists():
        return out

    for raw in p.read_text(errors="ignore").splitlines():
        raw = raw.strip()
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue

        kind = str(obj.get("kind") or obj.get("event") or "")
        if kind != "STATE_TRANSITION":
            continue
        meta = obj.get("metadata") if isinstance(obj.get("metadata"), dict) else {}
        tid_val = obj.get("trade_id")
        if tid_val is None and isinstance(meta, dict):
            tid_val = meta.get("trade_id")
        try:
            tid = int(tid_val)
        except Exception:
            continue
        if tid not in out or out[tid]:
            continue

        ts = str(obj.get("ts_utc") or obj.get("ts") or "")
        resp_ts = response_times.get(tid, "")
        if not resp_ts:
            continue
        ts_obj = _parse_iso(ts)
        resp_obj = _parse_iso(resp_ts)
        if resp_obj and ts_obj and ts_obj < resp_obj:
            continue

        from_s = str(obj.get("from") or obj.get("from_state") or "")
        to_s = str(obj.get("to") or obj.get("to_state") or "")
        reason = str(obj.get("reason") or "")
        out[tid] = f"{from_s}->{to_s} ({reason}) @ {ts}".strip()

    return out


def build_bridge_table(limit: int) -> list[dict[str, Any]]:
    trades = _load_latest_trades(limit)
    trade_ids = {t.trade_id for t in trades}

    decisions = _load_trade_decisions(trade_ids)
    bridge = _load_runtime_bridge(trade_ids)
    response_ts = {tid: str(v.get("broker_response_time") or "") for tid, v in bridge.items()}
    post_state = _load_state_transition_after_response(trade_ids, response_ts)

    rows: list[dict[str, Any]] = []
    for tr in trades:
        dec = decisions.get(tr.trade_id, {})
        b = bridge.get(tr.trade_id, {})
        decision = str(dec.get("aee_decision") or "")

        translated = str(b.get("translated_command") or "")
        if not translated:
            translated = _decision_to_command(decision or "HOLD", tr.direction)

        endpoint_used = str(b.get("broker_endpoint_used") or "")
        if not endpoint_used and str(b.get("command_send_time") or "") and translated.startswith("close_position"):
            endpoint_used = f"/v3/accounts/{{account_id}}/positions/{tr.pair}/close"

        local_state = str(post_state.get(tr.trade_id) or "")
        if not local_state:
            local_state = f"DB state={tr.state} note={tr.note}".strip()

        rows.append(
            {
                "trade_id": tr.trade_id,
                "broker_trade_id": tr.broker_trade_id,
                "AEE eval time": str(dec.get("aee_eval_time") or ""),
                "AEE decision": decision or "",
                "translated command": translated,
                "command send time": str(b.get("command_send_time") or ""),
                "broker endpoint used": endpoint_used,
                "broker response time": str(b.get("broker_response_time") or ""),
                "broker response status": str(b.get("broker_response_status") or ""),
                "local state transition after response": local_state,
            }
        )

    return rows


def print_markdown(rows: list[dict[str, Any]]) -> None:
    if not rows:
        print("No rows")
        return
    cols = list(rows[0].keys())
    print("| " + " | ".join(cols) + " |")
    print("|" + "|".join(["---"] * len(cols)) + "|")
    for row in rows:
        vals = [str(row.get(c, "")).replace("|", "/") for c in cols]
        print("| " + " | ".join(vals) + " |")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build AEE decision-to-broker execution bridge audit table")
    parser.add_argument("--limit", type=int, default=10, help="Number of latest trades to audit")
    parser.add_argument("--json-out", default=str(ROOT / "aee_execution_bridge_audit.json"))
    args = parser.parse_args()

    rows = build_bridge_table(args.limit)
    out_path = Path(args.json_out)
    out_path.write_text(json.dumps({"rows": rows, "limit": args.limit}, indent=2) + "\n")

    print_markdown(rows)
    print(f"\nWROTE {out_path}")


if __name__ == "__main__":
    main()
