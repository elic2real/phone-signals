#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import re
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
LOGS = ROOT / "logs"


def _parse_iso_ts(value: str) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    # Normalize trailing Z for fromisoformat compatibility.
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except Exception:
        return None


def _load_red_sample(limit: int = 18) -> List[dict]:
    rows: List[dict] = []
    with (ROOT / "critical_sl_audit_20.csv").open(newline="") as f:
        for row in csv.DictReader(f):
            tid_raw = str(row.get("trade_id") or "").strip()
            if tid_raw and tid_raw.lower() not in {"na", "none", "null"}:
                try:
                    row["_tid"] = int(float(tid_raw))
                except Exception:
                    row["_tid"] = None
            else:
                row["_tid"] = None
            rows.append(row)
    return rows[:limit]


def _scan_runtime(trade_ids: set[int]) -> Tuple[Dict[int, List[Tuple[str, str]]], Dict[str, List[str]]]:
    runtime_events: Dict[int, List[Tuple[str, str]]] = {tid: [] for tid in trade_ids}
    recovered_by_pair: Dict[str, List[str]] = {}

    ts_re = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
    tid_re = re.compile(r"'trade_id':\s*(\d+)")
    pair_re = re.compile(r"'pair':\s*'([^']+)'")

    for path in sorted(LOGS.glob("runtime.log*")):
        for line in path.read_text(errors="ignore").splitlines():
            ts_m = ts_re.search(line)
            ts = ts_m.group(1) if ts_m else ""
            if any(k in line for k in ("AEE_EVAL_SNAPSHOT", "AEE_CLOSE_REQUEST", "AEE_CLOSE_RESPONSE", "EXIT_ATTEMPT", "EXIT_RESPONSE")):
                tid_m = tid_re.search(line)
                if tid_m:
                    tid = int(tid_m.group(1))
                    if tid in runtime_events:
                        runtime_events[tid].append((ts, line))
            if "STALE_STATE_RECOVERED" in line:
                pair_m = pair_re.search(line)
                if pair_m:
                    recovered_by_pair.setdefault(pair_m.group(1), []).append(ts)

    return runtime_events, recovered_by_pair


def _scan_structured(trade_ids: set[int]) -> Dict[int, dict]:
    out: Dict[int, Dict[str, str | None]] = {
        tid: {
            "fill": None,
            "entry_result": None,
            "managing": None,
            "periodic": None,
            "pair_close_complete": None,
        }
        for tid in trade_ids
    }

    for path in sorted(LOGS.glob("trades.jsonl*")):
        with path.open() as f:
            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    obj = json.loads(raw)
                except Exception:
                    continue
                try:
                    tid = int(obj.get("trade_id"))
                except Exception:
                    continue
                if tid not in out:
                    continue

                kind = str(obj.get("kind") or obj.get("event") or "")
                ts = str(obj.get("ts_utc") or obj.get("ts") or "")
                if kind == "ORDER_FILLED" and not out[tid]["fill"]:
                    out[tid]["fill"] = ts
                elif kind == "ENTRY_RESULT" and obj.get("result") == "FILLED" and not out[tid]["entry_result"]:
                    out[tid]["entry_result"] = ts
                elif kind == "STATE_TRANSITION" and str(obj.get("to_state") or "").upper() == "MANAGING" and not out[tid]["managing"]:
                    out[tid]["managing"] = ts
                elif kind == "AEE_PERIODIC_DECISION" and not out[tid]["periodic"]:
                    out[tid]["periodic"] = ts
                elif kind == "STATE_TRANSITION" and str(obj.get("reason") or "") == "pair_close_complete" and not out[tid]["pair_close_complete"]:
                    out[tid]["pair_close_complete"] = ts

    return out


def build_table() -> tuple[list[dict], dict]:
    sample = _load_red_sample()
    trade_ids = {r["_tid"] for r in sample if isinstance(r.get("_tid"), int)}
    runtime_events, recovered_by_pair = _scan_runtime(trade_ids)
    structured = _scan_structured(trade_ids)

    conn = sqlite3.connect(str(ROOT / "phone_bot.db"))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    rows: List[dict] = []
    for src in sample:
        tid = src.get("_tid")
        broker_tid = str(src.get("broker_trade_id") or "").strip()

        db = None
        if isinstance(tid, int):
            db = cur.execute(
                "select id,pair,dir,ts as entry_ts,oanda_trade_id from trades where id=?",
                (tid,),
            ).fetchone()
        if db is None and broker_tid:
            db = cur.execute(
                "select id,pair,dir,ts as entry_ts,oanda_trade_id from trades where oanda_trade_id=?",
                (broker_tid,),
            ).fetchone()
            if db is not None:
                tid = int(db["id"])

        pair = (db["pair"] if db else src.get("pair")) or ""
        direction = (db["dir"] if db else src.get("dir")) or ""
        broker_sl_time = str(src.get("broker_close_time") or "").strip()

        evs = runtime_events.get(int(tid), []) if isinstance(tid, int) else []
        first_eval = next((ts for ts, line in evs if "AEE_EVAL_SNAPSHOT" in line), "")
        first_action = next((ts for ts, line in evs if "AEE_CLOSE_REQUEST" in line or "EXIT_ATTEMPT" in line), "")
        first_response = next((ts for ts, line in evs if "AEE_CLOSE_RESPONSE" in line or "EXIT_RESPONSE" in line), "")

        st = structured.get(int(tid), {}) if isinstance(tid, int) else {}
        fill = st.get("fill") or st.get("entry_result") or (db["entry_ts"] if db else "")
        managing = st.get("managing") or ""
        periodic = st.get("periodic") or ""
        state_removed = st.get("pair_close_complete") or ""
        stale = (recovered_by_pair.get(pair) or [""])[0]

        did_control = "no"
        blocker = "no_pre_sl_aee_telemetry"
        if first_eval and (not broker_sl_time or first_eval < broker_sl_time):
            did_control = "yes"
            blocker = ""
        elif first_eval and broker_sl_time and first_eval >= broker_sl_time:
            blocker = "aee_eval_after_sl"
        else:
            et = _parse_iso_ts(src.get("entry_time") or "")
            ct = _parse_iso_ts(broker_sl_time)
            hold_secs = (ct - et).total_seconds() if et and ct else None
            if hold_secs is not None and hold_secs < 25:
                blocker = "hold_time_below_never_green_min_hold"
            elif not managing:
                blocker = "missing_managing_state_evidence"
            elif not periodic:
                blocker = "no_periodic_scan_evidence_for_trade"
            else:
                blocker = "telemetry_gap_or_path_bypass"

        rows.append(
            {
                "trade_id": tid if isinstance(tid, int) else "unavailable",
                "broker_trade_id": (db["oanda_trade_id"] if db else "") or "unavailable",
                "pair": pair or "unavailable",
                "direction": direction or "unavailable",
                "fill_time": fill or "unavailable",
                "managing_state_entered_time": managing or "unavailable",
                "aee_eligible_time": fill or "unavailable",
                "first_aee_eval_time": first_eval or "unavailable",
                "first_aee_action_time": first_action or "unavailable",
                "first_close_request_time": first_action or "unavailable",
                "first_close_response_time": first_response or "unavailable",
                "broker_sl_time": broker_sl_time or "unavailable",
                "state_removed_time": state_removed or "unavailable",
                "stale_recovery_time": stale or "unavailable",
                "did_aee_get_control_before_sl": did_control,
                "blocker_reason_if_no_control": blocker or "unavailable",
            }
        )

    summary = {
        "rows": len(rows),
        "control_yes": sum(1 for r in rows if r["did_aee_get_control_before_sl"] == "yes"),
        "control_no": sum(1 for r in rows if r["did_aee_get_control_before_sl"] == "no"),
        "blockers": dict(Counter(r["blocker_reason_if_no_control"] for r in rows)),
    }
    return rows, summary


def main() -> None:
    rows, summary = build_table()

    if not rows:
        print("No rows found in critical_sl_audit_20.csv with usable trade_id values.")
        return

    out_json = ROOT / "aee_red_path_control_audit.json"
    out_json.write_text(json.dumps({"rows": rows, "summary": summary}, indent=2) + "\n")

    cols = list(rows[0].keys())
    print("| " + " | ".join(cols) + " |")
    print("|" + "|".join(["---"] * len(cols)) + "|")
    for row in rows:
        print("| " + " | ".join(str(row[c]).replace("|", "/") for c in cols) + " |")
    print("\nSUMMARY")
    print(json.dumps(summary, indent=2))
    print(f"\nWROTE {out_json}")


if __name__ == "__main__":
    main()
