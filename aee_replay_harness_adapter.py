#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from aee_state_machine_v2 import AEEContext, AEEState, transition_aee_state_with_packet


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _to_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in {"1", "true", "yes", "y", "on"}


def _stable_trade_id(trade: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    explicit = str(trade.get("trade_id") or "").strip()
    if explicit:
        return explicit
    fingerprint_src = {
        "rows": rows,
        "target_distance": trade.get("target_distance"),
        "baseline_final_pips": trade.get("baseline_final_pips"),
        "meta": trade.get("meta"),
    }
    fingerprint = json.dumps(fingerprint_src, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(fingerprint.encode("utf-8")).hexdigest()[:12]
    return f"trade_{digest}"


def _deterministic_timestamp(idx: int, row: dict[str, Any]) -> str:
    ts = str(row.get("timestamp", "")).strip()
    if ts:
        return ts
    base = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=max(0, idx - 1))
    return base.isoformat().replace("+00:00", "Z")


def _run_simple_protective_baseline(rows: list[dict[str, Any]], target_distance: float) -> dict[str, Any]:
    peak_pips = -1e9
    locked_profit_pips = 0.0
    final_pips = _safe_float(rows[-1].get("profit_now", rows[-1].get("pips", 0.0)), 0.0)
    final_reason = "protective_end_of_path"
    final_time_in_trade_sec = len(rows) * 60

    for idx, row in enumerate(rows, start=1):
        pips = _safe_float(row.get("profit_now", row.get("pips", 0.0)), 0.0)
        velocity_now = _safe_float(row.get("velocity_now", 0.0), 0.0)
        progress_r = _safe_float(row.get("progress_ratio", pips / max(0.1, target_distance)), 0.0)

        if pips > peak_pips:
            peak_pips = pips
        locked_profit_pips = max(locked_profit_pips, max(0.0, peak_pips * 0.40))

        giveback_r = max(0.0, (peak_pips - pips) / max(0.1, target_distance))
        panic_trigger = _to_bool(row.get("panic_trigger", False)) or (progress_r <= -0.80 and velocity_now <= -0.10)

        if panic_trigger:
            final_pips = pips
            final_reason = "protective_panic_exit"
            final_time_in_trade_sec = idx * 60
            break
        if giveback_r >= 0.70:
            final_pips = max(pips, locked_profit_pips)
            final_reason = "protective_giveback_exit"
            final_time_in_trade_sec = idx * 60
            break
        if idx == len(rows):
            final_pips = max(pips, locked_profit_pips)
            final_reason = "protective_end_of_path"
            final_time_in_trade_sec = idx * 60

    return {
        "final_money_result_pips": final_pips,
        "final_reason_code": final_reason,
        "time_in_trade_sec": final_time_in_trade_sec,
        "locked_profit_pips": locked_profit_pips,
    }


def _build_context(
    row: dict[str, Any],
    *,
    target_distance: float,
    peak_pips: float,
    bars_since_improvement: int,
    policy_overrides: dict[str, float] | None = None,
) -> AEEContext:
    unrealized_pips = _safe_float(row.get("profit_now", row.get("pips", 0.0)), 0.0)
    progress_r = _safe_float(row.get("progress_ratio", unrealized_pips / max(0.1, target_distance)), 0.0)
    velocity_now = _safe_float(row.get("velocity_now", 0.0), 0.0)
    continuation_score = _safe_float(row.get("continuation_score", 0.5 + min(0.5, max(-0.5, velocity_now / 5.0))), 0.0)
    stall_score = _safe_float(
        row.get("stall_score", min(1.0, bars_since_improvement / 6.0) if abs(velocity_now) <= 0.05 else 0.1),
        0.0,
    )
    panic_trigger = _to_bool(row.get("panic_trigger", False))
    infer_panic_enabled = True
    panic_progress_r_threshold = -0.80
    panic_velocity_threshold = -0.10
    if policy_overrides:
        infer_panic_enabled = not _to_bool(policy_overrides.get("disable_panic_inference", False))
        panic_progress_r_threshold = _safe_float(policy_overrides.get("panic_infer_progress_r", panic_progress_r_threshold), panic_progress_r_threshold)
        panic_velocity_threshold = _safe_float(policy_overrides.get("panic_infer_velocity", panic_velocity_threshold), panic_velocity_threshold)
    if infer_panic_enabled and (not panic_trigger) and progress_r <= panic_progress_r_threshold and velocity_now <= panic_velocity_threshold:
        panic_trigger = True

    giveback_r = max(0.0, (peak_pips - unrealized_pips) / max(0.1, target_distance))
    return AEEContext(
        progress_r=progress_r,
        unrealized_pips=unrealized_pips,
        giveback_r=giveback_r,
        continuation_score=continuation_score,
        stall_score=stall_score,
        panic_trigger=panic_trigger,
    )


def replay_trade_path(
    trade: dict[str, Any],
    *,
    initial_state: AEEState = "PROTECT",
    policy_overrides: dict[str, float] | None = None,
    policy_name: str = "baseline",
) -> dict[str, Any]:
    rows = list(trade.get("rows") or [])
    if not rows:
        raise ValueError("trade rows are required")

    trade_id = _stable_trade_id(trade, rows)
    meta = dict(trade.get("meta") or {})
    meta.setdefault("policy_name", str(policy_name))
    target_distance = max(0.1, _safe_float(trade.get("target_distance", rows[0].get("target_distance", 1.0)), 1.0))
    baseline_1to1_final_pips = _safe_float(
        trade.get("baseline_final_pips", rows[-1].get("static_pips", rows[-1].get("profit_now", 0.0))),
        0.0,
    )
    protective_baseline = _run_simple_protective_baseline(rows, target_distance)

    state: AEEState = initial_state
    packets: list[dict[str, Any]] = []
    peak_pips = -1e9
    bars_since_improvement = 0
    max_giveback_r = 0.0
    max_giveback_pips = 0.0
    final_aee_pips = _safe_float(rows[-1].get("profit_now", 0.0), 0.0)
    final_reason_code = "AEE_REPLAY_END_OF_PATH"
    final_transition = f"{state}->{state}"
    final_time_in_trade_sec = 0
    final_locked_profit_pips = 0.0

    for idx, row in enumerate(rows, start=1):
        pips = _safe_float(row.get("profit_now", row.get("pips", 0.0)), 0.0)
        if pips > peak_pips:
            peak_pips = pips
            bars_since_improvement = 0
        else:
            bars_since_improvement += 1

        ctx = _build_context(
            row,
            target_distance=target_distance,
            peak_pips=peak_pips,
            bars_since_improvement=bars_since_improvement,
            policy_overrides=policy_overrides,
        )
        timestamp = _deterministic_timestamp(idx, row)
        packet = transition_aee_state_with_packet(
            state,
            ctx,
            trade_id=trade_id,
            bar_index=_safe_int(row.get("bar_index", idx), idx),
            timestamp=timestamp,
            meta=meta,
            policy=policy_overrides,
        )
        packets.append(packet)

        giveback_pips = max(0.0, peak_pips - pips)
        max_giveback_pips = max(max_giveback_pips, giveback_pips)
        max_giveback_r = max(max_giveback_r, ctx.giveback_r)

        transition_key = f"{packet['state_before']}->{packet['state_after']}"
        state = packet["state_after"]

        # HARVEST and RUNNER imply protected profit behavior in this v2 adapter path.
        if packet["state_after"] in {"HARVEST", "RUNNER"}:
            final_locked_profit_pips = max(final_locked_profit_pips, max(0.0, peak_pips * 0.40))

        if packet["action"] == "FULL_EXIT":
            final_aee_pips = pips
            final_reason_code = str(packet["reason_code"])
            final_transition = transition_key
            final_time_in_trade_sec = _safe_int(packet.get("bar_index", idx), idx) * 60
            break

        if idx == len(rows):
            final_aee_pips = pips
            final_reason_code = str(packet["reason_code"])
            final_transition = transition_key
            final_time_in_trade_sec = _safe_int(packet.get("bar_index", idx), idx) * 60

    state_transitions = [f"{p['state_before']}->{p['state_after']}" for p in packets]
    baseline_protective_final_pips = _safe_float(protective_baseline.get("final_money_result_pips", 0.0), 0.0)

    return {
        "trade_id": trade_id,
        "packet_count": len(packets),
        "packets": packets,
        "state_transitions": state_transitions,
        "final_reason_code": final_reason_code,
        "final_state_transition": final_transition,
        "final_money_result_pips": final_aee_pips,
        "baseline_money_result_pips": baseline_1to1_final_pips,
        "baseline_1to1_money_result_pips": baseline_1to1_final_pips,
        "baseline_protective_money_result_pips": baseline_protective_final_pips,
        "delta_vs_baseline_pips": final_aee_pips - baseline_1to1_final_pips,
        "delta_vs_1to1_baseline_pips": final_aee_pips - baseline_1to1_final_pips,
        "delta_vs_protective_baseline_pips": final_aee_pips - baseline_protective_final_pips,
        "baseline_protective_reason_code": str(protective_baseline.get("final_reason_code", "protective_unknown")),
        "time_in_trade_sec": final_time_in_trade_sec,
        "max_giveback_r": max_giveback_r,
        "max_giveback_pips": max_giveback_pips,
        "locked_profit_pips": final_locked_profit_pips,
        "policy_name": str(policy_name),
    }


def _avg(values: list[float]) -> float:
    return (sum(values) / len(values)) if values else 0.0


def _aggregate_bucket(trades: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "count": len(trades),
        "avg_final_money_result_pips": _avg([_safe_float(t.get("final_money_result_pips", 0.0), 0.0) for t in trades]),
        "avg_baseline_1to1_money_result_pips": _avg([_safe_float(t.get("baseline_1to1_money_result_pips", t.get("baseline_money_result_pips", 0.0)), 0.0) for t in trades]),
        "avg_baseline_protective_money_result_pips": _avg([_safe_float(t.get("baseline_protective_money_result_pips", 0.0), 0.0) for t in trades]),
        "avg_delta_vs_1to1_baseline_pips": _avg([_safe_float(t.get("delta_vs_1to1_baseline_pips", t.get("delta_vs_baseline_pips", 0.0)), 0.0) for t in trades]),
        "avg_delta_vs_protective_baseline_pips": _avg([_safe_float(t.get("delta_vs_protective_baseline_pips", 0.0), 0.0) for t in trades]),
        "avg_time_in_trade_sec": _avg([_safe_float(t.get("time_in_trade_sec", 0.0), 0.0) for t in trades]),
        "avg_max_giveback_r": _avg([_safe_float(t.get("max_giveback_r", 0.0), 0.0) for t in trades]),
        "avg_locked_profit_pips": _avg([_safe_float(t.get("locked_profit_pips", 0.0), 0.0) for t in trades]),
    }


def build_baseline_comparison_report(trade_results: list[dict[str, Any]]) -> dict[str, Any]:
    by_reason: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_transition: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for tr in trade_results:
        by_reason[str(tr.get("final_reason_code", "UNKNOWN"))].append(tr)
        by_transition[str(tr.get("final_state_transition", "UNKNOWN->UNKNOWN"))].append(tr)

    return {
        "summary": _aggregate_bucket(trade_results),
        "by_reason_code": {k: _aggregate_bucket(v) for k, v in sorted(by_reason.items())},
        "by_state_transition": {k: _aggregate_bucket(v) for k, v in sorted(by_transition.items())},
        "trade_results": trade_results,
    }


def _load_input(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "trades" in payload:
        return list(payload.get("trades") or [])
    if isinstance(payload, list):
        return payload
    raise ValueError("Input JSON must be a list of trades or an object with a 'trades' key.")


def main() -> int:
    ap = argparse.ArgumentParser(description="Replay fixed trade paths through AEE v2 state-machine with packet emission.")
    ap.add_argument("--input", required=True, help="JSON file containing trades with per-bar rows")
    ap.add_argument("--output", required=True, help="Output JSON report path")
    ap.add_argument(
        "--packets-output",
        default="",
        help="Optional JSON path for flattened packet stream",
    )
    args = ap.parse_args()

    trades = _load_input(Path(args.input))
    trade_results = [replay_trade_path(t) for t in trades]
    report = build_baseline_comparison_report(trade_results)

    out_path = Path(args.output)
    out_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    if args.packets_output:
        all_packets: list[dict[str, Any]] = []
        for tr in trade_results:
            all_packets.extend(list(tr.get("packets") or []))
        Path(args.packets_output).write_text(json.dumps(all_packets, indent=2) + "\n", encoding="utf-8")

    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
