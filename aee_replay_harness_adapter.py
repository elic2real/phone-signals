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


def _ground_truth_outcomes(
    rows: list[dict[str, Any]],
    idx: int,
    *,
    target_distance: float,
    locked_floor_pips: float,
    objective_state: str = "MAXIMIZE_CONTINUATION",
) -> dict[str, Any]:
    here = rows[idx]
    now_pips = _safe_float(here.get("profit_now", here.get("pips", 0.0)), 0.0)
    future = rows[idx:]
    future_pips = [_safe_float(x.get("profit_now", x.get("pips", 0.0)), 0.0) for x in future]

    # v1 multi-horizon contract: immediate, short-horizon, and end-of-path outcomes.
    h1 = future_pips[min(1, len(future_pips) - 1)]
    h3 = future_pips[min(3, len(future_pips) - 1)]
    hend = future_pips[-1]

    close_now = now_pips
    hold_now = h1
    tighten_now = max(locked_floor_pips, h3)
    extend_now = max(future_pips)

    outcomes = {
        "CLOSE": close_now,
        "HOLD": hold_now,
        "TIGHTEN": tighten_now,
        "EXTEND": extend_now,
    }
    # Unconditional best: best across all possible actions over the full path.
    best_action = max(outcomes.items(), key=lambda kv: kv[1])[0]

    # Objective-conditioned best: uses the horizon appropriate for current objective.
    # MAXIMIZE_CONTINUATION: longer horizon (h3) — we're riding the trade.
    # MAXIMIZE_FLOOR / RELEASE_CAPITAL: nearest horizon (h1) — we're protecting floor.
    _horizon_map: dict[str, str] = {
        "MAXIMIZE_CONTINUATION": "h3",
        "MAXIMIZE_FLOOR": "h1",
        "RELEASE_CAPITAL": "h1",
    }
    conditioned_horizon_key = _horizon_map.get(str(objective_state), "h3")
    horizon_value = {"h1": h1, "h3": h3, "hend": hend}[conditioned_horizon_key]
    conditioned_outcomes = {
        "CLOSE": close_now,
        "HOLD": horizon_value,
        "TIGHTEN": max(locked_floor_pips, horizon_value),
        "EXTEND": extend_now if objective_state == "MAXIMIZE_CONTINUATION" else horizon_value,
    }
    conditioned_best_action = max(conditioned_outcomes.items(), key=lambda kv: kv[1])[0]

    return {
        "best_action": best_action,
        "conditioned_best_action": conditioned_best_action,
        "conditioned_horizon_key": conditioned_horizon_key,
        "outcomes_pips": outcomes,
        "conditioned_outcomes_pips": conditioned_outcomes,
        "horizons_pips": {
            "h1": h1,
            "h3": h3,
            "hend": hend,
        },
        "objective_conditioned_horizon": {
            "MAXIMIZE_CONTINUATION": "h3",
            "MAXIMIZE_FLOOR": "h1",
            "RELEASE_CAPITAL": "h1",
        },
        "target_distance": target_distance,
    }


def _build_context(
    row: dict[str, Any],
    *,
    idx: int,
    total_rows: int,
    target_distance: float,
    peak_pips: float,
    locked_floor_pips: float,
    bars_since_improvement: int,
    objective_state: str,
    objective_dwell_bars: int,
    objective_confirm_count: int,
    objective_pending_target: str,
    action_dwell_bars: int,
    last_action: str,
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
    t_norm = min(1.0, max(0.0, idx / max(1.0, float(total_rows))))
    time_unproductive_ratio = min(1.0, max(0.0, bars_since_improvement / max(1.0, float(idx))))
    time_since_last_progress = float(bars_since_improvement)
    productivity_rate = progress_r / max(1.0, float(idx))

    inefficiency_weight = _safe_float((policy_overrides or {}).get("inefficiency_weight", 1.0), 1.0)
    inefficiency_cost_r = inefficiency_weight * t_norm * time_unproductive_ratio
    locked_floor_r = max(0.0, locked_floor_pips / max(0.1, target_distance))
    # Continuation proxy: penalise giveback from peak so proxy degrades as trade retreats.
    # Range declared: [0, 2R].
    giveback_proxy_weight = _safe_float((policy_overrides or {}).get("giveback_proxy_weight", 0.50), 0.50)
    continuation_proxy_r = max(
        0.0,
        min(
            2.0,
            progress_r + (velocity_now * 0.75) - (time_unproductive_ratio * 0.35) - (giveback_r * giveback_proxy_weight),
        ),
    )

    return AEEContext(
        progress_r=progress_r,
        unrealized_pips=unrealized_pips,
        giveback_r=giveback_r,
        continuation_score=continuation_score,
        stall_score=stall_score,
        panic_trigger=panic_trigger,
        open_pnl_r=progress_r,
        locked_floor_r=locked_floor_r,
        giveback_from_peak_r=giveback_r,
        inefficiency_cost_r=inefficiency_cost_r,
        continuation_proxy_r=continuation_proxy_r,
        t_norm=t_norm,
        time_unproductive_ratio=time_unproductive_ratio,
        time_since_last_progress=time_since_last_progress,
        productivity_rate=productivity_rate,
        objective_state=str((policy_overrides or {}).get("objective_init_state", objective_state)),
        objective_dwell_bars=max(0, int(objective_dwell_bars)),
        objective_confirm_count=max(0, int(objective_confirm_count)),
        objective_pending_target=str(objective_pending_target or ""),
        action_dwell_bars=max(0, int(action_dwell_bars)),
        last_action=str(last_action or "HOLD"),
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
    objective_state = str((policy_overrides or {}).get("objective_init_state", "MAXIMIZE_CONTINUATION"))
    objective_dwell_bars = 0
    objective_confirm_count = 0
    objective_pending_target = ""
    action_dwell_bars = 0
    last_action = "HOLD"
    max_giveback_r = 0.0
    max_giveback_pips = 0.0
    final_aee_pips = _safe_float(rows[-1].get("profit_now", 0.0), 0.0)
    final_reason_code = "AEE_REPLAY_END_OF_PATH"
    final_transition = f"{state}->{state}"
    final_time_in_trade_sec = 0
    final_locked_profit_pips = 0.0
    ground_truth_trace: list[dict[str, Any]] = []

    if policy_overrides is None:
        policy_overrides = {}
    if "enable_objective_v1" not in policy_overrides:
        policy_overrides = dict(policy_overrides)
        policy_overrides["enable_objective_v1"] = 1.0

    for idx, row in enumerate(rows, start=1):
        pips = _safe_float(row.get("profit_now", row.get("pips", 0.0)), 0.0)
        if pips > peak_pips:
            peak_pips = pips
            bars_since_improvement = 0
        else:
            bars_since_improvement += 1

        # Target-lock floor: once pips reach or exceed the 1:1 target distance, lock in
        # target_distance as the floor — this matches the 1:1 baseline's guaranteed exit.
        if pips >= target_distance:
            final_locked_profit_pips = max(final_locked_profit_pips, target_distance)

        if state in {"HARVEST", "RUNNER"}:
            final_locked_profit_pips = max(final_locked_profit_pips, max(0.0, peak_pips * 0.40))

        ctx = _build_context(
            row,
            idx=idx,
            total_rows=len(rows),
            target_distance=target_distance,
            peak_pips=peak_pips,
            locked_floor_pips=final_locked_profit_pips,
            bars_since_improvement=bars_since_improvement,
            objective_state=objective_state,
            objective_dwell_bars=objective_dwell_bars,
            objective_confirm_count=objective_confirm_count,
            objective_pending_target=objective_pending_target,
            action_dwell_bars=action_dwell_bars,
            last_action=last_action,
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
        packet_meta = packet.get("meta") or {}
        objective_state = str(packet_meta.get("objective_state_after", objective_state))
        objective_dwell_bars = _safe_int(packet_meta.get("objective_dwell_bars", objective_dwell_bars), objective_dwell_bars)
        objective_confirm_count = _safe_int(packet_meta.get("objective_confirm_count", objective_confirm_count), objective_confirm_count)
        objective_pending_target = str(packet_meta.get("objective_pending_target", objective_pending_target))
        action_dwell_bars = _safe_int(packet_meta.get("action_dwell_bars", action_dwell_bars), action_dwell_bars)
        last_action = str(packet_meta.get("selected_internal_action", last_action))

        gt = _ground_truth_outcomes(
            rows, idx - 1,
            target_distance=target_distance,
            locked_floor_pips=final_locked_profit_pips,
            objective_state=objective_state,
        )
        packet_meta["ground_truth"] = gt
        packet["meta"] = packet_meta
        ground_truth_trace.append(
            {
                "bar_index": _safe_int(packet.get("bar_index", idx), idx),
                "objective_state": objective_state,
                "selected_action": last_action,
                "ground_truth_best_action": str(gt.get("best_action", "HOLD")),
                "ground_truth_conditioned_best_action": str(gt.get("conditioned_best_action", "HOLD")),
                "conditioned_horizon_key": str(gt.get("conditioned_horizon_key", "h3")),
                "ground_truth_outcomes_pips": dict(gt.get("outcomes_pips") or {}),
                "ground_truth_conditioned_outcomes_pips": dict(gt.get("conditioned_outcomes_pips") or {}),
            }
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

    # Unconditional alignment: selected action == horizon-agnostic best action.
    alignment_hits = sum(
        1
        for x in ground_truth_trace
        if str(x.get("selected_action", "")) == str(x.get("ground_truth_best_action", ""))
    )
    alignment_rate = (alignment_hits / len(ground_truth_trace)) if ground_truth_trace else 0.0

    # Objective-conditioned alignment: selected action == conditioned best action.
    conditioned_alignment_hits = sum(
        1
        for x in ground_truth_trace
        if str(x.get("selected_action", "")) == str(x.get("ground_truth_conditioned_best_action", ""))
    )
    conditioned_alignment_rate = (
        conditioned_alignment_hits / len(ground_truth_trace)
    ) if ground_truth_trace else 0.0

    # Per-objective alignment breakdown.
    obj_buckets: dict[str, list[bool]] = {}
    for step in ground_truth_trace:
        obj = str(step.get("objective_state", "MAXIMIZE_CONTINUATION"))
        hit = str(step.get("selected_action", "")) == str(step.get("ground_truth_conditioned_best_action", ""))
        obj_buckets.setdefault(obj, []).append(hit)
    alignment_by_objective: dict[str, dict[str, float]] = {
        obj: {
            "bars": len(hits),
            "alignment_rate": sum(hits) / len(hits) if hits else 0.0,
        }
        for obj, hits in sorted(obj_buckets.items())
    }

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
        "engine_version": "AEE_DISCOVERY_V1",
        "objective_state_final": objective_state,
        "ground_truth_trace": ground_truth_trace,
        "ground_truth_alignment_rate": alignment_rate,
        "ground_truth_conditioned_alignment_rate": conditioned_alignment_rate,
        "ground_truth_alignment_by_objective": alignment_by_objective,
    }


def _avg(values: list[float]) -> float:
    return (sum(values) / len(values)) if values else 0.0


def _aggregate_bucket(trades: list[dict[str, Any]]) -> dict[str, Any]:
    final_vals = [_safe_float(t.get("final_money_result_pips", 0.0), 0.0) for t in trades]
    baseline_vals = [_safe_float(t.get("baseline_money_result_pips", 0.0), 0.0) for t in trades]
    baseline_1to1_vals = [_safe_float(t.get("baseline_1to1_money_result_pips", t.get("baseline_money_result_pips", 0.0)), 0.0) for t in trades]
    baseline_protective_vals = [_safe_float(t.get("baseline_protective_money_result_pips", 0.0), 0.0) for t in trades]
    delta_vals = [_safe_float(t.get("delta_vs_baseline_pips", 0.0), 0.0) for t in trades]
    delta_1to1_vals = [_safe_float(t.get("delta_vs_1to1_baseline_pips", t.get("delta_vs_baseline_pips", 0.0)), 0.0) for t in trades]
    delta_protective_vals = [_safe_float(t.get("delta_vs_protective_baseline_pips", 0.0), 0.0) for t in trades]
    time_vals = [_safe_float(t.get("time_in_trade_sec", 0.0), 0.0) for t in trades]
    giveback_vals = [_safe_float(t.get("max_giveback_r", 0.0), 0.0) for t in trades]
    lock_vals = [_safe_float(t.get("locked_profit_pips", 0.0), 0.0) for t in trades]

    positive_delta_trades = sum(1 for d in delta_vals if d > 1e-9)
    negative_delta_trades = sum(1 for d in delta_vals if d < -1e-9)
    flat_delta_trades = len(delta_vals) - positive_delta_trades - negative_delta_trades

    return {
        "count": len(trades),
        "total_final_money_result_pips": sum(final_vals),
        "total_baseline_money_result_pips": sum(baseline_vals),
        "total_baseline_1to1_money_result_pips": sum(baseline_1to1_vals),
        "total_baseline_protective_money_result_pips": sum(baseline_protective_vals),
        "total_delta_vs_baseline_pips": sum(delta_vals),
        "total_delta_vs_1to1_baseline_pips": sum(delta_1to1_vals),
        "total_delta_vs_protective_baseline_pips": sum(delta_protective_vals),
        "avg_final_money_result_pips": _avg(final_vals),
        "avg_baseline_money_result_pips": _avg(baseline_vals),
        "avg_baseline_1to1_money_result_pips": _avg(baseline_1to1_vals),
        "avg_baseline_protective_money_result_pips": _avg(baseline_protective_vals),
        "avg_delta_vs_baseline_pips": _avg(delta_vals),
        "avg_delta_vs_1to1_baseline_pips": _avg(delta_1to1_vals),
        "avg_delta_vs_protective_baseline_pips": _avg(delta_protective_vals),
        "avg_time_in_trade_sec": _avg(time_vals),
        "avg_max_giveback_r": _avg(giveback_vals),
        "avg_locked_profit_pips": _avg(lock_vals),
        "positive_delta_trades": positive_delta_trades,
        "negative_delta_trades": negative_delta_trades,
        "flat_delta_trades": flat_delta_trades,
    }


def build_baseline_comparison_report(trade_results: list[dict[str, Any]]) -> dict[str, Any]:
    by_reason: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_transition: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for tr in trade_results:
        by_reason[str(tr.get("final_reason_code", "UNKNOWN"))].append(tr)
        by_transition[str(tr.get("final_state_transition", "UNKNOWN->UNKNOWN"))].append(tr)

    per_trade_delta = [
        {
            "trade_id": str(t.get("trade_id", "")),
            "policy_name": str(t.get("policy_name", "baseline")),
            "final_money_result_pips": _safe_float(t.get("final_money_result_pips", 0.0), 0.0),
            "baseline_money_result_pips": _safe_float(t.get("baseline_money_result_pips", 0.0), 0.0),
            "delta_vs_baseline_pips": _safe_float(t.get("delta_vs_baseline_pips", 0.0), 0.0),
            "final_reason_code": str(t.get("final_reason_code", "UNKNOWN")),
            "final_state_transition": str(t.get("final_state_transition", "UNKNOWN->UNKNOWN")),
            "time_in_trade_sec": _safe_float(t.get("time_in_trade_sec", 0.0), 0.0),
            "max_giveback_r": _safe_float(t.get("max_giveback_r", 0.0), 0.0),
            "locked_profit_pips": _safe_float(t.get("locked_profit_pips", 0.0), 0.0),
        }
        for t in trade_results
    ]

    return {
        "report_contract": {
            "baseline_definition": "baseline_money_result_pips is the fixed static result sourced from benchmark slice input (row.pips).",
            "candidate_definition": "final_money_result_pips is replay-kernel outcome from packet-emitting AEE state machine.",
            "delta_definition": "delta_vs_baseline_pips = final_money_result_pips - baseline_money_result_pips",
            "required_fields": [
                "per_trade_delta",
                "summary",
                "by_reason_code",
                "by_state_transition",
                "trade_results",
            ],
        },
        "summary": _aggregate_bucket(trade_results),
        "by_reason_code": {k: _aggregate_bucket(v) for k, v in sorted(by_reason.items())},
        "by_state_transition": {k: _aggregate_bucket(v) for k, v in sorted(by_transition.items())},
        "per_trade_delta": per_trade_delta,
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
