from __future__ import annotations
# --- CHANGE LIST: TELEMETRY + REJECT REASONS ---
def log_exit_telemetry(exit_reason, rule_trace, **kwargs):
    print(f"EXIT: {exit_reason} | Trace: {rule_trace} | Extra: {kwargs}")

#!/usr/bin/env python3
"""Tick replay simulator harness for phone_bot AEE validation.

Includes:
- runtime wire swap (no source edits to production bot logic)
- bucket scheduler + tick-mode override
- two-leg management output (core 80% + runner 20%)
"""

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import phone_bot as phone_bot


@dataclass
class Tick:
    instrument: str
    ts: float
    bid: float
    ask: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0


class SimLogCapture:
    def __init__(self) -> None:
        self.events: List[Dict[str, Any]] = []
        self.event_counts: Dict[str, int] = defaultdict(int)

    def log_runtime(self, level: str, msg: str, *args: Any, **fields: Any) -> None:
        payload: Dict[str, Any] = {}
        if args:
            payload["args"] = [str(a) for a in args]
        payload.update(fields)
        self.append(msg, payload, level=level)

    def append(self, event: str, payload: Dict[str, Any], level: str = "info") -> None:
        self.events.append({"idx": len(self.events), "event": str(event), "level": str(level), "payload": payload})
        self.event_counts[str(event)] += 1

    def summary(self) -> Dict[str, Any]:
        return {
            "total_events": len(self.events),
            "event_counts": dict(self.event_counts),
            "first_event": self.events[0] if self.events else None,
            "last_event": self.events[-1] if self.events else None,
        }


class SimPriceFeed:
    def __init__(self) -> None:
        self._last: Dict[str, Tick] = {}

    def push_tick(self, instrument: str, tick: Tick) -> None:
        self._last[instrument] = tick

    def get_tick(self, instrument: str) -> Optional[Tick]:
        return self._last.get(instrument)


class SimOanda:
    def __init__(self, price_feed: SimPriceFeed, ticks_by_inst: Dict[str, List[Tick]]) -> None:
        self.price_feed = price_feed
        self.ticks_by_inst = ticks_by_inst

    def pricing(self, pair: str) -> Tuple[float, float]:
        t = self.price_feed.get_tick(pair)
        if t is None:
            raise RuntimeError(f"pricing_missing:{pair}")
        return t.bid, t.ask

    def pricing_multi(self, pairs: List[str]) -> Dict[str, Tuple[float, float, float]]:
        out: Dict[str, Tuple[float, float, float]] = {}
        for p in pairs:
            t = self.price_feed.get_tick(p)
            if t is not None:
                out[p] = (t.bid, t.ask, t.ts)
        return out

    def candles(self, instrument: str, granularity: str, count: int = 500, *, price: str = "M") -> List[Dict[str, Any]]:
        _ = price
        tf = str(granularity).upper()
        bucket_sec = {"M1": 60, "H1": 3600, "H4": 14400}.get(tf, 60)
        candles = build_candles(self.ticks_by_inst.get(instrument, []), bucket_sec)
        return candles[-int(count):]


class SimEnvironment:
    def __init__(self, instruments: List[str], ticks_by_inst: Dict[str, List[Tick]], bucket_sec: float = 5.0) -> None:
        self.instruments = instruments
        self.ticks_by_inst = ticks_by_inst
        self.bucket_sec = max(0.1, float(bucket_sec))

        self.logs = SimLogCapture()
        self.price_feed = SimPriceFeed()
        self.oanda = SimOanda(self.price_feed, ticks_by_inst)

        self._orig_o = getattr(phone_bot, "o", None)
        self._orig_log_runtime = getattr(phone_bot, "log_runtime", None)
        self._orig_get_candles = getattr(phone_bot, "get_candles", None)
        self._orig_aee_states = getattr(phone_bot, "aee_states", None)
        self._orig_check_aee_exits = getattr(phone_bot, "check_aee_exits", None)

        self._current_tick_idx = 0
        self._current_pair_eval_idx = 0
        self._current_ts = 0.0
        self._current_pair = ""
        self._last_eval_trace: Optional[Dict[str, Any]] = None

        phone_bot.o = self.oanda
        phone_bot.log_runtime = self.logs.log_runtime
        phone_bot.get_candles = self._get_candles
        phone_bot.aee_states = {}

        # Patch: Use centralized arbiter for all exit logic
        def arbiter_exit(trade, metrics, aee_state, current_price, *, survival_mode=False, runner_ctx=None, eval_mode="NORMAL", now_ts_val=None):
            # Use the centralized arbiter from phone_bot
            pair = trade["pair"]
            direction = trade["dir"]
            bid = current_price if direction == "LONG" else current_price
            ask = current_price if direction == "SHORT" else current_price
            mid = current_price
            now = now_ts_val if now_ts_val is not None else self._current_ts
            spread_pips = phone_bot.to_pips(pair, ask - bid)
            speed_class = phone_bot.speed_class_from_setup_name(str(trade.get("setup", "")))
            arbiter_result = phone_bot.final_exit_decision(trade, pair, direction, bid, ask, mid, now, spread_pips, speed_class)
            return arbiter_result["reason"] if arbiter_result and arbiter_result.get("reason") else None
        phone_bot.check_aee_exits = arbiter_exit

    def restore_live_wiring(self) -> None:
        phone_bot.o = self._orig_o
        phone_bot.log_runtime = self._orig_log_runtime
        phone_bot.get_candles = self._orig_get_candles
        if self._orig_aee_states is not None:
            phone_bot.aee_states = self._orig_aee_states
        if self._orig_check_aee_exits is not None:
            phone_bot.check_aee_exits = self._orig_check_aee_exits  # type: ignore[assignment]

    def _get_candles(self, pair: str, tf: str, count: int) -> List[Dict[str, Any]]:
        return self.oanda.candles(pair, tf, count)

    def _instrumented_check_aee_exits(
        self,
        trade: dict,
        metrics: dict,
        aee_state: Any,
        current_price: float,
        *,
        survival_mode: bool = False,
        runner_ctx: Optional[dict] = None,
        eval_mode: str = "NORMAL",
        now_ts_val: Optional[float] = None,
    ) -> Optional[str]:
        if not callable(self._orig_check_aee_exits):
            return None

        truths = compute_aee_truths(
            metrics=metrics,
            aee_state=aee_state,
            current_price=float(current_price),
            eval_mode=str(eval_mode),
            now_ts_val=float(now_ts_val if now_ts_val is not None else self._current_ts),
            runner_ctx=runner_ctx,
        )

        chosen = self._orig_check_aee_exits(
            trade,
            metrics,
            aee_state,
            current_price,
            survival_mode=survival_mode,
            runner_ctx=runner_ctx,
            eval_mode=eval_mode,
            now_ts_val=now_ts_val,
        )

        self.logs.append(
            "AEE_EVAL_TRACE",
            {
                "idx": self._current_tick_idx,
                "pair_eval_idx": self._current_pair_eval_idx,
                "ts": self._current_ts,
                "instrument": self._current_pair,
                "trade_id": int(trade.get("id", 0) or 0),
                "eval_mode": str(eval_mode),
                "survival_mode": bool(survival_mode),
                "phase": str(getattr(aee_state, "phase", "")),
                "metrics": dict(metrics or {}),
                "aee_true": truths,
                "aee_chosen": chosen,
            },
        )
        rule_trace = dict(getattr(aee_state, "rule_trace", {}) or {})
        self._last_eval_trace = {
            "aee_true_rules": truths,
            "aee_chosen_rule": chosen,
            "eval_mode": str(eval_mode),
            "panic_hits": int(getattr(aee_state, "panic_hits", 0) or 0),
            "trade_age_sec": max(0.0, float(now_ts_val if now_ts_val is not None else self._current_ts) - float(getattr(aee_state, "entry_time", 0.0) or 0.0)),
            "rule_trace": rule_trace,
            "rule_hit_count": {
                "pulse": int((rule_trace.get("pulse") or {}).get("rule_hit_count", 0) or 0),
                "decay": int((rule_trace.get("decay") or {}).get("rule_hit_count", 0) or 0),
                "panic": int((rule_trace.get("panic") or {}).get("rule_hit_count", 0) or 0),
            },
            "rule_persistence": {
                "pulse": float((rule_trace.get("pulse") or {}).get("rule_persistence", 0.0) or 0.0),
                "decay": float((rule_trace.get("decay") or {}).get("rule_persistence", 0.0) or 0.0),
                "panic": float((rule_trace.get("panic") or {}).get("rule_persistence", 0.0) or 0.0),
            },
            "exit_allowed": {
                "pulse": bool((rule_trace.get("pulse") or {}).get("exit_allowed", False)),
                "decay": bool((rule_trace.get("decay") or {}).get("exit_allowed", False)),
                "panic": bool((rule_trace.get("panic") or {}).get("exit_allowed", False)),
            },
            "panic_subreasons": list((rule_trace.get("panic") or {}).get("panic_subreasons", []) or []),
            "panic_exit_metrics": dict((rule_trace.get("panic") or {}).get("panic_exit_metrics", {}) or {}),
        }
        return chosen

    def run_aee_replay(self, trade: Dict[str, Any], speed_class: str = "MED") -> Dict[str, Any]:
        merged: List[Tuple[float, str, Tick]] = []
        for inst in self.instruments:
            for t in self.ticks_by_inst.get(inst, []):
                merged.append((t.ts, inst, t))
        merged.sort(key=lambda item: item[0])

        records: List[Dict[str, Any]] = []

        # 80/20 legs
        core_open = True
        runner_open = True
        core_exit: Optional[Dict[str, Any]] = None
        runner_exit: Optional[Dict[str, Any]] = None
        runner_promoted = False
        overshoot_peak_atr = 0.0

        next_bucket_ts: Optional[float] = None
        tick_mode_active = False
        last_eval_metrics: Dict[str, Any] = {}

        entry_price = float(trade.get("entry", 0.0) or 0.0)
        entry_ts = float(trade.get("ts", 0.0) or 0.0)
        direction = str(trade.get("dir", "LONG")).upper()
        min_hold_core = float(getattr(phone_bot, "CORE_MIN_HOLD_TIME", getattr(phone_bot, "RUNNER_MIN_HOLD_TIME", 300.0)))
        min_hold_runner = float(getattr(phone_bot, "RUNNER_MIN_HOLD_TIME", 300.0))

        for n, (ts, inst, tick) in enumerate(merged, start=1):
            self._current_tick_idx = n
            self._current_ts = float(ts)
            self._current_pair = str(inst)
            self.price_feed.push_tick(inst, tick)

            if inst != trade["pair"]:
                continue

            if next_bucket_ts is None:
                next_bucket_ts = float(ts)

            bucket_due = float(ts) >= float(next_bucket_ts)
            should_eval = bucket_due or tick_mode_active

            rec: Dict[str, Any] = {
                "idx": n,
                "pair_eval_idx": None,
                "ts": ts,
                "instrument": inst,
                "bid": tick.bid,
                "ask": tick.ask,
                "mid": tick.mid,
                "evaluated": False,
                "bucket_due": bool(bucket_due),
                "tick_mode_active_before": bool(tick_mode_active),
                "phase": None,
                "tick_armed": None,
                "tick_due": None,
                "tick_reason": None,
                "exit_reason": None,
                "exit_blocked_min_hold": False,
                "runner_exit_blocked_min_hold": False,
                "metrics": {},
                "core_open": core_open,
                "runner_open": runner_open,
                "runner_promoted": runner_promoted,
                "overshoot_peak_atr": overshoot_peak_atr,
            }

            if should_eval and (core_open or runner_open):
                self._current_pair_eval_idx += 1
                rec["pair_eval_idx"] = self._current_pair_eval_idx
                rec["evaluated"] = True
                self._last_eval_trace = None

                spread_pips = phone_bot.to_pips(trade["pair"], tick.ask - tick.bid)
                aee_eval = phone_bot._aee_eval_for_trade(
                    tr=trade,
                    pair=trade["pair"],
                    direction=trade["dir"],
                    bid=tick.bid,
                    ask=tick.ask,
                    mid=tick.mid,
                    now=ts,
                    spread_pips=spread_pips,
                    speed_class=speed_class,
                )

                metrics = dict(aee_eval.get("metrics", {}) or {})
                last_eval_metrics = metrics

                rec.update(
                    {
                        "phase": aee_eval.get("phase"),
                        "tick_armed": bool(aee_eval.get("tick_armed")),
                        "tick_due": bool(aee_eval.get("tick_due")),
                        "eval_mode": "TICK" if bool(aee_eval.get("tick_armed")) else ("SURVIVAL" if bool(aee_eval.get("survival_mode")) else "BUCKET"),
                        "tick_reason": aee_eval.get("tick_reason"),
                        "exit_reason": aee_eval.get("exit_reason"),
                        "metrics": metrics,
                    }
                )
                if isinstance(self._last_eval_trace, dict):
                    rec["aee_true_rules"] = list(self._last_eval_trace.get("aee_true_rules", []))
                    rec["aee_chosen_rule"] = self._last_eval_trace.get("aee_chosen_rule")
                    rec["panic_confirmation_hits"] = int(self._last_eval_trace.get("panic_hits", 0) or 0)
                    rec["trade_age_sec"] = float(self._last_eval_trace.get("trade_age_sec", 0.0) or 0.0)
                    rec["rule_hit_count"] = dict(self._last_eval_trace.get("rule_hit_count", {}) or {})
                    rec["rule_persistence"] = dict(self._last_eval_trace.get("rule_persistence", {}) or {})
                    rec["exit_allowed"] = dict(self._last_eval_trace.get("exit_allowed", {}) or {})
                    rec["panic_subreasons"] = list(self._last_eval_trace.get("panic_subreasons", []) or [])
                    rec["panic_exit_metrics"] = dict(self._last_eval_trace.get("panic_exit_metrics", {}) or {})
                else:
                    rec["aee_true_rules"] = []
                    rec["aee_chosen_rule"] = rec.get("exit_reason")
                    rec["panic_confirmation_hits"] = 0
                    rec["trade_age_sec"] = max(0.0, float(ts) - float(trade.get("ts", ts) or ts))
                    rec["rule_hit_count"] = {"pulse": 0, "decay": 0, "panic": 0}
                    rec["rule_persistence"] = {"pulse": 0.0, "decay": 0.0, "panic": 0.0}
                    rec["exit_allowed"] = {"pulse": False, "decay": False, "panic": False}
                    rec["panic_subreasons"] = []
                    rec["panic_exit_metrics"] = {}

                tick_mode_active = bool(aee_eval.get("tick_armed"))
                if bucket_due and next_bucket_ts is not None:
                    while next_bucket_ts <= float(ts):
                        next_bucket_ts += self.bucket_sec

                # overshoot memory
                atr_exec = float(metrics.get("atr_exec", 0.0) or 0.0)
                tp_anchor = float(entry_price + 1.2 * atr_exec if direction == "LONG" else entry_price - 1.2 * atr_exec)
                if atr_exec > 0:
                    overshoot = (tick.mid - tp_anchor) if direction == "LONG" else (tp_anchor - tick.mid)
                    overshoot_peak_atr = max(overshoot_peak_atr, max(0.0, overshoot / atr_exec))

                trade_age_sec = max(0.0, float(ts) - entry_ts)

                # core exits on native AEE reason (delay non-panic exits until min hold)
                if core_open and rec["exit_reason"]:
                    is_panic = str(rec["exit_reason"]) == "PANIC_EXIT"
                    if (not is_panic) and trade_age_sec < min_hold_core:
                        rec["exit_blocked_min_hold"] = True
                    else:
                        core_open = False
                        core_exit = {
                            "pair_eval_idx": rec["pair_eval_idx"],
                            "idx": rec["idx"],
                            "ts": ts,
                            "price": tick.bid if direction == "LONG" else tick.ask,
                            "reason": rec["exit_reason"],
                            "metrics": metrics,
                        }
                        if rec.get("exit_reason") == "PULSE_STALL_CAPTURE":
                            core_exit["pulse_exit_time"] = float(ts)
                            core_exit["pulse_energy_state"] = {
                                "cps": float(metrics.get("cps", 0.0) or 0.0),
                                "dhr": float(metrics.get("dhr", 0.0) or 0.0),
                                "ge": float(metrics.get("ge", 0.0) or 0.0),
                                "velocity": float(metrics.get("velocity", 0.0) or 0.0),
                            }
                        self.logs.append(
                            "AEE_DECISION_ORDER_ACTION",
                            {
                                "ts": ts,
                                "pair_eval_idx": rec["pair_eval_idx"],
                                "aee_chosen_rule": rec["exit_reason"],
                                "order_action": "CLOSE_CORE",
                                "instrument": inst,
                            },
                        )

                # runner promotion
                rss = float(metrics.get("rss", 0.0) or 0.0)
                led = float(metrics.get("led", 0.0) or 0.0)
                dhr = float(metrics.get("dhr", 0.0) or 0.0)
                ge = float(metrics.get("ge", 0.0) or 0.0)
                if runner_open and (not runner_promoted):
                    epi = float(metrics.get("epi", 0.0) or 0.0)
                    runner_survival = float(metrics.get("rsp", 0.0) or 0.0)
                    regime = str(metrics.get("regime", "MIXED")).upper()
                    runner_energy_promote = (
                        rss >= float(getattr(phone_bot, "RUNNER_PROMOTE_RSS_MIN", phone_bot.ENERGY_RSS_PROMOTE))
                        and led >= float(getattr(phone_bot, "RUNNER_PROMOTE_LED_MIN", phone_bot.LED_EXPANSION_THRESHOLD))
                        and epi >= float(getattr(phone_bot, "RUNNER_PROMOTE_EPI_MIN", 0.50))
                        and runner_survival >= 0.52
                    )
                    runner_overshoot_promote = overshoot_peak_atr >= 0.45 and rss >= 0.55 and runner_survival >= 0.50
                    if regime != "CHOP" and (runner_energy_promote or runner_overshoot_promote):
                        runner_promoted = True

                # runner exits after promotion
                if runner_open and runner_promoted:
                    # === CHANGE LIST PATCH: runner giveback logic (protocol-compliant) ===
                    giveback_from_peak = max(0.0, overshoot_peak_atr - max(0.0, float(metrics.get("overshoot_peak_atr", overshoot_peak_atr) or overshoot_peak_atr)))
                    epi = float(metrics.get("epi", 0.0) or 0.0)
                    runner_survival = float(metrics.get("rsp", 0.0) or 0.0)
                    # Canonical protocol: convex continuation, let runner breathe, but enforce hard giveback ceiling
                    giveback_thresh = 0.20
                    if overshoot_peak_atr >= 1.20:
                        giveback_thresh = 0.32
                    elif overshoot_peak_atr >= 0.80:
                        giveback_thresh = 0.26
                    if rss >= 0.70 and epi >= 0.55 and led >= 0.55:
                        giveback_thresh += 0.06
                    if runner_survival >= 0.65:
                        giveback_thresh += 0.06
                    elif runner_survival <= 0.40:
                        giveback_thresh = max(0.12, giveback_thresh - 0.04)
                    # Enforce hard ceiling for giveback
                    giveback_thresh = min(giveback_thresh, 0.36)
                    runner_exit_now = False
                    runner_reason = ""
                    if rec["exit_reason"] == "PANIC_EXIT":
                        runner_exit_now = True
                        runner_reason = "PANIC_EXIT"
                    elif dhr >= 0.62 and (ge >= 0.45 or giveback_from_peak >= giveback_thresh):
                        runner_exit_now = True
                        runner_reason = "RUNNER_ENERGY_DECAY"
                    elif (not bool(rec.get("tick_armed"))) and dhr >= 0.55 and ge >= 0.35:
                        runner_exit_now = True
                        runner_reason = "RUNNER_CAPTURE_FADE"

                    if runner_exit_now:
                        if (runner_reason != "PANIC_EXIT") and trade_age_sec < min_hold_runner:
                            rec["runner_exit_blocked_min_hold"] = True
                        else:
                            runner_open = False
                            runner_exit = {
                                "pair_eval_idx": rec["pair_eval_idx"],
                                "idx": rec["idx"],
                                "ts": ts,
                                "price": tick.bid if direction == "LONG" else tick.ask,
                                "reason": runner_reason,
                                "metrics": metrics,
                                "overshoot_peak_atr": overshoot_peak_atr,
                            }
                            self.logs.append(
                                "AEE_DECISION_ORDER_ACTION",
                                {
                                    "ts": ts,
                                    "pair_eval_idx": rec["pair_eval_idx"],
                                    "aee_chosen_rule": runner_reason,
                                    "order_action": "CLOSE_RUNNER",
                                    "instrument": inst,
                                },
                            )

            rec["core_open"] = core_open
            rec["runner_open"] = runner_open
            rec["runner_promoted"] = runner_promoted
            rec["overshoot_peak_atr"] = overshoot_peak_atr
            records.append(rec)

            if not core_open and not runner_open:
                break

        # Enforce min hold by delaying exit evaluation for all non-PANIC exits (core and runner)
        # Find the last tick at or after min_hold for each leg
        def find_exit_tick(after_ts):
            for r in records:
                if r.get("instrument") == trade["pair"] and float(r.get("ts", 0.0) or 0.0) >= after_ts:
                    return r
            return None

        # Core leg
        if core_open:
            # If not PANIC, delay exit until min_hold is reached
            last_tick = find_exit_tick(entry_ts + min_hold_core)
            if last_tick is None:
                # No tick after min_hold, use last available
                last_tick = records[-1]
            core_open = False
            core_exit_ts = float(last_tick.get("ts"))
            core_exit_price = float(last_tick.get("bid") if direction == "LONG" else last_tick.get("ask"))
            core_exit = {
                "pair_eval_idx": last_tick.get("pair_eval_idx"),
                "idx": last_tick.get("idx"),
                "ts": core_exit_ts,
                "price": core_exit_price,
                "reason": "SIM_EOD_CLOSE",
                "metrics": dict(last_tick.get("metrics", {}) or last_eval_metrics),
            }
        # Runner leg
        if runner_open:
            last_tick = find_exit_tick(entry_ts + min_hold_runner)
            if last_tick is None:
                last_tick = records[-1]
            runner_open = False
            runner_exit_ts = float(last_tick.get("ts"))
            runner_exit_price = float(last_tick.get("bid") if direction == "LONG" else last_tick.get("ask"))
            runner_exit = {
                "pair_eval_idx": last_tick.get("pair_eval_idx"),
                "idx": last_tick.get("idx"),
                "ts": runner_exit_ts,
                "price": runner_exit_price,
                "reason": "SIM_EOD_CLOSE",
                "metrics": dict(last_tick.get("metrics", {}) or last_eval_metrics),
                "overshoot_peak_atr": overshoot_peak_atr,
            }

        def pnl_pips(exit_px: float) -> float:
            pip = float(phone_bot.pip_size(trade["pair"]))
            if pip <= 0:
                return 0.0
            if direction == "LONG":
                return (exit_px - entry_price) / pip
            return (entry_price - exit_px) / pip

        core_pips = pnl_pips(float(core_exit.get("price", entry_price) if core_exit else entry_price))
        runner_pips = pnl_pips(float(runner_exit.get("price", entry_price) if runner_exit else entry_price))
        total_weighted_pips = 0.8 * core_pips + 0.2 * runner_pips

        def leg_stats(leg_exit: Optional[Dict[str, Any]]) -> Dict[str, Any]:
            if not leg_exit:
                return {
                    "hold_sec": 0.0,
                    "mfe_pips": 0.0,
                    "mae_pips": 0.0,
                    "capture": None,
                    "left_on_table_pips": 0.0,
                }
            leg_ts = float(leg_exit.get("ts", 0.0) or 0.0)
            active_rows = [r for r in records if float(r.get("ts", 0.0) or 0.0) <= leg_ts and r.get("instrument") == trade["pair"]]
            mids = [float(r.get("mid", entry_price) or entry_price) for r in active_rows]
            pip = float(phone_bot.pip_size(trade["pair"]))
            hold_sec = max(0.0, leg_ts - float(trade.get("ts", leg_ts) or leg_ts))
            if not mids or pip <= 0:
                return {
                    "hold_sec": hold_sec,
                    "mfe_pips": 0.0,
                    "mae_pips": 0.0,
                    "capture": None,
                    "left_on_table_pips": 0.0,
                }
            if direction == "LONG":
                mfe = (max(mids) - entry_price) / pip
                mae = (min(mids) - entry_price) / pip
                realized = (float(leg_exit.get("price", entry_price) or entry_price) - entry_price) / pip
            else:
                mfe = (entry_price - min(mids)) / pip
                mae = (entry_price - max(mids)) / pip
                realized = (entry_price - float(leg_exit.get("price", entry_price) or entry_price)) / pip
            capture = (realized / mfe) if mfe > 0 else None
            return {
                "hold_sec": hold_sec,
                "mfe_pips": mfe,
                "mae_pips": mae,
                "capture": capture,
                "left_on_table_pips": (mfe - realized),
            }

        core_stats = leg_stats(core_exit)
        runner_stats = leg_stats(runner_exit)

        pair_records = [r for r in records if r.get("instrument") == trade["pair"] and isinstance(r.get("ts"), (int, float))]
        if pair_records:
            replay_wall_time_sec = max(0.0, float(pair_records[-1]["ts"]) - float(pair_records[0]["ts"]))
        else:
            replay_wall_time_sec = 0.0

        def panic_recovery(exit_info: Optional[Dict[str, Any]]) -> Dict[str, Optional[float]]:
            if not exit_info:
                return {"plus_30": None, "plus_60": None, "plus_120": None}
            if str(exit_info.get("reason", "")) != "PANIC_EXIT":
                return {"plus_30": None, "plus_60": None, "plus_120": None}
            exit_ts = float(exit_info.get("ts", 0.0) or 0.0)
            exit_px = float(exit_info.get("price", entry_price) or entry_price)
            pip = float(phone_bot.pip_size(trade["pair"]))
            if pip <= 0:
                return {"plus_30": None, "plus_60": None, "plus_120": None}
            out: Dict[str, Optional[float]] = {}
            for h in (30, 60, 120):
                horizon = exit_ts + float(h)
                window = [r for r in records if float(r.get("ts", 0.0) or 0.0) > exit_ts and float(r.get("ts", 0.0) or 0.0) <= horizon]
                if not window:
                    out[f"plus_{h}"] = None
                    continue
                mids = [float(r.get("mid", exit_px) or exit_px) for r in window]
                if direction == "LONG":
                    best = max(mids)
                    out[f"plus_{h}"] = (best - exit_px) / pip
                else:
                    best = min(mids)
                    out[f"plus_{h}"] = (exit_px - best) / pip
            return out

        core_panic_recovery = panic_recovery(core_exit)
        runner_panic_recovery = panic_recovery(runner_exit)

        exit_record = dict(core_exit or {})
        if exit_record:
            exit_record["exit_reason"] = str(exit_record.get("reason", "UNKNOWN"))
            exit_record["core_pips"] = core_pips
            exit_record["runner_pips"] = runner_pips
            exit_record["total_weighted_pips"] = total_weighted_pips

        return {
            "trade": trade,
            "live_parity_check": {
                "shared_aee_function": "phone_bot._aee_eval_for_trade",
                "shared_exit_function": "phone_bot.check_aee_exits",
                "trade_shape_keys": sorted(list(trade.keys())),
                "decision_to_action_trace_event": "AEE_DECISION_ORDER_ACTION",
            },
            "n_ticks": len(records),
            "bucket_sec": self.bucket_sec,
            "tick_records": records,
            "exit": exit_record,
            "legs": {
                "core": {
                    "weight": 0.8,
                    "entry_ts": entry_ts,
                    "entry_price": entry_price,
                    "exit": core_exit,
                    "exit_ts": float(core_exit.get("ts", 0.0) or 0.0) if core_exit else 0.0,
                    "pips": core_pips,
                    "hold_sec": core_stats["hold_sec"],
                    "mfe_pips": core_stats["mfe_pips"],
                    "mae_pips": core_stats["mae_pips"],
                    "capture": core_stats["capture"],
                    "left_on_table_pips": core_stats["left_on_table_pips"],
                    "panic_recovery_pips": core_panic_recovery,
                },
                "runner": {
                    "weight": 0.2,
                    "entry_ts": entry_ts,
                    "entry_price": entry_price,
                    "exit": runner_exit,
                    "exit_ts": float(runner_exit.get("ts", 0.0) or 0.0) if runner_exit else 0.0,
                    "pips": runner_pips,
                    "hold_sec": runner_stats["hold_sec"],
                    "mfe_pips": runner_stats["mfe_pips"],
                    "mae_pips": runner_stats["mae_pips"],
                    "capture": runner_stats["capture"],
                    "left_on_table_pips": runner_stats["left_on_table_pips"],
                    "panic_recovery_pips": runner_panic_recovery,
                    "promoted": runner_promoted,
                    "overshoot_peak_atr": overshoot_peak_atr,
                    "runner_capture_rate": runner_stats["capture"],
                    "runner_average_overshoot": overshoot_peak_atr,
                    "runner_tail_contributions": max(0.0, runner_pips),
                },
            },
            "weighted_pips": total_weighted_pips,
            "pips_per_hour": {
                "weighted": (total_weighted_pips / (replay_wall_time_sec / 3600.0)) if replay_wall_time_sec > 0 else 0.0,
                "core": (core_pips / (replay_wall_time_sec / 3600.0)) if replay_wall_time_sec > 0 else 0.0,
                "runner": (runner_pips / (replay_wall_time_sec / 3600.0)) if replay_wall_time_sec > 0 else 0.0,
            },
            "replay_wall_time_sec": replay_wall_time_sec,
            "log_summary": self.logs.summary(),
            "logs": self.logs.events,
        }


def compute_aee_truths(
    *,
    metrics: Dict[str, Any],
    aee_state: Any,
    current_price: float,
    eval_mode: str,
    now_ts_val: float,
    runner_ctx: Optional[dict],
) -> List[str]:
    progress = float(metrics.get("progress", 0.0) or 0.0)
    speed = float(metrics.get("speed", 0.0) or 0.0)
    velocity = float(metrics.get("velocity", 0.0) or 0.0)
    pullback = float(metrics.get("pullback", 0.0) or 0.0)
    pullback_rate = float(metrics.get("pullback_rate", 0.0) or 0.0)
    allowed_giveback_atr = float(
        metrics.get("allowed_giveback_atr", phone_bot.DECAY_LOCK_GIVEBACK_MIN_ATR) or phone_bot.DECAY_LOCK_GIVEBACK_MIN_ATR
    )
    leg_mult = float(metrics.get("leg_mult", 1.0) or 1.0)
    dist_to_tp = float(metrics.get("dist_to_tp", 9e9) or 9e9)
    near_tp_band = float(metrics.get("near_tp_band", 0.25) or 0.25)
    atr_exec = float(metrics.get("atr_exec", 0.0) or 0.0)
    cps = float(metrics.get("cps", 0.5) or 0.5)
    dhr = float(metrics.get("dhr", 0.0) or 0.0)
    no_new_high_sec = float(metrics.get("no_new_high_sec", 0.0) or 0.0)
    regime = str(metrics.get("regime", "MIXED")).upper()
    chop_mode = regime == "CHOP"
    trend_mode = regime == "TREND"

    truths: List[str] = []

    if eval_mode == "SURVIVAL" and (velocity <= -0.4 or pullback >= 0.35):
        truths.append("PANIC_EXIT")

    spread_pips = float(metrics.get("spread_pips", 0.0) or 0.0)
    med_spread = max(0.1, float(metrics.get("med_spread", spread_pips) or spread_pips or 0.1))
    spread_ratio = spread_pips / med_spread if med_spread > 0 else 0.0
    sig_velocity = velocity <= float(phone_bot.PANIC_VELOCITY)
    sig_pullback = pullback >= max(float(phone_bot.PANIC_PULLBACK), allowed_giveback_atr * leg_mult)
    sig_spread = spread_ratio >= float(phone_bot.PANIC_SPREAD_SHOCK_RATIO)
    panic_conditions = int(sig_velocity) + int(sig_pullback) + int(sig_spread)
    hazard_gate = dhr >= float(phone_bot.ENERGY_DHR_PANIC) and cps <= float(phone_bot.ENERGY_CPS_PANIC_MAX)
    catastrophic_spread = spread_ratio >= float(phone_bot.PANIC_SPREAD_CATA_RATIO)
    if (panic_conditions >= 2 and hazard_gate) or catastrophic_spread:
        truths.append("PANIC_EXIT")

    in_near_tp = dist_to_tp <= near_tp_band
    if in_near_tp:
        cond_pullback = pullback >= float(phone_bot.STALL_PULLBACK_ATR)
        cond_speed = speed < float(phone_bot.STALL_SPEED)
        cond_vel = velocity < 0
        if cond_pullback or (cond_speed and cond_vel):
            truths.append("NEAR_TP_STALL_CAPTURE")

    pulse_line = getattr(aee_state, "pulse_exit_line", None)
    pulse_cross_ts = float(getattr(aee_state, "pulse_cross_ts", 0.0) or 0.0)
    pulse_crossed = False
    if pulse_line is None and atr_exec > 0.0 and progress >= phone_bot.PULSE_PROGRESS:
        direction = str(getattr(aee_state, "direction", "LONG")).upper()
        if direction == "LONG":
            pulse_line = float(getattr(aee_state, "local_high", current_price)) - (phone_bot.PULSE_EXITLINE_ATR * atr_exec)
        else:
            pulse_line = float(getattr(aee_state, "local_low", current_price)) + (phone_bot.PULSE_EXITLINE_ATR * atr_exec)
    if pulse_line is not None:
        direction = str(getattr(aee_state, "direction", "LONG")).upper()
        if direction == "LONG" and current_price <= float(pulse_line):
            pulse_crossed = True
        if direction == "SHORT" and current_price >= float(pulse_line):
            pulse_crossed = True

    pulse_cross_age = max(0.0, now_ts_val - pulse_cross_ts) if (pulse_crossed and pulse_cross_ts > 0.0) else 0.0
    pulse_energy_weak = bool(
        cps <= float(phone_bot.ENERGY_CPS_THRESHOLD)
        and dhr >= float(phone_bot.ENERGY_DHR_DECAY_THRESHOLD)
        and float(metrics.get("ge", 0.0) or 0.0) >= float(max(0.18, phone_bot.ENERGY_GE_DECAY_THRESHOLD * 0.80))
    )
    pulse_recovered = bool(
        pulse_crossed
        and pulse_cross_age <= float(phone_bot.PULSE_RECLAIM_WINDOW_SEC)
        and (
            velocity >= 0.0
            or cps >= float(phone_bot.PULSE_RECOVER_CPS_MIN)
            or dhr <= float(phone_bot.PULSE_RECOVER_DHR_MAX)
        )
    )
    crest_confirmed = bool(
        no_new_high_sec >= float(getattr(phone_bot, "PULSE_CREST_NOEXT_SEC", 3.0))
        and pullback >= float(getattr(phone_bot, "PULSE_CREST_PULLBACK_ATR", 0.18))
    )
    reversal_confirmed = bool(velocity <= float(getattr(phone_bot, "PULSE_REV_VELOCITY", -0.05)) and dhr >= float(phone_bot.ENERGY_DHR_DECAY_THRESHOLD))
    chop_pulse_gate = True
    if chop_mode:
        chop_pulse_gate = bool(
            cps <= float(getattr(phone_bot, "CHOP_PULSE_CPS_MAX", 0.46))
            and dhr >= float(getattr(phone_bot, "CHOP_PULSE_DHR_MIN", 0.65))
            and pullback >= float(getattr(phone_bot, "CHOP_PULSE_PULLBACK_MIN", 0.30))
            and crest_confirmed
            and reversal_confirmed
        )
    trend_pulse_gate = True
    if trend_mode:
        trend_pulse_gate = bool(crest_confirmed and reversal_confirmed)
    if pulse_crossed and pulse_cross_age >= float(phone_bot.PULSE_RECLAIM_WINDOW_SEC) and pulse_energy_weak and (not pulse_recovered) and crest_confirmed and reversal_confirmed and chop_pulse_gate and trend_pulse_gate:
        truths.append("PULSE_STALL_CAPTURE")

    if chop_mode:
        rss = float(metrics.get("rss", 0.0) or 0.0)
        chop_scratch = bool(
            progress <= float(getattr(phone_bot, "CHOP_SCRATCH_PROGRESS_MAX", 0.55))
            and rss <= float(getattr(phone_bot, "CHOP_SCRATCH_RSS_MAX", 0.38))
            and cps <= float(getattr(phone_bot, "CHOP_SCRATCH_CPS_MAX", 0.46))
            and dhr >= float(getattr(phone_bot, "CHOP_SCRATCH_DHR_MIN", 0.62))
            and pullback >= float(getattr(phone_bot, "CHOP_SCRATCH_PULLBACK_MIN", 0.12))
            and no_new_high_sec >= 6.0
        )
        if chop_scratch:
            truths.append("CHOP_SCRATCH_EXIT")

    trade_age = max(0.0, now_ts_val - float(getattr(aee_state, "entry_time", now_ts_val) or now_ts_val))
    epi = float(metrics.get("epi", 0.0) or 0.0)
    ge = float(metrics.get("ge", 0.0) or 0.0)
    if (
        int(getattr(aee_state, "eval_samples", 0) or 0) >= int(phone_bot.MIN_EVAL_SAMPLES)
        and trade_age >= float(phone_bot.MIN_EVAL_AGE_SEC)
        and progress >= 0.45
        and speed < 0.70
        and velocity < 0.0
        and (pullback_rate >= phone_bot.PANIC_PULLBACKRATE or pullback >= allowed_giveback_atr)
        and cps < float(phone_bot.ENERGY_CPS_THRESHOLD)
        and epi <= float(phone_bot.ENERGY_EPI_DECAY_THRESHOLD)
        and dhr >= float(phone_bot.ENERGY_DHR_DECAY_THRESHOLD)
        and ge >= float(phone_bot.ENERGY_GE_DECAY_THRESHOLD)
        and no_new_high_sec >= float(phone_bot.DECAY_NO_NEW_HIGH_SEC)
    ):
        truths.append("FAILED_TO_CONTINUE_DECAY")

    order = ["PANIC_EXIT", "NEAR_TP_STALL_CAPTURE", "PULSE_STALL_CAPTURE", "CHOP_SCRATCH_EXIT", "FAILED_TO_CONTINUE_DECAY"]
    seen = set()
    return [x for x in order if x in truths and (x not in seen and not seen.add(x))]


def build_candles(ticks: List[Tick], bucket_seconds: int) -> List[Dict[str, Any]]:
    if not ticks:
        return []
    out: List[Dict[str, Any]] = []
    bucket: List[Tick] = []
    current_bucket: Optional[int] = None

    for t in sorted(ticks, key=lambda x: x.ts):
        bucket_id = int(t.ts // bucket_seconds)
        if current_bucket is None:
            current_bucket = bucket_id
        if bucket_id != current_bucket and bucket:
            out.append(_ticks_to_candle(bucket))
            bucket = []
            current_bucket = bucket_id
        bucket.append(t)

    if bucket:
        out.append(_ticks_to_candle(bucket))

    return out


def _ticks_to_candle(bucket: List[Tick]) -> Dict[str, Any]:
    mids = [t.mid for t in bucket]
    return {
        "time": bucket[-1].ts,
        "mid": {"o": str(mids[0]), "h": str(max(mids)), "l": str(min(mids)), "c": str(mids[-1])},
        "volume": len(bucket),
        "complete": True,
        "o": mids[0],
        "h": max(mids),
        "l": min(mids),
        "c": mids[-1],
    }


def parse_ts(raw: str) -> float:
    s = str(raw).strip()
    try:
        return float(s)
    except ValueError:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).timestamp()


def load_ticks_csv(path: str) -> Dict[str, List[Tick]]:
    out: Dict[str, List[Tick]] = defaultdict(list)
    with open(path, "r", newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        required = {"instrument", "ts", "bid", "ask"}
        missing = required - set(r.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing required columns: {sorted(missing)}")
        for row in r:
            tick = Tick(
                instrument=str(row["instrument"]).strip(),
                ts=parse_ts(str(row["ts"])),
                bid=float(row["bid"]),
                ask=float(row["ask"]),
            )
            out[tick.instrument].append(tick)

    for inst in out:
        out[inst].sort(key=lambda x: x.ts)
    return dict(out)


def create_default_trade(pair: str, direction: str, ticks_by_inst: Dict[str, List[Tick]], atr_pips: float, tp_atr: float) -> Dict[str, Any]:
    ticks = ticks_by_inst.get(pair, [])
    if not ticks:
        raise ValueError(f"No ticks found for pair {pair}")

    first = ticks[0]
    last = ticks[-1]
    
    # Detect actual price direction
    first_mid = (first.bid + first.ask) / 2
    last_mid = (last.bid + last.ask) / 2
    
    if last_mid > first_mid:
        actual_direction = "LONG"
    elif last_mid < first_mid:
        actual_direction = "SHORT"
    else:
        actual_direction = direction  # Use provided direction if no movement
    
    atr_entry = float(phone_bot.pip_size(pair)) * float(atr_pips)
    entry = first.ask if actual_direction == "LONG" else first.bid
    tp = entry + (tp_atr * atr_entry if actual_direction == "LONG" else -tp_atr * atr_entry)

    return {
        "id": 1,
        "ts": first.ts,
        "pair": pair,
        "setup": "SIM_RUN",
        "dir": actual_direction,  # Use detected direction
        "mode": "SIM",
        "units": 1000,
        "entry": entry,
        "atr_entry": atr_entry,
        "ttl_sec": 3600,
        "pg_t": 0,
        "pg_atr": 0.0,
        "tp": tp,
    }


def main() -> int:
    p = argparse.ArgumentParser(description="Replay tick CSV through phone_bot AEE with validation telemetry")
    p.add_argument("--ticks", required=True, help="CSV with columns instrument,ts,bid,ask")
    p.add_argument("--pair", required=True, help="Instrument to evaluate")
    p.add_argument("--direction", default="LONG", choices=["LONG", "SHORT"])
    p.add_argument("--atr-pips", type=float, default=10.0)
    p.add_argument("--tp-atr", type=float, default=1.2)
    p.add_argument("--speed-class", default="MED", choices=["FAST", "MED", "SLOW"])
    p.add_argument("--bucket-sec", type=float, default=5.0, help="Base management bucket cadence in seconds")
    p.add_argument("--out", default="sim_results.json")
    args = p.parse_args()

    ticks_by_inst = load_ticks_csv(args.ticks)
    trade = create_default_trade(args.pair, args.direction, ticks_by_inst, args.atr_pips, args.tp_atr)
    env = SimEnvironment(instruments=list(ticks_by_inst.keys()), ticks_by_inst=ticks_by_inst, bucket_sec=args.bucket_sec)
    try:
        results = env.run_aee_replay(trade=trade, speed_class=args.speed_class)
    finally:
        env.restore_live_wiring()

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(
        "SIM_SUMMARY",
        {
            "n_ticks": results["n_ticks"],
            "bucket_sec": args.bucket_sec,
            "exit": results["exit"],
            "weighted_pips": results.get("weighted_pips", 0.0),
            "out": args.out,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
