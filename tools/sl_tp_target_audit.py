#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import os
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import phone_bot
import sim_harness


class Tick:
    __slots__ = ("ts", "bid", "ask")
    def __init__(self, ts: float, bid: float, ask: float):
        self.ts = ts
        self.bid = bid
        self.ask = ask

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0


def parse_ts(raw: str) -> float:
    try:
        return float(str(raw).strip())
    except Exception:
        return 0.0


def load_ticks(path: str) -> tuple[str, list[Tick]]:
    pair = "EUR_USD"
    out: list[Tick] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            pair = str(row.get("instrument") or pair)
            out.append(Tick(parse_ts(row.get("ts") or "0"), float(row.get("bid") or 0.0), float(row.get("ask") or 0.0)))
    out.sort(key=lambda t: t.ts)
    return pair, out


def build_candles(ticks: list[Tick], bucket_sec: int = 300) -> list[dict[str, Any]]:
    if not ticks:
        return []
    out: list[dict[str, Any]] = []
    b0 = int(ticks[0].ts // bucket_sec) * bucket_sec
    o = h = l = c = ticks[0].mid
    for t in ticks:
        b = int(t.ts // bucket_sec) * bucket_sec
        if b != b0:
            out.append({"time": float(b0), "o": o, "h": h, "l": l, "c": c, "complete": True})
            b0 = b
            o = h = l = c = t.mid
        else:
            px = t.mid
            h = max(h, px)
            l = min(l, px)
            c = px
    out.append({"time": float(b0), "o": o, "h": h, "l": l, "c": c, "complete": True})
    return out


def first_tick_after(ticks: list[Tick], ts: float) -> Tick | None:
    for t in ticks:
        if t.ts > ts:
            return t
    return None


def speed_from_primitives(m_norm: float, atr_price: float, pair: str) -> str:
    pip = float(phone_bot.pip_size(pair))
    atr_pips = (atr_price / pip) if pip > 0 else 0.0
    vr = abs(m_norm)
    if vr >= 1.5 and atr_pips >= 0.6:
        return "FAST"
    if vr >= 0.6 and atr_pips >= 0.5:
        return "MED"
    return "SLOW"


def run_trade(pair: str, ticks: list[Tick], entry_tick: Tick, direction: str, speed: str, atr_entry: float) -> tuple[dict[str, Any], dict[str, Any]]:
    sp = phone_bot.get_speed_params(speed)
    if direction == "SHORT" and bool(getattr(phone_bot, "DISABLE_SHORT_SLOW", False)) and speed == "SLOW":
        raise RuntimeError("short_slow_disabled")
    tp1_atr = float(phone_bot.get_directional_tp1_atr(speed, direction, float(sp.get("tp1_atr", 1.0) or 1.0)))
    tp2_atr = float(sp.get("tp2_atr", 2.0) or 2.0)
    sl_atr = float(sp.get("sl_atr", 1.0) or 1.0)

    entry = entry_tick.ask if direction == "LONG" else entry_tick.bid
    tp1 = entry + (tp1_atr * atr_entry if direction == "LONG" else -tp1_atr * atr_entry)
    tp2 = entry + (tp2_atr * atr_entry if direction == "LONG" else -tp2_atr * atr_entry)
    sl = entry - (sl_atr * atr_entry if direction == "LONG" else -sl_atr * atr_entry)

    trade = {
        "id": 1,
        "ts": entry_tick.ts,
        "pair": pair,
        "setup": "ENTRY_DRIVEN_AUDIT",
        "dir": direction,
        "mode": "SIM",
        "units": 1000,
        "entry": entry,
        "atr_entry": atr_entry,
        "ttl_sec": int(sp.get("ttl_main", 3600) or 3600),
        "pg_t": 0,
        "pg_atr": float(sp.get("pg_atr", 0.0) or 0.0),
        "tp": tp1,
        "sl": sl,
    }

    ticks_fwd = [t for t in ticks if t.ts >= entry_tick.ts]
    env = sim_harness.SimEnvironment(instruments=[pair], ticks_by_inst={pair: ticks_fwd}, bucket_sec=5.0)
    try:
        rep = env.run_aee_replay(trade=trade, speed_class=speed)
    finally:
        env.restore_live_wiring()

    return rep, {
        "tp1_atr": tp1_atr,
        "tp2_atr": tp2_atr,
        "sl_atr": sl_atr,
        "tp1_price": tp1,
        "tp2_price": tp2,
        "sl_price": sl,
        "entry_price": entry,
    }


def pips(pair: str, px_delta: float) -> float:
    pip_sz = float(phone_bot.pip_size(pair))
    return (px_delta / pip_sz) if pip_sz > 0 else 0.0


def failure_mode(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return "neither"
    n = len(rows)
    sl_tight = sum(1 for r in rows if r["mae_exceeded_sl_immediate"]) / n
    tp_far = sum(1 for r in rows if r["mfe_never_close_tp1"]) / n
    if sl_tight >= 0.45 and tp_far >= 0.45:
        return "both"
    if sl_tight >= 0.45:
        return "SL too tight"
    if tp_far >= 0.45:
        return "TP too far"
    return "neither"


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "trade_count": 0,
            "avg_sl_pips": 0.0,
            "avg_tp1_pips": 0.0,
            "avg_tp2_pips": 0.0,
            "avg_mfe_pips": 0.0,
            "avg_mae_pips": 0.0,
            "tp1_reachable_pct": 0.0,
            "tp2_reachable_pct": 0.0,
            "sl_hit_before_meaningful_expansion_pct": 0.0,
            "mfe_exceeded_tp1_but_failed_pct": 0.0,
            "mfe_never_close_tp1_pct": 0.0,
            "mae_exceeded_sl_immediate_pct": 0.0,
            "core_exit_reason_counts": {},
            "dominant_target_failure_mode": "neither",
        }
    n = len(rows)
    c = Counter(r["core_exit_reason"] for r in rows)
    return {
        "trade_count": n,
        "avg_sl_pips": sum(r["risk_pips"] for r in rows) / n,
        "avg_tp1_pips": sum(r["reward_tp1_pips"] for r in rows) / n,
        "avg_tp2_pips": sum(r["reward_tp2_pips"] for r in rows) / n,
        "avg_mfe_pips": sum(r["mfe_pips"] for r in rows) / n,
        "avg_mae_pips": sum(r["mae_pips"] for r in rows) / n,
        "tp1_reachable_pct": sum(1 for r in rows if r["tp1_reachable"]) / n,
        "tp2_reachable_pct": sum(1 for r in rows if r["tp2_reachable"]) / n,
        "sl_hit_before_meaningful_expansion_pct": sum(1 for r in rows if r["sl_hit_before_meaningful_expansion"]) / n,
        "mfe_exceeded_tp1_but_failed_pct": sum(1 for r in rows if r["mfe_exceeded_tp1_but_failed"]) / n,
        "mfe_never_close_tp1_pct": sum(1 for r in rows if r["mfe_never_close_tp1"]) / n,
        "mae_exceeded_sl_immediate_pct": sum(1 for r in rows if r["mae_exceeded_sl_immediate"]) / n,
        "core_exit_reason_counts": dict(c),
        "dominant_target_failure_mode": failure_mode(rows),
    }


def load_live_vs_harness_geometry_mismatch() -> dict[str, Any]:
    live = {
        sc: {
            "tp1_atr": float(v.get("tp1_atr", 0.0) or 0.0),
            "tp2_atr": float(v.get("tp2_atr", 0.0) or 0.0),
            "sl_atr": float(v.get("sl_atr", 0.0) or 0.0),
        }
        for sc, v in dict(phone_bot.SPEED_CLASS_PARAMS).items()
        if sc in ("FAST", "MED", "SLOW")
    }
    harness_defaults = {
        "create_default_trade": {"atr_pips_default": 10.0, "tp_atr_default": 1.2},
        "note": "Forced-direction harness defaults differ from live speed geometry when used directly.",
    }
    return {
        "live_speed_geometry": live,
        "harness_default_geometry": harness_defaults,
        "mismatch_flag": True,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="SL/TP target construction and feasibility audit")
    ap.add_argument("--glob", default="scenarios/golden/v1.0/*.csv")
    ap.add_argument("--bucket-sec", type=int, default=300)
    ap.add_argument("--cooldown-sec", type=int, default=300)
    ap.add_argument("--schedule-32", action="store_true")
    ap.add_argument("--short-confirm-disp-atr", type=float, default=None, help="SHORT-only displacement gate in ATR units for this audit")
    ap.add_argument("--out", default="reports/sl_tp_target_audit.json")
    args = ap.parse_args()

    paths = [p for p in sorted(glob.glob(args.glob)) if p.endswith(".csv")]
    if args.schedule_32 and len(paths) >= 12:
        paths = list(paths[:12]) + list(paths[:8])

    records: list[dict[str, Any]] = []
    by_direction: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_speed: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_scenario: dict[str, list[dict[str, Any]]] = defaultdict(list)
    opp_count = Counter()

    for spath in paths:
        pair, ticks = load_ticks(spath)
        candles = build_candles(ticks, bucket_sec=max(60, args.bucket_sec))
        n = max(30, int(getattr(phone_bot, "MOM_N", 5) + 5))
        last_entry_ts = -1e18

        for i in range(n, len(candles) + 1):
            cwin = candles[:i]
            atr = float(phone_bot.compute_atr_price(cwin, int(getattr(phone_bot, "ATR_N", 14))) or 0.0)
            mom = float(phone_bot.momentum(cwin, int(getattr(phone_bot, "MOM_N", 5))) or 0.0)
            wr = float(phone_bot.williams_r(cwin, int(getattr(phone_bot, "WR_N", 14))) or float("nan"))
            if not (math.isfinite(atr) and atr > 0 and math.isfinite(mom)):
                continue

            st = phone_bot.PairState()
            st.m_norm = mom / atr
            st.wr = wr if math.isfinite(wr) else -50.0
            direction, _src = phone_bot._watch_primitive_contract_direction(st)
            if direction not in ("LONG", "SHORT"):
                continue
            if direction == "SHORT":
                thr = float(args.short_confirm_disp_atr) if args.short_confirm_disp_atr is not None else float(getattr(phone_bot, "ENTRY_CONFIRM_DISP_ATR", 0.2) or 0.2)
                if len(cwin) < 2:
                    continue
                prev_c = float(cwin[-2]["c"])
                cur_c = float(cwin[-1]["c"])
                short_disp_atr = max(0.0, (prev_c - cur_c) / atr) if atr > 0 else 0.0
                if short_disp_atr < thr:
                    continue
            opp_count[direction] += 1

            cts = float(cwin[-1]["time"])
            if cts - last_entry_ts < float(args.cooldown_sec):
                continue

            entry_tick = first_tick_after(ticks, cts)
            if entry_tick is None:
                continue

            speed = speed_from_primitives(float(st.m_norm), atr, pair)
            try:
                rep, geom = run_trade(pair, ticks, entry_tick, direction, speed, atr)
            except Exception:
                continue

            core = ((rep.get("legs") or {}).get("core") or {})
            core_exit = (core.get("exit") or {})
            core_exit_ts = float(core_exit.get("ts") or entry_tick.ts)
            core_exit_reason = str(core_exit.get("reason") or "NONE")

            # Future path window until core exit
            window = [t for t in ticks if t.ts >= entry_tick.ts and t.ts <= core_exit_ts]
            if not window:
                window = [entry_tick]

            entry_px = float(geom["entry_price"])
            if direction == "LONG":
                mids = [t.mid for t in window]
                mfe = pips(pair, max(mids) - entry_px)
                mae = pips(pair, entry_px - min(mids))
            else:
                mids = [t.mid for t in window]
                mfe = pips(pair, entry_px - min(mids))
                mae = pips(pair, max(mids) - entry_px)

            spread_pips = pips(pair, entry_tick.ask - entry_tick.bid)
            risk_pips = abs(pips(pair, entry_px - float(geom["sl_price"])))
            rew1 = abs(pips(pair, float(geom["tp1_price"]) - entry_px))
            rew2 = abs(pips(pair, float(geom["tp2_price"]) - entry_px))
            r1 = (rew1 / risk_pips) if risk_pips > 0 else None
            r2 = (rew2 / risk_pips) if risk_pips > 0 else None

            tp1_reachable = mfe >= rew1
            tp2_reachable = mfe >= rew2
            sl_hit_before_meaningful = (core_exit_reason == "SL_HIT" and mfe < 0.5 * rew1)
            mfe_tp1_failed = (mfe >= rew1 and core_exit_reason not in ("TP_HIT",))
            mfe_never_close = mfe < 0.5 * rew1
            mae_immediate = (mae >= risk_pips and float(core.get("hold_sec") or 0.0) <= 60.0)

            row = {
                "scenario": Path(spath).name,
                "direction": direction,
                "speed_bucket": speed,
                "entry_price": entry_px,
                "entry_timestamp": entry_tick.ts,
                "atr_at_entry": atr,
                "spread_at_entry_pips": spread_pips,
                "tp1_atr_used": geom["tp1_atr"],
                "tp2_atr_used": geom["tp2_atr"],
                "sl_atr_used": geom["sl_atr"],
                "tp1_price": geom["tp1_price"],
                "tp2_price": geom["tp2_price"],
                "sl_price": geom["sl_price"],
                "risk_pips": risk_pips,
                "reward_tp1_pips": rew1,
                "reward_tp2_pips": rew2,
                "initial_r_multiple_tp1": r1,
                "initial_r_multiple_tp2": r2,
                "mfe_pips": mfe,
                "mae_pips": mae,
                "tp1_reachable": bool(tp1_reachable),
                "tp2_reachable": bool(tp2_reachable),
                "sl_structurally_tight": bool(mae_immediate),
                "tp_structurally_ambitious": bool(mfe_never_close),
                "sl_hit_before_meaningful_expansion": bool(sl_hit_before_meaningful),
                "mfe_exceeded_tp1_but_failed": bool(mfe_tp1_failed),
                "mfe_never_close_tp1": bool(mfe_never_close),
                "mae_exceeded_sl_immediate": bool(mae_immediate),
                "core_exit_reason": core_exit_reason,
                "core_hold_sec": float(core.get("hold_sec") or 0.0),
            }

            records.append(row)
            by_direction[direction].append(row)
            by_speed[speed].append(row)
            by_scenario[Path(spath).name].append(row)
            last_entry_ts = cts

    out = {
        "dataset_glob": args.glob,
        "schedule_32": bool(args.schedule_32),
        "scenario_count": len(paths),
        "directional_opportunity_count": dict(opp_count),
        "records": records,
        "aggregate": {
            "by_direction": {k: summarize(v) for k, v in by_direction.items()},
            "by_speed": {k: summarize(v) for k, v in by_speed.items()},
            "by_direction_and_speed": {
                d: {s: summarize([r for r in rows if r["speed_bucket"] == s]) for s in ("FAST", "MED", "SLOW")}
                for d, rows in by_direction.items()
            },
            "by_scenario": {k: summarize(v) for k, v in by_scenario.items()},
        },
        "short_underperformance_diagnosis": None,
        "live_vs_harness_geometry": load_live_vs_harness_geometry_mismatch(),
    }

    short_rows = by_direction.get("SHORT", [])
    out["short_underperformance_diagnosis"] = failure_mode(short_rows) if short_rows else "opportunity scarcity"

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps({"out": args.out, "trade_records": len(records)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
