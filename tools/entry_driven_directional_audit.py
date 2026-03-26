#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import os
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import phone_bot
import sim_harness


@dataclass
class Tick:
    ts: float
    bid: float
    ask: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0


def parse_ts(raw: str) -> float:
    s = str(raw).strip()
    try:
        return float(s)
    except Exception:
        return 0.0


def load_ticks(path: str) -> tuple[str, list[Tick]]:
    pair = "EUR_USD"
    out: list[Tick] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            pair = str(row.get("instrument") or pair)
            out.append(Tick(ts=parse_ts(row.get("ts") or "0"), bid=float(row.get("bid") or 0.0), ask=float(row.get("ask") or 0.0)))
    out.sort(key=lambda t: t.ts)
    return pair, out


def build_candles(ticks: list[Tick], bucket_sec: int = 300) -> list[dict[str, Any]]:
    if not ticks:
        return []
    candles: list[dict[str, Any]] = []
    b0 = int(ticks[0].ts // bucket_sec) * bucket_sec
    o = h = l = c = ticks[0].mid
    for t in ticks:
        b = int(t.ts // bucket_sec) * bucket_sec
        if b != b0:
            candles.append({"time": float(b0), "o": o, "h": h, "l": l, "c": c, "complete": True})
            b0 = b
            o = h = l = c = t.mid
        else:
            px = t.mid
            h = max(h, px)
            l = min(l, px)
            c = px
    candles.append({"time": float(b0), "o": o, "h": h, "l": l, "c": c, "complete": True})
    return candles


def speed_from_primitives(m_norm: float, atr_price: float, pair: str) -> str:
    # Align with MODE_RULES-style volatility ratio bands when full runtime context is unavailable.
    pip = float(phone_bot.pip_size(pair))
    atr_pips = (atr_price / pip) if pip > 0 else 0.0
    vr = abs(m_norm)
    if vr >= 1.5 and atr_pips >= 0.6:
        return "FAST"
    if vr >= 0.6 and atr_pips >= 0.5:
        return "MED"
    return "SLOW"


def first_tick_at_or_after(ticks: list[Tick], ts: float, *, strict_after: bool = False) -> Tick | None:
    for t in ticks:
        if strict_after:
            if t.ts > ts:
                return t
        elif t.ts >= ts:
            return t
    return ticks[-1] if ticks else None


def run_trade(pair: str, ticks: list[Tick], entry_tick: Tick, direction: str, speed_class: str, atr_entry: float) -> dict[str, Any]:
    sp = phone_bot.get_speed_params(speed_class)
    if direction == "SHORT" and bool(getattr(phone_bot, "DISABLE_SHORT_SLOW", False)) and speed_class == "SLOW":
        raise RuntimeError("short_slow_disabled")
    tp1_atr = float(phone_bot.get_directional_tp1_atr(speed_class, direction, float(sp["tp1_atr"])))
    entry = entry_tick.ask if direction == "LONG" else entry_tick.bid
    tp = entry + (tp1_atr * atr_entry if direction == "LONG" else -tp1_atr * atr_entry)
    sl = entry - (float(sp["sl_atr"]) * atr_entry if direction == "LONG" else -float(sp["sl_atr"]) * atr_entry)
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
        "tp": tp,
        "sl": sl,
    }
    # Replay only from entry forward; historical pre-entry ticks can create invalid
    # back-in-time SL/TP hits in the harness loop.
    ticks_from_entry = [t for t in ticks if t.ts >= entry_tick.ts]
    if not ticks_from_entry:
        ticks_from_entry = [entry_tick]
    env = sim_harness.SimEnvironment(instruments=[pair], ticks_by_inst={pair: ticks_from_entry}, bucket_sec=5.0)
    try:
        return env.run_aee_replay(trade=trade, speed_class=speed_class)
    finally:
        env.restore_live_wiring()


def summarize_trades(trades: list[dict[str, Any]]) -> dict[str, Any]:
    n = len(trades)
    if n == 0:
        return {
            "trade_count": 0,
            "pips_mean": 0.0,
            "pph_mean": 0.0,
            "win_rate": 0.0,
            "avg_hold_sec": 0.0,
            "core_exit_reason_counts": {},
            "sl_hit_rate": 0.0,
            "tp_hit_rate": 0.0,
            "aee_close_rate": 0.0,
        }
    pips = [float(t.get("weighted_pips") or 0.0) for t in trades]
    pph = [float(((t.get("pips_per_hour") or {}).get("weighted") or 0.0)) for t in trades]
    hold = [float((((t.get("legs") or {}).get("core") or {}).get("hold_sec") or 0.0)) for t in trades]
    reasons = [str(((((t.get("legs") or {}).get("core") or {}).get("exit") or {}).get("reason") or "NONE")) for t in trades]
    c = Counter(reasons)
    aee_n = sum(1 for r in reasons if r not in ("TP_HIT", "SL_HIT", "SIM_EOD_CLOSE", "CORE_TTL_EXPIRED"))
    return {
        "trade_count": n,
        "pips_mean": sum(pips) / n,
        "pph_mean": sum(pph) / n,
        "win_rate": sum(1 for x in pips if x > 0) / n,
        "avg_hold_sec": sum(hold) / n,
        "core_exit_reason_counts": dict(c),
        "sl_hit_rate": c.get("SL_HIT", 0) / n,
        "tp_hit_rate": c.get("TP_HIT", 0) / n,
        "aee_close_rate": aee_n / n,
    }


def extract_knobs_snapshot() -> dict[str, Any]:
    speed = {}
    for sc in ("FAST", "MED", "SLOW"):
        sp = dict(phone_bot.SPEED_CLASS_PARAMS.get(sc, {}))
        speed[sc] = {
            "tp1_atr": sp.get("tp1_atr"),
            "tp2_atr": sp.get("tp2_atr"),
            "sl_atr": sp.get("sl_atr"),
            "ttl_main": sp.get("ttl_main"),
        }

    aee_keys = [
        "AEE_PROFIT_CAPTURE_MIN_ATR",
        "AEE_PROFIT_CAPTURE_MIN_HOLD_SEC",
        "AEE_PROFIT_CAPTURE_FORCE_SEC",
        "AEE_PROFIT_CAPTURE_DECAY_SPEED",
        "AEE_PROFIT_CAPTURE_DECAY_VELOCITY",
        "AEE_TIME_DECAY_START_SEC",
        "AEE_TIME_DECAY_FULL_SEC",
        "AEE_TIME_DECAY_NEAR_TP_BOOST_MAX",
        "AEE_TIME_DECAY_GIVEBACK_TIGHTEN_MAX",
        "PANIC_PULLBACKRATE",
        "PANIC_PULLBACK",
        "PANIC_VELOCITY",
    ]
    aee = {k: getattr(phone_bot, k) for k in aee_keys if hasattr(phone_bot, k)}

    entry_keys = [
        "MOMENTUM_OVERRIDE_THRESHOLD",
        "ENTRY_CONFIRM_DISP_ATR",
        "ENTRY_CONFIRM_M1_CLOSES",
        "ENTRY_CONFIRM_SEC",
        "ENTRY_BASE_MAX_DIST_ATR",
        "ENTRY_PULLBACK_ATR",
        "ENTRY_RECLAIM_TOL_ATR",
        "ENABLE_SHORT_ENTRIES",
    ]
    entry = {k: getattr(phone_bot, k) for k in entry_keys if hasattr(phone_bot, k)}

    friction_keys = [
        "COST_MULT",
        "FRICTION_SEVERITY_MULT",
        "ENTRY_BUFFER_PIPS",
        "EXIT_BUFFER_PIPS",
        "MAX_SPREAD_PIPS",
    ]
    friction = {k: getattr(phone_bot, k) for k in friction_keys if hasattr(phone_bot, k)}

    env_keys = set(aee_keys + entry_keys + friction_keys)
    runtime_overrides = {k: os.getenv(k) for k in sorted(env_keys) if os.getenv(k) is not None}

    return {
        "speed_class_params": speed,
        "aee": aee,
        "entry": entry,
        "friction_spread": friction,
        "runtime_overrides_applied": runtime_overrides,
        "notes": {
            "sl_semantics": "LONG SL uses bid<=sl; SHORT SL uses ask>=sl",
            "entry_source": "WATCH->PROMOTE via _watch_primitive_contract_direction on replayed candles",
            "geometry_source": "phone_bot.SPEED_CLASS_PARAMS + phone_bot AEE",
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Entry-driven directional audit using replayed WATCH->PROMOTE path")
    ap.add_argument("--glob", default="scenarios/golden/v1.0/*.csv")
    ap.add_argument("--bucket-sec", type=int, default=300)
    ap.add_argument("--cooldown-sec", type=int, default=300)
    ap.add_argument("--out-knobs", default="reports/current_knobs_snapshot.json")
    ap.add_argument("--out-audit", default="reports/entry_driven_directional_audit.json")
    ap.add_argument("--schedule-32", action="store_true", help="Use deterministic 32-run schedule: first 12 scenarios + first 8 repeats")
    ap.add_argument("--short-confirm-disp-atr", type=float, default=None, help="SHORT-only displacement gate in ATR units for this audit")
    ap.add_argument("--long-confirm-disp-atr", type=float, default=None, help="LONG-only displacement gate in ATR units for this audit")
    args = ap.parse_args()

    Path(args.out_knobs).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_audit).parent.mkdir(parents=True, exist_ok=True)

    knobs = extract_knobs_snapshot()
    Path(args.out_knobs).write_text(json.dumps(knobs, indent=2), encoding="utf-8")

    modes = {
        "LONG": {"LONG"},
        "SHORT": {"SHORT"},
        "BOTH": {"LONG", "SHORT"},
    }

    paths = sorted(glob.glob(args.glob))
    scenarios = [p for p in paths if p.endswith('.csv')]
    if args.schedule_32 and len(scenarios) >= 12:
        scenarios = list(scenarios[:12]) + list(scenarios[:8])

    results: dict[str, Any] = {
        "dataset_glob": args.glob,
        "bucket_sec": args.bucket_sec,
        "cooldown_sec": args.cooldown_sec,
        "schedule_32": bool(args.schedule_32),
        "scenario_count": len(scenarios),
        "modes": {},
    }

    for mode, allowed_dirs in modes.items():
        all_trades: list[dict[str, Any]] = []
        funnel = Counter()
        per_scenario: dict[str, Any] = {}
        per_speed: dict[str, list[dict[str, Any]]] = defaultdict(list)
        opp_dirs = Counter()

        for spath in scenarios:
            pair, ticks = load_ticks(spath)
            candles = build_candles(ticks, bucket_sec=max(60, args.bucket_sec))
            scenario_trades: list[dict[str, Any]] = []
            last_entry_ts = -1e18
            n = max(30, int(getattr(phone_bot, "MOM_N", 5) + 5))

            for i in range(n, len(candles) + 1):
                funnel["WATCH"] += 1
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
                if direction == "LONG":
                    thr_l = float(args.long_confirm_disp_atr) if args.long_confirm_disp_atr is not None else float(getattr(phone_bot, "ENTRY_CONFIRM_DISP_ATR", 0.2) or 0.2)
                    if len(cwin) < 2:
                        continue
                    prev_c = float(cwin[-2]["c"])
                    cur_c = float(cwin[-1]["c"])
                    long_disp_atr = max(0.0, (cur_c - prev_c) / atr) if atr > 0 else 0.0
                    if long_disp_atr < thr_l:
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
                opp_dirs[direction] += 1
                if direction not in allowed_dirs:
                    continue

                cts = float(cwin[-1]["time"])
                if cts - last_entry_ts < float(args.cooldown_sec):
                    continue

                tick = first_tick_at_or_after(ticks, cts, strict_after=True)
                if tick is None:
                    continue

                funnel["PROMOTE"] += 1
                funnel["ENTRY_ATTEMPT"] += 1
                speed_class = speed_from_primitives(float(st.m_norm), atr, pair)

                try:
                    rep = run_trade(pair=pair, ticks=ticks, entry_tick=tick, direction=direction, speed_class=speed_class, atr_entry=atr)
                    rep["_meta"] = {
                        "scenario": Path(spath).name,
                        "direction": direction,
                        "speed_class": speed_class,
                        "entry_ts": tick.ts,
                    }
                    scenario_trades.append(rep)
                    all_trades.append(rep)
                    per_speed[speed_class].append(rep)
                    funnel["ORDER_FILLED"] += 1
                    last_entry_ts = cts
                except Exception:
                    # Keep deterministic flow: failed fills count as attempts only.
                    continue

            per_scenario[Path(spath).name] = {
                "summary": summarize_trades(scenario_trades),
                "trade_count": len(scenario_trades),
            }

        mode_out = summarize_trades(all_trades)
        mode_out["entry_funnel_counts"] = {
            "WATCH": int(funnel.get("WATCH", 0)),
            "PROMOTE": int(funnel.get("PROMOTE", 0)),
            "ENTRY_ATTEMPT": int(funnel.get("ENTRY_ATTEMPT", 0)),
            "ORDER_FILLED": int(funnel.get("ORDER_FILLED", 0)),
        }
        mode_out["directional_opportunity_count"] = dict(opp_dirs)
        mode_out["per_scenario"] = per_scenario
        mode_out["per_speed"] = {k: summarize_trades(v) for k, v in per_speed.items()}
        results["modes"][mode] = mode_out

    Path(args.out_audit).write_text(json.dumps(results, indent=2), encoding="utf-8")
    print(json.dumps({"knobs": args.out_knobs, "audit": args.out_audit}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
