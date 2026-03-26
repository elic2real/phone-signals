#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

PIP = 0.0001


@dataclass
class EntryKnobs:
    confirm_disp_atr: float = 0.12
    confirm_sec: float = 0.75
    base_max_dist_atr: float = 0.35


def _session_mask(ts: pd.Series, session: str) -> np.ndarray:
    h = ts.dt.hour.to_numpy()
    if session == "LONDON":
        return (h >= 8) & (h < 16)
    if session == "ASIA":
        return (h >= 0) & (h < 8)
    return (h >= 16) & (h < 24)


def _atr14(high: np.ndarray, low: np.ndarray, close: np.ndarray) -> np.ndarray:
    tr = np.empty_like(close)
    tr[0] = high[0] - low[0]
    prev = close[:-1]
    tr[1:] = np.maximum.reduce([
        high[1:] - low[1:],
        np.abs(high[1:] - prev),
        np.abs(low[1:] - prev),
    ])
    out = np.zeros_like(close)
    alpha = 1.0 / 14.0
    out[0] = tr[0]
    for i in range(1, len(close)):
        out[i] = (1 - alpha) * out[i - 1] + alpha * tr[i]
    return out


def _ema(x: np.ndarray, span: int) -> np.ndarray:
    a = 2.0 / (span + 1.0)
    out = np.zeros_like(x)
    out[0] = x[0]
    for i in range(1, len(x)):
        out[i] = a * x[i] + (1 - a) * out[i - 1]
    return out


def load_eurusd(path: str, session: str) -> dict[str, np.ndarray]:
    df = pd.read_parquet(path)
    df = df[df["pair"] == "EUR_USD"].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.sort_values("timestamp")
    m = _session_mask(df["timestamp"], session)
    df = df.loc[m].reset_index(drop=True)
    c = df["close"].to_numpy(dtype=float)
    h = df["high"].to_numpy(dtype=float)
    l = df["low"].to_numpy(dtype=float)
    atr = _atr14(h, l, c)
    ema20 = _ema(c, 20)
    ret1 = np.zeros_like(c)
    ret1[1:] = c[1:] - c[:-1]
    mom3 = np.zeros_like(c)
    if len(c) > 3:
        mom3[3:] = c[3:] - c[:-3]
    vol20 = pd.Series(np.abs(ret1)).rolling(20, min_periods=1).mean().to_numpy(dtype=float)
    dist_atr = np.abs(c - ema20) / np.maximum(atr, 1e-12)
    return {
        "close": c,
        "atr": atr,
        "ret1": ret1,
        "mom3": mom3,
        "dist_atr": dist_atr,
        "vol20": vol20,
    }


def build_path_bank(data: dict[str, np.ndarray], horizon_bars: int = 20) -> dict[str, Any]:
    c = data["close"]
    atr = data["atr"]
    mom3 = data["mom3"]
    vol20 = data["vol20"]
    n = len(c)
    starts = np.arange(30, n - horizon_bars - 1)
    rel = np.zeros((len(starts), horizon_bars + 1), dtype=float)
    spread = np.zeros((len(starts), horizon_bars + 1), dtype=float)
    tags = []
    for k, i in enumerate(starts):
        seg = c[i : i + horizon_bars + 1]
        rel[k] = (seg - seg[0]) / PIP
        rv = np.maximum(vol20[i], 1e-12)
        base_sp = 0.8 + 0.25 * ((atr[i] / rv) if rv > 0 else 1.0)
        sp = np.clip(base_sp + np.random.normal(0, 0.08, size=horizon_bars + 1), 0.6, 3.0)
        spread[k] = sp
        fwd = rel[k, -1]
        tags.append(
            {
                "mom_bucket": "high_mom" if abs(mom3[i]) / max(atr[i], 1e-12) > 0.6 else "low_mom",
                "vol_bucket": "expanded" if atr[i] > np.quantile(atr[max(0, i - 200): i + 1], 0.7) else "compressed",
                "class": "continuation" if abs(fwd) > 4.0 else ("stall" if abs(fwd) < 1.5 else "chop"),
            }
        )
    return {"starts": starts, "rel": rel, "spread": spread, "tags": tags}


def _required_conf_bars(confirm_sec: float) -> int:
    if confirm_sec <= 0.6:
        return 1
    if confirm_sec <= 1.0:
        return 2
    return 3


def candidate_indices(data: dict[str, np.ndarray], bank: dict[str, Any], direction: str, knobs: EntryKnobs) -> np.ndarray:
    c = data["close"]
    atr = data["atr"]
    ret1 = data["ret1"]
    dist = data["dist_atr"]
    starts = bank["starts"]
    req = _required_conf_bars(knobs.confirm_sec)
    out = []
    sign = 1.0 if direction == "LONG" else -1.0
    for idx_i, i in enumerate(starts):
        if i - req < 1:
            continue
        disp = sign * ret1[i] / max(atr[i], 1e-12)
        if disp < knobs.confirm_disp_atr:
            continue
        ok = True
        for b in range(req):
            if sign * ret1[i - b] <= 0:
                ok = False
                break
        if not ok:
            continue
        if dist[i] > knobs.base_max_dist_atr:
            continue
        out.append(idx_i)
    return np.asarray(out, dtype=int)


def perturb_path(path_pips: np.ndarray, rng: np.random.Generator) -> np.ndarray:
    amp = rng.choice([0.9, 1.0, 1.1, 1.2])
    tscale = rng.choice([0.9, 1.0, 1.1])
    n = len(path_pips)
    t_old = np.arange(n)
    t_new = np.linspace(0, n - 1, n) / tscale
    t_new = np.clip(t_new, 0, n - 1)
    p = np.interp(t_new, t_old, path_pips)
    p = p * amp + rng.normal(0.0, 0.08, size=n)
    p[0] = 0.0
    return p


def score_ladders(paths: np.ndarray, spreads: np.ndarray, direction: str, ladders: list[float], max_hold_bars: int) -> dict[str, Any]:
    d = 1.0 if direction == "LONG" else -1.0
    signed = paths * d
    results = {}
    n = signed.shape[0]
    for k in ladders:
        outcomes = []
        times = []
        tp_hits = sl_hits = to_hits = 0
        mfe = []
        mae = []
        for i in range(n):
            s = float(spreads[i, 0])
            tp = s * k
            sl = s * k
            series = signed[i, : max_hold_bars + 1]
            hi = float(np.max(series))
            lo = float(np.min(series))
            mfe.append(hi)
            mae.append(-lo)
            hit_tp = np.where(series >= tp)[0]
            hit_sl = np.where(series <= -sl)[0]
            t_tp = int(hit_tp[0]) if hit_tp.size else 10**9
            t_sl = int(hit_sl[0]) if hit_sl.size else 10**9
            if t_tp < t_sl and t_tp <= max_hold_bars:
                tp_hits += 1
                outcomes.append(tp)
                times.append(t_tp * 5)
            elif t_sl < t_tp and t_sl <= max_hold_bars:
                sl_hits += 1
                outcomes.append(-sl)
                times.append(t_sl * 5)
            else:
                to_hits += 1
                outcomes.append(float(series[-1]))
                times.append(max_hold_bars * 5)
        arr = np.asarray(outcomes, dtype=float)
        tarr = np.asarray(times, dtype=float)
        pph = arr / np.maximum(tarr / 60.0, 1e-9)
        results[str(k)] = {
            "trade_count": int(n),
            "tp_hit_rate": float(tp_hits / n),
            "sl_hit_rate": float(sl_hits / n),
            "timeout_rate": float(to_hits / n),
            "pips_mean": float(np.mean(arr)),
            "pph_mean": float(np.mean(pph)),
            "avg_resolution_min": float(np.mean(tarr)),
            "mfe_mean": float(np.mean(mfe)),
            "mae_mean": float(np.mean(mae)),
        }
    return results


def run_direction(
    bank: dict[str, Any],
    data: dict[str, np.ndarray],
    direction: str,
    knobs: EntryKnobs,
    trades_target: int,
    ladders: list[float],
    max_hold_bars: int,
    seed: int,
) -> dict[str, Any]:
    idx = candidate_indices(data, bank, direction, knobs)
    if idx.size == 0:
        return {"error": "no_candidates", "knobs": knobs.__dict__}
    rng = np.random.default_rng(seed)
    choose = rng.choice(idx, size=trades_target, replace=True)
    base_paths = bank["rel"][choose]
    base_sp = bank["spread"][choose]
    synth = np.stack([perturb_path(base_paths[i], rng) for i in range(len(choose))], axis=0)
    scored = score_ladders(synth, base_sp, direction, ladders, max_hold_bars)
    best_k = max(ladders, key=lambda x: scored[str(x)]["pips_mean"])
    return {
        "direction": direction,
        "knobs": knobs.__dict__,
        "candidate_count": int(idx.size),
        "trades_simulated": int(trades_target),
        "best_ladder": float(best_k),
        "best": scored[str(best_k)],
        "ladders": scored,
    }


def one_factor_sweep(
    bank: dict[str, Any],
    data: dict[str, np.ndarray],
    direction: str,
    baseline: EntryKnobs,
    trades_target: int,
    ladders: list[float],
    max_hold_bars: int,
    seed: int,
) -> dict[str, Any]:
    grids = {
        "confirm_disp_atr": [0.10, 0.12, 0.14, 0.16],
        "confirm_sec": [0.50, 0.75, 1.00, 1.25],
        "base_max_dist_atr": [0.30, 0.35, 0.40, 0.45],
    }
    out = {"baseline": None, "sweeps": {}, "direction": direction}
    out["baseline"] = run_direction(bank, data, direction, baseline, trades_target, ladders, max_hold_bars, seed)
    ceiling = out["baseline"]
    for key, vals in grids.items():
        rows = []
        for v in vals:
            k = EntryKnobs(**baseline.__dict__)
            setattr(k, key, float(v))
            res = run_direction(bank, data, direction, k, trades_target, ladders, max_hold_bars, seed + int(v * 1000))
            rows.append({"value": v, "result": res})
            if "best" in res and ("best" not in ceiling or res["best"]["pips_mean"] > ceiling["best"]["pips_mean"]):
                ceiling = res
        out["sweeps"][key] = rows
    out["ceiling"] = ceiling
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Historically calibrated, synthetically expanded entry-only LONG/SHORT ceiling search")
    ap.add_argument("--input", default="data_tape_oanda_m5_15_stitched/pair=EUR_USD/stitched.parquet")
    ap.add_argument("--session", default="LONDON", choices=["ASIA", "LONDON", "NY"])
    ap.add_argument("--trades-per-dir", type=int, default=5000)
    ap.add_argument("--max-hold-min", type=int, default=100)
    ap.add_argument("--seed", type=int, default=123)
    ap.add_argument("--out", default="reports/hybrid_entry/phase1_synthetic_entry_ceiling.json")
    args = ap.parse_args()

    ladders = [1.5, 2.0, 2.5, 3.0, 3.5]
    bars_per_min = 5
    max_hold_bars = max(1, args.max_hold_min // bars_per_min)

    data = load_eurusd(args.input, args.session)
    bank = build_path_bank(data, horizon_bars=max_hold_bars)

    baseline = EntryKnobs()
    long_res = one_factor_sweep(bank, data, "LONG", baseline, args.trades_per_dir, ladders, max_hold_bars, args.seed)
    short_res = one_factor_sweep(bank, data, "SHORT", baseline, args.trades_per_dir, ladders, max_hold_bars, args.seed + 77)

    summary = {
        "objective": "entry_only_historical_calibrated_synthetic_expanded",
        "pair": "EUR_USD",
        "session": args.session,
        "source": args.input,
        "notes": [
            "Uses historical EURUSD M5 stitched tape; max_hold 100 min modeled as 20 bars.",
            "Synthetic expansion is bootstrapped forward paths + amplitude/time/noise perturbations.",
            "Long/short optimized separately.",
        ],
        "config": {
            "baseline_knobs": baseline.__dict__,
            "ladders": ladders,
            "trades_per_dir": args.trades_per_dir,
            "max_hold_min": args.max_hold_min,
        },
        "bank": {
            "path_count": int(bank["rel"].shape[0]),
            "horizon_bars": int(bank["rel"].shape[1] - 1),
        },
        "long": long_res,
        "short": short_res,
        "best_long_only": long_res.get("ceiling", {}),
        "best_short_only": short_res.get("ceiling", {}),
    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(str(out))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
