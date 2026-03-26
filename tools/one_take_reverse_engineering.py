#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import hashlib
import json
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np

PIP = 0.0001
LABEL_TP_PIPS = 3.0
LABEL_SL_PIPS = 3.0


@dataclass
class EntryCfg:
    confirm_disp_atr: float
    confirm_m1_closes: int
    confirm_sec: float
    base_max_dist_atr: float
    dist_vel_k: float
    accel_norm_atr_min: float = -999.0
    no_limits: bool = False


@dataclass
class AEECfg:
    min_profit_atr: float
    min_hold_sec: int
    decay_speed_atr_per_min: float
    force_sec: int


def load_ticks(path: str) -> dict[str, np.ndarray]:
    ts = []
    bid = []
    ask = []
    mid = []
    pair = "EUR_USD"
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            try:
                pair = str(row.get("instrument") or pair)
                t = float(row.get("ts") or 0.0)
                b = float(row.get("bid") or 0.0)
                a = float(row.get("ask") or 0.0)
                m = float(row.get("mid") or ((a + b) * 0.5 if a > 0 and b > 0 else 0.0))
                if t <= 0.0 or m <= 0.0:
                    continue
                ts.append(t)
                bid.append(b if b > 0 else m)
                ask.append(a if a > 0 else m)
                mid.append(m)
            except Exception:
                continue
    arr = {
        "pair": np.array([pair]),
        "ts": np.asarray(ts, dtype=float),
        "bid": np.asarray(bid, dtype=float),
        "ask": np.asarray(ask, dtype=float),
        "mid": np.asarray(mid, dtype=float),
    }
    return arr


def minute_close_indices(ts: np.ndarray) -> np.ndarray:
    if ts.size == 0:
        return np.array([], dtype=int)
    mins = (ts // 60).astype(np.int64)
    chg = np.where(np.diff(mins) != 0)[0]
    idx = np.concatenate([chg, np.array([len(ts) - 1])])
    return idx.astype(int)


def atr_from_minute_closes(mid: np.ndarray, m_idx: np.ndarray, n: int = 14) -> np.ndarray:
    c = mid[m_idx]
    r = np.zeros_like(c)
    r[1:] = np.abs(c[1:] - c[:-1])
    out = np.zeros_like(c)
    a = 1.0 / float(n)
    out[0] = r[0] if r.size else 0.0
    for i in range(1, len(c)):
        out[i] = (1.0 - a) * out[i - 1] + a * r[i]
    return out


def ema(x: np.ndarray, span: int = 20) -> np.ndarray:
    out = np.zeros_like(x)
    if x.size == 0:
        return out
    a = 2.0 / (span + 1.0)
    out[0] = x[0]
    for i in range(1, len(x)):
        out[i] = a * x[i] + (1.0 - a) * out[i - 1]
    return out


def first_hit(path_pips: np.ndarray, tp: float, sl: float) -> tuple[str, int]:
    hit_tp = np.where(path_pips >= tp)[0]
    hit_sl = np.where(path_pips <= -sl)[0]
    t_tp = int(hit_tp[0]) if hit_tp.size else 10**9
    t_sl = int(hit_sl[0]) if hit_sl.size else 10**9
    if t_tp < t_sl:
        return "TP", t_tp
    if t_sl < t_tp:
        return "SL", t_sl
    return "TIMEOUT", len(path_pips) - 1


def path_metrics(path_pips: np.ndarray, tp: float, sl: float, pre_range_pips: float, early_window_sec: int = 60) -> dict[str, Any]:
    mfe = float(np.max(path_pips))
    mae = float(-np.min(path_pips))
    out, tau = first_hit(path_pips, tp, sl)
    speed = mfe / max(1.0, float(np.argmax(path_pips) + 1))
    stall = float(np.mean(np.abs(np.diff(path_pips)) < 0.05)) if len(path_pips) > 1 else 1.0
    strength = (mfe - mae)
    e_n = int(max(1, min(len(path_pips) - 1, early_window_sec)))
    early_path = path_pips[: e_n + 1]
    early_mfe = float(np.max(early_path))
    early_mae = float(-np.min(early_path))
    early_impulse_ratio = early_mfe / max(1e-9, mfe) if mfe > 0.0 else 0.0
    early_net = float(early_path[-1] - early_path[0])
    early_len = float(np.sum(np.abs(np.diff(early_path)))) if len(early_path) > 1 else 0.0
    path_efficiency = abs(early_net) / max(1e-9, early_len) if early_len > 0.0 else 0.0
    breakout_strength = early_mfe / max(1e-9, pre_range_pips)
    return {
        "mfe": mfe,
        "mae": mae,
        "tau_hit": int(tau),
        "speed": float(speed),
        "strength": float(strength),
        "stall": stall,
        "outcome": out,
        "early_mfe": early_mfe,
        "early_mae": early_mae,
        "early_impulse_ratio": float(early_impulse_ratio),
        "path_efficiency": float(path_efficiency),
        "breakout_strength": float(breakout_strength),
    }


def classify_zone_v2(m: dict[str, Any], tp: float, sl: float) -> str:
    # ZoneLabelV2: predictable impulse quality, not just eventual profitability.
    # Uses only entry-time + fixed early-window path properties.
    # Calibrated thresholds (same locked dataset) to avoid class-collapse while
    # preserving measurable GOOD/BAD separability.
    eir_min = 0.20
    eff_min = 0.30
    early_mae_max_mult = 0.50
    breakout_strength_min = 0.60
    if (
        m["outcome"] == "TP"
        and m["early_impulse_ratio"] >= eir_min
        and m["path_efficiency"] >= eff_min
        and m["early_mae"] <= (early_mae_max_mult * sl)
        and m["breakout_strength"] >= breakout_strength_min
    ):
        return "GOOD"
    if m["outcome"] == "SL":
        return "BAD"
    if m["mfe"] < (0.50 * tp) and m["mae"] < (0.50 * sl):
        return "NOISE"
    if m["outcome"] == "TP":
        # TP reached without predictable impulse quality -> not learnable edge.
        return "BAD"
    return "BAD"


def build_opportunity_map(dataset: list[str], horizon_sec: int = 6000, tp_pips: float = 3.0, sl_pips: float = 3.0) -> list[dict[str, Any]]:
    rows = []
    for spath in dataset:
        d = load_ticks(spath)
        ts = d["ts"]
        mid = d["mid"]
        m_idx = minute_close_indices(ts)
        for i in m_idx:
            t0 = ts[i]
            j_end = np.searchsorted(ts, t0 + horizon_sec, side="right") - 1
            if j_end <= i + 10:
                continue
            p0 = max(0, i - 60)
            pre = mid[p0 : i + 1]
            pre_range_pips = float((np.max(pre) - np.min(pre)) / PIP) if pre.size else 0.0
            fwd = mid[i : j_end + 1]
            base = fwd[0]
            long_path = (fwd - base) / PIP
            short_path = -long_path
            long_m = path_metrics(long_path, tp_pips, sl_pips, pre_range_pips=pre_range_pips)
            short_m = path_metrics(short_path, tp_pips, sl_pips, pre_range_pips=pre_range_pips)
            rows.append({
                "scenario": Path(spath).name,
                "ts": float(t0),
                "idx": int(i),
                "path_long": long_path.tolist(),
                "path_short": short_path.tolist(),
                "pre_range_pips": pre_range_pips,
                "spread_pips": float((d["ask"][i] - d["bid"][i]) / PIP),
                "long": {**long_m, "bucket": classify_zone_v2(long_m, tp=tp_pips, sl=sl_pips)},
                "short": {**short_m, "bucket": classify_zone_v2(short_m, tp=tp_pips, sl=sl_pips)},
            })
    return rows


def add_entry_features(rows: list[dict[str, Any]], dataset_cache: dict[str, dict[str, np.ndarray]]) -> None:
    for r in rows:
        sc = r["scenario"]
        d = dataset_cache[sc]
        ts = d["ts"]
        mid = d["mid"]
        i = int(r["idx"])
        m_idx = d["m_idx"]
        mc = mid[m_idx]
        atr = d["atr_m"]
        ema20 = d["ema20_m"]
        mpos = np.searchsorted(m_idx, i, side="right") - 1
        # Keep feature coverage high for overfit diagnostics; early bars use shorter context.
        if mpos < 2:
            r["features"] = None
            continue
        atr_p = max(float(atr[mpos] / PIP), 1e-6)
        dist_atr = abs(float(mc[mpos] - ema20[mpos])) / max(float(atr[mpos]), 1e-12)
        # Local compression proxy: short-window ATR vs longer-window ATR.
        atr_short = float(np.mean(atr[max(0, mpos - 5): mpos + 1])) if mpos >= 1 else float(atr[mpos])
        atr_long = float(np.mean(atr[max(0, mpos - 30): mpos + 1])) if mpos >= 1 else float(atr[mpos])
        compression_ratio = atr_short / max(1e-12, atr_long)
        # Distance to local structure: nearest of recent high/low normalized by ATR.
        lo = float(np.min(mc[max(0, mpos - 20): mpos + 1]))
        hi = float(np.max(mc[max(0, mpos - 20): mpos + 1]))
        lo_prev = float(np.min(mc[max(0, mpos - 20): mpos])) if mpos > 0 else lo
        hi_prev = float(np.max(mc[max(0, mpos - 20): mpos])) if mpos > 0 else hi
        d_to_hi = abs(float(mc[mpos]) - hi)
        d_to_lo = abs(float(mc[mpos]) - lo)
        struct_dist_atr = min(d_to_hi, d_to_lo) / max(float(atr[mpos]), 1e-12)
        # directional per-second displacement over 45s reference window
        j0 = max(0, i - 45)
        dpx = float(mid[i] - mid[j0]) / PIP
        vel = dpx / 0.75  # pips per minute approx
        # Signed acceleration proxy, normalized by ATR pips.
        j1 = max(0, i - 15)
        j2 = max(0, i - 30)
        v_now = float(mid[i] - mid[j1]) / PIP
        v_prev = float(mid[j1] - mid[j2]) / PIP
        accel_signed = (v_now - v_prev)
        accel_norm_atr = accel_signed / max(atr_p, 1e-6)
        long_break = bool((mpos > 0) and (mc[mpos] > hi_prev) and (mc[mpos - 1] <= hi_prev))
        short_break = bool((mpos > 0) and (mc[mpos] < lo_prev) and (mc[mpos - 1] >= lo_prev))
        # m1 closes directional streaks
        streak_long = 0
        streak_short = 0
        for k in range(mpos, max(mpos - 4, 1), -1):
            dm = float(mc[k] - mc[k - 1]) / PIP
            if dm > 0:
                streak_long += 1
            if dm < 0:
                streak_short += 1
        r["features"] = {
            "atr_pips": atr_p,
            "dist_atr": dist_atr,
            "disp45_pips": dpx,
            "vel_pips_per_min": vel,
            "streak_long": streak_long,
            "streak_short": streak_short,
            "compression_ratio": compression_ratio,
            "struct_dist_atr": struct_dist_atr,
            "accel_norm_atr": accel_norm_atr,
            "long_break": long_break,
            "short_break": short_break,
            "structure_break_accel": accel_norm_atr,
        }


def triggered(r: dict[str, Any], direction: str, cfg: EntryCfg) -> bool:
    if cfg.no_limits:
        return True
    f = r.get("features")
    if not f:
        return False
    atr = max(f["atr_pips"], 1e-6)
    disp_atr = (f["disp45_pips"] / atr) if direction == "LONG" else (-f["disp45_pips"] / atr)
    if disp_atr < cfg.confirm_disp_atr:
        return False
    streak = f["streak_long"] if direction == "LONG" else f["streak_short"]
    # True time-based confirmation: require directional persistence over confirm_sec minutes.
    required_closes = max(int(cfg.confirm_m1_closes), int(np.ceil(max(0.0, float(cfg.confirm_sec)))))
    if streak < required_closes:
        return False
    if f["dist_atr"] > (cfg.base_max_dist_atr + cfg.dist_vel_k * max(0.0, abs(f["vel_pips_per_min"]) - 0.8) * 0.1):
        return False
    # accel_norm_atr is used as a soft ranking signal in fitter, not a hard gate.
    return True


def first_blocker_reason(r: dict[str, Any], direction: str, cfg: EntryCfg) -> str:
    f = r.get("features")
    if not f:
        return "missing_features"
    atr = max(f["atr_pips"], 1e-6)
    disp_atr = (f["disp45_pips"] / atr) if direction == "LONG" else (-f["disp45_pips"] / atr)
    if disp_atr < cfg.confirm_disp_atr:
        return "confirm_disp_atr"
    streak = f["streak_long"] if direction == "LONG" else f["streak_short"]
    required_closes = max(int(cfg.confirm_m1_closes), int(np.ceil(max(0.0, float(cfg.confirm_sec)))))
    if streak < required_closes:
        return "confirm_m1_closes"
    vel_bonus = cfg.dist_vel_k * max(0.0, abs(f["vel_pips_per_min"]) - 0.8) * 0.1
    cap_base = cfg.base_max_dist_atr
    cap_full = cfg.base_max_dist_atr + vel_bonus
    if f["dist_atr"] > cap_full:
        if vel_bonus <= 0.0 and f["dist_atr"] > cap_base:
            return "base_max_dist_atr"
        if vel_bonus > 0.0:
            return "dist_vel_k"
        return "base_max_dist_atr"
    return "passed"


def feature_coverage(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    ok = sum(1 for r in rows if r.get("features") is not None)
    by_scenario: dict[str, dict[str, int]] = {}
    for r in rows:
        sc = r["scenario"]
        rec = by_scenario.setdefault(sc, {"total": 0, "with_features": 0})
        rec["total"] += 1
        if r.get("features") is not None:
            rec["with_features"] += 1
    by_scenario_rate = {
        k: {
            **v,
            "coverage_rate": (v["with_features"] / max(1, v["total"])),
        }
        for k, v in sorted(by_scenario.items())
    }
    return {
        "total": int(total),
        "with_features": int(ok),
        "coverage_rate": float(ok / max(1, total)),
        "by_scenario": by_scenario_rate,
    }


def fit_entry(rows: list[dict[str, Any]], direction: str, mode: str = "balanced") -> dict[str, Any]:
    # "balanced": maximize GOOD while suppressing BAD/NOISE triggers.
    # "coverage": overfit for throughput/coverage (near-all opportunities).
    if mode == "coverage":
        disp_grid = [0.00, 0.02, 0.05, 0.08, 0.10]
        m1_grid = [0, 1]
        sec_grid = [0.00, 0.25, 0.50]
        dist_grid = [0.45, 0.60, 0.80, 1.00]
        vel_grid = [0.00, 0.20, 0.40]
        accel_grid = [-999.0]
    else:
        disp_grid = [0.10, 0.12, 0.14, 0.16]
        m1_grid = [1, 2]
        sec_grid = [0.50, 0.75, 1.00, 1.25]
        dist_grid = [0.30, 0.35, 0.40, 0.45]
        vel_grid = [0.40, 0.60, 0.80]
        accel_grid = [-999.0]

    best = None
    for disp in disp_grid:
        for m1 in m1_grid:
            for sec in sec_grid:
                for dist in dist_grid:
                    for vel in vel_grid:
                        for accel_min in accel_grid:
                            cfg = EntryCfg(disp, m1, sec, dist, vel, accel_min, False)
                            g = b = n = tg = tb = tn = 0
                            for r in rows:
                                side = r["long"] if direction == "LONG" else r["short"]
                                z = side["bucket"]
                                if z == "GOOD":
                                    g += 1
                                elif z == "BAD":
                                    b += 1
                                else:
                                    n += 1
                                t = triggered(r, direction, cfg)
                                if z == "GOOD" and t:
                                    tg += 1
                                elif z == "BAD" and t:
                                    tb += 1
                                elif z == "NOISE" and t:
                                    tn += 1
                            gcr = tg / max(1, g)
                            btr = tb / max(1, b)
                            ntr = tn / max(1, n)
                            if mode == "coverage":
                                tcr = (tg + tb + tn) / max(1, g + b + n)
                                # Primary goal: trigger as many opportunities as possible.
                                # Small tie-break toward GOOD capture.
                                score = tcr + 0.05 * gcr
                            else:
                                score = gcr - btr - ntr
                            rec = {
                                "cfg": cfg,
                                "score": score,
                                "good_capture_rate": gcr,
                                "bad_trigger_rate": btr,
                                "noise_trigger_rate": ntr,
                                "counts": {"good": g, "bad": b, "noise": n, "trig_good": tg, "trig_bad": tb, "trig_noise": tn},
                                "trigger_rate_total": (tg + tb + tn) / max(1, g + b + n),
                            }
                            if best is None or rec["score"] > best["score"]:
                                best = rec
    if mode == "coverage":
        # Explicit no-limit candidate for max-throughput overfit mode.
        cfg = EntryCfg(0.0, 0, 0.0, 999.0, 0.0, -999.0, True)
        g = b = n = tg = tb = tn = 0
        for r in rows:
            side = r["long"] if direction == "LONG" else r["short"]
            z = side["bucket"]
            if z == "GOOD":
                g += 1
                tg += 1
            elif z == "BAD":
                b += 1
                tb += 1
            else:
                n += 1
                tn += 1
        gcr = tg / max(1, g)
        btr = tb / max(1, b)
        ntr = tn / max(1, n)
        tcr = (tg + tb + tn) / max(1, g + b + n)
        rec = {
            "cfg": cfg,
            "score": tcr + 0.05 * gcr,
            "good_capture_rate": gcr,
            "bad_trigger_rate": btr,
            "noise_trigger_rate": ntr,
            "counts": {"good": g, "bad": b, "noise": n, "trig_good": tg, "trig_bad": tb, "trig_noise": tn},
            "trigger_rate_total": tcr,
        }
        if best is None or rec["score"] > best["score"]:
            best = rec
    assert best is not None
    return best


def collect_triggered_trades(rows: list[dict[str, Any]], direction: str, cfg: EntryCfg) -> list[dict[str, Any]]:
    out = []
    for r in rows:
        if not triggered(r, direction, cfg):
            continue
        side = r["long"] if direction == "LONG" else r["short"]
        path = np.asarray(r["path_long"] if direction == "LONG" else r["path_short"], dtype=float)
        out.append({
            "scenario": r["scenario"],
            "ts": r["ts"],
            "bucket": side["bucket"],
            "path": path,
            "spread_pips": float(r["spread_pips"]),
            "atr_pips": float(r["features"]["atr_pips"] if r.get("features") else 5.0),
        })
    return out


def simulate_aee_exit(path: np.ndarray, spread_pips: float, atr_pips: float, cfg: AEECfg, tp_mult: float = 2.5, sl_mult: float = 2.5) -> float:
    # Keep execution geometry aligned with labeling geometry to avoid objective mismatch.
    tp = float(LABEL_TP_PIPS)
    sl = float(LABEL_SL_PIPS)
    favorable = 0.0
    for i in range(1, len(path)):
        p = float(path[i])
        favorable = max(favorable, p)
        if p <= -sl:
            return -sl
        if p >= tp:
            return tp
        hold = i
        speed = (float(path[i]) - float(path[max(0, i - 30)])) / max(1.0, (i - max(0, i - 30))) * 60.0 / max(1.0, atr_pips)
        if hold >= cfg.min_hold_sec and favorable / max(1.0, atr_pips) >= cfg.min_profit_atr and speed <= cfg.decay_speed_atr_per_min:
            return p
        if hold >= cfg.force_sec:
            return p
    return float(path[-1])


def fit_aee(trades: list[dict[str, Any]]) -> dict[str, Any]:
    min_profit_grid = [0.25, 0.35, 0.45]
    hold_grid = [60, 90, 120]
    decay_grid = [0.20, 0.35, 0.50]
    force_grid = [600, 900, 1200]

    best = None
    for mp in min_profit_grid:
        for hs in hold_grid:
            for ds in decay_grid:
                for fs in force_grid:
                    cfg = AEECfg(mp, hs, ds, fs)
                    sum_good = sum_bad = 0.0
                    n_good = n_bad = n_noise = 0
                    for t in trades:
                        x = simulate_aee_exit(t["path"], t["spread_pips"], t["atr_pips"], cfg)
                        b = t["bucket"]
                        if b == "GOOD":
                            sum_good += x
                            n_good += 1
                        elif b == "BAD":
                            sum_bad += x
                            n_bad += 1
                        else:
                            n_noise += 1
                    avg_good = sum_good / max(1, n_good)
                    avg_bad = sum_bad / max(1, n_bad)
                    # maximize good extraction while minimizing bad damage
                    score = avg_good - max(0.0, -avg_bad)
                    rec = {
                        "cfg": cfg,
                        "score": score,
                        "avg_good_exit_pips": avg_good,
                        "avg_bad_exit_pips": avg_bad,
                        "counts": {"good": n_good, "bad": n_bad, "noise": n_noise},
                    }
                    if best is None or rec["score"] > best["score"]:
                        best = rec
    assert best is not None
    return best


def validate_full(trades_l: list[dict[str, Any]], trades_s: list[dict[str, Any]], aee_l: AEECfg, aee_s: AEECfg) -> dict[str, Any]:
    def run(trades: list[dict[str, Any]], cfg: AEECfg):
        if not trades:
            return {"n": 0, "pips_mean": 0.0, "win_rate": 0.0}
        exits = [simulate_aee_exit(t["path"], t["spread_pips"], t["atr_pips"], cfg) for t in trades]
        arr = np.asarray(exits, dtype=float)
        return {"n": int(len(arr)), "pips_mean": float(np.mean(arr)), "win_rate": float(np.mean(arr > 0.0))}

    long_v = run(trades_l, aee_l)
    short_v = run(trades_s, aee_s)
    both = np.asarray([*([simulate_aee_exit(t["path"], t["spread_pips"], t["atr_pips"], aee_l) for t in trades_l]), *([simulate_aee_exit(t["path"], t["spread_pips"], t["atr_pips"], aee_s) for t in trades_s])], dtype=float)
    both_m = float(np.mean(both)) if both.size else 0.0
    verdict = "PASS" if both_m > 0.0 and long_v["pips_mean"] > 0.0 else "FAIL"
    return {"long": long_v, "short": short_v, "both": {"n": int(both.size), "pips_mean": both_m}, "overfit_verdict": verdict}


def hash_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            b = f.read(1024 * 1024)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def summarize_pass_table(
    rows: list[dict[str, Any]],
    direction: str,
    cfg: EntryCfg,
    pips_mean: float,
    score: float,
) -> dict[str, Any]:
    side_key = "long" if direction == "LONG" else "short"
    good_total = bad_total = noise_total = 0
    good_trig = bad_trig = noise_trig = 0
    for r in rows:
        side = r[side_key]
        z = side["bucket"]
        if z == "GOOD":
            good_total += 1
        elif z == "BAD":
            bad_total += 1
        else:
            noise_total += 1
        if triggered(r, direction, cfg):
            if z == "GOOD":
                good_trig += 1
            elif z == "BAD":
                bad_trig += 1
            else:
                noise_trig += 1
    return {
        "direction": direction,
        "good_total": int(good_total),
        "good_triggered": int(good_trig),
        "good_capture_rate": float(good_trig / max(1, good_total)),
        "bad_total": int(bad_total),
        "bad_triggered": int(bad_trig),
        "bad_trigger_rate": float(bad_trig / max(1, bad_total)),
        "noise_total": int(noise_total),
        "noise_triggered": int(noise_trig),
        "noise_trigger_rate": float(noise_trig / max(1, noise_total)),
        "pips_mean": float(pips_mean),
        "score": float(score),
    }


def missed_good_blockers(rows: list[dict[str, Any]], direction: str, cfg: EntryCfg) -> dict[str, int]:
    side_key = "long" if direction == "LONG" else "short"
    counts: dict[str, int] = {}
    for r in rows:
        side = r[side_key]
        if side["bucket"] != "GOOD":
            continue
        if triggered(r, direction, cfg):
            continue
        reason = first_blocker_reason(r, direction, cfg)
        counts[reason] = counts.get(reason, 0) + 1
    return dict(sorted(counts.items(), key=lambda kv: kv[1], reverse=True))


def classify_model_verdict(scorecard: dict[str, Any]) -> str:
    lg = float(scorecard.get("long_good_capture", 0.0))
    sg = float(scorecard.get("short_good_capture", 0.0))
    lb = float(scorecard.get("long_bad_trigger", 1.0))
    sb = float(scorecard.get("short_bad_trigger", 1.0))
    both_p = float(((scorecard.get("both") or {}).get("pips_mean") or 0.0))
    if lg >= 0.50 and sg >= 0.50 and lb <= 0.15 and sb <= 0.15 and both_p > 0.0:
        return "MODEL_PASS"
    if lg >= 0.25 and sg >= 0.25 and lb <= 0.25 and sb <= 0.25 and both_p > 0.0:
        return "MODEL_MARGINAL"
    return "PIPELINE_PASS_MODEL_FAIL"


def main() -> int:
    ap = argparse.ArgumentParser(description="One-take reverse-engineering pipeline: map->zones->entry L/S->AEE L/S->validate")
    ap.add_argument("--glob", default="scenarios/golden/v1.0/*.csv")
    ap.add_argument("--horizon-min", type=int, default=100)
    ap.add_argument("--tp-pips", type=float, default=3.0)
    ap.add_argument("--sl-pips", type=float, default=3.0)
    ap.add_argument("--entry-fit-mode", choices=["balanced", "coverage"], default="balanced")
    ap.add_argument("--seed", type=int, default=123)
    ap.add_argument("--out-root", default="reports/one_take_runs")
    ap.add_argument("--out", default="reports/one_take_reverse_engineering.json")
    args = ap.parse_args()
    global LABEL_TP_PIPS, LABEL_SL_PIPS
    LABEL_TP_PIPS = float(args.tp_pips)
    LABEL_SL_PIPS = float(args.sl_pips)
    random.seed(args.seed)
    np.random.seed(args.seed)

    files = sorted([p for p in glob.glob(args.glob) if p.endswith('.csv')])
    if not files:
        raise SystemExit("no dataset files found")
    file_hashes = {Path(p).name: hash_file(p) for p in files}
    dataset_hash = hashlib.sha256("".join(file_hashes[k] for k in sorted(file_hashes)).encode("utf-8")).hexdigest()
    config_blob = {
        "glob": args.glob,
        "horizon_min": args.horizon_min,
        "tp_pips": args.tp_pips,
        "sl_pips": args.sl_pips,
        "entry_fit_mode": args.entry_fit_mode,
        "seed": args.seed,
    }
    config_hash = hashlib.sha256(json.dumps(config_blob, sort_keys=True).encode("utf-8")).hexdigest()
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir = Path(args.out_root) / f"{run_id}_{dataset_hash[:8]}_{config_hash[:8]}"
    run_dir.mkdir(parents=True, exist_ok=True)

    rows = build_opportunity_map(files, horizon_sec=args.horizon_min * 60, tp_pips=args.tp_pips, sl_pips=args.sl_pips)

    # preload per-scenario caches for feature extraction
    cache: dict[str, dict[str, np.ndarray]] = {}
    for fp in files:
        d = load_ticks(fp)
        m_idx = minute_close_indices(d["ts"])
        mc = d["mid"][m_idx]
        cache[Path(fp).name] = {
            **d,
            "m_idx": m_idx,
            "atr_m": atr_from_minute_closes(d["mid"], m_idx, n=14),
            "ema20_m": ema(mc, 20),
        }

    add_entry_features(rows, cache)

    # entry passes
    long_fit = fit_entry(rows, "LONG", mode=args.entry_fit_mode)
    short_fit = fit_entry(rows, "SHORT", mode=args.entry_fit_mode)

    long_cfg: EntryCfg = long_fit["cfg"]
    short_cfg: EntryCfg = short_fit["cfg"]

    # pass 3 both (directional config preferred)
    directional_entry = {
        "long": {
            "confirm_disp_atr": long_cfg.confirm_disp_atr,
            "confirm_m1_closes": long_cfg.confirm_m1_closes,
            "confirm_sec": long_cfg.confirm_sec,
            "base_max_dist_atr": long_cfg.base_max_dist_atr,
            "dist_vel_k": long_cfg.dist_vel_k,
        },
        "short": {
            "confirm_disp_atr": short_cfg.confirm_disp_atr,
            "confirm_m1_closes": short_cfg.confirm_m1_closes,
            "confirm_sec": short_cfg.confirm_sec,
            "base_max_dist_atr": short_cfg.base_max_dist_atr,
            "dist_vel_k": short_cfg.dist_vel_k,
        },
    }

    long_trades = collect_triggered_trades(rows, "LONG", long_cfg)
    short_trades = collect_triggered_trades(rows, "SHORT", short_cfg)

    # AEE passes
    long_aee_fit = fit_aee(long_trades)
    short_aee_fit = fit_aee(short_trades)
    aee_l: AEECfg = long_aee_fit["cfg"]
    aee_s: AEECfg = short_aee_fit["cfg"]

    validation = validate_full(long_trades, short_trades, aee_l, aee_s)

    def side_bucket_stats(side: str, fit: dict[str, Any]) -> dict[str, float]:
        return {
            f"{side.lower()}_good_capture": float(fit["good_capture_rate"]),
            f"{side.lower()}_bad_trigger": float(fit["bad_trigger_rate"]),
            f"{side.lower()}_noise_trigger": float(fit["noise_trigger_rate"]),
        }

    long_pass = summarize_pass_table(rows, "LONG", long_cfg, validation["long"]["pips_mean"], long_fit["score"])
    short_pass = summarize_pass_table(rows, "SHORT", short_cfg, validation["short"]["pips_mean"], short_fit["score"])
    both_pass = {
        "direction": "BOTH_DIRECTIONAL",
        "good_total": int(long_pass["good_total"] + short_pass["good_total"]),
        "good_triggered": int(long_pass["good_triggered"] + short_pass["good_triggered"]),
        "good_capture_rate": float((long_pass["good_triggered"] + short_pass["good_triggered"]) / max(1, long_pass["good_total"] + short_pass["good_total"])),
        "bad_total": int(long_pass["bad_total"] + short_pass["bad_total"]),
        "bad_triggered": int(long_pass["bad_triggered"] + short_pass["bad_triggered"]),
        "bad_trigger_rate": float((long_pass["bad_triggered"] + short_pass["bad_triggered"]) / max(1, long_pass["bad_total"] + short_pass["bad_total"])),
        "noise_total": int(long_pass["noise_total"] + short_pass["noise_total"]),
        "noise_triggered": int(long_pass["noise_triggered"] + short_pass["noise_triggered"]),
        "noise_trigger_rate": float((long_pass["noise_triggered"] + short_pass["noise_triggered"]) / max(1, long_pass["noise_total"] + short_pass["noise_total"])),
        "pips_mean": float(validation["both"]["pips_mean"]),
        "score": float(long_fit["score"] + short_fit["score"]),
    }
    scorecard = {
        **side_bucket_stats("LONG", long_fit),
        **side_bucket_stats("SHORT", short_fit),
        "long_entry_score": float(long_fit["score"]),
        "short_entry_score": float(short_fit["score"]),
        "long_triggered_trades": int(len(long_trades)),
        "short_triggered_trades": int(len(short_trades)),
        **validation,
    }
    scorecard["model_verdict"] = classify_model_verdict(scorecard)

    out_obj = {
        "meta": {
            "pair": "EURUSD",
            "session": "London",
            "weekday": "Monday",
            "dataset_glob": args.glob,
            "fixed_dataset_files": [Path(x).name for x in files],
            "file_hashes": file_hashes,
            "dataset_hash": dataset_hash,
            "horizon_min": args.horizon_min,
            "tp_pips": args.tp_pips,
            "sl_pips": args.sl_pips,
            "entry_fit_mode": args.entry_fit_mode,
            "seed": int(args.seed),
            "config_hash": config_hash,
            "run_id": run_id,
            "run_dir": str(run_dir),
        },
        "entry": {
            "long": directional_entry["long"],
            "short": directional_entry["short"],
        },
        "aee": {
            "long": {
                "min_profit_atr": aee_l.min_profit_atr,
                "min_hold_sec": aee_l.min_hold_sec,
                "decay_speed_atr_per_min": aee_l.decay_speed_atr_per_min,
                "force_sec": aee_l.force_sec,
            },
            "short": {
                "min_profit_atr": aee_s.min_profit_atr,
                "min_hold_sec": aee_s.min_hold_sec,
                "decay_speed_atr_per_min": aee_s.decay_speed_atr_per_min,
                "force_sec": aee_s.force_sec,
            },
        },
        "scorecard": scorecard,
        "entry_pass_tables": {
            "long_pass": long_pass,
            "short_pass": short_pass,
            "both_pass": both_pass,
        },
        "missed_good_first_blocker": {
            "long": missed_good_blockers(rows, "LONG", long_cfg),
            "short": missed_good_blockers(rows, "SHORT", short_cfg),
        },
        "diagnostics": {
            "opportunity_rows": int(len(rows)),
            "feature_coverage": feature_coverage(rows),
            "long_counts": long_fit["counts"],
            "short_counts": short_fit["counts"],
            "aee_long_fit": {
                "score": float(long_aee_fit["score"]),
                "avg_good_exit_pips": float(long_aee_fit["avg_good_exit_pips"]),
                "avg_bad_exit_pips": float(long_aee_fit["avg_bad_exit_pips"]),
            },
            "aee_short_fit": {
                "score": float(short_aee_fit["score"]),
                "avg_good_exit_pips": float(short_aee_fit["avg_good_exit_pips"]),
                "avg_bad_exit_pips": float(short_aee_fit["avg_bad_exit_pips"]),
            },
        },
    }

    outp = Path(args.out)
    outp.parent.mkdir(parents=True, exist_ok=True)
    outp.write_text(json.dumps(out_obj, indent=2), encoding="utf-8")
    (run_dir / "one_take_reverse_engineering.json").write_text(json.dumps(out_obj, indent=2), encoding="utf-8")
    print(str(outp))
    print(str(run_dir / "one_take_reverse_engineering.json"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
