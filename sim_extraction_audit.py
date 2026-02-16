#!/usr/bin/env python3
"""Simulation extraction audit CLI.

Analyze a directory of `sim_results*.json` files and produce a consolidated
`extraction_audit_report.json` focused on pips-per-hour and extraction quality.

Usage:
    python3 sim_extraction_audit.py --input_dir ./sim_outputs --output_file extraction_audit_report.json
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

# Exit-delay horizons in pair-local evaluation ticks.
DELAY_HORIZONS: Tuple[int, ...] = (1, 2, 5)


def _scenario_from_filename(name: str) -> str:
    stem = Path(name).stem
    parts = stem.split("__")
    return parts[-1] if len(parts) > 1 else "unknown"


# =========================
# Data containers
# =========================
@dataclass
class TradeAudit:
    file: str
    instrument: str
    direction: str
    exit_reason: str
    has_exit: bool
    entry_tick_idx: Optional[int]
    entry_ts: float
    entry_price: float
    exit_tick_idx: Optional[int]
    exit_pair_eval_idx: Optional[int]
    exit_ts: float
    exit_price: float
    hold_sec: float
    exit_pips: float
    core_pips: float
    runner_pips: float
    weighted_pips: float
    core_hold_sec: float
    runner_hold_sec: float
    mfe_pips: float
    mae_pips: float
    capture_pct: Optional[float]
    core_mfe_pips: float
    core_mae_pips: float
    runner_mfe_pips: float
    runner_mae_pips: float
    core_capture_pct: Optional[float]
    runner_capture_pct: Optional[float]
    core_left_on_table_pips: float
    runner_left_on_table_pips: float
    core_exit_reason: str
    runner_exit_reason: str
    cadence_switch: bool
    armed_gap_median_ts: Optional[float]
    unarmed_gap_median_ts: Optional[float]
    better_if_delay: Dict[int, bool]
    delta_if_delay_pips: Dict[int, Optional[float]]
    panic_recovery_plus_30_pips: Optional[float]
    panic_recovery_plus_60_pips: Optional[float]
    panic_recovery_plus_120_pips: Optional[float]
    scenario: str
    overshoot_peak_atr: float
    overshoot_pips_captured: float
    max_overshoot_seen_pips: float
    overshoot_capture_rate: Optional[float]
    pulse_wait_30_better: Optional[bool]


# =========================
# Safe numeric helpers
# =========================
def _safe_float(value: Any, default: float = float("nan")) -> float:
    try:
        v = float(value)
        if math.isfinite(v):
            return v
    except Exception:
        pass
    return default


def _safe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return default


def _finite(values: Iterable[Any]) -> List[float]:
    out: List[float] = []
    for v in values:
        if isinstance(v, (int, float)) and math.isfinite(float(v)):
            out.append(float(v))
    return out


def _mean(values: Iterable[Any]) -> float:
    vals = _finite(values)
    return float(sum(vals) / len(vals)) if vals else 0.0


def _std(values: Iterable[Any]) -> float:
    vals = _finite(values)
    if len(vals) < 2:
        return 0.0
    return float(statistics.pstdev(vals))


def _median(values: Iterable[Any]) -> float:
    vals = _finite(values)
    return float(statistics.median(vals)) if vals else 0.0


def _median_or_none(values: Iterable[Any]) -> Optional[float]:
    vals = _finite(values)
    return float(statistics.median(vals)) if vals else None


def _percentile(values: Iterable[Any], q: float) -> float:
    vals = sorted(_finite(values))
    if not vals:
        return 0.0
    if len(vals) == 1:
        return vals[0]
    pos = max(0.0, min(1.0, q)) * (len(vals) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return vals[lo]
    frac = pos - lo
    return vals[lo] + (vals[hi] - vals[lo]) * frac


# =========================
# Market conversion helpers
# =========================
def _pip_size(instrument: str) -> float:
    return 0.01 if str(instrument).upper().endswith("_JPY") else 0.0001


def _pips_from_price_delta(instrument: str, delta: float) -> float:
    pip = _pip_size(instrument)
    return float(delta / pip) if pip > 0 else 0.0


def _exit_price_for_direction(direction: str, rec: Dict[str, Any]) -> float:
    # Use executable side for realistic close assumption.
    if str(direction).upper() == "LONG":
        px = _safe_float(rec.get("bid"))
    else:
        px = _safe_float(rec.get("ask"))
    if math.isfinite(px):
        return px
    return _safe_float(rec.get("mid"), default=0.0)


# =========================
# Input parsing / record extraction
# =========================
def _find_exit_record(results: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # Prefer canonical top-level exit, fallback to first tick record with exit_reason.
    top_exit = results.get("exit")
    if isinstance(top_exit, dict) and top_exit:
        return top_exit
    for rec in results.get("tick_records", []):
        if isinstance(rec, dict) and rec.get("exit_reason"):
            return rec
    return None


def _pair_records(results: Dict[str, Any], instrument: str) -> List[Dict[str, Any]]:
    # Keep only rows for the target instrument, sorted by pair_eval_idx where available.
    rows: List[Dict[str, Any]] = []
    for rec in results.get("tick_records", []):
        if not isinstance(rec, dict):
            continue
        if str(rec.get("instrument", "")) != str(instrument):
            continue
        row = dict(rec)
        row["idx"] = _safe_int(row.get("idx"))
        row["pair_eval_idx"] = _safe_int(row.get("pair_eval_idx"))
        row["ts"] = _safe_float(row.get("ts"), default=float("nan"))
        row["mid"] = _safe_float(row.get("mid"), default=float("nan"))
        rows.append(row)

    # Order by pair_eval_idx when present, then global idx.
    rows.sort(key=lambda r: (r["pair_eval_idx"] if r["pair_eval_idx"] is not None else 10**12, r["idx"] if r["idx"] is not None else 10**12))
    return rows


def _first_record(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    return rows[0] if rows else None


# =========================
# Metric computations per run
# =========================
def _compute_mfe_mae_pips(direction: str, entry_price: float, instrument: str, rows: List[Dict[str, Any]]) -> Tuple[float, float]:
    mids = _finite(r.get("mid") for r in rows)
    if not mids or not math.isfinite(entry_price):
        return 0.0, 0.0

    if str(direction).upper() == "LONG":
        favorable = max(mids) - entry_price
        adverse = min(mids) - entry_price
    else:
        favorable = entry_price - min(mids)
        adverse = entry_price - max(mids)

    return _pips_from_price_delta(instrument, favorable), _pips_from_price_delta(instrument, adverse)


def _compute_cadence(rows: List[Dict[str, Any]]) -> Tuple[Optional[float], Optional[float], bool]:
    # Use consecutive evaluated decisions and split gaps by current-row tick_armed state.
    eval_rows = [r for r in rows if bool(r.get("evaluated", True)) and math.isfinite(_safe_float(r.get("ts")))]
    if len(eval_rows) < 2:
        return None, None, False

    armed_gaps: List[float] = []
    unarmed_gaps: List[float] = []

    prev = eval_rows[0]
    for curr in eval_rows[1:]:
        dt = _safe_float(curr.get("ts"), default=float("nan")) - _safe_float(prev.get("ts"), default=float("nan"))
        if not math.isfinite(dt) or dt < 0:
            prev = curr
            continue
        if bool(curr.get("tick_armed")):
            armed_gaps.append(dt)
        else:
            unarmed_gaps.append(dt)
        prev = curr

    armed_med = _median_or_none(armed_gaps)
    unarmed_med = _median_or_none(unarmed_gaps)
    cadence_switch = bool(armed_med is not None and unarmed_med is not None and armed_med < (0.7 * unarmed_med))
    return armed_med, unarmed_med, cadence_switch


def _compute_delay_sensitivity(
    direction: str,
    entry_price: float,
    exit_pair_eval_idx: Optional[int],
    exit_pips: float,
    instrument: str,
    rows: List[Dict[str, Any]],
) -> Tuple[Dict[int, bool], Dict[int, Optional[float]]]:
    by_pair_eval: Dict[int, Dict[str, Any]] = {int(r["pair_eval_idx"]): r for r in rows if r.get("pair_eval_idx") is not None}

    better: Dict[int, bool] = {}
    delta_pips: Dict[int, Optional[float]] = {}

    if exit_pair_eval_idx is None:
        for h in DELAY_HORIZONS:
            better[h] = False
            delta_pips[h] = None
        return better, delta_pips

    for h in DELAY_HORIZONS:
        delayed = by_pair_eval.get(int(exit_pair_eval_idx) + h)
        if delayed is None:
            better[h] = False
            delta_pips[h] = None
            continue

        delayed_exit_px = _exit_price_for_direction(direction, delayed)
        if str(direction).upper() == "LONG":
            delayed_pips = _pips_from_price_delta(instrument, delayed_exit_px - entry_price)
        else:
            delayed_pips = _pips_from_price_delta(instrument, entry_price - delayed_exit_px)

        delta = float(delayed_pips - exit_pips)
        better[h] = delta > 0
        delta_pips[h] = delta

    return better, delta_pips


def audit_file(path: Path) -> Optional[TradeAudit]:
    try:
        with path.open("r", encoding="utf-8") as f:
            results = json.load(f)
    except Exception:
        return None

    trade = results.get("trade") or {}
    instrument = str(trade.get("pair") or "")
    direction = str(trade.get("dir") or "LONG").upper()

    rows = _pair_records(results, instrument)
    entry_rec = _first_record(rows)
    exit_rec = _find_exit_record(results)

    entry_price = _safe_float(trade.get("entry"), default=float("nan"))
    entry_ts = _safe_float(trade.get("ts"), default=float("nan"))
    entry_tick_idx = None

    # Prefer actual first tick-record for entry tick/timestamp if available.
    if entry_rec is not None:
        entry_tick_idx = _safe_int(entry_rec.get("idx"))
        entry_ts_candidate = _safe_float(entry_rec.get("ts"), default=float("nan"))
        if math.isfinite(entry_ts_candidate):
            entry_ts = entry_ts_candidate
        if not math.isfinite(entry_price):
            # Fallback entry price from first observed mid only when trade entry is absent.
            entry_price = _safe_float(entry_rec.get("mid"), default=float("nan"))

    has_exit = isinstance(exit_rec, dict) and bool(exit_rec)
    if has_exit:
        exit_tick_idx = _safe_int(exit_rec.get("idx"))
        exit_pair_eval_idx = _safe_int(exit_rec.get("pair_eval_idx"))
        exit_ts = _safe_float(exit_rec.get("ts"), default=float("nan"))
        exit_price = _exit_price_for_direction(direction, exit_rec)
        exit_reason = str(exit_rec.get("exit_reason") or "UNKNOWN")
    else:
        exit_tick_idx = None
        exit_pair_eval_idx = None
        exit_ts = float("nan")
        exit_price = float("nan")
        exit_reason = "NO_EXIT"

    hold_sec = (exit_ts - entry_ts) if (math.isfinite(exit_ts) and math.isfinite(entry_ts)) else 0.0

    if math.isfinite(entry_price) and math.isfinite(exit_price):
        if direction == "LONG":
            exit_pips = _pips_from_price_delta(instrument, exit_price - entry_price)
        else:
            exit_pips = _pips_from_price_delta(instrument, entry_price - exit_price)
    else:
        exit_pips = 0.0

    legs = results.get("legs") if isinstance(results.get("legs"), dict) else {}
    core_pips = _safe_float((legs.get("core") or {}).get("pips"), default=exit_pips)
    runner_pips = _safe_float((legs.get("runner") or {}).get("pips"), default=0.0)
    weighted_pips = _safe_float(results.get("weighted_pips"), default=(0.8 * core_pips + 0.2 * runner_pips))
    if math.isfinite(weighted_pips):
        exit_pips = weighted_pips

    core_leg = legs.get("core") or {}
    runner_leg = legs.get("runner") or {}

    core_hold_sec = _safe_float(core_leg.get("hold_sec"), default=hold_sec)
    runner_hold_sec = _safe_float(runner_leg.get("hold_sec"), default=hold_sec)
    core_mfe_pips = _safe_float(core_leg.get("mfe_pips"), default=float("nan"))
    core_mae_pips = _safe_float(core_leg.get("mae_pips"), default=float("nan"))
    runner_mfe_pips = _safe_float(runner_leg.get("mfe_pips"), default=float("nan"))
    runner_mae_pips = _safe_float(runner_leg.get("mae_pips"), default=float("nan"))

    core_capture_pct = _safe_float(core_leg.get("capture"), default=float("nan"))
    runner_capture_pct = _safe_float(runner_leg.get("capture"), default=float("nan"))
    core_capture_pct = core_capture_pct if math.isfinite(core_capture_pct) else None
    runner_capture_pct = runner_capture_pct if math.isfinite(runner_capture_pct) else None

    core_left_on_table = _safe_float(core_leg.get("left_on_table_pips"), default=float("nan"))
    runner_left_on_table = _safe_float(runner_leg.get("left_on_table_pips"), default=float("nan"))
    if not math.isfinite(core_left_on_table) and math.isfinite(core_mfe_pips):
        core_left_on_table = core_mfe_pips - core_pips
    if not math.isfinite(runner_left_on_table) and math.isfinite(runner_mfe_pips):
        runner_left_on_table = runner_mfe_pips - runner_pips

    core_exit_reason = str((core_leg.get("exit") or {}).get("reason") or "NO_EXIT")
    runner_exit_reason = str((runner_leg.get("exit") or {}).get("reason") or "NO_EXIT")
    scenario = str(results.get("scenario") or _scenario_from_filename(path.name))

    panic_recovery = core_leg.get("panic_recovery_pips") if isinstance(core_leg.get("panic_recovery_pips"), dict) else {}
    pr30 = _safe_float(panic_recovery.get("plus_30"), default=float("nan"))
    pr60 = _safe_float(panic_recovery.get("plus_60"), default=float("nan"))
    pr120 = _safe_float(panic_recovery.get("plus_120"), default=float("nan"))

    mfe_pips, mae_pips = _compute_mfe_mae_pips(direction, entry_price, instrument, rows)
    capture_pct = (exit_pips / mfe_pips) if mfe_pips > 0 else None

    armed_med, unarmed_med, cadence_switch = _compute_cadence(rows)

    better_delay, delta_delay = _compute_delay_sensitivity(
        direction=direction,
        entry_price=entry_price,
        exit_pair_eval_idx=exit_pair_eval_idx,
        exit_pips=exit_pips,
        instrument=instrument,
        rows=rows,
    )

    overshoot_peak_atr = _safe_float(runner_leg.get("overshoot_peak_atr"), default=float("nan"))
    atr_entry = _safe_float(trade.get("atr_entry"), default=float("nan"))
    max_overshoot_seen_pips = 0.0
    if math.isfinite(overshoot_peak_atr) and math.isfinite(atr_entry):
        max_overshoot_seen_pips = _pips_from_price_delta(instrument, overshoot_peak_atr * atr_entry)
    overshoot_pips_captured = max(0.0, float(runner_pips) if math.isfinite(runner_pips) else 0.0)
    overshoot_capture_rate = None
    if max_overshoot_seen_pips > 1e-9:
        overshoot_capture_rate = overshoot_pips_captured / max_overshoot_seen_pips

    pulse_wait_30_better: Optional[bool] = None
    if exit_reason == "PULSE_STALL_CAPTURE":
        d1 = delta_delay.get(1)
        if d1 is not None:
            pulse_wait_30_better = bool(d1 > 0.0)

    return TradeAudit(
        file=str(path),
        instrument=instrument,
        direction=direction,
        exit_reason=exit_reason,
        has_exit=has_exit,
        entry_tick_idx=entry_tick_idx,
        entry_ts=float(entry_ts) if math.isfinite(entry_ts) else 0.0,
        entry_price=float(entry_price) if math.isfinite(entry_price) else 0.0,
        exit_tick_idx=exit_tick_idx,
        exit_pair_eval_idx=exit_pair_eval_idx,
        exit_ts=float(exit_ts) if math.isfinite(exit_ts) else 0.0,
        exit_price=float(exit_price) if math.isfinite(exit_price) else 0.0,
        hold_sec=float(hold_sec),
        exit_pips=float(exit_pips),
        core_pips=float(core_pips) if math.isfinite(core_pips) else 0.0,
        runner_pips=float(runner_pips) if math.isfinite(runner_pips) else 0.0,
        weighted_pips=float(weighted_pips) if math.isfinite(weighted_pips) else float(exit_pips),
        core_hold_sec=float(core_hold_sec) if math.isfinite(core_hold_sec) else float(hold_sec),
        runner_hold_sec=float(runner_hold_sec) if math.isfinite(runner_hold_sec) else float(hold_sec),
        mfe_pips=float(mfe_pips),
        mae_pips=float(mae_pips),
        capture_pct=float(capture_pct) if capture_pct is not None and math.isfinite(capture_pct) else None,
        core_mfe_pips=float(core_mfe_pips) if math.isfinite(core_mfe_pips) else 0.0,
        core_mae_pips=float(core_mae_pips) if math.isfinite(core_mae_pips) else 0.0,
        runner_mfe_pips=float(runner_mfe_pips) if math.isfinite(runner_mfe_pips) else 0.0,
        runner_mae_pips=float(runner_mae_pips) if math.isfinite(runner_mae_pips) else 0.0,
        core_capture_pct=float(core_capture_pct) if core_capture_pct is not None else None,
        runner_capture_pct=float(runner_capture_pct) if runner_capture_pct is not None else None,
        core_left_on_table_pips=float(core_left_on_table) if math.isfinite(core_left_on_table) else 0.0,
        runner_left_on_table_pips=float(runner_left_on_table) if math.isfinite(runner_left_on_table) else 0.0,
        core_exit_reason=core_exit_reason,
        runner_exit_reason=runner_exit_reason,
        cadence_switch=bool(cadence_switch),
        armed_gap_median_ts=armed_med,
        unarmed_gap_median_ts=unarmed_med,
        better_if_delay=better_delay,
        delta_if_delay_pips=delta_delay,
        panic_recovery_plus_30_pips=pr30 if math.isfinite(pr30) else None,
        panic_recovery_plus_60_pips=pr60 if math.isfinite(pr60) else None,
        panic_recovery_plus_120_pips=pr120 if math.isfinite(pr120) else None,
        scenario=scenario,
        overshoot_peak_atr=float(overshoot_peak_atr) if math.isfinite(overshoot_peak_atr) else 0.0,
        overshoot_pips_captured=float(overshoot_pips_captured),
        max_overshoot_seen_pips=float(max_overshoot_seen_pips),
        overshoot_capture_rate=float(overshoot_capture_rate) if overshoot_capture_rate is not None else None,
        pulse_wait_30_better=pulse_wait_30_better,
    )


# =========================
# Aggregation across all runs
# =========================
def aggregate(audits: List[TradeAudit]) -> Dict[str, Any]:
    total_trades = len(audits)
    if total_trades == 0:
        return {
            "summary": {
                "total_trades": 0,
                "avg_pips_per_trade": 0.0,
                "std_pips": 0.0,
                "median_hold_time_sec": 0.0,
                "avg_hold_time_sec": 0.0,
                "pips_per_hour": 0.0,
                "avg_capture_percent": 0.0,
                "cadence_switch_engagement_rate": 0.0,
                "early_exit_bias_percent_n1": 0.0,
            },
            "by_exit_reason": {},
            "cadence": {
                "armed_gap_median_ts": 0.0,
                "unarmed_gap_median_ts": 0.0,
                "switched_percent": 0.0,
                "switched_count": 0,
                "total": 0,
            },
            "exit_delay_sensitivity": {},
            "distributions": {
                "MFE": [],
                "MAE": [],
                "exit_pips": [],
                "hold_sec": [],
                "capture_percent": [],
            },
            "runs": [],
        }

    exit_pips_vals = [a.exit_pips for a in audits]
    weighted_pips_vals = [a.weighted_pips for a in audits]
    core_pips_vals = [a.core_pips for a in audits]
    runner_pips_vals = [a.runner_pips for a in audits]
    hold_vals = [a.hold_sec for a in audits if a.hold_sec > 0]
    core_hold_vals = [a.core_hold_sec for a in audits if a.core_hold_sec > 0]
    runner_hold_vals = [a.runner_hold_sec for a in audits if a.runner_hold_sec > 0]
    capture_vals = [a.capture_pct for a in audits if a.capture_pct is not None]
    core_capture_vals = [a.core_capture_pct for a in audits if a.core_capture_pct is not None]
    runner_capture_vals = [a.runner_capture_pct for a in audits if a.runner_capture_pct is not None]
    mfe_vals = [a.mfe_pips for a in audits]
    mae_vals = [a.mae_pips for a in audits]
    core_mfe_vals = [a.core_mfe_pips for a in audits]
    core_mae_vals = [a.core_mae_pips for a in audits]
    runner_mfe_vals = [a.runner_mfe_pips for a in audits]
    runner_mae_vals = [a.runner_mae_pips for a in audits]

    total_pips = sum(weighted_pips_vals)
    total_core_pips = sum(core_pips_vals)
    total_runner_pips = sum(runner_pips_vals)
    total_hold_sec = sum(hold_vals)
    total_core_hold_sec = sum(core_hold_vals)
    total_runner_hold_sec = sum(runner_hold_vals)
    pips_per_hour = (total_pips / (total_hold_sec / 3600.0)) if total_hold_sec > 0 else 0.0
    core_pips_per_hour = (total_core_pips / (total_core_hold_sec / 3600.0)) if total_core_hold_sec > 0 else 0.0
    runner_pips_per_hour = (total_runner_pips / (total_runner_hold_sec / 3600.0)) if total_runner_hold_sec > 0 else 0.0

    reason_counts = Counter(a.exit_reason for a in audits)
    core_reason_counts = Counter(a.core_exit_reason for a in audits)
    runner_reason_counts = Counter(a.runner_exit_reason for a in audits)
    by_exit_reason: Dict[str, Dict[str, Any]] = {}
    for reason, count in reason_counts.items():
        group = [a for a in audits if a.exit_reason == reason]
        by_exit_reason[reason] = {
            "count": int(count),
            "proportion": float(count / total_trades),
            "avg_pips": _mean(g.exit_pips for g in group),
            "avg_hold_sec": _mean(g.hold_sec for g in group),
            "avg_capture_percent": _mean(g.capture_pct for g in group if g.capture_pct is not None),
            "avg_mfe_pips": _mean(g.mfe_pips for g in group),
            "avg_mae_pips": _mean(g.mae_pips for g in group),
        }

    panic_neg = sum(abs(a.weighted_pips) for a in audits if a.weighted_pips < 0 and (a.core_exit_reason == "PANIC_EXIT" or a.runner_exit_reason == "PANIC_EXIT" or a.exit_reason == "PANIC_EXIT"))
    all_neg = sum(abs(a.weighted_pips) for a in audits if a.weighted_pips < 0)
    panic_dominance = (panic_neg / all_neg) if all_neg > 0 else 0.0

    median_weighted_pph = _percentile(
        [
            (a.weighted_pips / (a.hold_sec / 3600.0)) if a.hold_sec > 0 else 0.0
            for a in audits
        ],
        0.50,
    )

    regret_values = {
        "plus_30": _finite(a.panic_recovery_plus_30_pips for a in audits),
        "plus_60": _finite(a.panic_recovery_plus_60_pips for a in audits),
        "plus_120": _finite(a.panic_recovery_plus_120_pips for a in audits),
    }

    switched_count = sum(1 for a in audits if a.cadence_switch)
    switch_rate = float(switched_count / total_trades)

    # Delay-sensitivity aggregation.
    better_counts: Dict[int, int] = {h: 0 for h in DELAY_HORIZONS}
    compare_counts: Dict[int, int] = {h: 0 for h in DELAY_HORIZONS}
    avg_delta_sources: Dict[int, List[float]] = {h: [] for h in DELAY_HORIZONS}

    for a in audits:
        for h in DELAY_HORIZONS:
            delta = a.delta_if_delay_pips.get(h)
            if delta is None:
                continue
            compare_counts[h] += 1
            avg_delta_sources[h].append(float(delta))
            if a.better_if_delay.get(h, False):
                better_counts[h] += 1

    exit_delay_sensitivity: Dict[str, Any] = {}
    for h in DELAY_HORIZONS:
        den = compare_counts[h]
        exit_delay_sensitivity[f"n_plus_{h}"] = {
            "percent_better": float(better_counts[h] / den) if den else 0.0,
            "count_better": int(better_counts[h]),
            "count_compared": int(den),
            "avg_delta_pips": _mean(avg_delta_sources[h]),
        }

    early_exit_bias_n1 = float(better_counts[1] / compare_counts[1]) if compare_counts[1] else 0.0

    report = {
        "AEE_SCORECARD": {
            "weighted_pips_per_hour": float(pips_per_hour),
            "median_weighted_pips_per_hour": float(median_weighted_pph),
            "clip_metrics": {
                f"n_plus_{h}": {
                    "better_percent": float((better_counts[h] / compare_counts[h]) if compare_counts[h] else 0.0),
                    "avg_delta_pips": _mean(avg_delta_sources[h]),
                }
                for h in DELAY_HORIZONS
            },
            "tail_loss": {
                "p95": _percentile(weighted_pips_vals, 0.05),
                "p99": _percentile(weighted_pips_vals, 0.01),
            },
            "hold_distribution_sec": {
                "p50": _percentile(hold_vals, 0.50),
                "p75": _percentile(hold_vals, 0.75),
                "p90": _percentile(hold_vals, 0.90),
            },
            "exit_mix_by_reason": {
                k: {
                    "count": int(v),
                    "proportion": float(v / total_trades),
                }
                for k, v in reason_counts.items()
            },
            "exit_mix_by_leg": {
                "core": {k: int(v) for k, v in core_reason_counts.items()},
                "runner": {k: int(v) for k, v in runner_reason_counts.items()},
            },
        },
        "summary": {
            "total_trades": int(total_trades),
            "avg_pips_per_trade": _mean(weighted_pips_vals),
            "std_pips": _std(weighted_pips_vals),
            "median_hold_time_sec": _median(hold_vals),
            "avg_hold_time_sec": _mean(hold_vals),
            "pips_per_hour": float(pips_per_hour),
            "weighted_pips_per_hour": float(pips_per_hour),
            "core_pips_per_hour": float(core_pips_per_hour),
            "runner_pips_per_hour": float(runner_pips_per_hour),
            "median_hold_time_core_sec": _median(core_hold_vals),
            "median_hold_time_runner_sec": _median(runner_hold_vals),
            "avg_capture_percent": _mean(capture_vals),
            "avg_capture_percent_core": _mean(core_capture_vals),
            "avg_capture_percent_runner": _mean(runner_capture_vals),
            "cadence_switch_engagement_rate": switch_rate,
            "early_exit_bias_percent_n1": early_exit_bias_n1,
            "panic_dominance_index": panic_dominance,
            "pips_percentiles": {
                "p50": _percentile(weighted_pips_vals, 0.50),
                "p75": _percentile(weighted_pips_vals, 0.75),
                "p90": _percentile(weighted_pips_vals, 0.90),
                "p95": _percentile(weighted_pips_vals, 0.95),
            },
            "hold_percentiles_sec": {
                "p50": _percentile(hold_vals, 0.50),
                "p75": _percentile(hold_vals, 0.75),
                "p90": _percentile(hold_vals, 0.90),
                "p95": _percentile(hold_vals, 0.95),
            },
            "tail_losses": {
                "p95": _percentile(weighted_pips_vals, 0.05),
                "p99": _percentile(weighted_pips_vals, 0.01),
            },
        },
        "by_exit_reason": by_exit_reason,
        "by_exit_reason_leg": {
            "core": {k: {"count": int(v), "proportion": float(v / total_trades)} for k, v in core_reason_counts.items()},
            "runner": {k: {"count": int(v), "proportion": float(v / total_trades)} for k, v in runner_reason_counts.items()},
        },
        "cadence": {
            "armed_gap_median_ts": _median(a.armed_gap_median_ts for a in audits if a.armed_gap_median_ts is not None),
            "unarmed_gap_median_ts": _median(a.unarmed_gap_median_ts for a in audits if a.unarmed_gap_median_ts is not None),
            "switched_percent": switch_rate,
            "switched_count": int(switched_count),
            "total": int(total_trades),
        },
        "exit_delay_sensitivity": exit_delay_sensitivity,
        "early_exit_regret": {
            "plus_30_avg_pips": _mean(regret_values["plus_30"]),
            "plus_60_avg_pips": _mean(regret_values["plus_60"]),
            "plus_120_avg_pips": _mean(regret_values["plus_120"]),
            "plus_30_count": len(regret_values["plus_30"]),
            "plus_60_count": len(regret_values["plus_60"]),
            "plus_120_count": len(regret_values["plus_120"]),
        },
        "overshoot_capture": {
            "overshoot_pips_captured_avg": _mean(a.overshoot_pips_captured for a in audits),
            "max_overshoot_seen_pips_avg": _mean(a.max_overshoot_seen_pips for a in audits),
            "overshoot_capture_rate_avg": _mean(a.overshoot_capture_rate for a in audits if a.overshoot_capture_rate is not None),
        },
        "pulse_exit_quality": {
            "avg_pips": _mean(a.weighted_pips for a in audits if a.exit_reason == "PULSE_STALL_CAPTURE"),
            "p50_pips": _percentile([a.weighted_pips for a in audits if a.exit_reason == "PULSE_STALL_CAPTURE"], 0.50),
            "loss_rate": _mean([1.0 if a.weighted_pips < 0 else 0.0 for a in audits if a.exit_reason == "PULSE_STALL_CAPTURE"]),
            "wait_30_help_rate": _mean([1.0 if a.pulse_wait_30_better else 0.0 for a in audits if a.pulse_wait_30_better is not None]),
            "pulse_wait_30_better_count": int(sum(1 for a in audits if a.exit_reason == "PULSE_STALL_CAPTURE" and bool(a.pulse_wait_30_better))),
            "pulse_wait_30_better_avg_delta": _mean(
                a.delta_if_delay_pips.get(5)
                for a in audits
                if a.exit_reason == "PULSE_STALL_CAPTURE" and (a.delta_if_delay_pips.get(5) is not None) and (a.delta_if_delay_pips.get(5) > 0)
            ),
        },
        "scenario_breakdown": {
            s: {
                "count": int(len([a for a in audits if a.scenario == s])),
                "weighted_pips_per_hour": (
                    (sum(a.weighted_pips for a in audits if a.scenario == s) / (sum(a.hold_sec for a in audits if a.scenario == s) / 3600.0))
                    if sum(a.hold_sec for a in audits if a.scenario == s) > 0
                    else 0.0
                ),
                "avg_weighted_pips": _mean(a.weighted_pips for a in audits if a.scenario == s),
            }
            for s in sorted(set(a.scenario for a in audits))
        },
        "distributions": {
            "MFE": mfe_vals,
            "MAE": mae_vals,
            "core_MFE": core_mfe_vals,
            "core_MAE": core_mae_vals,
            "runner_MFE": runner_mfe_vals,
            "runner_MAE": runner_mae_vals,
            "exit_pips": weighted_pips_vals,
            "core_pips": core_pips_vals,
            "runner_pips": runner_pips_vals,
            "hold_sec": hold_vals,
            "core_hold_sec": core_hold_vals,
            "runner_hold_sec": runner_hold_vals,
            "capture_percent": capture_vals,
            "capture_percent_core": core_capture_vals,
            "capture_percent_runner": runner_capture_vals,
            "left_on_table_core": [a.core_left_on_table_pips for a in audits],
            "left_on_table_runner": [a.runner_left_on_table_pips for a in audits],
        },
        "runs": [
            {
                "file": a.file,
                "instrument": a.instrument,
                "direction": a.direction,
                "entry_tick_idx": a.entry_tick_idx,
                "entry_ts": a.entry_ts,
                "entry_price": a.entry_price,
                "exit_tick_idx": a.exit_tick_idx,
                "exit_pair_eval_idx": a.exit_pair_eval_idx,
                "exit_ts": a.exit_ts,
                "exit_price": a.exit_price,
                "exit_reason": a.exit_reason,
                "exit_pips": a.exit_pips,
                "core_pips": a.core_pips,
                "runner_pips": a.runner_pips,
                "weighted_pips": a.weighted_pips,
                "hold_sec": a.hold_sec,
                "core_hold_sec": a.core_hold_sec,
                "runner_hold_sec": a.runner_hold_sec,
                "mfe_pips": a.mfe_pips,
                "mae_pips": a.mae_pips,
                "capture_pct": a.capture_pct,
                "core_mfe_pips": a.core_mfe_pips,
                "core_mae_pips": a.core_mae_pips,
                "runner_mfe_pips": a.runner_mfe_pips,
                "runner_mae_pips": a.runner_mae_pips,
                "core_capture_pct": a.core_capture_pct,
                "runner_capture_pct": a.runner_capture_pct,
                "core_left_on_table_pips": a.core_left_on_table_pips,
                "runner_left_on_table_pips": a.runner_left_on_table_pips,
                "scenario": a.scenario,
                "overshoot_peak_atr": a.overshoot_peak_atr,
                "overshoot_pips_captured": a.overshoot_pips_captured,
                "max_overshoot_seen_pips": a.max_overshoot_seen_pips,
                "overshoot_capture_rate": a.overshoot_capture_rate,
                "core_exit_reason": a.core_exit_reason,
                "runner_exit_reason": a.runner_exit_reason,
                "cadence_switch": a.cadence_switch,
                "armed_gap_median_ts": a.armed_gap_median_ts,
                "unarmed_gap_median_ts": a.unarmed_gap_median_ts,
                "panic_recovery_plus_30_pips": a.panic_recovery_plus_30_pips,
                "panic_recovery_plus_60_pips": a.panic_recovery_plus_60_pips,
                "panic_recovery_plus_120_pips": a.panic_recovery_plus_120_pips,
                "delay_delta_pips": {f"n_plus_{k}": v for k, v in a.delta_if_delay_pips.items()},
                "delay_better": {f"n_plus_{k}": v for k, v in a.better_if_delay.items()},
            }
            for a in audits
        ],
    }

    return report


# =========================
# CLI entrypoint
# =========================
def find_input_files(input_dir: Path) -> List[Path]:
    # Prefer canonical naming, fallback to any JSON files.
    preferred = sorted(input_dir.glob("sim_results*.json"))
    if preferred:
        return preferred
    return sorted(input_dir.glob("*.json"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze many sim_results JSON files and write extraction_audit_report.json "
            "with pips/hour, cadence, delay sensitivity, and distributions."
        )
    )
    parser.add_argument("--input_dir", required=True, help="Directory containing sim_results JSON files")
    parser.add_argument("--output_file", required=True, help="Output JSON path for extraction audit report")
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"input_dir not found or not a directory: {input_dir}")

    files = find_input_files(input_dir)
    audits: List[TradeAudit] = []
    for path in files:
        audit = audit_file(path)
        if audit is not None:
            audits.append(audit)

    report = aggregate(audits)

    out_path = Path(args.output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"[sim_extraction_audit] analyzed_files={len(files)} valid_runs={len(audits)} output={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
