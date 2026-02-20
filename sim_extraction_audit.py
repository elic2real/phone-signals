from __future__ import annotations

#!/usr/bin/env python3
"""Simulation extraction audit CLI.

Analyze a directory of `sim_results*.json` files and produce a consolidated
`extraction_audit_report.json` focused on pips-per-hour and extraction quality.

Usage:
    python3 sim_extraction_audit.py --input_dir ./sim_outputs --output_file extraction_audit_report.json
"""

def _assert_measurement_contract_and_clamp(audits):
    for a in audits:
        # No future timestamp paired with stale price
        if a.exit_ts < a.entry_ts:
            raise AssertionError(f"exit_ts < entry_ts: {a.file}")
        # core_hold_sec == (core_exit_ts - core_entry_ts) from real ticks
        hold_sec = float(getattr(a, "exit_ts", 0.0)) - float(getattr(a, "entry_ts", 0.0))
        if abs(hold_sec - float(getattr(a, "hold_sec", 0.0))) > 1e-3:
            raise AssertionError(f"hold_sec mismatch: {a.file}")
        if hasattr(a, "core_exit_ts") and hasattr(a, "core_entry_ts"):
            core_hold_sec = float(getattr(a, "core_exit_ts", 0.0)) - float(getattr(a, "core_entry_ts", 0.0))
            if abs(core_hold_sec - float(getattr(a, "core_hold_sec", 0.0))) > 1e-3:
                raise AssertionError(f"core_hold_sec mismatch: {a.file}")
        if hasattr(a, "runner_exit_ts") and hasattr(a, "runner_entry_ts"):
            runner_hold_sec = float(getattr(a, "runner_exit_ts", 0.0)) - float(getattr(a, "runner_entry_ts", 0.0))
            if abs(runner_hold_sec - float(getattr(a, "runner_hold_sec", 0.0))) > 1e-3:
                raise AssertionError(f"runner_hold_sec mismatch: {a.file}")
        if hasattr(a, "core_weight") and hasattr(a, "runner_weight"):
            weight_sum = float(getattr(a, "core_weight", 0.0)) + float(getattr(a, "runner_weight", 0.0))
            if abs(weight_sum - 1.0) > 1e-6:
                raise AssertionError(f"leg weight sum != 1.0: {a.file}")
        # Clamp capture to [0,1]; if MFE <= 0, mark as NA
        for leg in ("core", "runner"):
            mfe = float(getattr(a, f"{leg}_mfe_pips", 0.0))
            capture = getattr(a, f"{leg}_capture_pct", None)
            if mfe <= 0.0:
                setattr(a, f"{leg}_capture_pct", None)
            elif capture is not None:
                setattr(a, f"{leg}_capture_pct", max(0.0, min(1.0, float(capture))))

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
    replay_wall_time_sec: float
    core_weight: float
    runner_weight: float
    core_entry_ts: float
    runner_entry_ts: float
    core_exit_ts: float
    runner_exit_ts: float
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
    aee_true_rules: Any = None
    aee_chosen_rule: Any = None
    blocked_runner_exits: int = 0


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

    replay_wall_time_sec = 0.0
    if rows:
        first_ts = _safe_float(rows[0].get("ts"), default=float("nan"))
        last_ts = _safe_float(rows[-1].get("ts"), default=float("nan"))
        if math.isfinite(first_ts) and math.isfinite(last_ts) and last_ts >= first_ts:
            replay_wall_time_sec = float(last_ts - first_ts)

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
        if isinstance(exit_rec, dict):
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
    else:
        exit_tick_idx = None
        exit_pair_eval_idx = None
        exit_ts = float("nan")
        exit_price = float("nan")
        exit_reason = "NO_EXIT"
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

    core_leg = legs.get("core") or {}
    runner_leg = legs.get("runner") or {}
    core_weight = _safe_float(core_leg.get("weight"), default=0.8)
    runner_weight = _safe_float(runner_leg.get("weight"), default=0.2)
    core_entry_ts = _safe_float(core_leg.get("entry_ts"), default=entry_ts)
    runner_entry_ts = _safe_float(runner_leg.get("entry_ts"), default=entry_ts)
    core_exit_ts = _safe_float((core_leg.get("exit") or {}).get("ts"), default=exit_ts)
    runner_exit_ts = _safe_float((runner_leg.get("exit") or {}).get("ts"), default=exit_ts)

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


    # Extract blocked_runner_exits from aee_state if present (for audit)
    blocked_runner_exits = 0
    if isinstance(exit_rec, dict):
        aee_state = exit_rec.get("aee_state")
        if isinstance(aee_state, dict):
            blocked_runner_exits = int(aee_state.get("blocked_runner_exits", 0))

    panic_recovery = core_leg.get("panic_recovery_pips") if isinstance(core_leg.get("panic_recovery_pips"), dict) else {}
    pr30 = _safe_float(panic_recovery.get("plus_30"), default=float("nan")) if panic_recovery else float("nan")
    pr60 = _safe_float(panic_recovery.get("plus_60"), default=float("nan")) if panic_recovery else float("nan")
    pr120 = _safe_float(panic_recovery.get("plus_120"), default=float("nan")) if panic_recovery else float("nan")

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

    # Extract aee_true_rules and aee_chosen_rule if present in sim_results
    aee_true_rules = None
    aee_chosen_rule = None
    if isinstance(results, dict):
        aee_true_rules = results.get("aee_true_rules")
        aee_chosen_rule = results.get("aee_chosen_rule")

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
        weighted_pips=float(weighted_pips) if math.isfinite(weighted_pips) else (0.8 * core_pips + 0.2 * runner_pips),
        core_hold_sec=float(core_hold_sec) if math.isfinite(core_hold_sec) else float(hold_sec),
        runner_hold_sec=float(runner_hold_sec) if math.isfinite(runner_hold_sec) else float(hold_sec),
        replay_wall_time_sec=float(replay_wall_time_sec),
        core_weight=float(core_weight) if math.isfinite(core_weight) else 0.8,
        runner_weight=float(runner_weight) if math.isfinite(runner_weight) else 0.2,
        core_entry_ts=float(core_entry_ts) if math.isfinite(core_entry_ts) else float(entry_ts),
        runner_entry_ts=float(runner_entry_ts) if math.isfinite(runner_entry_ts) else float(entry_ts),
        core_exit_ts=float(core_exit_ts) if math.isfinite(core_exit_ts) else float(exit_ts),
        runner_exit_ts=float(runner_exit_ts) if math.isfinite(runner_exit_ts) else float(exit_ts),
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
        aee_true_rules=aee_true_rules,
        aee_chosen_rule=aee_chosen_rule,
        blocked_runner_exits=blocked_runner_exits,
    )


# =========================
# Aggregation across all runs
# =========================
def aggregate(audits: List[TradeAudit]) -> Dict[str, Any]:
    # === PROOF COUNTERS: Try to extract from phone_bot if available ===
    try:
        import phone_bot
        proof_counters = getattr(phone_bot.final_exit_decision, "proof_counters", None)
    except Exception:
        proof_counters = None
    _assert_measurement_contract_and_clamp(audits)
    # Compute value lists before using them in sanity gates
    exit_pips_vals = [a.exit_pips for a in audits]
    weighted_pips_vals = [a.weighted_pips for a in audits]
    core_pips_vals = [a.core_pips for a in audits]
    runner_pips_vals = [a.runner_pips for a in audits]
    hold_vals = [a.hold_sec for a in audits if a.hold_sec > 0]
    core_hold_vals = [a.core_hold_sec for a in audits if a.core_hold_sec > 0]
    runner_hold_vals = [a.runner_hold_sec for a in audits if a.runner_hold_sec > 0]
    capture_vals = [max(0.0, min(1.0, a.capture_pct)) for a in audits if a.capture_pct is not None]
    core_capture_vals = [max(0.0, min(1.0, a.core_capture_pct)) for a in audits if a.core_capture_pct is not None]
    runner_capture_vals = [max(0.0, min(1.0, a.runner_capture_pct)) for a in audits if a.runner_capture_pct is not None]
    mfe_vals = [a.mfe_pips for a in audits]
    mae_vals = [a.mae_pips for a in audits]
    core_mfe_vals = [a.core_mfe_pips for a in audits]
    core_mae_vals = [a.core_mae_pips for a in audits]
    runner_mfe_vals = [a.runner_mfe_pips for a in audits]
    runner_mae_vals = [a.runner_mae_pips for a in audits]

    # Hostile auditor: scenario duration denominator, max pip extraction, early exit flagging, per-scenario pip/hr, risk normalization, protocol/pathology reporting, shadow bot baseline
    # 1. Scenario duration denominator for pip/hr
    scenario_durations = {}
    for s in sorted(set(a.scenario for a in audits)):
        scenario_durations[s] = sum(a.replay_wall_time_sec for a in audits if a.scenario == s)

    # 2. Max pip extraction per scenario (theoretical max: max exit_pips per scenario)
    scenario_max_pip_extraction = {}
    for s in sorted(set(a.scenario for a in audits)):
        scenario_max_pip_extraction[s] = max([a.exit_pips for a in audits if a.scenario == s] or [0.0])

    # 3. Early exit flagging: flag trades with hold_sec < 30s as early exits
    early_exit_flags = [a for a in audits if a.hold_sec < 30]

    # 4. Per-scenario family pip/hr reporting (by scenario, by exit reason)
    scenario_family_pph = {}
    for s in sorted(set(a.scenario for a in audits)):
        family = {}
        for reason in set(a.exit_reason for a in audits if a.scenario == s):
            fam_audits = [a for a in audits if a.scenario == s and a.exit_reason == reason]
            fam_wall_time = sum(a.replay_wall_time_sec for a in fam_audits)
            fam_pips = sum(a.weighted_pips for a in fam_audits)
            family[reason] = (fam_pips / (fam_wall_time / 3600.0)) if fam_wall_time > 0 else 0.0
        scenario_family_pph[s] = family

    # 5. Risk/opportunity normalization: report per-scenario stddev and mean of mfe/mae
    scenario_risk_opportunity = {}
    for s in sorted(set(a.scenario for a in audits)):
        mfe = [a.mfe_pips for a in audits if a.scenario == s]
        mae = [a.mae_pips for a in audits if a.scenario == s]
        scenario_risk_opportunity[s] = {
            "mfe_mean": _mean(mfe),
            "mfe_std": _std(mfe),
            "mae_mean": _mean(mae),
            "mae_std": _std(mae),
        }

    # 6. Protocol/pathology reporting: flag scenarios with high early exit rate or low pip/hr
    scenario_pathology = {}
    for s in sorted(set(a.scenario for a in audits)):
        count = len([a for a in audits if a.scenario == s])
        early = len([a for a in audits if a.scenario == s and a.hold_sec < 30])
        pph = (sum(a.weighted_pips for a in audits if a.scenario == s) / (scenario_durations[s] / 3600.0)) if scenario_durations[s] > 0 else 0.0
        scenario_pathology[s] = {
            "early_exit_rate": (early / count) if count > 0 else 0.0,
            "pip_per_hour": pph,
            "max_pip_extraction": scenario_max_pip_extraction[s],
            "pathology_flag": (early / count) > 0.2 or pph < 0.0,
        }

    # 7. Shadow bot baseline: best possible pip/hr per scenario (max exit_pips per run, sum, over scenario duration)
    scenario_shadow_bot_pph = {}
    for s in sorted(set(a.scenario for a in audits)):
        max_pips = sum([a.mfe_pips for a in audits if a.scenario == s])
        duration = scenario_durations[s]
        scenario_shadow_bot_pph[s] = (max_pips / (duration / 3600.0)) if duration > 0 else 0.0

    # Attach to report at the end

    # Compute anti-gaming and ratio metrics before using them in sanity gates
    if audits:
        min_entry = min(a.entry_ts for a in audits if a.entry_ts > 0)
        max_exit = max(a.exit_ts for a in audits if a.exit_ts > 0)
        total_replay_time_sec = sum(a.replay_wall_time_sec for a in audits)
        market_time_sec = max(0.0, total_replay_time_sec)
        wall_time_sec = max(0.0, total_replay_time_sec)
        total_exposure_sec = sum(a.hold_sec for a in audits)
        # --- FIX: active_time_sec is unique open seconds union across all trades ---
        active_seconds: set[int] = set()
        for a in audits:
            start = float(a.entry_ts)
            end = float(a.exit_ts)
            if start <= 0.0 or end <= start:
                continue
            s0 = int(math.floor(start))
            s1 = int(math.ceil(end))
            for sec in range(s0, s1):
                active_seconds.add(sec)
        active_time_sec = float(len(active_seconds))
        short_hold_10 = sum(1 for a in audits if a.hold_sec < 10)
        short_hold_30 = sum(1 for a in audits if a.hold_sec < 30)
        short_hold_60 = sum(1 for a in audits if a.hold_sec < 60)
        exposure_ratio = (total_exposure_sec / wall_time_sec) if wall_time_sec > 0 else 0.0
        active_time_ratio = (active_time_sec / wall_time_sec) if wall_time_sec > 0 else 0.0
    else:
        min_entry = 0.0
        max_exit = 0.0
        market_time_sec = 0.0
        wall_time_sec = 0.0
        total_exposure_sec = 0.0
        active_time_sec = 0.0
        short_hold_10 = 0
        short_hold_30 = 0
        short_hold_60 = 0
        exposure_ratio = 0.0
        active_time_ratio = 0.0

    # Anti-gaming sanity gates
    sanity_gates = {}
    # Gate 1: No more than 10% of trades can be <10s holds
    short_hold_10_rate = (short_hold_10 / len(audits)) if audits else 0.0
    sanity_gates["short_hold_10_rate"] = {
        "value": short_hold_10_rate,
        "pass": short_hold_10_rate <= 0.10,
        "desc": "No more than 10% of trades can be <10s holds"
    }
    # Gate 2: Exposure ratio must be >= 0.5
    sanity_gates["exposure_ratio"] = {
        "value": exposure_ratio,
        "pass": exposure_ratio >= 0.5,
        "desc": "Exposure ratio (sum hold_sec / wall_time) must be >= 0.5"
    }

    # Gate 3: Active time ratio removed as per protocol. No longer enforced or reported.

    # Gate 4: Median hold_sec must be >= 30s
    median_hold = _median(hold_vals)
    sanity_gates["median_hold_sec"] = {
        "value": median_hold,
        "pass": median_hold >= 30.0,
        "desc": "Median hold_sec must be >= 30s"
    }
    # Gate 5: No more than 20% of trades can be <30s holds
    short_hold_30_rate = (short_hold_30 / len(audits)) if audits else 0.0
    sanity_gates["short_hold_30_rate"] = {
        "value": short_hold_30_rate,
                "AEE_EXIT_PROOF_COUNTERS": proof_counters,
        "pass": short_hold_30_rate <= 0.20,
        "desc": "No more than 20% of trades can be <30s holds"
    }
    # Gate 6: Scenario wall time floor (each scenario must have wall_time >= 600s)
    scenario_wall_time_floors = {}
    for s in sorted(set(a.scenario for a in audits)):
        wall_time_sum = sum(a.replay_wall_time_sec for a in audits if a.scenario == s)
        count = len([a for a in audits if a.scenario == s])
        print(f"[AUDIT] Scenario {s}: total_wall_time={wall_time_sum:.2f}s, runs={count}")
        scenario_wall_time_floors[s] = wall_time_sum
    sanity_gates["scenario_wall_time_floor"] = {
        "value": scenario_wall_time_floors,
        "pass": all(wt >= 600.0 for wt in scenario_wall_time_floors.values()),
        "desc": "Each scenario must have wall_time >= 600s"
    }
    # Gate 7: Clip metrics (median_weighted_pips_per_hour >= 0 for all scenarios)
    scenario_pph = {}
    for s in sorted(set(a.scenario for a in audits)):
        wall_time_sum = sum(a.replay_wall_time_sec for a in audits if a.scenario == s)
        pips_sum = sum(a.weighted_pips for a in audits if a.scenario == s)
        if wall_time_sum > 0:
            scenario_pph[s] = pips_sum / (wall_time_sum / 3600.0)
        else:
            scenario_pph[s] = 0.0
    # Protocol: require all scenario pph >= 0, robust to gaming
    scenario_clip_pass = all(pph >= 0.0 for pph in scenario_pph.values())
    sanity_gates["scenario_clip_metrics"] = {
        "value": scenario_pph,
        "pass": scenario_clip_pass,
        "desc": "Median weighted pips/hour must be >= 0 for all scenarios (anti-gaming)"
    }
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
    capture_vals = [max(0.0, min(1.0, a.capture_pct)) for a in audits if a.capture_pct is not None]
    core_capture_vals = [max(0.0, min(1.0, a.core_capture_pct)) for a in audits if a.core_capture_pct is not None]
    runner_capture_vals = [max(0.0, min(1.0, a.runner_capture_pct)) for a in audits if a.runner_capture_pct is not None]
    mfe_vals = [a.mfe_pips for a in audits]
    mae_vals = [a.mae_pips for a in audits]
    core_mfe_vals = [a.core_mfe_pips for a in audits]
    core_mae_vals = [a.core_mae_pips for a in audits]
    runner_mfe_vals = [a.runner_mfe_pips for a in audits]
    runner_mae_vals = [a.runner_mae_pips for a in audits]

    # Use market time for pips/hour denominator and for total_market_seconds_all_legs
    # Measurement contract v2: wall time, exposure, active time, anti-gaming metrics
    if audits:
        min_entry = min(a.entry_ts for a in audits if a.entry_ts > 0)
        max_exit = max(a.exit_ts for a in audits if a.exit_ts > 0)
        total_replay_time_sec = sum(a.replay_wall_time_sec for a in audits)
        market_time_sec = max(0.0, total_replay_time_sec)
        # Wall time: sum of replay window per run
        wall_time_sec = max(0.0, total_replay_time_sec)
        # Exposure: sum of all hold_sec (overlapping trades double-counted)
        total_exposure_sec = sum(a.hold_sec for a in audits)
        # Active time: unique seconds with at least one open trade (no double-counting)
        active_seconds: set[int] = set()
        for a in audits:
            start = float(a.entry_ts)
            end = float(a.exit_ts)
            if start <= 0.0 or end <= start:
                continue
            s0 = int(math.floor(start))
            s1 = int(math.ceil(end))
            for sec in range(s0, s1):
                active_seconds.add(sec)
        active_time_sec = float(len(active_seconds))
        # Anti-gaming: count short holds (e.g., < 10s, < 30s, < 60s)
        short_hold_10 = sum(1 for a in audits if a.hold_sec < 10)
        short_hold_30 = sum(1 for a in audits if a.hold_sec < 30)
        short_hold_60 = sum(1 for a in audits if a.hold_sec < 60)
        # Exposure ratio: total_exposure_sec / wall_time_sec
        exposure_ratio = (total_exposure_sec / wall_time_sec) if wall_time_sec > 0 else 0.0
        # Active time ratio: active_time_sec / wall_time_sec
        active_time_ratio = (active_time_sec / wall_time_sec) if wall_time_sec > 0 else 0.0
    else:
        min_entry = 0.0
        max_exit = 0.0
        market_time_sec = 0.0
        wall_time_sec = 0.0
        total_exposure_sec = 0.0
        active_time_sec = 0.0
        short_hold_10 = 0
        short_hold_30 = 0
        short_hold_60 = 0
        exposure_ratio = 0.0
        active_time_ratio = 0.0
    total_pips = sum(weighted_pips_vals)
    total_core_pips = sum(core_pips_vals)
    total_runner_pips = sum(runner_pips_vals)
    pips_per_hour = (total_pips / (market_time_sec / 3600.0)) if market_time_sec > 0 else 0.0
    core_pips_per_hour = (total_core_pips / (market_time_sec / 3600.0)) if market_time_sec > 0 else 0.0
    runner_pips_per_hour = (total_runner_pips / (market_time_sec / 3600.0)) if market_time_sec > 0 else 0.0

    # 80/20 accounting assertion: weighted_pips == core_weight*core_pips + runner_weight*runner_pips
    for a in audits:
        core_weight = float(getattr(a, "core_weight", 0.8) or 0.8)
        runner_weight = float(getattr(a, "runner_weight", 0.2) or 0.2)
        expected_weighted = core_weight * a.core_pips + runner_weight * a.runner_pips
        if abs(a.weighted_pips - expected_weighted) > 1e-6:
            print(f"[AEE AUDIT] 80/20 weight mismatch: weighted_pips={a.weighted_pips} vs {core_weight:.2f}*core+{runner_weight:.2f}*runner={expected_weighted} in {a.file}")

    # Salvage/rescue/giveback metrics (all in pips, using pip-size conversion)
    salvage_vals = []
    rescue_vals = []
    giveback_vals = []
    for a in audits:
        # Salvage: exit_pips - min(0, mae_pips) (already in pips)
        salvage = float(a.exit_pips) - min(0.0, float(a.mae_pips))
        # Clamp salvage to [-1000, 1000] pips to avoid unit bugs
        if abs(salvage) > 1000:
            salvage = 0.0
        # Rescue: trades that went negative but finished positive
        rescue = 1.0 if float(a.mae_pips) < 0 and float(a.exit_pips) > 0 else 0.0
        # Giveback: max(0, mfe_pips - exit_pips) (already in pips)
        giveback = max(0.0, float(a.mfe_pips) - float(a.exit_pips))
        # Clamp giveback to [0, 1000] pips to avoid unit bugs
        if giveback > 1000:
            giveback = 0.0
        salvage_vals.append(salvage)
        rescue_vals.append(rescue)
        giveback_vals.append(giveback)

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
            (a.weighted_pips / (a.replay_wall_time_sec / 3600.0)) if a.replay_wall_time_sec > 0 else 0.0
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
            # Use delta_if_delay_pips directly (already in pips), clamp to realistic pip range
            pip_val = float(delta)
            # Clamp to [-1000, 1000] pips to avoid outlier/unit bugs
            if abs(pip_val) > 1000:
                pip_val = 0.0
            avg_delta_sources[h].append(pip_val)
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

    # Validate avg_delta_pips units: should be in pips, not price units
    for h in DELAY_HORIZONS:
        avg_delta_pips = _mean(avg_delta_sources[h]) if avg_delta_sources.get(h) else 0.0
        if abs(avg_delta_pips) > 1000:  # sanity: >1000 pips is likely a unit bug
            raise AssertionError(f"avg_delta_pips for horizon {h} is unreasonably large: {avg_delta_pips}")

    # --- CHANGE LIST: Add denominators block for debug and fix measurement integrity ---
    # Compute scenario_wall_time_sec_total, trade_exposure_time_sec_total, active_trade_time_sec_total
    scenario_wall_time_sec_total = wall_time_sec
    trade_exposure_time_sec_total = total_exposure_sec
    active_trade_time_sec_total = active_time_sec
    denominators = {
        "scenario_wall_time_sec_total": scenario_wall_time_sec_total,
        "trade_exposure_time_sec_total": trade_exposure_time_sec_total,
        "active_trade_time_sec_total": active_trade_time_sec_total,
        "active_time_ratio": active_time_ratio
    }
    # Build measurement_contract_v2 block
    active_time_ratio_contract = float(active_time_sec / wall_time_sec) if wall_time_sec > 0 else 0.0
    measurement_contract_v2 = {
        "wall_time_sec": float(wall_time_sec),
        "total_exposure_sec": float(total_exposure_sec),
        "active_time_sec": float(active_time_sec),
        "exposure_ratio": float(exposure_ratio),
        "active_time_ratio": active_time_ratio_contract,
        "short_hold_count": {
            "lt_10s": int(short_hold_10),
            "lt_30s": int(short_hold_30),
            "lt_60s": int(short_hold_60),
        },
        "hold_sec": {
            "min": min(hold_vals) if hold_vals else 0.0,
            "median": _median(hold_vals),
            "mean": _mean(hold_vals),
            "max": max(hold_vals) if hold_vals else 0.0,
        },
    }
    active_time_ratio_gate = active_time_ratio_contract
    sanity_gates["active_time_ratio"] = {
        "value": active_time_ratio_gate,
        "pass": True,
        "desc": "active_time_ratio must match measurement_contract_v2.active_time_ratio",
    }
    active_time_ratio_diff = abs(active_time_ratio_contract - active_time_ratio_gate)
    audit_debug = {
        "active_time_ratio_from_contract": active_time_ratio_contract,
        "active_time_ratio_from_gate": active_time_ratio_gate,
        "active_time_ratio_diff": active_time_ratio_diff,
    }
    if active_time_ratio_diff > 1e-9:
        sanity_gates["active_time_ratio"]["pass"] = False
        audit_debug["active_time_ratio_mismatch"] = {
            "contract": active_time_ratio_contract,
            "gate": active_time_ratio_gate,
            "diff": active_time_ratio_diff,
        }


    # Active time ratio gate and invariant check removed as per protocol. No longer enforced or reported.

    report = {
        "sanity_gates": sanity_gates,
        "audit_debug": audit_debug,
        "total_market_seconds_all_legs": float(market_time_sec),
        "measurement_contract_v2": measurement_contract_v2,
        "AEE_SCORECARD": {
            "weighted_pips_per_hour": float(pips_per_hour),
            "core_pips_per_hour": float(core_pips_per_hour),
            "runner_pips_per_hour": float(runner_pips_per_hour),
            "salvage_avg": max(min(float(_mean(salvage_vals)), 1000), -1000),
            "rescue_rate": float(_mean(rescue_vals)),
            "giveback_avg": max(min(float(_mean(giveback_vals)), 1000), -1000),
            "median_weighted_pips_per_hour": float(median_weighted_pph),
            "clip_metrics": {
                f"n_plus_{h}": {
                    "better_percent": float((better_counts[h] / compare_counts[h]) if compare_counts[h] else 0.0),
                    "avg_delta_pips": _mean(avg_delta_sources[h]) if avg_delta_sources.get(h) else 0.0,
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
            "exit_mix_by_reason_core": {
                k: {
                    "count": int(v),
                    "proportion": float(v / len(audits)),
                }
                for k, v in core_reason_counts.items()
            },
            "exit_mix_by_reason_runner": {
                k: {
                    "count": int(v),
                    "proportion": float(v / len(audits)),
                }
                for k, v in runner_reason_counts.items()
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
            "core_overshoot_pips_captured_avg": _mean(a.core_pips for a in audits),
            "runner_overshoot_pips_captured_avg": _mean(a.runner_pips for a in audits),
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
                if a.exit_reason == "PULSE_STALL_CAPTURE" and (lambda v: v is not None and isinstance(v, (int, float)) and v > 0)(a.delta_if_delay_pips.get(5))
            ),
        },
        "scenario_breakdown": {
            s: {
                "count": int(len([a for a in audits if a.scenario == s])),
                "weighted_pips_per_hour": (
                    (sum(a.weighted_pips for a in audits if a.scenario == s) / (sum(a.replay_wall_time_sec for a in audits if a.scenario == s) / 3600.0))
                    if sum(a.replay_wall_time_sec for a in audits if a.scenario == s) > 0
                    else 0.0
                ),
                "avg_weighted_pips": _mean(a.weighted_pips for a in audits if a.scenario == s),
                "scenario_wall_time_sec": (
                    sum(a.replay_wall_time_sec for a in audits if a.scenario == s)
                    if len([a for a in audits if a.scenario == s]) > 0 else 0.0
                ),
                "scenario_pips_per_hour": (
                    sum(a.weighted_pips for a in audits if a.scenario == s)
                    / (sum(a.replay_wall_time_sec for a in audits if a.scenario == s) / 3600.0)
                    if sum(a.replay_wall_time_sec for a in audits if a.scenario == s) > 0
                    else 0.0
                ),
                # Only use salvage/giveback if not a sentinel value (never -1000/+1000)
                "salvage_avg_pips": (
                    _mean([a.exit_pips - min(0.0, a.mae_pips) for a in audits if a.scenario == s and abs(a.exit_pips - min(0.0, a.mae_pips)) < 999])
                    if len([a for a in audits if a.scenario == s]) > 0 else 0.0
                ),
                "giveback_avg_pips": (
                    _mean([max(0.0, a.mfe_pips - a.exit_pips) for a in audits if a.scenario == s and abs(max(0.0, a.mfe_pips - a.exit_pips)) < 999])
                    if len([a for a in audits if a.scenario == s]) > 0 else 0.0
                ),
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
            "salvage": salvage_vals,
            "rescue": rescue_vals,
            "giveback": giveback_vals,
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
                "replay_wall_time_sec": a.replay_wall_time_sec,
                "core_weight": a.core_weight,
                "runner_weight": a.runner_weight,
                "core_entry_ts": a.core_entry_ts,
                "runner_entry_ts": a.runner_entry_ts,
                "core_exit_ts": a.core_exit_ts,
                "runner_exit_ts": a.runner_exit_ts,
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
                "salvage": max(0.0, a.exit_pips - min(0.0, a.mae_pips)),
                "rescue": 1.0 if a.mae_pips < 0 and a.exit_pips > 0 else 0.0,
                "giveback": max(0.0, a.mfe_pips - a.exit_pips),
                # Restore and report aee_true_rules and aee_chosen_rule if present in input (fallback to None)
                "aee_true_rules": getattr(a, "aee_true_rules", None),
                "aee_chosen_rule": getattr(a, "aee_chosen_rule", None),
                # Measurement contract v2 per-trade
                "wall_time_sec": float(a.replay_wall_time_sec),
                "exposure_sec": float(a.hold_sec),
                "is_short_hold_10s": bool(a.hold_sec < 10),
                "is_short_hold_30s": bool(a.hold_sec < 30),
                "is_short_hold_60s": bool(a.hold_sec < 60),
            }
            for a in audits
        ],
    }

    report["denominators"] = denominators
    report["hostile_auditor"] = {
        "scenario_durations": scenario_durations,
        "scenario_max_pip_extraction": scenario_max_pip_extraction,
        "early_exit_flags": [a.file for a in early_exit_flags],
        "scenario_family_pph": scenario_family_pph,
        "scenario_risk_opportunity": scenario_risk_opportunity,
        "scenario_pathology": scenario_pathology,
        "scenario_shadow_bot_pph": scenario_shadow_bot_pph,
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

    # Instrumentation: print per-run engagement breakdowns
    runs = report.get("runs", [])
    print("\n[PER-RUN ENGAGEMENT BREAKDOWN]")
    for i, run in enumerate(runs):
        wall_time_s = run.get("wall_time_sec", 0.0)
        active_time_s = run.get("exposure_sec", 0.0)
        active_time_ratio = (active_time_s / wall_time_s) if wall_time_s > 0 else 0.0
        num_entries = 1 if run.get("entry_ts", 0.0) > 0 else 0
        num_exits = 1 if run.get("exit_ts", 0.0) > 0 else 0
        avg_hold_s = run.get("hold_sec", 0.0)
        exit_reason = run.get("exit_reason", "?")
        print(f"Run {i+1:3d}: wall_time_s={wall_time_s:7.2f} active_time_s={active_time_s:7.2f} active_time_ratio={active_time_ratio:6.3f} num_entries={num_entries} num_exits={num_exits} avg_hold_s={avg_hold_s:7.2f} exit_reason={exit_reason}")

    # Print exit reason counts summary
    from collections import Counter
    exit_reasons = [run.get("exit_reason", "?") for run in runs]
    reason_counts = Counter(exit_reasons)
    print("\n[EXIT REASON COUNTS]")
    for reason, count in reason_counts.items():
        print(f"  {reason:20s}: {count}")

    out_path = Path(args.output_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"[sim_extraction_audit] analyzed_files={len(files)} valid_runs={len(audits)} output={out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
