#!/usr/bin/env python3
"""Stability-first AEE optimizer with session 5+6 outputs.

Session 5:
- distribution-first evaluation at 25/100/300 runs for each candidate
- stable-candidate gating against baseline
- Pareto front + full archive + delta table

Session 6:
- materialize best/worst run artifacts
- failure atlas for worst runs
- stable_candidates.json export
"""

from __future__ import annotations

import argparse
import json
import math
import random
import statistics
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

import phone_bot
from sim_harness import SimEnvironment, Tick, create_default_trade
from tick_generator import sample_scenario_mix

PARAM_BOUNDS: Dict[str, Tuple[float, float]] = {
    "ENERGY_CPS_THRESHOLD": (0.40, 0.68),
    "ENERGY_DHR_PANIC": (0.55, 0.88),
    "PANIC_CONFIRM_MIN_HITS": (2, 4),
    "PANIC_DEBOUNCE_SEC": (0.5, 2.0),
    "MIN_EVAL_AGE_SEC": (12.0, 60.0),
    "MIN_EVAL_SAMPLES": (5, 24),
    "ENERGY_RSS_PROMOTE": (0.50, 0.84),
    "RUNNER_PROMOTE_RSS_MIN": (0.50, 0.84),
    "RUNNER_PROMOTE_LED_MIN": (0.35, 0.80),
    "RUNNER_PROMOTE_EPI_MIN": (0.35, 0.80),
    "WPD_CHOP_THRESHOLD": (0.40, 0.82),
    "LED_EXPANSION_THRESHOLD": (0.35, 0.80),
    "ENERGY_GE_DECAY_THRESHOLD": (0.18, 0.42),
    "ENERGY_DHR_DECAY_THRESHOLD": (0.45, 0.72),
    "ENERGY_EPI_DECAY_THRESHOLD": (0.30, 0.60),
    "DECAY_NO_NEW_HIGH_SEC": (4.0, 20.0),
    "NEG_EXIT_CONFIRM_WINDOW_SEC": (1.0, 8.0),
    "NEG_EXIT_CONFIRM_MIN_HITS": (1, 6),
}

CONSTRAINTS = {
    "median_hold_time_sec_min": 45.0,
    "panic_rate_max": 0.25,
    "p95_loss_min": -6.0,
    "clip_rate_n_plus_5_max": 0.55,
    "runner_positive_rate_min": 0.45,
}

RUN_DISTRIBUTIONS: Tuple[int, ...] = (25, 100, 300)


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        out = float(v)
        if math.isfinite(out):
            return out
    except Exception:
        pass
    return default


def _percentile(values: List[float], q: float) -> float:
    vals = sorted(v for v in values if math.isfinite(v))
    if not vals:
        return 0.0
    if len(vals) == 1:
        return vals[0]
    qn = max(0.0, min(1.0, q))
    pos = qn * (len(vals) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return vals[lo]
    frac = pos - lo
    return vals[lo] + (vals[hi] - vals[lo]) * frac


def _variance(vals: List[float]) -> float:
    x = [v for v in vals if math.isfinite(v)]
    if len(x) < 2:
        return 0.0
    return float(statistics.pvariance(x))


def _skewness(vals: List[float]) -> float:
    x = [v for v in vals if math.isfinite(v)]
    n = len(x)
    if n < 3:
        return 0.0
    mean = sum(x) / n
    var = sum((v - mean) ** 2 for v in x) / n
    if var <= 1e-12:
        return 0.0
    std = math.sqrt(var)
    m3 = sum((v - mean) ** 3 for v in x) / n
    return float(m3 / (std ** 3))


def _entropy(counter: Counter[str]) -> float:
    tot = sum(counter.values())
    if tot <= 0:
        return 0.0
    e = 0.0
    for c in counter.values():
        p = c / tot
        if p > 0:
            e -= p * math.log(p)
    return float(e)


def _mean(values: List[float]) -> float:
    vals = [v for v in values if math.isfinite(v)]
    return float(sum(vals) / len(vals)) if vals else 0.0


def set_params(params: Dict[str, float]) -> None:
    for k, v in params.items():
        setattr(phone_bot, k, v)


def snapshot_params() -> Dict[str, float]:
    return {k: float(getattr(phone_bot, k)) for k in PARAM_BOUNDS}


def sample_params(rng: random.Random) -> Dict[str, float]:
    params: Dict[str, float] = {}
    for k, (lo, hi) in PARAM_BOUNDS.items():
        if float(lo).is_integer() and float(hi).is_integer():
            params[k] = int(rng.randint(int(lo), int(hi)))
        else:
            params[k] = float(rng.uniform(float(lo), float(hi)))
    return params


def mutate_params(base: Dict[str, float], rng: random.Random, scale: float = 0.25) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for k, (lo, hi) in PARAM_BOUNDS.items():
        span = float(hi - lo)
        center = float(base.get(k, lo))
        if float(lo).is_integer() and float(hi).is_integer():
            step = max(1, int(round(span * scale)))
            out[k] = int(max(int(lo), min(int(hi), int(center) + rng.randint(-step, step))))
        else:
            out[k] = float(max(lo, min(hi, center + rng.uniform(-span * scale, span * scale))))
    return out


def run_one(seed: int, bucket_sec: float = 5.0) -> Dict[str, Any]:
    rng = random.Random(seed)
    scenario, ticks = sample_scenario_mix(rng=rng)
    ticks.sort(key=lambda x: x["ts"])

    ticks_by_inst: Dict[str, List[Tick]] = {}
    for row in ticks:
        t = Tick(instrument=str(row["instrument"]), ts=float(row["ts"]), bid=float(row["bid"]), ask=float(row["ask"]))
        ticks_by_inst.setdefault(t.instrument, []).append(t)

    pair = next(iter(ticks_by_inst.keys()), "EUR_USD")
    trade = create_default_trade(pair, "LONG", ticks_by_inst, atr_pips=10.0, tp_atr=1.2)
    env = SimEnvironment(instruments=list(ticks_by_inst.keys()), ticks_by_inst=ticks_by_inst, bucket_sec=bucket_sec)
    try:
        out = env.run_aee_replay(trade=trade, speed_class="MED")
        out["scenario"] = scenario
        return out
    finally:
        env.restore_live_wiring()


def _compute_delay_from_result(result: Dict[str, Any]) -> Dict[str, Dict[str, float]]:
    recs = [r for r in (result.get("tick_records") or []) if isinstance(r, dict) and r.get("pair_eval_idx") is not None]
    if not recs:
        return {f"n_plus_{h}": {"clip_rate": 0.0, "clip_gain": 0.0, "count": 0} for h in (1, 2, 5)}

    by_idx = {int(r["pair_eval_idx"]): r for r in recs}
    trade = result.get("trade") or {}
    entry = _safe_float(trade.get("entry"), 0.0)
    pair = str(trade.get("pair") or "EUR_USD")
    direction = str(trade.get("dir") or "LONG").upper()
    pip = float(phone_bot.pip_size(pair)) or 0.0001

    ex = result.get("exit") or {}
    exit_idx = ex.get("pair_eval_idx")
    if exit_idx is None:
        return {f"n_plus_{h}": {"clip_rate": 0.0, "clip_gain": 0.0, "count": 0} for h in (1, 2, 5)}

    actual = _safe_float(result.get("weighted_pips"), 0.0)
    out: Dict[str, Dict[str, float]] = {}
    for h in (1, 2, 5):
        delayed = by_idx.get(int(exit_idx) + h)
        if delayed is None:
            out[f"n_plus_{h}"] = {"clip_rate": 0.0, "clip_gain": 0.0, "count": 0}
            continue
        px = _safe_float(delayed.get("bid") if direction == "LONG" else delayed.get("ask"), _safe_float(delayed.get("mid"), entry))
        delayed_pips = ((px - entry) / pip) if direction == "LONG" else ((entry - px) / pip)
        gain = delayed_pips - actual
        out[f"n_plus_{h}"] = {
            "clip_rate": 1.0 if gain > 0 else 0.0,
            "clip_gain": gain if gain > 0 else 0.0,
            "count": 1,
        }
    return out


def evaluate_config(params: Dict[str, float], n_runs: int, start_seed: int, bucket_sec: float, collect_runs: bool = False) -> Dict[str, Any]:
    # --- Measurement contract v2 and anti-gaming metrics (protocol-compliant, robust to gaming) ---
    # Use sim_extraction_audit.py logic: penalize short-hold dominance, low exposure/active time ratios, failing audit sanity gates
    # For each run, extract measurement_contract_v2 and sanity_gates if present
    run_summaries: List[Dict[str, Any]] = []
    for i in range(n_runs):
        run_seed = start_seed + i
        res = run_one(seed=run_seed, bucket_sec=bucket_sec)
        wp = _safe_float(res.get("weighted_pips"), 0.0)
        weighted.append(wp)
        pph = _safe_float(((res.get("pips_per_hour") or {}).get("weighted")), 0.0)
        weighted_pph.append(pph)
        core_pph.append(_safe_float(((res.get("pips_per_hour") or {}).get("core")), 0.0))
        runner_pph.append(_safe_float(((res.get("pips_per_hour") or {}).get("runner")), 0.0))
        legs = res.get("legs") if isinstance(res.get("legs"), dict) else {}
        core_vals.append(_safe_float((legs.get("core") or {}).get("pips"), 0.0))
        rp = _safe_float((legs.get("runner") or {}).get("pips"), 0.0)
        runner_vals.append(rp)
        if rp > 0:
            runner_positive += 1
        ex = res.get("exit") or {}
        reason = str(ex.get("exit_reason") or ex.get("reason") or "UNKNOWN")
        exit_reasons[reason] += 1
        total_exits += 1
        if reason == "PANIC_EXIT":
            panic_count += 1
            if wp < 0:
                panic_losses.append(abs(wp))
        if wp < 0:
            neg_losses.append(abs(wp))
        tr = res.get("trade") or {}
        hold = _safe_float(ex.get("ts"), 0.0) - _safe_float(tr.get("ts"), 0.0)
        if hold > 0:
            hold_vals.append(hold)
        rr = str(((legs.get("runner") or {}).get("exit") or {}).get("reason") or "")
        if rr == "RUNNER_ENERGY_DECAY":
            runner_decay_count += 1
        recs = [r for r in (res.get("tick_records") or []) if isinstance(r, dict) and r.get("pair_eval_idx") is not None]
        if recs:
            last = recs[-1]
            m = last.get("metrics") or {}
            p = _safe_float(m.get("progress"), 0.0)
            pk = _safe_float(m.get("peak_progress"), p)
            progress_at_exit.append(p)
            peak_progress.append(pk)
            if pk > 1e-6:
                giveback_ratios.append(p / pk)
            cps = _safe_float(m.get("cps"), 0.0)
            dhr = _safe_float(m.get("dhr"), 0.0)
            ge = _safe_float(m.get("ge"), 0.0)
            if cps < float(getattr(phone_bot, "ENERGY_CPS_THRESHOLD", 0.52)):
                cps_low_count += 1
            if dhr >= float(getattr(phone_bot, "ENERGY_DHR_DECAY_THRESHOLD", 0.58)):
                dhr_spike_count += 1
            if ge >= float(getattr(phone_bot, "ENERGY_GE_DECAY_THRESHOLD", 0.28)):
                ge_spike_count += 1
            if reason == "FAILED_TO_CONTINUE_DECAY" and cps < float(getattr(phone_bot, "ENERGY_CPS_THRESHOLD", 0.52)) and dhr >= float(getattr(phone_bot, "ENERGY_DHR_DECAY_THRESHOLD", 0.58)) and ge >= float(getattr(phone_bot, "ENERGY_GE_DECAY_THRESHOLD", 0.28)):
                energy_confirmed_decay += 1
        delays = _compute_delay_from_result(res)
        for k in delay_acc:
            delay_acc[k]["clip_hits"] += delays[k]["clip_rate"]
            delay_acc[k]["clip_gain"] += delays[k]["clip_gain"]
            delay_acc[k]["count"] += delays[k]["count"]
        if collect_runs:
            run_summaries.append(
                {
                    "seed": run_seed,
                    "scenario": str(res.get("scenario") or "unknown"),
                    "weighted_pips": wp,
                    "weighted_pips_per_hour": pph,
                    "core_pips": _safe_float((legs.get("core") or {}).get("pips"), 0.0),
                    "runner_pips": _safe_float((legs.get("runner") or {}).get("pips"), 0.0),
                    "exit_reason": reason,
                    "runner_exit_reason": str(((legs.get("runner") or {}).get("exit") or {}).get("reason") or "UNKNOWN"),
                    "result": res,
                }
            )
    # Now extract measurement_contracts and sanity_gates from run_summaries
    measurement_contracts = []
    sanity_gates_list = []
    for res in run_summaries:
        mc = None
        sg = None
        try:
            mc = (res.get("result") or {}).get("measurement_contract_v2")
            sg = (res.get("result") or {}).get("sanity_gates")
        except Exception:
            pass
        if mc:
            measurement_contracts.append(mc)
        if sg:
            sanity_gates_list.append(sg)
    # Aggregate anti-gaming metrics
    avg_exposure_ratio = _mean([mc.get("exposure_ratio", 0.0) for mc in measurement_contracts]) if measurement_contracts else 1.0
    avg_active_time_ratio = _mean([mc.get("active_time_ratio", 0.0) for mc in measurement_contracts]) if measurement_contracts else 1.0
    avg_short_hold_10_rate = _mean([mc.get("short_hold_count", {}).get("lt_10s", 0) / n_runs for mc in measurement_contracts]) if measurement_contracts else 0.0
    # Sanity gates: fail if any run fails any gate (robust to gaming)
    audit_sanity_pass = all(
        all(g.get("pass", True) for g in sg.values()) for sg in sanity_gates_list
    ) if sanity_gates_list else True
    set_params(params)

    weighted: List[float] = []
    weighted_pph: List[float] = []
    core_vals: List[float] = []
    runner_vals: List[float] = []
    core_pph: List[float] = []
    runner_pph: List[float] = []
    hold_vals: List[float] = []
    runner_positive = 0
    panic_count = 0
    panic_losses: List[float] = []
    neg_losses: List[float] = []
    runner_decay_count = 0
    total_exits = 0
    exit_reasons: Counter[str] = Counter()
    progress_at_exit: List[float] = []
    peak_progress: List[float] = []
    giveback_ratios: List[float] = []
    cps_low_count = 0
    dhr_spike_count = 0
    ge_spike_count = 0
    energy_confirmed_decay = 0

    delay_acc = {f"n_plus_{h}": {"clip_hits": 0.0, "clip_gain": 0.0, "count": 0} for h in (1, 2, 5)}
    run_summaries: List[Dict[str, Any]] = []

    for i in range(n_runs):
        run_seed = start_seed + i
        res = run_one(seed=run_seed, bucket_sec=bucket_sec)
        wp = _safe_float(res.get("weighted_pips"), 0.0)
        weighted.append(wp)

        pph = _safe_float(((res.get("pips_per_hour") or {}).get("weighted")), 0.0)
        weighted_pph.append(pph)
        core_pph.append(_safe_float(((res.get("pips_per_hour") or {}).get("core")), 0.0))
        runner_pph.append(_safe_float(((res.get("pips_per_hour") or {}).get("runner")), 0.0))

        legs = res.get("legs") if isinstance(res.get("legs"), dict) else {}
        core_vals.append(_safe_float((legs.get("core") or {}).get("pips"), 0.0))
        rp = _safe_float((legs.get("runner") or {}).get("pips"), 0.0)
        runner_vals.append(rp)
        if rp > 0:
            runner_positive += 1

        ex = res.get("exit") or {}
        reason = str(ex.get("exit_reason") or ex.get("reason") or "UNKNOWN")
        exit_reasons[reason] += 1
        total_exits += 1
        if reason == "PANIC_EXIT":
            panic_count += 1
            if wp < 0:
                panic_losses.append(abs(wp))

        if wp < 0:
            neg_losses.append(abs(wp))

        tr = res.get("trade") or {}
        hold = _safe_float(ex.get("ts"), 0.0) - _safe_float(tr.get("ts"), 0.0)
        if hold > 0:
            hold_vals.append(hold)

        rr = str(((legs.get("runner") or {}).get("exit") or {}).get("reason") or "")
        if rr == "RUNNER_ENERGY_DECAY":
            runner_decay_count += 1

        recs = [r for r in (res.get("tick_records") or []) if isinstance(r, dict) and r.get("pair_eval_idx") is not None]
        if recs:
            last = recs[-1]
            m = last.get("metrics") or {}
            p = _safe_float(m.get("progress"), 0.0)
            pk = _safe_float(m.get("peak_progress"), p)
            progress_at_exit.append(p)
            peak_progress.append(pk)
            if pk > 1e-6:
                giveback_ratios.append(p / pk)
            cps = _safe_float(m.get("cps"), 0.0)
            dhr = _safe_float(m.get("dhr"), 0.0)
            ge = _safe_float(m.get("ge"), 0.0)
            if cps < float(getattr(phone_bot, "ENERGY_CPS_THRESHOLD", 0.52)):
                cps_low_count += 1
            if dhr >= float(getattr(phone_bot, "ENERGY_DHR_DECAY_THRESHOLD", 0.58)):
                dhr_spike_count += 1
            if ge >= float(getattr(phone_bot, "ENERGY_GE_DECAY_THRESHOLD", 0.28)):
                ge_spike_count += 1
            if reason == "FAILED_TO_CONTINUE_DECAY" and cps < float(getattr(phone_bot, "ENERGY_CPS_THRESHOLD", 0.52)) and dhr >= float(getattr(phone_bot, "ENERGY_DHR_DECAY_THRESHOLD", 0.58)) and ge >= float(getattr(phone_bot, "ENERGY_GE_DECAY_THRESHOLD", 0.28)):
                energy_confirmed_decay += 1

        delays = _compute_delay_from_result(res)
        for k in delay_acc:
            delay_acc[k]["clip_hits"] += delays[k]["clip_rate"]
            delay_acc[k]["clip_gain"] += delays[k]["clip_gain"]
            delay_acc[k]["count"] += delays[k]["count"]

        if collect_runs:
            run_summaries.append(
                {
                    "seed": run_seed,
                    "scenario": str(res.get("scenario") or "unknown"),
                    "weighted_pips": wp,
                    "weighted_pips_per_hour": pph,
                    "core_pips": _safe_float((legs.get("core") or {}).get("pips"), 0.0),
                    "runner_pips": _safe_float((legs.get("runner") or {}).get("pips"), 0.0),
                    "exit_reason": reason,
                    "runner_exit_reason": str(((legs.get("runner") or {}).get("exit") or {}).get("reason") or "UNKNOWN"),
                    "result": res,
                }
            )

    total_hold = sum(hold_vals)
    weighted_pips_per_hour = (sum(weighted) / (total_hold / 3600.0)) if total_hold > 0 else 0.0
    core_pips_per_hour = (sum(core_vals) / (total_hold / 3600.0)) if total_hold > 0 else 0.0
    runner_pips_per_hour = (sum(runner_vals) / (total_hold / 3600.0)) if total_hold > 0 else 0.0

    delay: Dict[str, Dict[str, float]] = {}
    for k in delay_acc:
        cnt = max(1, int(delay_acc[k]["count"]))
        delay[k] = {
            "clip_rate": delay_acc[k]["clip_hits"] / cnt,
            "clip_gain": delay_acc[k]["clip_gain"] / cnt,
            "count": cnt,
        }

    late_bias_terms: List[float] = []
    if delay["n_plus_5"]["clip_rate"] > 0:
        late_bias_terms.append(delay["n_plus_5"]["clip_gain"])

    panic_rate = panic_count / max(1, n_runs)
    panic_loss_mass = panic_rate * (_mean(panic_losses) if panic_losses else 0.0)
    panic_dominance = (sum(panic_losses) / sum(neg_losses)) if neg_losses else 0.0
    runner_positive_rate = runner_positive / max(1, n_runs)
    runner_energy_decay_rate = runner_decay_count / max(1, total_exits)

    median_hold = _percentile(hold_vals, 0.50)
    p95_loss = _percentile(weighted, 0.05)
    p99_loss = _percentile(weighted, 0.01)
    median_weighted_pph = _percentile(weighted_pph, 0.50)
    median_weighted_ppt = _percentile(weighted, 0.50)

    progress_clip_penalty = 0.0
    if progress_at_exit and peak_progress:
        for p, pk in zip(progress_at_exit, peak_progress):
            if pk > 1e-6 and p < (0.60 * pk):
                progress_clip_penalty += (0.60 * pk - p)
        progress_clip_penalty /= max(1, len(progress_at_exit))

    score = (
        (2.0 * median_weighted_pph)
        + (1.2 * median_weighted_ppt)
        - (2.2 * max(0.0, abs(min(0.0, p95_loss)) - 6.0))
        - (2.8 * panic_loss_mass)
        - (2.6 * delay["n_plus_5"]["clip_rate"])
        - (1.4 * runner_energy_decay_rate)
        - (1.3 * progress_clip_penalty)
        - (3.0 * max(0.0, 0.10 - avg_exposure_ratio))
        - (3.0 * max(0.0, 0.10 - avg_active_time_ratio))
        - (4.0 * avg_short_hold_10_rate)
    )
    if not audit_sanity_pass:
        score -= 10.0
    if median_hold < 35.0 and weighted_pips_per_hour > 0.0:
        score -= 4.0

    constraints_ok = (
        median_hold >= CONSTRAINTS["median_hold_time_sec_min"]
        and panic_rate <= CONSTRAINTS["panic_rate_max"]
        and p95_loss >= CONSTRAINTS["p95_loss_min"]
        and delay["n_plus_5"]["clip_rate"] <= CONSTRAINTS["clip_rate_n_plus_5_max"]
        and runner_positive_rate >= CONSTRAINTS["runner_positive_rate_min"]
        and avg_exposure_ratio >= 0.5
        and avg_active_time_ratio >= 0.5
        and avg_short_hold_10_rate <= 0.10
        and audit_sanity_pass
    )

    out: Dict[str, Any] = {
        "avg_exposure_ratio": avg_exposure_ratio,
        "avg_active_time_ratio": avg_active_time_ratio,
        "avg_short_hold_10_rate": avg_short_hold_10_rate,
        "audit_sanity_pass": audit_sanity_pass,
        "params": params,
        "n_runs": n_runs,
        "score": score,
        "constraints_ok": constraints_ok,
        "weighted_pips_per_hour": weighted_pips_per_hour,
        "core_pips_per_hour": core_pips_per_hour,
        "runner_pips_per_hour": runner_pips_per_hour,
        "median_weighted_pips_per_hour": median_weighted_pph,
        "median_weighted_pips_per_trade": median_weighted_ppt,
        "avg_weighted_pips_per_trade": _mean(weighted),
        "variance_weighted_pips_per_hour": _variance(weighted_pph),
        "skewness_of_pips": _skewness(weighted),
        "runner_contribution_ratio": (sum(runner_vals) / sum(weighted)) if abs(sum(weighted)) > 1e-9 else 0.0,
        "exit_entropy": _entropy(exit_reasons),
        "average_progress_at_exit": _mean(progress_at_exit),
        "average_peak_progress": _mean(peak_progress),
        "giveback_ratio": _mean(giveback_ratios),
        "late_bias": _mean(late_bias_terms),
        "p95_loss": p95_loss,
        "p99_loss": p99_loss,
        "panic_rate": panic_rate,
        "panic_loss_mass": panic_loss_mass,
        "panic_dominance": panic_dominance,
        "runner_positive_rate": runner_positive_rate,
        "runner_energy_decay_rate": runner_energy_decay_rate,
        "median_hold_time_sec": median_hold,
        "hold_percentiles": {
            "p50": median_hold,
            "p75": _percentile(hold_vals, 0.75),
            "p90": _percentile(hold_vals, 0.90),
        },
        "delay": delay,
        "energy_dominance": {
            "pct_cps_below_threshold": cps_low_count / max(1, total_exits),
            "pct_dhr_spike": dhr_spike_count / max(1, total_exits),
            "pct_ge_spike": ge_spike_count / max(1, total_exits),
            "decay_energy_confirmed_rate": energy_confirmed_decay / max(1, exit_reasons.get("FAILED_TO_CONTINUE_DECAY", 0)),
        },
        "exit_reason_counts": dict(exit_reasons),
    }
    if collect_runs:
        out["runs"] = run_summaries
    return out


def dominates(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    a_obj = (
        _safe_float(a.get("median_weighted_pips_per_hour"), -1e18),
        _safe_float(a.get("median_weighted_pips_per_trade"), -1e18),
        -_safe_float(a.get("panic_loss_mass"), 1e18),
        -_safe_float(a.get("runner_energy_decay_rate"), 1e18),
        _safe_float(a.get("p95_loss"), -1e18),
        -_safe_float(((a.get("delay") or {}).get("n_plus_5") or {}).get("clip_rate"), 1e18),
    )
    b_obj = (
        _safe_float(b.get("median_weighted_pips_per_hour"), -1e18),
        _safe_float(b.get("median_weighted_pips_per_trade"), -1e18),
        -_safe_float(b.get("panic_loss_mass"), 1e18),
        -_safe_float(b.get("runner_energy_decay_rate"), 1e18),
        _safe_float(b.get("p95_loss"), -1e18),
        -_safe_float(((b.get("delay") or {}).get("n_plus_5") or {}).get("clip_rate"), 1e18),
    )
    return all(x >= y for x, y in zip(a_obj, b_obj)) and any(x > y for x, y in zip(a_obj, b_obj))


def pareto_front(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    front: List[Dict[str, Any]] = []
    for i, cand in enumerate(items):
        dominated = False
        for j, other in enumerate(items):
            if i == j:
                continue
            if dominates(other, cand):
                dominated = True
                break
        if not dominated:
            front.append(cand)
    front.sort(key=lambda x: _safe_float(x.get("score"), -1e18), reverse=True)
    return front


def evaluate_distributions(params: Dict[str, float], start_seed: int, bucket_sec: float, run_distributions: Tuple[int, ...]) -> Dict[str, Any]:
    by_n: Dict[str, Any] = {}
    for i, n in enumerate(run_distributions):
        by_n[str(n)] = evaluate_config(params, n_runs=n, start_seed=start_seed + i * 100000, bucket_sec=bucket_sec)
    primary = dict(by_n[str(run_distributions[-1])])
    primary["distribution_runs"] = by_n
    return primary


def _delta_table(baseline: Dict[str, Any], best: Dict[str, Any] | None) -> Dict[str, Any]:
    if best is None:
        return {"status": "no_stable_candidate", "baseline": baseline}
    keys = [
        "weighted_pips_per_hour",
        "median_weighted_pips_per_hour",
        "median_weighted_pips_per_trade",
        "p95_loss",
        "p99_loss",
        "panic_rate",
        "panic_loss_mass",
        "median_hold_time_sec",
        "runner_positive_rate",
    ]
    out: Dict[str, Any] = {"status": "ok", "rows": {}}
    for k in keys:
        b = _safe_float(baseline.get(k), 0.0)
        v = _safe_float(best.get(k), 0.0)
        out["rows"][k] = {"baseline": b, "best": v, "delta": (v - b)}
    b_clip = _safe_float(((baseline.get("delay") or {}).get("n_plus_5") or {}).get("clip_rate"), 0.0)
    v_clip = _safe_float(((best.get("delay") or {}).get("n_plus_5") or {}).get("clip_rate"), 0.0)
    out["rows"]["clip_rate_n_plus_5"] = {"baseline": b_clip, "best": v_clip, "delta": (v_clip - b_clip)}
    return out


def _write_delta_markdown(path: Path, delta: Dict[str, Any]) -> None:
    lines = ["# Baseline vs Best Stable Candidate", ""]
    if delta.get("status") != "ok":
        lines.append("No stable candidate found.")
    else:
        lines.append("| Metric | Baseline | Best | Delta |")
        lines.append("|---|---:|---:|---:|")
        for k, row in (delta.get("rows") or {}).items():
            lines.append(f"| {k} | {row['baseline']:.6f} | {row['best']:.6f} | {row['delta']:.6f} |")
    path.write_text("\n".join(lines), encoding="utf-8")


def _materialize_session6_outputs(best_params: Dict[str, float], seed: int, bucket_sec: float, out_dir: Path, n_runs: int = 100) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    eval_res = evaluate_config(best_params, n_runs=n_runs, start_seed=seed + 700000, bucket_sec=bucket_sec, collect_runs=True)
    runs = list(eval_res.get("runs", []))
    runs.sort(key=lambda r: _safe_float(r.get("weighted_pips_per_hour"), -1e18), reverse=True)

    best_runs = runs[:10]
    worst_runs = sorted(runs, key=lambda r: _safe_float(r.get("weighted_pips_per_hour"), 1e18))[:10]

    def dump_runs(items: List[Dict[str, Any]], prefix: str) -> List[str]:
        written: List[str] = []
        for i, r in enumerate(items, start=1):
            p = out_dir / f"sim_results_{prefix}_{i:02d}.json"
            with p.open("w", encoding="utf-8") as f:
                json.dump(r.get("result") or {}, f, indent=2)
            written.append(str(p))
        return written

    best_paths = dump_runs(best_runs, "best")
    worst_paths = dump_runs(worst_runs, "worst")

    atlas: List[Dict[str, Any]] = []
    for i, r in enumerate(worst_runs, start=1):
        result = r.get("result") or {}
        recs = [x for x in (result.get("tick_records") or []) if isinstance(x, dict)]
        atlas.append(
            {
                "rank": i,
                "seed": r.get("seed"),
                "scenario": r.get("scenario"),
                "weighted_pips_per_hour": r.get("weighted_pips_per_hour"),
                "weighted_pips": r.get("weighted_pips"),
                "exit_reason": r.get("exit_reason"),
                "delay": _compute_delay_from_result(result),
                "tick_trace": [
                    {
                        "pair_eval_idx": t.get("pair_eval_idx"),
                        "ts": t.get("ts"),
                        "metrics": t.get("metrics"),
                        "aee_true_rules": t.get("aee_true_rules"),
                        "aee_chosen_rule": t.get("aee_chosen_rule"),
                        "rule_persistence": t.get("rule_persistence"),
                        "rule_hit_count": t.get("rule_hit_count"),
                    }
                    for t in recs if t.get("evaluated")
                ],
            }
        )

    failure_json = out_dir / "failure_atlas.json"
    with failure_json.open("w", encoding="utf-8") as f:
        json.dump({"worst_runs": atlas}, f, indent=2)

    md_lines = ["# Failure Atlas (Worst Runs)", ""]
    for item in atlas:
        md_lines.append(
            f"- rank={item['rank']} seed={item['seed']} scenario={item['scenario']} pph={_safe_float(item['weighted_pips_per_hour']):.4f} exit={item['exit_reason']}"
        )
    failure_md = out_dir / "failure_atlas.md"
    failure_md.write_text("\n".join(md_lines), encoding="utf-8")

    return {
        "best_run_files": best_paths,
        "worst_run_files": worst_paths,
        "failure_atlas_json": str(failure_json),
        "failure_atlas_md": str(failure_md),
    }


def optimize(
    trials: int,
    runs_per_trial: int,
    seed: int,
    bucket_sec: float = 5.0,
    artifact_dir: str = "optimizer_artifacts",
    run_distributions: Tuple[int, ...] = RUN_DISTRIBUTIONS,
    stability_runs: int = 500,
    materialize_runs: int | None = None,
) -> Dict[str, Any]:
    rng = random.Random(seed)
    artifact_path = Path(artifact_dir)
    artifact_path.mkdir(parents=True, exist_ok=True)

    baseline_params = snapshot_params()
    baseline = evaluate_distributions(baseline_params, start_seed=seed + 10000, bucket_sec=bucket_sec, run_distributions=run_distributions)

    archive: List[Dict[str, Any]] = [dict(baseline, source="baseline")]
    candidates: List[Dict[str, float]] = []
    for _ in range(max(1, trials // 2)):
        candidates.append(sample_params(rng))

    for i in range(max(1, trials - len(candidates))):
        anchor = baseline_params
        if archive and rng.random() < 0.70:
            anchor = max(archive, key=lambda x: _safe_float(x.get("score"), -1e18)).get("params", baseline_params)
        candidates.append(mutate_params(anchor, rng, scale=0.20 if i % 2 == 0 else 0.34))

    for i, params in enumerate(candidates):
        res = evaluate_distributions(params, start_seed=seed + i * 173, bucket_sec=bucket_sec, run_distributions=run_distributions)
        res["source"] = "stage1"
        archive.append(res)

    stage1_front = pareto_front(archive[1:])

    baseline_anchor = baseline["distribution_runs"][str(run_distributions[-1])]
    stable_candidates: List[Dict[str, Any]] = []
    for i, cand in enumerate(stage1_front[: max(3, min(10, len(stage1_front)))]):
        eval500 = evaluate_config(cand["params"], n_runs=stability_runs, start_seed=seed + 500000 + i * 1597, bucket_sec=bucket_sec)
        eval500["source"] = "stability_500"
        archive.append(eval500)

        dropped = False
        pph300 = _safe_float(cand.get("median_weighted_pips_per_hour"), 0.0)
        pph500 = _safe_float(eval500.get("median_weighted_pips_per_hour"), 0.0)
        if abs(pph300) > 1e-9:
            dropped = ((pph300 - pph500) / abs(pph300)) > 0.15

        clip300 = _safe_float(((cand.get("delay") or {}).get("n_plus_5") or {}).get("clip_rate"), 1.0)
        p95_300 = _safe_float(cand.get("p95_loss"), -1e18)

        baseline_pph = _safe_float(baseline_anchor.get("weighted_pips_per_hour"), -1e18)
        baseline_clip = _safe_float(((baseline_anchor.get("delay") or {}).get("n_plus_5") or {}).get("clip_rate"), 1e18)
        baseline_p95 = _safe_float(baseline_anchor.get("p95_loss"), -1e18)

        stable_ok = (
            (not dropped)
            and bool(cand.get("constraints_ok"))
            and bool(eval500.get("constraints_ok"))
            and _safe_float(cand.get("weighted_pips_per_hour"), -1e18) > baseline_pph
            and _safe_float(cand.get("median_weighted_pips_per_hour"), -1e18) > 0.0
            and clip300 < baseline_clip
            and p95_300 > baseline_p95
        )

        if stable_ok:
            merged = dict(cand)
            merged["stability_500"] = eval500
            merged["stability"] = {
                "median_pph_reference": pph300,
                "median_pph_500": pph500,
                "drop_ratio": ((pph300 - pph500) / abs(pph300)) if abs(pph300) > 1e-9 else 0.0,
            }
            stable_candidates.append(merged)

    best = max(stable_candidates, key=lambda x: _safe_float(x.get("score"), -1e18)) if stable_candidates else None
    if best is not None:
        set_params(best["params"])

    delta = _delta_table(baseline_anchor, best)
    delta_json = artifact_path / "delta_table_baseline_vs_best.json"
    delta_md = artifact_path / "delta_table_baseline_vs_best.md"
    delta_json.write_text(json.dumps(delta, indent=2), encoding="utf-8")
    _write_delta_markdown(delta_md, delta)

    stable_path = artifact_path / "stable_candidates.json"
    stable_path.write_text(json.dumps(stable_candidates, indent=2), encoding="utf-8")

    session6 = {}
    session6_source = "none"
    materialize_candidate = best
    if materialize_candidate is None:
        ranked = sorted(archive[1:], key=lambda x: _safe_float(x.get("score"), -1e18), reverse=True)
        if ranked:
            materialize_candidate = ranked[0]
            session6_source = "top_unstable"
    else:
        session6_source = "best_stable"

    if materialize_candidate is not None:
        session6 = _materialize_session6_outputs(
            best_params=materialize_candidate["params"],
            seed=seed,
            bucket_sec=bucket_sec,
            out_dir=artifact_path,
            n_runs=(materialize_runs if materialize_runs is not None else max(50, runs_per_trial)),
        )
        session6["source"] = session6_source

    report = {
        "objective": {
            "maximize": ["median_weighted_pips_per_hour", "median_weighted_pips_per_trade"],
            "minimize": ["p95_loss", "panic_loss_mass", "delay.n_plus_5.clip_rate", "runner_energy_decay_rate"],
            "constraints": CONSTRAINTS,
            "distribution_runs": list(run_distributions),
        },
        "baseline": baseline,
        "baseline_anchor_runs": int(run_distributions[-1]),
        "best": best,
        "progress_declared": bool(best is not None),
        "pareto_front": stage1_front,
        "stable_candidates": stable_candidates,
        "leaderboard": sorted(archive[1:], key=lambda x: _safe_float(x.get("score"), -1e18), reverse=True)[:10],
        "archive": archive,
        "delta_table": delta,
        "artifacts": {
            "stable_candidates": str(stable_path),
            "delta_table_json": str(delta_json),
            "delta_table_md": str(delta_md),
            **session6,
        },
    }
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Stability-first optimizer for sustainable weighted pips/hour.")
    parser.add_argument("--trials", type=int, default=12)
    parser.add_argument("--runs-per-trial", type=int, default=50)
    parser.add_argument("--seed", type=int, default=123)
    parser.add_argument("--bucket-sec", type=float, default=5.0)
    parser.add_argument("--artifact-dir", default="optimizer_artifacts")
    parser.add_argument("--distribution-runs", default=",".join(str(x) for x in RUN_DISTRIBUTIONS), help="comma-separated run counts per candidate, e.g. 25,100,300")
    parser.add_argument("--stability-runs", type=int, default=500)
    parser.add_argument("--materialize-runs", type=int, default=50)
    parser.add_argument("--output", default="optimizer_report.json")
    args = parser.parse_args()

    run_distributions = tuple(int(x.strip()) for x in str(args.distribution_runs).split(",") if str(x).strip())
    if not run_distributions:
        run_distributions = RUN_DISTRIBUTIONS

    report = optimize(
        trials=args.trials,
        runs_per_trial=args.runs_per_trial,
        seed=args.seed,
        bucket_sec=args.bucket_sec,
        artifact_dir=args.artifact_dir,
        run_distributions=run_distributions,
        stability_runs=int(args.stability_runs),
        materialize_runs=int(args.materialize_runs),
    )
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("OBJECTIVE", report["objective"])
    print("BASELINE_ANCHOR", report["baseline"]["distribution_runs"][str(report["baseline_anchor_runs"])])
    print("BEST", report["best"])
    print("PARETO_COUNT", len(report.get("pareto_front", [])))
    print("STABLE_COUNT", len(report.get("stable_candidates", [])))
    print("ARTIFACTS", report.get("artifacts", {}))
    print(f"WROTE {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
