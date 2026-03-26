#!/usr/bin/env python3
"""
AEE Active-Policy Evidence-Pack Runner

Runs the current active entry policy through AEE v3 and prints
the full 5-layer result truth block:
  1. CONFIG TRUTH
  2. LOGIC TRUTH
  3. TRADE TRUTH (sampled)
  4. DATA TRUTH
  5. CAUSAL TRUTH

Usage:
  python3 run_aee_active_policy_evidencepack.py \
      --config entry_v23_policy_guarded_active.json

Writes artifacts to control/:
  config_snapshot_active.json
  logic_trace_summary_active.json
  trade_evidence_sample_active.json
  data_coverage_report_active.json
  expected_vs_actual_signature_active.json
  failure_layer_classification_active.json
  run_summary_active.json
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_aee_band_floor_baseline import (
    _eval_trade_baseline,
    _entry_filter_evaluate,
    _infer_trade_family,
    _load_rows,
    _stream_duration_hours,
    _context_from_stream,
    _safe_float,
    _safe_int,
)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_round(v: float, n: int = 6) -> float:
    try:
        return round(float(v), n)
    except Exception:
        return 0.0


def _sorted_top(counter_like: dict[str, int], top_n: int = 8) -> list[dict[str, Any]]:
    rows = [{"name": k, "count": int(v)} for k, v in counter_like.items()]
    rows.sort(key=lambda x: (-x["count"], x["name"]))
    return rows[:top_n]


def _parse_csv_set(raw: str) -> set[str]:
    return {part.strip().lower() for part in str(raw or "").split(",") if part.strip()}


def _load_slice_filters(path: Path) -> tuple[set[str], set[str], str]:
    if not path.exists():
        raise SystemExit(f"ERROR: slice file not found: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"ERROR: invalid slice JSON: {path} ({exc})")

    include_contexts = {
        str(x).strip().lower()
        for x in (payload.get("include_contexts") or [])
        if str(x).strip()
    }
    include_trade_ids = {
        str(x).strip()
        for x in (payload.get("include_trade_ids") or [])
        if str(x).strip()
    }
    slice_label = str(payload.get("slice_label", "")).strip()
    return include_contexts, include_trade_ids, slice_label


def _build_failure_bucket_dashboard(records: list[dict[str, Any]]) -> dict[str, Any]:
    fake_runners = [
        r for r in records if str(r.get("winner_taxonomy", "")) == "fake_runner"
    ]
    overheld = [
        r for r in records if str(r.get("winner_taxonomy", "")) == "overheld_winner"
    ]
    green_losses = [
        r for r in records
        if float(r.get("realized_pips", 0.0)) < 0.0
        and str(r.get("green_tier", "")) in {"bankable_green", "runner_green"}
    ]

    worst = sorted(
        [r for r in records if float(r.get("profit_given_back", 0.0)) > 0.0],
        key=lambda r: (-float(r.get("profit_given_back", 0.0)), float(r.get("realized_pips", 0.0))),
    )[:20]

    def _row(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "trade_id": r.get("trade_id"),
            "family": r.get("family"),
            "context": r.get("context"),
            "path_shape": r.get("path_shape"),
            "economic_state": r.get("economic_state"),
            "winner_taxonomy": r.get("winner_taxonomy"),
            "exit_reason": r.get("exit_reason"),
            "profit_given_back": _safe_round(r.get("profit_given_back", 0.0), 6),
            "realized_pips": _safe_round(r.get("realized_pips", 0.0), 6),
            "hold_sec": _safe_round(r.get("hold_sec", 0.0), 3),
        }

    return {
        "bucket_counts": {
            "fake_runner_count": len(fake_runners),
            "overheld_winner_count": len(overheld),
            "green_loss_count": len(green_losses),
        },
        "top_20_worst_examples": [_row(r) for r in worst],
    }


def _verdict(net_pph: float) -> str:
    if net_pph > 0.02:
        return "KEEP"
    if net_pph > -0.02:
        return "TUNE"
    return "KILL"


def _sample_by_bucket(
    records: list[dict[str, Any]], bucket: str, max_rows: int = 15
) -> list[dict[str, Any]]:
    if bucket == "winners":
        rows = [r for r in records if r.get("net_pips", 0.0) > 0.0]
    elif bucket == "losers":
        rows = [r for r in records if r.get("net_pips", 0.0) < 0.0]
    else:
        rows = [r for r in records if abs(float(r.get("net_pips", 0.0))) <= 0.25]
    rows.sort(key=lambda r: (str(r.get("family", "")), str(r.get("context", ""))))
    return rows[:max_rows]


def _eval_baseline_trade_net(
    trows: list[dict[str, Any]],
    mode: str,
    friction_per_trade: float,
    static_tp_pips: float,
    static_sl_pips: float,
    protective_sl_pips: float,
) -> float:
    path = [_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows]
    if not path:
        return -friction_per_trade

    if mode == "no_aee_loose":
        gross = path[-1]
        return gross - friction_per_trade

    if mode == "minimal_protective_only":
        gross = path[-1]
        for p in path:
            if p <= -abs(protective_sl_pips):
                gross = p
                break
        return gross - friction_per_trade

    # static_tp_sl baseline
    gross = path[-1]
    tp = abs(static_tp_pips)
    sl = abs(static_sl_pips)
    for p in path:
        if p >= tp:
            gross = tp
            break
        if p <= -sl:
            gross = -sl
            break
    return gross - friction_per_trade


def _aggregate_baseline_benchmarks(
    records: list[dict[str, Any]],
    total_hours: float,
) -> dict[str, Any]:
    n = len(records)
    baseline_modes = ["static_tp_sl", "minimal_protective_only", "no_aee_loose", "aee_candidate"]
    summary: dict[str, Any] = {}
    for mode in baseline_modes:
        total = sum(float(r.get("baseline_net", {}).get(mode, 0.0)) for r in records)
        avg = total / n if n else 0.0
        pph = total / total_hours if total_hours > 0 else 0.0
        summary[mode] = {
            "trade_count": n,
            "total_net_pips": _safe_round(total, 6),
            "avg_net_pips_per_trade": _safe_round(avg, 6),
            "realized_pph": _safe_round(pph, 6),
        }

    cand = summary["aee_candidate"]
    deltas: dict[str, Any] = {}
    for mode in ["static_tp_sl", "minimal_protective_only", "no_aee_loose"]:
        row = summary[mode]
        deltas[mode] = {
            "delta_realized_pph": _safe_round(cand["realized_pph"] - row["realized_pph"], 6),
            "delta_avg_net_pips_per_trade": _safe_round(
                cand["avg_net_pips_per_trade"] - row["avg_net_pips_per_trade"], 6
            ),
            "net_expectancy_shift": _safe_round(
                cand["avg_net_pips_per_trade"] - row["avg_net_pips_per_trade"], 6
            ),
        }

    return {
        "baselines": summary,
        "candidate_vs_baselines": deltas,
    }


def _baseline_gate_assessment(baseline_ab: dict[str, Any]) -> dict[str, Any]:
    deltas = baseline_ab.get("candidate_vs_baselines", {})
    gates: dict[str, bool] = {}
    for mode in ["static_tp_sl", "minimal_protective_only", "no_aee_loose"]:
        d = deltas.get(mode, {})
        pph_ok = float(d.get("delta_realized_pph", 0.0)) > 0.0
        ppt_ok = float(d.get("delta_avg_net_pips_per_trade", 0.0)) > 0.0
        gates[f"beats_{mode}_pph"] = pph_ok
        gates[f"beats_{mode}_avg_pips_per_trade"] = ppt_ok

    hard_amplifier_gate_pass = bool(
        gates.get("beats_minimal_protective_only_pph", False)
        and gates.get("beats_minimal_protective_only_avg_pips_per_trade", False)
        and gates.get("beats_no_aee_loose_pph", False)
        and gates.get("beats_no_aee_loose_avg_pips_per_trade", False)
    )

    return {
        "gates": gates,
        "all_pass": all(gates.values()) if gates else False,
        "hard_amplifier_gate_pass": hard_amplifier_gate_pass,
        "hard_reject_condition": (
            "if candidate fails minimal_protective_only OR no_aee_loose "
            "(pph or avg pips/trade), force REJECT"
        ),
    }


def _load_and_validate_intervention_basis(path: Path) -> dict[str, Any]:
    required = [
        "economic_objective",
        "green_tier_definition",
        "state_framework",
        "module_order",
        "source_runs_used",
        "source_logs_used",
        "dominant_damaging_branches",
        "affected_families",
        "affected_subclusters",
        "evidence_samples",
        "proposed_variable_or_transition",
        "expected_signature",
        "success_criteria",
    ]
    if not path.exists():
        raise SystemExit(f"ERROR: missing mandatory pre-run artifact: {path}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise SystemExit(f"ERROR: invalid intervention basis JSON: {path} ({exc})")
    missing = [k for k in required if payload.get(k) in (None, "", [], {})]
    if missing:
        raise SystemExit(
            "ERROR: intervention basis missing required fields: " + ", ".join(missing)
        )
    return payload


def _pct_change(current: float | None, prior: float | None) -> float | None:
    if current is None or prior is None:
        return None
    if abs(prior) < 1e-9:
        return None
    return (current - prior) / abs(prior)


def _performance_signature_assessment(
    current: dict[str, Any],
    prior: dict[str, Any] | None,
) -> dict[str, Any]:
    immediate_expected = {
        "realized_pph_change_pct": [0.20, 0.80],
        "gap_change_pct": [-0.40, -0.15],
        "efficiency_abs_change": [0.05, 0.25],
    }
    if not prior:
        return {
            "has_prior_reference": False,
            "reason": "no prior run summary available for bounded signature comparison",
            "immediate_signature": {
                "expected_range": immediate_expected,
                "pass": False,
            },
            "global_expectation": {"pass": False},
            "bankable_green_protection": {"pass": False},
            "throughput_signature": {"pass": False},
            "baseline_flip_signature": {"pass": False},
            "gap_reduction_signature": {"pass": False},
            "giveback_fix_signature": {"pass": False},
            "continuation_signature": {"pass": False},
            "loss_compression_stability": {"pass": bool(current.get("loss_compression_rate", 0.0) >= 0.85)},
            "core_rule_all_four": {"pass": False},
            "auto_reject_patterns": [],
        }

    rpct = _pct_change(current.get("realized_pph", 0.0), prior.get("realized_pph", 0.0))
    gpct = _pct_change(current.get("gap", 0.0), prior.get("gap", 0.0))
    eff_abs = current.get("extraction_efficiency", 0.0) - prior.get("extraction_efficiency", 0.0)
    gbpct = _pct_change(current.get("giveback_ratio", 0.0), prior.get("giveback_ratio", 0.0))
    ccpct = _pct_change(current.get("continuation_capture_rate", 0.0), prior.get("continuation_capture_rate", 0.0))
    bppct = _pct_change(current.get("breakout_pph", 0.0), prior.get("breakout_pph", 0.0))
    ffpct = _pct_change(current.get("fast_failure_exit_share", 0.0), prior.get("fast_failure_exit_share", 0.0))

    def _in(v: float | None, lo: float, hi: float) -> bool:
        return v is not None and lo <= v <= hi

    global_pass = (
        _in(rpct, 0.20, 0.80)
        and _in(gpct, -0.40, -0.15)
        and 0.05 <= eff_abs <= 0.25
    )
    bankable_red_rate_pct = _pct_change(
        current.get("bankable_green_loss_red_rate", 0.0),
        prior.get("bankable_green_loss_red_rate", 0.0),
    )
    tph_pct = _pct_change(current.get("trades_per_hour", 0.0), prior.get("trades_per_hour", 0.0))
    loser_hold_pct = _pct_change(current.get("avg_loser_hold_sec", 0.0), prior.get("avg_loser_hold_sec", 0.0))
    weak_winner_hold_pct = _pct_change(
        current.get("avg_weak_winner_hold_sec", 0.0),
        prior.get("avg_weak_winner_hold_sec", 0.0),
    )
    giveback_share_pct = _pct_change(
        current.get("giveback_exit_share", 0.0),
        prior.get("giveback_exit_share", 0.0),
    )
    giveback_fix_pass = (
        _in(gbpct, -0.30, -0.10)
        and _in(giveback_share_pct, -0.30, -0.10)
    )
    continuation_pass = _in(ccpct, 0.05, 0.25)
    loss_stability_pass = current.get("loss_compression_rate", 0.0) >= 0.85
    bankable_green_pass = (
        current.get("bankable_green_loss_red_rate", 1.0) <= 0.05
        or (bankable_red_rate_pct is not None and bankable_red_rate_pct <= -0.50)
    )
    throughput_pass = (
        tph_pct is not None and tph_pct > 0.0
        and loser_hold_pct is not None and loser_hold_pct < 0.0
        and weak_winner_hold_pct is not None and weak_winner_hold_pct < 0.0
    )
    baseline_flip_pass = bool(current.get("baseline_hard_amplifier_gate_pass", False))
    gap_reduction_pass = _in(gpct, -0.40, -0.15)

    auto_reject_patterns: list[str] = []
    if gbpct is not None and gbpct < 0 and (rpct is None or abs(rpct) < 0.02):
        auto_reject_patterns.append("COSMETIC_IMPROVEMENT")
    if bankable_red_rate_pct is None or not bankable_green_pass:
        auto_reject_patterns.append("BANKABLE_GREEN_PROTECTION_FAILED")
    if not gap_reduction_pass:
        auto_reject_patterns.append("GAP_NOT_MATERIALLY_REDUCED")
    if not baseline_flip_pass:
        auto_reject_patterns.append("BASELINE_STILL_BETTER")
    if ccpct is not None and ccpct > 0 and current.get("loss_compression_rate", 0.0) < prior.get("loss_compression_rate", 0.0) - 0.05:
        auto_reject_patterns.append("OVER_HOLD_BROKE_LOSS_COMPRESSION")
    if ccpct is not None and ccpct > 0 and rpct is not None and rpct <= 0.0:
        auto_reject_patterns.append("MORE_CONTINUATION_WORSE_PPH")
    if gbpct is not None and gbpct < 0 and ffpct is not None and ffpct > 0.10:
        auto_reject_patterns.append("SHIFTED_DAMAGE_GIVEBACK_TO_FAST_FAILURE")
    if not throughput_pass:
        auto_reject_patterns.append("THROUGHPUT_STALLED")

    auto_reject_patterns = list(dict.fromkeys(auto_reject_patterns))

    core_rule_pass = (
        current.get("loss_compression_rate", 0.0) >= prior.get("loss_compression_rate", 0.0) - 0.02
        and current.get("failure_to_win_conversion_rate", 0.0) > prior.get("failure_to_win_conversion_rate", 0.0)
        and bankable_green_pass
        and throughput_pass
        and rpct is not None and rpct > 0
    )

    return {
        "has_prior_reference": True,
        "immediate_signature": {
            "expected_range": immediate_expected,
            "realized_pph_change_pct": _safe_round(rpct if rpct is not None else 0.0, 6),
            "gap_change_pct": _safe_round(gpct if gpct is not None else 0.0, 6),
            "efficiency_abs_change": _safe_round(eff_abs, 6),
            "pass": global_pass,
        },
        "global_expectation": {
            "realized_pph_change_pct": _safe_round(rpct if rpct is not None else 0.0, 6),
            "gap_change_pct": _safe_round(gpct if gpct is not None else 0.0, 6),
            "efficiency_abs_change": _safe_round(eff_abs, 6),
            "pass": global_pass,
        },
        "bankable_green_protection": {
            "bankable_green_loss_red_rate": _safe_round(current.get("bankable_green_loss_red_rate", 0.0), 6),
            "prior_bankable_green_loss_red_rate": None if prior.get("bankable_green_loss_red_rate") is None else _safe_round(prior.get("bankable_green_loss_red_rate", 0.0), 6),
            "bankable_green_loss_red_rate_change_pct": None if bankable_red_rate_pct is None else _safe_round(bankable_red_rate_pct, 6),
            "near_elimination_target_max": 0.05,
            "pass": bankable_green_pass,
        },
        "throughput_signature": {
            "trades_per_hour_change_pct": None if tph_pct is None else _safe_round(tph_pct, 6),
            "avg_loser_hold_sec_change_pct": None if loser_hold_pct is None else _safe_round(loser_hold_pct, 6),
            "avg_weak_winner_hold_sec_change_pct": None if weak_winner_hold_pct is None else _safe_round(weak_winner_hold_pct, 6),
            "pass": throughput_pass,
        },
        "baseline_flip_signature": {
            "hard_amplifier_gate_pass": baseline_flip_pass,
            "pass": baseline_flip_pass,
        },
        "gap_reduction_signature": {
            "gap_change_pct": None if gpct is None else _safe_round(gpct, 6),
            "min_expected_reduction_pct": -0.15,
            "pass": gap_reduction_pass,
        },
        "giveback_fix_signature": {
            "giveback_ratio_change_pct": _safe_round(gbpct if gbpct is not None else 0.0, 6),
            "giveback_exit_share_change_pct": None if giveback_share_pct is None else _safe_round(giveback_share_pct, 6),
            "continuation_capture_change_pct": _safe_round(ccpct if ccpct is not None else 0.0, 6),
            "breakout_pph_change_pct": _safe_round(bppct if bppct is not None else 0.0, 6),
            "pass": giveback_fix_pass,
        },
        "continuation_signature": {
            "continuation_capture_change_pct": _safe_round(ccpct if ccpct is not None else 0.0, 6),
            "pass": continuation_pass,
        },
        "loss_compression_stability": {
            "loss_compression_rate": _safe_round(current.get("loss_compression_rate", 0.0), 6),
            "pass": loss_stability_pass,
        },
        "core_rule_all_four": {
            "pass": core_rule_pass,
        },
        "auto_reject_patterns": auto_reject_patterns,
    }


def _infer_path_shape(trows: list[dict[str, Any]], peak_idx: int, final_pnl: float) -> str:
    if not trows:
        return "UNKNOWN"
    total = max(len(trows), 1)
    peak_frac = (peak_idx + 1) / total
    avg_velocity = sum(_safe_float(r.get("velocity_now", 0.0), 0.0) for r in trows) / total
    peak_row = trows[peak_idx] if 0 <= peak_idx < len(trows) else trows[-1]
    release_quality = _safe_float(peak_row.get("release_quality", 0.0), 0.0)
    noise = _safe_float(peak_row.get("noise", 0.0), 0.0)

    if peak_frac <= 0.25 and final_pnl < 0:
        return "sharp_breakout_then_stall"
    if peak_frac >= 0.60 and avg_velocity > 0:
        return "slow_grind_then_reverse"
    if noise >= 0.55:
        return "chop_then_reverse"
    if release_quality >= 0.55:
        return "healthy_push_then_full_reversal"
    return "mixed_reversal_path"


def _economic_thresholds(friction_per_trade: float) -> dict[str, float]:
    bankable = max(2.0, friction_per_trade * 1.5)
    runner = max(5.0, bankable * 2.5)
    return {
        "micro_green_min_pips": 0.0,
        "bankable_green_min_pips": _safe_round(bankable, 6),
        "runner_green_min_pips": _safe_round(runner, 6),
        "weak_winner_max_pips": _safe_round(bankable, 6),
        "time_inefficient_min_hold_sec": 600.0,
        "time_value_floor_pips_per_min": 0.10,
    }


def _classify_green_tier(mfe_net: float, thresholds: dict[str, float]) -> str:
    if mfe_net >= float(thresholds.get("runner_green_min_pips", 5.0)):
        return "runner_green"
    if mfe_net >= float(thresholds.get("bankable_green_min_pips", 2.0)):
        return "bankable_green"
    if mfe_net > float(thresholds.get("micro_green_min_pips", 0.0)):
        return "micro_green"
    return "never_green"


def _classify_dead_trade_subtype(
    mfe_net: float,
    hold_sec: float,
    mae: float,
    realized_pips: float,
    path_shape: str,
    thresholds: dict[str, float],
) -> str | None:
    if mfe_net >= float(thresholds.get("bankable_green_min_pips", 2.0)):
        return None
    if hold_sec <= 180 or mae <= -2.0:
        return "immediate_failure"
    if path_shape == "chop_then_reverse" or (hold_sec >= 600 and abs(realized_pips) <= 1.0):
        return "noisy_dead_trade"
    return "slow_bleed"


def _classify_economic_state(
    mfe_net: float,
    realized_pips: float,
    hold_sec: float,
    thresholds: dict[str, float],
) -> str:
    minutes_open = max(hold_sec / 60.0, 1e-9)
    retained_rate = realized_pips / minutes_open
    bankable = float(thresholds.get("bankable_green_min_pips", 2.0))
    runner = float(thresholds.get("runner_green_min_pips", 5.0))
    time_floor = float(thresholds.get("time_value_floor_pips_per_min", 0.10))
    time_cut = float(thresholds.get("time_inefficient_min_hold_sec", 600.0))

    if hold_sec >= time_cut and retained_rate < time_floor:
        return "TIME_INEFFICIENT"
    if mfe_net < bankable:
        return "NEGATIVE_UNPROVEN"
    if realized_pips < max(0.5, bankable * 0.5):
        return "GREEN_UNPROTECTED"
    if mfe_net >= runner and realized_pips >= bankable:
        return "RUNNER_ELIGIBLE"
    return "GREEN_PROTECTED"


def _retained_rate_pips_per_min(realized_pips: float, hold_sec: float) -> float:
    minutes_open = max(hold_sec / 60.0, 1e-9)
    return realized_pips / minutes_open


def _classify_winner_taxonomy(
    realized_pips: float,
    mfe_net: float,
    hold_sec: float,
    thresholds: dict[str, float],
) -> str | None:
    if realized_pips <= 0.0:
        return None
    weak_max = float(thresholds.get("weak_winner_max_pips", 2.0))
    bankable = float(thresholds.get("bankable_green_min_pips", 2.0))
    runner = float(thresholds.get("runner_green_min_pips", 5.0))
    time_cut = float(thresholds.get("time_inefficient_min_hold_sec", 600.0))
    time_floor = float(thresholds.get("time_value_floor_pips_per_min", 0.10))
    retained_rate = _retained_rate_pips_per_min(realized_pips, hold_sec)

    if mfe_net >= runner and realized_pips <= weak_max:
        return "fake_runner"
    if hold_sec >= time_cut and retained_rate < time_floor:
        return "overheld_winner"
    if mfe_net >= runner and realized_pips >= runner * 0.8:
        return "true_runner"
    if mfe_net >= runner and realized_pips < bankable * 1.5:
        return "underheld_winner"
    if realized_pips <= weak_max:
        return "weak_winner"
    if retained_rate >= time_floor and hold_sec <= 300:
        return "efficient_winner"
    return "standard_winner"


def _build_green_loss_audit(records: list[dict[str, Any]]) -> dict[str, Any]:
    thresholds = dict((records[0].get("economic_thresholds") or {})) if records else _economic_thresholds(0.0)
    losers = [r for r in records if float(r.get("realized_pips", 0.0)) < 0.0]
    never_green = [r for r in losers if not bool(r.get("went_green", False))]
    green_then_loss = [r for r in losers if bool(r.get("went_green", False))]
    micro_green_losses = [r for r in losers if str(r.get("green_tier", "")) == "micro_green"]
    bankable_green_losses = [r for r in losers if str(r.get("green_tier", "")) in {"bankable_green", "runner_green"}]
    runner_green_losses = [r for r in losers if str(r.get("green_tier", "")) == "runner_green"]
    dead_trades = [r for r in records if r.get("dead_trade_subtype")]
    weak_winners = [
        r for r in records
        if 0.0 < float(r.get("realized_pips", 0.0)) <= float(thresholds.get("weak_winner_max_pips", 2.0))
    ]
    bankable_green_population = [r for r in records if str(r.get("green_tier", "")) in {"bankable_green", "runner_green"}]
    bankable_green_winners = [r for r in bankable_green_population if float(r.get("realized_pips", 0.0)) > 0.0]

    family_counts = Counter(str(r.get("family", "UNKNOWN")) for r in green_then_loss)
    exit_counts = Counter(str(r.get("exit_reason", "UNKNOWN")) for r in green_then_loss)
    path_counts = Counter(str(r.get("path_shape", "UNKNOWN")) for r in green_then_loss)
    tier_counts = Counter(str(r.get("green_tier", "UNKNOWN")) for r in losers)
    dead_trade_counts = Counter(str(r.get("dead_trade_subtype", "UNKNOWN")) for r in dead_trades)
    economic_state_counts = Counter(str(r.get("economic_state", "UNKNOWN")) for r in records)
    winner_rows = [r for r in records if float(r.get("realized_pips", 0.0)) > 0.0]
    winner_counts = Counter(str(r.get("winner_taxonomy", "UNKNOWN")) for r in winner_rows if r.get("winner_taxonomy"))
    path_shape_by_family: dict[str, Counter] = defaultdict(Counter)
    for r in green_then_loss:
        fam = str(r.get("family", "UNKNOWN"))
        shp = str(r.get("path_shape", "UNKNOWN"))
        path_shape_by_family[fam][shp] += 1

    def _trade_row(r: dict[str, Any]) -> dict[str, Any]:
        return {
            "trade_id": r.get("trade_id"),
            "family": r.get("family"),
            "context": r.get("context"),
            "green_tier": r.get("green_tier"),
            "economic_state": r.get("economic_state"),
            "dead_trade_subtype": r.get("dead_trade_subtype"),
            "entry_time": r.get("entry_time"),
            "time_of_first_green": r.get("time_of_first_green"),
            "time_of_peak_mfe": r.get("time_of_peak_mfe"),
            "time_of_exit": r.get("time_of_exit"),
            "mfe_pips": _safe_round(r.get("mfe", 0.0), 6),
            "mfe_net_pips": _safe_round(r.get("mfe_net", 0.0), 6),
            "profit_given_back": _safe_round(r.get("profit_given_back", 0.0), 6),
            "final_pnl": _safe_round(r.get("realized_pips", 0.0), 6),
            "hold_sec": _safe_round(r.get("hold_sec", 0.0), 3),
            "retained_rate_pips_per_min": _safe_round(r.get("retained_rate_pips_per_min", 0.0), 6),
            "exit_reason": r.get("exit_reason"),
            "path_shape": r.get("path_shape"),
            "lifecycle_at_peak": r.get("lifecycle_at_peak"),
            "winner_taxonomy": r.get("winner_taxonomy"),
        }

    green_then_loss_sorted = sorted(
        green_then_loss,
        key=lambda r: (-float(r.get("profit_given_back", 0.0)), float(r.get("realized_pips", 0.0))),
    )
    bankable_green_sorted = sorted(
        bankable_green_losses,
        key=lambda r: (-float(r.get("mfe_net", 0.0)), float(r.get("realized_pips", 0.0))),
    )
    dead_trade_sorted = sorted(
        dead_trades,
        key=lambda r: (-float(r.get("hold_sec", 0.0)), float(r.get("realized_pips", 0.0))),
    )
    giveback_exit_share = (
        sum(1 for r in green_then_loss if str(r.get("exit_reason", "")) == "AEE_GIVEBACK_EXIT") / len(green_then_loss)
        if green_then_loss else 0.0
    )
    runner_green_unprotected_losses = [
        r for r in losers
        if str(r.get("green_tier", "")) == "runner_green"
        and str(r.get("economic_state", "")) in {"GREEN_UNPROTECTED", "NEGATIVE_UNPROVEN"}
    ]
    bankable_green_unprotected_losses = [
        r for r in losers
        if str(r.get("green_tier", "")) in {"runner_green", "bankable_green"}
        and str(r.get("economic_state", "")) == "GREEN_UNPROTECTED"
    ]
    threshold_reactivity_proxy_count = sum(
        1
        for r in bankable_green_losses
        if str(r.get("exit_reason", "")) in {"AEE_BAND_FAST_FAILURE_EXIT", "AEE_GIVEBACK_EXIT"}
        and float(r.get("hold_sec", 0.0)) <= 120.0
    )

    exit_reason_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in records:
        exit_reason_rows[str(r.get("exit_reason", "UNKNOWN"))].append(r)
    branch_objective_alignment: list[dict[str, Any]] = []
    objective_misalignment_flags: list[str] = []
    for reason, rows in sorted(exit_reason_rows.items(), key=lambda kv: -len(kv[1])):
        avg_realized = sum(float(x.get("realized_pips", 0.0)) for x in rows) / len(rows)
        avg_hold = sum(float(x.get("hold_sec", 0.0)) for x in rows) / len(rows)
        avg_rate = sum(float(x.get("retained_rate_pips_per_min", 0.0)) for x in rows) / len(rows)
        share = len(rows) / len(records) if records else 0.0
        branch_objective_alignment.append({
            "exit_reason": reason,
            "trade_count": len(rows),
            "trade_share": _safe_round(share, 6),
            "avg_realized_pips": _safe_round(avg_realized, 6),
            "avg_hold_sec": _safe_round(avg_hold, 6),
            "avg_retained_rate_pips_per_min": _safe_round(avg_rate, 6),
        })
        if share >= 0.20 and avg_rate <= 0.0:
            objective_misalignment_flags.append(f"{reason}_NON_POSITIVE_RATE")

    overheld_winner_count = winner_counts.get("overheld_winner", 0)
    fake_runner_count = winner_counts.get("fake_runner", 0)

    return {
        "thresholds": thresholds,
        "summary": {
            "losing_trade_count": len(losers),
            "never_green_count": len(never_green),
            "micro_green_loss_count": len(micro_green_losses),
            "bankable_green_loss_count": len(bankable_green_losses),
            "runner_green_loss_count": len(runner_green_losses),
            "green_then_loss_count": len(green_then_loss),
            "green_then_loss_share_of_losers": _safe_round(
                len(green_then_loss) / len(losers) if losers else 0.0,
                6,
            ),
            "bankable_green_trade_count": len(bankable_green_population),
            "bankable_green_winner_count": len(bankable_green_winners),
            "bankable_green_protection_rate": _safe_round(
                len(bankable_green_winners) / len(bankable_green_population) if bankable_green_population else 0.0,
                6,
            ),
            "bankable_green_loss_red_rate": _safe_round(
                len(bankable_green_losses) / len(bankable_green_population) if bankable_green_population else 0.0,
                6,
            ),
            "avg_mfe_green_then_loss": _safe_round(
                sum(float(r.get("mfe", 0.0)) for r in green_then_loss) / len(green_then_loss)
                if green_then_loss else 0.0,
                6,
            ),
            "avg_profit_given_back": _safe_round(
                sum(float(r.get("profit_given_back", 0.0)) for r in green_then_loss) / len(green_then_loss)
                if green_then_loss else 0.0,
                6,
            ),
            "avg_profit_given_back_bankable_green": _safe_round(
                sum(float(r.get("profit_given_back", 0.0)) for r in bankable_green_losses) / len(bankable_green_losses)
                if bankable_green_losses else 0.0,
                6,
            ),
            "giveback_exit_share_of_green_losses": _safe_round(giveback_exit_share, 6),
        },
        "green_tier_distribution": dict(tier_counts),
        "winner_taxonomy_distribution": dict(winner_counts),
        "dead_trade_subtype_distribution": dict(dead_trade_counts),
        "economic_state_distribution": dict(economic_state_counts),
        "family_distribution": dict(family_counts),
        "exit_reason_distribution": dict(exit_counts),
        "path_shape_distribution": dict(path_counts),
        "path_shape_family_matrix": {
            fam: dict(cnt)
            for fam, cnt in sorted(path_shape_by_family.items(), key=lambda kv: kv[0])
        },
        "state_transition_stress": {
            "runner_green_unprotected_loss_count": len(runner_green_unprotected_losses),
            "bankable_green_unprotected_loss_count": len(bankable_green_unprotected_losses),
            "threshold_reactivity_proxy_count": threshold_reactivity_proxy_count,
            "runner_green_unprotected_loss_share": _safe_round(
                len(runner_green_unprotected_losses) / len(runner_green_losses) if runner_green_losses else 0.0,
                6,
            ),
            "bankable_green_unprotected_loss_share": _safe_round(
                len(bankable_green_unprotected_losses) / len(bankable_green_losses) if bankable_green_losses else 0.0,
                6,
            ),
        },
        "throughput": {
            "avg_loser_hold_sec": _safe_round(
                sum(float(r.get("hold_sec", 0.0)) for r in losers) / len(losers) if losers else 0.0,
                6,
            ),
            "avg_weak_winner_hold_sec": _safe_round(
                sum(float(r.get("hold_sec", 0.0)) for r in weak_winners) / len(weak_winners) if weak_winners else 0.0,
                6,
            ),
            "avg_bankable_green_loser_hold_sec": _safe_round(
                sum(float(r.get("hold_sec", 0.0)) for r in bankable_green_losses) / len(bankable_green_losses)
                if bankable_green_losses else 0.0,
                6,
            ),
            "time_inefficient_trade_count": sum(
                1 for r in records if str(r.get("economic_state", "")) == "TIME_INEFFICIENT"
            ),
            "time_inefficient_trade_share": _safe_round(
                sum(1 for r in records if str(r.get("economic_state", "")) == "TIME_INEFFICIENT") / len(records)
                if records else 0.0,
                6,
            ),
        },
        "objective_alignment": {
            "branch_objective_alignment": branch_objective_alignment,
            "objective_misalignment_flags": objective_misalignment_flags,
            "overheld_winner_count": int(overheld_winner_count),
            "fake_runner_count": int(fake_runner_count),
        },
        "green_then_loss_samples": [_trade_row(r) for r in green_then_loss_sorted[:25]],
        "bankable_green_loss_samples": [_trade_row(r) for r in bankable_green_sorted[:25]],
        "dead_trade_samples": [_trade_row(r) for r in dead_trade_sorted[:25]],
        "never_green_samples": [_trade_row(r) for r in never_green[:25]],
    }


def _classify_path_cluster(r: dict[str, Any], thresholds: dict[str, float]) -> str:
    mfe_net = float(r.get("mfe_net", 0.0))
    realized = float(r.get("realized_pips", 0.0))
    giveback = float(r.get("profit_given_back", 0.0))
    hold_sec = float(r.get("hold_sec", 0.0))
    bankable = float(thresholds.get("bankable_green_min_pips", 2.0))
    runner = float(thresholds.get("runner_green_min_pips", 5.0))
    shape = str(r.get("path_shape", "UNKNOWN"))

    if mfe_net >= runner and realized >= bankable and giveback <= bankable * 0.5 and hold_sec <= 420:
        return "clean_expansion"
    if mfe_net >= runner and realized > 0.0 and shape in {"slow_grind_then_reverse", "healthy_push_then_full_reversal"}:
        return "stall_then_continuation"
    if mfe_net >= bankable and realized < 0.0:
        return "pop_then_reverse"
    return "chop_no_follow_through"


def _split_family_taxonomy(
    base_family: str,
    path_shape: str,
    mfe_net: float,
    realized_pips: float,
    thresholds: dict[str, float],
) -> str:
    bankable = float(thresholds.get("bankable_green_min_pips", 2.0))
    runner = float(thresholds.get("runner_green_min_pips", 5.0))

    if base_family == "EXPANSION_BREAKOUT":
        if mfe_net >= bankable and realized_pips < 0.0:
            return "BREAKOUT__fake_breakout"
        if path_shape == "chop_then_reverse" or mfe_net < runner:
            return "BREAKOUT__weak_expansion"
        return "BREAKOUT__clean_expansion"

    if base_family == "RANGE_ESCAPE":
        if mfe_net >= bankable and realized_pips > 0.0 and path_shape != "chop_then_reverse":
            return "RANGE_ESCAPE__true_escape"
        return "RANGE_ESCAPE__noise_escape"

    if base_family == "OTHER":
        if realized_pips > 0.0 and mfe_net >= bankable:
            return "OTHER__structured"
        return "OTHER__unstructured"

    return base_family


def _build_family_taxonomy_audit(records: list[dict[str, Any]]) -> dict[str, Any]:
    families = sorted({str(r.get("family", "UNKNOWN")) for r in records})
    family_rows: list[dict[str, Any]] = []
    family_invalid = False

    for family in families:
        rows = [r for r in records if str(r.get("family", "UNKNOWN")) == family]
        if not rows:
            continue
        thresholds = dict((rows[0].get("economic_thresholds") or {}))
        by_cluster: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in rows:
            by_cluster[_classify_path_cluster(r, thresholds)].append(r)

        cluster_stats: list[dict[str, Any]] = []
        realized_values: list[float] = []
        dominant_exits: set[str] = set()
        for cname, crow in sorted(by_cluster.items(), key=lambda kv: (-len(kv[1]), kv[0])):
            realized = [float(x.get("realized_pips", 0.0)) for x in crow]
            holds = [float(x.get("hold_sec", 0.0)) for x in crow]
            mfe_vals = [float(x.get("mfe_net", 0.0)) for x in crow]
            bankable_red = sum(1 for x in crow if str(x.get("green_tier", "")) in {"bankable_green", "runner_green"} and float(x.get("realized_pips", 0.0)) < 0.0)
            exit_counter = Counter(str(x.get("exit_reason", "UNKNOWN")) for x in crow)
            dom_exit = exit_counter.most_common(1)[0][0] if exit_counter else "UNKNOWN"
            dominant_exits.add(dom_exit)
            realized_values.extend(realized)
            cluster_stats.append({
                "cluster": cname,
                "trade_count": len(crow),
                "avg_realized_pips": _safe_round(sum(realized) / len(realized) if realized else 0.0, 6),
                "avg_hold_sec": _safe_round(sum(holds) / len(holds) if holds else 0.0, 6),
                "avg_mfe_net_pips": _safe_round(sum(mfe_vals) / len(mfe_vals) if mfe_vals else 0.0, 6),
                "bankable_green_red_rate": _safe_round(bankable_red / len(crow) if crow else 0.0, 6),
                "dominant_exit_reason": dom_exit,
            })

        realized_spread = (max(realized_values) - min(realized_values)) if realized_values else 0.0
        has_sign_split = bool(realized_values) and (min(realized_values) < 0 < max(realized_values))
        invalid = len(by_cluster) >= 3 and (realized_spread >= 2.0 or has_sign_split) and len(dominant_exits) >= 2
        family_invalid = family_invalid or invalid
        family_rows.append({
            "family": family,
            "cluster_count": len(by_cluster),
            "realized_pips_spread": _safe_round(realized_spread, 6),
            "has_sign_split": has_sign_split,
            "dominant_exit_variety": len(dominant_exits),
            "invalid_family_flag": invalid,
            "clusters": cluster_stats,
        })

    return {
        "step": "STEP_1_SPLIT_FAMILIES",
        "family_invalid_detected": family_invalid,
        "family_rows": family_rows,
        "decision": (
            "problem_3_taxonomy_confirmed" if family_invalid else "problem_3_not_confirmed"
        ),
    }


def _build_module_collision_audit(records: list[dict[str, Any]], green_loss_audit: dict[str, Any]) -> dict[str, Any]:
    summary = green_loss_audit.get("summary", {}) if isinstance(green_loss_audit, dict) else {}
    throughput = green_loss_audit.get("throughput", {}) if isinstance(green_loss_audit, dict) else {}
    objective_alignment = green_loss_audit.get("objective_alignment", {}) if isinstance(green_loss_audit, dict) else {}
    bankable_red_rate = float(summary.get("bankable_green_loss_red_rate", 0.0))
    time_ineff_share = float(throughput.get("time_inefficient_trade_share", 0.0))
    overheld = int(objective_alignment.get("overheld_winner_count", 0))
    fake_runner = int(objective_alignment.get("fake_runner_count", 0))
    weak_hold = float(throughput.get("avg_weak_winner_hold_sec", 0.0))
    loser_hold = float(throughput.get("avg_loser_hold_sec", 0.0))

    conflicts: list[str] = []
    if bankable_red_rate > 0.20:
        conflicts.append("PROTECTION_MODULE_UNDERPERFORMING")
    if time_ineff_share > 0.05:
        conflicts.append("TIME_MODULE_UNDERPERFORMING")
    if fake_runner > 0:
        conflicts.append("EXTENSION_MODULE_FALSE_POSITIVES")
    if overheld > 0:
        conflicts.append("EXTENSION_TIME_COLLISION")
    if weak_hold > loser_hold:
        conflicts.append("THROUGHPUT_COLLISION_WEAK_WINNERS_OVERHELD")

    return {
        "step": "STEP_2_MODULARIZE_AEE",
        "objective_collision_detected": bool(conflicts),
        "conflict_flags": conflicts,
        "metrics": {
            "bankable_green_loss_red_rate": _safe_round(bankable_red_rate, 6),
            "time_inefficient_trade_share": _safe_round(time_ineff_share, 6),
            "overheld_winner_count": overheld,
            "fake_runner_count": fake_runner,
            "avg_weak_winner_hold_sec": _safe_round(weak_hold, 6),
            "avg_loser_hold_sec": _safe_round(loser_hold, 6),
        },
        "decision": (
            "problem_5_architecture_confirmed" if conflicts else "problem_5_not_confirmed"
        ),
    }


def _build_simplicity_reality_check(baseline_ab: dict[str, Any]) -> dict[str, Any]:
    deltas = baseline_ab.get("candidate_vs_baselines", {}) if isinstance(baseline_ab, dict) else {}

    def _beats(mode: str) -> bool:
        d = deltas.get(mode, {}) if isinstance(deltas, dict) else {}
        return bool(d.get("delta_realized_pph", 0.0) > 0 and d.get("delta_avg_net_pips_per_trade", 0.0) > 0)

    beats_min = _beats("minimal_protective_only")
    beats_loose = _beats("no_aee_loose")
    complexity_adds_value = beats_min and beats_loose
    return {
        "step": "STEP_3_COMPARE_TO_SIMPLICITY",
        "beats_minimal_protective": beats_min,
        "beats_no_aee_loose": beats_loose,
        "complexity_adds_value": complexity_adds_value,
        "decision": (
            "problem_6_not_confirmed" if complexity_adds_value else "problem_6_simplicity_wins"
        ),
    }


def _build_root_cause_decision_tree(
    family_taxonomy_audit: dict[str, Any],
    module_collision_audit: dict[str, Any],
    simplicity_reality_check: dict[str, Any],
) -> dict[str, Any]:
    step1 = bool(family_taxonomy_audit.get("family_invalid_detected", False))
    step2 = bool(module_collision_audit.get("objective_collision_detected", False))
    step3 = bool(not simplicity_reality_check.get("complexity_adds_value", False))

    if step1:
        root_cause = "PROBLEM_3_TAXONOMY_TOO_BROAD"
    elif step2:
        root_cause = "PROBLEM_5_MONOLITHIC_AEE_OBJECTIVE_COLLISION"
    elif step3:
        root_cause = "PROBLEM_6_SIMPLE_EXITS_DOMINATE"
    else:
        root_cause = "INCONCLUSIVE_REQUIRE_BEHAVIORAL_INTERVENTION"

    return {
        "decision_tree": {
            "step_1_family_split": family_taxonomy_audit,
            "step_2_modularize_aee": module_collision_audit,
            "step_3_simplicity_vs_complexity": simplicity_reality_check,
        },
        "root_cause_hypothesis": root_cause,
    }


def _classify_failure_layer(
    total_entry_only_pph: float,
    total_realized_pph: float,
    top_exit_branches: list[dict[str, Any]],
    aee_transformation_audit: dict[str, Any],
) -> dict[str, Any]:
    gap = total_entry_only_pph - total_realized_pph
    overfire = aee_transformation_audit.get("branch_overfire", {}) if isinstance(aee_transformation_audit, dict) else {}
    blocking_flags = overfire.get("blocking_flags", []) if isinstance(overfire, dict) else []
    primary_branch = top_exit_branches[0]["name"] if top_exit_branches else "UNKNOWN"
    secondary_branch = top_exit_branches[1]["name"] if len(top_exit_branches) > 1 else "UNKNOWN"
    if blocking_flags:
        return {
            "failure_layer": "AEE",
            "sublayer": "continuation_capture_and_giveback_logic",
            "confidence": 0.93,
            "reason": (
                f"Blocking overfire flags present ({', '.join(blocking_flags)}), "
                f"primary branch={primary_branch}, secondary branch={secondary_branch}"
            ),
        }
    if gap > 0.03:
        return {
            "failure_layer": "AEE",
            "sublayer": "exit_extraction_logic",
            "confidence": 0.87,
            "reason": (
                f"Entry-only edge remains above realized by {gap:.4f} pph "
                f"with dominant exit branch {primary_branch}"
            ),
        }
    if total_realized_pph <= 0.0:
        return {
            "failure_layer": "ENTRY",
            "sublayer": "signal_quality_or_gate_logic",
            "confidence": 0.72,
            "reason": "Realized edge is non-positive and no strong AEE gap signal dominates",
        }
    return {
        "failure_layer": "MIXED",
        "sublayer": "cross_layer",
        "confidence": 0.6,
        "reason": "No single layer dominates; inspect branch and family deltas",
    }


def _classify_overfire_flags(flags: list[str]) -> dict[str, Any]:
    severity_map = {
        "AEE_GIVEBACK_EXIT_OVERFIRE": "blocking",
        "HIGH_GIVEBACK_RATIO": "blocking",
        "AEE_BAND_FAST_FAILURE_EARLY_FIRE": "non_blocking",
    }
    classified: list[dict[str, str]] = []
    blocking_flags: list[str] = []
    non_blocking_flags: list[str] = []
    for flag in flags:
        sev = severity_map.get(flag, "non_blocking")
        classified.append({"flag": flag, "severity": sev})
        if sev == "blocking":
            blocking_flags.append(flag)
        else:
            non_blocking_flags.append(flag)
    return {
        "classified": classified,
        "blocking_flags": blocking_flags,
        "non_blocking_flags": non_blocking_flags,
        "has_blocking": bool(blocking_flags),
    }


def _auto_adjudicate_run(
    realized_pph: float,
    breakout_pph: float,
    champion_breakout_pph: float,
    transformation_audit: dict[str, Any],
    baseline_gate: dict[str, Any],
    performance_signature: dict[str, Any],
) -> dict[str, Any]:
    lc_pass = bool(transformation_audit.get("loss_compression", {}).get("pass", False))
    fw_pass = bool(transformation_audit.get("failure_to_win_conversion", {}).get("pass", False))
    cc_pass = bool(transformation_audit.get("continuation_capture", {}).get("pass", False))
    gb_pass = bool(transformation_audit.get("giveback_ratio", {}).get("pass", False))
    overfire = transformation_audit.get("branch_overfire", {})
    blocking_flags = overfire.get("blocking_flags", []) if isinstance(overfire, dict) else []

    promotion_gate_pass = all([lc_pass, fw_pass, cc_pass, gb_pass]) and not blocking_flags
    baseline_gate_pass = bool(baseline_gate.get("hard_amplifier_gate_pass", False))
    perf_patterns = performance_signature.get("auto_reject_patterns", [])
    performance_core_pass = bool(performance_signature.get("core_rule_all_four", {}).get("pass", False))
    protection_floor_ok = breakout_pph >= champion_breakout_pph * 0.9 if champion_breakout_pph > 0 else realized_pph > 0

    if not protection_floor_ok:
        verdict = "REJECT"
        reason = "primary edge below protection floor"
    elif not baseline_gate_pass:
        verdict = "REJECT"
        reason = (
            "AEE candidate fails hard amplifier gate vs minimal_protective_only "
            "and/or no_aee_loose"
        )
    elif perf_patterns:
        verdict = "REJECT"
        reason = "auto-reject performance pattern triggered: " + ", ".join(perf_patterns)
    elif not performance_core_pass:
        verdict = "REJECT"
        reason = "core four-movement rule failed (less loss, more small wins, more continuation, higher pph)"
    elif realized_pph > 0 and (not cc_pass) and (not gb_pass):
        verdict = "CONDITIONALLY_POSITIVE_BUT_STRUCTURALLY_BLOCKED"
        reason = "positive pph with continuation capture and giveback gates failed"
    elif promotion_gate_pass and realized_pph > 0:
        verdict = "PROMOTE"
        reason = "all transformation gates passed with no blocking overfire"
    elif blocking_flags:
        verdict = "BLOCKED"
        reason = f"blocking overfire flags present: {', '.join(blocking_flags)}"
    elif realized_pph > 0:
        verdict = "NO_OP"
        reason = "positive run but one or more structural gates not promotion-eligible"
    else:
        verdict = "REJECT"
        reason = "non-positive realized pph"

    return {
        "verdict": verdict,
        "reason": reason,
        "gates": {
            "loss_compression_rate_pass": lc_pass,
            "failure_to_win_conversion_rate_pass": fw_pass,
            "continuation_capture_rate_pass": cc_pass,
            "giveback_ratio_pass": gb_pass,
            "blocking_overfire_absent_pass": not bool(blocking_flags),
            "baseline_amplifier_gate_pass": baseline_gate_pass,
            "performance_core_rule_pass": performance_core_pass,
            "promotion_gate_pass": promotion_gate_pass,
        },
        "baseline_gate": baseline_gate,
        "performance_signature": performance_signature,
        "blocking_overfire_flags": blocking_flags,
        "auto_reject_patterns": perf_patterns,
        "is_structurally_healthy": bool(promotion_gate_pass),
    }


def _load_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _update_variable_map(
    variable_map_path: Path,
    run_id: str,
    primary_branch: str,
    secondary_branch: str,
    next_scope: str,
) -> dict[str, Any]:
    payload = _load_json_if_exists(variable_map_path)
    if not isinstance(payload, dict):
        return {"updated": False, "reason": "variable map missing or invalid"}
    rows = payload.get("variables")
    if not isinstance(rows, list):
        return {"updated": False, "reason": "variable map missing variables list"}

    for row in rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", ""))
        if name == "AEE_V3_FAMILY_HARD_GIVEBACK_EXIT":
            row["current_belief"] = "top_breakout_rangeescape_bottleneck"
            row["current_status"] = "primary_bottleneck"
            row["next_test_priority"] = "critical"
            obs = row.get("observed_effects", [])
            if isinstance(obs, list):
                obs.append(
                    f"{run_id}: dominant branch {primary_branch}; next scope={next_scope}"
                )
        if name == "AEE_V3_FAMILY_FAST_FAILURE_EXIT":
            row["current_belief"] = "secondary_bottleneck_after_giveback"
            row["current_status"] = "secondary_bottleneck"
            row["next_test_priority"] = "high"
            obs = row.get("observed_effects", [])
            if isinstance(obs, list):
                obs.append(
                    f"{run_id}: secondary branch {secondary_branch} after giveback dominance"
                )

    payload["updated_at"] = _iso_now()
    payload["last_run_bottleneck_update"] = {
        "run_id": run_id,
        "primary_bottleneck_branch": primary_branch,
        "secondary_bottleneck_branch": secondary_branch,
        "next_intervention_class": "AEE_INTERACTION",
        "next_scope": next_scope,
    }
    _write_json(variable_map_path, payload)
    return {"updated": True, "path": str(variable_map_path)}


def _update_priority_rank(
    priority_path: Path,
    run_id: str,
) -> dict[str, Any]:
    payload = _load_json_if_exists(priority_path)
    if not isinstance(payload, dict):
        return {"updated": False, "reason": "priority rank missing or invalid"}
    rows = payload.get("rows")
    if not isinstance(rows, list):
        return {"updated": False, "reason": "priority rank missing rows list"}

    for row in rows:
        if not isinstance(row, dict):
            continue
        var = str(row.get("variable", ""))
        if var == "AEE_V3_FAMILY_HARD_GIVEBACK_EXIT":
            row["status"] = "primary_bottleneck"
            row["expected_leverage"] = 10
            row["evidence_strength"] = 10
            row["collateral_risk"] = 5
            row["retest_readiness"] = 10
            row["priority_score"] = 105
            row["why_now"] = "Dominant giveback overfire leaks continuation capture in breakout/range contexts"
        if var == "AEE_V3_FAMILY_FAST_FAILURE_EXIT":
            row["status"] = "secondary_bottleneck"
            row["expected_leverage"] = 9
            row["evidence_strength"] = 10
            row["collateral_risk"] = 6
            row["retest_readiness"] = 9
            row["priority_score"] = 93
            row["why_now"] = "Secondary damage branch after giveback; tune only after giveback correction"

    rows_sorted = sorted(
        rows,
        key=lambda r: -int(r.get("priority_score", 0)) if isinstance(r, dict) else 0,
    )
    for idx, row in enumerate(rows_sorted, start=1):
        if isinstance(row, dict):
            row["rank"] = idx
    payload["rows"] = rows_sorted
    payload["updated_at"] = _iso_now()
    payload["last_run_bottleneck_update"] = {"run_id": run_id}
    _write_json(priority_path, payload)
    return {"updated": True, "path": str(priority_path)}


def _update_dual_champion_status(
    control_dir: Path,
    run_id: str,
    realized_pph: float,
    extraction_efficiency: float,
    champion_combined_pph: float,
    adjudication: dict[str, Any],
) -> dict[str, Any]:
    dual_path = control_dir / "champion_dual_status_active.json"
    prior = _load_json_if_exists(dual_path) or {}
    prior_structural = str(prior.get("structural_champion", "strategy_performance_report_raw.json"))

    improved_pph = realized_pph > champion_combined_pph
    improved_efficiency = extraction_efficiency > float(prior.get("latest_extraction_efficiency", 0.0))
    structural_healthy = bool(adjudication.get("is_structurally_healthy", False))

    payload = {
        "run_id": run_id,
        "updated_at": _iso_now(),
        "comparison": {
            "global_pph_improved": improved_pph,
            "extraction_efficiency_improved": improved_efficiency,
            "structural_health_improved": structural_healthy,
        },
        "performance_champion": "active_policy_latest" if improved_pph else "strategy_performance_report_raw.json",
        "structural_champion": "active_policy_latest" if structural_healthy else prior_structural,
        "latest_extraction_efficiency": _safe_round(extraction_efficiency, 6),
        "verdict": adjudication.get("verdict", "NO_OP"),
    }
    _write_json(dual_path, payload)
    return payload


def _build_next_task_recommendation(
    run_id: str,
    primary_branch: str,
    secondary_branch: str,
    root_cause_hypothesis: str,
) -> dict[str, Any]:
    if root_cause_hypothesis == "PROBLEM_3_TAXONOMY_TOO_BROAD":
        return {
            "run_id": run_id,
            "task_id": f"AUTO_NEXT_{run_id}",
            "intervention_class": "FORM_SEARCH",
            "title": "Taxonomy split implementation before AEE tuning",
            "scope": "Split mixed post-entry species inside broad families; do not change AEE logic",
            "starting_population": "family-level mixed clusters within EXPANSION_BREAKOUT and RANGE_ESCAPE",
            "module_order": [
                "split_expansion_breakout_into_clean_weak_fake",
                "split_range_escape_into_true_noise",
                "rerun_evidence_pack_with_same_aee_logic",
                "re-evaluate_simplicity_and_module_collision"
            ],
            "bounded_constraints": [
                "no AEE branch logic changes",
                "no threshold tuning",
                "no simulation changes",
                "rerun same evidence-pack after taxonomy split"
            ],
            "primary_bottleneck": "FAMILY_TAXONOMY_MIXING",
            "secondary_bottleneck": "AEE_BRANCH_COLLISION_SECONDARY",
            "target_branches": [
                "FAMILY_SPLIT::EXPANSION_BREAKOUT",
                "FAMILY_SPLIT::RANGE_ESCAPE"
            ],
            "expected_signature": {
                "family_invalid_detected": False,
                "objective_collision_detected": "weaken",
                "complexity_adds_value": "move_toward_true",
                "baseline_amplifier_gate_pass": "retest_after_split"
            },
        }

    return {
        "run_id": run_id,
        "task_id": f"AUTO_NEXT_{run_id}",
        "intervention_class": "AEE_INTERACTION",
        "title": "Protect bankable green and prune dead time",
        "scope": "BREAKOUT + RANGE_ESCAPE bankable-green protection plus dead-trade pruning",
        "starting_population": "bankable-green losers first, then negative-unproven time waste",
        "module_order": [
            "protect_bankable_green_fast",
            "kill_dead_trades_early",
            "extend_only_proven_trades",
            "apply_universal_time_pruning"
        ],
        "bounded_constraints": [
            "no global retune",
            "no entry changes",
            "no simulation changes",
            "rerun same evidence-pack after correction"
        ],
        "primary_bottleneck": primary_branch,
        "secondary_bottleneck": secondary_branch,
        "target_branches": [
            "AEE_BAND_FAST_FAILURE_EXIT",
            "AEE_GIVEBACK_EXIT"
        ],
        "expected_signature": {
            "realized_pph": "+20% to +80%",
            "gap": "-15% to -40%",
            "extraction_efficiency": "+0.05 to +0.25 absolute",
            "bankable_green_loss_red_rate": "near_zero",
            "trades_per_hour": "up",
            "avg_loser_hold_sec": "down",
            "avg_weak_winner_hold_sec": "down",
            "baseline_amplifier_gate_pass": True
        },
    }


def _compute_aee_transformation_audit(
    records: list[dict[str, Any]],
    exit_reason_counts: Counter,
) -> dict[str, Any]:
    n = len(records)
    if n == 0:
        return {
            "loss_compression": {},
            "failure_to_win_conversion": {},
            "continuation_capture": {},
            "giveback_ratio": {},
            "branch_role_audit": {},
            "branch_overfire_flags": [],
        }

    def _mean(vals: list[float]) -> float:
        return sum(vals) / len(vals) if vals else 0.0

    structural_losses = [
        abs(min(float(r.get("structural_pips_proxy", 0.0)), 0.0)) for r in records
        if float(r.get("structural_pips_proxy", 0.0)) < 0.0
    ]
    realized_losses = [
        abs(min(float(r.get("realized_pips", 0.0)), 0.0)) for r in records
        if float(r.get("realized_pips", 0.0)) < 0.0
    ]
    avg_structural_loss = _mean(structural_losses)
    avg_realized_loss = _mean(realized_losses)
    loss_compression_rate = (
        1.0 - (avg_realized_loss / avg_structural_loss)
        if avg_structural_loss > 0
        else 0.0
    )

    would_lose = [r for r in records if float(r.get("structural_pips_proxy", 0.0)) < 0.0]
    converted = [r for r in would_lose if float(r.get("realized_pips", 0.0)) > 0.0]
    failure_to_win_conversion_rate = (len(converted) / len(would_lose)) if would_lose else 0.0

    continuation_pool = []
    realized_from_mfe = []
    giveback_abs = []
    for r in records:
        mfe_net = max(float(r.get("mfe_net", 0.0)), 0.0)
        if mfe_net <= 0.0:
            continue
        realized_pos = max(float(r.get("realized_pips", 0.0)), 0.0)
        realized_clip = min(realized_pos, mfe_net)
        continuation_pool.append(mfe_net)
        realized_from_mfe.append(realized_clip)
        giveback_abs.append(max(mfe_net - realized_clip, 0.0))

    continuation_capture_rate = (
        sum(realized_from_mfe) / sum(continuation_pool)
        if continuation_pool and sum(continuation_pool) > 0
        else 0.0
    )
    giveback_ratio = (
        sum(giveback_abs) / sum(continuation_pool)
        if continuation_pool and sum(continuation_pool) > 0
        else 0.0
    )

    role_map = {
        "AEE_BAND_FAST_FAILURE_EXIT": "loss_compression",
        "AEE_NEVER_GREEN_TIMEOUT": "failure_to_win_conversion",
        "AEE_GIVEBACK_EXIT": "continuation_capture_and_profit_protection",
        "AEE_CONTINUATION_FAILED_EXIT": "late_failure",
    }
    branch_share = {
        k: (v / n) for k, v in sorted(exit_reason_counts.items(), key=lambda x: (-x[1], x[0]))
    }
    overfire_flags: list[str] = []
    giveback_share = branch_share.get("AEE_GIVEBACK_EXIT", 0.0)
    fast_fail_share = branch_share.get("AEE_BAND_FAST_FAILURE_EXIT", 0.0)
    if giveback_share >= 0.55 and continuation_capture_rate < 0.45:
        overfire_flags.append("AEE_GIVEBACK_EXIT_OVERFIRE")
    if fast_fail_share >= 0.35 and loss_compression_rate < 0.40:
        overfire_flags.append("AEE_BAND_FAST_FAILURE_EARLY_FIRE")
    if giveback_ratio > 0.60:
        overfire_flags.append("HIGH_GIVEBACK_RATIO")

    return {
        "loss_compression": {
            "avg_loss_after_aee": _safe_round(avg_realized_loss, 6),
            "avg_structural_sl_loss": _safe_round(avg_structural_loss, 6),
            "loss_compression_rate": _safe_round(loss_compression_rate, 6),
            "pass": bool(loss_compression_rate >= 0.30),
        },
        "failure_to_win_conversion": {
            "structural_loser_count": len(would_lose),
            "converted_winner_count": len(converted),
            "failure_to_win_conversion_rate": _safe_round(failure_to_win_conversion_rate, 6),
            "pass": bool(failure_to_win_conversion_rate >= 0.15),
        },
        "continuation_capture": {
            "avg_realized_from_mfe": _safe_round(_mean(realized_from_mfe), 6),
            "avg_mfe_net": _safe_round(_mean(continuation_pool), 6),
            "continuation_capture_rate": _safe_round(continuation_capture_rate, 6),
            "pass": bool(continuation_capture_rate >= 0.40),
        },
        "giveback_ratio": {
            "value": _safe_round(giveback_ratio, 6),
            "pass": bool(giveback_ratio <= 0.55),
        },
        "branch_role_audit": {
            "role_map": role_map,
            "branch_share": {k: _safe_round(v, 6) for k, v in branch_share.items()},
        },
        "branch_overfire_flags": overfire_flags,
    }


def _build_family_stats(
    records: list[dict[str, Any]],
    total_hours: float,
    friction_per_trade: float,
) -> dict[str, Any]:
    fam_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rec in records:
        fam_groups[rec["family"]].append(rec)

    results = {}
    for fam, recs in fam_groups.items():
        net_pips_list = [r["net_pips"] for r in recs]
        n = len(recs)
        wins = sum(1 for p in net_pips_list if p > 0)
        total_net = sum(net_pips_list)
        total_gross = sum(r["gross_pips"] for r in recs)
        avg_ppt = total_gross / n if n else 0.0
        avg_net_ppt = total_net / n if n else 0.0
        win_rate = wins / n if n else 0.0
        tph = n / total_hours if total_hours > 0 else 0.0
        net_pph = total_net / total_hours if total_hours > 0 else 0.0
        gross_pph = total_gross / total_hours if total_hours > 0 else 0.0
        avg_hold = sum(r.get("hold_sec", 0.0) for r in recs) / n if n else 0.0
        avg_mfe = sum(r.get("mfe", 0.0) for r in recs) / n if n else 0.0
        avg_mae = sum(r.get("mae", 0.0) for r in recs) / n if n else 0.0
        avg_entry_only = sum(r.get("entry_only_pips", 0.0) for r in recs) / n if n else 0.0
        gap_proxy = avg_entry_only - avg_net_ppt

        results[fam] = {
            "family": fam,
            "trade_count": n,
            "trades_per_hour": _safe_round(tph, 4),
            "win_count": wins,
            "win_rate": _safe_round(win_rate, 4),
            "avg_gross_pips_per_trade": _safe_round(avg_ppt, 4),
            "avg_net_pips_per_trade": _safe_round(avg_net_ppt, 4),
            "avg_mfe_pips": _safe_round(avg_mfe, 4),
            "avg_mae_pips": _safe_round(avg_mae, 4),
            "avg_hold_sec": _safe_round(avg_hold, 2),
            "avg_entry_only_pips": _safe_round(avg_entry_only, 4),
            "entry_only_vs_realized_gap_pips": _safe_round(gap_proxy, 4),
            "gross_pips_per_hour": _safe_round(gross_pph, 4),
            "net_pips_per_hour": _safe_round(net_pph, 4),
            "total_net_pips": _safe_round(total_net, 4),
            "total_gross_pips": _safe_round(total_gross, 4),
            "verdict": _verdict(net_pph),
        }
    return results


def _print_evidence_block(
    run_id: str,
    champion_reference: str,
    intervention_class: str,
    strategy_form: str,
    aee_version: str,
    simulation_mode: str,
    data_coverage: dict[str, Any],
    config_snapshot: dict[str, Any],
    ranked_by_pph: list[dict[str, Any]],
    top_entry_branches: list[dict[str, Any]],
    top_exit_branches: list[dict[str, Any]],
    all_records: list[dict[str, Any]],
    total_hours: float,
    total_entry_only_pph: float,
    total_realized_pph: float,
    gap_pph: float,
    extraction_efficiency: float,
    baseline_ab: dict[str, Any],
    expected_vs_actual: dict[str, Any],
    aee_transformation_audit: dict[str, Any],
    adjudication: dict[str, Any],
    failure_layer: dict[str, Any],
    champion_breakout_pph: float,
    champion_combined_pph: float,
    family_stats: dict[str, Any],
    champions_by_family: dict[str, float],
    skipped_entry: Counter,
) -> None:
    """Print the full 5-layer result truth block to stdout."""
    div = "=" * 72
    thin = "-" * 72

    n_trades = len(all_records)
    tph = n_trades / total_hours if total_hours > 0 else 0.0
    avg_net_ppt = sum(r["net_pips"] for r in all_records) / n_trades if n_trades else 0.0

    breakout = family_stats.get("EXPANSION_BREAKOUT", {})
    breakout_pph = breakout.get("net_pips_per_hour", 0.0)
    breakout_pph_delta = breakout_pph - champion_breakout_pph

    # --- COMPACT TRUTH BLOCK ---
    print()
    print(div)
    print("  AEE RUN — FULL EVIDENCE PACK")
    print(div)
    print()
    print(f"  RUN_ID:              {run_id}")
    print(f"  CHAMPION_REFERENCE:  {champion_reference}")
    print(f"  INTERVENTION_CLASS:  {intervention_class}")
    print(f"  STRATEGY_FORM:       {strategy_form}")
    print(f"  AEE_VERSION:         {aee_version}")
    print(f"  SIMULATION_MODE:     {simulation_mode}")
    print()

    # ── LAYER 1: CONFIG TRUTH ─────────────────────────────────────────
    print(thin)
    print("  LAYER 1 — CONFIG TRUTH")
    print(thin)
    thresh = config_snapshot.get("thresholds", {})
    enabled = config_snapshot.get("enabled_gates", {})
    disabled = config_snapshot.get("disabled_gates", {})
    fams = config_snapshot.get("strategy_family", [])
    excl_ctx = disabled.get("excluded_contexts", [])
    print(f"  Families included:     {', '.join(sorted(fams)) if fams else 'ALL'}")
    print(f"  Spread (pips):         {thresh.get('spread_pips', '?')}")
    print(f"  Slippage (per side):   {thresh.get('slippage_pips_per_side', '?')}")
    print(f"  Commission (rt):       {thresh.get('commission_pips_roundtrip', '?')}")
    print(f"  Eco viability mult:    {thresh.get('economic_viability_mult', '?')}")

    # AEE v3 family thresholds
    print()
    print("  AEE v3 family thresholds (EXPANSION_BREAKOUT):")
    print("    fast_fail_pips:           -1.6")
    print("    never_green_min (pips):   0.3")
    print("    never_green_timeout_min:  3.0")
    print("    promote_pips:             1.1")
    print("    stall_timeout_min:        1.5")
    print("    soft_giveback:            0.6")
    print("    hard_giveback:            1.2")
    print()
    print("  Enabled gates:")
    print(f"    micro_confirm:            {enabled.get('micro_confirm', False)}")
    print(f"    displacement_rules:       {enabled.get('displacement_rules', 0)}")
    print(f"    progress_rules:           {enabled.get('progress_rules', 0)}")
    print(f"    release_quality_rules:    {enabled.get('release_quality_rules', 0)}")
    print(f"    noise_rules:              {enabled.get('noise_rules', 0)}")
    print()
    print(f"  Disabled gates (raw_core_mode): {disabled.get('raw_core_mode', False)}")
    if excl_ctx:
        print(f"  Excluded contexts ({len(excl_ctx)}):")
        for ctx in sorted(excl_ctx)[:6]:
            print(f"    - {ctx}")
        if len(excl_ctx) > 6:
            print(f"    ... +{len(excl_ctx) - 6} more")

    # ── LAYER 2: LOGIC TRUTH ──────────────────────────────────────────
    print()
    print(thin)
    print("  LAYER 2 — LOGIC TRUTH")
    print(thin)
    print("  Top entry detector paths (by trade count):")
    for b in top_entry_branches[:5]:
        pct = b["count"] / n_trades * 100 if n_trades else 0
        print(f"    {b['name']:<30} {b['count']:>6,} trades  ({pct:5.1f}%)")
    block_total = sum(v for _, v in skipped_entry.items())
    print(f"\n  Entry blocks: {block_total:,} total")
    for reason, cnt in skipped_entry.most_common(5):
        print(f"    {reason:<40} {cnt:>6,}")
    print()
    print("  Top exit branches (by count):")
    for b in top_exit_branches[:6]:
        pct = b["count"] / n_trades * 100 if n_trades else 0
        pct_bar = "█" * int(pct / 2)
        print(f"    {b['name']:<38} {b['count']:>6,}  ({pct:5.1f}%)  {pct_bar}")
    print()
    print("  State transitions used (dominant → subordinate):")
    state_counts_raw = Counter(str(r.get("state", "UNKNOWN")) for r in all_records)
    for state, cnt in state_counts_raw.most_common():
        pct = cnt / n_trades * 100 if n_trades else 0
        print(f"    {state:<20} {cnt:>6,}  ({pct:5.1f}%)")

    # ── LAYER 3: TRADE TRUTH (SAMPLE) ────────────────────────────────
    print()
    print(thin)
    print("  LAYER 3 — TRADE TRUTH (SAMPLE)")
    print(thin)
    hdr = f"  {'Family':<22} {'Context':<30} {'Net pips':>9} {'MFE':>6} {'Hold sec':>9} Exit branch"
    print(hdr)
    print(f"  {thin}")
    all_records_sorted = sorted(all_records, key=lambda r: -r.get("net_pips", 0.0))
    winners_sample = [r for r in all_records_sorted if r.get("net_pips", 0.0) > 0][:6]
    losers_sample = sorted(
        [r for r in all_records if r.get("net_pips", 0.0) < 0],
        key=lambda r: r.get("net_pips", 0.0),
    )[:6]
    ambig_sample = [r for r in all_records if abs(float(r.get("net_pips", 0.0))) <= 0.15][:4]

    def _print_trade_row(r: dict[str, Any], tag: str) -> None:
        fam = str(r.get("family", "?"))[:22]
        ctx = str(r.get("context", "?"))[:30]
        net = _safe_round(r.get("net_pips", 0.0), 4)
        mfe = _safe_round(r.get("mfe", 0.0), 2)
        hold = _safe_round(r.get("hold_sec", 0.0), 0)
        branch = str(r.get("exit_reason", "?"))
        print(f"  {fam:<22} {ctx:<30} {net:>+9.4f} {mfe:>6.2f} {hold:>9.0f}  {branch}  [{tag}]")

    print("  > WINNERS (top 6 net pips):")
    for r in winners_sample:
        _print_trade_row(r, "W")
    print("  > LOSERS (worst 6 net pips):")
    for r in losers_sample:
        _print_trade_row(r, "L")
    print("  > AMBIGUOUS (≤0.15 net pips, first 4):")
    for r in ambig_sample:
        _print_trade_row(r, "A")

    # ── LAYER 4: DATA TRUTH ───────────────────────────────────────────
    print()
    print(thin)
    print("  LAYER 4 — DATA TRUTH")
    print(thin)
    print(f"  Pairs:          {', '.join(data_coverage.get('pair_coverage', []))}")
    print(f"  Streams:        {data_coverage.get('streams', '?')}")
    print(f"  Hours:          {data_coverage.get('hours', 0.0):,.1f}")
    print(f"  Unique days:    {data_coverage.get('unique_days', '?')}")
    sess = data_coverage.get("sessions_represented", {})
    print(f"  Sessions:       {dict(sess)}")
    dom = data_coverage.get("dominance_concentration", {})
    print(f"  Regime dist (trades admitted):")
    regime_dist = data_coverage.get("regime_distribution", {})
    for fam, cnt in sorted(regime_dist.items(), key=lambda x: -x[1]):
        pct = cnt / n_trades * 100 if n_trades else 0
        print(f"    {fam:<28} {cnt:>6,}  ({pct:5.1f}%)")
    print(f"  Top family (by trade share): {dom.get('top_family', '?')}  "
          f"({dom.get('top_family_trade_share', 0)*100:.1f}%)")

    # ── LAYER 5: CAUSAL TRUTH ─────────────────────────────────────────
    print()
    print(thin)
    print("  LAYER 5 — CAUSAL TRUTH")
    print(thin)
    print("  Signature check:")
    expected = expected_vs_actual.get("expected_signature", {})
    actual = expected_vs_actual.get("actual_signature", {})
    delta = expected_vs_actual.get("delta_vs_champion", {})
    matches = expected_vs_actual.get("matches_expected_model", False)
    for metric, exp_val in expected.items():
        act_val = actual.get(metric, "?")
        print(f"    {metric:<38}: expected={exp_val}  actual={act_val}")
    print()
    print("  A/B baseline check (AEE must beat simpler alternatives):")
    for mode, d in baseline_ab.get("candidate_vs_baselines", {}).items():
        print(
            f"    vs {mode:<26}: Δpph={float(d.get('delta_realized_pph', 0.0)):+.5f}  "
            f"Δavg_ppt={float(d.get('delta_avg_net_pips_per_trade', 0.0)):+.5f}"
        )
    print()
    print("  AEE transformation audit:")
    lc = aee_transformation_audit.get("loss_compression", {})
    fw = aee_transformation_audit.get("failure_to_win_conversion", {})
    cc = aee_transformation_audit.get("continuation_capture", {})
    gb = aee_transformation_audit.get("giveback_ratio", {})
    print(f"    loss_compression_rate             : {lc.get('loss_compression_rate', 0.0):.4f}  pass={lc.get('pass', False)}")
    print(f"    failure_to_win_conversion_rate    : {fw.get('failure_to_win_conversion_rate', 0.0):.4f}  pass={fw.get('pass', False)}")
    print(f"    continuation_capture_rate         : {cc.get('continuation_capture_rate', 0.0):.4f}  pass={cc.get('pass', False)}")
    print(f"    giveback_ratio                    : {gb.get('value', 0.0):.4f}  pass={gb.get('pass', False)}")
    overfire = aee_transformation_audit.get("branch_overfire", {})
    overfire_flags = overfire.get("blocking_flags", []) + overfire.get("non_blocking_flags", [])
    print(f"    branch_overfire_flags             : {', '.join(overfire_flags) if overfire_flags else 'NONE'}")
    print(f"    blocking_overfire_flags           : {', '.join(overfire.get('blocking_flags', [])) if overfire.get('blocking_flags') else 'NONE'}")
    print("  Hard adjudication gates:")
    gates = adjudication.get("gates", {})
    for k, v in gates.items():
        print(f"    {k:<38}: {v}")
    print()
    print("  Delta vs champion:")
    print(f"    breakout_net_pph:          {delta.get('breakout_net_pph', 0.0):+.5f}  "
          f"(champion={champion_breakout_pph:+.5f}  run={breakout_pph:+.5f})")
    print(f"    combined_keep_tune_net_pph: {delta.get('combined_keep_tune_net_pph', 0.0):+.5f}  "
          f"(champion={champion_combined_pph:+.5f}  run={sum(s.get('net_pips_per_hour',0) for s in family_stats.values() if s.get('verdict') in ('KEEP','TUNE')):+.5f})")
    print()
    print("  Per-family summary:")
    print(f"  {'Family':<28} {'T/hr':>6} {'Avg net ppt':>12} {'Win%':>6} {'Net pph':>9} {'Gap pips':>10}  Verdict")
    print(f"  {thin}")
    for s in sorted(family_stats.values(), key=lambda x: -x.get("net_pips_per_hour", 0.0)):
        verdict_mark = {"KEEP": "✓✓ KEEP", "TUNE": "~  TUNE", "KILL": "✗  KILL"}.get(s["verdict"], s["verdict"])
        print(
            f"  {s['family']:<28} {s['trades_per_hour']:>6.4f} "
            f"{s['avg_net_pips_per_trade']:>+12.4f} {s['win_rate']:>5.1%} "
            f"{s['net_pips_per_hour']:>+9.5f} {s['entry_only_vs_realized_gap_pips']:>+10.4f}  {verdict_mark}"
        )
    print()

    # ── COMBINED RESULTS BLOCK ────────────────────────────────────────
    print(thin)
    print("  LAYER AGGREGATE — FULL RESULTS")
    print(thin)
    print(f"  trade_count:              {n_trades:,}")
    print(f"  trades_per_hour:          {tph:.5f}")
    print(f"  avg_net_pips_per_trade:   {avg_net_ppt:+.5f}")
    print(f"  entry_only_pph:           {total_entry_only_pph:+.5f}")
    print(f"  realized_pph (combined):  {total_realized_pph:+.5f}")
    print(f"  gap (entry - realized):   {gap_pph:+.5f}")
    print(f"  extraction_efficiency:    {extraction_efficiency:+.5f}")
    print(f"  loss_compression_rate:    {lc.get('loss_compression_rate', 0.0):+.5f}")
    print(f"  failure_to_win_rate:      {fw.get('failure_to_win_conversion_rate', 0.0):+.5f}")
    print(f"  continuation_capture:     {cc.get('continuation_capture_rate', 0.0):+.5f}")
    print(f"  giveback_ratio:           {gb.get('value', 0.0):+.5f}")
    print(f"  breakout_net_pph:         {breakout_pph:+.5f}  (Δ vs champion: {breakout_pph_delta:+.5f})")
    print()
    print(f"  TOP DAMAGE:")
    print(f"    primary:   {top_exit_branches[0]['name'] if top_exit_branches else '?'}")
    print(f"    secondary: {top_exit_branches[1]['name'] if len(top_exit_branches) > 1 else '?'}")
    print()
    print(f"  FAILURE LAYER:  {failure_layer['failure_layer']}  (confidence: {failure_layer['confidence']:.2f})")
    print(f"  SUBLAYER:       {failure_layer.get('sublayer', 'UNKNOWN')}")
    print(f"  REASON:         {failure_layer['reason']}")
    print()

    # ── VERDICT ───────────────────────────────────────────────────────
    print(div)
    verdict = str(adjudication.get("verdict", "NO_OP"))
    reason = str(adjudication.get("reason", ""))
    if verdict == "PROMOTE":
        vmark = "✓✓ PROMOTE"
    elif verdict == "REJECT":
        vmark = "✗  REJECT"
    elif verdict == "BLOCKED":
        vmark = "!  BLOCKED"
    elif verdict == "CONDITIONALLY_POSITIVE_BUT_STRUCTURALLY_BLOCKED":
        vmark = "~  CONDITIONALLY_POSITIVE_BUT_STRUCTURALLY_BLOCKED"
    else:
        vmark = "~  NO_OP"

    print(f"  VERDICT: {vmark} — {reason}")
    print(div)
    print()


def main() -> None:
    ap = argparse.ArgumentParser(description="AEE Active-Policy Evidence-Pack Runner")
    ap.add_argument("--config", default="entry_v23_policy_guarded_active.json")
    ap.add_argument("--pair", default="EUR_USD")
    ap.add_argument("--spread-pips", type=float, default=0.8)
    ap.add_argument("--slippage-pips-per-side", type=float, default=0.15)
    ap.add_argument("--commission-pips-roundtrip", type=float, default=0.0)
    ap.add_argument("--latency-penalty-pips", type=float, default=0.0)
    ap.add_argument("--economic-viability-mult", type=float, default=1.10)
    ap.add_argument("--raw-core-mode", action="store_true")
    ap.add_argument("--max-streams", type=int, default=999)
    ap.add_argument("--champion-reference", default="strategy_performance_report_raw.json")
    ap.add_argument("--intervention-class", default="AEE_V3_ACTIVE_POLICY")
    ap.add_argument("--strategy-form", default="entry_v23_policy_guarded_active")
    ap.add_argument("--aee-version", default="v3")
    ap.add_argument("--simulation-mode", default="ACTIVE_POLICY_REPLAY")
    ap.add_argument("--dataset-window-id", default="EUR_USD_16_STREAM_FULL")
    ap.add_argument("--intervention-basis", default="control/aee_intervention_basis.json")
    ap.add_argument("--baseline-static-tp-pips", type=float, default=2.0)
    ap.add_argument("--baseline-static-sl-pips", type=float, default=2.0)
    ap.add_argument("--baseline-protective-sl-pips", type=float, default=1.6)
    ap.add_argument("--result-dir", default="control")
    ap.add_argument("--slice-file", default="")
    ap.add_argument("--slice-label", default="")
    ap.add_argument("--include-contexts", default="")
    ap.add_argument("--include-trade-ids", default="")
    ap.add_argument("--max-trades", type=int, default=0)
    ap.add_argument("--run-id", default="")
    ap.add_argument("--out", default="control/active_policy_v23_run_report.json")
    ap.add_argument("--context-out", default="control/active_policy_v23_run_by_context.json")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = (root / cfg_path).resolve()

    if not cfg_path.exists():
        print(f"ERROR: config not found: {cfg_path}", file=sys.stderr)
        raise SystemExit(1)

    basis_path = Path(args.intervention_basis)
    if not basis_path.is_absolute():
        basis_path = (root / basis_path).resolve()
    intervention_basis = _load_and_validate_intervention_basis(basis_path)

    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
    cfg.setdefault("extraction", {})["aee_version"] = str(args.aee_version).strip().lower()
    entry_filters = cfg.get("entry_filters") or {}

    include_families = {
        str(x).upper().strip()
        for x in (entry_filters.get("include_entry_families") or [])
        if str(x).strip()
    }
    exclude_families: set[str] = set()
    exclude_contexts_lc = {
        str(x).lower().strip() for x in entry_filters.get("exclude_contexts", [])
    }
    include_pairs = {
        str(x).upper().strip()
        for x in entry_filters.get("include_pairs", [])
        if str(x).strip()
    }
    include_sessions = {
        str(x).upper().strip()
        for x in entry_filters.get("include_sessions", [])
        if str(x).strip()
    }
    min_profit_now_pips_by_bar = list(entry_filters.get("min_profit_now_pips_by_bar", []))
    min_progress_ratio_by_bar = list(entry_filters.get("min_progress_ratio_by_bar", []))
    min_release_quality_by_bar = list(entry_filters.get("min_release_quality_by_bar", []))
    max_noise_by_bar = list(entry_filters.get("max_noise_by_bar", []))
    micro_confirm = dict(entry_filters.get("micro_confirm", {}))

    cli_include_contexts = _parse_csv_set(args.include_contexts)
    cli_include_trade_ids = {x.strip() for x in str(args.include_trade_ids or "").split(",") if x.strip()}
    file_include_contexts: set[str] = set()
    file_include_trade_ids: set[str] = set()
    slice_label = str(args.slice_label or "").strip()
    if str(args.slice_file or "").strip():
        slice_path = Path(args.slice_file)
        if not slice_path.is_absolute():
            slice_path = (root / slice_path).resolve()
        file_include_contexts, file_include_trade_ids, file_slice_label = _load_slice_filters(slice_path)
        if file_slice_label and not slice_label:
            slice_label = file_slice_label

    slice_include_contexts = file_include_contexts | cli_include_contexts
    slice_include_trade_ids = file_include_trade_ids | cli_include_trade_ids

    if args.raw_core_mode:
        include_families = {
            "EXPANSION_BREAKOUT", "RECLAIM_CONTINUATION",
            "PULLBACK_CONTINUATION", "RANGE_ESCAPE", "OTHER",
        }
        exclude_families = set()
        exclude_contexts_lc = set()
        include_pairs = set()
        include_sessions = set()
        min_profit_now_pips_by_bar = []
        min_progress_ratio_by_bar = []
        min_release_quality_by_bar = []
        max_noise_by_bar = []
        micro_confirm = {"enabled": False}

    friction_per_trade = (
        max(0.0, float(args.spread_pips))
        + 2.0 * max(0.0, float(args.slippage_pips_per_side))
        + max(0.0, float(args.commission_pips_roundtrip))
        + max(0.0, float(args.latency_penalty_pips))
    )

    pair = args.pair.upper().replace("/", "_")
    stream_glob = f"compiled_market_nodes/{pair}__*/aee_stage/aee_state_stream/aee_state_stream.csv"
    streams = sorted({p.resolve() for p in root.glob(stream_glob) if p.is_file()})
    streams = streams[: max(1, args.max_streams)]

    if not streams:
        print(f"ERROR: No streams for pair={pair} (glob: {stream_glob})", file=sys.stderr)
        raise SystemExit(1)

    run_id = (
        args.run_id.strip()
        or f"AEE_ACTIVE_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    )

    print(f"[{run_id}] Active-policy run  config={cfg_path.name}  pair={pair}  "
          f"streams={len(streams)}  raw_core={'ON' if args.raw_core_mode else 'OFF'}")
    print(f"  Families: {sorted(include_families)}")
    print(f"  Excluded contexts: {len(exclude_contexts_lc)}")
    if slice_include_contexts or slice_include_trade_ids or int(args.max_trades) > 0:
        print(
            "  Slice: "
            f"contexts={len(slice_include_contexts)} "
            f"trade_ids={len(slice_include_trade_ids)} "
            f"max_trades={int(args.max_trades)} "
            f"label={slice_label or 'N/A'}"
        )
    print(f"  Gates: "
          f"micro_confirm={'ON' if micro_confirm.get('enabled') else 'OFF'}  "
          f"displacement={len(min_profit_now_pips_by_bar)}  "
          f"progress={len(min_progress_ratio_by_bar)}  "
          f"noise={len(max_noise_by_bar)}")

    all_records: list[dict[str, Any]] = []
    total_hours = 0.0
    hours_by_context: dict[str, float] = {}
    skipped_entry: Counter = Counter()
    family_seen: Counter = Counter()
    sessions_seen: Counter = Counter()
    unique_days: set[str] = set()
    unique_pairs: set[str] = set()
    max_trades = max(0, int(args.max_trades))

    for sp in streams:
        rows = _load_rows(sp)
        if not rows:
            continue
        pair_str, day, session, context = _context_from_stream(root, sp)
        context_lc = context.lower()
        if slice_include_contexts and context_lc not in slice_include_contexts:
            continue
        unique_days.add(day)
        unique_pairs.add(pair_str)
        sessions_seen[session] += 1
        stream_hours = _stream_duration_hours(rows)
        total_hours += stream_hours
        hours_by_context[context] = hours_by_context.get(context, 0.0) + stream_hours

        by_trade: dict[str, list] = {}
        for r in rows:
            tid = str(r.get("trade_id", ""))
            by_trade.setdefault(tid, []).append(r)

        for trade_id, trows in by_trade.items():
            if max_trades > 0 and len(all_records) >= max_trades:
                break
            if slice_include_trade_ids and str(trade_id) not in slice_include_trade_ids:
                continue
            trows.sort(key=lambda x: _safe_int(x.get("bar_index", 0), 0))
            if not trows:
                continue
            inferred_family = _infer_trade_family(trows)
            family_seen[inferred_family] += 1

            filter_eval = _entry_filter_evaluate(
                trows, pair_str, context_lc,
                include_families, exclude_families, exclude_contexts_lc,
                min_profit_now_pips_by_bar, min_progress_ratio_by_bar,
                min_release_quality_by_bar, max_noise_by_bar,
                micro_confirm,
                include_pairs=include_pairs,
                include_sessions=include_sessions,
                family_specific_filters=None,
                inferred_family=inferred_family,
            )
            if filter_eval.get("blocked"):
                skipped_entry[str(filter_eval.get("reason"))] += 1
                continue

            aee = _eval_trade_baseline(
                trows, cfg,
                friction_per_trade_pips=friction_per_trade,
                economic_value_margin_mult=float(args.economic_viability_mult),
                spread_fallback_pips=max(0.0, float(args.spread_pips)),
            )

            gross_pips = _safe_float(aee.get("gross_pips", aee.get("pips", 0.0)), 0.0)
            net_pips = gross_pips - friction_per_trade
            entry_only_pips = max(
                0.0,
                max((_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows), default=0.0)
                - friction_per_trade,
            )
            mfe = max((_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows), default=0.0)
            mae = min((_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows), default=0.0)
            green_indices = [idx for idx, row in enumerate(trows) if _safe_float(row.get("profit_now", 0.0), 0.0) > 0.0]
            peak_idx = max(
                range(len(trows)),
                key=lambda idx: _safe_float(trows[idx].get("profit_now", 0.0), 0.0),
            ) if trows else 0
            peak_row = trows[peak_idx] if trows else {}
            first_green_row = trows[green_indices[0]] if green_indices else None
            exit_row = trows[-1]
            path_shape = _infer_path_shape(trows, peak_idx, net_pips)
            profit_given_back = max(0.0, mfe - net_pips)
            hold_sec = _safe_float(aee.get("hold_sec", len(trows) * 60.0), len(trows) * 60.0)
            mfe_net = max(0.0, mfe - friction_per_trade)
            economic_thresholds = _economic_thresholds(friction_per_trade)
            green_tier = _classify_green_tier(mfe_net, economic_thresholds)
            dead_trade_subtype = _classify_dead_trade_subtype(
                mfe_net=mfe_net,
                hold_sec=hold_sec,
                mae=mae,
                realized_pips=net_pips,
                path_shape=path_shape,
                thresholds=economic_thresholds,
            )
            economic_state = _classify_economic_state(
                mfe_net=mfe_net,
                realized_pips=net_pips,
                hold_sec=hold_sec,
                thresholds=economic_thresholds,
            )
            taxonomy_family = _split_family_taxonomy(
                base_family=inferred_family,
                path_shape=path_shape,
                mfe_net=mfe_net,
                realized_pips=net_pips,
                thresholds=economic_thresholds,
            )
            retained_rate_pips_per_min = _retained_rate_pips_per_min(net_pips, hold_sec)
            winner_taxonomy = _classify_winner_taxonomy(
                realized_pips=net_pips,
                mfe_net=mfe_net,
                hold_sec=hold_sec,
                thresholds=economic_thresholds,
            )

            all_records.append({
                "trade_id": trade_id,
                "pair": pair_str,
                "context": context,
                "day": day,
                "session": session,
                "family": taxonomy_family,
                "family_base": inferred_family,
                "gross_pips": gross_pips,
                "net_pips": net_pips,
                "mfe": mfe,
                "mfe_net": mfe_net,
                "mae": mae,
                "went_green": bool(green_indices),
                "green_tier": green_tier,
                "dead_trade_subtype": dead_trade_subtype,
                "economic_state": economic_state,
                "economic_thresholds": economic_thresholds,
                "retained_rate_pips_per_min": retained_rate_pips_per_min,
                "winner_taxonomy": winner_taxonomy,
                "entry_time": str(trows[0].get("entry_time", "")) if trows else "",
                "time_of_first_green": str(first_green_row.get("timestamp", "")) if first_green_row else None,
                "time_of_peak_mfe": str(peak_row.get("timestamp", "")),
                "time_of_exit": str(exit_row.get("timestamp", "")),
                "lifecycle_at_peak": str(peak_row.get("lifecycle_label", "UNKNOWN")),
                "path_shape": path_shape,
                "profit_given_back": profit_given_back,
                "entry_only_pips": entry_only_pips,
                "structural_pips_proxy": _safe_float(trows[-1].get("profit_now", 0.0), 0.0) - friction_per_trade,
                "realized_pips": net_pips,
                "friction_per_trade": friction_per_trade,
                "hold_sec": hold_sec,
                "exit_reason": str(aee.get("reason", "UNKNOWN")),
                "aee_branch": str(aee.get("reason", "UNKNOWN")),
                "state": str(aee.get("state", "UNKNOWN")),
                "logic_path": {
                    "detector": f"FAMILY::{inferred_family}",
                    "gating": "ENTRY_FILTER_EVALUATE",
                    "exit": str(aee.get("reason", "UNKNOWN")),
                },
                "baseline_net": {
                    "aee_candidate": net_pips,
                    "static_tp_sl": _eval_baseline_trade_net(
                        trows,
                        mode="static_tp_sl",
                        friction_per_trade=friction_per_trade,
                        static_tp_pips=float(args.baseline_static_tp_pips),
                        static_sl_pips=float(args.baseline_static_sl_pips),
                        protective_sl_pips=float(args.baseline_protective_sl_pips),
                    ),
                    "minimal_protective_only": _eval_baseline_trade_net(
                        trows,
                        mode="minimal_protective_only",
                        friction_per_trade=friction_per_trade,
                        static_tp_pips=float(args.baseline_static_tp_pips),
                        static_sl_pips=float(args.baseline_static_sl_pips),
                        protective_sl_pips=float(args.baseline_protective_sl_pips),
                    ),
                    "no_aee_loose": _eval_baseline_trade_net(
                        trows,
                        mode="no_aee_loose",
                        friction_per_trade=friction_per_trade,
                        static_tp_pips=float(args.baseline_static_tp_pips),
                        static_sl_pips=float(args.baseline_static_sl_pips),
                        protective_sl_pips=float(args.baseline_protective_sl_pips),
                    ),
                },
            })

        if max_trades > 0 and len(all_records) >= max_trades:
            break

    if not all_records:
        print("ERROR: No trades passed entry filter. Check config or stream paths.", file=sys.stderr)
        raise SystemExit(1)

    n_trades = len(all_records)
    family_stats = _build_family_stats(all_records, total_hours, friction_per_trade)
    ranked = sorted(family_stats.values(), key=lambda x: -x["net_pips_per_hour"])

    exit_reason_counts = Counter(str(r.get("exit_reason", "UNKNOWN")) for r in all_records)
    state_counts = Counter(str(r.get("state", "UNKNOWN")) for r in all_records)
    detector_counts = Counter(str(r.get("family", "UNKNOWN")) for r in all_records)

    total_entry_only = sum(float(r.get("entry_only_pips", 0.0)) for r in all_records)
    total_realized = sum(float(r.get("realized_pips", 0.0)) for r in all_records)
    total_entry_only_pph = total_entry_only / total_hours if total_hours > 0 else 0.0
    total_realized_pph = total_realized / total_hours if total_hours > 0 else 0.0
    gap_pph = total_entry_only_pph - total_realized_pph
    extraction_efficiency = (total_realized / total_entry_only) if total_entry_only > 0 else 0.0

    top_entry_branches = _sorted_top(dict(detector_counts), top_n=5)
    top_exit_branches = _sorted_top(dict(exit_reason_counts), top_n=8)
    aee_transformation_audit = _compute_aee_transformation_audit(all_records, exit_reason_counts)
    green_loss_audit = _build_green_loss_audit(all_records)
    failure_bucket_dashboard = _build_failure_bucket_dashboard(all_records)
    family_taxonomy_audit = _build_family_taxonomy_audit(all_records)
    module_collision_audit = _build_module_collision_audit(all_records, green_loss_audit)
    overfire_classification = _classify_overfire_flags(
        list(aee_transformation_audit.get("branch_overfire_flags", []))
    )
    aee_transformation_audit["branch_overfire"] = overfire_classification

    # Champion data
    champion_path = Path(args.champion_reference)
    if not champion_path.is_absolute():
        champion_path = (root / champion_path).resolve()
    champion_payload = None
    if champion_path.exists():
        try:
            champion_payload = json.loads(champion_path.read_text(encoding="utf-8"))
        except Exception:
            pass

    champion_breakout_pph = 0.0
    champion_combined_pph = 0.0
    champions_by_family: dict[str, float] = {}
    if champion_payload and isinstance(champion_payload, dict):
        for row in champion_payload.get("ranked_families", []):
            fam = str(row.get("family", ""))
            pph = _safe_float(row.get("net_pips_per_hour", 0.0), 0.0)
            champions_by_family[fam] = pph
            if fam == "EXPANSION_BREAKOUT":
                champion_breakout_pph = pph
        champion_combined_pph = _safe_float(
            champion_payload.get("combined_keep_tune_net_pph", 0.0), 0.0
        )

    run_breakout_pph = family_stats.get("EXPANSION_BREAKOUT", {}).get("net_pips_per_hour", 0.0)
    keep_families = [s for s in ranked if s.get("verdict") in ("KEEP", "TUNE")]
    combined_keep_tune_pph = sum(s["net_pips_per_hour"] for s in keep_families)
    baseline_ab = _aggregate_baseline_benchmarks(all_records, total_hours)
    simplicity_reality_check = _build_simplicity_reality_check(baseline_ab)
    root_cause_decision_tree = _build_root_cause_decision_tree(
        family_taxonomy_audit=family_taxonomy_audit,
        module_collision_audit=module_collision_audit,
        simplicity_reality_check=simplicity_reality_check,
    )
    baseline_gate = _baseline_gate_assessment(baseline_ab)
    previous_summary_path = (root / args.result_dir / "run_summary_active.json").resolve()
    previous_summary = _load_json_if_exists(previous_summary_path)
    previous_results = (previous_summary or {}).get("results", {}) if isinstance(previous_summary, dict) else {}
    previous_audit = (previous_summary or {}).get("aee_transformation_audit", {}) if isinstance(previous_summary, dict) else {}
    previous_top_exit = (previous_summary or {}).get("top_exit_branches", []) if isinstance(previous_summary, dict) else []
    prior_fast_fail_share = 0.0
    for row in previous_top_exit:
        if str(row.get("name", "")) == "AEE_BAND_FAST_FAILURE_EXIT":
            cnt = float(row.get("count", 0.0))
            n_prev = float(previous_results.get("trade_count", 0.0) or 0.0)
            prior_fast_fail_share = (cnt / n_prev) if n_prev > 0 else 0.0
    current_fast_fail_share = (exit_reason_counts.get("AEE_BAND_FAST_FAILURE_EXIT", 0) / n_trades) if n_trades > 0 else 0.0
    current_signature_state = {
        "realized_pph": total_realized_pph,
        "trades_per_hour": (n_trades / total_hours if total_hours > 0 else 0.0),
        "gap": gap_pph,
        "extraction_efficiency": extraction_efficiency,
        "giveback_ratio": float(aee_transformation_audit.get("giveback_ratio", {}).get("value", 0.0)),
        "continuation_capture_rate": float(aee_transformation_audit.get("continuation_capture", {}).get("continuation_capture_rate", 0.0)),
        "breakout_pph": run_breakout_pph,
        "loss_compression_rate": float(aee_transformation_audit.get("loss_compression", {}).get("loss_compression_rate", 0.0)),
        "failure_to_win_conversion_rate": float(aee_transformation_audit.get("failure_to_win_conversion", {}).get("failure_to_win_conversion_rate", 0.0)),
        "fast_failure_exit_share": current_fast_fail_share,
        "bankable_green_loss_red_rate": float(green_loss_audit.get("summary", {}).get("bankable_green_loss_red_rate", 0.0)),
        "avg_loser_hold_sec": float(green_loss_audit.get("throughput", {}).get("avg_loser_hold_sec", 0.0)),
        "avg_weak_winner_hold_sec": float(green_loss_audit.get("throughput", {}).get("avg_weak_winner_hold_sec", 0.0)),
        "giveback_exit_share": float(green_loss_audit.get("summary", {}).get("giveback_exit_share_of_green_losses", 0.0)),
        "baseline_hard_amplifier_gate_pass": bool(baseline_gate.get("hard_amplifier_gate_pass", False)),
    }
    previous_green_loss_audit = (previous_summary or {}).get("green_loss_audit", {}) if isinstance(previous_summary, dict) else {}
    prior_signature_state = {
        "realized_pph": float(previous_results.get("realized_pph", 0.0) or 0.0),
        "trades_per_hour": float(previous_results.get("trades_per_hour", 0.0) or 0.0),
        "gap": float(previous_results.get("gap", 0.0) or 0.0),
        "extraction_efficiency": float(previous_results.get("extraction_efficiency", 0.0) or 0.0),
        "giveback_ratio": float((previous_audit.get("giveback_ratio", {}) or {}).get("value", 0.0) if isinstance(previous_audit, dict) else 0.0),
        "continuation_capture_rate": float((previous_audit.get("continuation_capture", {}) or {}).get("continuation_capture_rate", 0.0) if isinstance(previous_audit, dict) else 0.0),
        "breakout_pph": float(((previous_summary or {}).get("family_results", {}) or {}).get("EXPANSION_BREAKOUT", {}).get("net_pips_per_hour", 0.0) if isinstance(previous_summary, dict) else 0.0),
        "loss_compression_rate": float((previous_audit.get("loss_compression", {}) or {}).get("loss_compression_rate", 0.0) if isinstance(previous_audit, dict) else 0.0),
        "failure_to_win_conversion_rate": float((previous_audit.get("failure_to_win_conversion", {}) or {}).get("failure_to_win_conversion_rate", 0.0) if isinstance(previous_audit, dict) else 0.0),
        "fast_failure_exit_share": prior_fast_fail_share,
        "bankable_green_loss_red_rate": ((previous_green_loss_audit.get("summary", {}) or {}).get("bankable_green_loss_red_rate") if isinstance(previous_green_loss_audit, dict) else None),
        "avg_loser_hold_sec": ((previous_green_loss_audit.get("throughput", {}) or {}).get("avg_loser_hold_sec") if isinstance(previous_green_loss_audit, dict) else None),
        "avg_weak_winner_hold_sec": ((previous_green_loss_audit.get("throughput", {}) or {}).get("avg_weak_winner_hold_sec") if isinstance(previous_green_loss_audit, dict) else None),
        "giveback_exit_share": ((previous_green_loss_audit.get("summary", {}) or {}).get("giveback_exit_share_of_green_losses") if isinstance(previous_green_loss_audit, dict) else None),
        "baseline_hard_amplifier_gate_pass": bool(((previous_summary or {}).get("auto_adjudication", {}) or {}).get("baseline_gate", {}).get("hard_amplifier_gate_pass", False)) if isinstance(previous_summary, dict) else False,
    }
    performance_signature = _performance_signature_assessment(
        current=current_signature_state,
        prior=prior_signature_state if any(v != 0.0 for v in prior_signature_state.values()) else None,
    )
    adjudication = _auto_adjudicate_run(
        realized_pph=total_realized_pph,
        breakout_pph=run_breakout_pph,
        champion_breakout_pph=champion_breakout_pph,
        transformation_audit=aee_transformation_audit,
        baseline_gate=baseline_gate,
        performance_signature=performance_signature,
    )
    failure_layer = _classify_failure_layer(
        total_entry_only_pph,
        total_realized_pph,
        top_exit_branches,
        aee_transformation_audit,
    )

    # Build all evidence-pack artifacts
    data_coverage = {
        "pair_coverage": sorted(unique_pairs),
        "streams": len(streams),
        "hours": _safe_round(total_hours, 4),
        "unique_days": len(unique_days),
        "sessions_represented": dict(sessions_seen),
        "session_quarter_distribution": {"UNKNOWN": len(streams)},
        "contexts_represented": {k: _safe_round(v, 4) for k, v in sorted(hours_by_context.items())},
        "regime_distribution": {k: int(v) for k, v in family_seen.items()},
        "dominance_concentration": {
            "top_family": ranked[0]["family"] if ranked else "UNKNOWN",
            "top_family_trade_share": _safe_round(
                (ranked[0]["trade_count"] / n_trades) if n_trades and ranked else 0.0, 6
            ),
        },
    }

    config_snapshot = {
        "run_id": run_id,
        "slice": {
            "slice_label": slice_label,
            "include_contexts": sorted(slice_include_contexts),
            "include_trade_ids_count": len(slice_include_trade_ids),
            "max_trades": max_trades,
        },
        "strategy_family": sorted(include_families),
        "strategy_form_id": args.strategy_form,
        "thresholds": {
            "spread_pips": float(args.spread_pips),
            "slippage_pips_per_side": float(args.slippage_pips_per_side),
            "commission_pips_roundtrip": float(args.commission_pips_roundtrip),
            "latency_penalty_pips": float(args.latency_penalty_pips),
            "economic_viability_mult": float(args.economic_viability_mult),
        },
        "enabled_gates": {
            "micro_confirm": bool(micro_confirm.get("enabled")),
            "displacement_rules": len(min_profit_now_pips_by_bar),
            "progress_rules": len(min_progress_ratio_by_bar),
            "release_quality_rules": len(min_release_quality_by_bar),
            "noise_rules": len(max_noise_by_bar),
        },
        "disabled_gates": {
            "raw_core_mode": bool(args.raw_core_mode),
            "excluded_contexts": sorted(exclude_contexts_lc),
        },
        "aee_version": args.aee_version,
        "simulation_mode": args.simulation_mode,
        "dataset_window_id": args.dataset_window_id,
        "pair_session_coverage": {"pairs": sorted(unique_pairs), "sessions": dict(sessions_seen)},
        "code_version": "workspace-local",
        "artifact_version": "run_aee_active_policy_evidencepack_v1",
        "config_path": str(cfg_path),
    }

    logic_trace_summary = {
        "run_id": run_id,
        "detector_logic_path": top_entry_branches,
        "gating_path": {
            "top_block_reasons": _sorted_top(dict(skipped_entry), top_n=8),
            "filter_config": {
                "include_families": sorted(include_families),
                "exclude_families": sorted(exclude_families),
                "exclude_contexts": sorted(exclude_contexts_lc),
            },
        },
        "exit_logic_path": top_exit_branches,
        "state_transitions_used": _sorted_top(dict(state_counts), top_n=8),
        "overrides_triggered": [
            {
                "name": "raw_core_mode",
                "enabled": bool(args.raw_core_mode),
                "effect": "disables non-core entry gates when true",
            }
        ],
    }

    trade_evidence_sample = {
        "run_id": run_id,
        "winners": _sample_by_bucket(all_records, "winners", max_rows=20),
        "losers": _sample_by_bucket(all_records, "losers", max_rows=20),
        "ambiguous": _sample_by_bucket(all_records, "ambiguous", max_rows=20),
    }

    expected_vs_actual = {
        "run_id": run_id,
        "champion_reference": str(champion_path.name),
        "intervention_class": args.intervention_class,
        "expected_signature": {
            "economic_objective": "maximize retained pips per hour after entry",
            "realized_pph_change_pct": [0.20, 0.80],
            "gap_change_pct": [-0.40, -0.15],
            "extraction_efficiency_abs_change": [0.05, 0.25],
            "bankable_green_loss_red_rate": "near_zero",
            "giveback_ratio_change_pct": [-0.30, -0.10],
            "trades_per_hour": "up",
            "avg_loser_hold_sec": "down",
            "avg_weak_winner_hold_sec": "down",
            "baseline_hard_amplifier_gate_pass": True,
        },
        "actual_signature": {
            "trade_count": n_trades,
            "trades_per_hour": _safe_round(n_trades / total_hours if total_hours > 0 else 0.0, 6),
            "avg_pips_per_trade": _safe_round(total_realized / n_trades if n_trades else 0.0, 6),
            "entry_only_pph": _safe_round(total_entry_only_pph, 6),
            "realized_pph": _safe_round(total_realized_pph, 6),
            "gap": _safe_round(gap_pph, 6),
            "extraction_efficiency": _safe_round(extraction_efficiency, 6),
            "loss_compression_rate": _safe_round(
                aee_transformation_audit.get("loss_compression", {}).get("loss_compression_rate", 0.0), 6
            ),
            "failure_to_win_conversion_rate": _safe_round(
                aee_transformation_audit.get("failure_to_win_conversion", {}).get("failure_to_win_conversion_rate", 0.0), 6
            ),
            "continuation_capture_rate": _safe_round(
                aee_transformation_audit.get("continuation_capture", {}).get("continuation_capture_rate", 0.0), 6
            ),
            "giveback_ratio": _safe_round(
                aee_transformation_audit.get("giveback_ratio", {}).get("value", 0.0), 6
            ),
            "bankable_green_loss_red_rate": _safe_round(
                green_loss_audit.get("summary", {}).get("bankable_green_loss_red_rate", 0.0), 6
            ),
            "bankable_green_protection_rate": _safe_round(
                green_loss_audit.get("summary", {}).get("bankable_green_protection_rate", 0.0), 6
            ),
            "avg_loser_hold_sec": _safe_round(
                green_loss_audit.get("throughput", {}).get("avg_loser_hold_sec", 0.0), 6
            ),
            "avg_weak_winner_hold_sec": _safe_round(
                green_loss_audit.get("throughput", {}).get("avg_weak_winner_hold_sec", 0.0), 6
            ),
            "baseline_gate_all_pass": bool(baseline_gate.get("all_pass", False)),
        },
        "delta_vs_champion": {
            "breakout_net_pph": _safe_round(run_breakout_pph - champion_breakout_pph, 6),
            "combined_keep_tune_net_pph": _safe_round(combined_keep_tune_pph - champion_combined_pph, 6),
        },
        "matches_expected_model": gap_pph <= 0.0 or total_realized_pph > 0.0,
        "hard_gates": adjudication.get("gates", {}),
        "auto_verdict": adjudication.get("verdict", "NO_OP"),
        "ab_baseline_comparison": baseline_ab,
        "performance_signature": performance_signature,
        "green_loss_audit": green_loss_audit,
        "family_taxonomy_audit": family_taxonomy_audit,
        "module_collision_audit": module_collision_audit,
        "simplicity_reality_check": simplicity_reality_check,
        "root_cause_decision_tree": root_cause_decision_tree,
    }

    green_loss_exit_rank = _sorted_top(dict(green_loss_audit.get("exit_reason_distribution", {})), top_n=4)
    primary_branch = green_loss_exit_rank[0]["name"] if green_loss_exit_rank else (top_exit_branches[0]["name"] if top_exit_branches else "UNKNOWN")
    secondary_branch = green_loss_exit_rank[1]["name"] if len(green_loss_exit_rank) > 1 else (top_exit_branches[1]["name"] if len(top_exit_branches) > 1 else "UNKNOWN")
    next_scope = "BREAKOUT + RANGE_ESCAPE bankable-green protection plus dead-trade pruning"
    next_task_recommendation = _build_next_task_recommendation(
        run_id=run_id,
        primary_branch=primary_branch,
        secondary_branch=secondary_branch,
        root_cause_hypothesis=str(root_cause_decision_tree.get("root_cause_hypothesis", "")),
    )

    control_dir = (root / "control").resolve()
    variable_map_update = _update_variable_map(
        variable_map_path=control_dir / "variable_causality_map.json",
        run_id=run_id,
        primary_branch=primary_branch,
        secondary_branch=secondary_branch,
        next_scope=next_scope,
    )
    priority_rank_update = _update_priority_rank(
        priority_path=control_dir / "variable_priority_rank.json",
        run_id=run_id,
    )
    champion_dual_status = _update_dual_champion_status(
        control_dir=control_dir,
        run_id=run_id,
        realized_pph=total_realized_pph,
        extraction_efficiency=extraction_efficiency,
        champion_combined_pph=champion_combined_pph,
        adjudication=adjudication,
    )

    run_summary_payload = {
        "run_id": run_id,
        "generated_at": _iso_now(),
        "champion_reference": str(champion_path.name),
        "intervention_class": args.intervention_class,
        "strategy_form": args.strategy_form,
        "aee_version": args.aee_version,
        "simulation_mode": args.simulation_mode,
        "data_coverage": data_coverage,
        "results": {
            "trade_count": n_trades,
            "trades_per_hour": _safe_round(n_trades / total_hours if total_hours > 0 else 0.0, 6),
            "avg_pips_per_trade": _safe_round(total_realized / n_trades if n_trades else 0.0, 6),
            "entry_only_pph": _safe_round(total_entry_only_pph, 6),
            "realized_pph": _safe_round(total_realized_pph, 6),
            "gap": _safe_round(gap_pph, 6),
            "extraction_efficiency": _safe_round(extraction_efficiency, 6),
        },
        "aee_transformation_audit": aee_transformation_audit,
        "family_results": {s["family"]: s for s in ranked},
        "failure_layer": failure_layer,
        "top_exit_branches": top_exit_branches,
        "verdict_by_family": {s["family"]: s["verdict"] for s in ranked},
        "auto_adjudication": adjudication,
        "intervention_basis": intervention_basis,
        "performance_signature": performance_signature,
        "next_intervention_target": {
            "primary_bottleneck_branch": primary_branch,
            "secondary_bottleneck_branch": secondary_branch,
            "intervention_class": "AEE_INTERACTION",
            "scope": next_scope,
        },
        "champion_dual_status": champion_dual_status,
        "map_updates": {
            "variable_causality_map": variable_map_update,
            "variable_priority_rank": priority_rank_update,
        },
        "ab_baseline_comparison": baseline_ab,
        "family_taxonomy_audit": family_taxonomy_audit,
        "module_collision_audit": module_collision_audit,
        "simplicity_reality_check": simplicity_reality_check,
        "root_cause_decision_tree": root_cause_decision_tree,
    }

    # Write artifacts
    result_dir = (root / args.result_dir).resolve()
    result_dir.mkdir(parents=True, exist_ok=True)

    artifact_map = {
        "config_snapshot_active.json": config_snapshot,
        "logic_trace_summary_active.json": logic_trace_summary,
        "trade_evidence_sample_active.json": trade_evidence_sample,
        "data_coverage_report_active.json": data_coverage,
        "expected_vs_actual_signature_active.json": expected_vs_actual,
        "failure_layer_classification_active.json": failure_layer,
        "aee_transformation_audit_active.json": aee_transformation_audit,
        "aee_baseline_ab_comparison_active.json": baseline_ab,
        "aee_performance_signature_active.json": performance_signature,
        "aee_green_loss_audit_active.json": green_loss_audit,
        "failure_bucket_dashboard_active.json": failure_bucket_dashboard,
        "aee_family_taxonomy_audit_active.json": family_taxonomy_audit,
        "aee_module_collision_audit_active.json": module_collision_audit,
        "aee_simplicity_reality_check_active.json": simplicity_reality_check,
        "aee_root_cause_decision_tree_active.json": root_cause_decision_tree,
        "aee_intervention_basis_active.json": intervention_basis,
        "auto_adjudication_active.json": adjudication,
        "next_task_recommendation_active.json": next_task_recommendation,
        "champion_dual_status_active.json": champion_dual_status,
        "run_summary_active.json": run_summary_payload,
    }
    for fname, payload in artifact_map.items():
        path = result_dir / fname
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"  Wrote: {path.name}")

    # Also write main family report
    out_path = root / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)
    main_report = {
        "generated_at": _iso_now(),
        "run_id": run_id,
        "config": str(cfg_path),
        "pair": pair,
        "total_streams": len(streams),
        "total_hours": _safe_round(total_hours, 2),
        "total_accepted_trades": n_trades,
        "friction_per_trade_pips": friction_per_trade,
        "family_distribution_in_data": dict(family_seen),
        "skipped_by_entry_filter": dict(skipped_entry),
        "ranked_families": ranked,
        "combined_keep_tune_net_pph": _safe_round(combined_keep_tune_pph, 6),
    }
    out_path.write_text(json.dumps(main_report, indent=2), encoding="utf-8")
    print(f"  Wrote: {out_path.name}")
    print()

    # Print the full 5-layer result truth block
    _print_evidence_block(
        run_id=run_id,
        champion_reference=str(champion_path.name),
        intervention_class=args.intervention_class,
        strategy_form=args.strategy_form,
        aee_version=args.aee_version,
        simulation_mode=args.simulation_mode,
        data_coverage=data_coverage,
        config_snapshot=config_snapshot,
        ranked_by_pph=ranked,
        top_entry_branches=top_entry_branches,
        top_exit_branches=top_exit_branches,
        all_records=all_records,
        total_hours=total_hours,
        total_entry_only_pph=total_entry_only_pph,
        total_realized_pph=total_realized_pph,
        gap_pph=gap_pph,
        extraction_efficiency=extraction_efficiency,
        baseline_ab=baseline_ab,
        expected_vs_actual=expected_vs_actual,
        aee_transformation_audit=aee_transformation_audit,
        adjudication=adjudication,
        failure_layer=failure_layer,
        champion_breakout_pph=champion_breakout_pph,
        champion_combined_pph=champion_combined_pph,
        family_stats=family_stats,
        champions_by_family=champions_by_family,
        skipped_entry=skipped_entry,
    )


if __name__ == "__main__":
    main()
