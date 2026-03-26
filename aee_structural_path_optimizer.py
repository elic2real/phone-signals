#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aee_historical_system_scoreboard as hs


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _quantile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    arr = sorted(values)
    idx = int(round((len(arr) - 1) * _clamp(q, 0.0, 1.0)))
    return float(arr[idx])


def _load_seed_configs(sweep_path: Path, top_n: int) -> list[tuple[str, Path]]:
    obj = json.loads(sweep_path.read_text(encoding="utf-8"))
    seeds: list[tuple[str, Path]] = []
    for row in obj.get("eligible_ranked_configs", [])[:top_n]:
        name = str(row.get("name", "")).strip()
        p = Path(str(row.get("config_path", "")).strip())
        if name and p.exists():
            seeds.append((name, p))
    return seeds


def _parse_seed_config_args(values: list[str], root: Path) -> list[tuple[str, Path]]:
    parsed: list[tuple[str, Path]] = []
    for raw in values:
        text = raw.strip()
        if not text or "=" not in text:
            continue
        name, p = text.split("=", 1)
        path = Path(p).expanduser()
        if not path.is_absolute():
            path = (root / path).resolve()
        if path.exists():
            parsed.append((name.strip(), path))
    return parsed


def _prepare_contexts(stream_paths: list[Path], usd_per_pip: float) -> list[dict[str, Any]]:
    contexts: list[dict[str, Any]] = []
    for sp in stream_paths:
        rows = hs._load_state_rows(sp)
        if not rows:
            continue

        by_trade: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in rows:
            by_trade[str(row.get("trade_id", "")).strip()].append(row)
        for t_rows in by_trade.values():
            t_rows.sort(key=lambda r: hs._safe_int(r.get("bar_index", 0), 0))

        duration_hr = hs._window_duration_hours(rows)
        static_outcomes = [hs._evaluate_static_trade(t_rows) for t_rows in by_trade.values() if t_rows]
        static_metrics = hs._compute_metrics(static_outcomes, duration_hr, usd_per_pip)
        opportunity_r_per_hour = static_metrics.get("avg_realized_r", 0.0) * static_metrics.get("close_cycle_capture_rate", 0.0)

        contexts.append(
            {
                "path": sp,
                "by_trade": by_trade,
                "duration_hr": duration_hr,
                "static_metrics": static_metrics,
                "opportunity_r_per_hour": opportunity_r_per_hour,
                "static_result": {
                    "name": "static_baseline",
                    "metrics": static_metrics,
                    "rejected": False,
                    "rejection_reasons": [],
                },
            }
        )
    return contexts


def _derive_behavioral_bands(contexts: list[dict[str, Any]]) -> dict[str, float]:
    peaks: list[float] = []
    for ctx in contexts:
        for t_rows in ctx["by_trade"].values():
            if not t_rows:
                continue
            td = max(0.1, hs._safe_float(t_rows[0].get("target_distance", 1.0), 1.0))
            peak_pips = max(hs._safe_float(r.get("profit_now", 0.0)) for r in t_rows)
            peaks.append(peak_pips / td)

    if not peaks:
        return {
            "noise_max": 0.18,
            "monetize_max": 0.45,
            "continuation_max": 0.9,
            "runner_max": 1.8,
        }

    q35 = _quantile(peaks, 0.35)
    q65 = _quantile(peaks, 0.65)
    q85 = _quantile(peaks, 0.85)
    q95 = _quantile(peaks, 0.95)

    # Clamp to practical decision bands so rare deep tails do not delay monetization.
    min_gap = 0.05
    noise_max = _clamp(max(0.08, q35), 0.12, 0.35)
    monetize_max = _clamp(max(noise_max + min_gap, q65), 0.25, 0.70)
    continuation_max = _clamp(max(monetize_max + min_gap, q85), 0.50, 1.25)
    runner_max = _clamp(max(continuation_max + min_gap, q95), 0.90, 2.20)

    # Preserve ordering after clamping.
    monetize_max = max(monetize_max, noise_max + min_gap)
    continuation_max = max(continuation_max, monetize_max + min_gap)
    runner_max = max(runner_max, continuation_max + min_gap)

    return {
        "noise_max": round(noise_max, 6),
        "monetize_max": round(monetize_max, 6),
        "continuation_max": round(continuation_max, 6),
        "runner_max": round(runner_max, 6),
    }


def _band_label(r_now: float, bands: dict[str, float]) -> str:
    if r_now <= bands["noise_max"]:
        return "A"
    if r_now <= bands["monetize_max"]:
        return "B"
    if r_now <= bands["continuation_max"]:
        return "C"
    if r_now <= bands["runner_max"]:
        return "D"
    return "E"


def _expected_remaining_r(r_now: float, band: str, bands: dict[str, float]) -> float:
    if band == "A":
        return max(0.0, bands["monetize_max"] - r_now)
    if band == "B":
        return max(0.0, bands["continuation_max"] - r_now)
    if band == "C":
        return max(0.0, bands["runner_max"] - r_now)
    if band == "D":
        return max(0.0, (bands["runner_max"] * 1.35) - r_now)
    return 0.0


def _family_library(family_set: str) -> list[dict[str, Any]]:
    if family_set == "profit_stall_harvester":
        return [
            {
                "name": "profit_stall_harvester",
                "proving_window_factor": 0.55,
                "qualify_progress_min": 0.58,
                "qualify_energy_min": -0.45,
                "harvest_giveback_factor": 0.70,
                "stall_tsp_factor": 0.65,
                "stall_velocity_max": 0.12,
                "stall_progress_max": 1.08,
                "force_band_touch": "B",
                "force_lock_r": 0.28,
                "force_giveback_pips": 0.6,
                "band_hold_prob_min": 0.55,
                "band_lock_r": 0.25,
                "opportunity_mult": 1.15,
                "decision_horizon_min": 5.0,
                "stall_timeout_min": 3.0,
                "stall_profit_min_r": 0.28,
                "stall_velocity_abs_max": 0.08,
                "fallback_peak_min_r": 999.0,
                "fallback_drop_r": 999.0,
                "fallback_lock_r": 0.0,
            }
        ]

    if family_set == "giveback_control":
        return [
            {
                "name": "giveback_control",
                "proving_window_factor": 0.65,
                "qualify_progress_min": 0.66,
                "qualify_energy_min": -0.35,
                "harvest_giveback_factor": 0.78,
                "stall_tsp_factor": 0.72,
                "stall_velocity_max": 0.10,
                "stall_progress_max": 1.04,
                "force_band_touch": "C",
                "force_lock_r": 0.32,
                "force_giveback_pips": 0.75,
                "band_hold_prob_min": 0.50,
                "band_lock_r": 0.30,
                "opportunity_mult": 1.05,
                "decision_horizon_min": 6.0,
                "stall_timeout_min": 999.0,
                "stall_profit_min_r": 999.0,
                "stall_velocity_abs_max": 0.0,
                "fallback_peak_min_r": 0.95,
                "fallback_drop_r": 0.30,
                "fallback_lock_r": 0.24,
            }
        ]

    return [
        {
            "name": "earlier_band_qualification",
            "proving_window_factor": 0.65,
            "qualify_progress_min": 0.65,
            "qualify_energy_min": -0.35,
            "harvest_giveback_factor": 0.80,
            "stall_tsp_factor": 0.80,
            "stall_velocity_max": 0.08,
            "stall_progress_max": 1.05,
            "force_band_touch": "C",
            "force_lock_r": 0.35,
            "force_giveback_pips": 0.9,
            "band_hold_prob_min": 0.42,
            "band_lock_r": 0.30,
            "opportunity_mult": 1.0,
            "decision_horizon_min": 6.0,
        },
        {
            "name": "band_forced_monetization",
            "proving_window_factor": 0.70,
            "qualify_progress_min": 0.70,
            "qualify_energy_min": -0.30,
            "harvest_giveback_factor": 0.75,
            "stall_tsp_factor": 0.75,
            "stall_velocity_max": 0.10,
            "stall_progress_max": 1.05,
            "force_band_touch": "B",
            "force_lock_r": 0.40,
            "force_giveback_pips": 0.7,
            "band_hold_prob_min": 0.50,
            "band_lock_r": 0.35,
            "opportunity_mult": 1.05,
            "decision_horizon_min": 7.0,
        },
        {
            "name": "stall_reversal_reclassification",
            "proving_window_factor": 0.65,
            "qualify_progress_min": 0.65,
            "qualify_energy_min": -0.35,
            "harvest_giveback_factor": 0.80,
            "stall_tsp_factor": 0.60,
            "stall_velocity_max": 0.16,
            "stall_progress_max": 1.00,
            "force_band_touch": "C",
            "force_lock_r": 0.30,
            "force_giveback_pips": 0.9,
            "band_hold_prob_min": 0.46,
            "band_lock_r": 0.28,
            "opportunity_mult": 0.95,
            "decision_horizon_min": 6.0,
        },
    ]


def _band_touch_threshold_r(fam: dict[str, Any], bands: dict[str, float]) -> float:
    band = str(fam.get("force_band_touch", "C")).upper().strip()
    if band == "B":
        return bands["noise_max"]
    if band == "C":
        return bands["monetize_max"]
    if band == "D":
        return bands["continuation_max"]
    return bands["runner_max"]


def _continuation_probability(progress_ratio: float, energy_ratio: float, velocity_now: float, giveback_now: float, peak_profit: float) -> float:
    giveback_ratio = giveback_now / max(0.2, peak_profit)
    score = (
        0.48
        + 0.40 * _clamp(progress_ratio - 0.85, -0.5, 0.5)
        + 0.30 * _clamp(energy_ratio, -0.7, 0.7)
        + 0.18 * _clamp(velocity_now / 0.25, -1.0, 1.0)
        - 0.34 * _clamp(giveback_ratio, 0.0, 1.4)
    )
    return _clamp(score, 0.01, 0.99)


def _evaluate_trade_structural(
    rows: list[dict[str, str]],
    cfg: dict[str, Any],
    fam: dict[str, Any],
    bands: dict[str, float],
    opportunity_r_per_hour: float,
) -> hs.TradeOutcome:
    first = rows[0]
    trade_id = str(first.get("trade_id", ""))
    direction = str(first.get("direction", "")).upper().strip() or "LONG"
    target_key = hs._target_key(first.get("target_distance", "0"))
    target_distance = max(0.1, hs._safe_float(first.get("target_distance", 1.0), 1.0))

    panic = hs._get_rule_conditions(cfg, "base_panic")
    decay = hs._get_rule_conditions(cfg, "base_decay")
    harvest = hs._get_rule_conditions(cfg, "base_harvest")
    dmods = (cfg.get("direction_modifiers", {}) or {}).get(direction, {}) or {}
    tmods = (cfg.get("target_modifiers", {}) or {}).get(target_key, {}) or {}

    base_proving = max(1, hs._safe_int(tmods.get("proving_window", 2), 2))
    proving_window = max(1, int(round(base_proving * float(fam.get("proving_window_factor", 1.0)))))

    panic_opp_pressure = hs._safe_float(dmods.get("panic_opposite_pressure", panic.get("opposite_direction_strength_min", 0.0)))
    harvest_profit_floor = hs._safe_float(dmods.get("harvest_profit_floor", harvest.get("profit_now_min", 0.0)))
    harvest_giveback_tol = hs._safe_float(tmods.get("harvest_giveback_tolerance", harvest.get("giveback_now_min", 0.0)))
    decay_tsp = hs._safe_float(tmods.get("decay_time_since_peak", decay.get("time_since_peak_min", 0.0)))

    max_profit = max(hs._safe_float(r.get("profit_now", 0.0)) for r in rows)
    peak_profit = -1e9
    force_band_touch_r = _band_touch_threshold_r(fam, bands)

    for row in rows:
        bar_index = max(1, hs._safe_int(row.get("bar_index", 1), 1))
        profit_now = hs._safe_float(row.get("profit_now", 0.0))
        velocity_now = hs._safe_float(row.get("velocity_now", 0.0))
        giveback_now = hs._safe_float(row.get("giveback_now", 0.0))
        opp = hs._safe_float(row.get("opposite_direction_strength", 0.0))
        time_open = hs._safe_float(row.get("time_open", bar_index))
        time_since_peak = hs._safe_float(row.get("time_since_peak", 0.0))
        progress_ratio = hs._safe_float(row.get("progress_ratio", 0.0))
        energy_ratio = hs._safe_float(row.get("energy_ratio", 0.0))

        peak_profit = max(peak_profit, profit_now)
        band_r = profit_now / target_distance
        band = _band_label(band_r, bands)

        panic_hit = (
            profit_now <= panic.get("profit_now_max", float("-inf"))
            and velocity_now <= panic.get("velocity_now_max", float("-inf"))
            and giveback_now >= panic.get("giveback_now_min", float("inf"))
            and opp >= max(panic.get("opposite_direction_strength_min", 0.0), panic_opp_pressure)
            and time_open >= panic.get("time_open_min", float("inf"))
        )
        if panic_hit:
            return hs.TradeOutcome(
                trade_id=trade_id,
                reason="AEE_BAND_FAST_FAILURE_EXIT",
                pips=profit_now,
                realized_r=(profit_now / target_distance),
                hold_sec=float(bar_index * 60),
                max_profit=max_profit,
                sl_like=True,
            )

        if (peak_profit / target_distance) >= force_band_touch_r and (peak_profit - profit_now) >= float(fam.get("force_giveback_pips", 999.0)):
            locked_pips = max(profit_now, float(fam.get("force_lock_r", 0.0)) * target_distance)
            return hs.TradeOutcome(
                trade_id=trade_id,
                reason="AEE_BAND_FORCED_MONETIZE",
                pips=locked_pips,
                realized_r=(locked_pips / target_distance),
                hold_sec=float(bar_index * 60),
                max_profit=max_profit,
                sl_like=locked_pips < 0.0,
            )

        peak_r = peak_profit / target_distance
        if (
            peak_r >= float(fam.get("fallback_peak_min_r", 999.0))
            and (peak_r - band_r) >= float(fam.get("fallback_drop_r", 999.0))
        ):
            locked_pips = max(profit_now, float(fam.get("fallback_lock_r", 0.0)) * target_distance)
            return hs.TradeOutcome(
                trade_id=trade_id,
                reason="AEE_BAND_GIVEBACK_CONTROL_EXIT",
                pips=locked_pips,
                realized_r=(locked_pips / target_distance),
                hold_sec=float(bar_index * 60),
                max_profit=max_profit,
                sl_like=locked_pips < 0.0,
            )

        if (
            band in {"B", "C", "D", "E"}
            and band_r >= float(fam.get("stall_profit_min_r", 999.0))
            and time_since_peak >= float(fam.get("stall_timeout_min", 999.0))
            and abs(velocity_now) <= float(fam.get("stall_velocity_abs_max", 0.0))
        ):
            return hs.TradeOutcome(
                trade_id=trade_id,
                reason="AEE_PROFIT_STALL_HARVEST",
                pips=profit_now,
                realized_r=(profit_now / target_distance),
                hold_sec=float(bar_index * 60),
                max_profit=max_profit,
                sl_like=profit_now < 0.0,
            )

        continuation_prob = _continuation_probability(progress_ratio, energy_ratio, velocity_now, giveback_now, peak_profit)
        exp_remaining_r = _expected_remaining_r(band_r, band, bands)
        expected_continuation_r = continuation_prob * exp_remaining_r
        decision_horizon_hr = float(fam.get("decision_horizon_min", 6.0)) / 60.0
        recycle_value_r = max(0.0, opportunity_r_per_hour) * decision_horizon_hr

        if band in {"B", "C"} and continuation_prob < float(fam.get("band_hold_prob_min", 0.45)):
            locked_pips = max(profit_now, float(fam.get("band_lock_r", 0.0)) * target_distance)
            return hs.TradeOutcome(
                trade_id=trade_id,
                reason="AEE_BAND_PROB_LOCK_EXIT",
                pips=locked_pips,
                realized_r=(locked_pips / target_distance),
                hold_sec=float(bar_index * 60),
                max_profit=max_profit,
                sl_like=locked_pips < 0.0,
            )

        if band in {"B", "C", "D"} and expected_continuation_r < (recycle_value_r * float(fam.get("opportunity_mult", 1.0))):
            locked_pips = max(profit_now, float(fam.get("band_lock_r", 0.0)) * target_distance)
            return hs.TradeOutcome(
                trade_id=trade_id,
                reason="AEE_OBJECTIVE_RECYCLE_EXIT",
                pips=locked_pips,
                realized_r=(locked_pips / target_distance),
                hold_sec=float(bar_index * 60),
                max_profit=max_profit,
                sl_like=locked_pips < 0.0,
            )

        harvest_hit = (
            bar_index >= proving_window
            and band in {"B", "C", "D", "E"}
            and progress_ratio >= float(fam.get("qualify_progress_min", 0.7))
            and energy_ratio >= float(fam.get("qualify_energy_min", -0.3))
            and profit_now >= harvest_profit_floor
            and giveback_now >= (harvest_giveback_tol * float(fam.get("harvest_giveback_factor", 1.0)))
        )
        if harvest_hit:
            return hs.TradeOutcome(
                trade_id=trade_id,
                reason="AEE_BAND_EARLY_PROFIT_LOCK",
                pips=profit_now,
                realized_r=(profit_now / target_distance),
                hold_sec=float(bar_index * 60),
                max_profit=max_profit,
                sl_like=False,
            )

        stall_hit = (
            band in {"B", "C", "D", "E"}
            and time_since_peak >= (decay_tsp * float(fam.get("stall_tsp_factor", 1.0)))
            and velocity_now <= float(fam.get("stall_velocity_max", 0.1))
            and progress_ratio <= float(fam.get("stall_progress_max", 1.05))
        )
        if stall_hit:
            return hs.TradeOutcome(
                trade_id=trade_id,
                reason="AEE_BAND_STALL_RECLASS_EXIT",
                pips=profit_now,
                realized_r=(profit_now / target_distance),
                hold_sec=float(bar_index * 60),
                max_profit=max_profit,
                sl_like=profit_now < 0.0,
            )

    last = rows[-1]
    fallback_pips = hs._safe_float(last.get("static_pips", hs._safe_float(last.get("profit_now", 0.0))))
    fallback_bar = max(1, hs._safe_int(last.get("bar_index", len(rows)), len(rows)))
    return hs.TradeOutcome(
        trade_id=trade_id,
        reason="STATIC_TIMEOUT",
        pips=fallback_pips,
        realized_r=(fallback_pips / target_distance),
        hold_sec=float(fallback_bar * 60),
        max_profit=max_profit,
        sl_like=fallback_pips < 0.0,
    )


def _score_candidate(
    seed_name: str,
    seed_path: Path,
    cfg: dict[str, Any],
    fam: dict[str, Any],
    contexts: list[dict[str, Any]],
    bands: dict[str, float],
    usd_per_pip: float,
    min_delta_pph: float,
    max_hold_ratio: float,
    min_stream_wins: int,
    min_stream_win_rate: float,
) -> dict[str, Any]:
    stream_rows: list[dict[str, Any]] = []
    sum_metrics = defaultdict(float)
    sum_delta = defaultdict(float)

    for ctx in contexts:
        outcomes = [
            _evaluate_trade_structural(
                t_rows,
                cfg,
                fam,
                bands,
                float(ctx.get("opportunity_r_per_hour", 0.0)),
            )
            for t_rows in ctx["by_trade"].values()
            if t_rows
        ]
        metrics = hs._compute_metrics(outcomes, ctx["duration_hr"], usd_per_pip)
        result = {"name": seed_name, "metrics": metrics}
        rejected, reject_reasons = hs._apply_rejection_rules(result, ctx["static_result"])
        delta_pph = metrics.get("realized_pips_per_hour", 0.0) - ctx["static_metrics"].get("realized_pips_per_hour", 0.0)
        hold_ratio = (
            metrics.get("avg_hold_sec", 0.0) / max(1.0, ctx["static_metrics"].get("avg_hold_sec", 0.0))
            if ctx["static_metrics"].get("avg_hold_sec", 0.0) > 0
            else 1.0
        )
        stream_win = (
            (not rejected)
            and delta_pph >= min_delta_pph
            and metrics.get("avg_realized_r", 0.0) > 0.0
            and hold_ratio <= max_hold_ratio
        )
        row = {
            "stream_path": str(ctx["path"]),
            "stream_win": stream_win,
            "rejected": rejected,
            "rejection_reasons": reject_reasons,
            "metrics": metrics,
            "delta_vs_static": {
                "realized_pips_per_hour": delta_pph,
                "realized_usd_per_hour": metrics.get("realized_usd_per_hour", 0.0) - ctx["static_metrics"].get("realized_usd_per_hour", 0.0),
                "avg_realized_r": metrics.get("avg_realized_r", 0.0) - ctx["static_metrics"].get("avg_realized_r", 0.0),
                "avg_hold_sec": metrics.get("avg_hold_sec", 0.0) - ctx["static_metrics"].get("avg_hold_sec", 0.0),
                "hold_ratio": hold_ratio,
            },
        }
        stream_rows.append(row)

        for k in ("realized_pips_per_hour", "realized_usd_per_hour", "avg_realized_r", "avg_hold_sec", "capital_recycling_rate"):
            sum_metrics[k] += metrics.get(k, 0.0)
        for k in ("realized_pips_per_hour", "realized_usd_per_hour", "avg_realized_r", "avg_hold_sec", "hold_ratio"):
            sum_delta[k] += row["delta_vs_static"].get(k, 0.0)

    n = max(1, len(stream_rows))
    stream_win_count = sum(1 for r in stream_rows if r.get("stream_win", False))
    stream_win_rate = stream_win_count / n
    promoted = (
        stream_win_count >= min_stream_wins
        and stream_win_rate >= min_stream_win_rate
        and (sum_metrics["avg_realized_r"] / n) > 0.0
        and (sum_delta["realized_pips_per_hour"] / n) >= min_delta_pph
        and (sum_delta["hold_ratio"] / n) <= max_hold_ratio
    )

    return {
        "seed_name": seed_name,
        "seed_config_path": str(seed_path),
        "family_name": fam["name"],
        "stream_count": n,
        "stream_win_count": stream_win_count,
        "stream_win_rate": stream_win_rate,
        "avg_metrics": {k: (sum_metrics[k] / n) for k in sum_metrics},
        "avg_delta_vs_static": {k: (sum_delta[k] / n) for k in sum_delta},
        "promoted": promoted,
        "stream_rows": stream_rows,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Small structural path/band logic batch runner with behavioral bands and objective wiring.")
    ap.add_argument("--seed-sweep", default="aee_historical_system_scoreboard_sweep.json")
    ap.add_argument("--top-seeds", type=int, default=2)
    ap.add_argument(
        "--seed-config",
        action="append",
        default=[],
        help="Explicit seed config in NAME=PATH format (repeatable). Overrides --seed-sweep when provided.",
    )
    ap.add_argument("--state-stream-glob", action="append", default=[])
    ap.add_argument("--max-streams", type=int, default=5)
    ap.add_argument("--usd-per-pip", type=float, default=0.8)
    ap.add_argument("--min-delta-pph", type=float, default=0.03)
    ap.add_argument("--max-hold-ratio", type=float, default=1.5)
    ap.add_argument("--min-stream-wins", type=int, default=3)
    ap.add_argument("--min-stream-win-rate", type=float, default=0.6)
    ap.add_argument(
        "--family-set",
        default="default",
        choices=["default", "profit_stall_harvester", "giveback_control"],
        help="Choose structural family set to evaluate.",
    )
    ap.add_argument("--out", default="aee_structural_batch1.json")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    sweep_path = Path(args.seed_sweep)
    if not sweep_path.is_absolute():
        sweep_path = (root / sweep_path).resolve()
    if not sweep_path.exists():
        raise SystemExit(f"seed sweep json not found: {sweep_path}")

    if args.seed_config:
        seeds = _parse_seed_config_args(args.seed_config, root)
    else:
        seeds = _load_seed_configs(sweep_path, int(args.top_seeds))
    if not seeds:
        raise SystemExit("no seed configs resolved from sweep file")

    stream_paths: list[Path] = []
    if args.state_stream_glob:
        for g in args.state_stream_glob:
            stream_paths.extend(hs._resolve_streams_from_globs([g], root))
    else:
        stream_paths = hs._resolve_streams_from_globs(["compiled_aee_stage_11_sessions*/aee_state_stream/aee_state_stream.csv"], root)
    stream_paths = hs._dedupe_paths(stream_paths)[: max(1, int(args.max_streams))]
    if not stream_paths:
        raise SystemExit("no state streams resolved")

    contexts = _prepare_contexts(stream_paths, float(args.usd_per_pip))
    if not contexts:
        raise SystemExit("all selected state streams are empty")

    behavioral_bands = _derive_behavioral_bands(contexts)

    families = _family_library(str(args.family_set))
    ranked_candidates: list[dict[str, Any]] = []
    for seed_name, seed_path in seeds:
        cfg = hs._read_json(seed_path)
        for fam in families:
            ranked_candidates.append(
                _score_candidate(
                    seed_name=seed_name,
                    seed_path=seed_path,
                    cfg=cfg,
                    fam=fam,
                    contexts=contexts,
                    bands=behavioral_bands,
                    usd_per_pip=float(args.usd_per_pip),
                    min_delta_pph=float(args.min_delta_pph),
                    max_hold_ratio=float(args.max_hold_ratio),
                    min_stream_wins=int(args.min_stream_wins),
                    min_stream_win_rate=float(args.min_stream_win_rate),
                )
            )

    ranked_candidates.sort(
        key=lambda r: (
            1 if r.get("promoted", False) else 0,
            r.get("stream_win_rate", 0.0),
            r.get("avg_delta_vs_static", {}).get("realized_pips_per_hour", 0.0),
            r.get("avg_metrics", {}).get("avg_realized_r", 0.0),
            -r.get("avg_delta_vs_static", {}).get("hold_ratio", 999.0),
        ),
        reverse=True,
    )
    promoted = [r for r in ranked_candidates if r.get("promoted", False)]

    payload = {
        "generated_at": _iso_now(),
        "seed_sweep": str(sweep_path),
        "seed_count": len(seeds),
        "family_count": len(families),
        "family_set": str(args.family_set),
        "candidate_count": len(ranked_candidates),
        "stream_count": len(contexts),
        "streams": [str(c["path"]) for c in contexts],
        "behavioral_bands": behavioral_bands,
        "gates": {
            "min_delta_pph": float(args.min_delta_pph),
            "max_hold_ratio": float(args.max_hold_ratio),
            "min_stream_wins": int(args.min_stream_wins),
            "min_stream_win_rate": float(args.min_stream_win_rate),
        },
        "ranked_candidates": ranked_candidates,
        "promoted_candidates": promoted,
        "structural_batch_status": (
            "no_promoted_structural_candidates" if not promoted else "has_promoted_structural_candidates"
        ),
    }

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = (root / out_path).resolve()
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "out": str(out_path),
                "candidate_count": len(ranked_candidates),
                "stream_count": len(contexts),
                "promoted_count": len(promoted),
                "behavioral_bands": behavioral_bands,
                "best_candidate": (
                    ranked_candidates[0]["seed_name"] + "::" + ranked_candidates[0]["family_name"]
                    if ranked_candidates
                    else None
                ),
                "best_promoted": (
                    promoted[0]["seed_name"] + "::" + promoted[0]["family_name"] if promoted else None
                ),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
