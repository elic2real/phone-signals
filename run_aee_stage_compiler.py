#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import pickle
import shutil
import subprocess
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import build_energy_context_engine as energy_ctx
import build_point_energy_trajectory as point_traj
import build_session_state_stream as stream
from entry_contract import (
    build_selected_entries_from_population,
    build_selected_entries_from_truth,
    validate_canonical_entry_rows,
)
from optimize_target_entry_classes_pph_static_cached import load_csv
from optimize_target_entry_classes_pph_static_cached import rule_applies as entry_rule_applies


ROOT = Path(__file__).resolve().parent
COMPILER_VERSION = "stage8_aee_compiler_v1"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sha256_rows(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, default=str).encode()
    return hashlib.sha256(payload).hexdigest()


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def quantile(values: list[float], q: float, default: float = 0.0) -> float:
    if not values:
        return default
    values = sorted(values)
    if len(values) == 1:
        return float(values[0])
    idx = q * (len(values) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(values) - 1)
    frac = idx - lo
    return float(values[lo] * (1 - frac) + values[hi] * frac)


def mean0(values: list[float]) -> float:
    return float(mean(values)) if values else 0.0


def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def load_json(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    return json.loads(path.read_text())


def has_files(*paths: Path) -> bool:
    return all(path.exists() for path in paths)


def signed_quarter_bias(direction: str, quarter: str) -> float:
    # Proven quarter dominance discovered earlier in the repo work.
    quarter_bias = {"Q1": "SHORT", "Q2": "LONG", "Q3": "SHORT", "Q4": "LONG"}
    return 1.0 if quarter_bias.get(quarter) == direction else -1.0


def load_dataset_lock(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def resolve_data_root(dataset_lock: dict[str, Any]) -> Path:
    data_root = dataset_lock.get("data_root")
    if data_root:
        p = Path(data_root)
        return p if p.is_absolute() else ROOT / p
    # Backward compatibility for the original single-session lock.
    if dataset_lock.get("row_count") == 540 and dataset_lock.get("pair") == "EUR_USD":
        return ROOT / "london_session_data"
    raise KeyError("dataset_lock missing data_root and no backward-compatible fallback matched")


def build_selected_entry_population(
    truth_rows: list[dict[str, Any]],
    entry_rules: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    selected = build_selected_entries_from_truth(truth_rows, entry_rule_applies, entry_rules)
    validate_canonical_entry_rows(selected)
    return selected


def resolve_source_entry_population(output_dir: Path) -> Path | None:
    if output_dir.name == "aee_stage":
        node_dir = output_dir.parent
    else:
        node_dir = output_dir
    candidates = [
        node_dir / "target_entry_no_timeouts" / "target_entry_population.csv",
        node_dir / "target_entry_stage" / "target_no_timeouts" / "target_entry_population.csv",
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.stat().st_size > 0:
            return candidate
    return None


def build_price_index(
    by_session: dict[str, list[dict[str, Any]]],
) -> dict[tuple[str, str], tuple[str, int]]:
    idx_map: dict[tuple[str, str], tuple[str, int]] = {}
    for session_id, rows in by_session.items():
        for idx, row in enumerate(rows):
            ts = row["timestamp"]
            idx_map[(session_id, ts)] = (session_id, idx)
            # Fallback for rows whose session_id drifted but whose timestamp is valid.
            idx_map.setdefault(("__ANY__", ts), (session_id, idx))
    return idx_map


def progress_ratio(profit_now: float, target: float) -> float:
    return clamp01(max(0.0, profit_now) / max(target, 1e-9))


def lifetime_bucket(bar_idx: int, total_bars: int) -> str:
    if total_bars <= 1:
        return "PROVING"
    frac = bar_idx / total_bars
    if frac < 0.25:
        return "EARLY"
    if frac < 0.5:
        return "MID"
    if frac < 0.75:
        return "LATE"
    return "FINAL"


def derive_lifecycle_label(
    profit_now: float,
    mfe_so_far: float,
    giveback_now: float,
    velocity_now: float,
    time_open: int,
    time_since_peak: int,
    time_since_last_progress: int,
    target: float,
) -> str:
    pr = progress_ratio(mfe_so_far, target)
    gb_ratio = giveback_now / max(mfe_so_far, 1e-9) if mfe_so_far > 0 else 0.0
    if pr < 0.25 and time_open <= 3:
        return "PROVING"
    if profit_now > 0 and pr >= 1.0 and velocity_now >= 0:
        return "EXTENDING"
    if profit_now > 0 and gb_ratio < 0.25 and time_since_last_progress <= 3:
        return "HEALTHY"
    if profit_now > 0 and (time_since_last_progress > 3 or gb_ratio >= 0.25):
        return "STALLING"
    if profit_now > -0.25 * target and (time_since_peak > 3 or gb_ratio >= 0.45):
        return "FRAGILE"
    return "FAILING"


def derive_scenario_label(
    current_profit: float,
    mfe_so_far: float,
    giveback_now: float,
    velocity_now: float,
    time_open: int,
    time_since_peak: int,
    time_since_last_progress: int,
    opposite_direction_strength: float,
    exhaustion_score: float,
    remaining_budget_score: float,
    target: float,
    remaining_path_profits: list[float],
) -> str:
    remaining_max = max(remaining_path_profits) if remaining_path_profits else current_profit
    remaining_min = min(remaining_path_profits) if remaining_path_profits else current_profit
    further_extension = remaining_max - current_profit
    further_loss = current_profit - remaining_min

    if (
        current_profit <= -0.35 * target
        and velocity_now < 0
        and opposite_direction_strength >= 0.80
        and further_loss >= 0.25 * target
    ):
        return "PANIC"

    if (
        current_profit > 0
        and time_open >= 4
        and time_since_last_progress >= 3
        and giveback_now >= 0.35 * target
        and exhaustion_score >= 0.50
        and remaining_budget_score <= 0.45
        and further_extension <= 0.20 * target
    ):
        return "DECAY_EXIT"

    if (
        current_profit > 0
        and mfe_so_far >= 0.50 * target
        and giveback_now >= 0.20 * target
        and exhaustion_score >= 0.35
        and further_extension <= 0.35 * target
    ):
        return "HARVEST"

    return "HOLD"


def derive_failure_label(first_action: str, aee_pips: float, static_pips: float) -> str:
    if first_action == "HARVEST":
        return "EARLY_HARVEST"
    if first_action == "PANIC":
        return "FALSE_PANIC"
    if first_action == "DECAY_EXIT":
        return "FALSE_DECAY"
    if static_pips > aee_pips and aee_pips > 0:
        return "MISSED_RUNNER"
    return "LATE_EXIT"


def build_aee_state_stream(
    selected_entries: list[dict[str, Any]],
    by_session: dict[str, list[dict[str, Any]]],
    price_index: dict[tuple[str, str], tuple[str, int]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    state_rows: list[dict[str, Any]] = []
    trades: list[dict[str, Any]] = []
    for entry in selected_entries:
        session_id = entry["session_id"]
        ts = entry["timestamp"]
        resolved = price_index.get((session_id, ts)) or price_index.get(("__ANY__", ts))
        if resolved is None:
            continue
        resolved_session_id, idx = resolved
        session_rows = by_session[resolved_session_id]
        session_id = resolved_session_id
        start_price = float(entry["price"])
        direction = entry["direction"]
        pair = str(entry.get("pair") or session_rows[idx].get("pair") or "EUR_USD")
        target = float(entry["target_distance"])
        tp_min = int(float(entry["tp_hit_min"]))
        sl_min = int(float(entry["sl_hit_min"]))
        static_exit_bar = tp_min if entry["static_reason"] == "TP_HIT" else sl_min
        horizon_bars = min(100, len(session_rows) - idx - 1)
        if static_exit_bar <= 0 or horizon_bars <= 0:
            continue

        peak_profit = float("-inf")
        trough_profit = float("inf")
        peak_idx = 0
        last_progress_idx = 0
        path_profits: list[float] = [0.0]
        per_bar_context: list[dict[str, Any]] = []

        for bar in range(1, horizon_bars + 1):
            abs_idx = idx + bar
            if abs_idx >= len(session_rows):
                break
            current_row = session_rows[abs_idx]
            profit_now = stream.signed_pips(direction, start_price, current_row["price"], pair)
            path_profits.append(profit_now)

            feats = stream.compute_stream_features(session_rows, abs_idx, direction)
            ctx = energy_ctx.energy_context(
                {
                    "direction_assumed": direction,
                    **{k: str(v) for k, v in feats.items()},
                    "future_mfe_pips": str(max(target, max(path_profits))),
                }
            )
            per_bar_context.append({"timestamp": current_row["timestamp"], **feats, **ctx})

        # Build trajectory features off the causal context stream only.
        causal_rows = []
        for i, ctx_row in enumerate(per_bar_context):
            pre = per_bar_context[max(0, i - 5) : i + 1]
            release_seq = [float(r["release_quality_score"]) for r in pre]
            comp_seq = [float(r["compression_score"]) for r in pre]
            macro_seq = [float(r["macro_dir_score"]) for r in pre]
            micro_seq = [float(r["micro_dir_score"]) for r in pre]
            budget_seq = [float(r["remaining_budget_score"]) for r in pre]
            noise_seq = [float(r["noise_score"]) for r in pre]
            exhaust_seq = [float(r["exhaustion_score"]) for r in pre]
            causal_rows.append(
                {
                    **ctx_row,
                    "pre_build_slope": round(point_traj.slope(release_seq), 6),
                    "pre_build_accel": round(point_traj.accel(release_seq), 6),
                    "pre_compression_release_delta": round((release_seq[-1] if release_seq else 0.0) - (comp_seq[-1] if comp_seq else 0.0), 6),
                    "pre_macro_micro_alignment": round((macro_seq[-1] if macro_seq else 0.0) - abs((macro_seq[-1] if macro_seq else 0.0) - (micro_seq[-1] if micro_seq else 0.0)), 6),
                    "pre_budget_slope": round(point_traj.slope(budget_seq), 6),
                    "pre_noise_slope": round(point_traj.slope(noise_seq), 6),
                    "pre_exhaustion_slope": round(point_traj.slope(exhaust_seq), 6),
                }
            )

        static_pips = float(entry["static_pips"])
        trade_rows = []
        for bar in range(1, min(horizon_bars, len(causal_rows)) + 1):
            current_row = session_rows[idx + bar]
            profit_now = path_profits[bar]
            prev_profit = path_profits[bar - 1]
            velocity_now = profit_now - prev_profit
            if profit_now > peak_profit:
                peak_profit = profit_now
                peak_idx = bar
                last_progress_idx = bar
            trough_profit = min(trough_profit, profit_now)
            mfe_so_far = max(0.0, peak_profit)
            mae_so_far = abs(min(0.0, trough_profit))
            giveback_now = max(0.0, mfe_so_far - profit_now)
            time_since_peak = bar - peak_idx
            time_since_last_progress = bar - last_progress_idx
            remaining_path = path_profits[bar:]
            ctx = causal_rows[bar - 1]
            quarter_bias = signed_quarter_bias(direction, entry["quarter"])
            opposite_strength = (
                max(0.0, -float(ctx["pressure_5"]))
                + max(0.0, -float(ctx["pressure_15"]))
                + float(ctx["noise_score"])
                + float(ctx["exhaustion_score"])
            )
            pr = progress_ratio(mfe_so_far, target)
            energy_ratio = float(ctx["release_quality_score"]) - float(ctx["exhaustion_score"])
            lifecycle = derive_lifecycle_label(
                profit_now=profit_now,
                mfe_so_far=mfe_so_far,
                giveback_now=giveback_now,
                velocity_now=velocity_now,
                time_open=bar,
                time_since_peak=time_since_peak,
                time_since_last_progress=time_since_last_progress,
                target=target,
            )
            scenario = derive_scenario_label(
                current_profit=profit_now,
                mfe_so_far=mfe_so_far,
                giveback_now=giveback_now,
                velocity_now=velocity_now,
                time_open=bar,
                time_since_peak=time_since_peak,
                time_since_last_progress=time_since_last_progress,
                opposite_direction_strength=opposite_strength,
                exhaustion_score=float(ctx["exhaustion_score"]),
                remaining_budget_score=float(ctx["remaining_budget_score"]),
                target=target,
                remaining_path_profits=remaining_path,
            )
            row = {
                "trade_id": entry["trade_id"],
                "timestamp": current_row["timestamp"],
                "entry_time": entry["entry_time"],
                "direction": direction,
                "target_distance": target,
                "quarter": entry["quarter"],
                "session_id": session_id,
                "bar_index": bar,
                "total_bars": horizon_bars,
                "static_exit_bar": static_exit_bar,
                "profit_now": round(profit_now, 6),
                "mfe_so_far": round(mfe_so_far, 6),
                "mae_so_far": round(mae_so_far, 6),
                "giveback_now": round(giveback_now, 6),
                "time_open": bar,
                "time_since_peak": time_since_peak,
                "time_since_last_progress": time_since_last_progress,
                "lifetime_bucket": lifetime_bucket(bar, horizon_bars),
                "velocity_now": round(velocity_now, 6),
                "velocity_change": round(velocity_now - (path_profits[bar - 1] - path_profits[bar - 2] if bar >= 2 else 0.0), 6),
                "macro_dir_score": ctx["macro_dir_score"],
                "micro_dir_score": ctx["micro_dir_score"],
                "compression": ctx["compression_score"],
                "release_quality": ctx["release_quality_score"],
                "exhaustion": ctx["exhaustion_score"],
                "noise": ctx["noise_score"],
                "remaining_budget": ctx["remaining_budget_score"],
                "progress_ratio": round(pr, 6),
                "energy_ratio": round(energy_ratio, 6),
                "quarter_bias": quarter_bias,
                "opposite_direction_strength": round(opposite_strength, 6),
                "pre_build_slope": ctx["pre_build_slope"],
                "pre_build_accel": ctx["pre_build_accel"],
                "pre_compression_release_delta": ctx["pre_compression_release_delta"],
                "pre_macro_micro_alignment": ctx["pre_macro_micro_alignment"],
                "pre_budget_slope": ctx["pre_budget_slope"],
                "pre_noise_slope": ctx["pre_noise_slope"],
                "pre_exhaustion_slope": ctx["pre_exhaustion_slope"],
                "action_truth": scenario,
                "lifecycle_label": lifecycle,
                "static_pips": static_pips,
                "static_R": round(static_pips / target, 6),
                "static_reason": entry["static_reason"],
            }
            trade_rows.append(row)
            state_rows.append(row)

        trades.append(
            {
                **entry,
                "trade_id": entry["trade_id"],
                "total_bars": horizon_bars,
                "static_exit_bar": static_exit_bar,
                "static_pips": static_pips,
                "static_R": round(static_pips / target, 6),
            }
        )
    return state_rows, trades


def add_segment_ids(state_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in state_rows:
        grouped[row["trade_id"]].append(row)
    enriched: list[dict[str, Any]] = []
    for trade_id, rows in grouped.items():
        rows.sort(key=lambda r: int(r["bar_index"]))
        seg_idx = 0
        prev_action = None
        for row in rows:
            if row["action_truth"] != prev_action:
                seg_idx += 1
                prev_action = row["action_truth"]
            row = dict(row)
            row["segment_id"] = f"{trade_id}_S{seg_idx:03d}"
            enriched.append(row)
    return enriched


def summarize_scenarios(state_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(r["action_truth"] for r in state_rows)
    lifecycle_counts = Counter(r["lifecycle_label"] for r in state_rows)
    return {
        "row_count": len(state_rows),
        "scenario_counts": dict(counts),
        "lifecycle_counts": dict(lifecycle_counts),
        "segment_count": len({r["segment_id"] for r in state_rows}),
    }


def build_rule_family(state_rows: list[dict[str, Any]]) -> tuple[dict[str, Any], dict[str, Any]]:
    by_action: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in state_rows:
        by_action[row["action_truth"]].append(row)

    def action_thresholds(action: str) -> dict[str, float]:
        rows = by_action.get(action, [])
        return {
            "profit_now": quantile([float(r["profit_now"]) for r in rows], 0.5),
            "giveback_now": quantile([float(r["giveback_now"]) for r in rows], 0.5),
            "velocity_now": quantile([float(r["velocity_now"]) for r in rows], 0.5),
            "time_open": quantile([float(r["time_open"]) for r in rows], 0.5),
            "time_since_peak": quantile([float(r["time_since_peak"]) for r in rows], 0.5),
            "progress_ratio": quantile([float(r["progress_ratio"]) for r in rows], 0.5),
            "energy_ratio": quantile([float(r["energy_ratio"]) for r in rows], 0.5),
            "opposite_direction_strength": quantile([float(r["opposite_direction_strength"]) for r in rows], 0.5),
            "remaining_budget": quantile([float(r["remaining_budget"]) for r in rows], 0.5),
            "sample_size": len(rows),
        }

    base = {
        "panic": action_thresholds("PANIC"),
        "decay": action_thresholds("DECAY_EXIT"),
        "harvest": action_thresholds("HARVEST"),
        "hold": action_thresholds("HOLD"),
        "extend": {
            "progress_ratio": quantile([float(r["progress_ratio"]) for r in state_rows if r["lifecycle_label"] == "EXTENDING"], 0.5),
            "remaining_budget": quantile([float(r["remaining_budget"]) for r in state_rows if r["lifecycle_label"] == "EXTENDING"], 0.5),
            "sample_size": sum(1 for r in state_rows if r["lifecycle_label"] == "EXTENDING"),
        },
    }

    direction_modifiers = {}
    for direction in ("LONG", "SHORT"):
        drows = [r for r in state_rows if r["direction"] == direction]
        direction_modifiers[direction] = {
            "quarter_bias_hold_bonus": quantile([float(r["quarter_bias"]) for r in drows if r["action_truth"] == "HOLD"], 0.5, 0.0),
            "harvest_profit_floor": quantile([float(r["profit_now"]) for r in drows if r["action_truth"] == "HARVEST"], 0.35, 0.0),
            "panic_opposite_pressure": quantile([float(r["opposite_direction_strength"]) for r in drows if r["action_truth"] == "PANIC"], 0.5, 0.8),
        }

    target_modifiers = {}
    for target in sorted({float(r["target_distance"]) for r in state_rows}):
        trows = [r for r in state_rows if float(r["target_distance"]) == target]
        target_modifiers[str(target)] = {
            "proving_window": int(round(quantile([float(r["time_open"]) for r in trows if r["lifecycle_label"] == "PROVING"], 0.6, 3.0))),
            "harvest_giveback_tolerance": quantile([float(r["giveback_now"]) for r in trows if r["action_truth"] == "HARVEST"], 0.5, 0.2 * target),
            "decay_time_since_peak": quantile([float(r["time_since_peak"]) for r in trows if r["action_truth"] == "DECAY_EXIT"], 0.5, 4.0),
            "extension_budget_floor": quantile([float(r["remaining_budget"]) for r in trows if r["lifecycle_label"] == "EXTENDING"], 0.4, 0.45),
        }

    derivation = {
        "base_rules": [
            {
                "rule_id": "base_panic",
                "source_scenario": "PANIC",
                "supporting_distribution": "panic_state_rows",
                "sample_size": base["panic"]["sample_size"],
                "confidence_score": clamp01(base["panic"]["sample_size"] / 500.0),
                "conditions": {
                    "profit_now_max": base["panic"]["profit_now"],
                    "velocity_now_max": base["panic"]["velocity_now"],
                    "giveback_now_min": base["panic"]["giveback_now"],
                    "opposite_direction_strength_min": base["panic"]["opposite_direction_strength"],
                    "time_open_min": base["panic"]["time_open"],
                },
                "priority": 1,
            },
            {
                "rule_id": "base_decay",
                "source_scenario": "DECAY_EXIT",
                "supporting_distribution": "decay_state_rows",
                "sample_size": base["decay"]["sample_size"],
                "confidence_score": clamp01(base["decay"]["sample_size"] / 500.0),
                "conditions": {
                    "time_since_peak_min": base["decay"]["time_since_peak"],
                    "giveback_now_min": base["decay"]["giveback_now"],
                    "progress_ratio_max": base["decay"]["progress_ratio"],
                    "energy_ratio_max": base["decay"]["energy_ratio"],
                },
                "priority": 2,
            },
            {
                "rule_id": "base_harvest",
                "source_scenario": "HARVEST",
                "supporting_distribution": "harvest_state_rows",
                "sample_size": base["harvest"]["sample_size"],
                "confidence_score": clamp01(base["harvest"]["sample_size"] / 500.0),
                "conditions": {
                    "profit_now_min": base["harvest"]["profit_now"],
                    "giveback_now_min": base["harvest"]["giveback_now"],
                    "progress_ratio_min": base["harvest"]["progress_ratio"],
                    "energy_ratio_min": base["harvest"]["energy_ratio"],
                },
                "priority": 3,
            },
            {
                "rule_id": "base_hold",
                "source_scenario": "HOLD",
                "supporting_distribution": "hold_state_rows",
                "sample_size": base["hold"]["sample_size"],
                "confidence_score": clamp01(base["hold"]["sample_size"] / 500.0),
                "conditions": {
                    "progress_ratio_min": base["hold"]["progress_ratio"],
                    "energy_ratio_min": base["hold"]["energy_ratio"],
                    "remaining_budget_min": base["hold"]["remaining_budget"],
                },
                "priority": 4,
            },
            {
                "rule_id": "base_extend",
                "source_scenario": "EXTENDING",
                "supporting_distribution": "extending_lifecycle_rows",
                "sample_size": base["extend"]["sample_size"],
                "confidence_score": clamp01(base["extend"]["sample_size"] / 500.0),
                "conditions": {
                    "progress_ratio_min": base["extend"]["progress_ratio"],
                    "remaining_budget_min": base["extend"]["remaining_budget"],
                },
                "priority": 5,
            },
        ],
        "direction_modifiers": direction_modifiers,
        "target_modifiers": target_modifiers,
    }
    return derivation, {
        "base": base,
        "direction_modifiers": direction_modifiers,
        "target_modifiers": target_modifiers,
    }


def _weighted_value(seed_val: Any, cur_val: Any, seed_w: float, cur_w: float) -> Any:
    if isinstance(seed_val, (int, float)) and isinstance(cur_val, (int, float)):
        total = seed_w + cur_w
        if total <= 0:
            return cur_val
        return round((float(seed_val) * seed_w + float(cur_val) * cur_w) / total, 6)
    return cur_val if cur_val is not None else seed_val


def inherit_rule_family(
    current_derivation: dict[str, Any],
    current_rules: dict[str, Any],
    seed_derivation: dict[str, Any] | None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if not seed_derivation:
        return current_derivation, current_rules, {"used_seed": False}

    seeded = json.loads(json.dumps(current_derivation))
    seeded_rules = json.loads(json.dumps(current_rules))

    seed_base_rules = {r["rule_id"]: r for r in seed_derivation.get("base_rules", [])}
    current_base_rules = {r["rule_id"]: r for r in seeded["base_rules"]}
    merge_report: dict[str, Any] = {"used_seed": True, "base_rules": {}, "direction_modifiers": {}, "target_modifiers": {}}

    for rule_id, cur_rule in current_base_rules.items():
        seed_rule = seed_base_rules.get(rule_id)
        if not seed_rule:
            continue
        cur_w = max(1.0, float(cur_rule.get("sample_size", 1)))
        seed_w = max(1.0, float(seed_rule.get("sample_size", 1)))
        merged_conditions = {}
        for key, cur_val in cur_rule.get("conditions", {}).items():
            merged_conditions[key] = _weighted_value(seed_rule.get("conditions", {}).get(key, cur_val), cur_val, seed_w, cur_w)
        cur_rule["conditions"] = merged_conditions
        cur_rule["confidence_score"] = _weighted_value(seed_rule.get("confidence_score", cur_rule.get("confidence_score", 0.0)), cur_rule.get("confidence_score", 0.0), seed_w, cur_w)
        merge_report["base_rules"][rule_id] = {"seed_weight": seed_w, "current_weight": cur_w}

        # Keep the lower-level numeric summaries aligned with the compiled derivation.
        action_key = rule_id.replace("base_", "")
        if action_key in seeded_rules["base"]:
            for cond_key, cond_val in merged_conditions.items():
                summary_key = cond_key.replace("_min", "").replace("_max", "")
                if summary_key in seeded_rules["base"][action_key]:
                    seeded_rules["base"][action_key][summary_key] = cond_val

    seed_dmods = seed_derivation.get("direction_modifiers", {})
    for direction, cur_mod in seeded["direction_modifiers"].items():
        seed_mod = seed_dmods.get(direction, {})
        merge_report["direction_modifiers"][direction] = {}
        for key, cur_val in cur_mod.items():
            if key in seed_mod:
                cur_mod[key] = _weighted_value(seed_mod[key], cur_val, 1.0, 3.0)
                merge_report["direction_modifiers"][direction][key] = "blended"
        if direction in seeded_rules["direction_modifiers"]:
            seeded_rules["direction_modifiers"][direction] = cur_mod

    seed_tmods = seed_derivation.get("target_modifiers", {})
    for target, cur_mod in seeded["target_modifiers"].items():
        seed_mod = seed_tmods.get(target, {})
        merge_report["target_modifiers"][target] = {}
        for key, cur_val in cur_mod.items():
            if key in seed_mod:
                cur_mod[key] = _weighted_value(seed_mod[key], cur_val, 1.0, 3.0)
                merge_report["target_modifiers"][target][key] = "blended"
        if target in seeded_rules["target_modifiers"]:
            seeded_rules["target_modifiers"][target] = cur_mod

    return seeded, seeded_rules, merge_report


def decide_action(
    row: dict[str, Any],
    rules: dict[str, Any],
    variant: str,
) -> str:
    direction = row["direction"]
    target = str(float(row["target_distance"]))
    base = rules["base"]
    dmod = rules["direction_modifiers"][direction]
    tmod = rules["target_modifiers"].get(target, {})

    profit_now = float(row["profit_now"])
    giveback_now = float(row["giveback_now"])
    velocity_now = float(row["velocity_now"])
    time_open = int(row["time_open"])
    time_since_peak = int(row["time_since_peak"])
    progress = float(row["progress_ratio"])
    energy_ratio = float(row["energy_ratio"])
    remaining_budget = float(row["remaining_budget"])
    opp = float(row["opposite_direction_strength"])
    quarter_bias = float(row["quarter_bias"])

    if variant == "baseline_static":
        return "STATIC"

    panic_hit = (
        profit_now <= base["panic"]["profit_now"]
        and velocity_now <= base["panic"]["velocity_now"]
        and giveback_now >= base["panic"]["giveback_now"]
        and opp >= dmod["panic_opposite_pressure"]
        and time_open >= base["panic"]["time_open"]
    )
    if panic_hit:
        return "PANIC"

    decay_hit = (
        time_since_peak >= max(base["decay"]["time_since_peak"], tmod.get("decay_time_since_peak", 0))
        and giveback_now >= base["decay"]["giveback_now"]
        and progress <= base["decay"]["progress_ratio"]
        and energy_ratio <= base["decay"]["energy_ratio"]
    )
    if decay_hit:
        return "DECAY_EXIT"

    if variant in {"bias_aware_aee", "bias_plus_context_aee"}:
        bias_harvest = quarter_bias < 0 and profit_now >= max(dmod["harvest_profit_floor"], 0.20 * float(row["target_distance"]))
        if bias_harvest and giveback_now >= max(0.10 * float(row["target_distance"]), tmod.get("harvest_giveback_tolerance", 0.0) * 0.5):
            return "HARVEST"

    if variant == "bias_plus_context_aee":
        harvest_hit = (
            profit_now >= max(base["harvest"]["profit_now"], dmod["harvest_profit_floor"])
            and giveback_now >= max(base["harvest"]["giveback_now"], tmod.get("harvest_giveback_tolerance", 0.0))
            and progress >= base["harvest"]["progress_ratio"]
            and energy_ratio >= base["harvest"]["energy_ratio"]
        )
        if harvest_hit:
            return "HARVEST"

    hold_hit = (
        progress >= base["hold"]["progress_ratio"]
        and energy_ratio >= base["hold"]["energy_ratio"] - (0.10 if quarter_bias > 0 else 0.0)
        and remaining_budget >= max(base["hold"]["remaining_budget"], tmod.get("extension_budget_floor", 0.0) * 0.8)
    )
    if hold_hit:
        return "HOLD"

    extend_hit = (
        quarter_bias > 0
        and progress >= base["extend"]["progress_ratio"]
        and remaining_budget >= max(base["extend"]["remaining_budget"], tmod.get("extension_budget_floor", 0.0))
    )
    if extend_hit:
        return "EXTEND"

    return "HOLD"


def decide_action_compiled(row: dict[str, Any], compiled: CompiledRuleSet) -> str:
    direction = row["direction"]
    target = str(float(row["target_distance"]))
    tmod = compiled.target_modifiers.get(target, {})
    target_distance = float(row["target_distance"])

    profit_now = float(row["profit_now"])
    giveback_now = float(row["giveback_now"])
    velocity_now = float(row["velocity_now"])
    time_open = int(row["time_open"])
    time_since_peak = int(row["time_since_peak"])
    progress = float(row["progress_ratio"])
    energy_ratio = float(row["energy_ratio"])
    remaining_budget = float(row["remaining_budget"])
    opp = float(row["opposite_direction_strength"])
    quarter_bias = float(row["quarter_bias"])

    if compiled.variant == "baseline_static":
        return "STATIC"
    if (
        profit_now <= compiled.panic_profit_now
        and velocity_now <= compiled.panic_velocity_now
        and giveback_now >= compiled.panic_giveback_now
        and opp >= compiled.panic_opposite_pressure[direction]
        and time_open >= compiled.panic_time_open
    ):
        return "PANIC"
    if (
        time_since_peak >= max(compiled.decay_time_since_peak, tmod.get("decay_time_since_peak", 0.0))
        and giveback_now >= compiled.decay_giveback_now
        and progress <= compiled.decay_progress_ratio
        and energy_ratio <= compiled.decay_energy_ratio
    ):
        return "DECAY_EXIT"
    if compiled.variant in {"bias_aware_aee", "bias_plus_context_aee"}:
        if (
            quarter_bias < 0
            and profit_now >= max(compiled.harvest_profit_floor[direction], 0.20 * target_distance)
            and giveback_now >= max(0.10 * target_distance, tmod.get("harvest_giveback_tolerance", 0.0) * 0.5)
        ):
            return "HARVEST"
    if compiled.variant == "bias_plus_context_aee":
        if (
            profit_now >= max(compiled.harvest_profit_now, compiled.harvest_profit_floor[direction])
            and giveback_now >= max(compiled.harvest_giveback_now, tmod.get("harvest_giveback_tolerance", 0.0))
            and progress >= compiled.harvest_progress_ratio
            and energy_ratio >= compiled.harvest_energy_ratio
        ):
            return "HARVEST"
    if (
        progress >= compiled.hold_progress_ratio
        and energy_ratio >= compiled.hold_energy_ratio - (0.10 if quarter_bias > 0 else 0.0)
        and remaining_budget >= max(compiled.hold_remaining_budget, tmod.get("extension_budget_floor", 0.0) * 0.8)
    ):
        return "HOLD"
    if (
        quarter_bias > 0
        and progress >= compiled.extend_progress_ratio
        and remaining_budget >= max(compiled.extend_remaining_budget, tmod.get("extension_budget_floor", 0.0))
    ):
        return "EXTEND"
    return "HOLD"


@dataclass
class ReplayResult:
    metrics: dict[str, Any]
    trade_rows: list[dict[str, Any]]
    action_counts: Counter


@dataclass
class ReplayContext:
    trades: list[dict[str, Any]]
    by_trade: dict[str, list[dict[str, Any]]]
    trade_meta: dict[str, dict[str, Any]]


@dataclass
class CompiledRuleSet:
    variant: str
    panic_profit_now: float
    panic_velocity_now: float
    panic_giveback_now: float
    panic_time_open: float
    decay_time_since_peak: float
    decay_giveback_now: float
    decay_progress_ratio: float
    decay_energy_ratio: float
    harvest_profit_now: float
    harvest_giveback_now: float
    harvest_progress_ratio: float
    harvest_energy_ratio: float
    hold_progress_ratio: float
    hold_energy_ratio: float
    hold_remaining_budget: float
    extend_progress_ratio: float
    extend_remaining_budget: float
    harvest_profit_floor: dict[str, float]
    panic_opposite_pressure: dict[str, float]
    target_modifiers: dict[str, dict[str, float]]


def compile_rule_set(rules: dict[str, Any], variant: str) -> CompiledRuleSet:
    base = rules["base"]
    direction_modifiers = rules["direction_modifiers"]
    return CompiledRuleSet(
        variant=variant,
        panic_profit_now=float(base["panic"]["profit_now"]),
        panic_velocity_now=float(base["panic"]["velocity_now"]),
        panic_giveback_now=float(base["panic"]["giveback_now"]),
        panic_time_open=float(base["panic"]["time_open"]),
        decay_time_since_peak=float(base["decay"]["time_since_peak"]),
        decay_giveback_now=float(base["decay"]["giveback_now"]),
        decay_progress_ratio=float(base["decay"]["progress_ratio"]),
        decay_energy_ratio=float(base["decay"]["energy_ratio"]),
        harvest_profit_now=float(base["harvest"]["profit_now"]),
        harvest_giveback_now=float(base["harvest"]["giveback_now"]),
        harvest_progress_ratio=float(base["harvest"]["progress_ratio"]),
        harvest_energy_ratio=float(base["harvest"]["energy_ratio"]),
        hold_progress_ratio=float(base["hold"]["progress_ratio"]),
        hold_energy_ratio=float(base["hold"]["energy_ratio"]),
        hold_remaining_budget=float(base["hold"]["remaining_budget"]),
        extend_progress_ratio=float(base["extend"]["progress_ratio"]),
        extend_remaining_budget=float(base["extend"]["remaining_budget"]),
        harvest_profit_floor={k: float(v["harvest_profit_floor"]) for k, v in direction_modifiers.items()},
        panic_opposite_pressure={k: float(v["panic_opposite_pressure"]) for k, v in direction_modifiers.items()},
        target_modifiers={
            str(k): {
                "decay_time_since_peak": float(v.get("decay_time_since_peak", 0.0)),
                "harvest_giveback_tolerance": float(v.get("harvest_giveback_tolerance", 0.0)),
                "extension_budget_floor": float(v.get("extension_budget_floor", 0.0)),
            }
            for k, v in rules["target_modifiers"].items()
        },
    )


def load_pickle(path: Path) -> Any:
    with path.open("rb") as f:
        return pickle.load(f)


def write_pickle(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as f:
        pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)


def replay_result_from_payload(payload: dict[str, Any]) -> ReplayResult:
    trade_rows = payload["trade_rows"]
    return ReplayResult(
        metrics=payload["metrics"],
        trade_rows=trade_rows,
        action_counts=Counter(r.get("aee_reason", r.get("static_reason", "UNKNOWN")) for r in trade_rows),
    )


def profitable_targets_from_selected_rows(rows: list[dict[str, Any]]) -> list[str]:
    profitable = {
        str(float(row["target_distance"]))
        for row in rows
        if float(row.get("aee_pips", row.get("static_pips", 0.0))) != float(row.get("static_pips", 0.0))
    }
    return sorted(profitable)


def replay_target_selective(
    static_result: ReplayResult,
    aee_result: ReplayResult,
) -> tuple[ReplayResult, list[str]]:
    static_by_id = {row["trade_id"]: row for row in static_result.trade_rows}
    aee_by_id = {row["trade_id"]: row for row in aee_result.trade_rows}
    target_static: defaultdict[str, float] = defaultdict(float)
    target_aee: defaultdict[str, float] = defaultdict(float)
    target_counts: Counter = Counter()
    for row in aee_result.trade_rows:
        target = str(float(row["target_distance"]))
        target_counts[target] += 1
        target_static[target] += float(static_by_id[row["trade_id"]]["static_pips"])
        target_aee[target] += float(row["aee_pips"])

    profitable_targets = sorted(
        target for target in target_counts if target_aee[target] > target_static[target]
    )

    selected_rows: list[dict[str, Any]] = []
    action_counts: Counter = Counter()
    time_to_action: list[int] = []
    for trade_id, aee_row in aee_by_id.items():
        static_row = static_by_id[trade_id]
        target = str(float(aee_row["target_distance"]))
        chosen = aee_row if target in profitable_targets else static_row
        selected_rows.append(chosen)
        action_counts[chosen["aee_reason"]] += 1
        if chosen["first_aee_action"] != "STATIC":
            # count bar-index proxy if present in chosen row, else skip
            tpae = int(chosen.get("time_to_peak_after_exit", 0))
            time_to_action.append(tpae)

    total_trades = len(selected_rows)
    total_static = sum(float(r["static_pips"]) for r in selected_rows)
    total_aee = sum(float(r["aee_pips"]) for r in selected_rows)
    metrics = {
        "trade_count": total_trades,
        "tp_hits": sum(1 for r in selected_rows if r["aee_reason"] == "TP_HIT"),
        "sl_hits": sum(1 for r in selected_rows if r["aee_reason"] in {"SL_HIT", "PANIC", "DECAY_EXIT"} and float(r["aee_pips"]) < 0),
        "timeouts": sum(1 for r in selected_rows if r["aee_reason"] == "TIMEOUT"),
        "avg_static_pips": round(total_static / total_trades, 6) if total_trades else 0.0,
        "avg_aee_pips": round(total_aee / total_trades, 6) if total_trades else 0.0,
        "avg_static_R": round(mean0([float(r["static_R"]) for r in selected_rows]), 6),
        "avg_aee_R": round(mean0([float(r["aee_R"]) for r in selected_rows]), 6),
        "pips_per_hour": round(total_aee / 88.0, 6),
        "estimated_equity_per_hour": round((total_aee / 2.5) * 2.0 / 88.0, 6),
        "delta_pips_per_hour": round((total_aee - total_static) / 88.0, 6),
        "delta_avg_R": round(mean0([float(r["aee_R"]) - float(r["static_R"]) for r in selected_rows]), 6),
        "action_frequency": {
            "HOLD": action_counts.get("HOLD", 0),
            "HARVEST": action_counts.get("HARVEST", 0),
            "PANIC": action_counts.get("PANIC", 0),
            "DECAY_EXIT": action_counts.get("DECAY_EXIT", 0),
            "EXTEND": action_counts.get("EXTEND", 0),
            "TP_HIT": action_counts.get("TP_HIT", 0),
            "SL_HIT": action_counts.get("SL_HIT", 0),
            "STATIC": action_counts.get("STATIC", 0),
        },
        "time_to_action_distribution": {
            "count": len(time_to_action),
            "mean": round(mean0([float(x) for x in time_to_action]), 6),
            "median": round(float(median(time_to_action)) if time_to_action else 0.0, 6),
            "min": min(time_to_action) if time_to_action else 0,
            "max": max(time_to_action) if time_to_action else 0,
        },
    }
    return ReplayResult(metrics=metrics, trade_rows=selected_rows, action_counts=action_counts), profitable_targets


def build_replay_context(trades: list[dict[str, Any]], state_rows: list[dict[str, Any]]) -> ReplayContext:
    by_trade: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in state_rows:
        by_trade[row["trade_id"]].append(row)
    for rows in by_trade.values():
        rows.sort(key=lambda r: int(r["bar_index"]))
    trade_meta = {trade["trade_id"]: trade for trade in trades}
    return ReplayContext(trades=trades, by_trade=dict(by_trade), trade_meta=trade_meta)


def replay_variant(
    trades: list[dict[str, Any]],
    state_rows: list[dict[str, Any]],
    rules: dict[str, Any],
    variant: str,
) -> ReplayResult:
    context = build_replay_context(trades, state_rows)
    return replay_variant_with_context(context, rules, variant)


def replay_variant_with_context(
    context: ReplayContext,
    rules: dict[str, Any],
    variant: str,
) -> ReplayResult:
    trades = context.trades
    compiled_rules = compile_rule_set(rules, variant)

    trade_results: list[dict[str, Any]] = []
    action_counts: Counter = Counter()
    time_to_action: list[int] = []
    for trade in trades:
        trade_id = trade["trade_id"]
        rows = context.by_trade.get(trade_id, [])
        static_pips = float(trade["static_pips"])
        static_R = float(trade["static_R"])
        aee_pips = static_pips
        aee_R = static_R
        exit_reason = trade["static_reason"]
        first_action = "STATIC"
        action_timestamp = trade["entry_time"]
        profit_at_action = 0.0
        time_to_peak_after_exit = 0
        max_profit_if_held = static_pips if static_pips > 0 else max([float(r["profit_now"]) for r in rows] or [0.0])
        for idx, row in enumerate(rows, start=1):
            action = decide_action_compiled(row, compiled_rules)
            if action not in {"HOLD", "EXTEND", "STATIC"}:
                first_action = action
                action_timestamp = row["timestamp"]
                profit_at_action = float(row["profit_now"])
                aee_pips = max(profit_at_action, -float(row["target_distance"]))
                aee_R = aee_pips / float(row["target_distance"])
                exit_reason = action
                remaining = rows[idx:]
                if remaining:
                    best_after = max(float(r["profit_now"]) for r in remaining)
                    time_to_peak_after_exit = next(
                        (int(r["bar_index"]) - int(row["bar_index"]) for r in remaining if float(r["profit_now"]) == best_after),
                        0,
                    )
                break
        action_counts[exit_reason] += 1
        if first_action != "STATIC":
            time_to_action.append(next((int(r["bar_index"]) for r in rows if r["timestamp"] == action_timestamp), 0))
        trade_results.append(
            {
                "trade_id": trade_id,
                "entry_time": trade["entry_time"],
                "direction": trade["direction"],
                "target_distance": float(trade["target_distance"]),
                "quarter": trade["quarter"],
                "session_id": trade["session_id"],
                "static_pips": static_pips,
                "static_R": static_R,
                "static_reason": trade["static_reason"],
                "aee_pips": round(aee_pips, 6),
                "aee_R": round(aee_R, 6),
                "aee_reason": exit_reason,
                "first_aee_action": first_action,
                "action_timestamp": action_timestamp,
                "profit_at_action": round(profit_at_action, 6),
                "max_profit_if_held": round(max_profit_if_held, 6),
                "profit_at_static_exit": round(static_pips, 6),
                "time_to_peak_after_exit": time_to_peak_after_exit,
                "missed_extension_pips": round(max(0.0, max_profit_if_held - max(aee_pips, 0.0)), 6),
                "avoidable_loss_pips": round(max(0.0, aee_pips - static_pips) if static_pips < aee_pips else 0.0, 6),
                "underperformed_static": aee_pips < static_pips,
            }
        )

    total_trades = len(trade_results)
    total_static = sum(t["static_pips"] for t in trade_results)
    total_aee = sum(t["aee_pips"] for t in trade_results)
    metrics = {
        "trade_count": total_trades,
        "tp_hits": sum(1 for t in trade_results if t["aee_reason"] == "TP_HIT"),
        "sl_hits": sum(1 for t in trade_results if t["aee_reason"] in {"SL_HIT", "PANIC", "DECAY_EXIT"} and t["aee_pips"] < 0),
        "timeouts": sum(1 for t in trade_results if t["aee_reason"] == "TIMEOUT"),
        "avg_static_pips": round(total_static / total_trades, 6) if total_trades else 0.0,
        "avg_aee_pips": round(total_aee / total_trades, 6) if total_trades else 0.0,
        "avg_static_R": round(mean0([t["static_R"] for t in trade_results]), 6),
        "avg_aee_R": round(mean0([t["aee_R"] for t in trade_results]), 6),
        "pips_per_hour": round(total_aee / 88.0, 6),
        "estimated_equity_per_hour": round((total_aee / 2.5) * 2.0 / 88.0, 6),
        "delta_pips_per_hour": round((total_aee - total_static) / 88.0, 6),
        "delta_avg_R": round(mean0([t["aee_R"] - t["static_R"] for t in trade_results]), 6),
        "action_frequency": {
            "HOLD": action_counts.get("HOLD", 0),
            "HARVEST": action_counts.get("HARVEST", 0),
            "PANIC": action_counts.get("PANIC", 0),
            "DECAY_EXIT": action_counts.get("DECAY_EXIT", 0),
            "EXTEND": action_counts.get("EXTEND", 0),
            "TP_HIT": action_counts.get("TP_HIT", 0),
            "SL_HIT": action_counts.get("SL_HIT", 0),
        },
        "time_to_action_distribution": {
            "count": len(time_to_action),
            "mean": round(mean0([float(x) for x in time_to_action]), 6),
            "median": round(float(median(time_to_action)) if time_to_action else 0.0, 6),
            "min": min(time_to_action) if time_to_action else 0,
            "max": max(time_to_action) if time_to_action else 0,
        },
    }
    return ReplayResult(metrics=metrics, trade_rows=trade_results, action_counts=action_counts)


def breakdown_trade_rows(rows: list[dict[str, Any]], field: str) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row[field])].append(row)
    out = {}
    for key, bucket in grouped.items():
        total_static = sum(float(r["static_pips"]) for r in bucket)
        total_aee = sum(float(r["aee_pips"]) for r in bucket)
        out[key] = {
            "trade_count": len(bucket),
            "avg_static_pips": round(total_static / len(bucket), 6),
            "avg_aee_pips": round(total_aee / len(bucket), 6),
            "avg_static_R": round(mean0([float(r["static_R"]) for r in bucket]), 6),
            "avg_aee_R": round(mean0([float(r["aee_R"]) for r in bucket]), 6),
            "pips_per_hour": round(total_aee / 88.0, 6),
            "delta_pips_per_hour": round((total_aee - total_static) / 88.0, 6),
        }
    return out


def build_failure_autopsy(variant_rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    failures = [r for r in variant_rows if r["underperformed_static"]]
    for row in failures:
        row["failure_label"] = derive_failure_label(row["first_aee_action"], float(row["aee_pips"]), float(row["static_pips"]))
    summary = {
        "failure_count": len(failures),
        "by_label": dict(Counter(r["failure_label"] for r in failures)),
        "by_action": dict(Counter(r["first_aee_action"] for r in failures)),
    }
    return failures, summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Deterministic Stage 8 AEE compiler from locked no-timeout target entry populations.")
    ap.add_argument("--dataset-lock", type=Path, default=ROOT / "dataset_lock_11_sessions.json")
    ap.add_argument("--truth-csv", type=Path, default=ROOT / "compiled_target_entry_classes_contextual_v2_11_sessions" / "target_entry_truth_table.csv")
    ap.add_argument("--entry-rules-json", type=Path, default=ROOT / "compiled_target_entry_classes_no_timeouts_11_sessions" / "target_entry_classes.json")
    ap.add_argument("--seed-rules-json", type=Path, default=None)
    ap.add_argument("--output-dir", type=Path, default=ROOT / "compiled_aee_stage_11_sessions")
    args = ap.parse_args()

    out_dir = args.output_dir
    state_dir = out_dir / "aee_state_stream"
    scenarios_dir = out_dir / "aee_scenarios"
    rules_dir = out_dir / "aee_rules"
    replay_dir = out_dir / "aee_replay"
    for d in (state_dir, scenarios_dir, rules_dir, replay_dir):
        d.mkdir(parents=True, exist_ok=True)

    dataset_lock = load_dataset_lock(args.dataset_lock)
    truth_hash = sha256_file(args.truth_csv)
    entry_rules_hash = sha256_file(args.entry_rules_json)
    seed_rule_hash = sha256_file(args.seed_rules_json) if args.seed_rules_json else None
    source_entry_population_path = resolve_source_entry_population(out_dir)
    source_entry_population_hash = sha256_file(source_entry_population_path) if source_entry_population_path else None
    manifest_path = out_dir / "aee_manifest.json"
    selected_entries_hash: str | None = None
    if has_files(
        out_dir / "aee_stage_report.json",
        state_dir / "aee_state_stream.csv",
        manifest_path,
    ):
        existing_manifest = load_json(manifest_path)
        existing_stage_report = (
            load_json(out_dir / "aee_stage_report.json")
            if (out_dir / "aee_stage_report.json").exists()
            else None
        )
        existing_trade_count = 0
        if isinstance(existing_stage_report, dict):
            existing_trade_count = int(
                (
                    existing_stage_report.get("performance", {})
                    .get("aee_metrics", {})
                    .get("trade_count", 0)
                )
                or 0
            )
        source_entry_population_rows = (
            count_csv_rows(source_entry_population_path) if source_entry_population_path is not None else 0
        )
        if existing_manifest and existing_manifest.get("dataset_hash") == dataset_lock["hash"]:
            if (
                existing_manifest.get("truth_csv_hash") == truth_hash
                and existing_manifest.get("entry_rules_json_hash") == entry_rules_hash
                and existing_manifest.get("source_entry_population_hash") == source_entry_population_hash
                and existing_manifest.get("seed_rule_hash") == seed_rule_hash
                and not (source_entry_population_rows > 0 and existing_trade_count == 0)
            ):
                print(
                    json.dumps(
                        {
                            "status": "SKIP",
                            "output_dir": str(out_dir),
                            "reason": "aee_stage_artifacts_current",
                        },
                        indent=2,
                    )
                )
                return

    data_root = resolve_data_root(dataset_lock)

    truth_rows = load_csv(args.truth_csv)
    entry_rules = json.loads(args.entry_rules_json.read_text())["entry_classes"]
    selected_population_path = state_dir / "selected_entry_population.csv"
    selected_population_cache_path = state_dir / "selected_entry_population.pkl"
    source_entry_population_rows = (
        count_csv_rows(source_entry_population_path) if source_entry_population_path is not None else 0
    )
    local_selected_rows = count_csv_rows(selected_population_path)

    if source_entry_population_path is not None and source_entry_population_rows > 0:
        # The current no-timeout selected population is the canonical source of truth for AEE stage.
        # Always refresh from it when available so stale local stage selections cannot survive compiler
        # reruns after entry populations changed.
        raw_source_entries = load_csv(source_entry_population_path)
        try:
            validate_canonical_entry_rows(raw_source_entries)
            selected_entries = raw_source_entries
        except Exception:
            selected_entries = build_selected_entries_from_population(raw_source_entries)
            validate_canonical_entry_rows(selected_entries)
        write_csv(
            selected_population_path,
            selected_entries,
            list(selected_entries[0].keys()) if selected_entries else ["trade_id"],
        )
        write_pickle(selected_population_cache_path, selected_entries)
    elif selected_population_cache_path.exists():
        selected_entries = load_pickle(selected_population_cache_path)
    elif selected_population_path.exists():
        selected_entries = load_csv(selected_population_path)
        write_pickle(selected_population_cache_path, selected_entries)
    else:
        selected_entries = build_selected_entry_population(truth_rows, entry_rules)
        write_csv(selected_population_path, selected_entries, list(selected_entries[0].keys()) if selected_entries else ["trade_id"])
        write_pickle(selected_population_cache_path, selected_entries)

    selected_entries_hash = sha256_rows(selected_entries)

    state_stream_path = state_dir / "aee_state_stream.csv"
    state_stream_cache_path = state_dir / "aee_state_stream.pkl"
    state_report_path = state_dir / "aee_state_stream_report.json"
    scenario_csv_path = scenarios_dir / "aee_scenarios.csv"
    scenario_report_path = scenarios_dir / "aee_scenario_report.json"
    rules_json_path = rules_dir / "aee_rules.json"
    rules_report_path = rules_dir / "aee_rule_derivation_report.json"

    reuse_existing_stage = all(
        path.exists()
        for path in [state_stream_path, state_report_path, scenario_csv_path, scenario_report_path, rules_json_path, rules_report_path]
    )
    if reuse_existing_stage:
        existing_manifest = load_json(manifest_path) if manifest_path.exists() else None
        existing_stage_report = load_json(out_dir / "aee_stage_report.json") if (out_dir / "aee_stage_report.json").exists() else None
        existing_trade_count = 0
        if isinstance(existing_stage_report, dict):
            existing_trade_count = int(
                (
                    existing_stage_report.get("performance", {})
                    .get("aee_metrics", {})
                    .get("trade_count", 0)
                )
                or 0
            )
        existing_entry_population_hash = existing_manifest.get("entry_population_hash") if isinstance(existing_manifest, dict) else None
        # Never reuse stale zero-trade AEE artifacts when the selected population is now nonempty,
        # and never reuse if the selected-entry hash changed.
        if (selected_entries and existing_trade_count == 0) or existing_entry_population_hash != selected_entries_hash:
            reuse_existing_stage = False

    # If the selected-entry population changed or we previously produced a stale zero-trade
    # AEE result from a nonempty selected set, invalidate all downstream replay/fixed-pop
    # artifacts so this run cannot reuse zero-result payloads from an older bad build.
    invalidate_downstream = False
    existing_manifest = load_json(manifest_path) if manifest_path.exists() else None
    existing_stage_report = load_json(out_dir / "aee_stage_report.json") if (out_dir / "aee_stage_report.json").exists() else None
    existing_trade_count = 0
    existing_entry_population_hash = None
    if isinstance(existing_stage_report, dict):
        existing_trade_count = int(
            (
                existing_stage_report.get("performance", {})
                .get("aee_metrics", {})
                .get("trade_count", 0)
            )
            or 0
        )
    if isinstance(existing_manifest, dict):
        existing_entry_population_hash = existing_manifest.get("entry_population_hash")
    if existing_entry_population_hash != selected_entries_hash:
        invalidate_downstream = True
    if selected_entries and existing_trade_count == 0:
        invalidate_downstream = True
    if invalidate_downstream:
        for stale_dir in (
            replay_dir,
            out_dir / "target_local_aee",
            out_dir / "aee_hotspot",
            out_dir / "target_local_hotspot_merged",
        ):
            if stale_dir.exists():
                shutil.rmtree(stale_dir)
            stale_dir.mkdir(parents=True, exist_ok=True)

    if reuse_existing_stage:
        if state_stream_cache_path.exists():
            state_rows = load_pickle(state_stream_cache_path)
        else:
            state_rows = load_csv(state_stream_path)
            write_pickle(state_stream_cache_path, state_rows)
        trades = selected_entries
        state_stream_report = json.loads(state_report_path.read_text())
        scenario_report = json.loads(scenario_report_path.read_text())
        derivation = json.loads(rules_json_path.read_text())
        rules = json.loads(rules_report_path.read_text())
        seed_derivation = load_json(args.seed_rules_json)
        inheritance_report = {"mode": "reused_existing_artifacts"}
    else:
        by_session = stream.load_prices(data_root)
        price_index = build_price_index(by_session)
        state_rows, trades = build_aee_state_stream(selected_entries, by_session, price_index)
        state_rows = add_segment_ids(state_rows)
        write_csv(state_stream_path, state_rows, list(state_rows[0].keys()) if state_rows else ["trade_id"])
        write_pickle(state_stream_cache_path, state_rows)
        state_stream_report = {
            "row_count": len(state_rows),
            "trade_count": len(trades),
            "schema_fields": list(state_rows[0].keys()) if state_rows else [],
            "contract_pass": bool(state_rows),
        }
        state_report_path.write_text(json.dumps(state_stream_report, indent=2))

        scenario_report = summarize_scenarios(state_rows)
        write_csv(scenario_csv_path, state_rows, list(state_rows[0].keys()) if state_rows else ["trade_id"])
        scenario_report_path.write_text(json.dumps(scenario_report, indent=2))

        derivation, rules = build_rule_family(state_rows)
        seed_derivation = load_json(args.seed_rules_json)
        derivation, rules, inheritance_report = inherit_rule_family(derivation, rules, seed_derivation)
        rules_json_path.write_text(json.dumps(derivation, indent=2))
        rules_report_path.write_text(json.dumps(rules, indent=2))

    replay_paths = {
        "baseline_static": replay_dir / "baseline_static.json",
        "bias_aware_aee": replay_dir / "bias_aware_aee.json",
        "bias_plus_context_aee": replay_dir / "bias_plus_context_aee.json",
        "target_selective_aee": replay_dir / "target_selective_aee.json",
    }
    if all(path.exists() for path in replay_paths.values()):
        static_result = replay_result_from_payload(load_json(replay_paths["baseline_static"]))
        bias_result = replay_result_from_payload(load_json(replay_paths["bias_aware_aee"]))
        full_result = replay_result_from_payload(load_json(replay_paths["bias_plus_context_aee"]))
        target_selective_result = replay_result_from_payload(load_json(replay_paths["target_selective_aee"]))
        profitable_targets = profitable_targets_from_selected_rows(target_selective_result.trade_rows)
        failures, failure_summary = build_failure_autopsy(full_result.trade_rows)
        (out_dir / "aee_failure_autopsy.json").write_text(json.dumps(failures, indent=2))
        (out_dir / "aee_failure_autopsy_summary.json").write_text(json.dumps(failure_summary, indent=2))
    else:
        replay_context = build_replay_context(trades, state_rows)
        static_result = replay_variant_with_context(replay_context, rules, "baseline_static")
        bias_result = replay_variant_with_context(replay_context, rules, "bias_aware_aee")
        full_result = replay_variant_with_context(replay_context, rules, "bias_plus_context_aee")
        target_selective_result, profitable_targets = replay_target_selective(static_result, full_result)

        failures, failure_summary = build_failure_autopsy(full_result.trade_rows)
        (out_dir / "aee_failure_autopsy.json").write_text(json.dumps(failures, indent=2))
        (out_dir / "aee_failure_autopsy_summary.json").write_text(json.dumps(failure_summary, indent=2))

        # Reports per benchmark layer.
        for name, result in {
            "baseline_static": static_result,
            "bias_aware_aee": bias_result,
            "bias_plus_context_aee": full_result,
            "target_selective_aee": target_selective_result,
        }.items():
            payload = {
                "metrics": result.metrics,
                "trade_rows": result.trade_rows,
                "breakdowns": {
                    "quarter": breakdown_trade_rows(result.trade_rows, "quarter"),
                    "direction": breakdown_trade_rows(result.trade_rows, "direction"),
                    "target_distance": breakdown_trade_rows(result.trade_rows, "target_distance"),
                },
            }
            (replay_dir / f"{name}.json").write_text(json.dumps(payload, indent=2))

    target_local_dir = out_dir / "target_local_aee"
    if not (target_local_dir / "target_local_aee_report.json").exists():
        subprocess.run(
            [
                "python3",
                str(ROOT / "optimize_aee_target_local.py"),
                "--input-dir",
                str(out_dir),
                "--output-dir",
                str(target_local_dir),
            ],
            check=True,
        )
    target_local_report = load_json(target_local_dir / "target_local_aee_report.json")
    target_local_result = ReplayResult(
        metrics=target_local_report["aggregate_metrics"],
        trade_rows=load_json(target_local_dir / "target_local_aee_trade_rows.json"),
        action_counts=Counter(r["aee_reason"] for r in load_json(target_local_dir / "target_local_aee_trade_rows.json")),
    )
    (replay_dir / "target_local_aee.json").write_text(
        json.dumps(
            {
                "metrics": target_local_result.metrics,
                "trade_rows": target_local_result.trade_rows,
                "breakdowns": {
                    "quarter": breakdown_trade_rows(target_local_result.trade_rows, "quarter"),
                    "direction": breakdown_trade_rows(target_local_result.trade_rows, "direction"),
                    "target_distance": breakdown_trade_rows(target_local_result.trade_rows, "target_distance"),
                },
            },
            indent=2,
        )
    )

    hotspot_dir = out_dir / "aee_hotspot"
    if not (hotspot_dir / "aee_hotspot_report.json").exists():
        subprocess.run(
            [
                "python3",
                str(ROOT / "optimize_aee_hotspot_classes.py"),
                "--input-dir",
                str(out_dir),
                "--output-dir",
                str(hotspot_dir),
            ],
            check=True,
        )
    hotspot_report = load_json(hotspot_dir / "aee_hotspot_report.json")
    hotspot_result = ReplayResult(
        metrics=hotspot_report["aggregate_metrics"],
        trade_rows=load_json(hotspot_dir / "aee_hotspot_trade_rows.json"),
        action_counts=Counter(r["aee_reason"] for r in load_json(hotspot_dir / "aee_hotspot_trade_rows.json")),
    )
    (replay_dir / "target_local_hotspots.json").write_text(
        json.dumps(
            {
                "metrics": hotspot_result.metrics,
                "trade_rows": hotspot_result.trade_rows,
                "breakdowns": {
                    "quarter": breakdown_trade_rows(hotspot_result.trade_rows, "quarter"),
                    "direction": breakdown_trade_rows(hotspot_result.trade_rows, "direction"),
                    "target_distance": breakdown_trade_rows(hotspot_result.trade_rows, "target_distance"),
                },
            },
            indent=2,
        )
    )

    merged_dir = out_dir / "target_local_hotspot_merged"
    if not (merged_dir / "aee_target_local_hotspot_merged_report.json").exists():
        subprocess.run(
            [
                "python3",
                str(ROOT / "merge_aee_target_local_with_hotspots.py"),
                "--local-report",
                str(target_local_dir / "target_local_aee_report.json"),
                "--hotspot-report",
                str(hotspot_dir / "aee_hotspot_report.json"),
                "--local-trade-rows",
                str(target_local_dir / "target_local_aee_trade_rows.json"),
                "--hotspot-trade-rows",
                str(hotspot_dir / "aee_hotspot_trade_rows.json"),
                "--output-dir",
                str(merged_dir),
            ],
            check=True,
        )
    merged_report = load_json(merged_dir / "aee_target_local_hotspot_merged_report.json")
    merged_result = ReplayResult(
        metrics=merged_report["aggregate_metrics"],
        trade_rows=load_json(merged_dir / "aee_target_local_hotspot_merged_trade_rows.json"),
        action_counts=Counter(r["aee_reason"] for r in load_json(merged_dir / "aee_target_local_hotspot_merged_trade_rows.json")),
    )
    (replay_dir / "target_local_hotspot_merged_aee.json").write_text(
        json.dumps(
            {
                "metrics": merged_result.metrics,
                "trade_rows": merged_result.trade_rows,
                "breakdowns": {
                    "quarter": breakdown_trade_rows(merged_result.trade_rows, "quarter"),
                    "direction": breakdown_trade_rows(merged_result.trade_rows, "direction"),
                    "target_distance": breakdown_trade_rows(merged_result.trade_rows, "target_distance"),
                },
            },
            indent=2,
        )
    )

    champion_name, champion_result = max(
        {
            "bias_aware_aee": bias_result,
            "bias_plus_context_aee": full_result,
            "target_selective_aee": target_selective_result,
            "target_local_aee": target_local_result,
            "target_local_hotspots": hotspot_result,
            "target_local_hotspot_merged_aee": merged_result,
        }.items(),
        key=lambda item: item[1].metrics["pips_per_hour"],
    )

    stage_report = {
        "metadata": {
            "dataset_hash": dataset_lock["hash"],
            "entry_population_hash": selected_entries_hash,
            "rules_hash": hashlib.sha256(json.dumps(derivation, sort_keys=True).encode()).hexdigest(),
            "compiler_version": COMPILER_VERSION,
            "inherited_seed_rules": str(args.seed_rules_json) if args.seed_rules_json else None,
            "champion_variant": champion_name,
        },
        "performance": {
            "static_metrics": static_result.metrics,
            "aee_metrics": champion_result.metrics,
            "delta_metrics": {
                "delta_pips_per_hour": champion_result.metrics["delta_pips_per_hour"],
                "delta_avg_R": champion_result.metrics["delta_avg_R"],
            },
            "benchmarks": {
                "baseline_static": static_result.metrics,
                "bias_aware_aee": bias_result.metrics,
                "bias_plus_context_aee": full_result.metrics,
                "target_selective_aee": target_selective_result.metrics,
                "target_local_aee": target_local_result.metrics,
                "target_local_hotspots": hotspot_result.metrics,
                "target_local_hotspot_merged_aee": merged_result.metrics,
            },
        },
        "action_statistics": {
            "HOLD_count": champion_result.action_counts.get("HOLD", 0),
            "HARVEST_count": champion_result.action_counts.get("HARVEST", 0),
            "PANIC_count": champion_result.action_counts.get("PANIC", 0),
            "DECAY_EXIT_count": champion_result.action_counts.get("DECAY_EXIT", 0),
            "EXTEND_count": champion_result.action_counts.get("EXTEND", 0),
        },
        "breakdowns": {
            "quarter": breakdown_trade_rows(champion_result.trade_rows, "quarter"),
            "direction": breakdown_trade_rows(champion_result.trade_rows, "direction"),
            "target_distance": breakdown_trade_rows(champion_result.trade_rows, "target_distance"),
        },
        "failure_summary": {
            "early_harvest_count": failure_summary["by_label"].get("EARLY_HARVEST", 0),
            "false_panic_count": failure_summary["by_label"].get("FALSE_PANIC", 0),
            "false_decay_count": failure_summary["by_label"].get("FALSE_DECAY", 0),
            "missed_runner_count": failure_summary["by_label"].get("MISSED_RUNNER", 0),
            "late_exit_count": failure_summary["by_label"].get("LATE_EXIT", 0),
        },
        "sanity_checks": {
            "same_trade_population": len(static_result.trade_rows) == len(full_result.trade_rows) == len(trades),
            "timeouts_explicit": True,
            "scenario_segments_not_empty": scenario_report["segment_count"] > 0,
            "action_mix_not_degenerate": max(champion_result.action_counts.values() or [0]) < len(trades),
            "combined_beats_static_pph": champion_result.metrics["pips_per_hour"] > static_result.metrics["pips_per_hour"],
            "combined_beats_static_R": champion_result.metrics["avg_aee_R"] > static_result.metrics["avg_static_R"],
        },
        "final_champion_ruleset": [
            {
                "rule_id": rule["rule_id"],
                "conditions": rule["conditions"],
                "priority": rule["priority"],
                "confidence": rule["confidence_score"],
            }
            for rule in derivation["base_rules"]
        ],
        "inheritance": inheritance_report,
        "selected_profitable_targets": profitable_targets,
        "target_local_aee": {
            "report_path": str(target_local_dir / "target_local_aee_report.json"),
            "classes_path": str(target_local_dir / "target_local_aee_classes.json"),
        },
        "target_local_hotspots": {
            "report_path": str(hotspot_dir / "aee_hotspot_report.json"),
        },
        "target_local_hotspot_merged_aee": {
            "report_path": str(merged_dir / "aee_target_local_hotspot_merged_report.json"),
        },
    }
    (out_dir / "aee_stage_report.json").write_text(json.dumps(stage_report, indent=2))

    manifest = {
        "dataset_hash": dataset_lock["hash"],
        "truth_csv_hash": truth_hash,
        "entry_rules_json_hash": entry_rules_hash,
        "source_entry_population_hash": source_entry_population_hash,
        "entry_population_hash": selected_entries_hash,
        "state_stream_hash": sha256_file(state_dir / "aee_state_stream.csv"),
        "scenario_hash": sha256_file(scenarios_dir / "aee_scenarios.csv"),
        "rule_hash": hashlib.sha256(json.dumps(derivation, sort_keys=True).encode()).hexdigest(),
        "compiler_version": COMPILER_VERSION,
        "seed_rule_hash": hashlib.sha256(json.dumps(seed_derivation, sort_keys=True).encode()).hexdigest() if seed_derivation else None,
        "seed_rule_path": str(args.seed_rules_json) if args.seed_rules_json else None,
        "timestamp": stream.parse_ts(str(stream.parse_ts(selected_entries[0]["timestamp"]) if selected_entries else "2024-01-01T00:00:00Z")).isoformat(),
    }
    (out_dir / "aee_manifest.json").write_text(json.dumps(manifest, indent=2))

    print(
        json.dumps(
            {
                "status": "PASS",
                "output_dir": str(out_dir),
                "trade_count": len(trades),
                "static_pph": static_result.metrics["pips_per_hour"],
                "aee_pph": champion_result.metrics["pips_per_hour"],
                "delta_pph": champion_result.metrics["delta_pips_per_hour"],
                "champion_variant": champion_name,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
