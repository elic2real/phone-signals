from __future__ import annotations

import json
from copy import deepcopy
from functools import lru_cache
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Tuple

from state_key import compute_quarter
from aee_state_machine import collect_trade_rows, load_thresholds, pnl, static_exit


ROOT = Path(__file__).resolve().parent


@lru_cache(maxsize=1)
def load_quarter_bias() -> Dict[str, str]:
    path = ROOT / "quarter_max_ceiling_report.json"
    if not path.exists():
        return {}
    with path.open() as f:
        report = json.load(f)
    bias: Dict[str, str] = {}
    for quarter in ("Q1", "Q2", "Q3", "Q4"):
        long_pph = report["LONG"]["quarters"][quarter]["static_pph"]
        short_pph = report["SHORT"]["quarters"][quarter]["static_pph"]
        bias[quarter] = "LONG" if long_pph >= short_pph else "SHORT"
    return bias


def state_action_param(
    current_profit: float,
    giveback_now: float,
    velocity_now: float,
    time_open: int,
    time_since_peak: int,
    time_under_profit_threshold: int,
    opposite_direction_strength: float,
    direction: str,
    distance: float,
    quarter_bias: str | None,
    thresholds: Dict[str, Any],
    params: Dict[str, float],
) -> str:
    panic = thresholds["panic"]
    decay = thresholds["decay"]
    stall = thresholds["stall"]
    bad_trade = thresholds["bad_trade"]

    panic_time_open = max(5, int((panic["time_open_median"] if direction == "LONG" else panic["time_open_median"] // 2) * params["panic_time_open_mult"]))
    panic_time_since_peak = max(2, int((panic["time_since_peak_median"] if direction == "LONG" else panic["time_since_peak_median"] // 2) * params["panic_time_since_peak_mult"]))

    base_panic_opp = max(
        panic["opposite_direction_strength_median"] * params["panic_base_mult"],
        bad_trade["opposite_direction_strength_median"] * params["bad_trade_base_mult"],
    )
    if direction == "LONG":
        panic_opp = max(base_panic_opp * params["long_panic_opp_mult"], distance * params["long_distance_floor_mult"])
        panic_current_profit = -params["long_panic_profit_floor"] * distance
    else:
        panic_opp = max(base_panic_opp * params["short_panic_opp_mult"], bad_trade["opposite_direction_strength_median"] * params["short_bad_trade_floor_mult"])
        panic_current_profit = params["short_panic_profit_floor"]

    decay_time_open = max(1, int(decay["time_open_median"] * params["decay_time_open_mult"]))
    decay_giveback = max(decay["giveback_now_median"] * params["decay_giveback_mult"], distance * params["decay_distance_floor_mult"])

    if (
        current_profit <= panic_current_profit
        and time_open >= panic_time_open
        and time_since_peak >= panic_time_since_peak
        and velocity_now <= panic["velocity_now_median"] * params["panic_velocity_mult"]
        and giveback_now >= panic["giveback_now_median"] * params["panic_giveback_mult"]
        and opposite_direction_strength >= panic_opp
    ):
        return "PANIC"

    if (
        time_open >= int(bad_trade["time_open_median"] * params["bad_trade_time_mult"])
        and opposite_direction_strength >= bad_trade["opposite_direction_strength_median"] * params["bad_trade_opp_mult"]
        and current_profit <= params["bad_trade_profit_floor"]
    ):
        return "DO_NOT_ENTER"

    # On the non-dominant side of a quarter, bank profit early once the path
    # starts giving back instead of forcing it into the generic decay/panic path.
    if (
        quarter_bias
        and direction != quarter_bias
        and current_profit >= max(0.6, min(1.5, distance * 0.35))
        and giveback_now >= max(0.35, distance * 0.12)
        and velocity_now <= 0.0
        and time_open >= 5
        and time_since_peak >= 1
    ):
        return "HARVEST"

    # On the dominant side of a quarter, tolerate ordinary pullback/consolidation
    # longer so continuation has a chance to extend.
    if (
        quarter_bias
        and direction == quarter_bias
        and current_profit >= max(1.0, distance * 0.4)
        and giveback_now <= max(0.75, distance * 0.22)
        and velocity_now >= -0.4
        and time_since_peak <= max(3, int(stall["time_since_peak_median"] if "time_since_peak_median" in stall else 6))
    ):
        return "HOLD"

    if (
        time_open >= decay_time_open
        and time_since_peak >= int(decay["time_since_peak_median"] * params["decay_time_since_peak_mult"])
        and giveback_now >= decay_giveback
    ):
        return "DECAY_EXIT"

    if (
        current_profit > 0
        and abs(velocity_now) <= max(0.2, stall["velocity_now_median"] + params["harvest_velocity_add"])
        and time_open >= max(5, int(stall["time_open_median"] * params["harvest_time_mult"]))
        and time_under_profit_threshold >= max(3, int(stall["time_under_profit_threshold_median"] * params["harvest_under_profit_mult"]))
        and giveback_now >= stall["giveback_now_median"] * params["harvest_giveback_mult"]
    ):
        return "HARVEST"

    return "HOLD"


def replay_trade_param(row: Dict[str, Any], thresholds: Dict[str, Any], params: Dict[str, float]) -> Dict[str, Any]:
    path = row["price_path"]
    start = float(row["price_start"])
    direction = row["direction"]
    distance = float(row["distance"])
    quarter = compute_quarter(row["timestamp_start"])
    quarter_bias = load_quarter_bias().get(quarter)
    peak_profit = 0.0
    peak_idx = 0
    time_under_profit_threshold = 0
    action_counts = {"HOLD": 0, "HARVEST": 0, "PANIC": 0, "DECAY_EXIT": 0, "DO_NOT_ENTER": 0}

    for idx in range(1, len(path)):
        current_profit = pnl(direction, start, path[idx])
        prev_profit = pnl(direction, start, path[idx - 1])
        velocity_now = current_profit - prev_profit
        if current_profit > peak_profit:
            peak_profit = current_profit
            peak_idx = idx
        giveback_now = max(0.0, peak_profit - current_profit)
        if current_profit < 1.5:
            time_under_profit_threshold += 1
        else:
            time_under_profit_threshold = 0
        opposite_direction_strength = pnl("SHORT" if direction == "LONG" else "LONG", start, path[idx])
        action = state_action_param(
            current_profit=current_profit,
            giveback_now=giveback_now,
            velocity_now=velocity_now,
            time_open=idx,
            time_since_peak=idx - peak_idx,
            time_under_profit_threshold=time_under_profit_threshold,
            opposite_direction_strength=opposite_direction_strength,
            direction=direction,
            distance=distance,
            quarter_bias=quarter_bias,
            thresholds=thresholds,
            params=params,
        )
        action_counts[action] += 1
        if action in {"PANIC", "DECAY_EXIT", "HARVEST"}:
            exit_pips = max(current_profit, -distance)
            return {
                "aee_pips": exit_pips,
                "aee_R": exit_pips / distance,
                "exit_reason": action,
                "action_counts": action_counts,
            }

    final_profit = max(pnl(direction, start, path[-1]), -distance)
    return {
        "aee_pips": final_profit,
        "aee_R": final_profit / distance,
        "exit_reason": "HOLD",
        "action_counts": action_counts,
    }


def summarize(rows: List[Dict[str, Any]], label: str, thresholds: Dict[str, Any], params: Dict[str, float]) -> Dict[str, Any]:
    trades = []
    action_totals = {"HOLD": 0, "HARVEST": 0, "PANIC": 0, "DECAY_EXIT": 0, "DO_NOT_ENTER": 0}
    for row in rows:
        static_pips, static_reason = static_exit(row)
        replay = replay_trade_param(row, thresholds, params)
        for key, val in replay["action_counts"].items():
            action_totals[key] += val
        trades.append(
            {
                "cluster_id": row["cluster_id"],
                "timestamp_start": row["timestamp_start"],
                "direction": row["direction"],
                "distance": row["distance"],
                "entry_mode": row["entry_mode"],
                "static_pips": static_pips,
                "static_R": static_pips / float(row["distance"]),
                "static_reason": static_reason,
                **replay,
            }
        )
    total_trades = len(trades)
    total_aee_pips = sum(t["aee_pips"] for t in trades)
    total_static_pips = sum(t["static_pips"] for t in trades)
    return {
        "label": label,
        "total_trades": total_trades,
        "HOLD_count": sum(1 for t in trades if t["exit_reason"] == "HOLD"),
        "HARVEST_count": sum(1 for t in trades if t["exit_reason"] == "HARVEST"),
        "PANIC_count": sum(1 for t in trades if t["exit_reason"] == "PANIC"),
        "DECAY_EXIT_count": sum(1 for t in trades if t["exit_reason"] == "DECAY_EXIT"),
        "DO_NOT_ENTER_count": action_totals["DO_NOT_ENTER"],
        "avg_static_pips": mean(t["static_pips"] for t in trades) if trades else 0.0,
        "avg_aee_pips": mean(t["aee_pips"] for t in trades) if trades else 0.0,
        "avg_static_R": mean(t["static_R"] for t in trades) if trades else 0.0,
        "avg_aee_R": mean(t["aee_R"] for t in trades) if trades else 0.0,
        "static_pips_per_hour": total_static_pips / 2.0 if trades else 0.0,
        "aee_pips_per_hour": total_aee_pips / 2.0 if trades else 0.0,
        "delta_pips_per_hour": (total_aee_pips - total_static_pips) / 2.0 if trades else 0.0,
        "delta_avg_R": (mean(t["aee_R"] for t in trades) - mean(t["static_R"] for t in trades)) if trades else 0.0,
    }


BASE = {
    "panic_time_open_mult": 1.0,
    "panic_time_since_peak_mult": 1.0,
    "panic_base_mult": 1.5,
    "bad_trade_base_mult": 0.75,
    "long_panic_opp_mult": 1.8,
    "short_panic_opp_mult": 1.75,
    "long_distance_floor_mult": 1.25,
    "short_bad_trade_floor_mult": 1.0,
    "long_panic_profit_floor": 0.25,
    "short_panic_profit_floor": 0.0,
    "panic_velocity_mult": 1.0,
    "panic_giveback_mult": 1.0,
    "bad_trade_time_mult": 1.0,
    "bad_trade_opp_mult": 1.5,
    "bad_trade_profit_floor": 0.0,
    "decay_time_open_mult": 1.0,
    "decay_time_since_peak_mult": 1.0,
    "decay_giveback_mult": 1.0,
    "decay_distance_floor_mult": 1.0,
    "harvest_velocity_add": 0.1,
    "harvest_time_mult": 0.4,
    "harvest_under_profit_mult": 0.4,
    "harvest_giveback_mult": 0.3,
}


PRESETS = {
    "baseline": {},
    "patient_panic": {
        "panic_time_open_mult": 1.4,
        "panic_time_since_peak_mult": 1.35,
        "panic_giveback_mult": 1.15,
        "bad_trade_opp_mult": 1.7,
    },
    "late_decay": {
        "decay_time_open_mult": 1.35,
        "decay_time_since_peak_mult": 1.2,
        "decay_giveback_mult": 1.15,
    },
    "faster_harvest": {
        "harvest_time_mult": 0.3,
        "harvest_under_profit_mult": 0.3,
        "harvest_giveback_mult": 0.22,
        "harvest_velocity_add": 0.14,
    },
    "patient_trend": {
        "panic_time_open_mult": 1.5,
        "panic_time_since_peak_mult": 1.5,
        "long_panic_opp_mult": 2.1,
        "short_panic_opp_mult": 2.0,
        "decay_time_open_mult": 1.4,
        "decay_giveback_mult": 1.2,
    },
    "salvage_weak": {
        "panic_time_open_mult": 1.6,
        "panic_time_since_peak_mult": 1.6,
        "panic_giveback_mult": 1.25,
        "bad_trade_opp_mult": 1.9,
        "decay_time_open_mult": 1.5,
        "decay_time_since_peak_mult": 1.3,
        "harvest_time_mult": 0.28,
        "harvest_under_profit_mult": 0.28,
        "harvest_giveback_mult": 0.2,
        "harvest_velocity_add": 0.16,
    },
    "long_q3_rescue": {
        "panic_time_open_mult": 1.8,
        "panic_time_since_peak_mult": 1.7,
        "long_panic_opp_mult": 2.3,
        "long_distance_floor_mult": 1.5,
        "panic_giveback_mult": 1.3,
        "decay_time_open_mult": 1.6,
        "decay_time_since_peak_mult": 1.25,
        "decay_giveback_mult": 1.25,
        "harvest_time_mult": 0.26,
        "harvest_under_profit_mult": 0.3,
        "harvest_giveback_mult": 0.18,
        "harvest_velocity_add": 0.18,
    },
    "short_q2_rescue": {
        "panic_time_open_mult": 1.7,
        "panic_time_since_peak_mult": 1.5,
        "short_panic_opp_mult": 2.15,
        "bad_trade_opp_mult": 2.0,
        "decay_time_open_mult": 1.55,
        "decay_time_since_peak_mult": 1.25,
        "harvest_time_mult": 0.25,
        "harvest_under_profit_mult": 0.25,
        "harvest_giveback_mult": 0.18,
        "harvest_velocity_add": 0.18,
    },
    "ultra_patient": {
        "panic_time_open_mult": 2.0,
        "panic_time_since_peak_mult": 1.9,
        "panic_base_mult": 1.65,
        "bad_trade_base_mult": 0.8,
        "long_panic_opp_mult": 2.4,
        "short_panic_opp_mult": 2.2,
        "long_distance_floor_mult": 1.6,
        "short_bad_trade_floor_mult": 1.1,
        "panic_giveback_mult": 1.4,
        "bad_trade_opp_mult": 1.8,
        "decay_time_open_mult": 1.75,
        "decay_time_since_peak_mult": 1.35,
        "decay_giveback_mult": 1.3,
        "harvest_velocity_add": 0.2,
        "harvest_time_mult": 0.24,
        "harvest_under_profit_mult": 0.24,
        "harvest_giveback_mult": 0.16,
    },
    "panic_light": {
        "panic_time_open_mult": 2.2,
        "panic_time_since_peak_mult": 2.0,
        "panic_base_mult": 1.8,
        "bad_trade_base_mult": 0.9,
        "long_panic_opp_mult": 2.6,
        "short_panic_opp_mult": 2.35,
        "long_distance_floor_mult": 1.8,
        "short_bad_trade_floor_mult": 1.2,
        "panic_giveback_mult": 1.5,
        "bad_trade_opp_mult": 2.0,
        "bad_trade_time_mult": 1.2,
        "decay_time_open_mult": 1.9,
        "decay_time_since_peak_mult": 1.45,
        "decay_giveback_mult": 1.35,
        "harvest_velocity_add": 0.22,
        "harvest_time_mult": 0.22,
        "harvest_under_profit_mult": 0.22,
        "harvest_giveback_mult": 0.15,
    },
    "harvest_heavy": {
        "panic_time_open_mult": 1.4,
        "panic_time_since_peak_mult": 1.3,
        "panic_base_mult": 1.45,
        "long_panic_opp_mult": 1.9,
        "short_panic_opp_mult": 1.9,
        "panic_giveback_mult": 1.1,
        "decay_time_open_mult": 1.2,
        "decay_time_since_peak_mult": 1.1,
        "decay_giveback_mult": 1.05,
        "harvest_velocity_add": 0.28,
        "harvest_time_mult": 0.18,
        "harvest_under_profit_mult": 0.18,
        "harvest_giveback_mult": 0.12,
    },
    "q4_salvage": {
        "panic_time_open_mult": 2.1,
        "panic_time_since_peak_mult": 1.9,
        "panic_base_mult": 1.7,
        "long_panic_opp_mult": 2.5,
        "short_panic_opp_mult": 2.4,
        "long_distance_floor_mult": 1.75,
        "panic_giveback_mult": 1.45,
        "bad_trade_opp_mult": 1.9,
        "decay_time_open_mult": 2.0,
        "decay_time_since_peak_mult": 1.5,
        "decay_giveback_mult": 1.4,
        "harvest_velocity_add": 0.24,
        "harvest_time_mult": 0.2,
        "harvest_under_profit_mult": 0.2,
        "harvest_giveback_mult": 0.13,
    },
}


def merge_params(overrides: Dict[str, float]) -> Dict[str, float]:
    params = deepcopy(BASE)
    params.update(overrides)
    return params


def main() -> None:
    thresholds = load_thresholds()
    rows = collect_trade_rows()
    partitioned: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for direction in ("LONG", "SHORT"):
        for quarter in ("Q1", "Q2", "Q3", "Q4"):
            subset = [r for r in rows if r["direction"] == direction and compute_quarter(r["timestamp_start"], "LONDON") == quarter]
            partitioned[(direction, quarter)] = subset

    best_rules = {}
    reports = {"long": {}, "short": {}, "combined": {}}

    for direction in ("LONG", "SHORT"):
        for quarter in ("Q1", "Q2", "Q3", "Q4"):
            best_name = None
            best_params = None
            best_report = None
            subset = partitioned[(direction, quarter)]
            for name, overrides in PRESETS.items():
                params = merge_params(overrides)
                report = summarize(subset, f"{direction}_{quarter}", thresholds, params)
                score = (report["delta_pips_per_hour"], report["avg_aee_pips"])
                if best_report is None or score > (best_report["delta_pips_per_hour"], best_report["avg_aee_pips"]):
                    best_name = name
                    best_params = params
                    best_report = report
            side_key = direction.lower()
            best_rules[f"{direction}_{quarter}"] = {"preset": best_name, "params": best_params}
            reports[side_key][quarter] = best_report

    for quarter in ("Q1", "Q2", "Q3", "Q4"):
        combined_rows = [r for r in rows if compute_quarter(r["timestamp_start"], "LONDON") == quarter]
        long_params = best_rules[f"LONG_{quarter}"]["params"]
        short_params = best_rules[f"SHORT_{quarter}"]["params"]
        trades = []
        for row in combined_rows:
            params = long_params if row["direction"] == "LONG" else short_params
            static_pips, static_reason = static_exit(row)
            replay = replay_trade_param(row, thresholds, params)
            trades.append({
                "static_pips": static_pips,
                "static_R": static_pips / float(row["distance"]),
                "aee_pips": replay["aee_pips"],
                "aee_R": replay["aee_R"],
                "exit_reason": replay["exit_reason"],
            })
        static_pips = sum(t["static_pips"] for t in trades)
        aee_pips = sum(t["aee_pips"] for t in trades)
        reports["combined"][quarter] = {
            "label": "COMBINED",
            "quarter": quarter,
            "total_trades": len(trades),
            "HOLD_count": sum(1 for t in trades if t["exit_reason"] == "HOLD"),
            "HARVEST_count": sum(1 for t in trades if t["exit_reason"] == "HARVEST"),
            "PANIC_count": sum(1 for t in trades if t["exit_reason"] == "PANIC"),
            "DECAY_EXIT_count": sum(1 for t in trades if t["exit_reason"] == "DECAY_EXIT"),
            "DO_NOT_ENTER_count": sum(1 for t in trades if t["exit_reason"] == "DO_NOT_ENTER"),
            "avg_static_pips": mean(t["static_pips"] for t in trades) if trades else 0.0,
            "avg_aee_pips": mean(t["aee_pips"] for t in trades) if trades else 0.0,
            "avg_static_R": mean(t["static_R"] for t in trades) if trades else 0.0,
            "avg_aee_R": mean(t["aee_R"] for t in trades) if trades else 0.0,
            "static_pips_per_hour": static_pips / 2.0 if trades else 0.0,
            "aee_pips_per_hour": aee_pips / 2.0 if trades else 0.0,
            "delta_pips_per_hour": (aee_pips - static_pips) / 2.0 if trades else 0.0,
            "delta_avg_R": (mean(t["aee_R"] for t in trades) - mean(t["static_R"] for t in trades)) if trades else 0.0,
        }

    out = {
        "node": {"pair": "EUR_USD", "weekday": "monday", "session": "LONDON"},
        "best_rules": best_rules,
        **reports,
    }
    (ROOT / "quarter_ceiling_rules.json").write_text(json.dumps(best_rules, indent=2))
    (ROOT / "compiled_ceiling_quarters_optimized.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
