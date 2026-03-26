#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Tuple


ROOT = Path(".")
PIP = 0.0001


def pnl(direction: str, start: float, px: float) -> float:
    return ((px - start) / PIP) if direction == "LONG" else ((start - px) / PIP)


def load_json(name: str) -> Dict[str, Any]:
    with (ROOT / name).open() as f:
        return json.load(f)


def load_thresholds() -> Dict[str, Any]:
    return {
        "stall": load_json("aee_stall_thresholds.json"),
        "panic": load_json("aee_panic_thresholds.json"),
        "decay": load_json("aee_decay_thresholds.json"),
        "bad_trade": load_json("aee_bad_trade_thresholds.json"),
    }


def collect_trade_rows() -> List[Dict[str, Any]]:
    unified = load_json("entry_metric_ceiling_report_unified.json")
    rows: List[Dict[str, Any]] = []
    for side in ("long", "short"):
        for mode in ("harvester", "runner"):
            for dist, payload in unified["results"][side][mode].items():
                profit = payload.get("profit_ceiling")
                if profit and profit.get("rows"):
                    for row in profit["rows"]:
                        clean = dict(row)
                        clean["entry_mode"] = mode
                        clean["entry_direction"] = side.upper()
                        clean["distance"] = float(dist)
                        rows.append(clean)
    return rows


def static_exit(row: Dict[str, Any]) -> Tuple[float, str]:
    return float(row["pips"]), row.get("reason", "STATIC")


def state_action(
    current_profit: float,
    peak_profit: float,
    giveback_now: float,
    velocity_now: float,
    time_open: int,
    time_since_peak: int,
    time_under_profit_threshold: int,
    opposite_direction_strength: float,
    direction: str,
    distance: float,
    thresholds: Dict[str, Any],
) -> str:
    panic = thresholds["panic"]
    decay = thresholds["decay"]
    stall = thresholds["stall"]
    bad_trade = thresholds["bad_trade"]

    # Only panic on strong adverse acceleration after some trade life and clear opposite pressure.
    panic_time_open = max(5, panic["time_open_median"] // 2)
    panic_time_since_peak = max(2, panic["time_since_peak_median"] // 2)
    panic_opp = max(
        panic["opposite_direction_strength_median"] * 1.5,
        bad_trade["opposite_direction_strength_median"] * 0.75,
    )
    panic_current_profit = 0.0
    decay_time_open = decay["time_open_median"]
    decay_giveback = decay["giveback_now_median"]
    if direction == "LONG":
        panic_time_open = max(panic["time_open_median"], 16)
        panic_time_since_peak = max(panic["time_since_peak_median"], 8)
        panic_opp = max(panic_opp * 1.8, distance * 1.25)
        panic_current_profit = -0.25 * distance
        decay_time_open = max(decay["time_open_median"], 75)
        decay_giveback = max(decay["giveback_now_median"], distance)
    else:
        panic_opp = max(panic_opp * 1.75, bad_trade["opposite_direction_strength_median"])

    if (
        current_profit <= panic_current_profit
        and time_open >= panic_time_open
        and time_since_peak >= panic_time_since_peak
        and velocity_now <= panic["velocity_now_median"]
        and giveback_now >= panic["giveback_now_median"]
        and opposite_direction_strength >= panic_opp
    ):
        return "PANIC"
    if (
        time_open >= bad_trade["time_open_median"]
        and opposite_direction_strength >= bad_trade["opposite_direction_strength_median"] * 1.5
        and current_profit <= 0
    ):
        return "DO_NOT_ENTER"
    if (
        time_open >= decay_time_open
        and time_since_peak >= decay["time_since_peak_median"]
        and giveback_now >= decay_giveback
    ):
        return "DECAY_EXIT"
    if (
        current_profit > 0
        and abs(velocity_now) <= max(0.2, stall["velocity_now_median"] + 0.1)
        and time_open >= max(5, int(stall["time_open_median"] * 0.4))
        and time_under_profit_threshold >= max(3, int(stall["time_under_profit_threshold_median"] * 0.4))
        and giveback_now >= stall["giveback_now_median"] * 0.3
    ):
        return "HARVEST"
    return "HOLD"


def replay_trade(row: Dict[str, Any], thresholds: Dict[str, Any]) -> Dict[str, Any]:
    path = row["price_path"]
    start = float(row["price_start"])
    direction = row["direction"]
    distance = float(row["distance"])

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
        action = state_action(
            current_profit=current_profit,
            peak_profit=peak_profit,
            giveback_now=giveback_now,
            velocity_now=velocity_now,
            time_open=idx,
            time_since_peak=idx - peak_idx,
            time_under_profit_threshold=time_under_profit_threshold,
            opposite_direction_strength=opposite_direction_strength,
            direction=direction,
            distance=distance,
            thresholds=thresholds,
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


def summarize(rows: List[Dict[str, Any]], label: str, thresholds: Dict[str, Any]) -> Dict[str, Any]:
    trades = []
    action_totals = {"HOLD": 0, "HARVEST": 0, "PANIC": 0, "DECAY_EXIT": 0, "DO_NOT_ENTER": 0}
    for row in rows:
        static_pips, static_reason = static_exit(row)
        replay = replay_trade(row, thresholds)
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
    avg_static_pips = mean(t["static_pips"] for t in trades) if trades else 0.0
    avg_aee_pips = mean(t["aee_pips"] for t in trades) if trades else 0.0
    avg_static_r = mean(t["static_R"] for t in trades) if trades else 0.0
    avg_aee_r = mean(t["aee_R"] for t in trades) if trades else 0.0
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
        "avg_static_pips": avg_static_pips,
        "avg_aee_pips": avg_aee_pips,
        "avg_static_R": avg_static_r,
        "avg_aee_R": avg_aee_r,
        "static_pips_per_hour": total_static_pips / 9.0 if trades else 0.0,
        "aee_pips_per_hour": total_aee_pips / 9.0 if trades else 0.0,
        "delta_pips_per_hour": (total_aee_pips - total_static_pips) / 9.0 if trades else 0.0,
        "delta_avg_R": avg_aee_r - avg_static_r,
        "trades": trades,
    }


def main() -> None:
    thresholds = load_thresholds()
    rows = collect_trade_rows()
    long_rows = [r for r in rows if r["direction"] == "LONG"]
    short_rows = [r for r in rows if r["direction"] == "SHORT"]
    combined = rows

    rules = {
        "panic": thresholds["panic"],
        "decay": thresholds["decay"],
        "stall": thresholds["stall"],
        "bad_trade": thresholds["bad_trade"],
    }
    (ROOT / "aee_state_machine_rules.json").write_text(json.dumps(rules, indent=2))

    long_report = summarize(long_rows, "LONG", thresholds)
    short_report = summarize(short_rows, "SHORT", thresholds)
    combined_report = summarize(combined, "COMBINED", thresholds)

    (ROOT / "aee_state_machine_replay_long.json").write_text(json.dumps(long_report, indent=2))
    (ROOT / "aee_state_machine_replay_short.json").write_text(json.dumps(short_report, indent=2))
    (ROOT / "aee_state_machine_replay_combined.json").write_text(json.dumps(combined_report, indent=2))


if __name__ == "__main__":
    main()
