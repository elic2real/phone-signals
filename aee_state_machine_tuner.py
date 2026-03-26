#!/usr/bin/env python3
from __future__ import annotations

import json
from itertools import product
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
                        clean["distance"] = float(dist)
                        rows.append(clean)
    return rows


def static_exit(row: Dict[str, Any]) -> float:
    return float(row["pips"])


def state_action(
    current_profit: float,
    giveback_now: float,
    velocity_now: float,
    time_open: int,
    time_since_peak: int,
    time_under_profit_threshold: int,
    opposite_direction_strength: float,
    direction: str,
    distance: float,
    base: Dict[str, Any],
    cfg: Dict[str, float],
) -> str:
    panic = base["panic"]
    decay = base["decay"]
    stall = base["stall"]
    bad_trade = base["bad_trade"]

    panic_time_open = max(5, int((panic["time_open_median"] if direction == "SHORT" else max(panic["time_open_median"], 12)) * cfg["panic_time_mult"]))
    panic_time_since_peak = max(2, int((panic["time_since_peak_median"] if direction == "SHORT" else max(panic["time_since_peak_median"], 6)) * cfg["panic_peak_mult"]))
    panic_opp = max(
        panic["opposite_direction_strength_median"] * cfg["panic_opp_mult"],
        bad_trade["opposite_direction_strength_median"] * cfg["bad_opp_mult"],
        distance * cfg["panic_dist_mult"] if direction == "LONG" else 0.0,
    )
    panic_profit = (-cfg["panic_profit_frac"] * distance) if direction == "LONG" else 0.0
    if (
        current_profit <= panic_profit
        and time_open >= panic_time_open
        and time_since_peak >= panic_time_since_peak
        and velocity_now <= panic["velocity_now_median"] * cfg["panic_velocity_mult"]
        and giveback_now >= panic["giveback_now_median"] * cfg["panic_giveback_mult"]
        and opposite_direction_strength >= panic_opp
    ):
        return "PANIC"

    if (
        current_profit <= 0
        and time_open >= int(bad_trade["time_open_median"] * cfg["bad_time_mult"])
        and opposite_direction_strength >= bad_trade["opposite_direction_strength_median"] * cfg["bad_trade_opp_mult"]
    ):
        return "DO_NOT_ENTER"

    if (
        time_open >= int(base["decay"]["time_open_median"] * cfg["decay_time_mult"])
        and time_since_peak >= int(base["decay"]["time_since_peak_median"] * cfg["decay_peak_mult"])
        and giveback_now >= base["decay"]["giveback_now_median"] * cfg["decay_giveback_mult"]
    ):
        return "DECAY_EXIT"

    if (
        current_profit > 0
        and abs(velocity_now) <= max(0.2, stall["velocity_now_median"] + cfg["harvest_velocity_add"])
        and time_open >= max(5, int(stall["time_open_median"] * cfg["harvest_time_mult"]))
        and time_under_profit_threshold >= max(3, int(stall["time_under_profit_threshold_median"] * cfg["harvest_under_mult"]))
        and giveback_now >= stall["giveback_now_median"] * cfg["harvest_giveback_mult"]
    ):
        return "HARVEST"

    return "HOLD"


def replay_trade(row: Dict[str, Any], base: Dict[str, Any], cfg: Dict[str, float]) -> Dict[str, Any]:
    path = row["price_path"]
    start = float(row["price_start"])
    direction = row["direction"]
    distance = float(row["distance"])

    peak_profit = 0.0
    peak_idx = 0
    under = 0
    exit_reason = "HOLD"
    for idx in range(1, len(path)):
        current = pnl(direction, start, path[idx])
        prev = pnl(direction, start, path[idx - 1])
        vel = current - prev
        if current > peak_profit:
            peak_profit = current
            peak_idx = idx
        giveback = max(0.0, peak_profit - current)
        under = under + 1 if current < 1.5 else 0
        opp = pnl("SHORT" if direction == "LONG" else "LONG", start, path[idx])
        action = state_action(current, giveback, vel, idx, idx - peak_idx, under, opp, direction, distance, base, cfg)
        if action in {"PANIC", "DECAY_EXIT", "HARVEST"}:
            exit_reason = action
            return {"aee_pips": max(current, -distance), "aee_R": max(current, -distance) / distance, "exit_reason": exit_reason}
    final = max(pnl(direction, start, path[-1]), -distance)
    return {"aee_pips": final, "aee_R": final / distance, "exit_reason": exit_reason}


def summarize(rows: List[Dict[str, Any]], base: Dict[str, Any], cfg: Dict[str, float], direction: str) -> Dict[str, Any]:
    rel = [r for r in rows if r["direction"] == direction]
    trades = []
    for row in rel:
        replay = replay_trade(row, base, cfg)
        trades.append({"static": static_exit(row), **replay})
    total_static = sum(t["static"] for t in trades)
    total_aee = sum(t["aee_pips"] for t in trades)
    return {
        "direction": direction,
        "trades": len(trades),
        "avg_static_pips": mean(t["static"] for t in trades) if trades else 0.0,
        "avg_aee_pips": mean(t["aee_pips"] for t in trades) if trades else 0.0,
        "avg_static_R": mean(t["static"] / 1.0 for t in trades) if trades else 0.0,
        "avg_aee_R": mean(t["aee_R"] for t in trades) if trades else 0.0,
        "static_pips_per_hour": total_static / 9.0 if trades else 0.0,
        "aee_pips_per_hour": total_aee / 9.0 if trades else 0.0,
        "delta_pips_per_hour": (total_aee - total_static) / 9.0 if trades else 0.0,
        "delta_avg_R": (mean(t["aee_R"] for t in trades) - mean(t["static"] for t in trades)) if trades else 0.0,
    }


def main() -> None:
    base = {
        "stall": load_json("aee_stall_thresholds.json"),
        "panic": load_json("aee_panic_thresholds.json"),
        "decay": load_json("aee_decay_thresholds.json"),
        "bad_trade": load_json("aee_bad_trade_thresholds.json"),
    }
    rows = collect_trade_rows()
    grid = product(
        [1.0, 1.15],      # panic_time_mult
        [1.0, 1.15],      # panic_peak_mult
        [1.5, 1.75],      # panic_opp_mult
        [1.0],            # bad_opp_mult
        [0.25, 0.33],     # panic_profit_frac
        [1.0],            # panic_velocity_mult
        [1.0, 1.1],       # panic_giveback_mult
        [1.0, 1.25],      # panic_dist_mult
        [1.0],            # bad_time_mult
        [1.25],           # bad_trade_opp_mult
        [1.0, 1.15],      # decay_time_mult
        [1.0],            # decay_peak_mult
        [1.0, 1.1],       # decay_giveback_mult
        [0.1, 0.12],      # harvest_velocity_add
        [0.33, 0.4],      # harvest_time_mult
        [0.33, 0.4],      # harvest_under_mult
        [0.25, 0.3],      # harvest_giveback_mult
    )
    best = None
    best_payload = None
    for vals in grid:
        cfg = {
            "panic_time_mult": vals[0],
            "panic_peak_mult": vals[1],
            "panic_opp_mult": vals[2],
            "bad_opp_mult": vals[3],
            "panic_profit_frac": vals[4],
            "panic_velocity_mult": vals[5],
            "panic_giveback_mult": vals[6],
            "panic_dist_mult": vals[7],
            "bad_time_mult": vals[8],
            "bad_trade_opp_mult": vals[9],
            "decay_time_mult": vals[10],
            "decay_peak_mult": vals[11],
            "decay_giveback_mult": vals[12],
            "harvest_velocity_add": vals[13],
            "harvest_time_mult": vals[14],
            "harvest_under_mult": vals[15],
            "harvest_giveback_mult": vals[16],
        }
        long_sum = summarize(rows, base, cfg, "LONG")
        short_sum = summarize(rows, base, cfg, "SHORT")
        combined_delta = long_sum["delta_pips_per_hour"] + short_sum["delta_pips_per_hour"]
        score = (
            1 if long_sum["delta_pips_per_hour"] > 0 else 0,
            1 if short_sum["delta_pips_per_hour"] > 0 else 0,
            combined_delta,
            long_sum["delta_avg_R"] + short_sum["delta_avg_R"],
        )
        if best is None or score > best:
            best = score
            best_payload = {"config": cfg, "long": long_sum, "short": short_sum, "combined_delta_pips_per_hour": combined_delta}
    (ROOT / "aee_state_machine_tuning_report.json").write_text(json.dumps(best_payload, indent=2))


if __name__ == "__main__":
    main()
