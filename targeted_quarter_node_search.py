from __future__ import annotations

import json
from copy import deepcopy
from itertools import product
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List

from aee_state_machine import collect_trade_rows, load_thresholds, pnl, static_exit
from state_key import compute_quarter


ROOT = Path(__file__).resolve().parent


def state_action_param(current_profit, giveback_now, velocity_now, time_open, time_since_peak, time_under_profit_threshold, opposite_direction_strength, direction, distance, thresholds, params):
    panic = thresholds["panic"]
    decay = thresholds["decay"]
    stall = thresholds["stall"]
    bad_trade = thresholds["bad_trade"]
    panic_time_open = max(5, int((panic["time_open_median"] if direction == "LONG" else max(1, panic["time_open_median"] // 2)) * params["panic_time_open_mult"]))
    panic_time_since_peak = max(2, int((panic["time_since_peak_median"] if direction == "LONG" else max(1, panic["time_since_peak_median"] // 2)) * params["panic_time_since_peak_mult"]))
    base_panic_opp = max(panic["opposite_direction_strength_median"] * params["panic_base_mult"], bad_trade["opposite_direction_strength_median"] * params["bad_trade_base_mult"])
    if direction == "LONG":
        panic_opp = max(base_panic_opp * params["long_panic_opp_mult"], distance * params["long_distance_floor_mult"])
        panic_current_profit = -params["long_panic_profit_floor"] * distance
    else:
        panic_opp = max(base_panic_opp * params["short_panic_opp_mult"], bad_trade["opposite_direction_strength_median"] * params["short_bad_trade_floor_mult"])
        panic_current_profit = params["short_panic_profit_floor"]
    decay_time_open = max(1, int(decay["time_open_median"] * params["decay_time_open_mult"]))
    decay_giveback = max(decay["giveback_now_median"] * params["decay_giveback_mult"], distance * params["decay_distance_floor_mult"])
    if current_profit <= panic_current_profit and time_open >= panic_time_open and time_since_peak >= panic_time_since_peak and velocity_now <= panic["velocity_now_median"] * params["panic_velocity_mult"] and giveback_now >= panic["giveback_now_median"] * params["panic_giveback_mult"] and opposite_direction_strength >= panic_opp:
        return "PANIC"
    if time_open >= int(bad_trade["time_open_median"] * params["bad_trade_time_mult"]) and opposite_direction_strength >= bad_trade["opposite_direction_strength_median"] * params["bad_trade_opp_mult"] and current_profit <= params["bad_trade_profit_floor"]:
        return "DO_NOT_ENTER"
    if time_open >= decay_time_open and time_since_peak >= int(decay["time_since_peak_median"] * params["decay_time_since_peak_mult"]) and giveback_now >= decay_giveback:
        return "DECAY_EXIT"
    if current_profit > 0 and abs(velocity_now) <= max(0.2, stall["velocity_now_median"] + params["harvest_velocity_add"]) and time_open >= max(5, int(stall["time_open_median"] * params["harvest_time_mult"])) and time_under_profit_threshold >= max(3, int(stall["time_under_profit_threshold_median"] * params["harvest_under_profit_mult"])) and giveback_now >= stall["giveback_now_median"] * params["harvest_giveback_mult"]:
        return "HARVEST"
    return "HOLD"


def replay_trade(row, thresholds, params):
    path = row["price_path"]; start = float(row["price_start"]); direction = row["direction"]; distance = float(row["distance"])
    peak_profit = 0.0; peak_idx = 0; time_under_profit_threshold = 0
    for idx in range(1, len(path)):
        current_profit = pnl(direction, start, path[idx]); prev_profit = pnl(direction, start, path[idx - 1]); velocity_now = current_profit - prev_profit
        if current_profit > peak_profit: peak_profit = current_profit; peak_idx = idx
        giveback_now = max(0.0, peak_profit - current_profit)
        time_under_profit_threshold = time_under_profit_threshold + 1 if current_profit < 1.5 else 0
        opposite_direction_strength = pnl("SHORT" if direction == "LONG" else "LONG", start, path[idx])
        action = state_action_param(current_profit, giveback_now, velocity_now, idx, idx - peak_idx, time_under_profit_threshold, opposite_direction_strength, direction, distance, thresholds, params)
        if action in {"PANIC", "DECAY_EXIT", "HARVEST"}:
            exit_pips = max(current_profit, -distance)
            return exit_pips, exit_pips / distance
    final_profit = max(pnl(direction, start, path[-1]), -distance)
    return final_profit, final_profit / distance


def summarize(rows, thresholds, params):
    vals = []
    for row in rows:
        static_pips, _ = static_exit(row)
        aee_pips, aee_r = replay_trade(row, thresholds, params)
        vals.append((static_pips, aee_pips, static_pips / float(row["distance"]), aee_r))
    static_total = sum(v[0] for v in vals); aee_total = sum(v[1] for v in vals)
    return {
        "trade_count": len(vals),
        "avg_static_pips": mean(v[0] for v in vals) if vals else 0.0,
        "avg_aee_pips": mean(v[1] for v in vals) if vals else 0.0,
        "avg_static_R": mean(v[2] for v in vals) if vals else 0.0,
        "avg_aee_R": mean(v[3] for v in vals) if vals else 0.0,
        "delta_pips_per_hour": (aee_total - static_total) / 2.0 if vals else 0.0,
    }


def main():
    thresholds = load_thresholds()
    rows = collect_trade_rows()
    optimized = json.loads((ROOT / "compiled_ceiling_quarters_optimized.json").read_text())
    rules = json.loads((ROOT / "quarter_ceiling_rules.json").read_text())
    targets = [("LONG","Q1"),("LONG","Q3"),("SHORT","Q2"),("SHORT","Q4")]
    out = {}
    for direction, quarter in targets:
        key = f"{direction}_{quarter}"
        base = deepcopy(rules[key]["params"])
        subset = [r for r in rows if r["direction"] == direction and compute_quarter(r["timestamp_start"], "LONDON") == quarter]
        best = summarize(subset, thresholds, base)
        best_params = deepcopy(base)
        for pto, ptsp, po, dto, dtsp, htm, hup, hgb, hva in product(
            [base["panic_time_open_mult"], base["panic_time_open_mult"]*1.15],
            [base["panic_time_since_peak_mult"], base["panic_time_since_peak_mult"]*1.15],
            [(base["long_panic_opp_mult"] if direction=="LONG" else base["short_panic_opp_mult"]), (base["long_panic_opp_mult"] if direction=="LONG" else base["short_panic_opp_mult"])*1.1],
            [base["decay_time_open_mult"], base["decay_time_open_mult"]*1.15],
            [base["decay_time_since_peak_mult"], base["decay_time_since_peak_mult"]*1.15],
            [base["harvest_time_mult"], max(0.1, base["harvest_time_mult"]*0.9)],
            [base["harvest_under_profit_mult"], max(0.1, base["harvest_under_profit_mult"]*0.9)],
            [base["harvest_giveback_mult"], max(0.05, base["harvest_giveback_mult"]*0.9)],
            [base["harvest_velocity_add"], base["harvest_velocity_add"]*1.1],
        ):
            params = deepcopy(base)
            params["panic_time_open_mult"] = pto
            params["panic_time_since_peak_mult"] = ptsp
            if direction == "LONG":
                params["long_panic_opp_mult"] = po
            else:
                params["short_panic_opp_mult"] = po
            params["decay_time_open_mult"] = dto
            params["decay_time_since_peak_mult"] = dtsP if False else dtsp
            params["harvest_time_mult"] = htm
            params["harvest_under_profit_mult"] = hup
            params["harvest_giveback_mult"] = hgb
            params["harvest_velocity_add"] = hva
            rep = summarize(subset, thresholds, params)
            if (rep["delta_pips_per_hour"], rep["avg_aee_pips"], rep["avg_aee_R"]) > (best["delta_pips_per_hour"], best["avg_aee_pips"], best["avg_aee_R"]):
                best = rep
                best_params = deepcopy(params)
        out[key] = {"best": best, "params": best_params, "baseline": optimized[direction.lower()][quarter]}
    (ROOT / "targeted_quarter_node_search.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
