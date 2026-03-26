#!/usr/bin/env python3
from __future__ import annotations

import ast
import csv
import json
from itertools import product
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List


ROOT = Path(".")
PIP = 0.0001
LEVELS = [2.5, 3.5, 5.0, 6.0]
GIVEBACK_MULTS = [0.18, 0.24, 0.32, 0.40]
HARVEST_MULTS = [0.85, 1.0, 1.1]
PANIC_MULTS = [1.0, 0.8]


def load_csv(path: str) -> List[Dict[str, Any]]:
    with (ROOT / path).open() as f:
        return list(csv.DictReader(f))


def write_json(path: str, data: Dict[str, Any]) -> None:
    (ROOT / path).write_text(json.dumps(data, indent=2))


def pnl(direction: str, start: float, px: float) -> float:
    return ((px - start) / PIP) if direction == "LONG" else ((start - px) / PIP)


def trade_population(direction: str, level: float) -> List[Dict[str, Any]]:
    key = f"{level:g}"
    return json.loads((ROOT / f"sweep_entry_{direction.lower()}_{key}.json").read_text())["trade_population_rows"]


def opportunities() -> Dict[tuple[str, str], Dict[str, Any]]:
    return {(r["timestamp_start"], r["direction"]): r for r in load_csv("phase1_correct_outputs/opportunities_dataset.csv")}


def static_result(opp: Dict[str, Any], level: float) -> float:
    path = list(ast.literal_eval(opp["price_path"]))
    start = float(opp["price_start"])
    direction = opp["direction"]
    for px in path[1:]:
        cur = pnl(direction, start, px)
        if cur >= level:
            return level / level
        if cur <= -level:
            return -1.0
    return pnl(direction, start, path[-1]) / level


def aee_result(opp: Dict[str, Any], level: float, harvest_mult: float, giveback_mult: float, panic_mult: float) -> Dict[str, float]:
    path = list(ast.literal_eval(opp["price_path"]))
    start = float(opp["price_start"])
    direction = opp["direction"]
    harvest_trigger = level * harvest_mult
    giveback_trigger = max(0.4, level * giveback_mult)
    panic_trigger = -level * panic_mult
    peak = 0.0
    exit_pips = pnl(direction, start, path[-1])
    for px in path[1:]:
        cur = pnl(direction, start, px)
        peak = max(peak, cur)
        if peak >= harvest_trigger and (peak - cur) >= giveback_trigger:
            exit_pips = cur
            break
        if cur <= panic_trigger:
            exit_pips = cur
            break
    return {"pips": exit_pips, "r": exit_pips / level}


def optimize_direction_level(direction: str, level: float) -> Dict[str, Any]:
    pop = trade_population(direction, level)
    opps = opportunities()
    if not pop:
        return {
            "direction": direction,
            "tp_pips": level,
            "sl_pips": level,
            "trade_population": 0,
            "best_config": None,
            "baseline_static_avg_R": 0.0,
            "best_aee_avg_R": 0.0,
            "best_delta_R": 0.0,
            "verdict": "FAIL",
        }
    static_rs = []
    for row in pop:
        opp = opps[(row["timestamp"], direction)]
        static_rs.append(static_result(opp, level))
    baseline_static = mean(static_rs)
    best = None
    for harvest_mult, giveback_mult, panic_mult in product(HARVEST_MULTS, GIVEBACK_MULTS, PANIC_MULTS):
        aee_rs = []
        for row in pop:
            opp = opps[(row["timestamp"], direction)]
            aee_rs.append(aee_result(opp, level, harvest_mult, giveback_mult, panic_mult)["r"])
        avg_aee = mean(aee_rs)
        delta = avg_aee - baseline_static
        cand = {
            "harvest_trigger_pips": level * harvest_mult,
            "giveback_trigger_pips": max(0.4, level * giveback_mult),
            "panic_trigger_pips": -level * panic_mult,
            "aee_avg_R": avg_aee,
            "delta_R": delta,
        }
        if best is None or cand["delta_R"] > best["delta_R"]:
            best = cand
    return {
        "direction": direction,
        "tp_pips": level,
        "sl_pips": level,
        "trade_population": len(pop),
        "best_config": best,
        "baseline_static_avg_R": baseline_static,
        "best_aee_avg_R": best["aee_avg_R"],
        "best_delta_R": best["delta_R"],
        "verdict": "PASS" if best["delta_R"] > 0 else "FAIL",
    }


def main() -> None:
    summary = []
    for direction in ("LONG", "SHORT"):
        for level in LEVELS:
            result = optimize_direction_level(direction, level)
            write_json(f"aee_rule_sweep_{direction.lower()}_{level:g}.json", result)
            summary.append(result)
    write_json("aee_rule_sweep_summary.json", {"results": summary})


if __name__ == "__main__":
    main()
