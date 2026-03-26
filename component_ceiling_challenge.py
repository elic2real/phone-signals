#!/usr/bin/env python3
from __future__ import annotations

import ast
import csv
import json
from collections import Counter
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List


ROOT = Path(".")
PIP = 0.0001
TARGET_PIPS = 2.5
STOP_PIPS = 2.5


def load_csv(path: str) -> List[Dict[str, Any]]:
    with (ROOT / path).open() as f:
        return list(csv.DictReader(f))


def write_json(path: str, data: Dict[str, Any]) -> None:
    (ROOT / path).write_text(json.dumps(data, indent=2))


def write_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with (ROOT / path).open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def pnl(direction: str, start: float, px: float) -> float:
    return ((px - start) / PIP) if direction == "LONG" else ((start - px) / PIP)


def cluster_teacher_by_direction(direction: str) -> Dict[str, str]:
    labels = load_csv("entry_labels.csv")
    out: Dict[str, str] = {}
    for row in sorted(labels, key=lambda r: (r["cluster_id"], r["timestamp"])):
        if row["direction"] != direction:
            continue
        out.setdefault(row["cluster_id"], row["entry_label"])
    return out


def triggered_by_direction(direction: str) -> List[Dict[str, Any]]:
    entry = json.loads((ROOT / "entry_optimal_config.json").read_text())
    threshold = float(entry["config"]["confirm_disp_atr"])
    states = load_csv("entry_states.csv")
    raw = [r for r in states if r["direction"] == direction and float(r["speed"]) >= threshold and int(r["valid_entry"]) == 1]
    first_by_cluster: Dict[str, Dict[str, Any]] = {}
    for row in sorted(raw, key=lambda r: (r["cluster_id"], r["timestamp"])):
        first_by_cluster.setdefault(row["cluster_id"], row)
    return list(first_by_cluster.values())


def static_exit_result(opp: Dict[str, Any]) -> Dict[str, Any]:
    path = list(ast.literal_eval(opp["price_path"]))
    start = float(opp["price_start"])
    direction = opp["direction"]
    for px in path[1:]:
        cur = pnl(direction, start, px)
        if cur >= TARGET_PIPS:
            return {"r": TARGET_PIPS / STOP_PIPS, "pips": TARGET_PIPS, "reason": "TP_HIT"}
        if cur <= -STOP_PIPS:
            return {"r": -1.0, "pips": -STOP_PIPS, "reason": "SL_HIT"}
    final_pips = pnl(direction, start, path[-1])
    return {"r": final_pips / STOP_PIPS, "pips": final_pips, "reason": "TIMEOUT"}


def aee_exit_result(opp: Dict[str, Any]) -> Dict[str, Any]:
    rules = json.loads((ROOT / "aee_optimal_rules.json").read_text())["rules"]
    harvest_profit = float(rules["harvest"]["if_profit_ge_pips"])
    harvest_giveback = float(rules["harvest"]["if_giveback_ge_pips"])
    panic_p = float(rules["panic"]["if_profit_le_pips"])
    path = list(ast.literal_eval(opp["price_path"]))
    start = float(opp["price_start"])
    direction = opp["direction"]
    peak = 0.0
    exit_pips = pnl(direction, start, path[-1])
    exit_reason = "TIMEOUT"
    runner = 0.0
    harvester = 0.0
    bars_since_peak = 0
    for px in path[1:]:
        cur = pnl(direction, start, px)
        if cur > peak:
            peak = cur
            bars_since_peak = 0
        else:
            bars_since_peak += 1
        giveback = peak - cur
        if peak >= harvest_profit and giveback >= harvest_giveback:
            exit_pips = cur
            exit_reason = "HARVEST"
            harvester = max(TARGET_PIPS, cur)
            runner = max(0.0, cur - TARGET_PIPS)
            break
        if cur <= panic_p:
            exit_pips = cur
            exit_reason = "PANIC"
            break
    if exit_reason == "TIMEOUT":
        harvester = min(max(exit_pips, 0.0), TARGET_PIPS)
        runner = max(0.0, exit_pips - harvester)
    return {
        "r": exit_pips / STOP_PIPS,
        "pips": exit_pips,
        "reason": exit_reason,
        "runner_pips": runner,
        "harvester_pips": harvester,
        "bars_since_peak_proxy": bars_since_peak,
    }


def build_entry_ceiling(direction: str) -> Dict[str, Any]:
    teacher = cluster_teacher_by_direction(direction)
    triggered = triggered_by_direction(direction)
    traded_ids = {r["cluster_id"] for r in triggered}
    counts = Counter(teacher.values())
    triggered_counts = Counter(teacher[cid] for cid in traded_ids)
    trade_population = {
        "direction": direction,
        "cluster_population": len(teacher),
        "good_clusters": counts.get("GOOD", 0),
        "bad_clusters": counts.get("BAD", 0),
        "noise_clusters": counts.get("NOISE", 0),
        "trades_taken": len(triggered),
        "traded_good_clusters": triggered_counts.get("GOOD", 0),
        "traded_bad_clusters": triggered_counts.get("BAD", 0),
        "traded_noise_clusters": triggered_counts.get("NOISE", 0),
        "capture_rate": triggered_counts.get("GOOD", 0) / max(1, counts.get("GOOD", 0)),
        "bad_trigger": triggered_counts.get("BAD", 0) / max(1, counts.get("BAD", 0)) if counts.get("BAD", 0) else 0.0,
        "noise_trigger": triggered_counts.get("NOISE", 0) / max(1, counts.get("NOISE", 0)) if counts.get("NOISE", 0) else 0.0,
        "trade_count": len(triggered),
        "verdict": "PASS" if triggered_counts.get("GOOD", 0) > 0 else "FAIL",
        "sl_tp_assumptions": {
            "stop_loss_pips": STOP_PIPS,
            "take_profit_pips": TARGET_PIPS,
            "stop_loss_r": 1.0,
            "take_profit_r": 1.0,
        },
    }
    write_json(f"entry_{direction.lower()}_ceiling.json", trade_population)
    write_csv(
        f"entry_{direction.lower()}_trade_population.csv",
        triggered,
        ["cluster_id", "timestamp", "direction", "future_mfe", "future_mae", "valid_entry", "speed", "efficiency", "extension"],
    )
    return trade_population


def build_aee_ceiling(direction: str) -> Dict[str, Any]:
    triggered = triggered_by_direction(direction)
    opps = {(r["timestamp_start"], r["direction"]): r for r in load_csv("phase1_correct_outputs/opportunities_dataset.csv")}
    rows = []
    static_rs = []
    aee_rs = []
    runner_pips = []
    harvester_pips = []
    panic_hits = 0
    for row in triggered:
        opp = opps[(row["timestamp"], direction)]
        s = static_exit_result(opp)
        a = aee_exit_result(opp)
        static_rs.append(s["r"])
        aee_rs.append(a["r"])
        runner_pips.append(a["runner_pips"])
        harvester_pips.append(a["harvester_pips"])
        if a["reason"] == "PANIC":
            panic_hits += 1
        rows.append(
            {
                "cluster_id": row["cluster_id"],
                "timestamp": row["timestamp"],
                "direction": direction,
                "static_r": s["r"],
                "static_pips": s["pips"],
                "static_reason": s["reason"],
                "aee_r": a["r"],
                "aee_pips": a["pips"],
                "aee_reason": a["reason"],
                "runner_pips": a["runner_pips"],
                "harvester_pips": a["harvester_pips"],
            }
        )
    delta = (mean(aee_rs) - mean(static_rs)) if static_rs else 0.0
    report = {
        "direction": direction,
        "trade_population": len(triggered),
        "static_avg_R": mean(static_rs) if static_rs else 0.0,
        "AEE_avg_R": mean(aee_rs) if aee_rs else 0.0,
        "delta_R": delta,
        "avg_static_pips": mean([r["static_pips"] for r in rows]) if rows else 0.0,
        "avg_aee_pips": mean([r["aee_pips"] for r in rows]) if rows else 0.0,
        "pips_per_hour": (sum(r["aee_pips"] for r in rows) / 9.0) if rows else 0.0,
        "extraction_efficiency": (mean([r["aee_pips"] / max(r["static_pips"], TARGET_PIPS) for r in rows]) if rows else 0.0),
        "runner_contribution_pips": mean(runner_pips) if runner_pips else 0.0,
        "harvester_contribution_pips": mean(harvester_pips) if harvester_pips else 0.0,
        "panic_contribution_rate": panic_hits / len(triggered) if triggered else 0.0,
        "decay_contribution_rate": 0.0,
        "verdict": "PASS" if delta > 0 else "FAIL",
        "sl_tp_baseline": {
            "static_take_profit_pips": TARGET_PIPS,
            "static_stop_loss_pips": STOP_PIPS,
            "aee_harvest_trigger_pips": TARGET_PIPS,
            "aee_panic_trigger_pips": -STOP_PIPS,
        },
    }
    write_json(f"aee_{direction.lower()}_ceiling.json", report)
    write_csv(
        f"aee_{direction.lower()}_trade_population.csv",
        rows,
        ["cluster_id", "timestamp", "direction", "static_r", "static_pips", "static_reason", "aee_r", "aee_pips", "aee_reason", "runner_pips", "harvester_pips"],
    )
    return report


def main() -> None:
    build_entry_ceiling("LONG")
    build_entry_ceiling("SHORT")
    build_aee_ceiling("LONG")
    build_aee_ceiling("SHORT")


if __name__ == "__main__":
    main()
