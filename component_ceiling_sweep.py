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
LEVELS = [2.5, 3.5, 5.0, 6.0, 7.0, 8.0]


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


def labels_by_key() -> Dict[tuple[str, str], str]:
    return {(r["timestamp_start"], r["direction"]): r["zone_label"] for r in load_csv("opportunity_zones_labeled.csv")}


def opportunities() -> Dict[tuple[str, str], Dict[str, Any]]:
    return {(r["timestamp_start"], r["direction"]): r for r in load_csv("phase1_correct_outputs/opportunities_dataset.csv")}


def cluster_rows(direction: str) -> List[Dict[str, Any]]:
    return [r for r in load_csv("opportunity_clusters.csv") if r["direction"] == direction]


def build_entry_states_for_level(direction: str, level: float) -> tuple[list[Dict[str, Any]], dict[str, str]]:
    opps = opportunities()
    labels = labels_by_key()
    out: List[Dict[str, Any]] = []
    cluster_teacher: Dict[str, str] = {}
    for cluster in cluster_rows(direction):
        member_ts = cluster["member_timestamps"].split("|")
        # teacher label for cluster: first state label in the cluster
        first_ts = member_ts[0]
        cluster_teacher[cluster["cluster_id"]] = labels[(first_ts, direction)]
        for ts in member_ts:
            opp = opps[(ts, direction)]
            future_mfe = float(opp["max_mfe_pips"])
            future_mae = float(opp["max_mae_pips"])
            out.append(
                {
                    "cluster_id": cluster["cluster_id"],
                    "timestamp": ts,
                    "direction": direction,
                    "speed": float(opp["speed"]),
                    "future_mfe": future_mfe,
                    "future_mae": future_mae,
                    "valid_entry": int(future_mfe >= level and future_mae <= level),
                    "entry_label": labels[(ts, direction)],
                }
            )
    return out, cluster_teacher


def entry_ceiling(direction: str, level: float) -> Dict[str, Any]:
    states, cluster_teacher = build_entry_states_for_level(direction, level)
    candidates = sorted({round(float(r["speed"]), 6) for r in states})
    best = None
    best_rows: List[Dict[str, Any]] = []
    counts = Counter(cluster_teacher.values())
    for threshold in candidates:
        raw = [r for r in states if float(r["speed"]) >= threshold and int(r["valid_entry"]) == 1]
        first_by_cluster: Dict[str, Dict[str, Any]] = {}
        for row in sorted(raw, key=lambda r: (r["cluster_id"], r["timestamp"])):
            first_by_cluster.setdefault(row["cluster_id"], row)
        chosen = list(first_by_cluster.values())
        if not chosen:
            continue
        chosen_counts = Counter(cluster_teacher[r["cluster_id"]] for r in chosen)
        capture = chosen_counts.get("GOOD", 0) / max(1, counts.get("GOOD", 0))
        bad_trigger = chosen_counts.get("BAD", 0) / max(1, counts.get("BAD", 0)) if counts.get("BAD", 0) else 0.0
        noise_trigger = chosen_counts.get("NOISE", 0) / max(1, counts.get("NOISE", 0)) if counts.get("NOISE", 0) else 0.0
        score = capture - 1.5 * bad_trigger
        cand = {
            "direction": direction,
            "tp_pips": level,
            "sl_pips": level,
            "speed_threshold": threshold,
            "cluster_population": len(cluster_teacher),
            "good_clusters": counts.get("GOOD", 0),
            "bad_clusters": counts.get("BAD", 0),
            "noise_clusters": counts.get("NOISE", 0),
            "trades_taken": len(chosen),
            "traded_good_clusters": chosen_counts.get("GOOD", 0),
            "traded_bad_clusters": chosen_counts.get("BAD", 0),
            "traded_noise_clusters": chosen_counts.get("NOISE", 0),
            "capture_rate": capture,
            "bad_trigger": bad_trigger,
            "noise_trigger": noise_trigger,
            "score": score,
            "constraint_satisfied": bad_trigger <= 0.15,
        }
        if best is None or (cand["constraint_satisfied"], cand["score"]) > (best["constraint_satisfied"], best["score"]):
            best = cand
            best_rows = chosen
    if best is None:
        return {
            "direction": direction,
            "tp_pips": level,
            "sl_pips": level,
            "speed_threshold": None,
            "cluster_population": len(cluster_teacher),
            "good_clusters": counts.get("GOOD", 0),
            "bad_clusters": counts.get("BAD", 0),
            "noise_clusters": counts.get("NOISE", 0),
            "trades_taken": 0,
            "traded_good_clusters": 0,
            "traded_bad_clusters": 0,
            "traded_noise_clusters": 0,
            "capture_rate": 0.0,
            "bad_trigger": 0.0,
            "noise_trigger": 0.0,
            "score": 0.0,
            "constraint_satisfied": False,
            "verdict": "FAIL",
            "trade_count": 0,
            "sl_tp_assumptions": {
                "stop_loss_pips": level,
                "take_profit_pips": level,
                "stop_loss_r": 1.0,
                "take_profit_r": 1.0,
            },
            "trade_population_rows": [],
        }
    best["verdict"] = "PASS" if best["traded_good_clusters"] > 0 else "FAIL"
    best["trade_count"] = best["trades_taken"]
    best["sl_tp_assumptions"] = {
        "stop_loss_pips": level,
        "take_profit_pips": level,
        "stop_loss_r": 1.0,
        "take_profit_r": 1.0,
    }
    best["trade_population_rows"] = best_rows
    return best


def static_exit(opp: Dict[str, Any], level: float) -> Dict[str, Any]:
    path = list(ast.literal_eval(opp["price_path"]))
    start = float(opp["price_start"])
    direction = opp["direction"]
    for px in path[1:]:
        cur = pnl(direction, start, px)
        if cur >= level:
            return {"pips": level, "r": 1.0, "reason": "TP_HIT"}
        if cur <= -level:
            return {"pips": -level, "r": -1.0, "reason": "SL_HIT"}
    final = pnl(direction, start, path[-1])
    return {"pips": final, "r": final / level, "reason": "TIMEOUT"}


def aee_exit(opp: Dict[str, Any], level: float) -> Dict[str, Any]:
    path = list(ast.literal_eval(opp["price_path"]))
    start = float(opp["price_start"])
    direction = opp["direction"]
    peak = 0.0
    exit_pips = pnl(direction, start, path[-1])
    exit_reason = "TIMEOUT"
    giveback_trigger = max(0.6, level * 0.24)
    runner = 0.0
    harvester = 0.0
    for px in path[1:]:
        cur = pnl(direction, start, px)
        peak = max(peak, cur)
        if peak >= level and (peak - cur) >= giveback_trigger:
            exit_pips = cur
            exit_reason = "HARVEST"
            harvester = max(level, min(cur, peak))
            runner = max(0.0, cur - level)
            break
        if cur <= -level:
            exit_pips = cur
            exit_reason = "PANIC"
            break
    if exit_reason == "TIMEOUT":
        harvester = min(max(exit_pips, 0.0), level)
        runner = max(0.0, exit_pips - harvester)
    return {
        "pips": exit_pips,
        "r": exit_pips / level,
        "reason": exit_reason,
        "runner_pips": runner,
        "harvester_pips": harvester,
        "giveback_trigger_pips": giveback_trigger,
    }


def aee_ceiling(direction: str, level: float, chosen_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    opps = opportunities()
    rows = []
    static_rs = []
    aee_rs = []
    runner = []
    harvester = []
    panic_hits = 0
    for row in chosen_rows:
        opp = opps[(row["timestamp"], direction)]
        s = static_exit(opp, level)
        a = aee_exit(opp, level)
        static_rs.append(s["r"])
        aee_rs.append(a["r"])
        runner.append(a["runner_pips"])
        harvester.append(a["harvester_pips"])
        if a["reason"] == "PANIC":
            panic_hits += 1
        rows.append(
            {
                "cluster_id": row["cluster_id"],
                "timestamp": row["timestamp"],
                "direction": direction,
                "static_pips": s["pips"],
                "static_r": s["r"],
                "static_reason": s["reason"],
                "aee_pips": a["pips"],
                "aee_r": a["r"],
                "aee_reason": a["reason"],
                "runner_pips": a["runner_pips"],
                "harvester_pips": a["harvester_pips"],
            }
        )
    delta = (mean(aee_rs) - mean(static_rs)) if rows else 0.0
    return {
        "direction": direction,
        "tp_pips": level,
        "sl_pips": level,
        "trade_population": len(rows),
        "static_avg_R": mean(static_rs) if rows else 0.0,
        "AEE_avg_R": mean(aee_rs) if rows else 0.0,
        "delta_R": delta,
        "avg_static_pips": mean([r["static_pips"] for r in rows]) if rows else 0.0,
        "avg_aee_pips": mean([r["aee_pips"] for r in rows]) if rows else 0.0,
        "pips_per_hour": (sum(r["aee_pips"] for r in rows) / 9.0) if rows else 0.0,
        "runner_contribution_pips": mean(runner) if rows else 0.0,
        "harvester_contribution_pips": mean(harvester) if rows else 0.0,
        "panic_contribution_rate": panic_hits / len(rows) if rows else 0.0,
        "decay_contribution_rate": 0.0,
        "extraction_efficiency": mean([r["aee_pips"] / max(level, r["static_pips"]) for r in rows]) if rows else 0.0,
        "verdict": "PASS" if delta > 0 else "FAIL",
        "sl_tp_baseline": {
            "static_take_profit_pips": level,
            "static_stop_loss_pips": level,
            "aee_harvest_trigger_pips": level,
            "aee_panic_trigger_pips": -level,
            "aee_giveback_trigger_pips": max(0.6, level * 0.24),
        },
        "trade_population_rows": rows,
    }


def main() -> None:
    summary_rows = []
    for level in LEVELS:
        e_long = entry_ceiling("LONG", level)
        e_short = entry_ceiling("SHORT", level)
        a_long = aee_ceiling("LONG", level, e_long["trade_population_rows"])
        a_short = aee_ceiling("SHORT", level, e_short["trade_population_rows"])
        write_json(f"sweep_entry_long_{level:g}.json", e_long)
        write_json(f"sweep_entry_short_{level:g}.json", e_short)
        write_json(f"sweep_aee_long_{level:g}.json", a_long)
        write_json(f"sweep_aee_short_{level:g}.json", a_short)
        for kind, obj in [("entry_long", e_long), ("entry_short", e_short), ("aee_long", a_long), ("aee_short", a_short)]:
            summary_rows.append(
                {
                    "component": kind,
                    "tp_pips": level,
                    "sl_pips": level,
                    "trade_count": obj.get("trade_count", obj.get("trade_population", 0)),
                    "capture_rate": obj.get("capture_rate", ""),
                    "bad_trigger": obj.get("bad_trigger", ""),
                    "avg_R": obj.get("AEE_avg_R", obj.get("static_avg_R", "")),
                    "delta_R": obj.get("delta_R", ""),
                    "pips_per_hour": obj.get("pips_per_hour", ""),
                    "verdict": obj["verdict"],
                }
            )
    write_csv(
        "component_ceiling_sweep_summary.csv",
        summary_rows,
        ["component", "tp_pips", "sl_pips", "trade_count", "capture_rate", "bad_trigger", "avg_R", "delta_R", "pips_per_hour", "verdict"],
    )


if __name__ == "__main__":
    main()
