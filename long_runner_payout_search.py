#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List

import entry_mode_distance_sweep as emds


ROOT = Path(".")
TARGET_DISTANCES = [2.5, 3.5, 5.0]


def load_profit_ceiling(distance: float) -> Dict[str, Any]:
    path = ROOT / f"ceiling_long_runner_{distance:g}_profit_ceiling.json"
    with path.open() as f:
        return json.load(f)


def simulate_runner_variant(row: Dict[str, Any], distance: float, partial_tp: float, partial_frac: float) -> Dict[str, float]:
    start = row["price_start"]
    direction = row["direction"]
    path = row["price_path"]
    partial_bank = partial_frac * partial_tp
    partial_hit = False
    for px in path[1:]:
        cur = emds.pnl(direction, start, px)
        if not partial_hit and cur >= partial_tp:
            partial_hit = True
        if cur <= -distance:
            if partial_hit:
                # assume the banked portion is already realized and the remainder stops out
                remainder_frac = max(0.0, 1.0 - partial_frac)
                total = partial_bank - remainder_frac * distance
                return {
                    "pips": total,
                    "r": total / distance,
                    "reason": "PARTIAL_THEN_SL",
                    "partial_bank_pips": partial_bank,
                    "runner_pips": -remainder_frac * distance,
                }
            return {"pips": -distance, "r": -1.0, "reason": "SL_HIT", "partial_bank_pips": 0.0, "runner_pips": 0.0}
    if partial_hit:
        for px in path[1:]:
            cur = emds.pnl(direction, start, px)
            if cur >= distance:
                runner_pips = (1.0 - partial_frac) * max(distance - partial_tp, 0.0)
                total = partial_bank + runner_pips
                return {
                    "pips": total,
                    "r": total / distance,
                    "reason": "PARTIAL_PLUS_TP",
                    "partial_bank_pips": partial_bank,
                    "runner_pips": runner_pips,
                }
        return {
            "pips": partial_bank,
            "r": partial_bank / distance,
            "reason": "PARTIAL_ONLY",
            "partial_bank_pips": partial_bank,
            "runner_pips": 0.0,
        }
    final = emds.pnl(direction, start, path[-1])
    return {"pips": final, "r": final / distance, "reason": "TIMEOUT", "partial_bank_pips": 0.0, "runner_pips": 0.0}


def score_variant(rows: List[Dict[str, Any]], distance: float, partial_tp: float, partial_frac: float) -> Dict[str, Any]:
    sim_rows = [{**row, **simulate_runner_variant(row, distance, partial_tp, partial_frac)} for row in rows]
    total_pips = sum(r["pips"] for r in sim_rows)
    wins = sum(1 for r in sim_rows if r["pips"] > 0)
    losses = sum(1 for r in sim_rows if r["pips"] < 0)
    return {
        "distance": distance,
        "partial_tp": partial_tp,
        "partial_fraction": partial_frac,
        "trade_count": len(sim_rows),
        "wins": wins,
        "losses": losses,
        "win_rate": wins / len(sim_rows) if sim_rows else 0.0,
        "total_pips": total_pips,
        "avg_pips": mean(r["pips"] for r in sim_rows) if sim_rows else 0.0,
        "pips_per_hour": total_pips / 9.0 if sim_rows else 0.0,
        "estimated_equity_per_hour_at_2pct_risk": ((total_pips / distance) * 0.02) / 9.0 if sim_rows else 0.0,
        "partial_bank_avg_pips": mean(r.get("partial_bank_pips", 0.0) for r in sim_rows) if sim_rows else 0.0,
        "runner_avg_pips": mean(r.get("runner_pips", 0.0) for r in sim_rows) if sim_rows else 0.0,
        "rows": sim_rows,
    }


def main() -> None:
    report: Dict[str, Any] = {"results": {}}
    partial_tps = [0.75, 1.0, 1.25, 1.5, 1.75, 2.0, 2.5]
    partial_fracs = [0.5, 0.6, 0.7, 0.8, 0.9]
    for distance in TARGET_DISTANCES:
        base = load_profit_ceiling(distance)
        rows = base["rows"]
        best = None
        for partial_tp in [x for x in partial_tps if x <= distance]:
            for partial_frac in partial_fracs:
                result = score_variant(rows, distance, partial_tp, partial_frac)
                score = (result["total_pips"], result["win_rate"], -result["losses"])
                if best is None or score > best[0]:
                    best = (score, result)
        report["results"][f"{distance:g}"] = {
            "base_profit_ceiling": {
                "trade_count": base["trade_count"],
                "wins": base["wins"],
                "losses": base["losses"],
                "win_rate": base["win_rate"],
                "total_pips": base["total_pips"],
                "pips_per_hour": base["pips_per_hour"],
                "estimated_equity_per_hour_at_2pct_risk": base["estimated_equity_per_hour_at_2pct_risk"],
            },
            "best_variant": best[1],
        }
        (ROOT / f"long_runner_{distance:g}_payout_search.json").write_text(json.dumps(report["results"][f"{distance:g}"], indent=2, default=str))
    (ROOT / "long_runner_payout_search_report.json").write_text(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
