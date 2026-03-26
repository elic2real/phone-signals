#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import aee_state_machine as asm

ROOT = Path(".")


def _trade_path_stats(row: Dict[str, Any]) -> Dict[str, float]:
    direction = str(row["direction"])
    start = float(row["price_start"])
    distance = float(row["distance"])
    path = list(row["price_path"])

    pnl_path = [asm.pnl(direction, start, px) for px in path]
    mfe_pips = max(pnl_path) if pnl_path else 0.0
    mae_pips = min(pnl_path) if pnl_path else 0.0
    return {
        "mfe_pips": mfe_pips,
        "mae_pips": mae_pips,
        "mfe_r": (mfe_pips / distance) if distance > 0 else 0.0,
        "mae_r": (mae_pips / distance) if distance > 0 else 0.0,
    }


def build_real_tape_proof() -> Dict[str, Any]:
    # Doctrine-aligned cutoffs used to classify tape behavior for summary metrics.
    meaningful_green_r = 0.10
    strong_continuation_r = 1.00
    extend_capture_min_r = 0.70

    thresholds = asm.load_thresholds()
    rows = asm.collect_trade_rows()

    trades: List[Dict[str, Any]] = []
    for row in rows:
        static_pips, _ = asm.static_exit(row)
        replay = asm.replay_trade(row, thresholds)
        path_stats = _trade_path_stats(row)
        distance = float(row["distance"])

        trades.append(
            {
                "entry_mode": str(row.get("entry_mode", "")).upper(),
                "direction": str(row["direction"]),
                "distance": distance,
                "static_R": static_pips / distance if distance > 0 else 0.0,
                "aee_R": float(replay["aee_R"]),
                "aee_pips": float(replay["aee_pips"]),
                "exit_reason": str(replay["exit_reason"]),
                **path_stats,
            }
        )

    total = len(trades)
    green = [t for t in trades if t["mfe_r"] >= meaningful_green_r]
    strong = [t for t in trades if t["mfe_r"] >= strong_continuation_r]

    # "Closed in profit" means AEE produced an explicit close action (non-HOLD) and realized > 0R.
    green_closed_profit = [
        t
        for t in green
        if t["exit_reason"] != "HOLD" and t["aee_R"] > 0.0
    ]

    # "Allowed to extend" means strong continuation realized at least 0.70R after AEE management.
    strong_allowed_extend = [t for t in strong if t["aee_R"] >= extend_capture_min_r]

    gross_r_before = sum(t["static_R"] for t in trades)
    net_r_after = sum(t["aee_R"] for t in trades)

    def _pct(part: int, whole: int) -> float:
        if whole <= 0:
            return 0.0
        return (100.0 * float(part)) / float(whole)

    by_mode: Dict[str, Dict[str, Any]] = {}
    for mode in ("HARVESTER", "RUNNER"):
        subset = [t for t in trades if t["entry_mode"] == mode]
        subset_green = [t for t in subset if t["mfe_r"] >= meaningful_green_r]
        subset_strong = [t for t in subset if t["mfe_r"] >= strong_continuation_r]
        subset_green_closed_profit = [
            t for t in subset_green if t["exit_reason"] != "HOLD" and t["aee_R"] > 0.0
        ]
        subset_strong_allowed = [t for t in subset_strong if t["aee_R"] >= extend_capture_min_r]

        by_mode[mode] = {
            "trades": len(subset),
            "went_green_pct": round(_pct(len(subset_green), len(subset)), 2),
            "green_closed_in_profit_pct": round(_pct(len(subset_green_closed_profit), len(subset_green)), 2),
            "strong_continuations_allowed_pct": round(_pct(len(subset_strong_allowed), len(subset_strong)), 2),
            "gross_r_before_aee": round(sum(t["static_R"] for t in subset), 4),
            "net_r_after_aee": round(sum(t["aee_R"] for t in subset), 4),
        }

    proof = {
        "metric_definitions": {
            "meaningful_green_r_threshold": meaningful_green_r,
            "strong_continuation_r_threshold": strong_continuation_r,
            "allowed_extend_min_realized_r": extend_capture_min_r,
            "green_closed_in_profit_rule": "exit_reason != HOLD and aee_R > 0",
        },
        "summary": {
            "trades": total,
            "went_green_pct": round(_pct(len(green), total), 2),
            "green_closed_in_profit_pct": round(_pct(len(green_closed_profit), len(green)), 2),
            "strong_continuations_allowed_pct": round(_pct(len(strong_allowed_extend), len(strong)), 2),
            "gross_r_before_aee": round(gross_r_before, 4),
            "net_r_after_aee": round(net_r_after, 4),
            "delta_r": round(net_r_after - gross_r_before, 4),
            "one_line_lock": "AEE works if it can consistently harvest temporary favorable excursions, close decaying trades in profit, and still allow the minority of genuinely strong trades to extend.",
        },
        "by_mode": by_mode,
    }

    return proof


def main() -> None:
    out = build_real_tape_proof()
    (ROOT / "aee_real_tape_proof.json").write_text(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
