#!/usr/bin/env python3
"""T-first asymmetric fusion sweep.

Directive-constrained family only:
- pure_T is anchor
- T is never replaced in productive continuation
- P/PnL gate in only under explicit degradation/non-productive conditions
- no naive weighted blends

Search objective:
1) Beat pure_T total delta vs 1:1
2) Improve toward 1:1 (reduce negative total)
3) Preserve runner non-interference (TP_HIT path-end behavior)
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from run_aee_kernel_combination_sweep import _load_trades, run_kernel_combination_sweep


def _family() -> list[dict[str, Any]]:
    """Generate T-first asymmetric family configurations.

    Each config enforces T ownership by default and only permits P/PnL intervention
    under weak/hard degradation gates.
    """
    combos: list[dict[str, Any]] = [
        {"combo_id": "pure_T_anchor", "kernels": ["T"], "fusion": "weighted_sum", "weights": {"T": 1.0}},
    ]

    base = {
        "veto_cp_min": 0.55,
        "veto_prod_min": 0.002,
        "veto_upr_max": 0.28,
        "veto_giveback_max": 0.22,
        "veto_pnl_min": 0.20,
        "hard_giveback_min": 0.42,
        "hard_cp_max": 0.22,
        "hard_prod_max": -0.001,
        "hard_ineff_min": 0.40,
        "weak_cp_max": 0.42,
        "weak_upr_min": 0.35,
        "weak_prod_max": 0.0,
        "weak_ineff_min": 0.22,
        "weak_stall_min": 0.45,
        "weak_w_t": 0.80,
        "weak_w_p": 0.15,
        "weak_w_q": 0.05,
        "hard_w_t": 0.30,
        "hard_w_p": 0.15,
        "hard_w_q": 0.55,
    }

    # Targeted grid around reversal-timing leak while preserving runner veto.
    # Stronger/looser hard degradation, and PnL close pressure under hard states.
    grid = [
        ("A1", 0.38, 0.24, 0.55, 0.80, 0.15, 0.05),
        ("A2", 0.40, 0.24, 0.60, 0.82, 0.13, 0.05),
        ("A3", 0.44, 0.22, 0.60, 0.80, 0.15, 0.05),
        ("A4", 0.46, 0.20, 0.65, 0.80, 0.15, 0.05),
        ("B1", 0.40, 0.20, 0.65, 0.75, 0.20, 0.05),
        ("B2", 0.42, 0.20, 0.70, 0.75, 0.20, 0.05),
        ("B3", 0.44, 0.18, 0.70, 0.70, 0.22, 0.08),
        ("B4", 0.46, 0.18, 0.72, 0.70, 0.22, 0.08),
        ("C1", 0.40, 0.26, 0.50, 0.85, 0.10, 0.05),
        ("C2", 0.42, 0.26, 0.50, 0.85, 0.10, 0.05),
        ("C3", 0.44, 0.26, 0.55, 0.85, 0.10, 0.05),
        ("C4", 0.46, 0.26, 0.55, 0.85, 0.10, 0.05),
    ]

    for tag, hard_gb, hard_cp, hard_q, weak_t, weak_p, weak_q in grid:
        cfg = dict(base)
        cfg.update(
            {
                "hard_giveback_min": hard_gb,
                "hard_cp_max": hard_cp,
                "hard_w_q": hard_q,
                "hard_w_t": max(0.15, 1.0 - hard_q - 0.15),
                "hard_w_p": 0.15,
                "weak_w_t": weak_t,
                "weak_w_p": weak_p,
                "weak_w_q": weak_q,
            }
        )
        combos.append(
            {
                "combo_id": f"tfirst_{tag}",
                "kernels": ["T", "P", "PnL"],
                "fusion": "tfirst_asymmetric",
                "fusion_config": cfg,
            }
        )

    # Pure T+PnL asymmetric variants (no P), for reversal repair with less interference.
    tpnl_grid = [
        ("Q1", 0.40, 0.24, 0.65, 0.82, 0.18),
        ("Q2", 0.42, 0.22, 0.70, 0.80, 0.20),
        ("Q3", 0.44, 0.20, 0.72, 0.78, 0.22),
        ("Q4", 0.46, 0.18, 0.75, 0.75, 0.25),
    ]
    for tag, hard_gb, hard_cp, hard_q, weak_t, weak_q in tpnl_grid:
        cfg = dict(base)
        cfg.update(
            {
                "hard_giveback_min": hard_gb,
                "hard_cp_max": hard_cp,
                "hard_w_q": hard_q,
                "hard_w_t": max(0.20, 1.0 - hard_q),
                "hard_w_p": 0.0,
                "weak_w_t": weak_t,
                "weak_w_p": 0.0,
                "weak_w_q": weak_q,
            }
        )
        combos.append(
            {
                "combo_id": f"tfirst_tpnl_{tag}",
                "kernels": ["T", "PnL"],
                "fusion": "tfirst_asymmetric",
                "fusion_config": cfg,
            }
        )

    # Ultra-strict T-preserving variants: weak gate almost disabled, hard-only repair.
    strict_grid = [
        ("S1", 0.35, 0.25, 0.75),
        ("S2", 0.38, 0.22, 0.78),
        ("S3", 0.40, 0.20, 0.82),
        ("S4", 0.42, 0.18, 0.85),
        ("S5", 0.45, 0.16, 0.88),
    ]
    for tag, hard_gb, hard_cp, hard_q in strict_grid:
        cfg = dict(base)
        cfg.update(
            {
                "hard_giveback_min": hard_gb,
                "hard_cp_max": hard_cp,
                "hard_w_q": hard_q,
                "hard_w_t": max(0.10, 1.0 - hard_q),
                "hard_w_p": 0.0,
                # effectively disable weak intervention so T keeps runner control
                "weak_cp_max": 0.08,
                "weak_upr_min": 0.92,
                "weak_prod_max": -0.02,
                "weak_ineff_min": 0.95,
                "weak_stall_min": 0.95,
                "weak_w_t": 1.0,
                "weak_w_p": 0.0,
                "weak_w_q": 0.0,
            }
        )
        combos.append(
            {
                "combo_id": f"tfirst_strict_{tag}",
                "kernels": ["T", "PnL"],
                "fusion": "tfirst_asymmetric",
                "fusion_config": cfg,
            }
        )

    return combos


def _assess(report: dict[str, Any]) -> dict[str, Any]:
    rows = report.get("all_combinations", [])
    anchor = next((r for r in rows if r.get("combo_id") == "pure_T_anchor"), None)
    if not anchor:
        raise RuntimeError("pure_T_anchor not present in sweep report")

    anchor_d1 = float(anchor.get("total_delta_vs_1to1", 0.0))

    candidates = [r for r in rows if r.get("combo_id") != "pure_T_anchor"]
    best = max(candidates, key=lambda r: float(r.get("total_delta_vs_1to1", -1e9))) if candidates else None

    if not best:
        return {
            "plateau": True,
            "reason": "no_candidates",
            "anchor_total_delta_vs_1to1": anchor_d1,
        }

    best_d1 = float(best.get("total_delta_vs_1to1", -1e9))
    improvement_vs_anchor = best_d1 - anchor_d1

    # Plateau condition for this family:
    # no config beats anchor, and best gain is <= +1.0 pip total.
    plateau = best_d1 <= anchor_d1 or improvement_vs_anchor <= 1.0

    return {
        "plateau": bool(plateau),
        "anchor_combo_id": "pure_T_anchor",
        "anchor_total_delta_vs_1to1": round(anchor_d1, 4),
        "best_combo_id": best.get("combo_id"),
        "best_total_delta_vs_1to1": round(best_d1, 4),
        "best_total_delta_vs_protective": round(float(best.get("total_delta_vs_protective", 0.0)), 4),
        "best_total_delta_vs_current": round(float(best.get("total_delta_vs_current", 0.0)), 4),
        "improvement_vs_pure_T_total_pips": round(improvement_vs_anchor, 4),
        "wins_all_three": bool(best.get("wins_all_three", False)),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="T-first asymmetric fusion sweep")
    ap.add_argument("--slice", default="control/aee_widened_replay_slice.json")
    ap.add_argument("--report-out", default="control/aee_tfirst_asymmetric_sweep_report.json")
    ap.add_argument("--max-trades", type=int, default=0)
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    trades = _load_trades(Path(args.slice))
    if args.max_trades > 0:
        trades = trades[: args.max_trades]

    combos = _family()
    report = run_kernel_combination_sweep(trades, combinations=combos, verbose=not args.quiet)
    report["family"] = "tfirst_asymmetric_only"
    report["assessment"] = _assess(report)

    out = Path(args.report_out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    ass = report["assessment"]
    print("\n[tfirst] assessment:")
    print(json.dumps(ass, indent=2))
    print(f"[tfirst] report written: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
