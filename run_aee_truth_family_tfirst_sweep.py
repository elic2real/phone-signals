#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from run_aee_kernel_combination_sweep import _load_trades, run_kernel_combination_sweep


def _family() -> list[dict[str, Any]]:
    combos: list[dict[str, Any]] = [
        {"combo_id": "pure_T_anchor", "kernels": ["T"], "fusion": "weighted_sum", "weights": {"T": 1.0}},
        # Pure truth-family probes first (required rule).
        {"combo_id": "pure_D", "kernels": ["D"], "fusion": "weighted_sum", "weights": {"D": 1.0}},
        {"combo_id": "pure_F", "kernels": ["F"], "fusion": "weighted_sum", "weights": {"F": 1.0}},
        {"combo_id": "pure_Pr", "kernels": ["Pr"], "fusion": "weighted_sum", "weights": {"Pr": 1.0}},
        {"combo_id": "pure_R", "kernels": ["R"], "fusion": "weighted_sum", "weights": {"R": 1.0}},
    ]

    # T-first only intervention families (no naive weighted blends).
    candidates = [
        ("D", [
            ("D1", 0.40, 0.22, 0.20, 0.80),
            ("D2", 0.36, 0.24, 0.18, 0.82),
            ("D3", 0.44, 0.20, 0.25, 0.75),
            ("D4", 0.46, 0.18, 0.30, 0.70),
        ]),
        ("F", [
            ("F1", 0.40, 0.22, 0.15, 0.85),
            ("F2", 0.44, 0.20, 0.20, 0.80),
            ("F3", 0.46, 0.18, 0.25, 0.75),
        ]),
        ("Pr", [
            ("Pr1", 0.38, 0.24, 0.18, 0.82),
            ("Pr2", 0.42, 0.22, 0.22, 0.78),
            ("Pr3", 0.46, 0.20, 0.28, 0.72),
        ]),
        ("R", [
            ("R1", 0.42, 0.22, 0.18, 0.82),
            ("R2", 0.46, 0.20, 0.22, 0.78),
            ("R3", 0.50, 0.18, 0.28, 0.72),
        ]),
    ]

    base = {
        "veto_cp_min": 0.55,
        "veto_prod_min": 0.002,
        "veto_upr_max": 0.28,
        "veto_giveback_max": 0.22,
        "veto_pnl_min": 0.20,
        "hard_prod_max": -0.001,
        "hard_ineff_min": 0.40,
        "weak_cp_max": 0.42,
        "weak_upr_min": 0.35,
        "weak_prod_max": 0.0,
        "weak_ineff_min": 0.22,
        "weak_stall_min": 0.45,
    }

    for kernel_id, grid in candidates:
        for tag, hard_gb, hard_cp, weak_i, hard_i in grid:
            cfg = dict(base)
            cfg.update(
                {
                    "hard_giveback_min": hard_gb,
                    "hard_cp_max": hard_cp,
                    "weak_w_t": max(0.65, 1.0 - weak_i),
                    "weak_w_intervention": weak_i,
                    "hard_w_t": max(0.15, 1.0 - hard_i),
                    "hard_w_intervention": hard_i,
                }
            )
            combos.append(
                {
                    "combo_id": f"tfirst_{tag}",
                    "kernels": ["T", kernel_id],
                    "fusion": "tfirst_asymmetric",
                    "fusion_config": cfg,
                }
            )

    # Minimal intervention controls: prove convergence boundary.
    for kernel_id in ["D", "F", "Pr", "R"]:
        combos.append(
            {
                "combo_id": f"tfirst_{kernel_id}_strict",
                "kernels": ["T", kernel_id],
                "fusion": "tfirst_asymmetric",
                "fusion_config": {
                    "veto_cp_min": 0.55,
                    "veto_prod_min": 0.002,
                    "veto_upr_max": 0.28,
                    "veto_giveback_max": 0.22,
                    "veto_pnl_min": 0.20,
                    "hard_giveback_min": 0.50,
                    "hard_cp_max": 0.15,
                    "hard_prod_max": -0.01,
                    "hard_ineff_min": 0.65,
                    "weak_cp_max": 0.05,
                    "weak_upr_min": 0.95,
                    "weak_prod_max": -0.02,
                    "weak_ineff_min": 0.90,
                    "weak_stall_min": 0.90,
                    "weak_w_t": 1.0,
                    "weak_w_intervention": 0.0,
                    "hard_w_t": 0.80,
                    "hard_w_intervention": 0.20,
                },
            }
        )

    return combos


def _assess(report: dict[str, Any]) -> dict[str, Any]:
    rows = report.get("all_combinations", [])
    anchor = next((r for r in rows if r.get("combo_id") == "pure_T_anchor"), None)
    if not anchor:
        raise RuntimeError("pure_T_anchor missing")
    anchor_d1 = float(anchor.get("total_delta_vs_1to1", 0.0))

    candidates = [r for r in rows if r.get("combo_id") != "pure_T_anchor"]
    best = max(candidates, key=lambda r: float(r.get("total_delta_vs_1to1", -1e9))) if candidates else None
    if not best:
        return {"plateau": True, "reason": "no_candidates", "anchor_total_delta_vs_1to1": anchor_d1}

    best_d1 = float(best.get("total_delta_vs_1to1", -1e9))
    return {
        "plateau": bool(best_d1 <= anchor_d1),
        "anchor_total_delta_vs_1to1": round(anchor_d1, 4),
        "best_combo_id": best.get("combo_id"),
        "best_total_delta_vs_1to1": round(best_d1, 4),
        "best_total_delta_vs_protective": round(float(best.get("total_delta_vs_protective", 0.0)), 4),
        "best_total_delta_vs_current": round(float(best.get("total_delta_vs_current", 0.0)), 4),
        "improvement_vs_pure_T_total_pips": round(best_d1 - anchor_d1, 4),
        "wins_all_three": bool(best.get("wins_all_three", False)),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Truth-family T-first sweep")
    ap.add_argument("--slice", default="control/aee_widened_replay_slice.json")
    ap.add_argument("--report-out", default="control/aee_truth_family_tfirst_report.json")
    ap.add_argument("--max-trades", type=int, default=0)
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    trades = _load_trades(Path(args.slice))
    if args.max_trades > 0:
        trades = trades[: args.max_trades]

    report = run_kernel_combination_sweep(trades, combinations=_family(), verbose=not args.quiet)
    report["family"] = "truth_family_tfirst"
    report["assessment"] = _assess(report)

    out = Path(args.report_out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("\n[truth-family] assessment:")
    print(json.dumps(report["assessment"], indent=2))
    print(f"[truth-family] report written: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
