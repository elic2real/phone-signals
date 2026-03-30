#!/usr/bin/env python3
"""Kernel Combination Sweep — tests all kernel pair/triple combinations against each other
and against the 1:1, protective, and current-winner baselines.

This is NOT a parameter sweep.  It tests whether the real edge lives in
multi-kernel interaction, not single-kernel tuning.

Combination matrix:
  Pure kernels    : P, T, PnL
  Pairs (wsum)    : P+T (equal/P-heavy/T-heavy),
                    P+PnL (equal/P-heavy/PnL-heavy),
                    T+PnL (equal/T-heavy/PnL-heavy)
  Triple (wsum)   : P+T+PnL (equal/P-heavy/T-heavy/PnL-heavy)
  Gated pairs     : P+T, P+PnL (gated by regime)
  Gated triple    : P+T+PnL (gated by regime)
  Confidence-wtd  : P+T, P+PnL, T+PnL, P+T+PnL

Attribution contract:
  Every result row carries per-kernel weight + dominant kernel at exit step.

Usage:
  python run_aee_kernel_combination_sweep.py [--slice control/aee_widened_replay_slice.json]
                                              [--report-out control/aee_kernel_combination_sweep_report.json]
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

from aee_replay_harness_adapter import (
    _build_context,
    _run_simple_protective_baseline,
    _safe_float,
    _safe_int,
    _stable_trade_id,
    replay_trade_path,
)
from aee_kernel_combination import score_kernels_and_fuse

# ─────────────────────────────────────────────────────────────────────────────
# Combination definitions
# ─────────────────────────────────────────────────────────────────────────────

COMBINATIONS: list[dict[str, Any]] = [
    # ── Pure kernels ──────────────────────────────────────────────────────
    {"combo_id": "pure_P",   "kernels": ["P"],         "fusion": "weighted_sum", "weights": {"P": 1.0}},
    {"combo_id": "pure_T",   "kernels": ["T"],         "fusion": "weighted_sum", "weights": {"T": 1.0}},
    {"combo_id": "pure_PnL", "kernels": ["PnL"],       "fusion": "weighted_sum", "weights": {"PnL": 1.0}},

    # ── P + T pairs ───────────────────────────────────────────────────────
    {"combo_id": "PT_equal",   "kernels": ["P", "T"], "fusion": "weighted_sum", "weights": {"P": 1.0, "T": 1.0}},
    {"combo_id": "PT_P_heavy", "kernels": ["P", "T"], "fusion": "weighted_sum", "weights": {"P": 2.0, "T": 1.0}},
    {"combo_id": "PT_T_heavy", "kernels": ["P", "T"], "fusion": "weighted_sum", "weights": {"P": 1.0, "T": 2.0}},

    # ── P + PnL pairs ─────────────────────────────────────────────────────
    {"combo_id": "PPnL_equal",     "kernels": ["P", "PnL"], "fusion": "weighted_sum", "weights": {"P": 1.0, "PnL": 1.0}},
    {"combo_id": "PPnL_P_heavy",   "kernels": ["P", "PnL"], "fusion": "weighted_sum", "weights": {"P": 2.0, "PnL": 1.0}},
    {"combo_id": "PPnL_PnL_heavy", "kernels": ["P", "PnL"], "fusion": "weighted_sum", "weights": {"P": 1.0, "PnL": 2.0}},

    # ── T + PnL pairs ─────────────────────────────────────────────────────
    {"combo_id": "TPnL_equal",     "kernels": ["T", "PnL"], "fusion": "weighted_sum", "weights": {"T": 1.0, "PnL": 1.0}},
    {"combo_id": "TPnL_T_heavy",   "kernels": ["T", "PnL"], "fusion": "weighted_sum", "weights": {"T": 2.0, "PnL": 1.0}},
    {"combo_id": "TPnL_PnL_heavy", "kernels": ["T", "PnL"], "fusion": "weighted_sum", "weights": {"T": 1.0, "PnL": 2.0}},

    # ── P + T + PnL triple ───────────────────────────────────────────────
    {"combo_id": "PTQ_equal",    "kernels": ["P", "T", "PnL"], "fusion": "weighted_sum", "weights": {"P": 1.0, "T": 1.0, "PnL": 1.0}},
    {"combo_id": "PTQ_P_heavy",  "kernels": ["P", "T", "PnL"], "fusion": "weighted_sum", "weights": {"P": 2.0, "T": 1.0, "PnL": 1.0}},
    {"combo_id": "PTQ_T_heavy",  "kernels": ["P", "T", "PnL"], "fusion": "weighted_sum", "weights": {"P": 1.0, "T": 2.0, "PnL": 1.0}},
    {"combo_id": "PTQ_Q_heavy",  "kernels": ["P", "T", "PnL"], "fusion": "weighted_sum", "weights": {"P": 1.0, "T": 1.0, "PnL": 2.0}},

    # ── Gated (regime-selected) ───────────────────────────────────────────
    {"combo_id": "PT_gated",   "kernels": ["P", "T"],         "fusion": "gated"},
    {"combo_id": "PPnL_gated", "kernels": ["P", "PnL"],       "fusion": "gated"},
    {"combo_id": "PTQ_gated",  "kernels": ["P", "T", "PnL"],  "fusion": "gated"},

    # ── Confidence-weighted ───────────────────────────────────────────────
    {"combo_id": "PT_conf",   "kernels": ["P", "T"],         "fusion": "confidence_weighted"},
    {"combo_id": "PPnL_conf", "kernels": ["P", "PnL"],       "fusion": "confidence_weighted"},
    {"combo_id": "TPnL_conf", "kernels": ["T", "PnL"],       "fusion": "confidence_weighted"},
    {"combo_id": "PTQ_conf",  "kernels": ["P", "T", "PnL"],  "fusion": "confidence_weighted"},
]

# ─────────────────────────────────────────────────────────────────────────────
# Per-combo trade replay
# ─────────────────────────────────────────────────────────────────────────────

_MIN_ACTION_DWELL = 2
_ACTION_SWITCH_GAP = 0.20
_FLOOR_BREACH_TOLERANCE_R = 0.05


def _replay_combo(
    trade: dict[str, Any],
    combo: dict[str, Any],
    current_winner_pips: float,
) -> dict[str, Any]:
    """Run one combination policy against a single trade path.

    Returns a result dict comparable to widened-validation rows.
    """
    rows = list(trade.get("rows") or [])
    if not rows:
        return {}

    kernels: list[str] = combo["kernels"]
    fusion: str = combo["fusion"]
    weights: dict[str, float] | None = combo.get("weights")
    fusion_config: dict[str, float] | None = combo.get("fusion_config")

    target_distance = max(0.1, _safe_float(trade.get("target_distance", 1.0), 1.0))
    baseline_1to1 = _safe_float(trade.get("baseline_final_pips", 0.0), 0.0)
    protective = _run_simple_protective_baseline(rows, target_distance)
    baseline_protective = _safe_float(protective.get("final_money_result_pips", 0.0), 0.0)

    # Replay state
    peak_pips: float = -1e9
    bars_since_improvement: int = 0
    locked_floor_pips: float = 0.0
    last_action: str = "HOLD"
    action_dwell_bars: int = 0
    is_protected: bool = False   # simplified state flag for floor lock

    final_pips: float = _safe_float(rows[-1].get("profit_now", 0.0), 0.0)
    exit_bar: int = len(rows)
    exit_attribution: dict[str, float] = {}
    exit_regime: str = "path_end"
    max_giveback_r: float = 0.0
    regime_counts: dict[str, int] = defaultdict(int)

    for idx, row in enumerate(rows, 1):
        pips = _safe_float(row.get("profit_now", 0.0), 0.0)

        if pips > peak_pips:
            peak_pips = pips
            bars_since_improvement = 0
        else:
            bars_since_improvement += 1

        # Target-lock floor (same as replay_trade_path)
        if pips >= target_distance:
            locked_floor_pips = max(locked_floor_pips, target_distance)
        if is_protected:
            locked_floor_pips = max(locked_floor_pips, max(0.0, peak_pips * 0.40))

        ctx = _build_context(
            row,
            idx=idx,
            total_rows=len(rows),
            target_distance=target_distance,
            peak_pips=peak_pips,
            locked_floor_pips=locked_floor_pips,
            bars_since_improvement=bars_since_improvement,
            objective_state="MAXIMIZE_CONTINUATION",
            objective_dwell_bars=0,
            objective_confirm_count=0,
            objective_pending_target="",
            action_dwell_bars=action_dwell_bars,
            last_action=last_action,
        )

        result = score_kernels_and_fuse(
            ctx,
            kernels,
            fusion,
            weights,
            fusion_config=fusion_config,
        )
        regime = result["regime"]
        regime_counts[regime] += 1

        # Anti-thrash guard (same thresholds as v1 engine)
        chosen = result["best_action"]
        gap = result["confidence_gap"]
        if chosen != last_action:
            if action_dwell_bars < _MIN_ACTION_DWELL or gap < _ACTION_SWITCH_GAP:
                chosen = last_action
                action_dwell_bars += 1
            else:
                action_dwell_bars = 1
        else:
            action_dwell_bars += 1
        last_action = chosen

        # Floor-breach hard override (same rule as _compute_action_values_v1)
        if (locked_floor_pips > 0.0 and
                pips < locked_floor_pips - _FLOOR_BREACH_TOLERANCE_R * target_distance):
            chosen = "CLOSE"
            last_action = "CLOSE"

        giveback = max(0.0, peak_pips - pips) / max(0.1, target_distance)
        max_giveback_r = max(max_giveback_r, giveback)

        # Promote to protected state once sufficiently in profit
        if pips >= target_distance * 0.60 and locked_floor_pips > 0.0:
            is_protected = True
        # Update floor for protected state
        if is_protected:
            locked_floor_pips = max(locked_floor_pips, max(0.0, peak_pips * 0.40))

        if chosen == "CLOSE" or ctx.panic_trigger:
            final_pips = pips
            exit_bar = idx
            exit_attribution = dict(result["attribution"])
            exit_attribution["dominant_kernel"] = max(result["attribution"], key=result["attribution"].get)
            exit_attribution["panic"] = ctx.panic_trigger
            exit_regime = regime
            break

    dominant_regime = max(regime_counts, key=lambda k: regime_counts[k]) if regime_counts else "neutral"

    delta_1to1 = final_pips - baseline_1to1
    delta_protective = final_pips - baseline_protective
    delta_current = final_pips - current_winner_pips

    return {
        "combo_id": combo["combo_id"],
        "kernels": kernels,
        "fusion": fusion,
        "result_pips": final_pips,
        "exit_bar": exit_bar,
        "total_bars": len(rows),
        "early_exit": exit_bar < len(rows),
        "baseline_1to1_pips": baseline_1to1,
        "baseline_protective_pips": baseline_protective,
        "current_winner_pips": current_winner_pips,
        "delta_vs_1to1": delta_1to1,
        "delta_vs_protective": delta_protective,
        "delta_vs_current": delta_current,
        "beats_1to1": delta_1to1 > 0.0,
        "beats_protective": delta_protective > 0.0,
        "beats_current": delta_current > 0.0,
        "max_giveback_r": max_giveback_r,
        "locked_profit_pips": locked_floor_pips,
        "exit_regime": exit_regime,
        "dominant_regime": dominant_regime,
        "regime_counts": dict(regime_counts),
        "exit_attribution": exit_attribution,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Per-combo aggregation
# ─────────────────────────────────────────────────────────────────────────────

def _avg(values: list[float]) -> float:
    return (sum(values) / len(values)) if values else 0.0


def _aggregate_combo_results(trade_results: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate per-trade combo results into summary statistics."""
    n = len(trade_results)
    if n == 0:
        return {"trade_count": 0}

    d1 = [r["delta_vs_1to1"] for r in trade_results]
    dp = [r["delta_vs_protective"] for r in trade_results]
    dc = [r["delta_vs_current"] for r in trade_results]

    beats_1to1_count = sum(1 for r in trade_results if r["beats_1to1"])
    beats_prot_count = sum(1 for r in trade_results if r["beats_protective"])
    beats_curr_count = sum(1 for r in trade_results if r["beats_current"])

    # Per-regime breakdown
    regime_buckets: dict[str, list[dict]] = defaultdict(list)
    for r in trade_results:
        regime_buckets[r["exit_regime"]].append(r)

    regime_breakdown: dict[str, Any] = {}
    for regime, trades in sorted(regime_buckets.items()):
        regime_breakdown[regime] = {
            "count": len(trades),
            "total_delta_vs_1to1": sum(t["delta_vs_1to1"] for t in trades),
            "avg_delta_vs_1to1": _avg([t["delta_vs_1to1"] for t in trades]),
            "total_delta_vs_current": sum(t["delta_vs_current"] for t in trades),
            "avg_delta_vs_current": _avg([t["delta_vs_current"] for t in trades]),
        }

    # Per-dominant-kernel breakdown (only for multi-kernel combos)
    kernel_buckets: dict[str, list[dict]] = defaultdict(list)
    for r in trade_results:
        ea = r.get("exit_attribution") or {}
        dk = str(ea.get("dominant_kernel", "unknown"))
        kernel_buckets[dk].append(r)

    kernel_contribution: dict[str, Any] = {}
    for kid, trades in sorted(kernel_buckets.items()):
        kernel_contribution[kid] = {
            "count": len(trades),
            "pct_of_exits": round(len(trades) / n * 100, 1),
            "avg_delta_vs_1to1": _avg([t["delta_vs_1to1"] for t in trades]),
            "avg_delta_vs_current": _avg([t["delta_vs_current"] for t in trades]),
        }

    # Dominant regime across all trade paths (by majority of bars)
    reg_totals: dict[str, int] = defaultdict(int)
    for r in trade_results:
        for reg, cnt in (r.get("regime_counts") or {}).items():
            reg_totals[reg] += cnt
    top_regime = max(reg_totals, key=lambda k: reg_totals[k]) if reg_totals else "neutral"

    return {
        "trade_count": n,
        "total_delta_vs_1to1": round(sum(d1), 4),
        "avg_delta_vs_1to1": round(_avg(d1), 4),
        "total_delta_vs_protective": round(sum(dp), 4),
        "avg_delta_vs_protective": round(_avg(dp), 4),
        "total_delta_vs_current": round(sum(dc), 4),
        "avg_delta_vs_current": round(_avg(dc), 4),
        "beats_1to1": sum(d1) > 0.0,
        "beats_protective": sum(dp) > 0.0,
        "beats_current": sum(dc) > 0.0,
        "beats_1to1_trade_count": beats_1to1_count,
        "beats_protective_trade_count": beats_prot_count,
        "beats_current_trade_count": beats_curr_count,
        "wins_all_three": sum(d1) > 0.0 and sum(dp) > 0.0 and sum(dc) > 0.0,
        "score": round(sum(d1) + sum(dp) + sum(dc), 4),
        "early_exit_count": sum(1 for r in trade_results if r["early_exit"]),
        "early_exit_rate": round(sum(1 for r in trade_results if r["early_exit"]) / n, 4),
        "avg_max_giveback_r": round(_avg([r["max_giveback_r"] for r in trade_results]), 4),
        "regime_breakdown": regime_breakdown,
        "kernel_contribution": kernel_contribution,
        "dominant_regime_overall": top_regime,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main sweep
# ─────────────────────────────────────────────────────────────────────────────

def run_kernel_combination_sweep(
    trades: list[dict[str, Any]],
    combinations: list[dict[str, Any]] | None = None,
    verbose: bool = True,
) -> dict[str, Any]:
    """Run the full combination sweep and return structured report."""
    if combinations is None:
        combinations = COMBINATIONS

    # Pre-compute current winner result for every trade (one pass).
    # The winner policy is already frozen in control/aee_runtime_policy_v1_winner.json.
    winner_policy_path = Path("control/aee_runtime_policy_v1_winner.json")
    if winner_policy_path.exists():
        winner_payload = json.loads(winner_policy_path.read_text(encoding="utf-8"))
        winner_policy = winner_payload.get("policy") or {}
    else:
        winner_policy = {"enable_objective_v1": 1.0}

    winner_policy = {str(k): float(v) for k, v in winner_policy.items()}
    winner_policy.setdefault("enable_objective_v1", 1.0)

    if verbose:
        print(f"[sweep] Pre-computing current winner on {len(trades)} trades...")
    winner_pips_by_id: dict[str, float] = {}
    for tr in trades:
        tid = _stable_trade_id(tr, list(tr.get("rows") or []))
        wres = replay_trade_path(tr, policy_overrides=winner_policy, policy_name="current_winner")
        winner_pips_by_id[tid] = _safe_float(wres.get("final_money_result_pips", 0.0), 0.0)

    if verbose:
        print(f"[sweep] Running {len(combinations)} combinations × {len(trades)} trades...")

    all_combo_summaries: list[dict[str, Any]] = []
    per_combo_trades: dict[str, list[dict[str, Any]]] = {}

    for combo in combinations:
        cid = combo["combo_id"]
        trade_results: list[dict[str, Any]] = []

        for tr in trades:
            rows = list(tr.get("rows") or [])
            tid = _stable_trade_id(tr, rows)
            current_winner_pips = winner_pips_by_id.get(tid, 0.0)

            res = _replay_combo(tr, combo, current_winner_pips)
            if res:
                res["trade_id"] = tid
                res["meta"] = dict(tr.get("meta") or {})
                trade_results.append(res)

        summary = _aggregate_combo_results(trade_results)
        summary["combo_id"] = cid
        summary["kernels"] = combo["kernels"]
        summary["fusion"] = combo["fusion"]
        if combo.get("weights"):
            summary["weights"] = combo["weights"]
        if combo.get("fusion_config"):
            summary["fusion_config"] = combo["fusion_config"]

        all_combo_summaries.append(summary)
        per_combo_trades[cid] = trade_results

        if verbose:
            sign_1to1 = "+" if summary["total_delta_vs_1to1"] >= 0 else ""
            sign_cur  = "+" if summary["total_delta_vs_current"] >= 0 else ""
            beat_mark = "★" if summary["wins_all_three"] else ("~" if summary["beats_current"] else " ")
            print(
                f"  {beat_mark} {cid:<25}  vs_1to1={sign_1to1}{summary['total_delta_vs_1to1']:+.2f}  "
                f"vs_prot={summary['total_delta_vs_protective']:+.2f}  "
                f"vs_cur={sign_cur}{summary['total_delta_vs_current']:+.2f}  "
                f"({summary['fusion']}, {'+'.join(combo['kernels'])})"
            )

    # Rank all combos: primary = beats_all_three; secondary = total_delta_1to1 + protective + current
    ranked = sorted(
        all_combo_summaries,
        key=lambda s: (
            1 if s["wins_all_three"] else 0,
            1 if s["beats_1to1"] else 0,
            1 if s["beats_protective"] else 0,
            s["score"],
        ),
        reverse=True,
    )

    # Per-regime sweep summary: for each regime, which combo dominates?
    regime_leaders: dict[str, dict[str, Any]] = {}
    for regime in ("trend", "stall", "reversal", "neutral"):
        best_for_regime: dict[str, Any] | None = None
        best_val = -1e9
        for s in all_combo_summaries:
            rb = s.get("regime_breakdown", {})
            val = rb.get(regime, {}).get("avg_delta_vs_1to1", -1e9)
            if val > best_val:
                best_val = val
                best_for_regime = {"combo_id": s["combo_id"], "avg_delta_vs_1to1": round(val, 4)}
        if best_for_regime:
            regime_leaders[regime] = best_for_regime

    # Kernel contribution summary across all combos
    kernel_effect: dict[str, dict[str, list[float]]] = {"P": {"delta_1to1": [], "delta_cur": []},
                                                          "T": {"delta_1to1": [], "delta_cur": []},
                                                          "PnL": {"delta_1to1": [], "delta_cur": []}}
    for cid, trade_results in per_combo_trades.items():
        for tr_res in trade_results:
            ea = tr_res.get("exit_attribution") or {}
            dk = str(ea.get("dominant_kernel", ""))
            if dk in kernel_effect:
                kernel_effect[dk]["delta_1to1"].append(tr_res["delta_vs_1to1"])
                kernel_effect[dk]["delta_cur"].append(tr_res["delta_vs_current"])

    kernel_summary: dict[str, Any] = {}
    for kid, vals in kernel_effect.items():
        if vals["delta_1to1"]:
            kernel_summary[kid] = {
                "exit_count": len(vals["delta_1to1"]),
                "avg_delta_vs_1to1_when_dominant": round(_avg(vals["delta_1to1"]), 4),
                "avg_delta_vs_current_when_dominant": round(_avg(vals["delta_cur"]), 4),
            }

    top3 = ranked[:3]
    winners = [s for s in ranked if s["wins_all_three"]]

    if verbose:
        print(f"\n[sweep] Winners (beat all 3 baselines): {len(winners)}")
        for w in winners:
            print(f"  ★ {w['combo_id']}  score={w['score']:+.2f}"
                  f"  vs_1to1={w['total_delta_vs_1to1']:+.2f}"
                  f"  vs_prot={w['total_delta_vs_protective']:+.2f}"
                  f"  vs_cur={w['total_delta_vs_current']:+.2f}")

        print(f"\n[sweep] Top 3 (by composite score):")
        for s in top3:
            print(f"  {s['combo_id']}  score={s['score']:+.2f}  fusion={s['fusion']}")

        print(f"\n[sweep] Regime leaders (avg Δ vs 1:1):")
        for regime, leader in regime_leaders.items():
            print(f"  {regime:<10}: {leader['combo_id']}  avg={leader['avg_delta_vs_1to1']:+.4f}")

        print(f"\n[sweep] Kernel dominance summary:")
        for kid, stats in kernel_summary.items():
            print(f"  Kernel {kid}: dominated {stats['exit_count']} exits,  "
                  f"avg vs 1:1 = {stats['avg_delta_vs_1to1_when_dominant']:+.4f},  "
                  f"avg vs current = {stats['avg_delta_vs_current_when_dominant']:+.4f}")

    return {
        "sweep_type": "kernel_combination",
        "trade_count": len(trades),
        "combination_count": len(combinations),
        "winner_count": len(winners),
        "all_combinations": ranked,
        "winners": winners,
        "top3": top3,
        "regime_leaders": regime_leaders,
        "kernel_dominance_summary": kernel_summary,
        "per_combo_trades": {cid: per_combo_trades[cid] for cid in list(per_combo_trades)[:5]},
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

def _load_trades(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "trades" in payload:
        return list(payload["trades"])
    if isinstance(payload, list):
        return payload
    raise ValueError("Input must be a list or object with 'trades'.")


def main() -> int:
    ap = argparse.ArgumentParser(description="AEE Kernel Combination Sweep")
    ap.add_argument("--slice", default="control/aee_widened_replay_slice.json",
                    help="Replay trade slice (default: widened 300-trade slice).")
    ap.add_argument("--report-out", default="control/aee_kernel_combination_sweep_report.json",
                    help="Output report path.")
    ap.add_argument("--max-trades", type=int, default=0,
                    help="Limit trades (0 = all).")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    slice_path = Path(args.slice)
    if not slice_path.exists():
        print(f"ERROR: slice not found: {slice_path}", file=sys.stderr)
        return 1

    trades = _load_trades(slice_path)
    if args.max_trades > 0:
        trades = trades[: args.max_trades]

    report = run_kernel_combination_sweep(trades, verbose=not args.quiet)

    out_path = Path(args.report_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"\n[sweep] Report written to {out_path}  ({len(trades)} trades, {len(COMBINATIONS)} combos)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
