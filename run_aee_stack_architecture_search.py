#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from aee_replay_harness_adapter import _build_context, _safe_float, _stable_trade_id, replay_trade_path
from aee_stack_architecture import decide_stacked_architecture
from run_aee_kernel_combination_sweep import _load_trades

_MIN_ACTION_DWELL = 2
_FLOOR_BREACH_TOLERANCE_R = 0.05


def _stack_candidates() -> list[dict[str, Any]]:
    """Constrained architecture search space.

    Hard constraints:
    - T fixed as anchor.
    - Panic is always-on interrupt (outside search).
    - Only D and F are reorderable/searchable middle layers.
    """
    out: list[dict[str, Any]] = []

    out.append(
        {
            "stack_id": "T_only",
            "composition": ["T"],
            "layer_order": [],
            "activation_logic": {"D": None, "F": None},
            "intervention_type": {"D": None, "F": None},
            "permissions": {"D": "observe_only", "F": "observe_only"},
            "config": {},
        }
    )

    # T + D variants (subfamilies + permissions + action type)
    d_subfamilies = ["D_giveback", "D_stall", "D_decay", "D_failed_push"]
    d_actions = ["downgrade_hold_tighten", "force_tighten", "kernel_suggest"]
    d_permissions = ["downgrade_only", "tighten_only", "close_allowed"]

    for dsub in d_subfamilies:
        for da in d_actions:
            for dp in d_permissions:
                out.append(
                    {
                        "stack_id": f"T_D__{dsub}__{da}__{dp}",
                        "composition": ["T", "D"],
                        "layer_order": ["D"],
                        "activation_logic": {"D": dsub, "F": None},
                        "intervention_type": {"D": da, "F": None},
                        "permissions": {"D": dp, "F": "observe_only"},
                        "config": {
                            "deg_trigger_mode": dsub,
                            "deg_action_mode": da,
                            "deg_allow_weak": False,
                        },
                    }
                )

    # T + F variants
    f_triggers = ["breach_only", "breach_or_risk"]
    f_actions = ["tighten_or_close", "force_tighten"]
    f_permissions = ["tighten_only", "close_allowed"]

    for ft in f_triggers:
        for fa in f_actions:
            for fp in f_permissions:
                out.append(
                    {
                        "stack_id": f"T_F__{ft}__{fa}__{fp}",
                        "composition": ["T", "F"],
                        "layer_order": ["F"],
                        "activation_logic": {"D": None, "F": ft},
                        "intervention_type": {"D": None, "F": fa},
                        "permissions": {"D": "observe_only", "F": fp},
                        "config": {
                            "floor_trigger_mode": ft,
                            "floor_action_mode": fa,
                        },
                    }
                )

    # T + D + F with order as explicit search dimension
    orders = [["D", "F"], ["F", "D"]]
    for dsub in d_subfamilies:
        for da in ["downgrade_hold_tighten", "force_tighten", "kernel_suggest"]:
            for dp in ["downgrade_only", "tighten_only", "close_allowed"]:
                for ft in ["breach_only", "breach_or_risk"]:
                    for fa in ["tighten_or_close", "force_tighten"]:
                        for fp in ["tighten_only", "close_allowed"]:
                            for order in orders:
                                out.append(
                                    {
                                        "stack_id": (
                                            f"T_D_F__{dsub}__{da}__{dp}__"
                                            f"{ft}__{fa}__{fp}__{''.join(order)}"
                                        ),
                                        "composition": ["T", "D", "F"],
                                        "layer_order": order,
                                        "activation_logic": {"D": dsub, "F": ft},
                                        "intervention_type": {"D": da, "F": fa},
                                        "permissions": {"D": dp, "F": fp},
                                        "config": {
                                            "deg_trigger_mode": dsub,
                                            "deg_action_mode": da,
                                            "deg_allow_weak": False,
                                            "floor_trigger_mode": ft,
                                            "floor_action_mode": fa,
                                        },
                                    }
                                )

    return out


def _precompute_anchors(
    trades: list[dict[str, Any]],
) -> tuple[dict[str, float], dict[str, float], dict[str, float], dict[str, float], set[str], set[str]]:
    """Precompute current winner + pure_T anchors and runner/top trade sets."""
    winner_policy_path = Path("control/aee_runtime_policy_v1_winner.json")
    if winner_policy_path.exists():
        winner_payload = json.loads(winner_policy_path.read_text(encoding="utf-8"))
        winner_policy = winner_payload.get("policy") or {}
    else:
        winner_policy = {"enable_objective_v1": 1.0}
    winner_policy = {str(k): float(v) for k, v in winner_policy.items()}
    winner_policy.setdefault("enable_objective_v1", 1.0)

    from run_aee_kernel_combination_sweep import _replay_combo

    pure_t_combo = {
        "combo_id": "pure_T",
        "kernels": ["T"],
        "fusion": "weighted_sum",
        "weights": {"T": 1.0},
    }

    current_winner: dict[str, float] = {}
    pure_t: dict[str, float] = {}
    protective: dict[str, float] = {}
    pure_t_gb: dict[str, float] = {}

    for tr in trades:
        tid = _stable_trade_id(tr, list(tr.get("rows") or []))
        wres = replay_trade_path(tr, policy_overrides=winner_policy, policy_name="current_winner")
        current_winner[tid] = _safe_float(wres.get("final_money_result_pips", 0.0), 0.0)

        pure_t_res = _replay_combo(tr, pure_t_combo, current_winner[tid])
        pure_t[tid] = _safe_float(pure_t_res.get("result_pips", 0.0), 0.0)
        protective[tid] = _safe_float(pure_t_res.get("baseline_protective_pips", 0.0), 0.0)
        pure_t_gb[tid] = _safe_float(pure_t_res.get("max_giveback_r", 0.0), 0.0)

    # pure_T runner/non-interference audit set: path-end under pure_T
    pure_t_path_end_ids: set[str] = set()
    for tr in trades:
        tid = _stable_trade_id(tr, list(tr.get("rows") or []))
        final_path_pips = _safe_float((tr.get("rows") or [{}])[-1].get("profit_now", 0.0), 0.0)
        if abs(pure_t.get(tid, 0.0) - final_path_pips) < 1e-9:
            pure_t_path_end_ids.add(tid)

    # top pure_T winners for top-trade non-interference audit
    top_n = max(5, int(len(pure_t) * 0.05))
    top_pure_t_ids = {
        tid for tid, _ in sorted(pure_t.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
    }

    return current_winner, pure_t, protective, pure_t_gb, pure_t_path_end_ids, top_pure_t_ids


def _replay_stack_trade(
    trade: dict[str, Any],
    stack: dict[str, Any],
    current_winner_pips: float,
    pure_t_pips: float,
    protective_pips: float,
    pure_t_giveback_r: float,
) -> dict[str, Any]:
    rows = list(trade.get("rows") or [])
    if not rows:
        return {}

    target_distance = max(0.1, _safe_float(trade.get("target_distance", 1.0), 1.0))
    baseline_1to1 = _safe_float(trade.get("baseline_final_pips", 0.0), 0.0)

    peak_pips = -1e9
    bars_since_improvement = 0
    locked_floor_pips = 0.0
    last_action = "HOLD"
    action_dwell_bars = 0
    is_protected = False

    final_pips = _safe_float(rows[-1].get("profit_now", 0.0), 0.0)
    exit_bar = len(rows)
    max_giveback_r = 0.0
    regime_counts: dict[str, int] = defaultdict(int)

    layer_activation_bars: dict[str, int] = defaultdict(int)
    layer_affected_bars: dict[str, int] = defaultdict(int)
    affected_trade = False

    for idx, row in enumerate(rows, 1):
        pips = _safe_float(row.get("profit_now", 0.0), 0.0)

        if pips > peak_pips:
            peak_pips = pips
            bars_since_improvement = 0
        else:
            bars_since_improvement += 1

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

        decision = decide_stacked_architecture(ctx, stack)
        chosen = decision["action"]

        for step in decision.get("layer_trace", []):
            layer_activation_bars[step] += 1
        if chosen != decision.get("base_action"):
            affected_trade = True
            if decision.get("layer_trace"):
                layer_affected_bars[decision["layer_trace"][-1]] += 1

        if chosen != last_action:
            if action_dwell_bars < _MIN_ACTION_DWELL:
                chosen = last_action
                action_dwell_bars += 1
            else:
                action_dwell_bars = 1
        else:
            action_dwell_bars += 1
        last_action = chosen

        if locked_floor_pips > 0.0 and pips < locked_floor_pips - _FLOOR_BREACH_TOLERANCE_R * target_distance:
            chosen = "CLOSE"
            last_action = "CLOSE"

        giveback = max(0.0, peak_pips - pips) / max(0.1, target_distance)
        max_giveback_r = max(max_giveback_r, giveback)

        if pips >= target_distance * 0.60 and locked_floor_pips > 0.0:
            is_protected = True
        if is_protected:
            locked_floor_pips = max(locked_floor_pips, max(0.0, peak_pips * 0.40))

        if ctx.panic_trigger or ctx.giveback_from_peak_r >= 0.40:
            regime_counts["reversal"] += 1
        elif ctx.time_unproductive_ratio >= 0.40 or ctx.stall_score >= 0.50:
            regime_counts["stall"] += 1
        elif ctx.continuation_score >= 0.60 and abs(ctx.progress_r) >= 0.25:
            regime_counts["trend"] += 1
        else:
            regime_counts["neutral"] += 1

        if chosen == "CLOSE" or ctx.panic_trigger:
            final_pips = pips
            exit_bar = idx
            break

    dominant_regime = max(regime_counts, key=lambda k: regime_counts[k]) if regime_counts else "neutral"

    return {
        "result_pips": final_pips,
        "exit_bar": exit_bar,
        "total_bars": len(rows),
        "early_exit": exit_bar < len(rows),
        "baseline_1to1_pips": baseline_1to1,
        "baseline_protective_pips": protective_pips,
        "pure_t_pips": pure_t_pips,
        "current_winner_pips": current_winner_pips,
        "delta_vs_1to1": final_pips - baseline_1to1,
        "delta_vs_protective": final_pips - protective_pips,
        "delta_vs_pure_t": final_pips - pure_t_pips,
        "delta_vs_current": final_pips - current_winner_pips,
        "max_giveback_r": max_giveback_r,
        "pure_t_max_giveback_r": pure_t_giveback_r,
        "dominant_regime": dominant_regime,
        "regime_counts": dict(regime_counts),
        "layer_activation_bars": dict(layer_activation_bars),
        "layer_affected_bars": dict(layer_affected_bars),
        "affected_trade": affected_trade,
    }


def _mean(vals: list[float]) -> float:
    return (sum(vals) / len(vals)) if vals else 0.0


def _summarize_stack(rows: list[dict[str, Any]], pure_t_path_end_ids: set[str], top_pure_t_ids: set[str]) -> dict[str, Any]:
    n = len(rows)
    d1 = [x["delta_vs_1to1"] for x in rows]
    dp = [x["delta_vs_protective"] for x in rows]
    dt = [x["delta_vs_pure_t"] for x in rows]
    dc = [x["delta_vs_current"] for x in rows]

    reversal_rows = [x for x in rows if x["dominant_regime"] == "reversal"]
    rev_dt = [x["delta_vs_pure_t"] for x in reversal_rows]

    pure_t_runner_rows = [x for x in rows if x["trade_id"] in pure_t_path_end_ids]
    runner_preserved = sum(1 for x in pure_t_runner_rows if not x["early_exit"])
    runner_preservation_rate = (runner_preserved / len(pure_t_runner_rows)) if pure_t_runner_rows else 0.0
    early_close_damage = _mean([x["delta_vs_pure_t"] for x in pure_t_runner_rows]) if pure_t_runner_rows else 0.0

    top_trade_rows = [x for x in rows if x["trade_id"] in top_pure_t_ids]
    productive_interruptions = sum(1 for x in top_trade_rows if x["early_exit"] and x["delta_vs_pure_t"] < 0.0)

    giveback_reduction = _mean([x["pure_t_max_giveback_r"] - x["max_giveback_r"] for x in rows])

    act_bars: dict[str, int] = defaultdict(int)
    for r in rows:
        for k, v in (r.get("layer_activation_bars") or {}).items():
            act_bars[k] += int(v)
    total_bars = sum(int(r.get("total_bars", 0)) for r in rows)

    activated_rows = [x for x in rows if x.get("affected_trade")]
    non_activated_rows = [x for x in rows if not x.get("affected_trade")]
    activated_winners = [x for x in activated_rows if x["pure_t_pips"] > 0.0]
    activated_losers = [x for x in activated_rows if x["pure_t_pips"] <= 0.0]

    intervention_cost = {
        "activation_frequency_trades": round(len(activated_rows) / max(1, n), 4),
        "activation_frequency_bars": {k: round(v / max(1, total_bars), 4) for k, v in sorted(act_bars.items())},
        "activation_on_winners": len(activated_winners),
        "activation_on_losers": len(activated_losers),
        "net_delta_on_activated_trades_vs_pure_t": round(_mean([x["delta_vs_pure_t"] for x in activated_rows]), 4),
        "collateral_damage_on_non_target_winners": round(_mean([x["delta_vs_pure_t"] for x in activated_winners]), 4),
        "delta_on_non_activated_trades_vs_pure_t": round(_mean([x["delta_vs_pure_t"] for x in non_activated_rows]), 4),
    }

    # Dual evaluation: global + conditional contribution
    global_score = sum(dt)
    conditional_score = (
        2.0 * _mean(rev_dt)
        + 1.5 * runner_preservation_rate
        + 1.0 * giveback_reduction
        - 1.5 * max(0.0, -intervention_cost["collateral_damage_on_non_target_winners"])
    )

    return {
        "trade_count": n,
        "total_delta_vs_1to1": round(sum(d1), 4),
        "total_delta_vs_protective": round(sum(dp), 4),
        "total_delta_vs_pure_t": round(sum(dt), 4),
        "total_delta_vs_current": round(sum(dc), 4),
        "wins_vs_1to1": sum(d1) > 0.0,
        "wins_vs_protective": sum(dp) > 0.0,
        "wins_vs_pure_t": sum(dt) > 0.0,
        "wins_vs_current": sum(dc) > 0.0,
        "runner_preservation_rate": round(runner_preservation_rate, 4),
        "runner_trade_count": len(pure_t_runner_rows),
        "early_close_damage_vs_pure_t_runner": round(early_close_damage, 4),
        "reversal_improvement_vs_pure_t_avg": round(_mean(rev_dt), 4),
        "giveback_reduction_vs_pure_t_avg": round(giveback_reduction, 4),
        "productive_continuation_interrupted_count": productive_interruptions,
        "intervention_cost": intervention_cost,
        "global_score": round(global_score, 4),
        "conditional_contribution_score": round(conditional_score, 4),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Stack architecture search")
    ap.add_argument("--slice", default="control/aee_widened_replay_slice.json")
    ap.add_argument("--report-out", default="control/aee_stack_architecture_search_report.json")
    ap.add_argument("--max-trades", type=int, default=0)
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    trades = _load_trades(Path(args.slice))
    if args.max_trades > 0:
        trades = trades[: args.max_trades]

    current_winner, pure_t, protective, pure_t_gb, pure_t_path_end_ids, top_pure_t_ids = _precompute_anchors(trades)
    candidates = _stack_candidates()

    if not args.quiet:
        print(f"[stack-search] Running {len(candidates)} architecture candidates on {len(trades)} trades...")

    all_results: list[dict[str, Any]] = []
    for cand in candidates:
        sid = cand["stack_id"]
        rows: list[dict[str, Any]] = []
        for tr in trades:
            tid = _stable_trade_id(tr, list(tr.get("rows") or []))
            r = _replay_stack_trade(
                tr,
                cand,
                current_winner_pips=current_winner.get(tid, 0.0),
                pure_t_pips=pure_t.get(tid, 0.0),
                protective_pips=protective.get(tid, 0.0),
                pure_t_giveback_r=pure_t_gb.get(tid, 0.0),
            )
            r["trade_id"] = tid
            rows.append(r)

        summary = _summarize_stack(rows, pure_t_path_end_ids, top_pure_t_ids)
        summary["stack_id"] = sid
        summary["composition"] = list(cand.get("composition") or [])
        summary["activation_logic"] = dict(cand.get("activation_logic") or {})
        summary["intervention_type"] = dict(cand.get("intervention_type") or {})
        summary["permission_set"] = dict(cand.get("permissions") or {})
        summary["placement_order"] = list(cand.get("layer_order") or [])
        summary["config"] = dict(cand.get("config") or {})
        all_results.append(summary)

        if not args.quiet:
            print(
                f"  {sid:<56} ΔT={summary['total_delta_vs_pure_t']:+.2f} "
                f"runner={summary['runner_preservation_rate']:.2f} "
                f"revΔ={summary['reversal_improvement_vs_pure_t_avg']:+.3f}"
            )

    ranked = sorted(
        all_results,
        key=lambda x: (
            1 if x["wins_vs_pure_t"] else 0,
            x["global_score"],
            x["conditional_contribution_score"],
            x["total_delta_vs_1to1"],
        ),
        reverse=True,
    )

    report = {
        "search_type": "stack_architecture",
        "trade_count": len(trades),
        "candidate_count": len(candidates),
        "best": ranked[0] if ranked else {},
        "top10": ranked[:10],
        "all_candidates": ranked,
    }

    out = Path(args.report_out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")

    if not args.quiet and ranked:
        print("\n[stack-search] best candidate:")
        print(
            json.dumps(
                {
                    "stack_id": ranked[0]["stack_id"],
                    "total_delta_vs_1to1": ranked[0]["total_delta_vs_1to1"],
                    "total_delta_vs_pure_t": ranked[0]["total_delta_vs_pure_t"],
                    "total_delta_vs_current": ranked[0]["total_delta_vs_current"],
                    "runner_preservation_rate": ranked[0]["runner_preservation_rate"],
                    "reversal_improvement_vs_pure_t_avg": ranked[0]["reversal_improvement_vs_pure_t_avg"],
                    "intervention_cost": ranked[0]["intervention_cost"],
                    "global_score": ranked[0]["global_score"],
                    "conditional_contribution_score": ranked[0]["conditional_contribution_score"],
                },
                indent=2,
            )
        )

    print(f"[stack-search] report written: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
