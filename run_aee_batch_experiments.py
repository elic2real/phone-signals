#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from aee_replay_harness_adapter import replay_trade_path
from run_aee_scenario_layering import classify_scenario


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _load_trades(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "trades" in payload:
        return list(payload.get("trades") or [])
    if isinstance(payload, list):
        return payload
    raise ValueError("Input must be list or object with 'trades'.")


def _merge_policy(*parts: dict[str, float]) -> dict[str, float]:
    out: dict[str, float] = {}
    for part in parts:
        if not part:
            continue
        out.update({str(k): float(v) for k, v in part.items()})
    return out


def _aggregate_breakdown(rows: list[dict[str, Any]], key: str) -> dict[str, dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[str(row.get(key, "UNKNOWN"))].append(row)

    out: dict[str, dict[str, Any]] = {}
    for name, b in sorted(buckets.items()):
        deltas = [_safe_float(x.get("delta_vs_baseline_pips", 0.0), 0.0) for x in b]
        out[name] = {
            "count": len(b),
            "total_delta_vs_baseline_pips": sum(deltas),
            "avg_delta_vs_baseline_pips": (sum(deltas) / len(deltas)) if deltas else 0.0,
        }
    return out


def _aggregate_breakdown_for_metric(rows: list[dict[str, Any]], key: str, metric_key: str) -> dict[str, dict[str, Any]]:
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[str(row.get(key, "UNKNOWN"))].append(row)

    out: dict[str, dict[str, Any]] = {}
    for name, b in sorted(buckets.items()):
        vals = [_safe_float(x.get(metric_key, 0.0), 0.0) for x in b]
        out[name] = {
            "count": len(b),
            f"total_{metric_key}": sum(vals),
            f"avg_{metric_key}": (sum(vals) / len(vals)) if vals else 0.0,
        }
    return out


def _default_config() -> dict[str, Any]:
    # All v1 kernels run the objective state machine (enable_objective_v1=1.0).
    # The legacy kernel_legacy_thresholds is the 1:1 comparative without v1 engine.
    return {
        "kernel_candidates": [
            # ── Legacy / baseline (no v1 objective engine — comparative anchor) ──
            {
                "kernel_id": "kernel_legacy_thresholds",
                "kernel_type": "pure",
                "components": [
                    {
                        "component_id": "LEGACY_THRESHOLD_MACHINE",
                        "definition": "Original threshold-based transitions without objective/action-value layer.",
                    }
                ],
                "policy": {"enable_objective_v1": 0.0},
            },
            # ── v1 objective engine — default coefficients ──
            {
                "kernel_id": "kernel_v1_defaults",
                "kernel_type": "pure",
                "components": [
                    {
                        "component_id": "OBJECTIVE_STATE_MACHINE_V1",
                        "definition": "Dynamic objective state machine with R-domain action-value selection at default parameters.",
                    }
                ],
                "policy": {"enable_objective_v1": 1.0},
            },
            # ── v1 + tighter release trigger (exit sooner when risk rises) ──
            {
                "kernel_id": "kernel_v1_early_release",
                "kernel_type": "pure",
                "components": [
                    {
                        "component_id": "OBJECTIVE_STATE_MACHINE_V1",
                        "definition": "v1 engine with lower giveback threshold before switching to RELEASE_CAPITAL.",
                    }
                ],
                "policy": {
                    "enable_objective_v1": 1.0,
                    "release_giveback_trigger_r": 0.65,
                    "floor_giveback_trigger_r": 0.22,
                },
            },
            # ── v1 + delayed release (let winners breathe longer) ──
            {
                "kernel_id": "kernel_v1_late_release",
                "kernel_type": "pure",
                "components": [
                    {
                        "component_id": "OBJECTIVE_STATE_MACHINE_V1",
                        "definition": "v1 engine with higher giveback tolerance before RELEASE_CAPITAL.",
                    }
                ],
                "policy": {
                    "enable_objective_v1": 1.0,
                    "release_giveback_trigger_r": 1.00,
                    "floor_giveback_trigger_r": 0.45,
                },
            },
            # ── v1 + stronger continuation signal requirement ──
            {
                "kernel_id": "kernel_v1_strong_continuation",
                "kernel_type": "pure",
                "components": [
                    {
                        "component_id": "OBJECTIVE_STATE_MACHINE_V1",
                        "definition": "v1 engine requiring higher continuation proxy to stay in MAXIMIZE_CONTINUATION.",
                    }
                ],
                "policy": {
                    "enable_objective_v1": 1.0,
                    "continuation_proxy_enter_r": 0.75,
                    "release_continuation_max_r": 0.15,
                },
            },
            # ── v1 + high inefficiency sensitivity ──
            {
                "kernel_id": "kernel_v1_ineff_sensitive",
                "kernel_type": "pure",
                "components": [
                    {
                        "component_id": "INEFFICIENCY_COST_WEIGHT",
                        "definition": "v1 engine with elevated inefficiency cost weighting — exits stalled trades sooner.",
                    }
                ],
                "policy": {
                    "enable_objective_v1": 1.0,
                    "inefficiency_weight": 2.5,
                    "release_inefficiency_min_r": 0.30,
                },
            },
            # ── v1 + tight anti-thrash (less action switching) ──
            {
                "kernel_id": "kernel_v1_tight_antithrash",
                "kernel_type": "pure",
                "components": [
                    {
                        "component_id": "ANTI_THRASH_TIGHT",
                        "definition": "v1 engine with increased minimum action dwell and higher confidence gap requirement.",
                    }
                ],
                "policy": {
                    "enable_objective_v1": 1.0,
                    "min_action_dwell": 3,
                    "action_switch_confidence_gap": 0.35,
                    "objective_min_dwell": 3,
                    "objective_confirm_bars": 3,
                },
            },
            # ── v1 composite: early release + inefficiency cost ──
            {
                "kernel_id": "kernel_v1_composite_release_ineff",
                "kernel_type": "composite",
                "components": [
                    {
                        "component_id": "OBJECTIVE_STATE_MACHINE_V1",
                        "definition": "v1 engine with lower release trigger.",
                    },
                    {
                        "component_id": "INEFFICIENCY_COST_WEIGHT",
                        "definition": "Elevated inefficiency weighting to catch unproductive capital.",
                    },
                ],
                "policy": {
                    "enable_objective_v1": 1.0,
                    "release_giveback_trigger_r": 0.70,
                    "inefficiency_weight": 2.0,
                    "release_inefficiency_min_r": 0.35,
                    "floor_giveback_trigger_r": 0.25,
                },
            },
            # ── v1 composite: strong continuation + anti-thrash ──
            {
                "kernel_id": "kernel_v1_composite_cont_antithrash",
                "kernel_type": "composite",
                "components": [
                    {
                        "component_id": "OBJECTIVE_STATE_MACHINE_V1",
                        "definition": "v1 engine with high continuation threshold.",
                    },
                    {
                        "component_id": "ANTI_THRASH_TIGHT",
                        "definition": "Higher dwell and confidence requirements to avoid thrashing.",
                    },
                ],
                "policy": {
                    "enable_objective_v1": 1.0,
                    "continuation_proxy_enter_r": 0.70,
                    "release_continuation_max_r": 0.12,
                    "min_action_dwell": 3,
                    "action_switch_confidence_gap": 0.30,
                    "objective_min_dwell": 3,
                },
            },
        ],
        # Parameter sets explore action-value coefficient calibration in R-domain.
        "parameter_sets": [
            {"param_id": "P0_defaults", "parameters": {}},
            {
                "param_id": "P1_extend_bonus",
                "parameters": {
                    "continuation_extend_bonus_r": 0.25,
                    "floor_tighten_bonus_r": 0.28,
                },
            },
            {
                "param_id": "P2_release_close_strong",
                "parameters": {
                    "release_close_bonus_r": 0.40,
                    "action_switch_confidence_gap": 0.15,
                },
            },
        ],
        # Stop logic variants vary the objective state thresholds.
        "stop_logic_variants": [
            {"stop_variant_id": "S0_objective_balanced", "overrides": {}},
            {
                "stop_variant_id": "S1_objective_floor_sensitive",
                "overrides": {
                    "floor_giveback_trigger_r": 0.20,
                    "floor_productivity_min": -0.10,
                },
            },
            {
                "stop_variant_id": "S2_objective_release_early",
                "overrides": {
                    "release_giveback_trigger_r": 0.60,
                    "release_continuation_max_r": 0.20,
                    "release_inefficiency_min_r": 0.35,
                },
            },
        ],
        # Scenario overrides modify v1 parameters under specific failure modes.
        "scenario_overrides": {
            "FAST_PANIC_FAILURE": {
                "panic_infer_progress_r": -1.10,
                "panic_infer_velocity": -0.20,
                "release_giveback_trigger_r": 0.60,
            },
            "BUILD_GIVEBACK_CASCADE": {
                "floor_giveback_trigger_r": 0.20,
                "release_giveback_trigger_r": 0.65,
                "inefficiency_weight": 1.8,
            },
            "PROTECT_LAYER_BREAK": {
                "continuation_proxy_enter_r": 0.40,
                "release_continuation_max_r": 0.30,
                "objective_min_dwell": 1,
            },
        },
    }


def run_batch_experiments(*, trades_path: Path, report_out: Path) -> dict[str, Any]:
    cfg = _default_config()
    return run_batch_experiments_with_config(trades_path=trades_path, report_out=report_out, config=cfg)


def run_batch_experiments_with_config(*, trades_path: Path, report_out: Path, config: dict[str, Any]) -> dict[str, Any]:
    cfg = dict(config or {})
    trades = _load_trades(trades_path)

    current_rows: dict[str, dict[str, Any]] = {}
    for tr in trades:
        row = replay_trade_path(tr, policy_name="baseline", policy_overrides={"enable_objective_v1": 0.0})
        row["scenario_id"] = classify_scenario(row)
        current_rows[str(row.get("trade_id"))] = row

    experiments: list[dict[str, Any]] = []
    for kernel in cfg["kernel_candidates"]:
        kernel_id = str(kernel["kernel_id"])
        kernel_type = str(kernel.get("kernel_type", "pure"))
        components = list(kernel.get("components") or [])
        kernel_policy = dict(kernel.get("policy") or {})
        for param_set in cfg["parameter_sets"]:
            param_id = str(param_set["param_id"])
            params = dict(param_set.get("parameters") or {})
            for stop_variant in cfg["stop_logic_variants"]:
                stop_id = str(stop_variant["stop_variant_id"])
                stop_overrides = dict(stop_variant.get("overrides") or {})

                experiment_id = f"{kernel_id}__{param_id}__{stop_id}"
                trade_rows: list[dict[str, Any]] = []
                for tr in trades:
                    current = current_rows.get(str(tr.get("trade_id")), {})
                    scenario_id = str(current.get("scenario_id", classify_scenario(current if current else {})))
                    scenario_policy = dict((cfg.get("scenario_overrides") or {}).get(scenario_id, {}))
                    policy = _merge_policy(kernel_policy, params, stop_overrides, scenario_policy)

                    out = replay_trade_path(
                        tr,
                        policy_name=kernel_id,
                        policy_overrides=policy,
                    )
                    out["scenario_id"] = scenario_id
                    out["kernel_id"] = kernel_id
                    out["kernel_type"] = kernel_type
                    out["components"] = components
                    out["parameter_set_id"] = param_id
                    out["parameters"] = params
                    out["stop_logic_variant"] = stop_id
                    out["experiment_id"] = experiment_id
                    out["delta_vs_current_pips"] = _safe_float(out.get("final_money_result_pips", 0.0), 0.0) - _safe_float(current.get("final_money_result_pips", 0.0), 0.0)
                    trade_rows.append(out)

                deltas = [_safe_float(x.get("delta_vs_baseline_pips", 0.0), 0.0) for x in trade_rows]
                current_deltas = [_safe_float(x.get("delta_vs_current_pips", 0.0), 0.0) for x in trade_rows]
                baseline_1to1_deltas = [_safe_float(x.get("delta_vs_1to1_baseline_pips", x.get("delta_vs_baseline_pips", 0.0)), 0.0) for x in trade_rows]
                baseline_protective_deltas = [_safe_float(x.get("delta_vs_protective_baseline_pips", 0.0), 0.0) for x in trade_rows]
                gt_alignments = [_safe_float(x.get("ground_truth_alignment_rate", 0.0), 0.0) for x in trade_rows]
                scenario_breakdown = _aggregate_breakdown(trade_rows, "scenario_id")
                scenario_breakdown_vs_current = _aggregate_breakdown_for_metric(trade_rows, "scenario_id", "delta_vs_current_pips")
                scenario_breakdown_vs_1to1 = _aggregate_breakdown_for_metric(trade_rows, "scenario_id", "delta_vs_1to1_baseline_pips")
                scenario_breakdown_vs_protective = _aggregate_breakdown_for_metric(trade_rows, "scenario_id", "delta_vs_protective_baseline_pips")
                reason_breakdown = _aggregate_breakdown(trade_rows, "final_reason_code")
                transition_breakdown = _aggregate_breakdown(trade_rows, "final_state_transition")

                regressed_scenarios = [
                    s for s, b in scenario_breakdown.items()
                    if _safe_float(b.get("avg_delta_vs_baseline_pips", 0.0), 0.0) < -1e-9
                ]
                # Major regression: scenario average delta worse than -0.5 pips/trade.
                major_regressions = [
                    s for s, b in scenario_breakdown.items()
                    if _safe_float(b.get("avg_delta_vs_baseline_pips", 0.0), 0.0) <= -0.50
                ]

                experiments.append(
                    {
                        "experiment_id": experiment_id,
                        "kernel_id": kernel_id,
                        "kernel_type": kernel_type,
                        "components": components,
                        "component_definitions": {str(c.get("component_id", "UNKNOWN")): str(c.get("definition", "")) for c in components},
                        "parameter_set_id": param_id,
                        "parameters": params,
                        "stop_logic_variant": stop_id,
                        "effective_policy": policy,
                        "total_delta_vs_baseline_pips": sum(deltas),
                        "avg_delta_vs_baseline_pips": (sum(deltas) / len(deltas)) if deltas else 0.0,
                        "total_delta_vs_1to1_baseline_pips": sum(baseline_1to1_deltas),
                        "avg_delta_vs_1to1_baseline_pips": (sum(baseline_1to1_deltas) / len(baseline_1to1_deltas)) if baseline_1to1_deltas else 0.0,
                        "total_delta_vs_protective_baseline_pips": sum(baseline_protective_deltas),
                        "avg_delta_vs_protective_baseline_pips": (sum(baseline_protective_deltas) / len(baseline_protective_deltas)) if baseline_protective_deltas else 0.0,
                        "total_delta_vs_current_pips": sum(current_deltas),
                        "avg_delta_vs_current_pips": (sum(current_deltas) / len(current_deltas)) if current_deltas else 0.0,
                        "avg_ground_truth_alignment_rate": (sum(gt_alignments) / len(gt_alignments)) if gt_alignments else 0.0,
                        "win_count": sum(1 for d in deltas if d > 1e-9),
                        "loss_count": sum(1 for d in deltas if d < -1e-9),
                        "flat_count": len(deltas) - sum(1 for d in deltas if abs(d) > 1e-9),
                        "pure_or_composite": kernel_type,
                        "per_scenario_delta": scenario_breakdown,
                        "per_scenario_delta_vs_current": scenario_breakdown_vs_current,
                        "per_scenario_delta_vs_1to1_baseline": scenario_breakdown_vs_1to1,
                        "per_scenario_delta_vs_protective_baseline": scenario_breakdown_vs_protective,
                        "reason_code_breakdown": reason_breakdown,
                        "transition_breakdown": transition_breakdown,
                        "regressions": {
                            "regressed_scenarios": regressed_scenarios,
                            "major_regression_scenarios": major_regressions,
                            "has_major_regression": len(major_regressions) > 0,
                        },
                        "per_trade": [
                            {
                                "trade_id": str(r.get("trade_id", "")),
                                "final_result": _safe_float(r.get("final_money_result_pips", 0.0), 0.0),
                                "baseline_result": _safe_float(r.get("baseline_money_result_pips", 0.0), 0.0),
                                "baseline_1to1_result": _safe_float(r.get("baseline_1to1_money_result_pips", r.get("baseline_money_result_pips", 0.0)), 0.0),
                                "baseline_protective_result": _safe_float(r.get("baseline_protective_money_result_pips", 0.0), 0.0),
                                "delta": _safe_float(r.get("delta_vs_baseline_pips", 0.0), 0.0),
                                "delta_vs_1to1_baseline": _safe_float(r.get("delta_vs_1to1_baseline_pips", r.get("delta_vs_baseline_pips", 0.0)), 0.0),
                                "delta_vs_protective_baseline": _safe_float(r.get("delta_vs_protective_baseline_pips", 0.0), 0.0),
                                "delta_vs_current": _safe_float(r.get("delta_vs_current_pips", 0.0), 0.0),
                                "reason_code": str(r.get("final_reason_code", "UNKNOWN")),
                                "state_transition": str(r.get("final_state_transition", "UNKNOWN->UNKNOWN")),
                                "giveback": _safe_float(r.get("max_giveback_r", 0.0), 0.0),
                                "time_in_trade": _safe_float(r.get("time_in_trade_sec", 0.0), 0.0),
                                "locked_profit": _safe_float(r.get("locked_profit_pips", 0.0), 0.0),
                                "scenario_id": str(r.get("scenario_id", "UNKNOWN")),
                                "ground_truth_alignment_rate": _safe_float(r.get("ground_truth_alignment_rate", 0.0), 0.0),
                            }
                            for r in trade_rows
                        ],
                    }
                )

    ranked = sorted(
        experiments,
        key=lambda e: (
            -_safe_float(e.get("total_delta_vs_baseline_pips", 0.0), 0.0),
            -_safe_float(e.get("total_delta_vs_current_pips", 0.0), 0.0),
            1 if bool((e.get("regressions") or {}).get("has_major_regression", False)) else 0,
            len((e.get("regressions") or {}).get("regressed_scenarios", [])),
        ),
    )

    report = {
        "experiment_engine": {
            "input_slice": str(trades_path),
            "trade_count": len(trades),
            "current_policy_name": "baseline",
            "kernel_candidate_count": len(cfg["kernel_candidates"]),
            "parameter_set_count": len(cfg["parameter_sets"]),
            "stop_variant_count": len(cfg["stop_logic_variants"]),
            "scenario_override_count": len(cfg.get("scenario_overrides") or {}),
        },
        "ranked_experiments": ranked,
        "best_experiment": ranked[0] if ranked else {},
    }
    report_out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_out),
        "experiment_count": len(ranked),
        "best_experiment_id": str((ranked[0] or {}).get("experiment_id", "")) if ranked else "",
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run batch AEE kernel experiments on fixed replay slice.")
    ap.add_argument("--input", default="control/aee_kernel_benchmark_slice.json")
    ap.add_argument("--report-out", default="control/aee_batch_experiment_report.json")
    ap.add_argument("--config", default="", help="Optional JSON config path overriding default candidate grid")
    args = ap.parse_args()

    cfg = _default_config()
    if args.config:
        cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))

    summary = run_batch_experiments_with_config(
        trades_path=Path(args.input),
        report_out=Path(args.report_out),
        config=cfg,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
