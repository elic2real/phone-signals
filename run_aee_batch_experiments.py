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


def _default_config() -> dict[str, Any]:
    return {
        "kernel_candidates": [
            {"kernel_id": "kernel_baseline", "policy": {}},
            {"kernel_id": "kernel_progress_entry", "policy": {"protect_progress_r": 0.20}},
            {"kernel_id": "kernel_build_guard", "policy": {"build_safety_giveback_r": 0.85}},
            {
                "kernel_id": "kernel_harvest_early",
                "policy": {
                    "build_to_harvest_unrealized_pips": 2.5,
                    "build_to_harvest_progress_r": 0.45,
                },
            },
        ],
        "parameter_sets": [
            {"param_id": "P0", "parameters": {}},
            {"param_id": "P1", "parameters": {"protect_continuation_score": 0.40}},
            {"param_id": "P2", "parameters": {"harvest_giveback_r": 0.65}},
        ],
        "stop_logic_variants": [
            {"stop_variant_id": "S0_BALANCED", "overrides": {}},
            {
                "stop_variant_id": "S1_TIGHT",
                "overrides": {
                    "build_safety_giveback_r": 0.80,
                    "harvest_giveback_r": 0.60,
                    "runner_safety_giveback_r": 0.80,
                },
            },
            {
                "stop_variant_id": "S2_LOOSE",
                "overrides": {
                    "build_safety_giveback_r": 1.05,
                    "harvest_giveback_r": 0.80,
                    "runner_safety_giveback_r": 0.95,
                },
            },
        ],
        "scenario_overrides": {
            "FAST_PANIC_FAILURE": {
                "panic_infer_progress_r": -1.10,
                "panic_infer_velocity": -0.20,
            },
            "BUILD_GIVEBACK_CASCADE": {
                "build_to_harvest_unrealized_pips": 2.20,
                "build_to_harvest_progress_r": 0.40,
            },
            "PROTECT_LAYER_BREAK": {
                "protect_progress_r": 0.35,
                "protect_continuation_score": 0.55,
            },
        },
    }


def run_batch_experiments(*, trades_path: Path, report_out: Path) -> dict[str, Any]:
    cfg = _default_config()
    trades = _load_trades(trades_path)

    baseline_rows: dict[str, dict[str, Any]] = {}
    for tr in trades:
        row = replay_trade_path(tr, policy_name="baseline", policy_overrides={})
        row["scenario_id"] = classify_scenario(row)
        baseline_rows[str(row.get("trade_id"))] = row

    experiments: list[dict[str, Any]] = []
    for kernel in cfg["kernel_candidates"]:
        kernel_id = str(kernel["kernel_id"])
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
                    baseline = baseline_rows.get(str(tr.get("trade_id")), {})
                    scenario_id = str(baseline.get("scenario_id", classify_scenario(baseline if baseline else {})))
                    scenario_policy = dict((cfg.get("scenario_overrides") or {}).get(scenario_id, {}))
                    policy = _merge_policy(kernel_policy, params, stop_overrides, scenario_policy)

                    out = replay_trade_path(
                        tr,
                        policy_name=kernel_id,
                        policy_overrides=policy,
                    )
                    out["scenario_id"] = scenario_id
                    out["kernel_id"] = kernel_id
                    out["parameters"] = params
                    out["stop_logic_variant"] = stop_id
                    out["experiment_id"] = experiment_id
                    out["delta_vs_baseline_kernel_pips"] = _safe_float(out.get("final_money_result_pips", 0.0), 0.0) - _safe_float(baseline.get("final_money_result_pips", 0.0), 0.0)
                    trade_rows.append(out)

                deltas = [_safe_float(x.get("delta_vs_baseline_pips", 0.0), 0.0) for x in trade_rows]
                scenario_breakdown = _aggregate_breakdown(trade_rows, "scenario_id")
                reason_breakdown = _aggregate_breakdown(trade_rows, "final_reason_code")
                transition_breakdown = _aggregate_breakdown(trade_rows, "final_state_transition")

                regressed_scenarios = [
                    s for s, b in scenario_breakdown.items() if _safe_float(b.get("total_delta_vs_baseline_pips", 0.0), 0.0) < 0.0
                ]
                major_regressions = [
                    s for s, b in scenario_breakdown.items() if _safe_float(b.get("total_delta_vs_baseline_pips", 0.0), 0.0) <= -2.0
                ]

                experiments.append(
                    {
                        "experiment_id": experiment_id,
                        "kernel_id": kernel_id,
                        "parameters": params,
                        "stop_logic_variant": stop_id,
                        "total_delta_vs_baseline_pips": sum(deltas),
                        "avg_delta_vs_baseline_pips": (sum(deltas) / len(deltas)) if deltas else 0.0,
                        "win_count": sum(1 for d in deltas if d > 1e-9),
                        "loss_count": sum(1 for d in deltas if d < -1e-9),
                        "flat_count": len(deltas) - sum(1 for d in deltas if abs(d) > 1e-9),
                        "per_scenario_delta": scenario_breakdown,
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
                                "delta": _safe_float(r.get("delta_vs_baseline_pips", 0.0), 0.0),
                                "reason_code": str(r.get("final_reason_code", "UNKNOWN")),
                                "state_transition": str(r.get("final_state_transition", "UNKNOWN->UNKNOWN")),
                                "giveback": _safe_float(r.get("max_giveback_r", 0.0), 0.0),
                                "time_in_trade": _safe_float(r.get("time_in_trade_sec", 0.0), 0.0),
                                "locked_profit": _safe_float(r.get("locked_profit_pips", 0.0), 0.0),
                                "scenario_id": str(r.get("scenario_id", "UNKNOWN")),
                            }
                            for r in trade_rows
                        ],
                    }
                )

    ranked = sorted(
        experiments,
        key=lambda e: (
            -_safe_float(e.get("total_delta_vs_baseline_pips", 0.0), 0.0),
            1 if bool((e.get("regressions") or {}).get("has_major_regression", False)) else 0,
            len((e.get("regressions") or {}).get("regressed_scenarios", [])),
        ),
    )

    report = {
        "experiment_engine": {
            "input_slice": str(trades_path),
            "trade_count": len(trades),
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
    args = ap.parse_args()

    summary = run_batch_experiments(
        trades_path=Path(args.input),
        report_out=Path(args.report_out),
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
