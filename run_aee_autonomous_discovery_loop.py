#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from run_aee_batch_experiments import _default_config, run_batch_experiments_with_config


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _is_winner(exp: dict[str, Any]) -> bool:
    regressions = exp.get("regressions") or {}
    has_major = bool(regressions.get("has_major_regression", False))
    return (
        _safe_float(exp.get("total_delta_vs_1to1_baseline_pips", 0.0), 0.0) > 0.0
        and _safe_float(exp.get("total_delta_vs_protective_baseline_pips", 0.0), 0.0) > 0.0
        and _safe_float(exp.get("total_delta_vs_current_pips", 0.0), 0.0) > 0.0
        and not has_major
    )


def _score(exp: dict[str, Any]) -> tuple[float, float, float, int, int]:
    regressions = exp.get("regressions") or {}
    has_major = bool(regressions.get("has_major_regression", False))
    regressed_count = len(regressions.get("regressed_scenarios") or [])
    return (
        _safe_float(exp.get("total_delta_vs_1to1_baseline_pips", 0.0), 0.0),
        _safe_float(exp.get("total_delta_vs_protective_baseline_pips", 0.0), 0.0),
        _safe_float(exp.get("total_delta_vs_current_pips", 0.0), 0.0),
        0 if has_major else 1,
        -regressed_count,
    )


def _clamp_policy_value(key: str, value: float) -> float:
    # Integer-valued params: clamp to reasonable integer range.
    if "dwell" in key or "confirm" in key:
        return float(min(8, max(1, round(value))))
    if "_r" in key or "score" in key:
        return min(2.0, max(0.01, value))
    if "weight" in key:
        return min(5.0, max(0.1, value))
    if "velocity" in key:
        return min(1.0, max(-1.0, value))
    if "pips" in key:
        return min(20.0, max(-20.0, value))
    if "bonus" in key or "penalty" in key:
        return min(1.0, max(0.0, value))
    return value


def _mutation_step(key: str) -> float:
    if "dwell" in key or "confirm" in key:
        return 1.0
    if "_r" in key:
        return 0.05
    if "score" in key:
        return 0.05
    if "weight" in key:
        return 0.3
    if "velocity" in key:
        return 0.05
    if "pips" in key:
        return 0.2
    if "bonus" in key or "penalty" in key:
        return 0.05
    return 0.1


def _policy_signature(policy: dict[str, float]) -> tuple[tuple[str, float], ...]:
    return tuple(sorted((str(k), float(v)) for k, v in policy.items()))


def _build_iteration_config(
    *,
    iteration: int,
    base_cfg: dict[str, Any],
    previous_ranked: list[dict[str, Any]],
    max_candidates: int,
) -> dict[str, Any]:
    kernel_candidates: list[dict[str, Any]] = [
        {
            "kernel_id": f"iter{iteration:02d}_v1_defaults",
            "kernel_type": "pure",
            "components": [
                {
                    "component_id": "OBJECTIVE_STATE_MACHINE_V1",
                    "definition": "v1 engine at default parameters — discovery iteration anchor.",
                }
            ],
            "policy": {"enable_objective_v1": 1.0},
        }
    ]

    seen = {_policy_signature({"enable_objective_v1": 1.0}), _policy_signature({})}

    # Keep a few best kernels from previous round, then mutate around them.
    sources = previous_ranked[:3] if previous_ranked else list(base_cfg.get("kernel_candidates") or [])[:3]

    for idx, src in enumerate(sources):
        if len(kernel_candidates) >= max_candidates:
            break

        source_policy = dict(src.get("effective_policy") or src.get("policy") or {})
        # Every mutated candidate must run the v1 engine.
        source_policy["enable_objective_v1"] = 1.0
        source_kernel_type = str(src.get("kernel_type", "pure"))
        source_components = list(src.get("components") or [])
        sig = _policy_signature(source_policy)
        if sig not in seen:
            kernel_candidates.append(
                {
                    "kernel_id": f"iter{iteration:02d}_seed_{idx}",
                    "kernel_type": source_kernel_type,
                    "components": source_components,
                    "policy": source_policy,
                }
            )
            seen.add(sig)

        # Mutate only the meaningful v1 parameters; skip enable_objective_v1 itself.
        _v1_mutable_keys = {
            "release_giveback_trigger_r", "floor_giveback_trigger_r",
            "continuation_proxy_enter_r", "release_continuation_max_r",
            "release_inefficiency_min_r", "inefficiency_weight",
            "min_action_dwell", "action_switch_confidence_gap",
            "objective_min_dwell", "objective_confirm_bars",
            "release_close_bonus_r", "floor_tighten_bonus_r",
            "continuation_extend_bonus_r", "floor_productivity_min",
        }
        for k, v in sorted(source_policy.items()):
            if k == "enable_objective_v1":
                continue
            if k not in _v1_mutable_keys:
                continue
            if len(kernel_candidates) >= max_candidates:
                break
            f = _safe_float(v, 0.0)
            step = _mutation_step(str(k))
            for direction in (-1.0, 1.0):
                if len(kernel_candidates) >= max_candidates:
                    break
                mut = dict(source_policy)
                mut[str(k)] = _clamp_policy_value(str(k), f + (direction * step))
                msig = _policy_signature(mut)
                if msig in seen:
                    continue
                kernel_candidates.append(
                    {
                        "kernel_id": f"iter{iteration:02d}_mut_{idx}_{str(k)}_{'down' if direction < 0 else 'up'}",
                        "kernel_type": "refined",
                        "components": source_components
                        + [
                            {
                                "component_id": "AUTONOMOUS_MUTATION",
                                "definition": f"Auto mutation on {k} by {direction * step:+.3f}.",
                            }
                        ],
                        "policy": mut,
                    }
                )
                seen.add(msig)

    # Fall back to default candidates if mutation sources were too sparse.
    for base in list(base_cfg.get("kernel_candidates") or []):
        if len(kernel_candidates) >= max_candidates:
            break
        pol = dict(base.get("policy") or {})
        sig = _policy_signature(pol)
        if sig in seen:
            continue
        kernel_candidates.append(
            {
                "kernel_id": f"iter{iteration:02d}_{base.get('kernel_id', 'candidate')}",
                "kernel_type": str(base.get("kernel_type", "pure")),
                "components": list(base.get("components") or []),
                "policy": pol,
            }
        )
        seen.add(sig)

    return {
        "kernel_candidates": kernel_candidates,
        "parameter_sets": list(base_cfg.get("parameter_sets") or [{"param_id": "P0", "parameters": {}}]),
        "stop_logic_variants": list(base_cfg.get("stop_logic_variants") or [{"stop_variant_id": "S0_BALANCED", "overrides": {}}]),
        "scenario_overrides": dict(base_cfg.get("scenario_overrides") or {}),
    }


def run_autonomous_discovery_loop(
    *,
    trades_path: Path,
    report_out: Path,
    max_iterations: int = 8,
    plateau_window: int = 2,
    improvement_epsilon: float = 0.05,
    max_candidates: int = 10,
) -> dict[str, Any]:
    base_cfg = _default_config()
    iteration_reports_dir = report_out.parent
    stem = report_out.stem

    previous_ranked: list[dict[str, Any]] = []
    best_overall: dict[str, Any] = {}
    best_primary = float("-inf")
    plateau_streak = 0
    iterations: list[dict[str, Any]] = []
    stop_condition = "iteration_limit"

    for i in range(max_iterations):
        cfg = _build_iteration_config(
            iteration=i,
            base_cfg=base_cfg,
            previous_ranked=previous_ranked,
            max_candidates=max_candidates,
        )

        iter_report = iteration_reports_dir / f"{stem}_iter_{i:02d}.json"
        try:
            summary = run_batch_experiments_with_config(
                trades_path=trades_path,
                report_out=iter_report,
                config=cfg,
            )
        except Exception as exc:
            stop_condition = "structural_error"
            iterations.append(
                {
                    "iteration": i,
                    "status": "error",
                    "error": str(exc),
                    "report_path": str(iter_report),
                }
            )
            break

        payload = json.loads(iter_report.read_text(encoding="utf-8"))
        ranked = list(payload.get("ranked_experiments") or [])
        if not ranked:
            stop_condition = "structural_error"
            iterations.append(
                {
                    "iteration": i,
                    "status": "empty",
                    "report_path": str(iter_report),
                    "summary": summary,
                }
            )
            break

        best = dict(ranked[0])
        primary = _safe_float(best.get("total_delta_vs_1to1_baseline_pips", 0.0), 0.0)
        improved = primary > (best_primary + improvement_epsilon)
        if improved:
            best_primary = primary
            plateau_streak = 0
            if not best_overall or _score(best) > _score(best_overall):
                best_overall = best
        else:
            plateau_streak += 1
            if not best_overall:
                best_overall = best

        iterations.append(
            {
                "iteration": i,
                "status": "ok",
                "report_path": str(iter_report),
                "experiment_count": int(summary.get("experiment_count", 0)),
                "best_experiment_id": str(best.get("experiment_id", "")),
                "best_kernel_id": str(best.get("kernel_id", "")),
                "best_total_delta_vs_1to1_baseline_pips": primary,
                "best_total_delta_vs_protective_baseline_pips": _safe_float(best.get("total_delta_vs_protective_baseline_pips", 0.0), 0.0),
                "best_total_delta_vs_current_pips": _safe_float(best.get("total_delta_vs_current_pips", 0.0), 0.0),
                "has_major_regression": bool((best.get("regressions") or {}).get("has_major_regression", False)),
                "plateau_streak": plateau_streak,
            }
        )

        if _is_winner(best):
            stop_condition = "winner_found"
            if not best_overall or _score(best) > _score(best_overall):
                best_overall = best
            break

        if plateau_streak >= plateau_window:
            stop_condition = "plateau"
            break

        previous_ranked = ranked

    out = {
        "protocol": "AEE_RESEARCH_KERNEL_DISCOVERY_LOCKED",
        "input_slice": str(trades_path),
        "max_iterations": max_iterations,
        "plateau_window": plateau_window,
        "improvement_epsilon": improvement_epsilon,
        "max_candidates": max_candidates,
        "stop_condition": stop_condition,
        "winner_found": _is_winner(best_overall) if best_overall else False,
        "best_overall": best_overall,
        "iterations": iterations,
    }
    report_out.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    return {
        "report_path": str(report_out),
        "stop_condition": stop_condition,
        "iteration_count": len(iterations),
        "winner_found": bool(out["winner_found"]),
        "best_experiment_id": str((best_overall or {}).get("experiment_id", "")),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run autonomous AEE kernel discovery loop in Codespaces.")
    ap.add_argument("--input", default="control/aee_kernel_benchmark_slice.json")
    ap.add_argument("--report-out", default="control/aee_autonomous_discovery_loop_report.json")
    ap.add_argument("--max-iterations", type=int, default=8)
    ap.add_argument("--plateau-window", type=int, default=2)
    ap.add_argument("--improvement-epsilon", type=float, default=0.05)
    ap.add_argument("--max-candidates", type=int, default=10)
    args = ap.parse_args()

    summary = run_autonomous_discovery_loop(
        trades_path=Path(args.input),
        report_out=Path(args.report_out),
        max_iterations=args.max_iterations,
        plateau_window=args.plateau_window,
        improvement_epsilon=args.improvement_epsilon,
        max_candidates=args.max_candidates,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
