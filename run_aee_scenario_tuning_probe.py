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


def _avg(values: list[float]) -> float:
    return (sum(values) / len(values)) if values else 0.0


def _load_slice(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and "trades" in payload:
        return list(payload.get("trades") or [])
    if isinstance(payload, list):
        return payload
    raise ValueError("benchmark slice must be list or object with 'trades'")


def _default_probe_policies() -> list[dict[str, Any]]:
    return [
        {
            "name": "baseline",
            "overrides": {},
            "intent": "reference",
        },
        {
            "name": "panic_soften",
            "overrides": {
                "panic_infer_progress_r": -1.10,
                "panic_infer_velocity": -0.20,
                "build_safety_giveback_r": 1.10,
            },
            "intent": "reduce fast panic exits",
        },
        {
            "name": "harvest_earlier",
            "overrides": {
                "build_to_harvest_unrealized_pips": 2.5,
                "build_to_harvest_progress_r": 0.45,
                "build_safety_giveback_r": 1.00,
            },
            "intent": "reduce build giveback cascade",
        },
        {
            "name": "protect_stricter",
            "overrides": {
                "protect_progress_r": 0.35,
                "protect_continuation_score": 0.55,
                "build_safety_giveback_r": 0.95,
            },
            "intent": "avoid weak protect->build transitions",
        },
    ]


def _group_avg(values: list[dict[str, Any]], key: str) -> float:
    return _avg([_safe_float(v.get(key, 0.0), 0.0) for v in values])


def run_scenario_tuning_probe(*, benchmark_slice_path: Path, report_out: Path) -> dict[str, Any]:
    trades = _load_slice(benchmark_slice_path)
    probes = _default_probe_policies()

    baseline_runs: dict[str, dict[str, Any]] = {}
    for tr in trades:
        res = replay_trade_path(tr, policy_name="baseline", policy_overrides={})
        res["scenario_id"] = classify_scenario(res)
        baseline_runs[str(res.get("trade_id"))] = res

    probe_summaries: list[dict[str, Any]] = []
    for probe in probes:
        name = str(probe["name"])
        overrides = dict(probe.get("overrides") or {})
        runs: list[dict[str, Any]] = []
        for tr in trades:
            res = replay_trade_path(tr, policy_name=name, policy_overrides=overrides)
            base = baseline_runs.get(str(res.get("trade_id")), {})
            res["scenario_id"] = classify_scenario(base if base else res)
            res["improvement_vs_baseline_kernel_pips"] = _safe_float(res.get("final_money_result_pips", 0.0), 0.0) - _safe_float(base.get("final_money_result_pips", 0.0), 0.0)
            runs.append(res)

        by_scenario: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in runs:
            by_scenario[str(r.get("scenario_id", "UNKNOWN"))].append(r)

        scenario_summary = {
            k: {
                "count": len(v),
                "avg_improvement_vs_baseline_kernel_pips": _group_avg(v, "improvement_vs_baseline_kernel_pips"),
                "avg_final_money_result_pips": _group_avg(v, "final_money_result_pips"),
                "avg_time_in_trade_sec": _group_avg(v, "time_in_trade_sec"),
                "avg_max_giveback_r": _group_avg(v, "max_giveback_r"),
            }
            for k, v in sorted(by_scenario.items())
        }

        probe_summaries.append(
            {
                "policy_name": name,
                "intent": str(probe.get("intent", "")),
                "overrides": overrides,
                "trade_count": len(runs),
                "avg_improvement_vs_baseline_kernel_pips": _group_avg(runs, "improvement_vs_baseline_kernel_pips"),
                "avg_final_money_result_pips": _group_avg(runs, "final_money_result_pips"),
                "scenario_summary": scenario_summary,
            }
        )

    ranked = sorted(probe_summaries, key=lambda x: _safe_float(x.get("avg_improvement_vs_baseline_kernel_pips", 0.0), 0.0), reverse=True)
    report = {
        "benchmark_slice_path": str(benchmark_slice_path),
        "trade_count": len(trades),
        "policy_rankings": ranked,
    }
    report_out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return {
        "trade_count": len(trades),
        "policy_count": len(ranked),
        "best_policy": (ranked[0]["policy_name"] if ranked else ""),
        "report_path": str(report_out),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Run scenario-focused tuning probes on fixed kernel benchmark slice.")
    ap.add_argument("--input", default="control/aee_kernel_benchmark_slice.json")
    ap.add_argument("--report-out", default="control/aee_scenario_tuning_probe_report.json")
    args = ap.parse_args()

    summary = run_scenario_tuning_probe(
        benchmark_slice_path=Path(args.input),
        report_out=Path(args.report_out),
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
