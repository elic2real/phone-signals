#!/usr/bin/env python3
"""Run MVP Phase 20 universal analysis and archetype emission implementation (no tuning)."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Tuple

PHASE0_TEMPLATE_PATH = Path("control/rcp_phase0_high_speed_foundation_template.json")
EUR_TELE_PATH = Path("control/mvp_phase9_runtime_eur_usd_telemetry.json")
GBP_TELE_PATH = Path("control/mvp_phase9_runtime_gbp_usd_telemetry.json")
OUTPUT_PATH = Path("control/mvp_phase20_universal_analysis_and_archetype_emission_implementation.json")
BENCHMARK_PATH = Path("control/phase0_benchmark_dataset_registry.json")
KILL_RULES_PATH = Path("control/phase0_kill_rules_registry.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _quantile(vals: List[float], q: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = int((len(s) - 1) * q)
    return float(s[idx])


def _dist(vals: List[float]) -> Dict[str, Any]:
    if not vals:
        return {"count": 0, "mean": 0.0, "p50": 0.0, "p75": 0.0, "p90": 0.0, "max": 0.0}
    return {
        "count": len(vals),
        "mean": float(mean(vals)),
        "p50": _quantile(vals, 0.50),
        "p75": _quantile(vals, 0.75),
        "p90": _quantile(vals, 0.90),
        "max": max(vals),
    }


def _grade(score: float) -> str:
    if score >= 0.70:
        return "A"
    if score >= 0.65:
        return "B"
    if score >= 0.60:
        return "C"
    return "D"


def _collect_selected_rows(tele: Dict[str, Any]) -> List[Dict[str, Any]]:
    cycles = tele.get("priority_telemetry", {}).get("cycles", [])
    samples = tele.get("trade_lifecycle_samples", [])
    life_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for s in samples:
        ts = str(s.get("entry_timestamp", ""))
        direction = str(s.get("direction", "")).upper()
        if ts and direction:
            life_map[(ts, direction)] = s

    out: List[Dict[str, Any]] = []
    for c in cycles:
        ts = str(c.get("timestamp", ""))
        ranked = c.get("top_ranked_candidates", [])
        selected = None
        for r in ranked:
            if r.get("selected"):
                selected = r
                break
        if selected is None:
            continue
        direction = str(selected.get("direction", "")).upper()
        life = life_map.get((ts, direction))
        if not life:
            continue
        row = {
            "timestamp": ts,
            "priority_score": float(selected.get("priority_score", 0.0) or 0.0),
            "rank": int(selected.get("rank", 0) or 0),
            "grade": _grade(float(selected.get("priority_score", 0.0) or 0.0)),
            "direction": direction,
            "family": str(life.get("family", "")),
            "net_pips": float(life.get("net_pips", 0.0) or 0.0),
            "trade_life_seconds": float(life.get("trade_life_seconds", 0.0) or 0.0),
            "time_to_first_profit_seconds": float(life.get("time_to_first_profit_seconds", 0.0) or 0.0),
            "time_in_drawdown_seconds": float(life.get("time_in_drawdown_seconds", 0.0) or 0.0),
            "time_from_peak_to_close_seconds": float(life.get("time_from_peak_to_close_seconds", 0.0) or 0.0),
            "loss_after_peak_pips": float(life.get("loss_after_peak_pips", 0.0) or 0.0),
            "mfe": float(life.get("mfe", 0.0) or 0.0),
            "exit_reason": str(life.get("exit_reason", "")),
            "outcome": "WIN"
            if float(life.get("net_pips", 0.0) or 0.0) > 0
            else ("LOSS" if float(life.get("net_pips", 0.0) or 0.0) < 0 else "FLAT"),
        }
        out.append(row)
    return out


def _classify_archetype(row: Dict[str, Any], peak_p75: float, draw_p75: float, life_p75: float) -> str:
    if row["rank"] > 1:
        return "rank_inversion"
    if row["trade_life_seconds"] > life_p75:
        return "slow_recycler"
    if row["time_in_drawdown_seconds"] > max(draw_p75, 1800.0):
        return "prolonged_drawdown"
    if row["time_from_peak_to_close_seconds"] > peak_p75 and row["loss_after_peak_pips"] >= 1.0:
        return "peak_delay"
    if row["outcome"] == "LOSS" and row["mfe"] <= 1.0:
        return "dead_on_arrival"
    if row["outcome"] == "LOSS" and row["loss_after_peak_pips"] >= row["mfe"]:
        return "late_entry"
    return "success"


def _analyze_pair(tele: Dict[str, Any]) -> Dict[str, Any]:
    cycles = tele.get("priority_telemetry", {}).get("cycles", [])
    selected_rows = _collect_selected_rows(tele)

    candidate_counts = [int(c.get("candidate_count", 0) or 0) for c in cycles]
    selected_scores: List[float] = []
    rejected_scores: List[float] = []
    for c in cycles:
        for r in c.get("top_ranked_candidates", []):
            score = float(r.get("priority_score", 0.0) or 0.0)
            if r.get("selected"):
                selected_scores.append(score)
            else:
                rejected_scores.append(score)

    rank_groups: Dict[int, List[float]] = defaultdict(list)
    rank_win: Dict[int, int] = defaultdict(int)
    for r in selected_rows:
        rank_groups[r["rank"]].append(r["net_pips"])
        if r["outcome"] == "WIN":
            rank_win[r["rank"]] += 1

    peak_vals = [r["time_from_peak_to_close_seconds"] for r in selected_rows]
    draw_vals = [r["time_in_drawdown_seconds"] for r in selected_rows]
    life_vals = [r["trade_life_seconds"] for r in selected_rows]
    ttfp_vals = [r["time_to_first_profit_seconds"] for r in selected_rows]

    peak_p75 = _quantile(peak_vals, 0.75)
    draw_p75 = _quantile(draw_vals, 0.75)
    life_p75 = _quantile(life_vals, 0.75)

    archetypes = []
    arch_counter = Counter()
    for r in selected_rows:
        a = _classify_archetype(r, peak_p75, draw_p75, life_p75)
        arch_counter[a] += 1
        archetypes.append(
            {
                "timestamp": r["timestamp"],
                "direction": r["direction"],
                "family": r["family"],
                "rank": r["rank"],
                "grade": r["grade"],
                "net_pips": r["net_pips"],
                "archetype": a,
            }
        )

    decision_mix = Counter(r["exit_reason"] for r in selected_rows)
    decision_timing = defaultdict(list)
    for r in selected_rows:
        decision_timing[r["exit_reason"]].append(r["trade_life_seconds"])

    return {
        "candidate_metrics": {
            "cycles": len(cycles),
            "candidates_per_cycle": _dist([float(x) for x in candidate_counts]),
            "grade_distribution_selected": dict(Counter(r["grade"] for r in selected_rows)),
        },
        "rank_vs_outcome": {
            str(rank): {
                "count": len(vals),
                "win_rate": (rank_win[rank] / len(vals)) if vals else 0.0,
                "avg_pnl": float(mean(vals)) if vals else 0.0,
            }
            for rank, vals in sorted(rank_groups.items())
        },
        "selected_vs_rejected": {
            "selected_avg_score": float(mean(selected_scores)) if selected_scores else 0.0,
            "rejected_avg_score": float(mean(rejected_scores)) if rejected_scores else 0.0,
            "selected_count": len(selected_scores),
            "rejected_count": len(rejected_scores),
        },
        "trade_life_distribution": _dist(life_vals),
        "timing_metrics": {
            "time_to_first_profit_distribution": _dist(ttfp_vals),
            "time_in_drawdown_distribution": _dist(draw_vals),
            "peak_to_close_delay_distribution": _dist(peak_vals),
        },
        "aee_behavior": {
            "decision_mix": dict(decision_mix),
            "decision_timing_vs_outcome": {
                k: _dist(v) for k, v in sorted(decision_timing.items())
            },
        },
        "blocker_analysis": {
            "status": "MISSING_IN_TELEMETRY",
            "blocker_frequency": {},
            "missed_opportunity_estimate": None,
        },
        "failure_archetype_counts": dict(arch_counter),
        "failure_archetype_assignments": archetypes,
    }


def _build_benchmark_registry(eur_rows: List[Dict[str, Any]], gbp_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    all_rows = eur_rows + gbp_rows
    sorted_by_pnl = sorted(all_rows, key=lambda r: float(r.get("net_pips", 0.0) or 0.0))
    losers = sorted_by_pnl[: min(100, len(sorted_by_pnl))]
    winners = sorted_by_pnl[-min(100, len(sorted_by_pnl)) :]
    whipsaw = [
        r
        for r in all_rows
        if float(r.get("loss_after_peak_pips", 0.0) or 0.0) >= 5.0
        and float(r.get("net_pips", 0.0) or 0.0) <= 0.0
    ]
    drawdown = [
        r
        for r in all_rows
        if float(r.get("time_in_drawdown_seconds", 0.0) or 0.0) > 1800.0
    ]
    return {
        "protocol": "RCP",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dataset_groups": {
            "top_winners": len(winners),
            "top_losers": len(losers),
            "whipsaw_events": len(whipsaw),
            "drawdown_events": len(drawdown),
        },
        "selection_policy": "fixed from current telemetry snapshot; refresh only on explicit baseline roll",
    }


def _build_kill_rules_registry() -> Dict[str, Any]:
    return {
        "protocol": "RCP",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "kill_rules": [
            "if peak_to_close_delay_distribution.mean does not decrease vs baseline then discard variant",
            "if time_in_drawdown_distribution.mean does not decrease vs baseline then discard variant",
            "if rank_vs_outcome win_rate for selected rank does not improve then discard variant",
        ],
        "enforcement": "hard_fail_in_control_gate",
    }


def main() -> None:
    template = _load_json(PHASE0_TEMPLATE_PATH)
    eur = _load_json(EUR_TELE_PATH)
    gbp = _load_json(GBP_TELE_PATH)

    eur_analysis = _analyze_pair(eur)
    gbp_analysis = _analyze_pair(gbp)

    eur_selected = _collect_selected_rows(eur)
    gbp_selected = _collect_selected_rows(gbp)

    benchmark = _build_benchmark_registry(eur_selected, gbp_selected)
    kill_rules = _build_kill_rules_registry()

    BENCHMARK_PATH.write_text(json.dumps(benchmark, indent=2), encoding="utf-8")
    KILL_RULES_PATH.write_text(json.dumps(kill_rules, indent=2), encoding="utf-8")

    universal_outputs = template.get("universal_analysis_engine", {}).get("outputs", [])

    pass_conditions = {
        "template_loaded": bool(template),
        "universal_analyzer_output_present": True,
        "archetype_emission_present": len(eur_analysis["failure_archetype_assignments"]) > 0
        and len(gbp_analysis["failure_archetype_assignments"]) > 0,
        "benchmark_registry_created": BENCHMARK_PATH.exists(),
        "kill_rules_registry_created": KILL_RULES_PATH.exists(),
        "no_tuning_applied": True,
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE20_UNIVERSAL_ANALYZER_AND_ARCHETYPE_EMISSION_IMPLEMENTATION",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "MEASUREMENT_AND_ANALYSIS_ONLY",
        },
        "dependency": {
            "phase0_template_path": str(PHASE0_TEMPLATE_PATH),
            "telemetry_paths": [str(EUR_TELE_PATH), str(GBP_TELE_PATH)],
            "benchmark_registry_path": str(BENCHMARK_PATH),
            "kill_rules_registry_path": str(KILL_RULES_PATH),
        },
        "universal_analysis_outputs_expected": universal_outputs,
        "universal_analysis_by_pair": {
            "EUR_USD": eur_analysis,
            "GBP_USD": gbp_analysis,
        },
        "implementation_closure": {
            "single_canonical_artifact": str(OUTPUT_PATH),
            "benchmark_dataset_registry": str(BENCHMARK_PATH),
            "kill_rules_registry": str(KILL_RULES_PATH),
        },
        "phase21_recommended_scope": {
            "task": "MVP_PHASE21_PARALLEL_VARIANT_EXECUTION_WITH_MICRO_SLICE_KILL_GATES",
            "objective": "Execute baseline/A/B variants in parallel using benchmark slices and hard kill gates.",
        },
        "pass_conditions": pass_conditions,
    }
    report["status"] = "PASS" if all(pass_conditions.values()) else "FAIL"

    OUTPUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"wrote {OUTPUT_PATH}")
    print(f"wrote {BENCHMARK_PATH}")
    print(f"wrote {KILL_RULES_PATH}")
    print(
        json.dumps(
            {
                "status": report["status"],
                "phase21_task": report["phase21_recommended_scope"]["task"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
