#!/usr/bin/env python3
"""Build MVP Phase 10 priority failure pattern analysis (no tuning)."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, List, Tuple

PHASE9_PATH = Path("control/mvp_phase9_priority_and_trade_life_telemetry_closure_no_tuning.json")
EUR_TELE_PATH = Path("control/mvp_phase9_runtime_eur_usd_telemetry.json")
GBP_TELE_PATH = Path("control/mvp_phase9_runtime_gbp_usd_telemetry.json")
OUTPUT_PATH = Path("control/mvp_phase10_priority_failure_pattern_analysis_no_tuning.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _grade(score: float) -> str:
    if score >= 0.70:
        return "A"
    if score >= 0.65:
        return "B"
    if score >= 0.60:
        return "C"
    return "D"


def _safe_mean(vals: List[float]) -> float:
    return float(mean(vals)) if vals else 0.0


def _safe_median(vals: List[float]) -> float | None:
    return float(median(vals)) if vals else None


def _pct(part: int, total: int) -> float:
    return float(part) / float(total) if total else 0.0


def _quantile(vals: List[float], q: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = int((len(s) - 1) * q)
    return float(s[idx])


def _dist_stats(vals: List[float]) -> Dict[str, Any]:
    return {
        "count": len(vals),
        "mean": _safe_mean(vals),
        "median": _safe_median(vals),
        "p75": _quantile(vals, 0.75),
        "p90": _quantile(vals, 0.90),
        "max": max(vals) if vals else 0.0,
    }


def _analyze_pair(pair: str, tele: Dict[str, Any]) -> Dict[str, Any]:
    p = tele.get("priority_telemetry", {})
    cycles = p.get("cycles", []) if isinstance(p, dict) else []
    lifecycle_samples = tele.get("trade_lifecycle_samples", [])

    lifecycle_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for s in lifecycle_samples:
        ts = str(s.get("entry_timestamp", ""))
        direction = str(s.get("direction", "")).upper()
        if ts and direction:
            lifecycle_map[(ts, direction)] = s

    rank_counts: Dict[int, Dict[str, int]] = defaultdict(lambda: {"selected": 0, "total": 0})
    selected_rows: List[Dict[str, Any]] = []
    rejected_rows: List[Dict[str, Any]] = []
    selected_vs_rejected_rows: List[Dict[str, Any]] = []

    for c in cycles:
        ts = str(c.get("timestamp", ""))
        cand = c.get("top_ranked_candidates", [])
        if not isinstance(cand, list):
            continue

        selected = None
        for r in cand:
            rank = int(r.get("rank", 0) or 0)
            if rank > 0:
                rank_counts[rank]["total"] += 1
            if r.get("selected"):
                selected = r
                if rank > 0:
                    rank_counts[rank]["selected"] += 1
                break

        for r in cand:
            if r.get("selected"):
                selected_rows.append({"timestamp": ts, **r})
            else:
                rejected_rows.append({"timestamp": ts, **r})

        if selected is not None:
            sel_score = float(selected.get("priority_score", 0.0) or 0.0)
            rejected_scores = [
                float(r.get("priority_score", 0.0) or 0.0)
                for r in cand
                if not r.get("selected")
            ]
            best_rej = max(rejected_scores) if rejected_scores else None
            selected_vs_rejected_rows.append(
                {
                    "timestamp": ts,
                    "selected_rank": int(selected.get("rank", 0) or 0),
                    "selected_score": sel_score,
                    "best_rejected_score": best_rej,
                    "selected_minus_best_rejected": None if best_rej is None else (sel_score - best_rej),
                    "selected_direction": str(selected.get("direction", "")).upper(),
                }
            )

    selected_enriched: List[Dict[str, Any]] = []
    for r in selected_rows:
        key = (str(r.get("timestamp", "")), str(r.get("direction", "")).upper())
        life = lifecycle_map.get(key)
        if not life:
            continue
        score = float(r.get("priority_score", 0.0) or 0.0)
        selected_enriched.append(
            {
                "rank": int(r.get("rank", 0) or 0),
                "priority_score": score,
                "grade": _grade(score),
                "trade_life_seconds": float(life.get("trade_life_seconds", 0.0) or 0.0),
                "time_to_first_profit_seconds": float(life.get("time_to_first_profit_seconds", 0.0) or 0.0),
                "time_in_drawdown_seconds": float(life.get("time_in_drawdown_seconds", 0.0) or 0.0),
                "time_from_peak_to_close_seconds": float(life.get("time_from_peak_to_close_seconds", 0.0) or 0.0),
            }
        )

    rank_vs_selection = []
    for rank in sorted(rank_counts):
        rc = rank_counts[rank]
        rank_vs_selection.append(
            {
                "rank": rank,
                "cycles_seen": rc["total"],
                "selected_count": rc["selected"],
                "selection_rate": _pct(rc["selected"], rc["total"]),
            }
        )

    rank_vs_outcome: List[Dict[str, Any]] = []
    by_rank: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
    by_grade: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in selected_enriched:
        by_rank[row["rank"]].append(row)
        by_grade[row["grade"]].append(row)

    for rank in sorted(by_rank):
        vals = by_rank[rank]
        rank_vs_outcome.append(
            {
                "rank": rank,
                "sample_count": len(vals),
                "trade_life_seconds": _dist_stats([v["trade_life_seconds"] for v in vals]),
                "time_to_first_profit_seconds": _dist_stats([v["time_to_first_profit_seconds"] for v in vals]),
                "time_in_drawdown_seconds": _dist_stats([v["time_in_drawdown_seconds"] for v in vals]),
                "time_from_peak_to_close_seconds": _dist_stats([v["time_from_peak_to_close_seconds"] for v in vals]),
            }
        )

    grade_distributions = {}
    for g in sorted(by_grade):
        vals = by_grade[g]
        grade_distributions[g] = {
            "sample_count": len(vals),
            "trade_life_seconds": _dist_stats([v["trade_life_seconds"] for v in vals]),
            "time_to_first_profit_seconds": _dist_stats([v["time_to_first_profit_seconds"] for v in vals]),
            "time_in_drawdown_seconds": _dist_stats([v["time_in_drawdown_seconds"] for v in vals]),
            "time_from_peak_to_close_seconds": _dist_stats([v["time_from_peak_to_close_seconds"] for v in vals]),
        }

    selected_scores = [float(r.get("priority_score", 0.0) or 0.0) for r in selected_rows]
    rejected_scores = [float(r.get("priority_score", 0.0) or 0.0) for r in rejected_rows]

    weak_selection_cases = [
        r for r in selected_vs_rejected_rows
        if r["selected_minus_best_rejected"] is not None and r["selected_minus_best_rejected"] < -1e-9
    ]

    life_vals = [v["trade_life_seconds"] for v in selected_enriched]
    draw_vals = [v["time_in_drawdown_seconds"] for v in selected_enriched]
    peak_vals = [v["time_from_peak_to_close_seconds"] for v in selected_enriched]
    ttfp_vals = [v["time_to_first_profit_seconds"] for v in selected_enriched]

    life_p75 = _quantile(life_vals, 0.75)
    draw_p75 = _quantile(draw_vals, 0.75)
    peak_p75 = _quantile(peak_vals, 0.75)
    ttfp_p75 = _quantile(ttfp_vals, 0.75)

    high_rank_slow = [v for v in selected_enriched if v["rank"] <= 2 and v["trade_life_seconds"] > life_p75]
    prolonged_drawdown = [v for v in selected_enriched if v["time_in_drawdown_seconds"] > draw_p75]
    long_peak_delay = [v for v in selected_enriched if v["time_from_peak_to_close_seconds"] > peak_p75]
    slow_first_profit = [v for v in selected_enriched if v["time_to_first_profit_seconds"] > ttfp_p75]

    archetypes = [
        {
            "name": "selected_below_rejected_score",
            "count": len(weak_selection_cases),
            "rate_over_selected_cycles": _pct(len(weak_selection_cases), len(selected_vs_rejected_rows)),
            "description": "Selected candidate had lower priority_score than best rejected candidate in same cycle.",
        },
        {
            "name": "high_rank_slow_recycler",
            "count": len(high_rank_slow),
            "rate_over_selected_with_lifecycle": _pct(len(high_rank_slow), len(selected_enriched)),
            "description": "Rank<=2 selected trades with trade_life above pair p75.",
        },
        {
            "name": "prolonged_drawdown_selected",
            "count": len(prolonged_drawdown),
            "rate_over_selected_with_lifecycle": _pct(len(prolonged_drawdown), len(selected_enriched)),
            "description": "Selected trades spending longer than p75 time in drawdown.",
        },
        {
            "name": "long_peak_to_close_delay",
            "count": len(long_peak_delay),
            "rate_over_selected_with_lifecycle": _pct(len(long_peak_delay), len(selected_enriched)),
            "description": "Selected trades with long delay from peak to close above p75.",
        },
        {
            "name": "slow_time_to_first_profit",
            "count": len(slow_first_profit),
            "rate_over_selected_with_lifecycle": _pct(len(slow_first_profit), len(selected_enriched)),
            "description": "Selected trades with time_to_first_profit above p75.",
        },
    ]

    archetypes.sort(key=lambda x: (-int(x.get("count", 0)), x.get("name", "")))

    selected_vs_rejected = {
        "sample_sizes": {
            "selected_candidates": len(selected_rows),
            "rejected_candidates": len(rejected_rows),
            "selected_with_lifecycle_join": len(selected_enriched),
        },
        "priority_score": {
            "selected_mean": _safe_mean(selected_scores),
            "selected_median": _safe_median(selected_scores),
            "rejected_mean": _safe_mean(rejected_scores),
            "rejected_median": _safe_median(rejected_scores),
            "selected_minus_rejected_mean": _safe_mean(selected_scores) - _safe_mean(rejected_scores),
        },
        "selected_minus_best_rejected": {
            "count": len([r for r in selected_vs_rejected_rows if r["selected_minus_best_rejected"] is not None]),
            "mean": _safe_mean([
                float(r["selected_minus_best_rejected"])
                for r in selected_vs_rejected_rows
                if r["selected_minus_best_rejected"] is not None
            ]),
            "median": _safe_median([
                float(r["selected_minus_best_rejected"])
                for r in selected_vs_rejected_rows
                if r["selected_minus_best_rejected"] is not None
            ]),
            "negative_cases": len(weak_selection_cases),
        },
    }

    return {
        "pair": pair,
        "rank_vs_selection_table": rank_vs_selection,
        "rank_vs_outcome_table": rank_vs_outcome,
        "selected_vs_rejected_comparison": selected_vs_rejected,
        "trade_life_distribution_by_rank_grade": grade_distributions,
        "time_to_first_profit_distribution_by_rank_grade": {
            g: v["time_to_first_profit_seconds"] for g, v in grade_distributions.items()
        },
        "drawdown_time_distribution_by_rank_grade": {
            g: v["time_in_drawdown_seconds"] for g, v in grade_distributions.items()
        },
        "peak_to_close_delay_distribution_by_rank_grade": {
            g: v["time_from_peak_to_close_seconds"] for g, v in grade_distributions.items()
        },
        "failure_archetypes": archetypes,
        "diagnostic_quantiles": {
            "trade_life_p75": life_p75,
            "time_in_drawdown_p75": draw_p75,
            "time_from_peak_to_close_p75": peak_p75,
            "time_to_first_profit_p75": ttfp_p75,
        },
    }


def _merge_archetypes(a: List[Dict[str, Any]], b: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    bucket: Dict[str, Dict[str, Any]] = {}
    for row in a + b:
        n = row.get("name", "unknown")
        if n not in bucket:
            bucket[n] = {
                "name": n,
                "count": 0,
                "description": row.get("description", ""),
            }
        bucket[n]["count"] += int(row.get("count", 0) or 0)
    out = list(bucket.values())
    out.sort(key=lambda x: (-x["count"], x["name"]))
    return out


def main() -> None:
    phase9 = _load_json(PHASE9_PATH)
    eur = _load_json(EUR_TELE_PATH)
    gbp = _load_json(GBP_TELE_PATH)

    eur_result = _analyze_pair("EUR_USD", eur)
    gbp_result = _analyze_pair("GBP_USD", gbp)

    combined_archetypes = _merge_archetypes(
        eur_result.get("failure_archetypes", []),
        gbp_result.get("failure_archetypes", []),
    )

    top_leaks = combined_archetypes[:3]
    recommendations = []
    for leak in top_leaks:
        name = leak.get("name")
        if name == "selected_below_rejected_score":
            recommendations.append(
                "Verify timestamp-direction selection join fidelity and inspect cycle-level tie-breaking where selected score trails rejected score."
            )
        elif name == "high_rank_slow_recycler":
            recommendations.append(
                "Analyze high-rank but slow-recycling cohorts by context/day and check whether ranking overweights persistence over velocity."
            )
        elif name == "prolonged_drawdown_selected":
            recommendations.append(
                "Segment prolonged-drawdown selected trades by grade and rank to isolate whether specific rank bands absorb excessive adverse time."
            )
        elif name == "long_peak_to_close_delay":
            recommendations.append(
                "Inspect exit timing delay after peak for selected trades by rank grade to identify over-hold patterns without changing thresholds."
            )
        elif name == "slow_time_to_first_profit":
            recommendations.append(
                "Compare slow first-profit selected trades against fast cohorts by rank and score band to identify selection lag signatures."
            )

    if not recommendations:
        recommendations = [
            "No dominant leak count exceeded zero in current sample; expand cycle coverage window and re-run pattern analysis before tuning.",
        ]

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE10_PRIORITY_FAILURE_PATTERN_ANALYSIS_NO_TUNING",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
        },
        "dependency": {
            "phase9_status": phase9.get("status"),
            "phase9_path": str(PHASE9_PATH),
        },
        "pair_analyses": {
            "EUR_USD": eur_result,
            "GBP_USD": gbp_result,
        },
        "combined_failure_archetypes": combined_archetypes,
        "top_selection_leaks": top_leaks,
        "non_tuning_diagnosis_recommendations": recommendations,
        "pass_conditions": {
            "phase9_dependency_passed": phase9.get("status") == "PASS",
            "rank_vs_outcome_tables_present": bool(eur_result.get("rank_vs_outcome_table")) and bool(gbp_result.get("rank_vs_outcome_table")),
            "selected_vs_rejected_comparisons_present": bool(eur_result.get("selected_vs_rejected_comparison")) and bool(gbp_result.get("selected_vs_rejected_comparison")),
            "distribution_blocks_present": (
                bool(eur_result.get("trade_life_distribution_by_rank_grade"))
                and bool(gbp_result.get("trade_life_distribution_by_rank_grade"))
            ),
            "top_archetypes_present": len(combined_archetypes) > 0,
            "no_tuning_applied": True,
        },
    }

    report["status"] = "PASS" if all(report["pass_conditions"].values()) else "FAIL"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"wrote {OUTPUT_PATH}")
    print(json.dumps({"status": report["status"], "top_selection_leaks": top_leaks[:3]}, indent=2))


if __name__ == "__main__":
    main()
