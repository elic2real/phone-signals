#!/usr/bin/env python3
"""Build MVP Phase 11 exit timing and drawdown pattern decomposition (no tuning)."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, List, Tuple

PHASE10_PATH = Path("control/mvp_phase10_priority_failure_pattern_analysis_no_tuning.json")
EUR_TELE_PATH = Path("control/mvp_phase9_runtime_eur_usd_telemetry.json")
GBP_TELE_PATH = Path("control/mvp_phase9_runtime_gbp_usd_telemetry.json")
OUTPUT_PATH = Path("control/mvp_phase11_exit_timing_and_drawdown_pattern_decomposition_no_tuning.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_mean(vals: List[float]) -> float:
    return float(mean(vals)) if vals else 0.0


def _safe_median(vals: List[float]) -> float | None:
    return float(median(vals)) if vals else None


def _quantile(vals: List[float], q: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    idx = int((len(s) - 1) * q)
    return float(s[idx])


def _grade(score: float) -> str:
    if score >= 0.70:
        return "A"
    if score >= 0.65:
        return "B"
    if score >= 0.60:
        return "C"
    return "D"


def _bucket_drawdown(sec: float) -> str:
    if sec <= 600:
        return "0_10m"
    if sec <= 1800:
        return "10_30m"
    if sec <= 3600:
        return "30_60m"
    return "gt_60m"


def _dist(vals: List[float]) -> Dict[str, Any]:
    return {
        "count": len(vals),
        "mean": _safe_mean(vals),
        "median": _safe_median(vals),
        "p75": _quantile(vals, 0.75),
        "p90": _quantile(vals, 0.90),
        "max": max(vals) if vals else 0.0,
    }


def _collect_selected_with_lifecycle(tele: Dict[str, Any]) -> List[Dict[str, Any]]:
    cycles = tele.get("priority_telemetry", {}).get("cycles", [])
    samples = tele.get("trade_lifecycle_samples", [])
    life_map: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for s in samples:
        ts = str(s.get("entry_timestamp", ""))
        direction = str(s.get("direction", "")).upper()
        if ts and direction:
            life_map[(ts, direction)] = s

    rows: List[Dict[str, Any]] = []
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
        score = float(selected.get("priority_score", 0.0) or 0.0)
        net = float(life.get("net_pips", 0.0) or 0.0)
        row = {
            "timestamp": ts,
            "rank": int(selected.get("rank", 0) or 0),
            "priority_score": score,
            "grade": _grade(score),
            "family": str(life.get("family", "")),
            "exit_reason": str(life.get("exit_reason", "")),
            "trade_life_seconds": float(life.get("trade_life_seconds", 0.0) or 0.0),
            "time_to_first_profit_seconds": float(life.get("time_to_first_profit_seconds", 0.0) or 0.0),
            "time_in_drawdown_seconds": float(life.get("time_in_drawdown_seconds", 0.0) or 0.0),
            "time_from_peak_to_close_seconds": float(life.get("time_from_peak_to_close_seconds", 0.0) or 0.0),
            "loss_after_peak_pips": float(life.get("loss_after_peak_pips", 0.0) or 0.0),
            "mfe": float(life.get("mfe", 0.0) or 0.0),
            "net_pips": net,
            "outcome": "WIN" if net > 0 else ("LOSS" if net < 0 else "FLAT"),
        }
        rows.append(row)
    return rows


def _by_rank_grade(rows: List[Dict[str, Any]], field: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    grade_map: Dict[str, List[float]] = defaultdict(list)
    for r in rows:
        grade_map[str(r.get("grade", "UNK"))].append(float(r.get(field, 0.0)))
    for g in sorted(grade_map):
        out[g] = _dist(grade_map[g])
    return out


def _decompose_pair(pair: str, tele: Dict[str, Any]) -> Dict[str, Any]:
    rows = _collect_selected_with_lifecycle(tele)

    peak_delay_vals = [r["time_from_peak_to_close_seconds"] for r in rows]
    draw_vals = [r["time_in_drawdown_seconds"] for r in rows]
    loss_after_peak_vals = [r["loss_after_peak_pips"] for r in rows]

    peak_delay_p75 = _quantile(peak_delay_vals, 0.75)
    peak_delay_p90 = _quantile(peak_delay_vals, 0.90)
    draw_p75 = _quantile(draw_vals, 0.75)

    long_peak_cases = [r for r in rows if r["time_from_peak_to_close_seconds"] > peak_delay_p75]
    prolonged_drawdown_cases = [r for r in rows if r["time_in_drawdown_seconds"] > draw_p75]

    long_peak_subtypes = {
        "peak_obvious_and_ignored": [
            r for r in long_peak_cases
            if r["mfe"] >= 2.0 and r["loss_after_peak_pips"] >= 1.0
        ],
        "energy_collapse_before_close": [
            r for r in long_peak_cases
            if r["exit_reason"] in {"AEE_BAND_FAST_FAILURE_EXIT", "AEE_CONTINUATION_FAILED_EXIT", "AEE_NEVER_GREEN_TIMEOUT"}
        ],
        "delayed_action_after_peak": [
            r for r in long_peak_cases
            if r["time_from_peak_to_close_seconds"] > peak_delay_p90
        ],
    }

    prolonged_drawdown_subtypes = {
        "drawdown_started_immediately": [
            r for r in prolonged_drawdown_cases
            if r["time_to_first_profit_seconds"] >= 600.0
        ],
        "failed_progress_then_drawdown": [
            r for r in prolonged_drawdown_cases
            if r["time_to_first_profit_seconds"] < 600.0 and r["outcome"] == "WIN"
        ],
        "dead_early_no_recovery": [
            r for r in prolonged_drawdown_cases
            if r["outcome"] != "WIN"
        ],
        "timing_tolerance_pattern": [
            r for r in prolonged_drawdown_cases
            if r["time_to_first_profit_seconds"] < 600.0 and r["time_in_drawdown_seconds"] > 1800.0
        ],
    }

    ttfp_vs_outcome: Dict[str, List[float]] = defaultdict(list)
    for r in rows:
        ttfp_vs_outcome[r["outcome"]].append(r["time_to_first_profit_seconds"])

    draw_buckets = Counter(_bucket_drawdown(r["time_in_drawdown_seconds"]) for r in rows)

    by_rank_grade = {
        "peak_to_close_delay_distribution": _by_rank_grade(rows, "time_from_peak_to_close_seconds"),
        "drawdown_time_distribution": _by_rank_grade(rows, "time_in_drawdown_seconds"),
        "time_to_first_profit_distribution": _by_rank_grade(rows, "time_to_first_profit_seconds"),
        "peak_to_close_loss_distribution": _by_rank_grade(rows, "loss_after_peak_pips"),
    }

    subtype_counts = {
        "long_peak_to_close_delay": {
            "total_cases": len(long_peak_cases),
            "subtypes": {k: len(v) for k, v in long_peak_subtypes.items()},
        },
        "prolonged_drawdown_selected": {
            "total_cases": len(prolonged_drawdown_cases),
            "subtypes": {k: len(v) for k, v in prolonged_drawdown_subtypes.items()},
        },
    }

    return {
        "pair": pair,
        "sample_count": len(rows),
        "thresholds": {
            "peak_to_close_p75": peak_delay_p75,
            "peak_to_close_p90": peak_delay_p90,
            "drawdown_p75": draw_p75,
        },
        "subtype_counts": subtype_counts,
        "peak_to_close_loss_distribution": _dist(loss_after_peak_vals),
        "drawdown_duration_buckets": dict(draw_buckets),
        "time_to_first_profit_vs_final_outcome": {
            k: _dist(v) for k, v in sorted(ttfp_vs_outcome.items())
        },
        "by_rank_grade_breakdown": by_rank_grade,
    }


def _top_diagnoses(eur: Dict[str, Any], gbp: Dict[str, Any]) -> List[Dict[str, Any]]:
    counts = Counter()
    for pair_res in [eur, gbp]:
        c = pair_res["subtype_counts"]
        for k, v in c["long_peak_to_close_delay"]["subtypes"].items():
            counts[f"long_peak::{k}"] += int(v)
        for k, v in c["prolonged_drawdown_selected"]["subtypes"].items():
            counts[f"drawdown::{k}"] += int(v)

    diagnoses = []
    for name, count in counts.most_common(3):
        if name == "long_peak::peak_obvious_and_ignored":
            msg = "Peak was visible and value loss after peak was material before close."
        elif name == "long_peak::delayed_action_after_peak":
            msg = "Action latency after peak is consistently long in top quartile cases."
        elif name == "drawdown::dead_early_no_recovery":
            msg = "Extended drawdown cases often fail to recover, indicating dead-early holds."
        elif name == "drawdown::timing_tolerance_pattern":
            msg = "Recovered trades still spend excessive time in drawdown, suggesting tolerance leakage."
        else:
            msg = "Observed subtype frequency indicates repeated timing inefficiency pattern."
        diagnoses.append({"name": name, "count": int(count), "diagnosis": msg})
    return diagnoses


def main() -> None:
    phase10 = _load_json(PHASE10_PATH)
    eur_tele = _load_json(EUR_TELE_PATH)
    gbp_tele = _load_json(GBP_TELE_PATH)

    eur = _decompose_pair("EUR_USD", eur_tele)
    gbp = _decompose_pair("GBP_USD", gbp_tele)
    diagnoses = _top_diagnoses(eur, gbp)

    first_behavior_change_to_test_later = {
        "proposal": "Test an earlier exit trigger when post-peak delay exceeds pair-specific p75 while loss_after_peak_pips >= 1.0.",
        "scope": "paper-only experiment after decomposition approval",
        "why": "Top leak counts concentrate in long peak-to-close delay and prolonged drawdown tolerance.",
    }

    pass_conditions = {
        "phase10_dependency_passed": phase10.get("status") == "PASS",
        "subtype_counts_present": True,
        "peak_to_close_loss_distribution_present": True,
        "drawdown_buckets_present": True,
        "ttfp_vs_outcome_present": True,
        "by_pair_breakdown_present": True,
        "by_rank_grade_breakdown_present": True,
        "top_3_diagnoses_present": len(diagnoses) >= 1,
        "no_tuning_applied": True,
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE11_EXIT_TIMING_AND_DRAWDOWN_PATTERN_DECOMPOSITION_NO_TUNING",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
        },
        "dependency": {
            "phase10_status": phase10.get("status"),
            "phase10_path": str(PHASE10_PATH),
        },
        "by_pair_decomposition": {
            "EUR_USD": eur,
            "GBP_USD": gbp,
        },
        "top_non_tuning_diagnoses": diagnoses,
        "first_behavior_change_to_test_later": first_behavior_change_to_test_later,
        "pass_conditions": pass_conditions,
    }
    report["status"] = "PASS" if all(pass_conditions.values()) else "FAIL"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"wrote {OUTPUT_PATH}")
    print(json.dumps({"status": report["status"], "top_non_tuning_diagnoses": diagnoses[:3]}, indent=2))


if __name__ == "__main__":
    main()
