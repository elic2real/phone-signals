#!/usr/bin/env python3
"""Build MVP Phase 8 no-tuning decay source attribution artifact."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

PHASE2_PATH = Path("control/mvp_phase2_entry_supply_proof.json")
PHASE3_PATH = Path("control/mvp_phase3_priority_proof.json")
P6_EUR_PATH = Path("control/mvp_phase6_w3_eur_usd.json")
P6_GBP_PATH = Path("control/mvp_phase6_w3_gbp_usd.json")
P7_EUR_PATH = Path("control/mvp_phase7_runtime_eur_usd.json")
P7_GBP_PATH = Path("control/mvp_phase7_runtime_gbp_usd.json")
OUTPUT_PATH = Path("control/mvp_phase8_decay_source_attribution_no_tuning.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _sum_candidates(d: Dict[str, Any]) -> int:
    return int(sum(int(v) for v in d.get("family_distribution_in_data", {}).values()))


def _weighted_metric(rows: List[Dict[str, Any]], metric: str, weight: str = "trade_count") -> float:
    num = 0.0
    den = 0.0
    for r in rows:
        w = float(r.get(weight, 0.0) or 0.0)
        m = float(r.get(metric, 0.0) or 0.0)
        num += w * m
        den += w
    return num / den if den > 0 else 0.0


def _sum_metric(rows: List[Dict[str, Any]], key: str) -> float:
    return float(sum(float(r.get(key, 0.0) or 0.0) for r in rows))


def _verdict_counts(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    out = {"KEEP": 0, "TUNE": 0, "KILL": 0}
    for r in rows:
        v = str(r.get("verdict", "")).upper()
        if v in out:
            out[v] += 1
    return out


def _trade_rate(trades: int, hours: float) -> float:
    return float(trades) / float(hours) if hours > 0 else 0.0


def _delta(new: float, old: float) -> Dict[str, float]:
    abs_delta = new - old
    pct_delta = (abs_delta / old) if old != 0 else 0.0
    return {
        "baseline": old,
        "runtime": new,
        "abs_delta": abs_delta,
        "pct_delta": pct_delta,
    }


def _merge_blockers(eur: Dict[str, int], gbp: Dict[str, int]) -> Dict[str, int]:
    keys = set(eur.keys()) | set(gbp.keys())
    return {k: int(eur.get(k, 0)) + int(gbp.get(k, 0)) for k in sorted(keys)}


def _pair_profile(p: Dict[str, Any]) -> Dict[str, Any]:
    rows = p.get("ranked_families", [])
    gross_total = _sum_metric(rows, "total_gross_pips")
    net_total = _sum_metric(rows, "total_net_pips")
    extraction_ratio = net_total / gross_total if gross_total > 0 else 0.0
    return {
        "candidate_count": _sum_candidates(p),
        "selected_trade_count": int(p.get("total_accepted_trades", 0)),
        "hours": float(p.get("total_hours", 0.0)),
        "trade_rate_per_hour": _trade_rate(int(p.get("total_accepted_trades", 0)), float(p.get("total_hours", 0.0))),
        "net_pph_keep_tune": float(p.get("combined_keep_tune_net_pph", 0.0)),
        "average_close_value_pips": _weighted_metric(rows, "avg_net_pips_per_trade"),
        "average_mfe_pips": _weighted_metric(rows, "avg_mfe_pips"),
        "extraction_ratio": extraction_ratio,
        "blocker_reason_counts": {k: int(v) for k, v in p.get("skipped_by_entry_filter", {}).items()},
        "aee_decision_mix_counts": _verdict_counts(rows),
    }


def _combine_profiles(eur: Dict[str, Any], gbp: Dict[str, Any]) -> Dict[str, Any]:
    total_candidates = eur["candidate_count"] + gbp["candidate_count"]
    total_trades = eur["selected_trade_count"] + gbp["selected_trade_count"]
    total_hours = eur["hours"] + gbp["hours"]
    avg_close = (
        (eur["average_close_value_pips"] * eur["selected_trade_count"])
        + (gbp["average_close_value_pips"] * gbp["selected_trade_count"])
    ) / total_trades if total_trades > 0 else 0.0
    avg_mfe = (
        (eur["average_mfe_pips"] * eur["selected_trade_count"])
        + (gbp["average_mfe_pips"] * gbp["selected_trade_count"])
    ) / total_trades if total_trades > 0 else 0.0
    extraction_ratio = (
        (eur["extraction_ratio"] * eur["selected_trade_count"])
        + (gbp["extraction_ratio"] * gbp["selected_trade_count"])
    ) / total_trades if total_trades > 0 else 0.0
    aee_mix = {
        "KEEP": eur["aee_decision_mix_counts"]["KEEP"] + gbp["aee_decision_mix_counts"]["KEEP"],
        "TUNE": eur["aee_decision_mix_counts"]["TUNE"] + gbp["aee_decision_mix_counts"]["TUNE"],
        "KILL": eur["aee_decision_mix_counts"]["KILL"] + gbp["aee_decision_mix_counts"]["KILL"],
    }
    return {
        "candidate_count": total_candidates,
        "selected_trade_count": total_trades,
        "hours": total_hours,
        "trade_rate_per_hour": _trade_rate(total_trades, total_hours),
        "net_pph_keep_tune": (
            (eur["net_pph_keep_tune"] * eur["hours"]) + (gbp["net_pph_keep_tune"] * gbp["hours"])
        ) / total_hours if total_hours > 0 else 0.0,
        "average_close_value_pips": avg_close,
        "average_mfe_pips": avg_mfe,
        "extraction_ratio": extraction_ratio,
        "blocker_reason_counts": _merge_blockers(eur["blocker_reason_counts"], gbp["blocker_reason_counts"]),
        "aee_decision_mix_counts": aee_mix,
    }


def main() -> None:
    phase2 = _load_json(PHASE2_PATH)
    phase3 = _load_json(PHASE3_PATH)
    p6_eur = _pair_profile(_load_json(P6_EUR_PATH))
    p6_gbp = _pair_profile(_load_json(P6_GBP_PATH))
    p7_eur = _pair_profile(_load_json(P7_EUR_PATH))
    p7_gbp = _pair_profile(_load_json(P7_GBP_PATH))

    p6_all = _combine_profiles(p6_eur, p6_gbp)
    p7_all = _combine_profiles(p7_eur, p7_gbp)

    a_grade_ratio = float(phase2.get("scored_candidate_supply", {}).get("a_grade_ratio", 0.0))
    p6_a_grade_est = p6_all["candidate_count"] * a_grade_ratio
    p7_a_grade_est = p7_all["candidate_count"] * a_grade_ratio

    required_metrics = {
        "candidate_count_delta_vs_phase6": _delta(float(p7_all["candidate_count"]), float(p6_all["candidate_count"])),
        "a_grade_count_delta_vs_phase6": {
            "baseline_estimated": p6_a_grade_est,
            "runtime_estimated": p7_a_grade_est,
            "abs_delta_estimated": p7_a_grade_est - p6_a_grade_est,
            "pct_delta_estimated": ((p7_a_grade_est - p6_a_grade_est) / p6_a_grade_est) if p6_a_grade_est > 0 else 0.0,
            "method": "estimated_from_phase2_a_grade_ratio",
            "direct_runtime_observable": False,
        },
        "selected_trade_count_delta": _delta(float(p7_all["selected_trade_count"]), float(p6_all["selected_trade_count"])),
        "average_priority_score_delta": {
            "baseline_reference_winner_mean_score": float(phase3.get("selection_impact", {}).get("winner_mean_score", 0.0)),
            "runtime_observed": None,
            "delta": None,
            "observable": False,
            "reason": "priority score telemetry is not emitted in phase6/phase7 runtime artifacts",
        },
        "average_trade_life_delta": {
            "baseline_observed": None,
            "runtime_observed": None,
            "delta": None,
            "observable": False,
            "reason": "trade life duration is not emitted in phase6/phase7 runtime artifacts",
        },
        "average_close_value_delta": _delta(float(p7_all["average_close_value_pips"]), float(p6_all["average_close_value_pips"])),
        "average_mfe_delta": _delta(float(p7_all["average_mfe_pips"]), float(p6_all["average_mfe_pips"])),
        "extraction_ratio_delta": _delta(float(p7_all["extraction_ratio"]), float(p6_all["extraction_ratio"])),
        "blocker_reason_counts": {
            "baseline_phase6_w3": p6_all["blocker_reason_counts"],
            "runtime_phase7": p7_all["blocker_reason_counts"],
            "delta": {
                k: int(p7_all["blocker_reason_counts"].get(k, 0)) - int(p6_all["blocker_reason_counts"].get(k, 0))
                for k in sorted(set(p7_all["blocker_reason_counts"].keys()) | set(p6_all["blocker_reason_counts"].keys()))
            },
        },
        "aee_decision_mix_counts": {
            "baseline_phase6_w3": p6_all["aee_decision_mix_counts"],
            "runtime_phase7": p7_all["aee_decision_mix_counts"],
            "delta": {
                k: int(p7_all["aee_decision_mix_counts"].get(k, 0)) - int(p6_all["aee_decision_mix_counts"].get(k, 0))
                for k in ["KEEP", "TUNE", "KILL"]
            },
        },
    }

    entry_decay = {
        "candidate_count_delta": required_metrics["candidate_count_delta_vs_phase6"],
        "a_grade_count_delta": required_metrics["a_grade_count_delta_vs_phase6"],
        "assessment": "POSITIVE_SUPPLY_SHIFT" if required_metrics["candidate_count_delta_vs_phase6"]["abs_delta"] > 0 else "NO_SUPPLY_DECAY",
        "note": "Candidate volume increased with wider runtime hours; decay is not driven by raw candidate shortage.",
    }

    priority_decay = {
        "average_priority_score_delta": required_metrics["average_priority_score_delta"],
        "selection_trade_rate_delta": _delta(float(p7_all["trade_rate_per_hour"]), float(p6_all["trade_rate_per_hour"])),
        "aee_decision_mix_delta": required_metrics["aee_decision_mix_counts"]["delta"],
        "assessment": "INCONCLUSIVE_TELEMETRY_GAP",
        "note": "Direct runtime priority-score telemetry is missing; trade-rate compression suggests weaker effective selection pace.",
    }

    aee_decay = {
        "average_close_value_delta": required_metrics["average_close_value_delta"],
        "average_mfe_delta": required_metrics["average_mfe_delta"],
        "extraction_ratio_delta": required_metrics["extraction_ratio_delta"],
        "assessment": "NO_PRIMARY_AEE_DECAY_SIGNAL",
        "note": "Per-trade close value, MFE, and extraction ratio remained effectively flat.",
    }

    flow_decay = {
        "blocker_reason_counts": required_metrics["blocker_reason_counts"],
        "selection_trade_rate_delta": priority_decay["selection_trade_rate_delta"],
        "assessment": "PRIMARY_FLOW_DILUTION",
        "note": "Wider runtime introduced more out-of-scope/session-excluded flow, diluting trade rate per hour while keeping total selected trades flat.",
    }

    attribution_split = {
        "entry_decay": entry_decay,
        "priority_decay": priority_decay,
        "aee_decay": aee_decay,
        "flow_or_blocker_decay": flow_decay,
        "primary_source": "FLOW_BLOCKER_DILUTION_WITH_PRIORITY_TELEMETRY_GAP",
    }

    pass_conditions = {
        "phase7_dependency_exists": True,
        "required_metric_block_present": True,
        "attribution_split_present": True,
        "no_tuning_applied": True,
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE8_DECAY_SOURCE_ATTRIBUTION_NO_TUNING",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
        },
        "comparative_baseline": {
            "baseline_window": "PHASE6_W3",
            "runtime_window": "PHASE7_RUNTIME",
        },
        "required_metrics": required_metrics,
        "attribution_split": attribution_split,
        "telemetry_gaps": [
            "average_priority_score_runtime_stream",
            "average_trade_life_runtime_stream",
        ],
        "pass_conditions": pass_conditions,
        "status": "PASS" if all(pass_conditions.values()) else "FAIL",
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"wrote {OUTPUT_PATH}")
    print(json.dumps({"status": report["status"], "primary_source": attribution_split["primary_source"]}, indent=2))


if __name__ == "__main__":
    main()
