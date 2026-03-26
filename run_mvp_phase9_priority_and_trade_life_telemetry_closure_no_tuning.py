#!/usr/bin/env python3
"""Build MVP Phase 9 priority and trade-life telemetry closure artifact (no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

EUR_RUN_PATH = Path("control/mvp_phase9_runtime_eur_usd.json")
GBP_RUN_PATH = Path("control/mvp_phase9_runtime_gbp_usd.json")
EUR_TELE_PATH = Path("control/mvp_phase9_runtime_eur_usd_telemetry.json")
GBP_TELE_PATH = Path("control/mvp_phase9_runtime_gbp_usd_telemetry.json")
PHASE8_PATH = Path("control/mvp_phase8_decay_source_attribution_no_tuning.json")
OUTPUT_PATH = Path("control/mvp_phase9_priority_and_trade_life_telemetry_closure_no_tuning.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def _priority_stats(tele: Dict[str, Any]) -> Dict[str, Any]:
    p = tele.get("priority_telemetry", {})
    cycles = p.get("cycles", []) if isinstance(p, dict) else []
    selected_in_top = sum(1 for c in cycles if c.get("selected_present_in_top_n"))
    return {
        "available": bool(p.get("available")),
        "cycle_count": int(p.get("cycle_count_evaluated", 0) or 0),
        "top_n": int(p.get("top_n", 0) or 0),
        "selected_count": int(p.get("selected_count", 0) or 0),
        "avg_selected_rank": p.get("avg_selected_rank"),
        "median_selected_rank": p.get("median_selected_rank"),
        "selected_present_top_n_rate": _safe_div(float(selected_in_top), float(len(cycles))) if cycles else 0.0,
        "field_presence": {
            "priority_score": bool(cycles and cycles[0].get("top_ranked_candidates") and "priority_score" in cycles[0]["top_ranked_candidates"][0]),
            "rank": bool(cycles and cycles[0].get("top_ranked_candidates") and "rank" in cycles[0]["top_ranked_candidates"][0]),
            "selected": bool(cycles and cycles[0].get("top_ranked_candidates") and "selected" in cycles[0]["top_ranked_candidates"][0]),
        },
        "reason": p.get("reason", ""),
    }


def _lifecycle_stats(tele: Dict[str, Any]) -> Dict[str, Any]:
    s = tele.get("trade_lifecycle_summary", {})
    samples = tele.get("trade_lifecycle_samples", [])
    has_fields = False
    if samples:
        first = samples[0]
        has_fields = all(
            k in first
            for k in [
                "entry_timestamp",
                "close_timestamp",
                "trade_life_seconds",
                "time_to_first_profit_seconds",
                "time_in_drawdown_seconds",
                "time_from_entry_to_close_seconds",
                "time_from_peak_to_close_seconds",
            ]
        )

    return {
        "summary": {
            "avg_trade_life_seconds": s.get("avg_trade_life_seconds"),
            "median_trade_life_seconds": s.get("median_trade_life_seconds"),
            "avg_time_to_first_profit_seconds": s.get("avg_time_to_first_profit_seconds"),
            "median_time_to_first_profit_seconds": s.get("median_time_to_first_profit_seconds"),
            "avg_time_in_drawdown_seconds": s.get("avg_time_in_drawdown_seconds"),
            "median_time_in_drawdown_seconds": s.get("median_time_in_drawdown_seconds"),
            "avg_time_from_peak_to_close_seconds": s.get("avg_time_from_peak_to_close_seconds"),
            "median_time_from_peak_to_close_seconds": s.get("median_time_from_peak_to_close_seconds"),
        },
        "sample_count": len(samples),
        "required_fields_present_in_sample": has_fields,
    }


def main() -> None:
    p8 = _load_json(PHASE8_PATH)
    eur_run = _load_json(EUR_RUN_PATH)
    gbp_run = _load_json(GBP_RUN_PATH)
    eur_tele = _load_json(EUR_TELE_PATH)
    gbp_tele = _load_json(GBP_TELE_PATH)

    eur_priority = _priority_stats(eur_tele)
    gbp_priority = _priority_stats(gbp_tele)
    eur_life = _lifecycle_stats(eur_tele)
    gbp_life = _lifecycle_stats(gbp_tele)

    combined = {
        "runtime_net_pph": {
            "EUR_USD": float(eur_run.get("combined_keep_tune_net_pph", 0.0)),
            "GBP_USD": float(gbp_run.get("combined_keep_tune_net_pph", 0.0)),
        },
        "priority": {
            "avg_selected_rank_mean": (
                _safe_div(
                    float(eur_priority.get("avg_selected_rank") or 0.0)
                    + float(gbp_priority.get("avg_selected_rank") or 0.0),
                    2.0,
                )
            ),
            "selected_present_top_n_rate_mean": (
                _safe_div(
                    float(eur_priority.get("selected_present_top_n_rate", 0.0))
                    + float(gbp_priority.get("selected_present_top_n_rate", 0.0),),
                    2.0,
                )
            ),
        },
        "trade_life": {
            "avg_trade_life_seconds_mean": _safe_div(
                float(eur_life["summary"].get("avg_trade_life_seconds") or 0.0)
                + float(gbp_life["summary"].get("avg_trade_life_seconds") or 0.0),
                2.0,
            ),
            "avg_time_to_first_profit_seconds_mean": _safe_div(
                float(eur_life["summary"].get("avg_time_to_first_profit_seconds") or 0.0)
                + float(gbp_life["summary"].get("avg_time_to_first_profit_seconds") or 0.0),
                2.0,
            ),
            "avg_time_in_drawdown_seconds_mean": _safe_div(
                float(eur_life["summary"].get("avg_time_in_drawdown_seconds") or 0.0)
                + float(gbp_life["summary"].get("avg_time_in_drawdown_seconds") or 0.0),
                2.0,
            ),
        },
    }

    pass_conditions = {
        "phase8_dependency_passed": p8.get("status") == "PASS",
        "priority_telemetry_available_eur": bool(eur_priority.get("available")),
        "priority_telemetry_available_gbp": bool(gbp_priority.get("available")),
        "priority_fields_present_eur": all(bool(v) for v in eur_priority.get("field_presence", {}).values()),
        "priority_fields_present_gbp": all(bool(v) for v in gbp_priority.get("field_presence", {}).values()),
        "trade_life_fields_present_eur": bool(eur_life.get("required_fields_present_in_sample")),
        "trade_life_fields_present_gbp": bool(gbp_life.get("required_fields_present_in_sample")),
        "capital_efficiency_proxies_present": (
            eur_life["summary"].get("avg_time_to_first_profit_seconds") is not None
            and gbp_life["summary"].get("avg_time_to_first_profit_seconds") is not None
            and eur_life["summary"].get("avg_time_in_drawdown_seconds") is not None
            and gbp_life["summary"].get("avg_time_in_drawdown_seconds") is not None
        ),
        "aee_timing_present": (
            eur_life["summary"].get("avg_time_from_peak_to_close_seconds") is not None
            and gbp_life["summary"].get("avg_time_from_peak_to_close_seconds") is not None
        ),
        "no_tuning_applied": True,
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE9_PRIORITY_AND_TRADE_LIFE_TELEMETRY_CLOSURE_NO_TUNING",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
        },
        "dependency": {
            "phase8_status": p8.get("status"),
            "phase8_path": str(PHASE8_PATH),
        },
        "telemetry_closure": {
            "EUR_USD": {
                "priority": eur_priority,
                "trade_lifecycle": eur_life,
            },
            "GBP_USD": {
                "priority": gbp_priority,
                "trade_lifecycle": gbp_life,
            },
            "combined_summary": combined,
        },
        "pass_conditions": pass_conditions,
    }
    report["status"] = "PASS" if all(pass_conditions.values()) else "FAIL"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"wrote {OUTPUT_PATH}")
    print(json.dumps({"status": report["status"]}, indent=2))


if __name__ == "__main__":
    main()
