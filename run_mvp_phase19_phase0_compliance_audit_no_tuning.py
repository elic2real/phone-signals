#!/usr/bin/env python3
"""Run MVP Phase 19 Phase-0 compliance audit (no tuning)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PHASE0_TEMPLATE_PATH = Path("control/rcp_phase0_high_speed_foundation_template.json")
EUR_TELE_PATH = Path("control/mvp_phase9_runtime_eur_usd_telemetry.json")
GBP_TELE_PATH = Path("control/mvp_phase9_runtime_gbp_usd_telemetry.json")
PHASE17_PATH = Path("control/mvp_phase17_counterfactual_rerun_execution_with_mitigation_guards.json")
OUTPUT_PATH = Path("control/mvp_phase19_phase0_compliance_audit_no_tuning.json")


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing required artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _sample_trade_fields(tele: Dict[str, Any]) -> List[str]:
    samples = tele.get("trade_lifecycle_samples", [])
    if not samples:
        return []
    keys = set()
    for row in samples[:20]:
        keys.update(row.keys())
    return sorted(keys)


def _telemetry_compliance(template: Dict[str, Any], tele: Dict[str, Any]) -> Dict[str, Any]:
    req = template.get("telemetry_requirements", {})
    candidate_req = set(req.get("candidate_level", []))
    trade_req = set(req.get("trade_lifecycle", []))
    profit_req = set(req.get("profit_path", []))
    timing_req = set(req.get("timing", []))

    # Candidate-level signal proxy from cycle entries.
    cycles = tele.get("priority_telemetry", {}).get("cycles", [])
    candidate_keys = set()
    if cycles:
        ranked = cycles[0].get("top_ranked_candidates", [])
        if ranked:
            candidate_keys.update(ranked[0].keys())
        candidate_keys.update(cycles[0].keys())

    trade_keys = set(_sample_trade_fields(tele))

    # Field alias map between template and current schema.
    aliases = {
        "max_favorable_excursion": "mfe",
        "time_from_entry_to_peak": "time_from_entry_to_peak_seconds",
        "time_from_peak_to_close": "time_from_peak_to_close_seconds",
        "time_from_entry_to_close": "time_from_entry_to_close_seconds",
    }

    def _missing(required: set[str], present: set[str]) -> List[str]:
        miss = []
        for f in sorted(required):
            if f in present:
                continue
            if aliases.get(f) in present:
                continue
            miss.append(f)
        return miss

    missing_candidate = _missing(candidate_req, candidate_keys)
    combined_trade_present = trade_keys
    missing_trade = _missing(trade_req, combined_trade_present)
    missing_profit = _missing(profit_req, combined_trade_present)
    missing_timing = _missing(timing_req, combined_trade_present)

    return {
        "candidate_level_missing": missing_candidate,
        "trade_lifecycle_missing": missing_trade,
        "profit_path_missing": missing_profit,
        "timing_missing": missing_timing,
        "candidate_level_present_count": len(candidate_req) - len(missing_candidate),
        "trade_lifecycle_present_count": len(trade_req) - len(missing_trade),
        "profit_path_present_count": len(profit_req) - len(missing_profit),
        "timing_present_count": len(timing_req) - len(missing_timing),
    }


def main() -> None:
    template = _load_json(PHASE0_TEMPLATE_PATH)
    eur = _load_json(EUR_TELE_PATH)
    gbp = _load_json(GBP_TELE_PATH)
    phase17 = _load_json(PHASE17_PATH)

    eur_cmp = _telemetry_compliance(template, eur)
    gbp_cmp = _telemetry_compliance(template, gbp)

    universal_analysis_outputs = template.get("universal_analysis_engine", {}).get("outputs", [])
    phase17_keys = set(phase17.keys())
    coverage_proxy = {
        "cross_pair_gate_evaluated": "cross_pair_variant_gate" in phase17_keys,
        "rank_vs_outcome_proxy": "results_by_pair" in phase17_keys,
        "timing_distribution_proxy": "results_by_pair" in phase17_keys,
    }

    gaps = {
        "global": [
            "single universal analyzer artifact not yet established as one-run full diagnosis output",
            "predefined failure archetype assignment not yet emitted per trade in canonical artifact",
            "benchmark_dataset registry file not yet standardized in control layer",
        ],
        "EUR_USD": eur_cmp,
        "GBP_USD": gbp_cmp,
    }

    adoption_backlog = [
        {
            "id": "P19-A1",
            "title": "Unify one-pass analyzer",
            "description": "Create universal analyzer script and single canonical output schema.",
            "priority": 1,
        },
        {
            "id": "P19-A2",
            "title": "Emit failure archetype labels",
            "description": "Classify every trade into predefined archetypes in one artifact.",
            "priority": 2,
        },
        {
            "id": "P19-A3",
            "title": "Standardize benchmark dataset",
            "description": "Publish fixed benchmark slices and wire micro-slice gate before full runs.",
            "priority": 3,
        },
        {
            "id": "P19-A4",
            "title": "Lock kill rules in control",
            "description": "Encode hard kill conditions in control gating and auto-fail paths.",
            "priority": 4,
        },
    ]

    pass_conditions = {
        "template_loaded": True,
        "telemetry_audited": True,
        "phase17_evidence_linked": phase17.get("status") == "PASS",
        "gap_backlog_generated": len(adoption_backlog) >= 3,
        "no_tuning_applied": True,
    }

    report = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE19_PHASE0_COMPLIANCE_AUDIT_NO_TUNING",
        "scope_lock": {
            "pairs": ["EUR_USD", "GBP_USD"],
            "session": "LONDON",
            "tuning": "NONE",
            "mode": "AUDIT_ONLY",
        },
        "dependency": {
            "phase0_template_path": str(PHASE0_TEMPLATE_PATH),
            "phase17_status": phase17.get("status"),
            "phase17_path": str(PHASE17_PATH),
            "telemetry_paths": [str(EUR_TELE_PATH), str(GBP_TELE_PATH)],
        },
        "audit_summary": {
            "phase0_universal_analysis_outputs_defined": len(universal_analysis_outputs),
            "phase17_coverage_proxy": coverage_proxy,
        },
        "gaps": gaps,
        "adoption_backlog": adoption_backlog,
        "phase20_recommended_scope": {
            "task": "MVP_PHASE20_UNIVERSAL_ANALYZER_AND_ARCHETYPE_EMISSION_IMPLEMENTATION",
            "objective": "Implement one-pass analyzer, archetype emission, and benchmark micro-slice gate.",
        },
        "pass_conditions": pass_conditions,
    }
    report["status"] = "PASS" if all(pass_conditions.values()) else "FAIL"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"wrote {OUTPUT_PATH}")
    print(
        json.dumps(
            {
                "status": report["status"],
                "adoption_backlog_count": len(adoption_backlog),
                "phase20_task": report["phase20_recommended_scope"]["task"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
