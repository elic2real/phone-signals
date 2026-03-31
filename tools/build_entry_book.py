from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

WORKSPACE = Path(__file__).resolve().parent.parent
DEFAULT_TRIAGE_MASTER = WORKSPACE / "control" / "universal_triage" / "universal_triage_master.json"
DEFAULT_OUT_DIR = WORKSPACE / "control" / "entry_book"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return read_json(path)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def assign_role(entry: dict, proof: dict, triage_row: dict) -> str:
    expectancy = entry.get("setup_expectancy")
    density = triage_row.get("D_raw")
    stability_pass = bool(proof.get("stability_pass"))
    trigger_quality = entry.get("trigger_quality_score")

    if stability_pass and isinstance(expectancy, (int, float)) and expectancy >= 2.0:
        return "ANCHOR"

    if isinstance(density, (int, float)) and density >= 10.0 and isinstance(expectancy, (int, float)) and expectancy >= 1.0:
        return "THROUGHPUT"

    if isinstance(trigger_quality, (int, float)) and trigger_quality >= 0.7:
        return "SPECIALIST"

    return "SPECIALIST"


def entry_state_policy() -> dict:
    return {
        "WATCH": {
            "description": "Domain-level economics and session preconditions are valid.",
            "must_hold": ["integrity_ok", "tier_allowed", "session_match"],
        },
        "GET_READY": {
            "description": "Host zone and structural setup are formed.",
            "must_hold": ["setup_valid", "structure_valid", "path_family_valid"],
        },
        "ARM": {
            "description": "Trigger preconditions are armed with admissible friction.",
            "must_hold": ["spread_within_budget", "residual_business_positive", "armed_state_reached"],
        },
        "ENTER": {
            "description": "Trigger confirmation event transitions to execute.",
            "must_hold": ["confirmed_state", "execution_band_valid", "latency_budget_ok"],
        },
        "INVALIDATE": {
            "description": "Setup or trigger assumptions broke before/after entry.",
            "must_hold": ["invalid_state_or_expired", "promotion_no_longer_valid_or_structure_break"],
        },
    }


def entry_economics_policy() -> dict:
    return {
        "TIER_1": {
            "min_net_expectancy": 2.0,
            "min_density": 10.0,
            "min_trigger_quality": 0.70,
            "spread_tolerance": "strict",
            "priority": "aggressive",
        },
        "TIER_2": {
            "min_net_expectancy": 1.0,
            "min_density": 8.0,
            "min_trigger_quality": 0.55,
            "spread_tolerance": "normal",
            "priority": "routine",
        },
        "TIER_3": {
            "min_net_expectancy": 0.4,
            "min_density": 5.0,
            "min_trigger_quality": 0.40,
            "spread_tolerance": "conditional",
            "priority": "opportunistic",
        },
    }


def summarize_portfolio_utility(entries: list[dict], proof_rows: list[dict]) -> dict:
    by_tier: dict[str, list[dict]] = {"TIER_1": [], "TIER_2": [], "TIER_3": []}
    by_role: dict[str, int] = {"ANCHOR": 0, "THROUGHPUT": 0, "SPECIALIST": 0}

    for entry in entries:
        tier = entry.get("tier")
        if tier in by_tier:
            by_tier[tier].append(entry)
        role = entry.get("role")
        if role in by_role:
            by_role[role] += 1

    avg_density = None
    densities = [e.get("domain_density") for e in entries if isinstance(e.get("domain_density"), (int, float))]
    if densities:
        avg_density = sum(densities) / len(densities)

    proof_fail_reasons: dict[str, int] = {}
    for row in proof_rows:
        proof = row.get("proof") or {}
        if proof.get("proof_pass"):
            continue
        for reason in proof.get("reason_codes") or []:
            proof_fail_reasons[reason] = proof_fail_reasons.get(reason, 0) + 1

    return {
        "active_entries_total": len(entries),
        "active_by_tier": {k: len(v) for k, v in by_tier.items()},
        "active_by_role": by_role,
        "avg_density_active_entries": avg_density,
        "tier2_or_tier3_throughput_contribution": {
            "count": sum(1 for e in entries if e.get("tier") in {"TIER_2", "TIER_3"} and e.get("role") == "THROUGHPUT"),
            "status": "ADDS_THROUGHPUT" if any(e.get("tier") in {"TIER_2", "TIER_3"} and e.get("role") == "THROUGHPUT" for e in entries) else "NO_ADDITIONAL_THROUGHPUT",
        },
        "proof_fail_reason_histogram": proof_fail_reasons,
    }


def pick_candidates(master_rows: list[dict], top_tier2: int, include_tier3_throughput: int) -> list[dict]:
    tier1 = [r for r in master_rows if r.get("assigned_class") == "TIER_1"]
    tier2 = [r for r in master_rows if r.get("assigned_class") == "TIER_2"]
    tier3 = [r for r in master_rows if r.get("assigned_class") == "TIER_3"]

    tier1.sort(key=lambda r: (r.get("CTS") or -1), reverse=True)
    tier2.sort(key=lambda r: (r.get("CTS") or -1), reverse=True)
    tier3.sort(key=lambda r: ((r.get("D_raw") or -1), (r.get("CTS") or -1)), reverse=True)

    chosen = []
    chosen.extend(tier1)
    chosen.extend(tier2[:top_tier2])
    chosen.extend(tier3[:include_tier3_throughput])

    # de-duplicate by domain_id while preserving order
    seen = set()
    dedup = []
    for row in chosen:
        domain_id = row.get("domain_id")
        if domain_id in seen:
            continue
        seen.add(domain_id)
        dedup.append(row)
    return dedup


def stability_report_path(row: dict) -> Path:
    pair = (row.get("pair") or "").lower()
    session = (row.get("session") or "").lower()
    direction = (row.get("direction") or "").lower()
    return WORKSPACE / "control" / "stability_tests" / f"{pair}_{session}_{direction}_stability.json"


def trigger_validation_report_path(row: dict) -> Path:
    domain_id = row.get("domain_id")
    return (
        WORKSPACE
        / "control"
        / "scale_batch_validation"
        / str(domain_id)
        / "trigger_validation_reports"
        / "trigger_validation_report.json"
    )


def promotion_validation_report_path(row: dict) -> Path:
    domain_id = row.get("domain_id")
    return (
        WORKSPACE
        / "control"
        / "scale_batch_validation"
        / str(domain_id)
        / "setup_phase_reports_promotion"
        / "validation_report.json"
    )


def trigger_truth_path(row: dict) -> Path:
    domain_id = row.get("domain_id")
    return WORKSPACE / "PC2" / "discovery" / "scale_batches" / str(domain_id) / "trigger_truth.json"


def setup_truth_path(row: dict) -> Path:
    domain_id = row.get("domain_id")
    return WORKSPACE / "PC2" / "discovery" / "scale_batches" / str(domain_id) / "setup_truth.json"


def evaluate_proof(row: dict) -> dict:
    reasons: list[str] = []

    stability = read_json_if_exists(stability_report_path(row))
    trigger_val = read_json_if_exists(trigger_validation_report_path(row))
    promotion = read_json_if_exists(promotion_validation_report_path(row))

    stability_pass = False
    if stability:
        stability_pass = bool((stability.get("stability") or {}).get("stable", False))
    else:
        reasons.append("PROOF_MISSING_STABILITY_REPORT")
    if not stability_pass:
        reasons.append("PROOF_STABILITY_FAIL")

    distinctness_status = None
    if trigger_val:
        distinctness_status = (trigger_val.get("sibling_distinctness") or {}).get("status")
    else:
        reasons.append("PROOF_MISSING_TRIGGER_VALIDATION")

    distinctness_pass = distinctness_status in {"DISTINCT", "PASS"}
    if not distinctness_pass:
        reasons.append("PROOF_DISTINCTNESS_FAIL")

    promotion_status = None
    promotion_reason = None
    if promotion:
        promotion_status = promotion.get("status")
        promotion_reason = (promotion.get("promotion_gate") or {}).get("reason")
    else:
        reasons.append("PROOF_MISSING_PROMOTION_VALIDATION")

    promotion_pass = promotion_status == "PASS"
    if not promotion_pass:
        reasons.append("PROOF_PROMOTION_FAIL")

    density_ok = row.get("D_raw") is not None
    if not density_ok:
        reasons.append("PROOF_DENSITY_MISSING")

    # sample-growth collapse proxy from stability summaries if available
    collapse_check_pass = True
    if stability:
        summaries = (stability.get("summaries") or [])
        if summaries:
            base = summaries[0]
            end = summaries[-1]
            base_setups = base.get("setup_count") or 0
            end_setups = end.get("setup_count") or 0
            base_triggers = base.get("trigger_count") or 0
            end_triggers = end.get("trigger_count") or 0
            if base_setups > 0 and end_setups < (0.6 * base_setups):
                collapse_check_pass = False
                reasons.append("PROOF_SETUP_COLLAPSE")
            if base_triggers > 0 and end_triggers < (0.6 * base_triggers):
                collapse_check_pass = False
                reasons.append("PROOF_TRIGGER_COLLAPSE")

    if not collapse_check_pass:
        reasons.append("PROOF_SAMPLE_GROWTH_COLLAPSE")

    proof_pass = stability_pass and distinctness_pass and promotion_pass and density_ok and collapse_check_pass

    return {
        "proof_pass": proof_pass,
        "stability_pass": stability_pass,
        "distinctness_pass": distinctness_pass,
        "promotion_pass": promotion_pass,
        "density_ok": density_ok,
        "collapse_check_pass": collapse_check_pass,
        "distinctness_status": distinctness_status,
        "promotion_status": promotion_status,
        "promotion_reason": promotion_reason,
        "reason_codes": sorted(set(reasons)),
    }


def build_entries_for_row(row: dict) -> list[dict]:
    trigger_truth = read_json_if_exists(trigger_truth_path(row)) or {}
    setup_truth = read_json_if_exists(setup_truth_path(row)) or {}
    setup_by_label = {r.get("setup_label"): r for r in (setup_truth.get("records") or [])}

    entries = []
    for record in (trigger_truth.get("records") or []):
        setup_label = record.get("setup_label")
        setup_obj = setup_by_label.get(setup_label, {})

        entry = {
            "domain_id": row.get("domain_id"),
            "tier": row.get("assigned_class"),
            "pair": row.get("pair"),
            "session": row.get("session"),
            "direction": row.get("direction"),
            "target_bucket": record.get("target_bucket"),
            "setup_label": setup_label,
            "trigger_label": record.get("trigger_label"),
            "trigger_family": record.get("trigger_family"),
            "path_family": record.get("path_family"),
            "structure_label": record.get("structure_label"),
            "entry_zone": record.get("host_zone"),
            "execution_band": record.get("execution_band"),
            "setup_expectancy": setup_obj.get("expectancy"),
            "trigger_quality_score": (record.get("trigger_quality") or {}).get("trigger_quality_score"),
            "invalidation_logic": {
                "max_pre_entry_mae_to_bucket_ratio": (record.get("criteria") or {}).get("max_pre_entry_mae_to_bucket_ratio"),
                "stagnation_hazard_max": (record.get("criteria") or {}).get("stagnation_hazard_max"),
                "zone_residency_sec_max": (record.get("criteria") or {}).get("zone_residency_sec_max"),
                "failed_push_count_max": (record.get("criteria") or {}).get("max_failed_push_count"),
            },
            "state_machine": {
                "WATCH": "domain economic and session preconditions true",
                "GET_READY": "host zone formed and structural setup valid",
                "ARM": "state ARMED reached; spread and residual business admissible",
                "ENTER": "state CONFIRMED -> EXECUTE transition",
                "INVALIDATE": "state INVALID or EXPIRED or promotion constraints violated",
            },
        }
        entries.append(entry)

    return entries


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build proof report and tiered entry book from universal triage survivors.")
    p.add_argument("--triage-master", type=Path, default=DEFAULT_TRIAGE_MASTER)
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    p.add_argument("--top-tier2", type=int, default=2)
    p.add_argument("--include-tier3-throughput", type=int, default=0)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    master_rows = read_json(args.triage_master)

    candidates = pick_candidates(
        master_rows=master_rows,
        top_tier2=args.top_tier2,
        include_tier3_throughput=args.include_tier3_throughput,
    )

    proof_rows = []
    active_entries = []
    excluded_entries = []

    for row in candidates:
        proof = evaluate_proof(row)
        proof_rows.append(
            {
                "domain_id": row.get("domain_id"),
                "pair": row.get("pair"),
                "session": row.get("session"),
                "direction": row.get("direction"),
                "assigned_class": row.get("assigned_class"),
                "next_action_from_triage": row.get("next_action"),
                "proof": proof,
            }
        )
        if proof["proof_pass"]:
            domain_entries = build_entries_for_row(row)
            for entry in domain_entries:
                entry["role"] = assign_role(entry, proof, row)
                entry["domain_density"] = row.get("D_raw")
                entry["domain_cts"] = row.get("CTS")
                entry["domain_next_action"] = row.get("next_action")
                active_entries.append(entry)
        else:
            excluded_entries.append(
                {
                    "domain_id": row.get("domain_id"),
                    "assigned_class": row.get("assigned_class"),
                    "excluded_reason_codes": proof.get("reason_codes") or ["PROOF_FAIL"],
                    "next_action": "REVIEW_OR_RETIRE",
                }
            )

    proof_report = {
        "generated_at_utc": now_utc(),
        "source_triage": str(args.triage_master),
        "candidate_counts": {
            "selected": len(candidates),
            "proof_pass": sum(1 for x in proof_rows if (x.get("proof") or {}).get("proof_pass")),
            "proof_fail": sum(1 for x in proof_rows if not (x.get("proof") or {}).get("proof_pass")),
        },
        "rows": proof_rows,
    }

    entry_book = {
        "generated_at_utc": now_utc(),
        "scope": "POST_TRIAGE_ENTRY_OPERATIONALIZATION",
        "active_entry_count": len(active_entries),
        "entries": active_entries,
        "tiered_entries": {
            "TIER_1": [e for e in active_entries if e.get("tier") == "TIER_1"],
            "TIER_2": [e for e in active_entries if e.get("tier") == "TIER_2"],
            "TIER_3": [e for e in active_entries if e.get("tier") == "TIER_3"],
            "EXCLUDED": excluded_entries,
        },
        "entry_state_policy": entry_state_policy(),
        "entry_economics_policy": entry_economics_policy(),
        "portfolio_utility": summarize_portfolio_utility(active_entries, proof_rows),
    }

    out_dir = args.out_dir
    write_json(out_dir / "proof_stage_report.json", proof_report)
    write_json(out_dir / "tiered_entry_book.json", entry_book)
    write_json(out_dir / "entry_state_policy.json", entry_state_policy())
    write_json(out_dir / "entry_economics_policy.json", entry_economics_policy())
    write_json(out_dir / "entry_portfolio_utility.json", summarize_portfolio_utility(active_entries, proof_rows))

    print(
        json.dumps(
            {
                "out_dir": str(out_dir),
                "selected_candidates": proof_report["candidate_counts"]["selected"],
                "proof_pass": proof_report["candidate_counts"]["proof_pass"],
                "proof_fail": proof_report["candidate_counts"]["proof_fail"],
                "active_entry_count": entry_book["active_entry_count"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
