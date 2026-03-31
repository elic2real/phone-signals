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

    for row in candidates:
        proof = evaluate_proof(row)
        proof_rows.append(
            {
                "domain_id": row.get("domain_id"),
                "assigned_class": row.get("assigned_class"),
                "next_action_from_triage": row.get("next_action"),
                "proof": proof,
            }
        )
        if proof["proof_pass"]:
            active_entries.extend(build_entries_for_row(row))

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
    }

    out_dir = args.out_dir
    write_json(out_dir / "proof_stage_report.json", proof_report)
    write_json(out_dir / "tiered_entry_book.json", entry_book)

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
