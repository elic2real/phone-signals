from __future__ import annotations

import argparse
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

WORKSPACE = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = WORKSPACE / "control" / "closeout_decision_report.json"
DEFAULT_OUT_DIR = WORKSPACE / "control" / "universal_triage"


@dataclass
class Thresholds:
    cost_buffer: float
    min_density: float
    min_setup_count: int
    min_trigger_count: int
    tier1_expectancy: float
    tier1_density: float
    tier1_trigger_quality: float
    tier1_stability: float
    tier2_expectancy: float
    tier2_density: float
    tier2_trigger_quality: float
    tier2_stability: float
    tier3_expectancy: float
    tier3_density: float
    tier3_trigger_quality: float
    tier3_stability: float
    parked_cts_max: float


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_json_if_exists(path: Path) -> Any | None:
    if not path.exists():
        return None
    return read_json(path)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def min_max_norm(values: list[float | None]) -> list[float | None]:
    non_null = [v for v in values if v is not None]
    if not non_null:
        return [None for _ in values]
    lo = min(non_null)
    hi = max(non_null)
    if hi == lo:
        return [0.5 if v is not None else None for v in values]
    out = []
    for value in values:
        if value is None:
            out.append(None)
        else:
            out.append((value - lo) / (hi - lo))
    return out


def pair_to_complex(pair: str) -> str:
    p = pair.upper()
    if p in {"EUR_USD", "EUR_GBP", "EUR_JPY"}:
        return "EUR_COMPLEX"
    if p in {"GBP_USD", "GBP_JPY", "EUR_GBP"}:
        return "GBP_COMPLEX"
    if p in {"USD_JPY", "EUR_JPY", "GBP_JPY", "AUD_JPY", "CHF_JPY", "NZD_JPY"}:
        return "JPY_PHYSICS"
    if p in {"AUD_USD", "NZD_USD", "USD_CAD"}:
        return "COMMODITY_CCY"
    return "OTHER"


def parse_domain(domain: str) -> tuple[str, str, str]:
    pair, session, direction = domain.split("/")
    return pair, session, direction


def resolve_source_path(pair: str, session: str, weekday: str) -> str:
    candidates = [
        WORKSPACE / "PC2" / "mapping_minimal" / "compiled_friday_refactor" / f"{pair}__{weekday}__{session}" / "phase1" / "opportunity_map_raw.csv",
        WORKSPACE / "PC2" / "mapping_minimal" / "mapping_minimal" / "compiled_friday_refactor" / f"{pair}__{weekday}__{session}" / "phase1" / "opportunity_map_raw.csv",
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return str(candidates[0])


def extract_best_target_bucket(batch_name: str) -> int | None:
    setup_path = WORKSPACE / "PC2" / "discovery" / "scale_batches" / batch_name / "setup_truth.json"
    setup = read_json_if_exists(setup_path)
    if not setup:
        return None
    records = setup.get("records", [])
    if not records:
        return None
    best = max(records, key=lambda r: r.get("expectancy", float("-inf")))
    return best.get("target_bucket")


def build_universe_from_closeout(closeout_path: Path) -> list[dict]:
    closeout = read_json(closeout_path)
    rows = closeout.get("rows", [])
    invalid_batches = set(closeout.get("invalidated_batches", []))

    universe: list[dict] = []
    for row in rows:
        batch = row.get("batch")
        domain = row.get("domain")
        if not batch or not domain:
            continue
        pair, session, direction = parse_domain(domain)
        stability = row.get("stability") or {}
        universe.append(
            {
                "domain_id": batch,
                "batch": batch,
                "pair": pair,
                "session": session,
                "direction": direction,
                "target_bucket": extract_best_target_bucket(batch),
                "weekday": "Friday",
                "source_cohort_path": resolve_source_path(pair, session, "Friday"),
                "complex_label": pair_to_complex(pair),
                "integrity_status": "OK" if batch not in invalid_batches else "QUARANTINE",
                "integrity_reason": "Q_DUPLICATE_SESSION" if batch in invalid_batches else None,
                "E_raw": row.get("expectancy"),
                "D_raw": row.get("density_normalized_per_hour"),
                "T_raw": row.get("trigger_quality"),
                "density_status": row.get("density_status"),
                "setup_count": row.get("setup_count"),
                "trigger_count": row.get("trigger_count"),
                "stability_obj": stability,
                "rank": row.get("rank"),
            }
        )

    invalid_dir = WORKSPACE / "PC2" / "discovery" / "scale_batches"
    for invalid_file in invalid_dir.glob("*/INVALID.json"):
        invalid_doc = read_json_if_exists(invalid_file)
        if not invalid_doc:
            continue
        batch = invalid_doc.get("batch") or invalid_file.parent.name
        if any(item["domain_id"] == batch for item in universe):
            continue
        pieces = batch.split("_")
        if len(pieces) < 5:
            continue
        pair = f"{pieces[0].upper()}_{pieces[1].upper()}"
        session = pieces[2].capitalize()
        direction = pieces[3].upper()
        universe.append(
            {
                "domain_id": batch,
                "batch": batch,
                "pair": pair,
                "session": session,
                "direction": direction,
                "target_bucket": None,
                "weekday": "Friday",
                "source_cohort_path": resolve_source_path(pair, session, "Friday"),
                "complex_label": pair_to_complex(pair),
                "integrity_status": "QUARANTINE",
                "integrity_reason": "Q_DUPLICATE_SESSION",
                "E_raw": None,
                "D_raw": None,
                "T_raw": None,
                "density_status": "MISSING_ODM",
                "setup_count": 0,
                "trigger_count": 0,
                "stability_obj": {},
                "rank": None,
            }
        )

    return universe


def stability_proxy(stability_obj: dict, setup_count: int | None, trigger_count: int | None, e_raw: float | None) -> float | None:
    drift = stability_obj.get("expectancy_drift_100_to_300")
    stable_flag = stability_obj.get("stable")

    if drift is None and stable_flag is None:
        return None

    drift_component = 0.5
    if isinstance(drift, (int, float)):
        drift_component = max(0.0, min(1.0, 1.0 + drift))

    setup_component = 0.0 if not setup_count else min(1.0, setup_count / 5.0)
    trigger_component = 0.0 if not trigger_count else min(1.0, trigger_count / 5.0)

    sign_component = 0.0
    if isinstance(e_raw, (int, float)):
        sign_component = 1.0 if e_raw > 0 else 0.0

    stable_component = 0.5
    if stable_flag is True:
        stable_component = 1.0
    elif stable_flag is False:
        stable_component = 0.0

    score = (
        0.35 * drift_component
        + 0.20 * setup_component
        + 0.20 * trigger_component
        + 0.15 * sign_component
        + 0.10 * stable_component
    )
    return max(0.0, min(1.0, score))


def apply_hard_gates(domain: dict, t: Thresholds) -> tuple[str | None, list[str], str | None]:
    reason_codes: list[str] = []

    required_identifiers = ["pair", "session", "direction", "source_cohort_path"]
    if any(not domain.get(field) for field in required_identifiers):
        reason_codes.append("Q_MISSING_SOURCE")
        return "QUARANTINE", reason_codes, "VERIFY_DATA"

    if domain.get("integrity_status") == "QUARANTINE":
        reason_codes.append(domain.get("integrity_reason") or "Q_LABEL_MISMATCH")
        return "QUARANTINE", reason_codes, "VERIFY_DATA"

    e_raw = domain.get("E_raw")
    if e_raw is None:
        reason_codes.append("D_BELOW_COST")
        return "DEAD", reason_codes, "RETIRE"
    if e_raw <= t.cost_buffer:
        reason_codes.append("D_EDGE_TOO_THIN")
        return "DEAD", reason_codes, "RETIRE"

    density = domain.get("D_raw")
    if domain.get("density_status") == "MISSING_ODM":
        reason_codes.append("D_DENSITY_NOT_NORMALIZED")
        return "DEAD", reason_codes, "RETIRE"
    if density is None or density < t.min_density:
        reason_codes.append("D_LOW_DENSITY")
        return "DEAD", reason_codes, "RETIRE"

    if (domain.get("setup_count") or 0) < t.min_setup_count:
        reason_codes.append("D_NO_SETUP_EVIDENCE")
        return "DEAD", reason_codes, "RETIRE"

    if (domain.get("trigger_count") or 0) < t.min_trigger_count:
        reason_codes.append("D_NO_TRIGGER_EVIDENCE")
        return "DEAD", reason_codes, "RETIRE"

    if domain.get("T_raw") is None:
        reason_codes.append("D_NO_STRUCTURAL_SIGNAL")
        return "DEAD", reason_codes, "RETIRE"

    return None, reason_codes, None


def meets_all(values: dict[str, float | None], thresholds: dict[str, float]) -> bool:
    for key, threshold in thresholds.items():
        val = values.get(key)
        if val is None or val < threshold:
            return False
    return True


def classify_survivor(domain: dict, t: Thresholds) -> tuple[str, list[str], str]:
    checks = {
        "E_raw": domain.get("E_raw"),
        "D_raw": domain.get("D_raw"),
        "T_raw": domain.get("T_raw"),
        "S_raw": domain.get("S_raw"),
    }

    tier1 = {
        "E_raw": t.tier1_expectancy,
        "D_raw": t.tier1_density,
        "T_raw": t.tier1_trigger_quality,
        "S_raw": t.tier1_stability,
    }
    tier2 = {
        "E_raw": t.tier2_expectancy,
        "D_raw": t.tier2_density,
        "T_raw": t.tier2_trigger_quality,
        "S_raw": t.tier2_stability,
    }
    tier3 = {
        "E_raw": t.tier3_expectancy,
        "D_raw": t.tier3_density,
        "T_raw": t.tier3_trigger_quality,
        "S_raw": t.tier3_stability,
    }

    if meets_all(checks, tier1):
        return "TIER_1", ["T1_HIGH_EDGE_HIGH_DENSITY", "T1_HIGH_STABILITY_HIGH_TRIGGER_QUALITY"], "PROVE_NOW"

    if meets_all(checks, tier2):
        return "TIER_2", ["T2_GOOD_EDGE_MEDIUM_DENSITY"], "KEEP_IN_ACTIVE_QUEUE"

    if meets_all(checks, tier3):
        cts = domain.get("CTS")
        if cts is not None and cts <= t.parked_cts_max:
            return "PARKED", ["PARK_VALID_LOW_PRIORITY"], "PARK"
        return "TIER_3", ["T3_ABOVE_COST_LOW_STABILITY"], "KEEP_IN_MAP"

    return "PARKED", ["PARK_WAITING_FOR_BETTER_COMPARATORS"], "PARK"


def add_group_normalizations(rows: list[dict]) -> None:
    by_group: dict[str, list[int]] = {}
    for idx, row in enumerate(rows):
        key = f"{row.get('session')}|{row.get('complex_label')}|{row.get('direction')}"
        by_group.setdefault(key, []).append(idx)

    for metric in ["E_raw", "D_raw", "T_raw", "S_raw"]:
        norm_key = metric.replace("_raw", "_norm_group")
        for _, idxs in by_group.items():
            vals = [rows[i].get(metric) for i in idxs]
            norms = min_max_norm(vals)
            for i, norm in zip(idxs, norms):
                rows[i][norm_key] = norm


def compute_summary(rows: list[dict]) -> dict:
    counts = Counter(row["assigned_class"] for row in rows)
    dead_reasons = Counter()
    quarantine_reasons = Counter()
    for row in rows:
        for reason in row.get("reason_codes", []):
            if row["assigned_class"] == "DEAD":
                dead_reasons[reason] += 1
            if row["assigned_class"] == "QUARANTINE":
                quarantine_reasons[reason] += 1

    survivors = [r for r in rows if r["assigned_class"].startswith("TIER") or r["assigned_class"] == "PARKED"]

    def top_by(key: str, n: int = 5) -> list[dict]:
        items = [r for r in survivors if r.get(key) is not None]
        items.sort(key=lambda r: r[key], reverse=True)
        return [
            {
                "domain_id": r["domain_id"],
                key: r[key],
                "assigned_class": r["assigned_class"],
                "next_action": r["next_action"],
            }
            for r in items[:n]
        ]

    return {
        "total_domains": len(rows),
        "class_counts": {
            "TIER_1": counts.get("TIER_1", 0),
            "TIER_2": counts.get("TIER_2", 0),
            "TIER_3": counts.get("TIER_3", 0),
            "PARKED": counts.get("PARKED", 0),
            "DEAD": counts.get("DEAD", 0),
            "QUARANTINE": counts.get("QUARANTINE", 0),
        },
        "top_cts_domains": top_by("CTS"),
        "top_expectancy_domains": top_by("E_raw"),
        "top_density_domains": top_by("D_raw"),
        "top_trigger_quality_domains": top_by("T_raw"),
        "dead_reason_histogram": dict(dead_reasons),
        "quarantine_reason_histogram": dict(quarantine_reasons),
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Single-Pass Universal Triage Engine")
    p.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    p.add_argument("--cost-buffer", type=float, default=0.20)
    p.add_argument("--min-density", type=float, default=1.0)
    p.add_argument("--min-setup-count", type=int, default=1)
    p.add_argument("--min-trigger-count", type=int, default=1)

    p.add_argument("--tier1-expectancy", type=float, default=2.0)
    p.add_argument("--tier1-density", type=float, default=10.0)
    p.add_argument("--tier1-trigger-quality", type=float, default=0.70)
    p.add_argument("--tier1-stability", type=float, default=0.70)

    p.add_argument("--tier2-expectancy", type=float, default=1.0)
    p.add_argument("--tier2-density", type=float, default=8.0)
    p.add_argument("--tier2-trigger-quality", type=float, default=0.55)
    p.add_argument("--tier2-stability", type=float, default=0.50)

    p.add_argument("--tier3-expectancy", type=float, default=0.40)
    p.add_argument("--tier3-density", type=float, default=5.0)
    p.add_argument("--tier3-trigger-quality", type=float, default=0.40)
    p.add_argument("--tier3-stability", type=float, default=0.30)

    p.add_argument("--parked-cts-max", type=float, default=0.45)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    thresholds = Thresholds(
        cost_buffer=args.cost_buffer,
        min_density=args.min_density,
        min_setup_count=args.min_setup_count,
        min_trigger_count=args.min_trigger_count,
        tier1_expectancy=args.tier1_expectancy,
        tier1_density=args.tier1_density,
        tier1_trigger_quality=args.tier1_trigger_quality,
        tier1_stability=args.tier1_stability,
        tier2_expectancy=args.tier2_expectancy,
        tier2_density=args.tier2_density,
        tier2_trigger_quality=args.tier2_trigger_quality,
        tier2_stability=args.tier2_stability,
        tier3_expectancy=args.tier3_expectancy,
        tier3_density=args.tier3_density,
        tier3_trigger_quality=args.tier3_trigger_quality,
        tier3_stability=args.tier3_stability,
        parked_cts_max=args.parked_cts_max,
    )

    universe = build_universe_from_closeout(args.input)

    rows: list[dict] = []
    for domain in universe:
        s_raw = stability_proxy(
            stability_obj=domain.get("stability_obj") or {},
            setup_count=domain.get("setup_count"),
            trigger_count=domain.get("trigger_count"),
            e_raw=domain.get("E_raw"),
        )
        domain["S_raw"] = s_raw
        rows.append(domain)

    rows_by_id = {row["domain_id"]: row for row in rows}

    for metric in ["E_raw", "D_raw", "T_raw", "S_raw"]:
        norm_vals = min_max_norm([row.get(metric) for row in rows])
        for row, norm in zip(rows, norm_vals):
            row[metric.replace("_raw", "_norm_global")] = norm

    add_group_normalizations(rows)

    for row in rows:
        assigned_class, reasons, next_action = apply_hard_gates(row, thresholds)
        row["reason_codes"] = reasons
        if assigned_class is None:
            cts_components = [
                row.get("E_norm_global"),
                row.get("D_norm_global"),
                row.get("T_norm_global"),
                row.get("S_norm_global"),
            ]
            row["CTS"] = sum((v or 0.0) for v in cts_components) / 4.0
            assigned_class, class_reasons, next_action = classify_survivor(row, thresholds)
            row["reason_codes"] = row["reason_codes"] + class_reasons
        else:
            row["CTS"] = None

        row["assigned_class"] = assigned_class
        row["next_action"] = next_action

    master_rows = []
    for row in rows:
        master_rows.append(
            {
                "domain_id": row.get("domain_id"),
                "pair": row.get("pair"),
                "session": row.get("session"),
                "direction": row.get("direction"),
                "target_bucket": row.get("target_bucket"),
                "source_cohort_path": row.get("source_cohort_path"),
                "integrity_status": row.get("integrity_status"),
                "complex_label": row.get("complex_label"),
                "E_raw": row.get("E_raw"),
                "D_raw": row.get("D_raw"),
                "T_raw": row.get("T_raw"),
                "S_raw": row.get("S_raw"),
                "E_norm_global": row.get("E_norm_global"),
                "D_norm_global": row.get("D_norm_global"),
                "T_norm_global": row.get("T_norm_global"),
                "S_norm_global": row.get("S_norm_global"),
                "E_norm_group": row.get("E_norm_group"),
                "D_norm_group": row.get("D_norm_group"),
                "T_norm_group": row.get("T_norm_group"),
                "S_norm_group": row.get("S_norm_group"),
                "CTS": row.get("CTS"),
                "assigned_class": row.get("assigned_class"),
                "reason_codes": row.get("reason_codes"),
                "next_action": row.get("next_action"),
            }
        )

    summary = compute_summary(master_rows)
    deadpool = [
        row for row in master_rows if row.get("assigned_class") in {"DEAD", "QUARANTINE"}
    ]

    out_dir = args.out_dir
    write_json(out_dir / "universal_triage_master.json", master_rows)
    write_json(out_dir / "universal_triage_summary.json", summary)
    write_json(out_dir / "universal_triage_deadpool.json", deadpool)

    print(json.dumps(
        {
            "out_dir": str(out_dir),
            "total_domains": len(master_rows),
            "class_counts": summary["class_counts"],
        },
        indent=2,
    ))


if __name__ == "__main__":
    main()
