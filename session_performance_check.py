#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from repair_playbook import playbook_for, route_for_issues, verification_checks_for_issues


ROOT = Path(__file__).resolve().parent
SESSION_HOURS = 88.0
SYMMETRIC_BREAK_EVEN = 0.505
WEAK_EDGE_WARNING = 0.51
MIN_SIDE_TRADES = 25
MIN_SIDE_TRADES_PER_HOUR = 0.20
MIN_OPPORTUNITIES = 100
PATHOLOGICAL_TOTAL_OPPORTUNITIES = 15
PATHOLOGICAL_BEST_CLASS_TRADES = 1
ULTRA_THIN_BEST_CLASS_TRADES = 5


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def jload(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def run(
    dataset_lock: Path,
    entry_population_csv: Path,
    output_dir: Path,
    trade_rows_json: Path | None = None,
    session_potential_json: Path | None = None,
    session_opportunity_map_json: Path | None = None,
    session_calibration_json: Path | None = None,
    symmetric_break_even: float = SYMMETRIC_BREAK_EVEN,
    weak_edge_warning: float = WEAK_EDGE_WARNING,
    min_side_trades: int = MIN_SIDE_TRADES,
    min_side_trades_per_hour: float = MIN_SIDE_TRADES_PER_HOUR,
    min_opportunities: int = MIN_OPPORTUNITIES,
    require_aee_trade_rows: bool = False,
) -> dict[str, Any]:
    lock = jload(dataset_lock)
    population = pd.read_csv(entry_population_csv)
    trades = pd.read_json(trade_rows_json) if trade_rows_json and trade_rows_json.exists() else None

    if "direction_assumed" in population.columns:
        population["dir"] = population["direction_assumed"]
    else:
        population["dir"] = population["direction"]

    selected_opportunities_total = int(len(population))
    pair_summary = (
        population.groupby("dir")
        .agg(
            selected_count=("timestamp", "count"),
            entry_win_rate=("static_pips", lambda s: float((pd.Series(s) > 0).mean())),
            entry_total_pips=("static_pips", "sum"),
            entry_avg_pips=("static_pips", "mean"),
        )
        .reset_index()
        .to_dict(orient="records")
    )
    best_class_trade_count = 0
    best_class_direction = None
    best_class_target_distance = None
    best_class_rule_ids: list[str] = []
    best_class_rescue_rule_ids: list[str] = []
    best_class_rule_count = 0
    report_hint_path = entry_population_csv.parent / "target_entry_class_report.json"
    if report_hint_path.exists():
        try:
            target_report = jload(report_hint_path)
            for row in target_report.get("summary", []) or []:
                trade_count = int(row.get("trade_count", 0) or 0)
                score = (
                    trade_count,
                    float(row.get("tp_hit_rate", 0.0) or 0.0),
                    float(row.get("total_pips", 0.0) or 0.0),
                )
                current = (
                    best_class_trade_count,
                    0.0 if best_class_direction is None else 1.0,
                    0.0,
                )
                if score > current:
                    best_class_trade_count = trade_count
                    best_class_direction = str(row.get("direction", ""))
                    best_class_target_distance = row.get("target_distance")
                    rules = row.get("rules", []) or []
                    best_class_rule_ids = [
                        str(rule.get("path_class_id", ""))
                        for rule in rules
                        if isinstance(rule, dict) and rule.get("path_class_id")
                    ]
                    best_class_rescue_rule_ids = [
                        rule_id
                        for rule_id in best_class_rule_ids
                        if "rescue" in rule_id.lower()
                    ]
                    best_class_rule_count = int(row.get("rule_count", len(best_class_rule_ids)) or 0)
        except Exception:
            pass

    downstream_summary: dict[str, dict[str, Any]] = {}
    if trades is not None and not trades.empty:
        trades["dir"] = trades["direction"]
        grouped = (
            trades.groupby("dir")
            .agg(
                downstream_trade_count=("trade_id", "count"),
                downstream_win_rate=("aee_pips", lambda s: float((pd.Series(s) > 0).mean())),
                downstream_total_pips=("aee_pips", "sum"),
                downstream_avg_pips=("aee_pips", "mean"),
            )
            .reset_index()
            .to_dict(orient="records")
        )
        downstream_summary = {str(r["dir"]).upper(): r for r in grouped}

    aee_trade_rows_present = bool(trade_rows_json and trade_rows_json.exists())
    aee_trade_rows_nonempty = bool(trades is not None and not trades.empty)

    session_potential = jload(session_potential_json) if session_potential_json and session_potential_json.exists() else {}
    session_opportunity_map = (
        jload(session_opportunity_map_json) if session_opportunity_map_json and session_opportunity_map_json.exists() else {}
    )
    pair_rollup = session_potential.get("pair_rollup", {}) if isinstance(session_potential, dict) else {}
    potential_zones = session_potential.get("zones", []) if isinstance(session_potential, dict) else []
    opportunity_rollup = session_opportunity_map.get("pair_rollup", {}) if isinstance(session_opportunity_map, dict) else {}
    if opportunity_rollup.get("total_opportunities") is not None:
        opportunities_total = int(opportunity_rollup.get("total_opportunities", 0) or 0)
    elif potential_zones:
        opportunities_total = int(
            sum(int(zone.get("opportunity_count", 0) or 0) for zone in potential_zones if isinstance(zone, dict))
        )
    else:
        opportunities_total = selected_opportunities_total
    session_calibration = jload(session_calibration_json) if session_calibration_json and session_calibration_json.exists() else {}
    calibration_pair_summary = session_calibration.get("pair_summary", []) if isinstance(session_calibration, dict) else []
    calibration_by_side = {str(r.get("dir", "")).upper(): r for r in calibration_pair_summary if isinstance(r, dict)}

    issues: list[dict[str, Any]] = []
    sides: dict[str, dict[str, Any]] = {}
    if require_aee_trade_rows and not aee_trade_rows_nonempty:
        issues.append(
            {
                "severity": "repair",
                "issue": "missing_aee_trade_rows",
                "trade_rows_json": str(trade_rows_json) if trade_rows_json else None,
                "aee_trade_rows_present": aee_trade_rows_present,
                "aee_trade_rows_nonempty": aee_trade_rows_nonempty,
            }
        )

    for row in pair_summary:
        side = str(row["dir"]).upper()
        trades_per_hour = float(row["selected_count"]) / SESSION_HOURS
        downstream = downstream_summary.get(side, {})
        effective_win_rate = float(downstream.get("downstream_win_rate", row["entry_win_rate"]))
        side_payload = {
            "direction": side,
            "selected_count": int(row["selected_count"]),
            "trades_per_hour": trades_per_hour,
            "entry_win_rate": float(row["entry_win_rate"]),
            "entry_total_pips": float(row["entry_total_pips"]),
            "entry_avg_pips": float(row["entry_avg_pips"]),
            "effective_win_rate": effective_win_rate,
            "downstream_trade_count": int(downstream.get("downstream_trade_count", 0) or 0),
            "downstream_win_rate": float(downstream.get("downstream_win_rate", 0.0) or 0.0),
            "downstream_total_pips": float(downstream.get("downstream_total_pips", 0.0) or 0.0),
            "expected_opportunities_per_hour": float(pair_rollup.get(f"expected_{side.lower()}_opportunities_per_hour", 0.0) or 0.0),
            "expected_recyclable_opportunities_per_hour": float(pair_rollup.get(f"expected_{side.lower()}_recyclable_opportunities_per_hour", 0.0) or 0.0),
            "utilization_ratio": float(pair_rollup.get(f"{side.lower()}_utilization_ratio", 0.0) or 0.0),
            "recycling_utilization_ratio": float(pair_rollup.get(f"{side.lower()}_recycling_utilization_ratio", 0.0) or 0.0),
            "calibration_selected_count": int(calibration_by_side.get(side, {}).get("selected_count", 0) or 0),
            "mapped_opportunity_count": int(opportunity_rollup.get(f"{side.lower()}_opportunity_count", 0) or 0),
            "mapped_opportunity_density_per_hour": float(
                opportunity_rollup.get(f"{side.lower()}_opportunity_density_per_hour", 0.0) or 0.0
            ),
        }
        sides[side] = side_payload

        if side_payload["selected_count"] < min_side_trades:
            issues.append(
                {
                    "severity": "repair",
                    "issue": "side_trade_count_too_low",
                    "direction": side,
                    "actual": side_payload["selected_count"],
                    "min_required": min_side_trades,
                }
            )
        if side_payload["trades_per_hour"] < min_side_trades_per_hour:
            issues.append(
                {
                    "severity": "repair",
                    "issue": "side_density_too_low",
                    "direction": side,
                    "actual": side_payload["trades_per_hour"],
                    "min_required": min_side_trades_per_hour,
                }
            )
        if side_payload["effective_win_rate"] < symmetric_break_even:
            issues.append(
                {
                    "severity": "repair",
                    "issue": "below_symmetric_break_even",
                    "direction": side,
                    "actual": side_payload["effective_win_rate"],
                    "min_required": symmetric_break_even,
                }
            )
        elif side_payload["effective_win_rate"] < weak_edge_warning:
            issues.append(
                {
                    "severity": "warn",
                    "issue": "weak_edge_near_break_even",
                    "direction": side,
                    "actual": side_payload["effective_win_rate"],
                    "warning_threshold": weak_edge_warning,
                }
            )

    if opportunities_total < min_opportunities:
        issues.append(
            {
                "severity": "repair",
                "issue": "total_opportunity_count_too_low",
                "actual": opportunities_total,
                "min_required": min_opportunities,
            }
        )
        if opportunities_total <= PATHOLOGICAL_TOTAL_OPPORTUNITIES:
            issues.append(
                {
                    "severity": "repair",
                    "issue": "pathological_total_opportunity_count",
                    "actual": opportunities_total,
                    "max_allowed": PATHOLOGICAL_TOTAL_OPPORTUNITIES,
                }
            )

    if "LONG" not in sides or "SHORT" not in sides:
        missing = [s for s in ("LONG", "SHORT") if s not in sides]
        issues.append(
            {
                "severity": "repair",
                "issue": "missing_directional_coverage",
                "missing_directions": missing,
            }
        )

    if best_class_trade_count <= PATHOLOGICAL_BEST_CLASS_TRADES:
        issues.append(
            {
                "severity": "repair",
                "issue": "pathological_best_class_trade_count",
                "actual": best_class_trade_count,
                "max_allowed": PATHOLOGICAL_BEST_CLASS_TRADES,
                "best_class_direction": best_class_direction,
                "best_class_target_distance": best_class_target_distance,
            }
        )
    elif best_class_trade_count <= ULTRA_THIN_BEST_CLASS_TRADES:
        issues.append(
            {
                "severity": "repair",
                "issue": "ultra_thin_best_class_trade_count",
                "actual": best_class_trade_count,
                "max_allowed": ULTRA_THIN_BEST_CLASS_TRADES,
                "best_class_direction": best_class_direction,
                "best_class_target_distance": best_class_target_distance,
            }
        )

    long_side = sides.get("LONG")
    short_side = sides.get("SHORT")
    if long_side and short_side:
        long_count = max(int(long_side["selected_count"]), 1)
        short_count = max(int(short_side["selected_count"]), 1)
        dominant_side = "LONG" if long_count >= short_count else "SHORT"
        weak_side = "SHORT" if dominant_side == "LONG" else "LONG"
        dominant = sides[dominant_side]
        weak = sides[weak_side]
        ratio = dominant["selected_count"] / max(weak["selected_count"], 1)
        weak_expected = weak["expected_opportunities_per_hour"]
        weak_util = weak["utilization_ratio"]
        dominant_util = dominant["utilization_ratio"]
        if ratio >= 4.0 and weak_expected >= 1.0 and weak_util <= 0.15 and dominant_util >= 0.20:
            issues.append(
                {
                    "severity": "repair",
                    "issue": "directional_overfit",
                    "dominant_direction": dominant_side,
                    "weak_direction": weak_side,
                    "selected_count_ratio": ratio,
                    "weak_expected_opportunities_per_hour": weak_expected,
                    "weak_utilization_ratio": weak_util,
                }
            )
        for side_name, side_payload in [("LONG", long_side), ("SHORT", short_side)]:
            if (
                side_payload["expected_opportunities_per_hour"] >= 1.0
                and side_payload["utilization_ratio"] <= 0.10
                and side_payload["trades_per_hour"] <= min_side_trades_per_hour
            ):
                issues.append(
                    {
                        "severity": "repair",
                        "issue": "underutilized_expected_direction",
                        "direction": side_name,
                        "expected_opportunities_per_hour": side_payload["expected_opportunities_per_hour"],
                        "actual_trades_per_hour": side_payload["trades_per_hour"],
                        "utilization_ratio": side_payload["utilization_ratio"],
                    }
                )

    repair_issues = [issue for issue in issues if issue.get("severity") == "repair"]
    status = "PASS" if not repair_issues else "REPAIR_REQUIRED"
    issue_playbook = {}
    for issue in issues:
        issue_name = str(issue.get("issue"))
        issue_playbook[issue_name] = playbook_for(issue_name)
    repair_issue_names = [str(issue.get("issue")) for issue in repair_issues]
    recommended_failure_route = route_for_issues(repair_issue_names) if repair_issue_names else "none"
    verification_checks = verification_checks_for_issues(repair_issue_names)
    repair_count = len(repair_issues)
    report = {
        "status": status,
        "mode": "session_performance_check",
        "timestamp": iso_now(),
        "node": {
            "pair": lock.get("pair"),
            "weekday": lock.get("weekday"),
            "session": lock.get("session"),
        },
        "symmetric_break_even": symmetric_break_even,
        "opportunity_count": opportunities_total,
        "selected_opportunity_count": selected_opportunities_total,
        "aee_trade_rows_present": aee_trade_rows_present,
        "aee_trade_rows_nonempty": aee_trade_rows_nonempty,
        "sides": sides,
        "issue_count": len(issues),
        "repair_count": repair_count,
        "best_class_trade_count": best_class_trade_count,
        "best_class_direction": best_class_direction,
        "best_class_target_distance": best_class_target_distance,
        "best_class_rule_count": best_class_rule_count,
        "best_class_rule_ids": best_class_rule_ids,
        "best_class_rescue_rule_ids": best_class_rescue_rule_ids,
        "best_class_has_rescue_rule": bool(best_class_rescue_rule_ids),
        "issues": issues,
        "issue_playbook": issue_playbook,
        "recommended_failure_route": recommended_failure_route,
        "verification_checks": verification_checks,
        "weak_edge_warning": weak_edge_warning,
    }
    inputs_hash = hashlib.sha256(
        json.dumps(
            {
                "dataset_lock_hash": sha256_file(dataset_lock),
                "entry_population_hash": sha256_file(entry_population_csv),
                "trade_rows_hash": sha256_file(trade_rows_json) if trade_rows_json and trade_rows_json.exists() else None,
                "session_potential_hash": sha256_file(session_potential_json) if session_potential_json and session_potential_json.exists() else None,
                "session_opportunity_map_hash": (
                    sha256_file(session_opportunity_map_json)
                    if session_opportunity_map_json and session_opportunity_map_json.exists()
                    else None
                ),
                "session_calibration_hash": sha256_file(session_calibration_json) if session_calibration_json and session_calibration_json.exists() else None,
                "target_entry_class_report_hash": sha256_file(report_hint_path) if report_hint_path.exists() else None,
                "script_hash": sha256_file(Path(__file__)),
                "symmetric_break_even": symmetric_break_even,
                "weak_edge_warning": weak_edge_warning,
                "min_side_trades": min_side_trades,
                "min_side_trades_per_hour": min_side_trades_per_hour,
                "min_opportunities": min_opportunities,
                "require_aee_trade_rows": require_aee_trade_rows,
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    manifest = {
        "runner": "session_performance_check.py",
        "inputs_hash": inputs_hash,
        "dataset_lock": str(dataset_lock),
        "entry_population_csv": str(entry_population_csv),
        "trade_rows_json": str(trade_rows_json) if trade_rows_json else None,
        "session_potential_json": str(session_potential_json) if session_potential_json else None,
        "session_opportunity_map_json": str(session_opportunity_map_json) if session_opportunity_map_json else None,
        "session_calibration_json": str(session_calibration_json) if session_calibration_json else None,
        "report": str(output_dir / "session_performance_check_report.json"),
    }

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "session_performance_check_report.json").write_text(json.dumps(report, indent=2))
    (output_dir / "session_performance_check_manifest.json").write_text(json.dumps(manifest, indent=2))
    return report


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset-lock", type=Path, required=True)
    ap.add_argument("--entry-population-csv", type=Path, required=True)
    ap.add_argument("--output-dir", type=Path, required=True)
    ap.add_argument("--trade-rows-json", type=Path)
    ap.add_argument("--session-potential-json", type=Path)
    ap.add_argument("--session-opportunity-map-json", type=Path)
    ap.add_argument("--session-calibration-json", type=Path)
    ap.add_argument("--symmetric-break-even", type=float, default=SYMMETRIC_BREAK_EVEN)
    ap.add_argument("--weak-edge-warning", type=float, default=WEAK_EDGE_WARNING)
    ap.add_argument("--min-side-trades", type=int, default=MIN_SIDE_TRADES)
    ap.add_argument("--min-side-trades-per-hour", type=float, default=MIN_SIDE_TRADES_PER_HOUR)
    ap.add_argument("--min-opportunities", type=int, default=MIN_OPPORTUNITIES)
    ap.add_argument("--require-aee-trade-rows", action="store_true")
    args = ap.parse_args()
    report = run(
        dataset_lock=args.dataset_lock,
        entry_population_csv=args.entry_population_csv,
        output_dir=args.output_dir,
        trade_rows_json=args.trade_rows_json,
        session_potential_json=args.session_potential_json,
        session_opportunity_map_json=args.session_opportunity_map_json,
        session_calibration_json=args.session_calibration_json,
        symmetric_break_even=args.symmetric_break_even,
        weak_edge_warning=args.weak_edge_warning,
        min_side_trades=args.min_side_trades,
        min_side_trades_per_hour=args.min_side_trades_per_hour,
        min_opportunities=args.min_opportunities,
        require_aee_trade_rows=args.require_aee_trade_rows,
    )
    print(json.dumps({"status": report["status"], "node": report["node"], "issue_count": report["issue_count"]}, indent=2))


if __name__ == "__main__":
    main()
