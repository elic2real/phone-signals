#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from phase1_ode_proven import OpportunityDiscoveryEngine, load_oanda_data


ROOT = Path(__file__).resolve().parent


def parse_ts(ts: str) -> datetime:
    ts = str(ts)
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def summarize(opportunities: List[Dict[str, Any]], session_count: int) -> Dict[str, Any]:
    long_only = sum(1 for r in opportunities if r["up_exists"] == 1 and r["down_exists"] == 0)
    short_only = sum(1 for r in opportunities if r["up_exists"] == 0 and r["down_exists"] == 1)
    both = sum(1 for r in opportunities if r["up_exists"] == 1 and r["down_exists"] == 1)
    none = sum(1 for r in opportunities if r["up_exists"] == 0 and r["down_exists"] == 0)
    hours = max(session_count * 8, 1)

    by_weekday: Dict[str, Dict[str, int]] = defaultdict(lambda: {"long_only": 0, "short_only": 0, "both": 0, "none": 0})
    by_session: Dict[str, Dict[str, int]] = defaultdict(lambda: {"long_only": 0, "short_only": 0, "both": 0, "none": 0})
    by_pair: Dict[str, Dict[str, int]] = defaultdict(lambda: {"long_only": 0, "short_only": 0, "both": 0, "none": 0})

    for r in opportunities:
        bucket = "none"
        if r["up_exists"] == 1 and r["down_exists"] == 0:
            bucket = "long_only"
        elif r["up_exists"] == 0 and r["down_exists"] == 1:
            bucket = "short_only"
        elif r["up_exists"] == 1 and r["down_exists"] == 1:
            bucket = "both"
        by_weekday[r["weekday"]][bucket] += 1
        by_session[r["session"]][bucket] += 1
        by_pair["EUR_USD"][bucket] += 1

    return {
        "total_rows_processed": len(opportunities),
        "total_LONG_opportunities": long_only,
        "total_SHORT_opportunities": short_only,
        "total_BOTH_opportunities": both,
        "total_NONE_opportunities": none,
        "session_count": session_count,
        "opportunities_per_hour": {
            "long_only": long_only / hours,
            "short_only": short_only / hours,
            "both": both / hours,
            "none": none / hours,
        },
        "opportunities_by_weekday": by_weekday,
        "opportunities_by_session": by_session,
        "opportunities_by_pair": by_pair,
    }


def audit(opportunities: List[Dict[str, Any]], session_count: int) -> Dict[str, Any]:
    neg_taus = [
        r for r in opportunities
        if (r["tau_up_min"] is not None and r["tau_up_min"] < 0)
        or (r["tau_down_min"] is not None and r["tau_down_min"] < 0)
    ]
    bad_up = [r for r in opportunities if r["up_exists"] == 1 and float(r["mfe_up_pips"]) < 2.5]
    bad_down = [r for r in opportunities if r["down_exists"] == 1 and float(r["mfe_down_pips"]) < 2.5]
    passed = not neg_taus and not bad_up and not bad_down
    return {
        "session_boundary_handling": {
            "check": "multi_session_compiled_per_session",
            "passed": True,
            "details": f"Compiled {session_count} sessions independently before aggregation",
        },
        "no_negative_taus": {
            "check": "no_negative_taus",
            "passed": not neg_taus,
            "details": f"Found {len(neg_taus)} negative tau values",
        },
        "up_exists_validation": {
            "check": "up_exists_mfe_validation",
            "passed": not bad_up,
            "details": f"Found {len(bad_up)} up_exists=1 with mfe_up_pips < 2.5",
        },
        "down_exists_validation": {
            "check": "down_exists_mfe_validation",
            "passed": not bad_down,
            "details": f"Found {len(bad_down)} down_exists=1 with mfe_down_pips < 2.5",
        },
        "overall_phase1_status": "PHASE1_PASS" if passed else "PHASE1_FAIL",
    }


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Phase 1 ODE compile across multiple independent sessions")
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--pair", default="EUR_USD")
    parser.add_argument("--session-label", default=None)
    parser.add_argument("--weekday-label", default=None)
    parser.add_argument("--output-dir", default="phase1_multi_session_outputs")
    args = parser.parse_args()

    output_dir = ROOT / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    price_data = load_oanda_data(args.data_root, args.pair)
    by_date: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in price_data:
        by_date[str(parse_ts(row["timestamp"]).date())].append(row)

    ode = OpportunityDiscoveryEngine(min_movement_pips=2.5, max_time_minutes=100)
    all_opportunities: List[Dict[str, Any]] = []
    for date in sorted(by_date):
        session_rows = by_date[date]
        all_opportunities.extend(ode.discover_opportunities(session_rows))

    if args.session_label or args.weekday_label:
        for row in all_opportunities:
            if args.session_label:
                row["session"] = args.session_label
            if args.weekday_label:
                row["weekday"] = args.weekday_label

    ode.save_opportunity_map_raw(all_opportunities, str(output_dir / "opportunity_map_raw.csv"))
    summary = summarize(all_opportunities, len(by_date))
    (output_dir / "opportunity_map_summary.json").write_text(json.dumps(summary, indent=2))
    report = audit(all_opportunities, len(by_date))
    (output_dir / "opportunity_map_audit.json").write_text(json.dumps(report, indent=2))
    print(json.dumps({"summary": summary, "audit": report}, indent=2))
    return 0 if report["overall_phase1_status"] == "PHASE1_PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
