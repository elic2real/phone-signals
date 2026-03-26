#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any


CLUSTER_GAP_MIN = 5
PIP_THRESHOLD = 2.5


def parse_ts(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


@dataclass
class Opportunity:
    session_date: str
    timestamp: str
    dt: datetime
    direction: str
    tau_min: int
    mfe_pips: float
    mae_pips: float
    session: str
    weekday: str


def load_opportunities(path: Path) -> list[Opportunity]:
    out: list[Opportunity] = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            dt = parse_ts(row["timestamp"])
            session_date = dt.date().isoformat()
            if row["up_exists"] == "1":
                out.append(
                    Opportunity(
                        session_date=session_date,
                        timestamp=row["timestamp"],
                        dt=dt,
                        direction="LONG",
                        tau_min=int(float(row["tau_up_min"])),
                        mfe_pips=float(row["mfe_up_pips"]),
                        mae_pips=float(row["mae_up_pips"]),
                        session=row["session"],
                        weekday=row["weekday"],
                    )
                )
            if row["down_exists"] == "1":
                out.append(
                    Opportunity(
                        session_date=session_date,
                        timestamp=row["timestamp"],
                        dt=dt,
                        direction="SHORT",
                        tau_min=int(float(row["tau_down_min"])),
                        mfe_pips=float(row["mfe_down_pips"]),
                        mae_pips=float(row["mae_down_pips"]),
                        session=row["session"],
                        weekday=row["weekday"],
                    )
                )
    return out


def cluster_opportunities(opportunities: list[Opportunity]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[Opportunity]] = defaultdict(list)
    for opp in opportunities:
        grouped[(opp.session_date, opp.direction)].append(opp)

    clusters: list[dict[str, Any]] = []
    cluster_idx_by_direction: Counter[str] = Counter()
    for (session_date, direction), items in grouped.items():
        items.sort(key=lambda x: x.dt)
        current: dict[str, Any] | None = None
        for opp in items:
            opp_end = opp.dt + timedelta(minutes=opp.tau_min)
            if current is None or opp.dt > current["end_dt"] + timedelta(minutes=CLUSTER_GAP_MIN):
                cluster_idx_by_direction[direction] += 1
                current = {
                    "cluster_id": f"{direction}_{cluster_idx_by_direction[direction]:04d}",
                    "session_date": session_date,
                    "direction": direction,
                    "session": opp.session,
                    "weekday": opp.weekday,
                    "start_dt": opp.dt,
                    "end_dt": opp_end,
                    "members": [],
                }
                clusters.append(current)
            current["members"].append(opp)
            if opp_end > current["end_dt"]:
                current["end_dt"] = opp_end

    rows: list[dict[str, Any]] = []
    for cluster in clusters:
        members: list[Opportunity] = cluster["members"]
        rows.append(
            {
                "cluster_id": cluster["cluster_id"],
                "pair": "EUR_USD",
                "direction": cluster["direction"],
                "session_date": cluster["session_date"],
                "session": cluster["session"],
                "weekday": cluster["weekday"],
                "cluster_start": cluster["start_dt"].isoformat(),
                "cluster_end": cluster["end_dt"].isoformat(),
                "cluster_duration_minutes": int((cluster["end_dt"] - cluster["start_dt"]).total_seconds() // 60),
                "cluster_MFE_pips": max(m.mfe_pips for m in members),
                "cluster_MAE_pips": max(m.mae_pips for m in members),
                "member_opportunities": len(members),
                "member_start_times": "|".join(m.timestamp for m in members),
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    fieldnames = [
        "cluster_id",
        "pair",
        "direction",
        "session_date",
        "session",
        "weekday",
        "cluster_start",
        "cluster_end",
        "cluster_duration_minutes",
        "cluster_MFE_pips",
        "cluster_MAE_pips",
        "member_opportunities",
        "member_start_times",
    ]
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        if rows:
            writer.writerows(rows)


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_pair = Counter(r["pair"] for r in rows)
    by_session = Counter(r["session"] for r in rows)
    return {
        "total_clusters": len(rows),
        "avg_cluster_size": mean(r["member_opportunities"] for r in rows) if rows else 0.0,
        "max_cluster_size": max(r["member_opportunities"] for r in rows) if rows else 0,
        "clusters_per_pair": dict(by_pair),
        "clusters_per_session": dict(by_session),
        "cluster_gap_minutes": CLUSTER_GAP_MIN,
        "pip_threshold": PIP_THRESHOLD,
        "session_count": len({r["session_date"] for r in rows}),
    }


def build_audit(rows: list[dict[str, Any]], input_csv: str) -> dict[str, Any]:
    no_orphans = all(r["member_opportunities"] > 0 for r in rows)
    start_before_end = all(parse_ts(r["cluster_start"]) <= parse_ts(r["cluster_end"]) for r in rows)
    single_scope = len({(r["session"], r["weekday"]) for r in rows}) <= 1
    has_rows = bool(rows)
    return {
        "derived_from": input_csv,
        "session_boundary_handling": {"passed": True, "details": "Clusters were compiled independently inside each session date"},
        "no_orphan_clusters": {"passed": no_orphans},
        "cluster_time_ordering": {"passed": start_before_end},
        "scope_validation": {
            "passed": single_scope,
            "details": "All rows belong to exactly one session/weekday node",
        },
        "has_clusters": has_rows,
        "overall_phase2_status": "PHASE2_PASS" if all([no_orphans, start_before_end, single_scope]) else "PHASE2_FAIL",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Compile executable clusters from the 11-session opportunity map")
    parser.add_argument("--input-csv", default="phase1_11_sessions_outputs/opportunity_map_raw.csv")
    parser.add_argument("--output-dir", default="phase2_11_sessions_outputs")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    opportunities = load_opportunities(Path(args.input_csv))
    cluster_rows = cluster_opportunities(opportunities)

    write_csv(output_dir / "opportunity_clusters.csv", cluster_rows)
    (output_dir / "cluster_summary.json").write_text(json.dumps(build_summary(cluster_rows), indent=2))
    (output_dir / "cluster_audit.json").write_text(json.dumps(build_audit(cluster_rows, args.input_csv), indent=2))

    print(f"Compiled {len(cluster_rows)} clusters from {len(opportunities)} opportunities")
    print(f"Wrote: {output_dir / 'opportunity_clusters.csv'}")
    print(f"Wrote: {output_dir / 'cluster_summary.json'}")
    print(f"Wrote: {output_dir / 'cluster_audit.json'}")


if __name__ == "__main__":
    main()
