#!/usr/bin/env python3
"""Build MVP Phase 2 entry-supply proof metrics under locked scope.

This runner is intentionally analysis-only. It does not tune AEE, priority,
or routing behavior. It quantifies candidate supply before and after scoring
for the locked battlefield scope.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


LOCKED_PAIRS = {"EUR_USD", "GBP_USD"}
LOCKED_SESSION = "london"
A_GRADE_MIN_COMPOSITE_SCORE = 0.80
RAW_STREAM_PATH = Path("global_raw_candidate_stream.parquet")
TRUE_STREAM_PATH = Path("global_true_candidate_stream.parquet")
OUTPUT_PATH = Path("control/mvp_phase2_entry_supply_proof.json")


@dataclass
class SupplyStats:
    total_candidates: int
    unique_timestamps: int
    peak_simultaneous_candidates: int
    timestamps_with_competition_ge_2: int
    avg_candidates_per_timestamp: float


def _extract_pair_from_node(node: Any) -> Optional[str]:
    if not isinstance(node, str) or "__" not in node:
        return None
    return node.split("__", 1)[0].upper()


def _extract_session_from_node(node: Any) -> Optional[str]:
    if not isinstance(node, str):
        return None
    parts = node.split("__")
    if len(parts) < 3:
        return None
    return parts[2].strip().lower()


def _stats_from_counts(counts: pd.Series) -> SupplyStats:
    if counts.empty:
        return SupplyStats(
            total_candidates=0,
            unique_timestamps=0,
            peak_simultaneous_candidates=0,
            timestamps_with_competition_ge_2=0,
            avg_candidates_per_timestamp=0.0,
        )
    return SupplyStats(
        total_candidates=int(counts.sum()),
        unique_timestamps=int(counts.shape[0]),
        peak_simultaneous_candidates=int(counts.max()),
        timestamps_with_competition_ge_2=int((counts >= 2).sum()),
        avg_candidates_per_timestamp=float(counts.mean()),
    )


def _build_hourly_density(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {
            "hours_observed": 0,
            "total_candidates": 0,
            "average_candidates_per_hour": 0.0,
            "median_candidates_per_hour": 0.0,
            "p90_candidates_per_hour": 0.0,
            "max_candidates_per_hour": 0,
            "top_10_hours": [],
        }

    hours = df["timestamp"].dt.floor("h")
    per_hour = hours.value_counts().sort_index()
    top_hours = per_hour.sort_values(ascending=False).head(10)

    return {
        "hours_observed": int(per_hour.shape[0]),
        "total_candidates": int(per_hour.sum()),
        "average_candidates_per_hour": float(per_hour.mean()),
        "median_candidates_per_hour": float(per_hour.median()),
        "p90_candidates_per_hour": float(per_hour.quantile(0.90)),
        "max_candidates_per_hour": int(per_hour.max()),
        "top_10_hours": [
            {"hour": idx.isoformat(), "count": int(val)}
            for idx, val in top_hours.items()
        ],
    }


def _build_simultaneous_snapshot(df: pd.DataFrame, include_score: bool) -> List[Dict[str, Any]]:
    if df.empty:
        return []

    counts = df.groupby("timestamp").size().sort_values(ascending=False).head(10)
    rows: List[Dict[str, Any]] = []
    for ts, count in counts.items():
        bucket = df[df["timestamp"] == ts].copy()
        entry = {
            "timestamp": ts.isoformat(),
            "candidate_count": int(count),
            "pairs": sorted(bucket["pair"].dropna().astype(str).unique().tolist()),
        }
        if include_score and "composite_score" in bucket.columns:
            ranked = bucket.sort_values("composite_score", ascending=False).head(5)
            entry["top_5_scored_candidates"] = [
                {
                    "pair": str(row["pair"]),
                    "direction": str(row.get("direction_assumed", "")),
                    "composite_score": float(row["composite_score"]),
                }
                for _, row in ranked.iterrows()
            ]
        rows.append(entry)
    return rows


def main() -> None:
    if not RAW_STREAM_PATH.exists():
        raise FileNotFoundError(f"Missing required raw candidate stream: {RAW_STREAM_PATH}")
    if not TRUE_STREAM_PATH.exists():
        raise FileNotFoundError(f"Missing required true candidate stream: {TRUE_STREAM_PATH}")

    raw = pd.read_parquet(RAW_STREAM_PATH)
    true = pd.read_parquet(TRUE_STREAM_PATH)

    raw["timestamp"] = pd.to_datetime(raw["timestamp"], utc=True, errors="coerce")
    raw = raw.dropna(subset=["timestamp"]).copy()
    raw["pair"] = raw.get("pair", pd.Series(dtype=str)).astype(str).str.upper()
    raw_session = raw.get("session", pd.Series(dtype=str)).astype(str).str.lower()
    node_session = raw.get("node", pd.Series(dtype=str)).apply(_extract_session_from_node)
    raw["session_norm"] = raw_session.where(raw_session != "", node_session)

    raw_locked = raw[
        raw["pair"].isin(LOCKED_PAIRS)
        & (raw["session_norm"] == LOCKED_SESSION)
    ].copy()

    true["timestamp"] = pd.to_datetime(true["timestamp"], utc=True, errors="coerce")
    true = true.dropna(subset=["timestamp"]).copy()
    true["pair"] = true.get("node", pd.Series(dtype=str)).apply(_extract_pair_from_node)
    true["session_norm"] = true.get("node", pd.Series(dtype=str)).apply(_extract_session_from_node)

    true_locked = true[
        true["pair"].isin(LOCKED_PAIRS)
        & (true["session_norm"] == LOCKED_SESSION)
    ].copy()

    raw_counts = raw_locked.groupby("timestamp").size()
    true_counts = true_locked.groupby("timestamp").size()
    raw_stats = _stats_from_counts(raw_counts)
    true_stats = _stats_from_counts(true_counts)

    a_grade = true_locked[true_locked["composite_score"] >= A_GRADE_MIN_COMPOSITE_SCORE].copy()

    proof = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE2_ENTRY_SUPPLY_PROOF",
        "scope_lock": {
            "pairs": sorted(LOCKED_PAIRS),
            "session": LOCKED_SESSION.upper(),
        },
        "a_grade_definition": {
            "field": "composite_score",
            "threshold": A_GRADE_MIN_COMPOSITE_SCORE,
            "rationale": "Direct quality threshold alignment with A-grade semantics (>=0.80) used in priority-grade code paths.",
        },
        "pre_ranking_generation_evidence": {
            "source": str(RAW_STREAM_PATH),
            "has_priority_columns": any(
                col in raw.columns for col in ["priority_score", "rank", "priority_rank"]
            ),
            "has_quality_score_column": "composite_score" in raw.columns,
            "locked_scope_supply": {
                "total_candidates": raw_stats.total_candidates,
                "unique_timestamps": raw_stats.unique_timestamps,
                "peak_simultaneous_candidates": raw_stats.peak_simultaneous_candidates,
                "timestamps_with_competition_ge_2": raw_stats.timestamps_with_competition_ge_2,
                "avg_candidates_per_timestamp": raw_stats.avg_candidates_per_timestamp,
                "sample_top_competition_timestamps": _build_simultaneous_snapshot(
                    raw_locked,
                    include_score=False,
                ),
            },
        },
        "scored_candidate_supply": {
            "source": str(TRUE_STREAM_PATH),
            "locked_scope_total_candidates": true_stats.total_candidates,
            "locked_scope_unique_timestamps": true_stats.unique_timestamps,
            "locked_scope_peak_simultaneous_candidates": true_stats.peak_simultaneous_candidates,
            "locked_scope_timestamps_with_competition_ge_2": true_stats.timestamps_with_competition_ge_2,
            "locked_scope_avg_candidates_per_timestamp": true_stats.avg_candidates_per_timestamp,
            "a_grade_count": int(a_grade.shape[0]),
            "a_grade_ratio": float((a_grade.shape[0] / true_locked.shape[0]) if true_locked.shape[0] else 0.0),
            "sample_top_competition_timestamps_with_scores": _build_simultaneous_snapshot(
                true_locked,
                include_score=True,
            ),
        },
        "candidate_density_per_hour": {
            "pre_ranking_raw": _build_hourly_density(raw_locked),
            "scored_true": _build_hourly_density(true_locked),
            "a_grade_only": _build_hourly_density(a_grade),
        },
    }

    pass_conditions = {
        "pre_ranking_candidates_exist": proof["pre_ranking_generation_evidence"]["locked_scope_supply"]["total_candidates"] > 0,
        "simultaneous_pool_exists": proof["pre_ranking_generation_evidence"]["locked_scope_supply"]["peak_simultaneous_candidates"] >= 2,
        "a_grade_candidates_exist": proof["scored_candidate_supply"]["a_grade_count"] > 0,
        "hourly_density_positive": proof["candidate_density_per_hour"]["pre_ranking_raw"]["average_candidates_per_hour"] > 0,
    }
    proof["pass_conditions"] = pass_conditions
    proof["status"] = "PASS" if all(pass_conditions.values()) else "FAIL"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(proof, indent=2), encoding="utf-8")

    print(f"wrote {OUTPUT_PATH}")
    print(json.dumps({"status": proof["status"], "pass_conditions": pass_conditions}, indent=2))


if __name__ == "__main__":
    main()
