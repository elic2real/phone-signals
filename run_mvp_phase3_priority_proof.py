#!/usr/bin/env python3
"""Build MVP Phase 3 priority proof metrics under locked scope.

This runner is analysis-only and does not modify AEE, priority weights,
or architecture. It measures whether ranking has real queue pressure to solve.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd


CONFIG_PATH = Path("entry_v23_policy_guarded_active.json")
TRUE_STREAM_PATH = Path("global_true_candidate_stream.parquet")
OUTPUT_PATH = Path("control/mvp_phase3_priority_proof.json")
A_GRADE_MIN_COMPOSITE_SCORE = 0.80


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
    return parts[2].strip().upper()


def _load_scope_lock() -> Dict[str, Set[str]]:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8")) if CONFIG_PATH.exists() else {}
    ef = cfg.get("entry_filters", {}) if isinstance(cfg, dict) else {}

    pairs = ef.get("include_pairs") if isinstance(ef, dict) else None
    sessions = ef.get("include_sessions") if isinstance(ef, dict) else None

    pair_set = {str(p).upper() for p in (pairs or [])}
    session_set = {str(s).upper() for s in (sessions or [])}

    if not pair_set:
        pair_set = {"EUR_USD", "GBP_USD"}
    if not session_set:
        session_set = {"LONDON"}

    return {"pairs": pair_set, "sessions": session_set}


def _top_competition_windows(df: pd.DataFrame) -> List[Dict[str, Any]]:
    if df.empty:
        return []

    grouped = (
        df.groupby("timestamp")
        .size()
        .sort_values(ascending=False)
        .head(10)
    )
    out: List[Dict[str, Any]] = []
    for ts, cnt in grouped.items():
        bucket = df[df["timestamp"] == ts].copy().sort_values("composite_score", ascending=False)
        out.append(
            {
                "timestamp": ts.isoformat(),
                "pool_size": int(cnt),
                "pairs": sorted(bucket["pair"].dropna().astype(str).unique().tolist()),
                "top_3": [
                    {
                        "pair": str(row["pair"]),
                        "direction": str(row.get("direction_assumed", "")),
                        "composite_score": float(row["composite_score"]),
                    }
                    for _, row in bucket.head(3).iterrows()
                ],
            }
        )
    return out


def main() -> None:
    if not TRUE_STREAM_PATH.exists():
        raise FileNotFoundError(f"Missing scored candidate stream: {TRUE_STREAM_PATH}")

    scope = _load_scope_lock()
    include_pairs = scope["pairs"]
    include_sessions = scope["sessions"]

    df = pd.read_parquet(TRUE_STREAM_PATH)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"]).copy()
    df["pair"] = df.get("node", pd.Series(dtype=str)).apply(_extract_pair_from_node)
    df["session_norm"] = df.get("node", pd.Series(dtype=str)).apply(_extract_session_from_node)

    locked = df[
        df["pair"].isin(include_pairs)
        & df["session_norm"].isin(include_sessions)
    ].copy()

    if locked.empty:
        result = {
            "protocol": "RCP",
            "protocol_version": "RCP_V2",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "task_id": "MVP_PHASE3_PRIORITY_PROOF",
            "status": "FAIL",
            "reason": "No locked-scope scored candidates found.",
            "scope_lock": {
                "pairs": sorted(include_pairs),
                "sessions": sorted(include_sessions),
            },
        }
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(json.dumps(result, indent=2))
        return

    # Simulate priority allocation pressure: one winner per timestamp by composite score.
    ranked = locked.sort_values(["timestamp", "composite_score"], ascending=[True, False]).copy()
    winners = ranked.groupby("timestamp", as_index=False).head(1).copy()
    losers = ranked.drop(winners.index).copy()

    pool_sizes = ranked.groupby("timestamp").size()
    competing_timestamps = int((pool_sizes >= 2).sum())

    winner_scores = winners["composite_score"]
    mean_pool_scores = ranked.groupby("timestamp")["composite_score"].mean()
    winner_vs_pool = winners[["timestamp", "composite_score"]].merge(
        mean_pool_scores.rename("pool_mean_score"), on="timestamp", how="left"
    )
    winner_vs_pool["edge_over_pool_mean"] = winner_vs_pool["composite_score"] - winner_vs_pool["pool_mean_score"]

    a_grade = ranked[ranked["composite_score"] >= A_GRADE_MIN_COMPOSITE_SCORE].copy()
    winner_a_grade = winners[winners["composite_score"] >= A_GRADE_MIN_COMPOSITE_SCORE].copy()
    loser_a_grade = losers[losers["composite_score"] >= A_GRADE_MIN_COMPOSITE_SCORE].copy()

    per_hour_candidates = ranked["timestamp"].dt.floor("h").value_counts().sort_index()
    per_hour_winners = winners["timestamp"].dt.floor("h").value_counts().sort_index()

    result = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE3_PRIORITY_PROOF",
        "scope_lock": {
            "pairs": sorted(include_pairs),
            "sessions": sorted(include_sessions),
        },
        "allocation_rule_under_test": {
            "name": "timestamp_top1_by_composite_score",
            "description": "At each timestamp, highest composite_score candidate is selected as winner.",
            "tuning_changed": False,
        },
        "priority_pressure": {
            "total_candidates": int(ranked.shape[0]),
            "total_timestamps": int(pool_sizes.shape[0]),
            "timestamps_with_competition_ge_2": competing_timestamps,
            "competition_rate": float(competing_timestamps / pool_sizes.shape[0]) if pool_sizes.shape[0] else 0.0,
            "peak_pool_size": int(pool_sizes.max()),
            "avg_pool_size": float(pool_sizes.mean()),
            "p90_pool_size": float(pool_sizes.quantile(0.90)),
        },
        "selection_impact": {
            "winners": int(winners.shape[0]),
            "displaced_candidates": int(losers.shape[0]),
            "displacement_rate": float(losers.shape[0] / ranked.shape[0]) if ranked.shape[0] else 0.0,
            "winner_mean_score": float(winner_scores.mean()),
            "pool_mean_score": float(ranked["composite_score"].mean()),
            "winner_minus_pool_mean": float(winner_scores.mean() - ranked["composite_score"].mean()),
            "winner_edge_over_pool_mean_avg": float(winner_vs_pool["edge_over_pool_mean"].mean()),
        },
        "a_grade_priority_pressure": {
            "a_grade_threshold": A_GRADE_MIN_COMPOSITE_SCORE,
            "a_grade_total": int(a_grade.shape[0]),
            "a_grade_winners": int(winner_a_grade.shape[0]),
            "a_grade_displaced": int(loser_a_grade.shape[0]),
            "a_grade_displacement_rate": float(loser_a_grade.shape[0] / a_grade.shape[0]) if a_grade.shape[0] else 0.0,
        },
        "throughput_compression": {
            "avg_candidates_per_hour": float(per_hour_candidates.mean()) if not per_hour_candidates.empty else 0.0,
            "avg_winners_per_hour": float(per_hour_winners.mean()) if not per_hour_winners.empty else 0.0,
            "compression_ratio_candidates_to_winners": (
                float(per_hour_candidates.mean() / per_hour_winners.mean())
                if (not per_hour_candidates.empty and not per_hour_winners.empty and per_hour_winners.mean() > 0)
                else 0.0
            ),
        },
        "sample_top_competition_windows": _top_competition_windows(ranked),
    }

    pass_conditions = {
        "competition_exists": result["priority_pressure"]["timestamps_with_competition_ge_2"] > 0,
        "displacement_exists": result["selection_impact"]["displaced_candidates"] > 0,
        "ranking_adds_value": result["selection_impact"]["winner_minus_pool_mean"] > 0,
        "a_grade_queue_pressure_exists": result["a_grade_priority_pressure"]["a_grade_displaced"] > 0,
    }
    result["pass_conditions"] = pass_conditions
    result["status"] = "PASS" if all(pass_conditions.values()) else "FAIL"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"wrote {OUTPUT_PATH}")
    print(json.dumps({"status": result["status"], "pass_conditions": pass_conditions}, indent=2))


if __name__ == "__main__":
    main()
