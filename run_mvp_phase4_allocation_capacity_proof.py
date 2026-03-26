#!/usr/bin/env python3
"""Build MVP Phase 4 allocation-capacity proof metrics under locked scope.

This runner is measurement-only. It does not tune AEE, priority, or routing.
It quantifies how finite slot capacity constrains candidate conversion.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd


CONFIG_PATH = Path("entry_v23_policy_guarded_active.json")
TRUE_STREAM_PATH = Path("global_true_candidate_stream.parquet")
OUTPUT_PATH = Path("control/mvp_phase4_allocation_capacity_proof.json")
A_GRADE_MIN_COMPOSITE_SCORE = 0.80
CAPACITY_LEVELS = [1, 2, 3]


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


def _simulate_capacity(df: pd.DataFrame, capacity: int) -> Dict[str, Any]:
    ranked = df.sort_values(["timestamp", "composite_score"], ascending=[True, False]).copy()
    accepted = ranked.groupby("timestamp", as_index=False).head(capacity).copy()
    rejected = ranked.drop(accepted.index).copy()

    a_grade_all = ranked[ranked["composite_score"] >= A_GRADE_MIN_COMPOSITE_SCORE]
    a_grade_acc = accepted[accepted["composite_score"] >= A_GRADE_MIN_COMPOSITE_SCORE]
    a_grade_rej = rejected[rejected["composite_score"] >= A_GRADE_MIN_COMPOSITE_SCORE]

    ts_sizes = ranked.groupby("timestamp").size()
    ts_bound = int((ts_sizes > capacity).sum())

    hour_all = ranked["timestamp"].dt.floor("h").value_counts().sort_index()
    hour_acc = accepted["timestamp"].dt.floor("h").value_counts().sort_index()

    return {
        "capacity_per_timestamp": capacity,
        "accepted_candidates": int(accepted.shape[0]),
        "rejected_candidates": int(rejected.shape[0]),
        "rejection_rate": float(rejected.shape[0] / ranked.shape[0]) if ranked.shape[0] else 0.0,
        "binding_timestamps": ts_bound,
        "binding_rate": float(ts_bound / ts_sizes.shape[0]) if ts_sizes.shape[0] else 0.0,
        "a_grade_total": int(a_grade_all.shape[0]),
        "a_grade_accepted": int(a_grade_acc.shape[0]),
        "a_grade_rejected": int(a_grade_rej.shape[0]),
        "a_grade_rejection_rate": float(a_grade_rej.shape[0] / a_grade_all.shape[0]) if a_grade_all.shape[0] else 0.0,
        "accepted_avg_score": float(accepted["composite_score"].mean()) if not accepted.empty else 0.0,
        "rejected_avg_score": float(rejected["composite_score"].mean()) if not rejected.empty else 0.0,
        "avg_candidates_per_hour": float(hour_all.mean()) if not hour_all.empty else 0.0,
        "avg_accepted_per_hour": float(hour_acc.mean()) if not hour_acc.empty else 0.0,
    }


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
            "task_id": "MVP_PHASE4_ALLOCATION_CAPACITY_PROOF",
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

    baseline = {
        "total_candidates": int(locked.shape[0]),
        "total_timestamps": int(locked["timestamp"].nunique()),
        "avg_pool_size": float(locked.groupby("timestamp").size().mean()),
        "peak_pool_size": int(locked.groupby("timestamp").size().max()),
        "a_grade_total": int((locked["composite_score"] >= A_GRADE_MIN_COMPOSITE_SCORE).sum()),
    }

    scenarios = [_simulate_capacity(locked, c) for c in CAPACITY_LEVELS]
    by_cap = {s["capacity_per_timestamp"]: s for s in scenarios}

    cap1 = by_cap[1]
    cap2 = by_cap[2]
    cap3 = by_cap[3]

    incremental = {
        "accepted_gain_1_to_2": cap2["accepted_candidates"] - cap1["accepted_candidates"],
        "accepted_gain_2_to_3": cap3["accepted_candidates"] - cap2["accepted_candidates"],
        "a_grade_gain_1_to_2": cap2["a_grade_accepted"] - cap1["a_grade_accepted"],
        "a_grade_gain_2_to_3": cap3["a_grade_accepted"] - cap2["a_grade_accepted"],
    }

    result = {
        "protocol": "RCP",
        "protocol_version": "RCP_V2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "task_id": "MVP_PHASE4_ALLOCATION_CAPACITY_PROOF",
        "scope_lock": {
            "pairs": sorted(include_pairs),
            "sessions": sorted(include_sessions),
        },
        "allocation_model": {
            "ranking_key": "composite_score_desc",
            "capacity_levels_tested": CAPACITY_LEVELS,
            "tuning_changed": False,
        },
        "baseline_pool": baseline,
        "capacity_scenarios": scenarios,
        "incremental_relief": incremental,
    }

    pass_conditions = {
        "finite_capacity_binds_at_cap1": cap1["binding_timestamps"] > 0,
        "candidate_blocking_exists_at_cap1": cap1["rejected_candidates"] > 0,
        "a_grade_blocking_exists_at_cap1": cap1["a_grade_rejected"] > 0,
        "additional_capacity_relieves_pressure": incremental["accepted_gain_1_to_2"] > 0,
    }

    result["pass_conditions"] = pass_conditions
    result["status"] = "PASS" if all(pass_conditions.values()) else "FAIL"

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print(f"wrote {OUTPUT_PATH}")
    print(json.dumps({"status": result["status"], "pass_conditions": pass_conditions}, indent=2))


if __name__ == "__main__":
    main()
