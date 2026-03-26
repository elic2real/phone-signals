#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow.parquet as pq

from cache_key_utils import script_hash
from session_cache import build_session_cache_manager


TARGET = 2.5
SESSION_CONFIG = {
    "sydney": {"tz": "Australia/Sydney", "start_hour": 7},
    "asia": {"tz": "Asia/Tokyo", "start_hour": 7},
    "london": {"tz": "Europe/London", "start_hour": 7},
    "new_york": {"tz": "America/New_York", "start_hour": 7},
}


def parse_ts(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def pip_size(pair: str) -> float:
    return 0.01 if pair.upper().endswith("_JPY") else 0.0001


def signed_pips(direction: str, start: float, end: float, pair: str = "EUR_USD") -> float:
    raw = (end - start) / pip_size(pair)
    return raw if direction == "LONG" else -raw


def quarter_from_dt(dt: datetime, session: str = "london") -> str:
    cfg = SESSION_CONFIG.get(session, SESSION_CONFIG["london"])
    local_dt = dt.astimezone(ZoneInfo(cfg["tz"]))
    minute_of_session = (local_dt.hour - int(cfg["start_hour"])) * 60 + local_dt.minute
    if minute_of_session < 120:
        return "Q1"
    if minute_of_session < 240:
        return "Q2"
    if minute_of_session < 360:
        return "Q3"
    return "Q4"


def directional_pressure(prices: list[float], direction: str, window: int, pair: str) -> float:
    if len(prices) < 2:
        return 0.0
    window = min(window, len(prices) - 1)
    diffs = [(prices[i] - prices[i - 1]) / pip_size(pair) for i in range(len(prices) - window, len(prices))]
    signed = [d if direction == "LONG" else -d for d in diffs]
    pos = sum(max(0.0, d) for d in signed)
    neg = sum(abs(min(0.0, d)) for d in signed)
    return (pos - neg) / max(pos + neg, 1e-9)


def close_position(prices: list[float], direction: str, window: int) -> float:
    if len(prices) < 2:
        return 0.5
    seg = prices[-min(window, len(prices)) :]
    hi = max(seg)
    lo = min(seg)
    pos = (seg[-1] - lo) / max(hi - lo, 1e-9)
    return pos if direction == "LONG" else (1.0 - pos)


def _load_prices_uncached(data_root: Path) -> dict[str, list[dict[str, Any]]]:
    print("\n[INSTRUMENTATION] Starting load_prices", file=sys.stderr, flush=True)
    print(f"[INSTRUMENTATION] data_root: {data_root}", file=sys.stderr, flush=True)

    discovery_start = time.time()
    parquet_files = sorted(data_root.rglob("part-000.parquet"))
    discovery_elapsed = time.time() - discovery_start

    print(f"[INSTRUMENTATION] Discovered {len(parquet_files)} parquet files in {discovery_elapsed:.2f}s", file=sys.stderr, flush=True)
    for i, p in enumerate(parquet_files[:10]):
        print(f"[INSTRUMENTATION]   File {i+1}: {p}", file=sys.stderr, flush=True)
    if len(parquet_files) > 10:
        print(f"[INSTRUMENTATION]   ... and {len(parquet_files) - 10} more files", file=sys.stderr, flush=True)

    by_session: dict[str, list[dict[str, Any]]] = defaultdict(list)
    pairs_encountered = set()
    total_rows_processed = 0
    last_checkpoint = time.time()

    for file_idx, p in enumerate(parquet_files):
        file_start = time.time()
        pair = p.parent.parent.parent.name.split("=", 1)[-1]
        pairs_encountered.add(pair)

        print(f"[INSTRUMENTATION] Processing file {file_idx+1}/{len(parquet_files)}: {pair} - {p.name}", file=sys.stderr, flush=True)

        available = set(pq.read_schema(p).names)
        columns = [c for c in ["timestamp", "close", "pair", "session_id", "session", "weekday"] if c in available]

        read_start = time.time()
        df = pd.read_parquet(p, columns=columns)
        read_elapsed = time.time() - read_start

        row_count = len(df)
        total_rows_processed += row_count
        print(f"[INSTRUMENTATION]   Read {row_count} rows in {read_elapsed:.2f}s", file=sys.stderr, flush=True)

        parse_start = time.time()
        for rec_idx, rec in enumerate(df.to_dict("records")):
            dt = parse_ts(str(rec["timestamp"]))
            session_id = str(rec.get("session_id") or dt.date().isoformat())
            by_session[session_id].append(
                {
                    "timestamp": str(rec["timestamp"]),
                    "dt": dt,
                    "price": float(rec["close"]),
                    "pair": str(rec.get("pair") or pair),
                    "session_id": session_id,
                    "session": str(rec.get("session") or "london").lower(),
                    "weekday": str(rec.get("weekday") or dt.strftime("%A").lower()).lower(),
                }
            )

            if time.time() - last_checkpoint > 10:
                print(
                    f"[INSTRUMENTATION] CHECKPOINT: File {file_idx+1}/{len(parquet_files)}, Row {rec_idx+1}/{row_count}, Total rows: {total_rows_processed}",
                    file=sys.stderr,
                    flush=True,
                )
                last_checkpoint = time.time()

        parse_elapsed = time.time() - parse_start
        file_elapsed = time.time() - file_start
        print(f"[INSTRUMENTATION]   Parsed {row_count} rows in {parse_elapsed:.2f}s (total file time: {file_elapsed:.2f}s)", file=sys.stderr, flush=True)

    print(f"[INSTRUMENTATION] All files processed. Total rows: {total_rows_processed}", file=sys.stderr, flush=True)
    print(f"[INSTRUMENTATION] Pairs encountered: {sorted(pairs_encountered)}", file=sys.stderr, flush=True)
    print(f"[INSTRUMENTATION] Unique sessions: {len(by_session)}", file=sys.stderr, flush=True)

    sort_start = time.time()
    for rows in by_session.values():
        rows.sort(key=lambda r: r["dt"])
    sort_elapsed = time.time() - sort_start
    print(f"[INSTRUMENTATION] Sorted all sessions in {sort_elapsed:.2f}s", file=sys.stderr, flush=True)

    return by_session


def load_prices(data_root: Path, cache_manager=None) -> dict[str, list[dict[str, Any]]]:
    if cache_manager and cache_manager.has_complete_cache():
        return cache_manager.load_all_sessions()
    by_session = _load_prices_uncached(data_root)
    if cache_manager:
        cache_manager.store_sessions(by_session)
    return by_session


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def compute_stream_features(session_rows: list[dict[str, Any]], idx: int, direction: str) -> dict[str, float]:
    prices = [r["price"] for r in session_rows[: idx + 1]]
    pair = session_rows[idx].get("pair", "EUR_USD")
    last = prices[-1]
    p5 = directional_pressure(prices, direction, 5, pair)
    p15 = directional_pressure(prices, direction, 15, pair)
    p30 = directional_pressure(prices, direction, 30, pair)
    session_name = session_rows[idx].get("session", "london")
    q = quarter_from_dt(session_rows[idx]["dt"], session_name)
    quarter_start_map = {"Q1": 0, "Q2": 120, "Q3": 240, "Q4": 360}
    start_idx = max(0, quarter_start_map[q])
    quarter_prices = [r["price"] for r in session_rows[start_idx : idx + 1]]
    qtd = directional_pressure(quarter_prices, direction, max(1, len(quarter_prices) - 1), pair) if len(quarter_prices) > 1 else 0.0
    running = directional_pressure(prices, direction, max(1, len(prices) - 1), pair)

    lookback = prices[-21:] if len(prices) >= 21 else prices
    if direction == "LONG":
        local_low = min(lookback)
        local_high = max(lookback)
        dist_low = (last - local_low) / pip_size(pair)
        dist_high = (local_high - last) / pip_size(pair)
        breakout_distance = max(0.0, (last - max(lookback[:-1])) / pip_size(pair)) if len(lookback) > 1 else 0.0
    else:
        local_low = min(lookback)
        local_high = max(lookback)
        dist_low = (last - local_low) / pip_size(pair)
        dist_high = (local_high - last) / pip_size(pair)
        breakout_distance = max(0.0, (min(lookback[:-1]) - last) / pip_size(pair)) if len(lookback) > 1 else 0.0

    diffs = [(prices[i] - prices[i - 1]) / pip_size(pair) for i in range(max(1, len(prices) - 10), len(prices))]
    signed = [d if direction == "LONG" else -d for d in diffs]
    velocity_now = signed[-1] if signed else 0.0
    velocity_3 = mean(signed[-3:]) if len(signed) >= 3 else (mean(signed) if signed else 0.0)
    velocity_change = velocity_now - (signed[-2] if len(signed) >= 2 else 0.0)
    adiffs = [abs(d) for d in diffs]
    compression = (max(lookback[-5:]) - min(lookback[-5:])) / max((max(lookback) - min(lookback)), 1e-9) if len(lookback) >= 5 else 1.0

    return {
        "pressure_5": p5,
        "pressure_15": p15,
        "pressure_30": p30,
        "pressure_ratio_5_15": p5 - p15,
        "pressure_ratio_15_30": p15 - p30,
        "session_relative_bias": p15 - running,
        "quarter_relative_bias": p5 - qtd,
        "directional_dominance_qtd": qtd,
        "signed_close_position_5": close_position(prices, direction, 5),
        "distance_to_local_low": dist_low,
        "distance_to_local_high": dist_high,
        "breakout_distance": breakout_distance,
        "velocity_now": velocity_now,
        "velocity_3": velocity_3,
        "velocity_change": velocity_change,
        "compression": compression,
        "recent_range_20": (max(lookback) - min(lookback)) / pip_size(pair) if len(lookback) > 1 else 0.0,
        "recent_vol_10": mean(adiffs) if adiffs else 0.0,
    }


def simulate_path(session_rows: list[dict[str, Any]], idx: int, direction: str) -> dict[str, Any]:
    start = session_rows[idx]["price"]
    pair = session_rows[idx].get("pair", "EUR_USD")
    mfe = 0.0
    mae = 0.0
    tp_hit = None
    sl_hit = None
    for fwd, row in enumerate(session_rows[idx + 1 :], start=1):
        sp = signed_pips(direction, start, row["price"], pair)
        mfe = max(mfe, sp)
        mae = min(mae, sp)
        if tp_hit is None and sp >= TARGET:
            tp_hit = fwd
        if sl_hit is None and sp <= -TARGET:
            sl_hit = fwd
        if fwd >= 100 and tp_hit is not None and sl_hit is not None:
            break
    if tp_hit is not None and (sl_hit is None or tp_hit <= sl_hit):
        outcome = "GOOD"
        static_pips = TARGET
    elif sl_hit is not None and (tp_hit is None or sl_hit < tp_hit):
        outcome = "BAD"
        static_pips = -TARGET
    else:
        outcome = "NOISE"
        static_pips = 0.0
    return {
        "future_mfe_pips": mfe,
        "future_mae_pips": abs(mae),
        "tp_hit_min": tp_hit or 0,
        "sl_hit_min": sl_hit or 0,
        "outcome_label": outcome,
        "static_pips": static_pips,
        "static_R": static_pips / TARGET,
    }


def cluster_impulses(stream_rows: list[dict[str, Any]], time_window_minutes: int = 15, price_threshold_pips: float = 1.5) -> list[dict[str, Any]]:
    """
    Group overlapping/consecutive triggers into impulse clusters.
    
    Consolidates raw observations that are part of the same price movement
    into distinct impulse clusters to prevent counting microstructure noise.
    
    Args:
        stream_rows: Raw observation rows sorted by timestamp
        time_window_minutes: Max time gap between observations in same cluster
        price_threshold_pips: Max price distance from cluster origin
    
    Returns:
        stream_rows with cluster_id, cluster_size, cluster_origin_ts added
    """
    if not stream_rows:
        return []
    
    # Group by session_id and direction for independent clustering
    by_session_direction = defaultdict(list)
    for row in stream_rows:
        key = (row["session_id"], row["direction_assumed"])
        by_session_direction[key].append(row)
    
    result = []
    global_cluster_id = 0
    
    for (session_id, direction), rows in by_session_direction.items():
        # Sort by timestamp then by a stable secondary key to ensure deterministic ordering
        rows.sort(key=lambda r: (r["dt"], r.get("price", 0)))
        
        # First pass: merge exact duplicate timestamps into single observations
        # This prevents the same timestamp from creating multiple cluster origins
        unique_timestamps = {}
        for row in rows:
            ts_key = (row["timestamp"], row["price"])
            if ts_key not in unique_timestamps:
                unique_timestamps[ts_key] = row
        
        # Use deduplicated rows for clustering
        deduped_rows = sorted(unique_timestamps.values(), key=lambda r: (r["dt"], r.get("price", 0)))
        
        clusters = []
        current_cluster = []
        
        for row in deduped_rows:
            if not current_cluster:
                current_cluster = [row]
                continue
            
            # Check if this row belongs to current cluster
            time_diff_minutes = (row["dt"] - current_cluster[0]["dt"]).total_seconds() / 60
            price_diff_pips = abs(row["price"] - current_cluster[0]["price"]) / pip_size(row.get("pair", "EUR_USD"))
            
            # Same cluster if within time window AND price threshold
            if time_diff_minutes <= time_window_minutes and price_diff_pips <= price_threshold_pips:
                current_cluster.append(row)
            else:
                # Close current cluster, start new one
                clusters.append(current_cluster)
                current_cluster = [row]
        
        # Don't forget last cluster
        if current_cluster:
            clusters.append(current_cluster)
        
        # Assign cluster IDs and metadata
        for cluster in clusters:
            cluster_origin_ts = cluster[0]["timestamp"]
            cluster_origin_dt = cluster[0]["dt"]
            
            for row in cluster:
                row["cluster_id"] = global_cluster_id
                row["cluster_size"] = len(cluster)
                row["cluster_origin_ts"] = cluster_origin_ts
                row["cluster_age_minutes"] = (row["dt"] - cluster_origin_dt).total_seconds() / 60
                result.append(row)
            
            global_cluster_id += 1
    
    return result


def derive_entry_windows(stream_rows: list[dict[str, Any]], window_duration_minutes: int = 5) -> list[dict[str, Any]]:
    """
    Identify valid entry windows from clustered impulses.
    
    An entry window opens at the start of each impulse cluster and remains
    open for a fixed duration or until the impulse invalidates.
    
    Args:
        stream_rows: Clustered observation rows
        window_duration_minutes: How long entry window stays open
    
    Returns:
        stream_rows with entry_window_id, entry_window_open_ts, entry_window_close_ts added
    """
    if not stream_rows:
        return []
    
    # Group by cluster_id
    by_cluster = defaultdict(list)
    for row in stream_rows:
        by_cluster[row["cluster_id"]].append(row)
    
    result = []
    entry_window_id = 0
    
    for cluster_id, rows in by_cluster.items():
        rows.sort(key=lambda r: r["dt"])
        
        # Entry window opens at cluster origin
        window_open_dt = rows[0]["dt"]
        window_open_ts = rows[0]["timestamp"]
        
        # Window closes after duration or at cluster end
        window_close_dt = window_open_dt + pd.Timedelta(minutes=window_duration_minutes)
        
        for row in rows:
            row["entry_window_id"] = entry_window_id
            row["entry_window_open_ts"] = window_open_ts
            row["entry_window_close_ts"] = window_close_dt.isoformat()
            row["is_entry_window_open"] = row["dt"] <= window_close_dt
            result.append(row)
        
        entry_window_id += 1
    
    return result


def derive_action_truth(direction: str, outcome: str, path: dict[str, Any], feats: dict[str, float]) -> str:
    bias_aligned = feats["directional_dominance_qtd"] > 0.05
    strong_bias_aligned = feats["directional_dominance_qtd"] > 0.12
    fast_trigger = path["tp_hit_min"] and path["tp_hit_min"] <= 8
    strong_transition = (
        feats["pressure_5"] > 0.18
        and feats["pressure_ratio_5_15"] > 0.10
        and feats["compression"] < 0.60
        and feats["signed_close_position_5"] > 0.55
    )
    breakout_trigger = feats["breakout_distance"] > 0.20 and feats["velocity_now"] > -0.10
    hold_continuation = (
        feats["pressure_15"] > 0.12
        and feats["velocity_3"] > -0.05
        and feats["quarter_relative_bias"] > -0.05
    )
    bias_trigger = (
        feats["pressure_15"] > 0.18
        and feats["signed_close_position_5"] > 0.70
        and feats["compression"] < 0.55
        and feats["velocity_now"] > -0.05
    )
    short_bias_trigger = (
        direction == "SHORT"
        and strong_bias_aligned
        and feats["pressure_5"] > 0.08
        and feats["pressure_ratio_5_15"] > -0.05
        and feats["signed_close_position_5"] > 0.62
        and feats["recent_vol_10"] > 0.45
    )
    harvest_setup = (
        path["future_mfe_pips"] > TARGET
        and path["tp_hit_min"] > 8
        and (feats["pressure_5"] <= 0.10 or feats["velocity_now"] <= 0.10)
    )

    if outcome == "GOOD":
        if fast_trigger and (strong_transition or breakout_trigger):
            return f"ENTER_{direction}"
        if bias_aligned and bias_trigger and path["future_mfe_pips"] >= TARGET * 1.6:
            return f"ENTER_{direction}"
        if short_bias_trigger and path["future_mfe_pips"] >= TARGET * 1.4:
            return f"ENTER_{direction}"
        if hold_continuation and path["future_mfe_pips"] >= TARGET * 1.5:
            return f"HOLD_{direction}"
        if harvest_setup or (bias_aligned and path["future_mfe_pips"] > TARGET * 1.2):
            return f"HARVEST_{direction}"
        return "DO_NOT_ENTER"

    if outcome == "BAD":
        if (
            feats["pressure_5"] < -0.25
            or feats["pressure_ratio_5_15"] < -0.20
            or feats["velocity_now"] < -0.40
            or feats["velocity_change"] < -0.30
        ):
            return f"PANIC_{direction}"
        return "DO_NOT_ENTER"

    if bias_aligned and path["future_mfe_pips"] > TARGET * 1.1 and feats["pressure_15"] > 0.10:
        return f"HARVEST_{direction}"
    return "DO_NOT_ENTER"


def validate_stage6_integrity(raw_count: int, cluster_count: int, entry_window_count: int, session_count: int, stream_rows: list[dict[str, Any]] = None) -> dict[str, Any]:
    """
    Validate Stage 6 integrity and flag physically impossible patterns.
    
    Returns dict with:
        valid: bool
        violations: list of violation codes
        warnings: list of warning messages
    """
    violations = []
    warnings = []
    
    # Physical limits for a typical 8-hour trading session
    TYPICAL_SESSION_HOURS = 8
    MAX_IMPULSES_PER_HOUR = 80  # Accounts for breakouts and microstructure noise
    MAX_IMPULSES_PER_SESSION = TYPICAL_SESSION_HOURS * MAX_IMPULSES_PER_HOUR
    
    # Collapse ratio bounds
    MIN_CLUSTER_COLLAPSE = 0.002  # If <0.2%, clustering is too aggressive (500:1)
    MAX_CLUSTER_COLLAPSE = 0.95   # If >95%, clustering is too weak (almost 1:1)
    
    # Average cluster size bounds
    MIN_AVG_CLUSTER_SIZE = 1.5    # Should consolidate at least some observations
    MAX_AVG_CLUSTER_SIZE = 400    # Slow grind trends can have very large clusters
    
    avg_cluster_size = raw_count / cluster_count if cluster_count > 0 else 0
    cluster_collapse_ratio = cluster_count / raw_count if raw_count > 0 else 0
    
    # RED FLAG 1: Extreme cluster collapse (too much consolidation)
    if cluster_collapse_ratio < MIN_CLUSTER_COLLAPSE or avg_cluster_size > MAX_AVG_CLUSTER_SIZE:
        violations.append("EXTREME_CLUSTER_COLLAPSE")
        warnings.append(f"⛔ Cluster collapse ratio {cluster_collapse_ratio:.4f} < {MIN_CLUSTER_COLLAPSE} OR avg cluster size {avg_cluster_size:.0f} > {MAX_AVG_CLUSTER_SIZE} - clustering too aggressive")
    
    # RED FLAG 2: Weak clustering (almost no consolidation)
    if cluster_collapse_ratio > MAX_CLUSTER_COLLAPSE:
        violations.append("WEAK_CLUSTERING")
        warnings.append(f"⛔ Cluster collapse ratio {cluster_collapse_ratio:.4f} > {MAX_CLUSTER_COLLAPSE} - clustering too weak (almost 1:1)")
    
    # RED FLAG 3: Physically impossible impulse count
    expected_max_clusters = session_count * MAX_IMPULSES_PER_SESSION
    if cluster_count > expected_max_clusters:
        violations.append("IMPOSSIBLE_IMPULSE_COUNT")
        warnings.append(f"⛔ Cluster count {cluster_count} > {expected_max_clusters} max for {session_count} sessions - physically impossible")
    
    # RED FLAG 4: Insufficient consolidation
    if avg_cluster_size < MIN_AVG_CLUSTER_SIZE and cluster_count > 0:
        violations.append("INSUFFICIENT_CONSOLIDATION")
        warnings.append(f"⚠️  Avg cluster size {avg_cluster_size:.1f} < {MIN_AVG_CLUSTER_SIZE} - insufficient consolidation")
    
    # RED FLAG 5: Entry window count mismatch
    if entry_window_count > cluster_count * 1.1:  # Allow 10% variance
        violations.append("ENTRY_WINDOW_INFLATION")
        warnings.append(f"⛔ Entry windows {entry_window_count} > clusters {cluster_count} - entry window logic is inflating supply")
    
    # RED FLAG 6: Zero clusters (complete failure)
    if cluster_count == 0 and raw_count > 0:
        violations.append("ZERO_CLUSTERS")
        warnings.append(f"⛔ Zero clusters from {raw_count} raw observations - clustering completely failed")
    
    # RED FLAG 7: Duplicate cluster origins (same impulse counted multiple times)
    if stream_rows:
        # Group by cluster_id to check for duplicate origins within same cluster
        cluster_origin_map = {}
        for row in stream_rows:
            cluster_id = row.get("cluster_id")
            origin_key = (row.get("cluster_origin_ts"), row.get("session_id"), row.get("direction_assumed"))
            
            if cluster_id not in cluster_origin_map:
                cluster_origin_map[cluster_id] = origin_key
            elif cluster_origin_map[cluster_id] != origin_key:
                # Same cluster_id with different origin - this is a real bug
                violations.append("DUPLICATE_CLUSTER_ORIGINS")
                warnings.append(f"⛔ Cluster {cluster_id} has inconsistent origin timestamps - clustering logic is broken")
                break
    
    # WARNING: Low cluster count (might be valid for quiet sessions)
    avg_clusters_per_session = cluster_count / session_count if session_count > 0 else 0
    if avg_clusters_per_session < 5 and cluster_count > 0:
        warnings.append(f"⚠️  Only {avg_clusters_per_session:.1f} clusters/session - verify this is a quiet period")
    
    # WARNING: High cluster count (might be valid for volatile sessions)
    if avg_clusters_per_session > MAX_IMPULSES_PER_HOUR * 4:  # 4 hours worth
        warnings.append(f"⚠️  {avg_clusters_per_session:.0f} clusters/session - verify this is a volatile period")
    
    return {
        "valid": len(violations) == 0,
        "violations": violations,
        "warnings": warnings,
        "metrics": {
            "raw_count": raw_count,
            "cluster_count": cluster_count,
            "entry_window_count": entry_window_count,
            "session_count": session_count,
            "avg_cluster_size": round(avg_cluster_size, 2),
            "cluster_collapse_ratio": round(cluster_collapse_ratio, 4),
            "avg_clusters_per_session": round(avg_clusters_per_session, 2)
        }
    }


def build_stream_and_truth(data_root: Path, cache_manager=None) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_session = load_prices(data_root, cache_manager=cache_manager)
    
    # Step 1: Generate raw observations
    print("[STAGE 6] Generating raw observations...", file=sys.stderr, flush=True)
    raw_stream_rows: list[dict[str, Any]] = []
    truth_rows: list[dict[str, Any]] = []
    
    for session_id, rows in sorted(by_session.items()):
        for idx, row in enumerate(rows):
            if idx < 30:
                continue
            quarter = quarter_from_dt(row["dt"], row.get("session", "london"))
            for direction in ("LONG", "SHORT"):
                feats = compute_stream_features(rows, idx, direction)
                path = simulate_path(rows, idx, direction)
                action = derive_action_truth(direction, path["outcome_label"], path, feats)
                base = {
                    "timestamp": row["timestamp"],
                    "dt": row["dt"],  # Keep dt for clustering
                    "session_id": session_id,
                    "quarter": quarter,
                    "direction_assumed": direction,
                    "price": row["price"],
                    "pair": row.get("pair", "EUR_USD"),
                    **feats,
                    **path,
                }
                raw_stream_rows.append(base)
                truth_rows.append(
                    {
                        **base,
                        "action_truth": action,
                    }
                )
    
    raw_count = len(raw_stream_rows)
    session_count = len(by_session)
    print(f"[STAGE 6] Raw observations: {raw_count}", file=sys.stderr, flush=True)
    print(f"[STAGE 6] Sessions: {session_count}", file=sys.stderr, flush=True)
    
    # Step 2: Apply impulse clustering
    print("[STAGE 6] Clustering impulses...", file=sys.stderr, flush=True)
    clustered_stream_rows = cluster_impulses(raw_stream_rows, time_window_minutes=10, price_threshold_pips=1.0)
    cluster_count = len(set(row["cluster_id"] for row in clustered_stream_rows))
    print(f"[STAGE 6] Impulse clusters: {cluster_count}", file=sys.stderr, flush=True)
    print(f"[STAGE 6] Cluster collapse ratio: {cluster_count / raw_count:.4f} ({raw_count / cluster_count:.1f} obs/cluster)", file=sys.stderr, flush=True)
    
    # Step 3: Derive entry windows
    print("[STAGE 6] Deriving entry windows...", file=sys.stderr, flush=True)
    stream_rows = derive_entry_windows(clustered_stream_rows, window_duration_minutes=5)
    entry_window_count = len(set(row["entry_window_id"] for row in stream_rows))
    print(f"[STAGE 6] Entry windows: {entry_window_count}", file=sys.stderr, flush=True)
    
    # Step 4: Validate integrity
    print("[STAGE 6] Validating integrity...", file=sys.stderr, flush=True)
    validation = validate_stage6_integrity(raw_count, cluster_count, entry_window_count, session_count, stream_rows)
    
    if not validation["valid"]:
        print(f"[STAGE 6] ⛔ INTEGRITY VIOLATIONS DETECTED:", file=sys.stderr, flush=True)
        for violation in validation["violations"]:
            print(f"[STAGE 6]   - {violation}", file=sys.stderr, flush=True)
    
    for warning in validation["warnings"]:
        print(f"[STAGE 6] {warning}", file=sys.stderr, flush=True)
    
    if validation["valid"]:
        print(f"[STAGE 6] ✅ Stage 6 integrity validated", file=sys.stderr, flush=True)
    
    # Apply same clustering to truth rows
    clustered_truth_rows = cluster_impulses(truth_rows, time_window_minutes=10, price_threshold_pips=1.0)
    truth_rows = derive_entry_windows(clustered_truth_rows, window_duration_minutes=5)
    
    return stream_rows, truth_rows


def summarize_transitions(truth_rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_action = Counter(r["action_truth"] for r in truth_rows)
    by_quarter = Counter((r["quarter"], r["action_truth"]) for r in truth_rows)
    focus_actions = ["ENTER_LONG", "ENTER_SHORT", "HOLD_LONG", "HOLD_SHORT", "HARVEST_LONG", "HARVEST_SHORT", "PANIC_LONG", "PANIC_SHORT", "DO_NOT_ENTER"]
    action_profiles = {}
    for action in focus_actions:
        rows = [r for r in truth_rows if r["action_truth"] == action]
        if not rows:
            continue
        action_profiles[action] = {
            "count": len(rows),
            "pressure_5_mean": mean(r["pressure_5"] for r in rows),
            "pressure_15_mean": mean(r["pressure_15"] for r in rows),
            "quarter_bias_mean": mean(r["directional_dominance_qtd"] for r in rows),
            "velocity_now_mean": mean(r["velocity_now"] for r in rows),
            "compression_mean": mean(r["compression"] for r in rows),
            "future_mfe_mean": mean(r["future_mfe_pips"] for r in rows),
            "future_mae_mean": mean(r["future_mae_pips"] for r in rows),
        }
    return {
        "row_count": len(truth_rows),
        "action_counts": dict(by_action),
        "quarter_action_counts": {f"{q}|{a}": c for (q, a), c in by_quarter.items()},
        "action_profiles": action_profiles,
    }


def derive_unified_surface(truth_rows: list[dict[str, Any]]) -> dict[str, Any]:
    enter_long_rows = [r for r in truth_rows if r["action_truth"] == "ENTER_LONG"]
    enter_short_rows = [r for r in truth_rows if r["action_truth"] == "ENTER_SHORT"]
    hold_rows = [r for r in truth_rows if r["action_truth"] in {"HOLD_LONG", "HOLD_SHORT"}]
    harvest_rows = [r for r in truth_rows if r["action_truth"] in {"HARVEST_LONG", "HARVEST_SHORT"}]
    panic_rows = [r for r in truth_rows if r["action_truth"] in {"PANIC_LONG", "PANIC_SHORT"}]
    return {
        "logic": "stream_action_surface_v1",
        "enter_long": {
            "pressure_5_min": mean(r["pressure_5"] for r in enter_long_rows) if enter_long_rows else 0.0,
            "directional_dominance_qtd_min": mean(r["directional_dominance_qtd"] for r in enter_long_rows) if enter_long_rows else 0.0,
            "compression_max": mean(r["compression"] for r in enter_long_rows) if enter_long_rows else 1.0,
        },
        "enter_short": {
            "pressure_5_min": mean(r["pressure_5"] for r in enter_short_rows) if enter_short_rows else 0.0,
            "directional_dominance_qtd_min": mean(r["directional_dominance_qtd"] for r in enter_short_rows) if enter_short_rows else 0.0,
            "compression_max": mean(r["compression"] for r in enter_short_rows) if enter_short_rows else 1.0,
        },
        "hold": {
            "pressure_15_min": mean(r["pressure_15"] for r in hold_rows) if hold_rows else 0.0,
            "velocity_floor": mean(r["velocity_now"] for r in hold_rows) if hold_rows else 0.0,
        },
        "harvest": {
            "future_mfe_floor": mean(r["future_mfe_pips"] for r in harvest_rows) if harvest_rows else 0.0,
            "pressure_5_floor": mean(r["pressure_5"] for r in harvest_rows) if harvest_rows else 0.0,
        },
        "panic": {
            "pressure_5_ceiling": mean(r["pressure_5"] for r in panic_rows) if panic_rows else 0.0,
            "velocity_ceiling": mean(r["velocity_now"] for r in panic_rows) if panic_rows else 0.0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--session-cache-dir", type=Path, default=None)
    args = parser.parse_args()

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    cache_manager = build_session_cache_manager(
        data_root=args.data_root,
        script_path=Path(__file__),
        cache_dir=args.session_cache_dir,
        extra={"script_hash": script_hash(Path(__file__))},
    )

    stream_rows, truth_rows = build_stream_and_truth(args.data_root, cache_manager=cache_manager)

    stream_fields = [
        "timestamp", "session_id", "quarter", "direction_assumed", "price",
        "cluster_id", "cluster_size", "cluster_origin_ts", "cluster_age_minutes",
        "entry_window_id", "entry_window_open_ts", "entry_window_close_ts", "is_entry_window_open",
        "pressure_5", "pressure_15", "pressure_30",
        "pressure_ratio_5_15", "pressure_ratio_15_30",
        "session_relative_bias", "quarter_relative_bias", "directional_dominance_qtd",
        "signed_close_position_5", "distance_to_local_low", "distance_to_local_high",
        "breakout_distance", "velocity_now", "velocity_3", "velocity_change",
        "compression", "recent_range_20", "recent_vol_10",
        "future_mfe_pips", "future_mae_pips", "tp_hit_min", "sl_hit_min",
        "outcome_label", "static_pips", "static_R",
    ]
    truth_fields = stream_fields + ["action_truth"]

    write_csv(out_dir / "session_energy_state_stream.csv", stream_rows, stream_fields)
    write_csv(out_dir / "state_action_truth_table.csv", truth_rows, truth_fields)

    transition_report = summarize_transitions(truth_rows)
    unified_surface = derive_unified_surface(truth_rows)

    (out_dir / "state_transition_report.json").write_text(json.dumps(transition_report, indent=2))
    (out_dir / "unified_action_surface.json").write_text(json.dumps(unified_surface, indent=2))

    # Compute Stage 6 integrity metrics
    raw_observation_count = len(stream_rows)
    cluster_count = len(set(r["cluster_id"] for r in stream_rows))
    entry_window_count = len(set(r["entry_window_id"] for r in stream_rows))
    session_count = len(set(r["session_id"] for r in stream_rows))
    avg_cluster_size = raw_observation_count / cluster_count if cluster_count > 0 else 0
    
    # Re-run validation for report
    validation = validate_stage6_integrity(raw_observation_count, cluster_count, entry_window_count, session_count, stream_rows)
    
    summary = {
        "stream_rows": len(stream_rows),
        "truth_rows": len(truth_rows),
        "action_counts": transition_report["action_counts"],
        "quarters": sorted({r["quarter"] for r in truth_rows}),
        "stage_6_integrity": {
            "raw_observation_count": raw_observation_count,
            "cluster_count": cluster_count,
            "entry_window_count": entry_window_count,
            "session_count": session_count,
            "avg_cluster_size": round(avg_cluster_size, 2),
            "cluster_collapse_ratio": round(cluster_count / raw_observation_count, 4) if raw_observation_count > 0 else 0,
            "entry_from_cluster_ratio": round(entry_window_count / cluster_count, 4) if cluster_count > 0 else 0,
            "avg_clusters_per_session": round(cluster_count / session_count, 2) if session_count > 0 else 0,
            "validation": {
                "valid": validation["valid"],
                "violations": validation["violations"],
                "warnings": validation["warnings"]
            }
        }
    }
    (out_dir / "session_state_build_report.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
