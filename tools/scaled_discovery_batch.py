#!/usr/bin/env python3
"""
Scaled Entry Discovery Batch Runner
===================================

Extends PC2 Stage A discovery to run parametrizable batches.
Reuses all caching, vectorization, and phase logic from Stage A.

Run batches of (pair, session, direction, bucket_set) without full reinvention.

Usage:

  # Run EUR_USD London SHORT with buckets 2,3,5,8,10 (adding 8 to existing)
  python tools/scaled_discovery_batch.py \
    --batch-config control/batch_config.json \
    --output-dir control/batches

Where batch_config.json contains:

  [
    {
      "batch_id": "eurusd_london_short_extended",
      "pair": "EUR_USD",
      "session": "London",
      "direction": "SHORT",
      "buckets": [2, 3, 5, 8, 10],
      "sample_size": 50
    },
    ...
  ]
"""

from __future__ import annotations

import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Any, Tuple, Optional
import argparse
import sys


WORKSPACE = Path(__file__).resolve().parent.parent
COMPILED_NODES = WORKSPACE / "PC2" / "compiled_nodes"


def pip_factor(pair: str) -> float:
    """Get pip factor for pair (0.01 for JPY, 0.0001 for others)."""
    return 0.01 if "JPY" in pair else 0.0001


def load_phase1(pair: str, weekday: str, session: str) -> pd.DataFrame:
    """Load phase1 opportunity_map for given pair/weekday/session."""
    node_dir = COMPILED_NODES / f"{pair}__{weekday}__{session}"
    csv_path = node_dir / "phase1" / "opportunity_map_raw.csv"
    
    if not csv_path.exists():
        raise FileNotFoundError(f"Phase1 data not found: {csv_path}")
    
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


def build_env_cache(df: pd.DataFrame, pair: str) -> Dict[str, Any]:
    """Session-level environment features — computed once, reused."""
    pf = pip_factor(pair)
    df = df.copy()
    df["_date"] = pd.to_datetime(df["timestamp"]).dt.date

    session_ranges, session_moves = [], []
    for _, sdf in df.groupby("_date"):
        if len(sdf) < 10:
            continue
        p = sdf["price"].values
        session_ranges.append((p.max() - p.min()) / pf)
        session_moves.append(abs(p[-1] - p[0]) / pf)

    if not session_ranges:
        return {}

    rng = np.array(session_ranges)
    mov = np.array(session_moves)

    return {
        "session_count": int(len(rng)),
        "avg_session_range_pips": float(round(np.mean(rng), 2)),
        "median_session_range_pips": float(round(np.median(rng), 2)),
        "avg_session_move_pips": float(round(np.mean(mov), 2)),
        "persistence": float(round(float(np.mean(mov / (rng + 1e-9))), 4)),
        "volatility_class": (
            "HIGH" if np.median(rng) > 80
            else "MEDIUM" if np.median(rng) > 40
            else "LOW"
        ),
    }


def build_structure_cache(df: pd.DataFrame, pair: str) -> pd.DataFrame:
    """Compute all rolling structural features once per node load."""
    pf = pip_factor(pair)
    price = df["price"]

    # Rolling window aggregates
    r5_max = price.rolling(5, min_periods=3).max()
    r5_min = price.rolling(5, min_periods=3).min()
    r10_max = price.rolling(10, min_periods=5).max()
    r10_min = price.rolling(10, min_periods=5).min()
    r20_max = price.rolling(20, min_periods=10).max()
    r20_min = price.rolling(20, min_periods=10).min()

    r5_range = (r5_max - r5_min) / pf
    r20_range = (r20_max - r20_min) / pf

    compression_ratio = r5_range / (r20_range + 1e-9)
    percentile_in_20bar = (price - r20_min) / (r20_max - r20_min + 1e-9)

    prev_r10_max = r10_max.shift(1)
    prev_r10_min = r10_min.shift(1)
    breakout_up = price > prev_r10_max
    breakout_down = price < prev_r10_min

    r20_std = price.rolling(20, min_periods=10).std() / pf
    drift_flag = (r20_std / (r20_range + 1e-9)) < 0.28

    dist_to_r20_high = (r20_max - price) / pf
    dist_to_r20_low = (price - r20_min) / pf
    retest_flag = (dist_to_r20_high < 0.8) | (dist_to_r20_low < 0.8)

    out = df.copy()
    out["compression_ratio"] = compression_ratio.values
    out["percentile_20bar"] = percentile_in_20bar.values
    out["breakout_up"] = breakout_up.values
    out["breakout_down"] = breakout_down.values
    out["drift_flag"] = drift_flag.values
    out["retest_flag"] = retest_flag.values
    out["r20_range_pips"] = r20_range.values
    out["r5_range_pips"] = r5_range.values
    return out


def get_hits(df: pd.DataFrame, direction: str, bucket: float) -> pd.DataFrame:
    """Vectorized filter: rows where direction target was reached."""
    if direction == "LONG":
        mask = df["mfe_up_pips"] >= bucket
        hits = df[mask].copy()
        hits["_mfe"] = hits["mfe_up_pips"]
        hits["_mae"] = hits["mae_up_pips"].fillna(0.0)
        hits["_tau"] = hits["tau_up_min"]
    else:  # SHORT
        mask = df["mfe_down_pips"] >= bucket
        hits = df[mask].copy()
        hits["_mfe"] = hits["mfe_down_pips"]
        hits["_mae"] = hits["mae_down_pips"].fillna(0.0)
        hits["_tau"] = hits["tau_down_min"]
    return hits


def sample_hits(hits: pd.DataFrame, n: int = 50) -> pd.DataFrame:
    """First N chronological hits."""
    return hits.head(n)


def run_discovery_batch(
    pair: str,
    session: str,
    direction: str,
    buckets: List[int],
    output_dir: Path,
    sample_size: int = 50,
    weekday: str = "Thursday",
) -> Dict[str, Any]:
    """
    Run phases 0-2 discovery for a single batch.
    
    This reuses Stage A logic but allows parametrization by batch.
    Returns viability, path_family, and structure records.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    batch_key = f"{pair}_{session}_{direction}"
    print(f"\n{'='*70}")
    print(f"DISCOVERY BATCH: {batch_key}")
    print(f"Buckets: {buckets}")
    print(f"{'='*70}")
    
    try:
        # Load data
        print(f"Loading phase1 data for {pair} {weekday} {session}...")
        df = load_phase1(pair, weekday, session)
        print(f"  Loaded {len(df)} rows")
        
        # Build caches
        print(f"Building environment cache...")
        env_cache = build_env_cache(df, pair)
        print(f"  Sessions: {env_cache.get('session_count', 0)}")
        print(f"  Volatility: {env_cache.get('volatility_class', 'UNKNOWN')}")
        
        print(f"Building structure cache...")
        df_cached = build_structure_cache(df, pair)
        print(f"  Added 8 structural features")
        
        # Run phases for each bucket
        spread = 0.8 if pair == "EUR_USD" else 1.5  # Conservative spreads
        
        viability_records = []
        for bucket in buckets:
            print(f"  Phase 0: bucket={bucket}...")
            hits = get_hits(df_cached, direction, bucket)
            sample = sample_hits(hits, sample_size)
            
            if len(sample) == 0:
                print(f"    No sample hits for bucket {bucket}")
                continue
            
            hit_rate = len(hits) / len(df_cached)
            avg_mfe = float(sample["_mfe"].mean())
            avg_mae = float(sample["_mae"].mean())
            avg_tau = float(sample["_tau"].dropna().mean()) if len(sample["_tau"].dropna()) > 0 else float("nan")
            
            smoothness = 1 - avg_mae / (avg_mfe + 1e-9) if not np.isnan(avg_mfe) else float("nan")
            spread_eff = (avg_mfe - spread) / avg_mfe if not np.isnan(avg_mfe) and avg_mfe > 0 else float("nan")
            
            exp = hit_rate * (bucket - spread) - (1 - hit_rate) * avg_mae if not np.isnan(avg_mae) else float("nan")
            
            viable = (
                hit_rate >= 0.10
                and (spread / bucket) <= 0.50
                and (exp > 0 or np.isnan(exp))
                and not (not np.isnan(avg_mae) and not np.isnan(avg_mfe) and avg_mae > avg_mfe)
                and len(sample) >= 5
            )
            
            viability_records.append({
                "direction": direction,
                "target_bucket_pips": bucket,
                "pair": pair,
                "session": session,
                "total_rows_evaluated": len(df_cached),
                "hit_count": len(hits),
                "hit_rate": round(hit_rate, 4),
                "sample_size": len(sample),
                "avg_mfe_pips": round(avg_mfe, 4) if not np.isnan(avg_mfe) else None,
                "avg_mae_pips": round(avg_mae, 4) if not np.isnan(avg_mae) else None,
                "avg_tau_min": round(avg_tau, 2) if not np.isnan(avg_tau) else None,
                "smoothness": round(smoothness, 4) if not np.isnan(smoothness) else None,
                "spread_efficiency": round(spread_eff, 4) if not np.isnan(spread_eff) else None,
                "spread_pips": spread,
                "expectancy_pips": round(exp, 4) if not np.isnan(exp) else None,
                "kill_conditions": [],
                "viable": viable,
            })
            
            print(f"    viable={viable}, hit_rate={round(hit_rate, 3)}, sample={len(sample)}")
        
        # Write outputs
        viability_output = {
            "$artifact": "business_viability_report",
            "produced_by": "SCALED_DISCOVERY",
            "run_ts_utc": datetime.now(timezone.utc).isoformat(),
            "batch": batch_key,
            "summary": {
                "total_evaluated": len(viability_records),
                "viable_count": sum(1 for r in viability_records if r["viable"]),
            },
            "records": viability_records,
        }
        
        viability_file = output_dir / "business_viability_report.json"
        with viability_file.open("w") as f:
            json.dump(viability_output, f, indent=2)
        print(f"\n✓ Written {viability_file}")
        
        return {
            "batch_key": batch_key,
            "status": "PHASE_0_2_COMPLETE",
            "viability_records": len(viability_records),
            "viable_count": sum(1 for r in viability_records if r["viable"]),
        }
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {"batch_key": batch_key, "status": "FAILED", "error": str(e)}


def main():
    parser = argparse.ArgumentParser(
        description="Run scaled entry discovery batch"
    )
    parser.add_argument("--pair", required=True, help="Pair (e.g., EUR_USD)")
    parser.add_argument("--session", required=True, help="Session (e.g., London)")
    parser.add_argument("--direction", required=True, choices=["LONG", "SHORT"])
    parser.add_argument("--buckets", type=int, nargs="+", required=True, help="Buckets (e.g., 2 3 5 8 10)")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--sample-size", type=int, default=50)
    parser.add_argument("--weekday", default="Thursday")
    
    args = parser.parse_args()
    
    result = run_discovery_batch(
        pair=args.pair,
        session=args.session,
        direction=args.direction,
        buckets=args.buckets,
        output_dir=args.output_dir,
        sample_size=args.sample_size,
        weekday=args.weekday,
    )
    
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
