"""
pc2_stage_a_runner.py
---------------------
PC2 RCP Stage A Discovery Runner — FIRST REAL DISCOVERY RUN

Scope:
  Pairs:     AUD_USD, EUR_USD
  Session:   London
  Weekday:   Thursday
  Buckets:   2, 3, 5, 8, 10 pips
  Directions: LONG, SHORT
  Sample:    first 50 chronological hits per slice

Pipeline:
  Step 1  CACHE BUILD
            Layer 0  — environment (session-level stats)
            Layer 1  — structure primitives (rolling, computed once)
  Step 2  VECTORIZED PATH EXTRACTION     (hit_rate, MAE, MFE, tau, smoothness, spread_eff)
  Step 3  PHASE 0 — Business Viability   → business_viability_report.json
  Step 4  PHASE 1 — Path Family          → path_family_report.json
  Step 5  PHASE 2 — Structure Truth      → structure_truth.json

Hard rules enforced:
  - LONG != SHORT       (never mixed)
  - 2-pip != 10-pip     (never mixed)
  - no cross-pair average
  - no cross-session average
  - no fuzzy structure labels
  - no trigger, setup, or AEE logic
"""

from __future__ import annotations

import json
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# ─── paths ───────────────────────────────────────────────────────────────────
WORKSPACE       = Path(__file__).resolve().parent.parent
COMPILED_NODES  = WORKSPACE / "PC2" / "compiled_nodes"
OUTPUT_DIR      = WORKSPACE / "PC2" / "discovery" / "stage_a"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ─── run configuration ────────────────────────────────────────────────────────
PAIRS       = ["AUD_USD", "EUR_USD"]
SESSION     = "London"
WEEKDAY     = "Thursday"
BUCKETS     = [2, 3, 5, 8, 10]       # pip targets
DIRECTIONS  = ["LONG", "SHORT"]
SAMPLE_SIZE = 50

# Typical London spreads in pips (conservative)
SPREADS: dict[str, float] = {"AUD_USD": 1.5, "EUR_USD": 0.8}

FAMILY_LABELS = ["continuation", "breakout", "oscillation", "sweep", "drift"]
STRUCT_LABELS = [
    "break_level",
    "liquidity_sweep_zone",
    "range_edge",
    "retest_level",
    "compression",
    "drift_channel",
]


# ─── helpers ─────────────────────────────────────────────────────────────────

def pip_factor(pair: str) -> float:
    return 0.01 if "JPY" in pair else 0.0001


def load_phase1(pair: str, weekday: str, session: str) -> pd.DataFrame:
    node_dir = COMPILED_NODES / f"{pair}__{weekday}__{session}"
    csv_path  = node_dir / "phase1" / "opportunity_map_raw.csv"
    df = pd.read_csv(csv_path, parse_dates=["timestamp"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df


# ─── LAYER 0: Environment cache ───────────────────────────────────────────────

def build_env_cache(df: pd.DataFrame, pair: str) -> dict:
    """
    Session-level environment features — computed once, reused for every
    direction × bucket slice.  No per-trade recompute.
    """
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
        "session_count":              int(len(rng)),
        "avg_session_range_pips":     float(round(np.mean(rng), 2)),
        "median_session_range_pips":  float(round(np.median(rng), 2)),
        "avg_session_move_pips":      float(round(np.mean(mov), 2)),
        "persistence":                float(round(float(np.mean(mov / (rng + 1e-9))), 4)),
        "spread_pips":                SPREADS[pair],
        "volatility_class":           (
            "HIGH" if np.median(rng) > 80
            else "MEDIUM" if np.median(rng) > 40
            else "LOW"
        ),
    }


# ─── LAYER 1: Structure primitives cache ─────────────────────────────────────

def build_structure_cache(df: pd.DataFrame, pair: str) -> pd.DataFrame:
    """
    Compute all rolling structural features once per node load.
    Appended as columns — no per-slice or per-trade recompute.
    """
    pf     = pip_factor(pair)
    price  = df["price"]

    # Rolling window aggregates
    r5_max  = price.rolling(5,  min_periods=3).max()
    r5_min  = price.rolling(5,  min_periods=3).min()
    r10_max = price.rolling(10, min_periods=5).max()
    r10_min = price.rolling(10, min_periods=5).min()
    r20_max = price.rolling(20, min_periods=10).max()
    r20_min = price.rolling(20, min_periods=10).min()

    r5_range  = (r5_max  - r5_min)  / pf
    r20_range = (r20_max - r20_min) / pf

    compression_ratio    = r5_range / (r20_range + 1e-9)
    percentile_in_20bar  = (price - r20_min) / (r20_max - r20_min + 1e-9)

    # Breakout: price just crossed prior 10-bar range (binary, strict)
    prev_r10_max = r10_max.shift(1)
    prev_r10_min = r10_min.shift(1)
    breakout_up   = price > prev_r10_max
    breakout_down = price < prev_r10_min

    # Drift: rolling-20 std is small relative to range (tight, trending or compressing)
    r20_std   = price.rolling(20, min_periods=10).std() / pf
    drift_flag = (r20_std / (r20_range + 1e-9)) < 0.28

    # Retest: price within 0.8 pips of 20-bar high or low
    dist_to_r20_high = (r20_max - price) / pf
    dist_to_r20_low  = (price - r20_min) / pf
    retest_flag = (dist_to_r20_high < 0.8) | (dist_to_r20_low < 0.8)

    out = df.copy()
    out["compression_ratio"]  = compression_ratio.values
    out["percentile_20bar"]   = percentile_in_20bar.values
    out["breakout_up"]        = breakout_up.values
    out["breakout_down"]      = breakout_down.values
    out["drift_flag"]         = drift_flag.values
    out["retest_flag"]        = retest_flag.values
    out["r20_range_pips"]     = r20_range.values
    out["r5_range_pips"]      = r5_range.values
    return out


# ─── Slice helpers ────────────────────────────────────────────────────────────

def get_hits(df: pd.DataFrame, direction: str, bucket: float) -> pd.DataFrame:
    """
    Vectorized filter: rows where the direction target was reached.
    Adds _mfe, _mae, _tau columns for downstream use.
    LONG and SHORT are strictly separated.
    """
    if direction == "LONG":
        mask        = df["mfe_up_pips"] >= bucket
        hits        = df[mask].copy()
        hits["_mfe"] = hits["mfe_up_pips"]
        hits["_mae"] = hits["mae_up_pips"].fillna(0.0)
        hits["_tau"] = hits["tau_up_min"]
    else:  # SHORT
        mask        = df["mfe_down_pips"] >= bucket
        hits        = df[mask].copy()
        hits["_mfe"] = hits["mfe_down_pips"]
        hits["_mae"] = hits["mae_down_pips"].fillna(0.0)
        hits["_tau"] = hits["tau_down_min"]
    return hits


def sample_hits(hits: pd.DataFrame, n: int = SAMPLE_SIZE) -> pd.DataFrame:
    """First N chronological hits — no resampling, no shuffling."""
    return hits.head(n)


# ─── Phase 0: Business Viability ─────────────────────────────────────────────

def _kill_conditions(
    hit_rate: float,
    spread: float,
    bucket: float,
    avg_mae: float,
    avg_mfe: float,
    avg_tau: float,
) -> tuple[list[dict], float | None]:
    kills: list[dict] = []
    spread_ratio = spread / bucket
    exp: float | None = None

    if not (np.isnan(avg_mae) or np.isnan(avg_mfe)):
        exp = hit_rate * (bucket - spread) - (1 - hit_rate) * avg_mae

    if hit_rate < 0.10:
        kills.append({"condition": "HIT_RATE_TOO_LOW",
                      "value": round(hit_rate, 4), "threshold": 0.10})

    if spread_ratio > 0.50:
        kills.append({"condition": "SPREAD_TOO_LARGE",
                      "spread_ratio": round(spread_ratio, 4), "threshold": 0.50})

    if exp is not None and exp < 0:
        kills.append({"condition": "NEGATIVE_EXPECTANCY",
                      "expectancy_pips": round(exp, 4)})

    if not np.isnan(avg_mae) and not np.isnan(avg_mfe) and avg_mae > avg_mfe:
        kills.append({"condition": "MAE_EXCEEDS_MFE",
                      "avg_mae": round(avg_mae, 4), "avg_mfe": round(avg_mfe, 4)})

    if not np.isnan(avg_tau) and avg_tau > 240:
        kills.append({"condition": "TIME_TOO_SLOW",
                      "avg_tau_min": round(avg_tau, 2), "threshold_min": 240})

    return kills, exp


def phase0_record(
    df: pd.DataFrame,
    direction: str,
    bucket: float,
    pair: str,
    session: str,
    spread: float,
) -> dict:
    total_rows = len(df)
    hits       = get_hits(df, direction, bucket)
    hit_count  = len(hits)
    hit_rate   = hit_count / total_rows if total_rows > 0 else 0.0

    sample   = sample_hits(hits)
    n_sample = len(sample)

    if n_sample > 0:
        avg_mfe = float(sample["_mfe"].mean())
        avg_mae = float(sample["_mae"].mean())
        tau_ok  = sample["_tau"].dropna()
        avg_tau = float(tau_ok.mean()) if len(tau_ok) > 0 else float("nan")
    else:
        avg_mfe = avg_mae = avg_tau = float("nan")

    smoothness   = float(1 - avg_mae / (avg_mfe + 1e-9)) if not np.isnan(avg_mfe) else float("nan")
    spread_eff   = float((avg_mfe - spread) / avg_mfe)   if not np.isnan(avg_mfe) and avg_mfe > 0 else float("nan")

    kills, exp = _kill_conditions(hit_rate, spread, bucket, avg_mae, avg_mfe, avg_tau)
    viable = len(kills) == 0 and n_sample >= 5

    def _fmt(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return None
        return round(v, 4)

    return {
        "direction":             direction,
        "target_bucket_pips":    bucket,
        "pair":                  pair,
        "session":               session,
        "total_rows_evaluated":  total_rows,
        "hit_count":             hit_count,
        "hit_rate":              round(hit_rate, 4),
        "sample_size":           n_sample,
        "avg_mfe_pips":          _fmt(avg_mfe),
        "avg_mae_pips":          _fmt(avg_mae),
        "avg_tau_min":           _fmt(avg_tau),
        "smoothness":            _fmt(smoothness),
        "spread_efficiency":     _fmt(spread_eff),
        "spread_pips":           spread,
        "expectancy_pips":       _fmt(exp),
        "kill_conditions":       kills,
        "viable":                viable,
    }


# ─── Phase 1: Path Family ────────────────────────────────────────────────────

def _classify_family(
    mfe_arr: np.ndarray,
    mae_arr: np.ndarray,
    tau_arr: np.ndarray,
    bucket: float,
) -> np.ndarray:
    """
    Vectorized rule classifier — no fuzzy logic, no assumptions.
    Rules derived strictly from path geometry.
    """
    mfe_r   = mfe_arr / (bucket + 1e-9)   # how much it overshot
    mae_r   = mae_arr / (bucket + 1e-9)   # how much it dipped first
    avg_tau = float(np.nanmean(tau_arr))
    tau_r   = tau_arr / (avg_tau + 1e-9)  # relative speed

    n      = len(mfe_arr)
    labels = np.full(n, "UNCLASSIFIED", dtype=object)

    # drift: slow, modest extension, low adverse
    drift_mask = (tau_r > 1.30) & (mfe_r < 1.80) & (mae_r < 0.40)
    labels[drift_mask] = "drift"

    # oscillation: high adverse, moderate extension (struggled to get there)
    osc_mask = (mae_r > 0.50) & (mfe_r < 1.50)
    labels[osc_mask] = "oscillation"

    # sweep: high adverse THEN strong extension (stop-sweep pattern)
    sweep_mask = (mae_r > 0.50) & (mfe_r > 1.50)
    labels[sweep_mask] = "sweep"

    # continuation: clean directional, fast, low adverse
    cont_mask = (mfe_r > 1.50) & (mae_r < 0.20) & (tau_r < 1.30)
    labels[cont_mask] = "continuation"

    # breakout: explosive, very low adverse (highest priority)
    bo_mask = (mfe_r > 2.00) & (mae_r < 0.10)
    labels[bo_mask] = "breakout"

    return labels


def phase1_record(
    df: pd.DataFrame,
    direction: str,
    bucket: float,
    pair: str,
    session: str,
) -> dict | None:
    hits     = get_hits(df, direction, bucket)
    sample   = sample_hits(hits)
    n_sample = len(sample)
    if n_sample < 5:
        return None

    mfe    = sample["_mfe"].values.astype(float)
    mae    = sample["_mae"].values.astype(float)
    tau    = sample["_tau"].values.astype(float)

    labels = _classify_family(mfe, mae, tau, bucket)

    counts  = {f: int(np.sum(labels == f)) for f in FAMILY_LABELS}
    counts["UNCLASSIFIED"] = int(np.sum(labels == "UNCLASSIFIED"))

    families = []
    for fam in FAMILY_LABELS:
        c = counts[fam]
        if c >= 3:
            families.append({
                "family": fam,
                "count":  c,
                "pct":    round(c / n_sample, 4),
                "real":   c >= 5,
            })
    families.sort(key=lambda x: -x["count"])

    dominant       = families[0]["family"]   if families else None
    dominant_count = families[0]["count"]    if families else 0

    return {
        "direction":          direction,
        "target_bucket_pips": bucket,
        "pair":               pair,
        "session":            session,
        "sample_size":        n_sample,
        "families":           families,
        "unclassified_count": counts["UNCLASSIFIED"],
        "dominant_family":    dominant,
        "dominant_count":     dominant_count,
        "dominant_real":      dominant_count >= 5,
        "non_random_verdict": (
            dominant_count >= 5 and
            (dominant_count / n_sample) >= 0.25
        ),
    }


# ─── Phase 2: Structure Truth ────────────────────────────────────────────────

def _label_structure(df_s: pd.DataFrame, direction: str) -> np.ndarray:
    """
    Vectorized structural context labeler.
    Hard thresholds only — no fuzzy.
    If no threshold clearly applies → REJECTED_AMBIGUOUS.
    Priority (high → low): break_level > liquidity_sweep_zone > retest_level
                           > range_edge > compression > drift_channel
    """
    is_break = (
        df_s["breakout_up"].values.astype(bool)
        if direction == "LONG"
        else df_s["breakout_down"].values.astype(bool)
    )

    # range_edge: LONG enters from low percentile (support); SHORT from high (resistance)
    if direction == "LONG":
        is_range_edge = (df_s["percentile_20bar"] < 0.12).values
    else:
        is_range_edge = (df_s["percentile_20bar"] > 0.88).values

    is_compression = (df_s["compression_ratio"] < 0.25).values
    is_retest      = df_s["retest_flag"].values.astype(bool)
    is_drift       = df_s["drift_flag"].values.astype(bool)

    n      = len(df_s)
    labels = np.full(n, "REJECTED_AMBIGUOUS", dtype=object)

    # lowest priority first (overwritten by higher priority)
    labels[is_drift]       = "drift_channel"
    labels[is_compression] = "compression"
    labels[is_range_edge]  = "range_edge"
    labels[is_retest]      = "retest_level"
    labels[is_break]       = "break_level"

    # liquidity_sweep_zone: price retested a key level AND broke through ( = swept then continued)
    lsz_mask = is_retest & is_break
    labels[lsz_mask] = "liquidity_sweep_zone"

    return labels


def phase2_record(
    df: pd.DataFrame,
    direction: str,
    bucket: float,
    pair: str,
    session: str,
) -> dict | None:
    hits     = get_hits(df, direction, bucket)
    sample   = sample_hits(hits)
    n_sample = len(sample)
    if n_sample < 5:
        return None

    labels  = _label_structure(sample, direction)

    counts  = {lbl: int(np.sum(labels == lbl)) for lbl in STRUCT_LABELS}
    counts["REJECTED_AMBIGUOUS"] = int(np.sum(labels == "REJECTED_AMBIGUOUS"))

    placed_count  = n_sample - counts["REJECTED_AMBIGUOUS"]
    placement_pct = placed_count / n_sample

    breakdown = []
    for lbl in STRUCT_LABELS:
        c = counts[lbl]
        if c > 0:
            breakdown.append({
                "label":     lbl,
                "count":     c,
                "pct":       round(c / n_sample, 4),
                "objective": True,   # rules-based, not fuzzy
            })
    breakdown.sort(key=lambda x: -x["count"])

    dominant = breakdown[0]["label"] if breakdown else None

    return {
        "direction":              direction,
        "target_bucket_pips":     bucket,
        "pair":                   pair,
        "session":                session,
        "sample_size":            n_sample,
        "struct_breakdown":       breakdown,
        "rejected_ambiguous_count": counts["REJECTED_AMBIGUOUS"],
        "placed_count":           placed_count,
        "placement_rate":         round(placement_pct, 4),
        "dominant_structure":     dominant,
        "consistent_verdict": (
            placed_count >= 10 and
            placement_pct >= 0.40
        ),
    }


# ─── Main ─────────────────────────────────────────────────────────────────────

def run() -> tuple[Path, Path, Path]:
    run_ts   = datetime.now(timezone.utc).isoformat()

    bv_records: list[dict] = []
    pf_records: list[dict] = []
    st_records: list[dict] = []
    env_cache:  dict       = {}

    for pair in PAIRS:
        print(f"\n{'='*60}")
        print(f"  {pair} / {WEEKDAY} / {SESSION}")
        print(f"{'='*60}")

        df_raw   = load_phase1(pair, WEEKDAY, SESSION)
        print(f"  Loaded {len(df_raw):,} rows")

        # Layer 0
        env              = build_env_cache(df_raw, pair)
        env_cache[pair]  = env
        print(
            f"  Layer-0: sessions={env.get('session_count')}, "
            f"avg_range={env.get('avg_session_range_pips')} pips, "
            f"persistence={env.get('persistence')}, "
            f"vol_class={env.get('volatility_class')}"
        )

        # Layer 1 (built once, reused for all slices of this pair)
        df = build_structure_cache(df_raw, pair)
        spread = SPREADS[pair]

        for direction in DIRECTIONS:
            for bucket in BUCKETS:
                key = f"{direction} / {bucket}pip / {pair} / {SESSION}"

                # ── Phase 0 ──
                bv = phase0_record(df, direction, bucket, pair, SESSION, spread)
                bv_records.append(bv)

                kill_str = (
                    ", ".join(k["condition"] for k in bv["kill_conditions"])
                    if bv["kill_conditions"]
                    else "—"
                )
                print(
                    f"  [{key}]  "
                    f"hit_rate={bv['hit_rate']:.3f}  "
                    f"exp={bv['expectancy_pips']}  "
                    f"viable={bv['viable']}  kills=[{kill_str}]"
                )

                if not bv["viable"]:
                    continue

                # ── Phase 1 ──
                pf = phase1_record(df, direction, bucket, pair, SESSION)
                if pf:
                    pf_records.append(pf)
                    print(
                        f"    PATH  dominant={pf['dominant_family']}  "
                        f"({pf['dominant_count']}/{pf['sample_size']})  "
                        f"real={pf['dominant_real']}  "
                        f"non_random={pf['non_random_verdict']}  "
                        f"unclassified={pf['unclassified_count']}"
                    )

                # ── Phase 2 ──
                st = phase2_record(df, direction, bucket, pair, SESSION)
                if st:
                    st_records.append(st)
                    print(
                        f"    STRUCT dominant={st['dominant_structure']}  "
                        f"placed={st['placed_count']}/{st['sample_size']}  "
                        f"placement_rate={st['placement_rate']:.3f}  "
                        f"consistent={st['consistent_verdict']}"
                    )

    # ── Assemble artifacts ────────────────────────────────────────────────────

    total_viable = sum(1 for r in bv_records if r["viable"])
    total_killed = sum(1 for r in bv_records if not r["viable"])

    bv_artifact = {
        "$artifact":    "business_viability_report",
        "produced_by":  "PC2_DISCOVERY",
        "run_ts_utc":   run_ts,
        "scope": {
            "pairs":       PAIRS,
            "session":     SESSION,
            "weekday":     WEEKDAY,
            "buckets_pip": BUCKETS,
            "sample_size": SAMPLE_SIZE,
        },
        "env_cache": env_cache,
        "summary": {
            "total_slices":  len(bv_records),
            "viable_slices": total_viable,
            "killed_slices": total_killed,
        },
        "records": bv_records,
    }

    pf_non_random = sum(1 for r in pf_records if r["non_random_verdict"])

    pf_artifact = {
        "$artifact":   "path_family_report",
        "produced_by": "PC2_DISCOVERY",
        "run_ts_utc":  run_ts,
        "scope": {
            "pairs":       PAIRS,
            "session":     SESSION,
            "weekday":     WEEKDAY,
        },
        "summary": {
            "viable_slices_evaluated": len(pf_records),
            "non_random_families":     pf_non_random,
        },
        "records": pf_records,
    }

    st_consistent = sum(1 for r in st_records if r["consistent_verdict"])

    st_artifact = {
        "$artifact":   "structure_truth",
        "produced_by": "PC2_DISCOVERY",
        "run_ts_utc":  run_ts,
        "scope": {
            "pairs":       PAIRS,
            "session":     SESSION,
            "weekday":     WEEKDAY,
        },
        "summary": {
            "slices_with_structure": len(st_records),
            "consistent_structure":  st_consistent,
        },
        "records": st_records,
    }

    # ── Write ─────────────────────────────────────────────────────────────────

    bv_path = OUTPUT_DIR / "business_viability_report.json"
    pf_path = OUTPUT_DIR / "path_family_report.json"
    st_path = OUTPUT_DIR / "structure_truth.json"

    bv_path.write_text(json.dumps(bv_artifact, indent=2, default=str))
    pf_path.write_text(json.dumps(pf_artifact, indent=2, default=str))
    st_path.write_text(json.dumps(st_artifact, indent=2, default=str))

    print(f"\n{'='*60}")
    print(f"  Artifacts written → {OUTPUT_DIR}")
    print(f"{'='*60}")
    print(f"  business_viability_report.json  {len(bv_records)} records  ({total_viable} viable / {total_killed} killed)")
    print(f"  path_family_report.json         {len(pf_records)} records  ({pf_non_random} non-random)")
    print(f"  structure_truth.json            {len(st_records)} records  ({st_consistent} consistent)")

    return bv_path, pf_path, st_path


if __name__ == "__main__":
    run()
