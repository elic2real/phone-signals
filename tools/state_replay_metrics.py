#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import time
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
import math
from pathlib import Path
from typing import Any

import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from active_artifacts import load_active_artifacts
from vol_bucket_spec import bucket_from_rank, cuts_for_session, load_vol_bucket_spec


PAIRS_15 = [
    "EUR_USD",
    "GBP_USD",
    "USD_JPY",
    "USD_CHF",
    "AUD_USD",
    "USD_CAD",
    "NZD_USD",
    "EUR_GBP",
    "EUR_JPY",
    "GBP_JPY",
    "AUD_JPY",
    "CHF_JPY",
    "EUR_CHF",
    "AUD_CAD",
    "NZD_JPY",
]


def _session_from_hour(h: int) -> str:
    if 8 <= h < 16:
        return "LONDON"
    if 14 <= h < 21:
        return "NY"
    return "ASIA"


def _session_quarter_from_ts(h: int, session: str) -> str:
    if session == "ASIA":
        i = min(3, max(0, h // 2))
    elif session == "LONDON":
        i = min(3, max(0, (h - 8) // 2))
    else:
        i = min(3, max(0, (h - 16) // 2))
    return f"Q{i+1}"


def _load_levels(seed_path: Path) -> dict[str, dict[str, dict[str, Any]]]:
    if not seed_path.exists():
        return {"GLOBAL": {}, "SESSION_GLOBAL": {}, "SESSION_PAIR": {}, "COARSE": {}}
    obj = json.loads(seed_path.read_text(encoding="utf-8"))
    if "levels" in obj and isinstance(obj["levels"], dict):
        lv = obj["levels"]
        return {
            "GLOBAL": lv.get("GLOBAL", {}) or {},
            "SESSION_GLOBAL": lv.get("SESSION_GLOBAL", {}) or {},
            "SESSION_PAIR": lv.get("SESSION_PAIR", {}) or {},
            "SESSION_FAMILY": lv.get("SESSION_FAMILY", {}) or {},
            "COARSE": lv.get("COARSE", {}) or {},
        }
    return {
        "GLOBAL": obj.get("GLOBAL", {}) or {},
        "SESSION_GLOBAL": obj.get("SESSION_GLOBAL", {}) or {},
        "SESSION_PAIR": obj.get("SESSION_PAIR", {}) or {},
        "SESSION_FAMILY": obj.get("SESSION_FAMILY", {}) or {},
        "COARSE": obj.get("COARSE", {}) or {},
    }


def _load_patch(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        p = obj.get("patches") if isinstance(obj, dict) else []
        return p if isinstance(p, list) else []
    except Exception:
        return []


def _family_for_pair(pair: str) -> str:
    p = str(pair or "").upper()
    if "CHF" in p:
        return "CHF_FAMILY"
    if "JPY" in p:
        return "JPY_FAMILY"
    return "USD_FAMILY"


def _atr_bucket_from_ratio(ratio: float) -> str:
    if ratio < 0.85:
        return "ATR_LOW"
    if ratio > 1.15:
        return "ATR_HIGH"
    return "ATR_MID"


def _merge_knobs(
    levels: dict[str, dict[str, dict[str, Any]]],
    patches: list[dict[str, Any]],
    pair: str,
    session: str,
    quarter: str,
    atr_bucket: str,
    vol_bucket: str,
) -> tuple[dict[str, Any], str, str]:
    out: dict[str, Any] = {}
    matched_level = "NONE"
    matched_key = ""
    g = levels.get("GLOBAL", {}) or {}
    if "GLOBAL" in g and isinstance(g["GLOBAL"], dict):
        out.update(g["GLOBAL"])
        matched_level = "GLOBAL"
        matched_key = "GLOBAL"
    sg_key = f"session={session}"
    sp_key = f"session={session}|pair={pair}"
    c_key = f"session={session}|pair={pair}|speed=MED"
    session_quarter = f"{session}_{quarter}"
    fam_qv_key = f"{_family_for_pair(pair)}|{session_quarter}|{vol_bucket}"
    pair_qv_key = f"{pair}|{session_quarter}|{vol_bucket}"
    fam_q_key = f"{_family_for_pair(pair)}|{session_quarter}|{atr_bucket}"
    pair_q_key = f"{pair}|{session_quarter}|{atr_bucket}"
    fam_key = f"{_family_for_pair(pair)}|{session}|{atr_bucket}"
    pair_key = f"{pair}|{session}|{atr_bucket}"
    for k, level in ((sg_key, "SESSION_GLOBAL"), (sp_key, "SESSION_PAIR"), (c_key, "COARSE")):
        m = (levels.get(level, {}) or {}).get(k)
        if isinstance(m, dict):
            out.update(m)
            matched_level = level
            matched_key = k
    # Optional seed support for family-aware level.
    fm = (levels.get("SESSION_FAMILY", {}) or {}).get(fam_key)
    if isinstance(fm, dict):
        out.update(fm)
        matched_level = "SESSION_FAMILY"
        matched_key = fam_key

    patch_map: dict[tuple[str, str], dict[str, Any]] = {}
    for p in patches:
        if not isinstance(p, dict):
            continue
        level = str(p.get("level", ""))
        key = str(p.get("key", ""))
        knobs = p.get("knobs") or {}
        if not isinstance(knobs, dict):
            continue
        patch_map[(level, key)] = knobs

    # Patch precedence: pair ATR -> family ATR -> legacy chain.
    for level, key in (
        ("SESSION_PAIR", pair_qv_key),
        ("SESSION_FAMILY", fam_qv_key),
        ("SESSION_PAIR", pair_q_key),
        ("SESSION_FAMILY", fam_q_key),
        ("SESSION_PAIR", pair_key),
        ("SESSION_FAMILY", fam_key),
        ("COARSE", c_key),
        ("SESSION_PAIR", sp_key),
        ("SESSION_GLOBAL", sg_key),
        ("GLOBAL", "GLOBAL"),
    ):
        knobs = patch_map.get((level, key))
        if knobs is not None:
            out.update(knobs)
            matched_level = level
            matched_key = key
            break
    return out, matched_level, matched_key


def _adj_from_knobs(knobs: dict[str, Any]) -> float:
    strict = float(knobs.get("aee.strictness_mult", 1.0) or 1.0)
    dist = float(knobs.get("entry.tick.base_max_dist_atr", 0.30) or 0.30)
    confirm_disp = float(knobs.get("entry.tick.confirm_disp_atr", 0.10) or 0.10)
    confirm_sec = float(knobs.get("entry.tick.confirm_sec", 3.0) or 3.0)
    fail_windows = float(knobs.get("aee.fail_windows", 3.0) or 3.0)
    promote_mfe = float(knobs.get("promote_mfe_atr", knobs.get("promote_mfe_pips", 0.25)) or 0.25)
    stall_band = float(
        knobs.get(
            "aee.stall_proximity_band",
            knobs.get("aee.stall_band", 0.20),
        )
        or 0.20
    )
    giveback_trigger = float(
        knobs.get(
            "aee.giveback_trigger",
            knobs.get("aee.giveback_trigger_atr", 0.35),
        )
        or 0.35
    )
    adj = (
        (1.0 - strict) * 0.32
        + (dist - 0.30) * 0.22
        + (confirm_disp - 0.10) * 0.20
        - (confirm_sec - 3.0) * 0.03
        + (3.0 - fail_windows) * 0.03
        + (0.25 - promote_mfe) * 0.06
        + (stall_band - 0.20) * 0.05
        + (giveback_trigger - 0.35) * 0.04
    )
    return max(-0.30, min(0.30, adj))


def _first_passage_ceiling_atr(
    is_long: bool,
    entry: float,
    atr: float,
    fut_high: list[float],
    fut_low: list[float],
    x_atr: float,
    y_atr: float,
) -> float:
    if atr <= 0.0:
        return 0.0
    plus = entry + x_atr * atr if is_long else entry - x_atr * atr
    minus = entry - y_atr * atr if is_long else entry + y_atr * atr

    hit_plus = None
    hit_minus = None
    best_fav = 0.0

    for k in range(len(fut_high)):
        hi = float(fut_high[k])
        lo = float(fut_low[k])
        fav = (hi - entry) / atr if is_long else (entry - lo) / atr
        if fav > best_fav:
            best_fav = fav

        plus_now = (hi >= plus) if is_long else (lo <= plus)
        minus_now = (lo <= minus) if is_long else (hi >= minus)

        # Conservative tie-break on same bar: adverse first.
        if hit_plus is None and plus_now and not minus_now:
            hit_plus = k
        if hit_minus is None and minus_now:
            hit_minus = k

        if hit_minus is not None and hit_plus is None:
            # Barrier failed before target: ceiling is best favorable until failure bar.
            return max(0.0, best_fav)
        if hit_plus is not None and (hit_minus is None or hit_plus < hit_minus):
            return max(0.0, x_atr)

    return max(0.0, best_fav)


@dataclass
class Acc:
    n: int = 0
    sum_before_ee: float = 0.0
    sum_after_nopatch_ee: float = 0.0
    sum_after_patch_ee: float = 0.0
    sum_before_cap: float = 0.0
    sum_after_nopatch_cap: float = 0.0
    sum_after_patch_cap: float = 0.0


def _parse_utc(s: str) -> datetime | None:
    if not s:
        return None
    x = s.strip()
    if x.endswith("Z"):
        x = x[:-1] + "+00:00"
    dt = datetime.fromisoformat(x)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _wall_time_sec_from_bounds(start_utc: str, end_utc: str) -> float:
    ds = _parse_utc(start_utc)
    de = _parse_utc(end_utc)
    if ds is None or de is None:
        return 0.0
    return max(0.0, (de - ds).total_seconds())


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tape-root", default="data_tape_stitched")
    ap.add_argument("--seed", default="tunes/tune_map_seed_v2.json")
    ap.add_argument("--pairs", default=",".join(PAIRS_15))
    ap.add_argument("--horizon-bars", type=int, default=12)
    ap.add_argument("--ceiling-mode", choices=["proxy", "first_passage"], default="proxy")
    ap.add_argument("--x-atr", type=float, default=1.0)
    ap.add_argument("--y-atr", type=float, default=0.5)
    ap.add_argument("--vol-cut-low-pct", type=float, default=0.33, help="Low cut percentile in [0,1]")
    ap.add_argument("--vol-cut-high-pct", type=float, default=0.66, help="High cut percentile in [0,1]")
    ap.add_argument("--vol-spec", default="calibration/vol_bucket_spec_active_asia.json")
    ap.add_argument("--active-artifacts", default="calibration/active/ACTIVE_ARTIFACTS.json")
    ap.add_argument("--start-utc", default="")
    ap.add_argument("--end-utc", default="")
    ap.add_argument("--patch", default="")
    ap.add_argument("--base-cache-in", default="", help="Path to precomputed base state aggregates JSON")
    ap.add_argument("--base-cache-out", default="", help="Path to write precomputed base state aggregates JSON")
    ap.add_argument(
        "--enforce-family-touch",
        action="store_true",
        help="Fail if patch contains SESSION_FAMILY keys but touches zero targets",
    )
    ap.add_argument(
        "--min-touched-targets",
        type=int,
        default=0,
        help="Fail if touched_targets is below this threshold",
    )
    ap.add_argument(
        "--min-vol-bucket-touched",
        type=int,
        default=0,
        help="Fail if any VOL bucket has touched count below this threshold when VOL keys are used",
    )
    ap.add_argument("--out", default="proof_artifacts/STATE_15120_BEFORE_AFTER_AUDIT.json")
    ap.add_argument("--bar-sec", type=float, default=300.0, help="Bar duration in seconds for hold-time proxy metrics")
    ap.add_argument(
        "--enforce-tier-touches",
        action="store_true",
        help="Fail when expected tuned tiers are present in patch but matched zero rows",
    )
    ap.add_argument(
        "--enforce-quarter-no-shadow",
        action="store_true",
        help="Fail when quarter keys exist but quarter-level touches are zero while non-quarter session keys are touched",
    )
    args = ap.parse_args()

    t0 = time.perf_counter()
    tape_root = Path(args.tape_root)
    levels = _load_levels(Path(args.seed))
    patches = _load_patch(Path(args.patch)) if args.patch else []
    pairs = [p.strip().upper() for p in args.pairs.split(",") if p.strip()]
    h = max(2, int(args.horizon_bars))
    dt_start = _parse_utc(args.start_utc)
    dt_end = _parse_utc(args.end_utc)
    vol_low_pct = max(0.01, min(0.49, float(args.vol_cut_low_pct)))
    vol_high_pct = max(vol_low_pct + 0.01, min(0.99, float(args.vol_cut_high_pct)))
    vol_spec = None
    session_specs: dict[str, dict[str, Any]] = {}
    active_meta: dict[str, Any] = {}
    try:
        if args.active_artifacts and Path(str(args.active_artifacts)).exists():
            aa = load_active_artifacts(str(args.active_artifacts))
            active_meta = {
                "active_artifacts_sha256": aa.get("active_artifacts_sha256", ""),
                "session_vol_specs_sha256": {s: (aa.get("sessions", {}).get(s, {}) or {}).get("vol_spec_sha256", "") for s in ("ASIA", "LONDON", "NY")},
            }
            for s in ("ASIA", "LONDON", "NY"):
                sp = str((aa.get("sessions", {}).get(s, {}) or {}).get("vol_spec", "") or "")
                if sp:
                    session_specs[s] = load_vol_bucket_spec(sp)
        elif args.vol_spec:
            vol_spec = load_vol_bucket_spec(str(args.vol_spec))
    except Exception:
        vol_spec = None

    acc: dict[tuple[str, str, str, str, str, str, str], Acc] = {}
    base_loaded_from_cache = False
    cache_source: dict[str, Any] = {}
    cache_fingerprint = ""
    t_load = 0.0
    t_eval = 0.0

    if args.base_cache_in:
        cpath = Path(args.base_cache_in)
        cache_fingerprint = hashlib.sha256(cpath.read_bytes()).hexdigest()
        obj = json.loads(cpath.read_text(encoding="utf-8"))
        cache_source = obj.get("source") or {}
        cache_mode = str((obj.get("source") or {}).get("ceiling_mode", "proxy"))
        if cache_mode != args.ceiling_mode:
            raise SystemExit(
                f"base-cache ceiling_mode mismatch: cache={cache_mode} requested={args.ceiling_mode}. "
                "Rebuild cache with matching --ceiling-mode."
            )
        for r in obj.get("states", []):
            key = (
                str(r["pair"]),
                str(r["session"]),
                str(r["weekday"]),
                str(r["quarter"]),
                str(r["month"]),
                str(r.get("atr_bucket", "ATR_MID")),
                str(r.get("vol_bucket", "VOL_MID")),
            )
            acc[key] = Acc(
                n=int(r.get("n", 0) or 0),
                sum_before_ee=float(r.get("sum_before_ee", 0.0) or 0.0),
                sum_before_cap=float(r.get("sum_before_cap", 0.0) or 0.0),
            )
        base_loaded_from_cache = True
    else:
        for pair in pairs:
            fp = tape_root / f"pair={pair}" / "stitched.parquet"
            if not fp.exists():
                continue
            t1 = time.perf_counter()
            df = pd.read_parquet(fp)
            t_load += time.perf_counter() - t1
            if len(df) <= h + 20:
                continue
            if not {"timestamp", "open", "high", "low", "close"}.issubset(df.columns):
                continue
            ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            if dt_start is not None:
                df = df[ts >= pd.Timestamp(dt_start)]
                ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            if dt_end is not None:
                df = df[ts <= pd.Timestamp(dt_end)]
                ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            close = pd.to_numeric(df["close"], errors="coerce")
            high = pd.to_numeric(df["high"], errors="coerce")
            low = pd.to_numeric(df["low"], errors="coerce")
            prev_close = close.shift(1)
            tr = pd.concat([(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
            atr = tr.rolling(14, min_periods=14).mean()
            atr_long = tr.rolling(100, min_periods=100).mean()

            # Session-local terciles per pair/session within the current slice.
            sess_vals: dict[str, list[float]] = {"ASIA": [], "LONDON": [], "NY": []}
            for j in range(20, len(df) - h):
                if pd.isna(ts.iloc[j]) or pd.isna(atr.iloc[j]) or atr.iloc[j] <= 0:
                    continue
                dtj = ts.iloc[j].to_pydatetime().astimezone(timezone.utc)
                sj = _session_from_hour(int(dtj.hour))
                atr_j = float(atr.iloc[j])
                atr_lj = float(atr_long.iloc[j]) if not pd.isna(atr_long.iloc[j]) else atr_j
                rj = (atr_j / max(atr_lj, 1e-9)) if atr_lj > 0.0 else 1.0
                sess_vals[sj].append(float(rj))
            sess_q: dict[str, tuple[float, float]] = {}
            for sj in ("ASIA", "LONDON", "NY"):
                vals = sess_vals.get(sj, [])
                if vals:
                    svals = sorted(vals)
                    nsv = len(svals)
                    q33 = svals[min(nsv - 1, max(0, int(vol_low_pct * (nsv - 1))))]
                    q66 = svals[min(nsv - 1, max(0, int(vol_high_pct * (nsv - 1))))]
                    sess_q[sj] = (float(q33), float(q66))
                else:
                    sess_q[sj] = (0.95, 1.05)

            t2 = time.perf_counter()
            for i in range(20, len(df) - h):
                if pd.isna(ts.iloc[i]) or pd.isna(close.iloc[i]) or pd.isna(atr.iloc[i]) or atr.iloc[i] <= 0:
                    continue
                dt = ts.iloc[i].to_pydatetime().astimezone(timezone.utc)
                session = _session_from_hour(int(dt.hour))
                weekday = dt.strftime("%a")
                quarter = _session_quarter_from_ts(int(dt.hour), session)
                month = f"{int(dt.month):02d}"
                atr_i = float(atr.iloc[i])
                atr_l = float(atr_long.iloc[i]) if not pd.isna(atr_long.iloc[i]) else atr_i
                atr_ratio = (atr_i / max(atr_l, 1e-9)) if atr_l > 0.0 else 1.0
                atr_bucket = _atr_bucket_from_ratio(atr_ratio)
                vals = sess_vals.get(session, [])
                if vals:
                    rank = sum(1 for x in vals if x <= atr_ratio) / max(1, len(vals))
                else:
                    rank = 0.5
                if session_specs:
                    lo, hi = cuts_for_session(session_specs.get(session, {}), session)
                elif vol_spec is not None:
                    lo, hi = cuts_for_session(vol_spec, session)
                else:
                    lo, hi = vol_low_pct, vol_high_pct
                vol_bucket = bucket_from_rank(float(rank), float(lo), float(hi))
                state = (pair, session, weekday, quarter, month, atr_bucket, vol_bucket)

                entry = float(close.iloc[i])
                fut = close.iloc[i + 1 : i + h + 1].to_numpy(dtype=float)
                if len(fut) == 0:
                    continue
                mom = float(close.iloc[i] - close.iloc[i - 3])
                is_long = mom >= 0.0
                move = (float(fut[-1]) - entry) if is_long else (entry - float(fut[-1]))
                before_ee = move / atr_i
                if args.ceiling_mode == "first_passage":
                    fut_high = high.iloc[i + 1 : i + h + 1].to_list()
                    fut_low = low.iloc[i + 1 : i + h + 1].to_list()
                    ceiling = _first_passage_ceiling_atr(
                        is_long=is_long,
                        entry=entry,
                        atr=atr_i,
                        fut_high=fut_high,
                        fut_low=fut_low,
                        x_atr=float(args.x_atr),
                        y_atr=float(args.y_atr),
                    )
                else:
                    mfe = (float(fut.max()) - entry) / atr_i if is_long else (entry - float(fut.min())) / atr_i
                    ceiling = max(0.0, mfe)
                before_cap = (before_ee / ceiling) if ceiling > 1e-9 else 0.0

                a = acc.setdefault(state, Acc())
                a.n += 1
                a.sum_before_ee += before_ee
                a.sum_before_cap += before_cap
            t_eval += time.perf_counter() - t2

    if args.base_cache_out:
        crows = []
        for (pair, session, weekday, quarter, month, atr_bucket, vol_bucket), a in acc.items():
            crows.append(
                {
                    "pair": pair,
                    "session": session,
                    "weekday": weekday,
                    "quarter": quarter,
                    "month": month,
                    "atr_bucket": atr_bucket,
                    "vol_bucket": vol_bucket,
                    "n": a.n,
                    "sum_before_ee": a.sum_before_ee,
                    "sum_before_cap": a.sum_before_cap,
                }
            )
        Path(args.base_cache_out).write_text(
            json.dumps(
                {
                    "generated_utc": datetime.now(timezone.utc).isoformat(),
                    "source": {
                        "tape_root": str(tape_root),
                        "seed": str(args.seed),
                        "start_utc": args.start_utc,
                        "end_utc": args.end_utc,
                        "horizon_bars": h,
                        "ceiling_mode": args.ceiling_mode,
                        "x_atr": float(args.x_atr),
                        "y_atr": float(args.y_atr),
                        "vol_cut_low_pct": float(vol_low_pct),
                        "vol_cut_high_pct": float(vol_high_pct),
                    },
                    "states": crows,
                }
            ),
            encoding="utf-8",
        )

    patch_match_level_by_state: dict[tuple[str, str, str, str, str, str, str], str] = {}
    patch_match_key_by_state: dict[tuple[str, str, str, str, str, str, str], str] = {}
    for (pair, session, weekday, quarter, month, atr_bucket, vol_bucket), a in acc.items():
        if a.n <= 0:
            continue
        mean_before_ee = a.sum_before_ee / a.n
        mean_before_cap = a.sum_before_cap / a.n
        knobs_nopatch, _m0, _k0 = _merge_knobs(levels, [], pair, session, quarter, atr_bucket, vol_bucket)
        knobs_patch, m1, k1 = _merge_knobs(levels, patches, pair, session, quarter, atr_bucket, vol_bucket)
        state_key = (pair, session, weekday, quarter, month, atr_bucket, vol_bucket)
        patch_match_level_by_state[state_key] = m1
        patch_match_key_by_state[state_key] = k1
        adj_nopatch = _adj_from_knobs(knobs_nopatch)
        adj_patch = _adj_from_knobs(knobs_patch)
        a.sum_after_nopatch_ee = (mean_before_ee * (1.0 + adj_nopatch)) * a.n
        a.sum_after_nopatch_cap = (mean_before_cap * (1.0 + 0.6 * adj_nopatch)) * a.n
        a.sum_after_patch_ee = (mean_before_ee * (1.0 + adj_patch)) * a.n
        a.sum_after_patch_cap = (mean_before_cap * (1.0 + 0.6 * adj_patch)) * a.n

    sessions = ["ASIA", "LONDON", "NY"]
    weekdays = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    quarters = ["Q1", "Q2", "Q3", "Q4"]
    months = [f"{m:02d}" for m in range(1, 13)]
    atr_buckets = ["ATR_LOW", "ATR_MID", "ATR_HIGH"]
    vol_buckets = ["VOL_LOW", "VOL_MID", "VOL_HIGH"]

    rows = []
    covered = 0
    for pair in pairs:
        for session in sessions:
            for weekday in weekdays:
                for quarter in quarters:
                    for month in months:
                        for atr_bucket in atr_buckets:
                            for vol_bucket in vol_buckets:
                                key = (pair, session, weekday, quarter, month, atr_bucket, vol_bucket)
                                a = acc.get(key)
                                if a and a.n > 0:
                                    covered += 1
                                    b_ee = a.sum_before_ee / a.n
                                    aft_ee = a.sum_after_patch_ee / a.n
                                    nop_ee = a.sum_after_nopatch_ee / a.n
                                    b_cap = a.sum_before_cap / a.n
                                    aft_cap = a.sum_after_patch_cap / a.n
                                    nop_cap = a.sum_after_nopatch_cap / a.n
                                    rows.append(
                                        {
                                            "pair": pair,
                                            "session": session,
                                            "weekday": weekday,
                                            "quarter": quarter,
                                            "month": month,
                                            "atr_bucket": atr_bucket,
                                            "vol_bucket": vol_bucket,
                                            "n": a.n,
                                            "before_expected_extraction_atr": float(b_ee),
                                            "nopatch_expected_extraction_atr": float(nop_ee),
                                            "after_expected_extraction_atr": float(aft_ee),
                                            "delta_expected_extraction_atr": float(aft_ee - b_ee),
                                            "delta_expected_extraction_atr_vs_nopatch": float(aft_ee - nop_ee),
                                            "before_capture_to_ceiling": float(b_cap),
                                            "nopatch_capture_to_ceiling": float(nop_cap),
                                            "after_capture_to_ceiling": float(aft_cap),
                                            "delta_capture_to_ceiling": float(aft_cap - b_cap),
                                            "delta_capture_to_ceiling_vs_nopatch": float(aft_cap - nop_cap),
                                            "matched_level_patch": patch_match_level_by_state.get(key, "NONE"),
                                            "matched_key_patch": patch_match_key_by_state.get(key, ""),
                                            "status": "COVERED",
                                        }
                                    )
                                else:
                                    rows.append(
                                        {
                                            "pair": pair,
                                            "session": session,
                                            "weekday": weekday,
                                            "quarter": quarter,
                                            "month": month,
                                            "atr_bucket": atr_bucket,
                                            "vol_bucket": vol_bucket,
                                            "n": 0,
                                            "before_expected_extraction_atr": None,
                                            "nopatch_expected_extraction_atr": None,
                                            "after_expected_extraction_atr": None,
                                            "delta_expected_extraction_atr": None,
                                            "delta_expected_extraction_atr_vs_nopatch": None,
                                            "before_capture_to_ceiling": None,
                                            "nopatch_capture_to_ceiling": None,
                                            "after_capture_to_ceiling": None,
                                            "delta_capture_to_ceiling": None,
                                            "delta_capture_to_ceiling_vs_nopatch": None,
                                            "matched_level_patch": "NONE",
                                            "matched_key_patch": "",
                                            "status": "INSUFFICIENT_DATA",
                                        }
                                    )

    out = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "source": {
            "tape_root": str(tape_root),
            "seed": str(args.seed),
            "patch": str(args.patch or ""),
            "base_cache_in": str(args.base_cache_in or ""),
            "base_cache_out": str(args.base_cache_out or ""),
            "base_loaded_from_cache": bool(base_loaded_from_cache),
            "start_utc": args.start_utc,
            "end_utc": args.end_utc,
            "horizon_bars": h,
            "ceiling_mode": args.ceiling_mode,
            "x_atr": float(args.x_atr),
            "y_atr": float(args.y_atr),
            "vol_cut_low_pct": float(vol_low_pct),
            "vol_cut_high_pct": float(vol_high_pct),
            **active_meta,
        },
        "timing_sec": {
            "total": round(time.perf_counter() - t0, 6),
            "load": round(t_load, 6),
            "base_eval": round(t_eval, 6),
        },
        "total_possible_states": len(rows),
        "covered_states": covered,
        "coverage_ratio": round(covered / max(1, len(rows)), 6),
        "rows": rows,
    }
    covered_rows = [r for r in rows if r.get("status") == "COVERED"]
    has_quarter_patch_keys = any(
        isinstance(p, dict) and "_Q" in str(p.get("key", "")) for p in patches
    )
    has_vol_patch_keys = any(
        isinstance(p, dict) and "|VOL_" in str(p.get("key", "")) for p in patches
    )
    total_before_e = float(sum(((r.get("before_expected_extraction_atr") or 0.0) * (r.get("n") or 0)) for r in covered_rows))
    total_nopatch_e = float(sum(((r.get("nopatch_expected_extraction_atr") or 0.0) * (r.get("n") or 0)) for r in covered_rows))
    total_after_e = float(sum(((r.get("after_expected_extraction_atr") or 0.0) * (r.get("n") or 0)) for r in covered_rows))
    exits_base = int(sum(int(r.get("n") or 0) for r in covered_rows))
    exits_patch = exits_base
    wall_time_sec = _wall_time_sec_from_bounds(str(args.start_utc or ""), str(args.end_utc or ""))
    if wall_time_sec <= 0.0:
        wall_time_sec = _wall_time_sec_from_bounds(str(cache_source.get("start_utc", "")), str(cache_source.get("end_utc", "")))
    wall_h = (wall_time_sec / 3600.0) if wall_time_sec > 0.0 else 0.0
    eph_base = (total_before_e / wall_h) if wall_h > 0 else 0.0
    eph_nopatch = (total_nopatch_e / wall_h) if wall_h > 0 else 0.0
    eph_patch = (total_after_e / wall_h) if wall_h > 0 else 0.0
    exits_h_base = (exits_base / wall_h) if wall_h > 0 else 0.0
    exits_h_patch = (exits_patch / wall_h) if wall_h > 0 else 0.0
    entries_h_base = exits_h_base
    entries_h_patch = exits_h_patch
    avg_hold_sec_proxy = float(max(1.0, args.bar_sec) * h)
    e_per_trade_base = (total_before_e / exits_base) if exits_base > 0 else 0.0
    e_per_trade_patch = (total_after_e / exits_patch) if exits_patch > 0 else 0.0

    target_acc: dict[tuple[str, ...], dict[str, float]] = {}
    for r in covered_rows:
        if has_vol_patch_keys:
            tk = (
                str(r.get("pair")),
                str(r.get("session")),
                str(r.get("quarter", "")),
                str(r.get("vol_bucket", "VOL_MID")),
            )
        elif has_quarter_patch_keys:
            tk = (
                str(r.get("pair")),
                str(r.get("session")),
                str(r.get("quarter", "")),
                str(r.get("atr_bucket", "ATR_MID")),
            )
        else:
            tk = (str(r.get("pair")), str(r.get("session")), str(r.get("atr_bucket", "ATR_MID")))
        a = target_acc.setdefault(
            tk,
            {
                "n": 0.0,
                "E_base": 0.0,
                "E_nopatch": 0.0,
                "E_patch": 0.0,
            },
        )
        n = float(r.get("n") or 0.0)
        a["n"] += n
        a["E_base"] += float(r.get("before_expected_extraction_atr") or 0.0) * n
        a["E_nopatch"] += float(r.get("nopatch_expected_extraction_atr") or 0.0) * n
        a["E_patch"] += float(r.get("after_expected_extraction_atr") or 0.0) * n

    target_rows = []
    for tk, a in target_acc.items():
        if has_vol_patch_keys:
            pair, session, quarter, vol_bucket = tk
            atr_bucket = ""
            target_key = f"{pair}|{session}_{quarter}|{vol_bucket}"
        elif has_quarter_patch_keys:
            pair, session, quarter, atr_bucket = tk
            vol_bucket = ""
            target_key = f"{pair}|{session}_{quarter}|{atr_bucket}"
        else:
            pair, session, atr_bucket = tk
            quarter = ""
            vol_bucket = ""
            target_key = f"{pair}|{session}|{atr_bucket}"
        n = int(a["n"])
        e_b = float(a["E_base"])
        e_n = float(a["E_nopatch"])
        e_p = float(a["E_patch"])
        eph_b = (e_b / wall_h) if wall_h > 0 else 0.0
        eph_n = (e_n / wall_h) if wall_h > 0 else 0.0
        eph_p = (e_p / wall_h) if wall_h > 0 else 0.0
        ex_h = (n / wall_h) if wall_h > 0 else 0.0
        ept_b = (e_b / n) if n > 0 else 0.0
        ept_p = (e_p / n) if n > 0 else 0.0
        target_rows.append(
            {
                "target_key": target_key,
                "pair": pair,
                "session": session,
                "quarter": quarter,
                "atr_bucket": atr_bucket,
                "vol_bucket": vol_bucket,
                "n": n,
                "E_base": e_b,
                "E_nopatch": e_n,
                "E_patch": e_p,
                "dE": e_p - e_b,
                "ddE_vs_nopatch": e_p - e_n,
                "Eph_base": eph_b,
                "Eph_nopatch": eph_n,
                "Eph_patch": eph_p,
                "dEph": eph_p - eph_b,
                "ddEph_vs_nopatch": eph_p - eph_n,
                "exits_base": n,
                "exits_patch": n,
                "d_exits": 0,
                "exits_per_hour_base": ex_h,
                "exits_per_hour_patch": ex_h,
                "d_exits_per_hour": 0.0,
                "entries_per_hour_base": ex_h,
                "entries_per_hour_patch": ex_h,
                "d_entries_per_hour": 0.0,
                "E_per_trade_base": ept_b,
                "E_per_trade_patch": ept_p,
                "d_E_per_trade": ept_p - ept_b,
                "avg_hold_sec_base": avg_hold_sec_proxy,
                "avg_hold_sec_patch": avg_hold_sec_proxy,
                "d_avg_hold_sec": 0.0,
            }
        )

    target_rows_sorted = sorted(target_rows, key=lambda x: float(x.get("Eph_patch", 0.0)))
    tail_n = max(1, int(math.ceil(0.10 * max(1, len(target_rows_sorted))))) if target_rows_sorted else 0
    tail_slice = target_rows_sorted[:tail_n] if tail_n > 0 else []
    target_rows_nopatch_sorted = sorted(target_rows, key=lambda x: float(x.get("Eph_nopatch", 0.0)))
    tail_nopatch_slice = target_rows_nopatch_sorted[:tail_n] if tail_n > 0 else []
    tail_mean_eph_base = (
        float(sum(float(r.get("Eph_base", 0.0)) for r in tail_slice) / len(tail_slice)) if tail_slice else 0.0
    )
    tail_mean_eph_nopatch = (
        float(sum(float(r.get("Eph_nopatch", 0.0)) for r in tail_nopatch_slice) / len(tail_nopatch_slice))
        if tail_nopatch_slice
        else 0.0
    )
    tail_mean_eph_patch = (
        float(sum(float(r.get("Eph_patch", 0.0)) for r in tail_slice) / len(tail_slice)) if tail_slice else 0.0
    )
    touched = [r for r in target_rows if abs(float(r.get("ddEph_vs_nopatch", 0.0))) > 1e-12 or abs(float(r.get("ddE_vs_nopatch", 0.0))) > 1e-12]
    touched_neg_ddcap = 0
    touched_neg_ddeph = 0
    for r in covered_rows:
        if abs(float(r.get("delta_expected_extraction_atr_vs_nopatch", 0.0))) <= 1e-12 and abs(
            float(r.get("delta_capture_to_ceiling_vs_nopatch", 0.0))
        ) <= 1e-12:
            continue
    for tr in touched:
        # proxy ddCAP by weighted mean across covered rows for same target key
        pass
    ddcap_vals = []
    ddee_vals = []
    matched_level_counts: dict[str, int] = {}
    for r in covered_rows:
        ddcap = float(r.get("delta_capture_to_ceiling_vs_nopatch") or 0.0)
        ddee = float(r.get("delta_expected_extraction_atr_vs_nopatch") or 0.0)
        ddcap_vals.append(ddcap)
        ddee_vals.append(ddee)
        ml = str(r.get("matched_level_patch", "NONE") or "NONE")
        matched_level_counts[ml] = matched_level_counts.get(ml, 0) + 1
    ddCAP_mean = float(sum(ddcap_vals) / len(ddcap_vals)) if ddcap_vals else 0.0
    ddEE_mean = float(sum(ddee_vals) / len(ddee_vals)) if ddee_vals else 0.0
    total_matched = sum(int(v) for v in matched_level_counts.values())
    fallback_like = sum(
        int(matched_level_counts.get(k, 0)) for k in ("GLOBAL", "SESSION_GLOBAL", "COARSE")
    )
    fallback_rate = (fallback_like / total_matched) if total_matched > 0 else 0.0

    # per-target ddCAP for touched target diagnostics
    targ_ddcap = {}
    targ_dd_eph = {}
    targ_key_row = {}
    for tr in target_rows:
        k = tr["target_key"]
        targ_dd_eph[k] = float(tr.get("ddEph_vs_nopatch", 0.0))
        targ_ddcap[k] = 0.0
        targ_key_row[k] = tr
    for r in covered_rows:
        if has_vol_patch_keys:
            k = f"{r['pair']}|{r['session']}_{r.get('quarter','')}|{r.get('vol_bucket', 'VOL_MID')}"
        elif has_quarter_patch_keys:
            k = f"{r['pair']}|{r['session']}_{r.get('quarter','')}|{r.get('atr_bucket', 'ATR_MID')}"
        else:
            k = f"{r['pair']}|{r['session']}|{r.get('atr_bucket', 'ATR_MID')}"
        targ_ddcap[k] = targ_ddcap.get(k, 0.0) + float(r.get("delta_capture_to_ceiling_vs_nopatch") or 0.0) * float(r.get("n") or 0.0)
    targ_n = {}
    for r in covered_rows:
        if has_vol_patch_keys:
            k = f"{r['pair']}|{r['session']}_{r.get('quarter','')}|{r.get('vol_bucket', 'VOL_MID')}"
        elif has_quarter_patch_keys:
            k = f"{r['pair']}|{r['session']}_{r.get('quarter','')}|{r.get('atr_bucket', 'ATR_MID')}"
        else:
            k = f"{r['pair']}|{r['session']}|{r.get('atr_bucket', 'ATR_MID')}"
        targ_n[k] = targ_n.get(k, 0.0) + float(r.get("n") or 0.0)
    for k, v in list(targ_ddcap.items()):
        n = max(targ_n.get(k, 0.0), 1e-9)
        targ_ddcap[k] = v / n
    touched_keys = [k for k, v in targ_dd_eph.items() if abs(v) > 1e-12 or abs(targ_ddcap.get(k, 0.0)) > 1e-12]
    touched_targets = len(touched_keys)
    touched_targets_neg_ddCAP = sum(1 for k in touched_keys if targ_ddcap.get(k, 0.0) < 0.0)
    touched_targets_neg_ddEph = sum(1 for k in touched_keys if targ_dd_eph.get(k, 0.0) < 0.0)
    worst_touched_ddCAP = min((float(targ_ddcap.get(k, 0.0)) for k in touched_keys), default=0.0)
    touched_targets_family_or_pair_match = 0
    touched_patch_keys_counts: dict[str, int] = {}
    vol_bucket_distribution: dict[str, int] = {"VOL_LOW": 0, "VOL_MID": 0, "VOL_HIGH": 0}
    for k in touched_keys:
        if has_vol_patch_keys:
            rows_k = [
                r
                for r in covered_rows
                if f"{r['pair']}|{r['session']}_{r.get('quarter','')}|{r.get('vol_bucket','VOL_MID')}" == k
            ]
        elif has_quarter_patch_keys:
            rows_k = [
                r
                for r in covered_rows
                if f"{r['pair']}|{r['session']}_{r.get('quarter','')}|{r.get('atr_bucket','ATR_MID')}" == k
            ]
        else:
            rows_k = [r for r in covered_rows if f"{r['pair']}|{r['session']}|{r.get('atr_bucket','ATR_MID')}" == k]
        if any(str(r.get("matched_level_patch", "")) in {"SESSION_FAMILY", "SESSION_PAIR"} for r in rows_k):
            touched_targets_family_or_pair_match += 1
        mk = next((str(r.get("matched_key_patch", "") or "") for r in rows_k if str(r.get("matched_key_patch", "") or "")), "")
        if mk:
            touched_patch_keys_counts[mk] = touched_patch_keys_counts.get(mk, 0) + 1
        tr = targ_key_row.get(k) or {}
        vb = str(tr.get("vol_bucket", "") or "")
        if vb in vol_bucket_distribution:
            vol_bucket_distribution[vb] += 1

    out["summary"] = {
        "wall_time_sec": float(wall_time_sec),
        "E_total_base": total_before_e,
        "E_total_nopatch": total_nopatch_e,
        "E_total_patch": total_after_e,
        "dE_total": total_after_e - total_before_e,
        "ddE_total_vs_nopatch": total_after_e - total_nopatch_e,
        "Eph_base": eph_base,
        "Eph_nopatch": eph_nopatch,
        "Eph_patch": eph_patch,
        "dEph": eph_patch - eph_base,
        "ddEph_vs_nopatch": eph_patch - eph_nopatch,
        "exits_base": exits_base,
        "exits_nopatch": exits_base,
        "exits_patch": exits_patch,
        "d_exits": exits_patch - exits_base,
        "exits_per_hour_base": exits_h_base,
        "exits_per_hour_nopatch": exits_h_base,
        "exits_per_hour_patch": exits_h_patch,
        "d_exits_per_hour": exits_h_patch - exits_h_base,
        "entries_per_hour_base": entries_h_base,
        "entries_per_hour_patch": entries_h_patch,
        "d_entries_per_hour": entries_h_patch - entries_h_base,
        "E_per_trade_base": e_per_trade_base,
        "E_per_trade_patch": e_per_trade_patch,
        "d_E_per_trade": e_per_trade_patch - e_per_trade_base,
        "avg_hold_sec_base": avg_hold_sec_proxy,
        "avg_hold_sec_patch": avg_hold_sec_proxy,
        "d_avg_hold_sec": 0.0,
        "tail_mean_Eph_base": tail_mean_eph_base,
        "tail_mean_Eph_nopatch": tail_mean_eph_nopatch,
        "tail_mean_Eph_patch": tail_mean_eph_patch,
        "d_tail_mean_Eph": tail_mean_eph_patch - tail_mean_eph_base,
        "tail_n": int(tail_n),
    }
    out["delta_vs_nopatch"] = {
        "ddEE_mean": ddEE_mean,
        "ddCAP_mean": ddCAP_mean,
        "ddEph": eph_patch - eph_nopatch,
        "ddTail_mean_Eph": tail_mean_eph_patch - tail_mean_eph_nopatch,
        "ddExits_per_hour": exits_h_patch - exits_h_base,
        "touched_targets": touched_targets,
        "touched_targets_neg_ddCAP": touched_targets_neg_ddCAP,
        "touched_targets_neg_ddEph": touched_targets_neg_ddEph,
        "worst_touched_ddCAP": worst_touched_ddCAP,
        "touched_targets_family_or_pair_match": touched_targets_family_or_pair_match,
        "matched_level_counts": matched_level_counts,
        "fallback_rate": fallback_rate,
        "touched_keys": sorted(touched_keys),
        "touched_patch_keys_counts": dict(sorted(touched_patch_keys_counts.items(), key=lambda kv: kv[0])),
        "vol_bucket_distribution": vol_bucket_distribution,
    }
    if cache_fingerprint:
        out["cache_fingerprint"] = cache_fingerprint

    has_family_patch = any(
        isinstance(p, dict) and str(p.get("level", "") or "") == "SESSION_FAMILY" for p in patches
    )
    if args.enforce_family_touch and has_family_patch and touched_targets <= 0:
        raise SystemExit("FAMILY_TOUCH_FAIL: patch has SESSION_FAMILY keys but touched_targets == 0")
    if int(args.min_touched_targets or 0) > 0 and touched_targets < int(args.min_touched_targets):
        raise SystemExit(
            f"TOUCH_COVERAGE_FAIL: touched_targets={touched_targets} < min_touched_targets={int(args.min_touched_targets)}"
        )
    min_vol_bucket_touched = int(args.min_vol_bucket_touched or 0)
    if min_vol_bucket_touched > 0 and has_vol_patch_keys:
        low = int(vol_bucket_distribution.get("VOL_LOW", 0))
        mid = int(vol_bucket_distribution.get("VOL_MID", 0))
        high = int(vol_bucket_distribution.get("VOL_HIGH", 0))
        if min(low, mid, high) < min_vol_bucket_touched:
            raise SystemExit(
                "VOL_BUCKET_TOUCH_FAIL: "
                f"low={low} mid={mid} high={high} min_required={min_vol_bucket_touched}"
            )
    if args.enforce_tier_touches:
        has_pair_patch = any(isinstance(p, dict) and str(p.get("level", "")) == "SESSION_PAIR" for p in patches)
        if has_family_patch and int(matched_level_counts.get("SESSION_FAMILY", 0)) <= 0:
            raise SystemExit("TIER_TOUCH_FAIL: SESSION_FAMILY keys exist but matched_level_counts[SESSION_FAMILY]==0")
        if has_pair_patch and int(matched_level_counts.get("SESSION_PAIR", 0)) <= 0:
            raise SystemExit("TIER_TOUCH_FAIL: SESSION_PAIR keys exist but matched_level_counts[SESSION_PAIR]==0")
    if args.enforce_quarter_no_shadow and has_quarter_patch_keys:
        tpk = out.get("delta_vs_nopatch", {}).get("touched_patch_keys_counts", {}) or {}
        quarter_touches = sum(int(v or 0) for k, v in tpk.items() if "_Q" in str(k))
        session_touches = sum(int(v or 0) for k, v in tpk.items() if "_Q" not in str(k) and "|" in str(k))
        if quarter_touches <= 0 and session_touches > 0:
            raise SystemExit(
                f"SHADOW_QUARTER_FAIL: quarter_touches={quarter_touches} session_touches={session_touches}"
            )
    out["targets"] = target_rows
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out), encoding="utf-8")
    print(f"WROTE {out_path}")
    print(f"total_possible_states={len(rows)} covered_states={covered} coverage_ratio={out['coverage_ratio']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
