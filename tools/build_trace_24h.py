#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd


def _session_from_hour(h: int) -> str:
    if 8 <= h < 16:
        return "LONDON"
    if 14 <= h < 21:
        return "NY"
    return "ASIA"


def _quarter_from_month(m: int) -> str:
    return f"Q{((m - 1) // 3) + 1}"


def _parse_utc(s: str) -> datetime:
    x = s.strip()
    if x.endswith("Z"):
        x = x[:-1] + "+00:00"
    dt = datetime.fromisoformat(x)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _pip_factor(pair: str) -> float:
    # price delta -> pips
    return 100.0 if pair.endswith("JPY") else 10000.0


def _spread_bucket(spread_pips: float) -> str:
    if spread_pips <= 1.5:
        return "SB_0_1p5"
    if spread_pips <= 3.0:
        return "SB_1p5_3"
    if spread_pips <= 5.0:
        return "SB_3_5"
    return "SB_GT_5"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tape-root", default="data_tape_stitched_15_full")
    ap.add_argument("--pairs", default="")
    ap.add_argument("--start-utc", required=True)
    ap.add_argument("--end-utc", required=True)
    ap.add_argument("--horizon-bars", type=int, default=12)
    ap.add_argument("--eval-step", type=int, default=1, help="rows between eval snapshots")
    ap.add_argument("--substeps-per-bar", type=int, default=6, help="intra-bar synthetic eval steps")
    ap.add_argument("--stall-band-min", type=float, default=0.6)
    ap.add_argument("--out", default="proof_artifacts/TRACE_24H.jsonl.gz")
    ap.add_argument("--manifest-out", default="proof_artifacts/TRACE_MANIFEST.json")
    args = ap.parse_args()

    tape_root = Path(args.tape_root)
    pairs = [p.strip().upper() for p in args.pairs.split(",") if p.strip()]
    if not pairs:
        pairs = [p.name.split("=")[1] for p in tape_root.glob("pair=*") if p.is_dir()]
    dt_start = _parse_utc(args.start_utc)
    dt_end = _parse_utc(args.end_utc)
    h = max(2, int(args.horizon_bars))
    eval_step = max(1, int(args.eval_step))
    substeps = max(1, int(args.substeps_per_bar))
    stall_band_min = float(args.stall_band_min)

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    n_rows = 0
    n_trades = 0
    buckets = set()
    rows_per_trade = []
    spread_bucket_counts: dict[str, int] = {"SB_0_1p5": 0, "SB_1p5_3": 0, "SB_3_5": 0, "SB_GT_5": 0}
    atr_values_by_pair_session: dict[tuple[str, str], list[float]] = {}

    # First pass: collect ATR exec values per (pair, session) for quantile bucketization.
    for pair in pairs:
        fp = tape_root / f"pair={pair}" / "stitched.parquet"
        if not fp.exists():
            continue
        df = pd.read_parquet(fp)
        if not {"timestamp", "open", "high", "low", "close"}.issubset(df.columns):
            continue
        ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df = df[(ts >= pd.Timestamp(dt_start)) & (ts <= pd.Timestamp(dt_end))].copy()
        ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        if len(df) <= h + 20:
            continue
        close = pd.to_numeric(df["close"], errors="coerce")
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        prev_close = close.shift(1)
        tr = pd.concat([(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
        atr = tr.rolling(14, min_periods=14).mean()
        pipf = _pip_factor(pair)
        for i in range(20, len(df) - h):
            if pd.isna(ts.iloc[i]) or pd.isna(atr.iloc[i]) or atr.iloc[i] <= 0:
                continue
            dt = ts.iloc[i].to_pydatetime().astimezone(timezone.utc)
            session = _session_from_hour(int(dt.hour))
            atr_pips = float(atr.iloc[i]) * pipf
            atr_values_by_pair_session.setdefault((pair, session), []).append(atr_pips)

    atr_cutpoints: dict[tuple[str, str], tuple[float, float]] = {}
    for k, vals in atr_values_by_pair_session.items():
        if not vals:
            continue
        s = sorted(vals)
        n = len(s)
        p33 = s[int(0.33 * (n - 1))]
        p66 = s[int(0.66 * (n - 1))]
        atr_cutpoints[k] = (float(p33), float(p66))

    with gzip.open(out_path, "wt", encoding="utf-8") as gz:
        for pair in pairs:
            fp = tape_root / f"pair={pair}" / "stitched.parquet"
            if not fp.exists():
                continue
            df = pd.read_parquet(fp)
            if not {"timestamp", "open", "high", "low", "close"}.issubset(df.columns):
                continue
            ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            df = df[(ts >= pd.Timestamp(dt_start)) & (ts <= pd.Timestamp(dt_end))].copy()
            ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            if len(df) <= h + 20:
                continue
            close = pd.to_numeric(df["close"], errors="coerce")
            high = pd.to_numeric(df["high"], errors="coerce")
            low = pd.to_numeric(df["low"], errors="coerce")
            prev_close = close.shift(1)
            tr = pd.concat([(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
            atr = tr.rolling(14, min_periods=14).mean()
            pipf = _pip_factor(pair)
            # deterministic spread proxy by pair class + local volatility
            base_spread = 1.0 if pair in {"EUR_USD", "USD_JPY", "GBP_USD", "USD_CHF", "USD_CAD", "AUD_USD", "NZD_USD"} else 1.8

            for i in range(20, len(df) - h):
                if pd.isna(ts.iloc[i]) or pd.isna(close.iloc[i]) or pd.isna(atr.iloc[i]) or atr.iloc[i] <= 0:
                    continue
                dt = ts.iloc[i].to_pydatetime().astimezone(timezone.utc)
                session = _session_from_hour(int(dt.hour))
                weekday = dt.strftime("%a")
                quarter = _quarter_from_month(int(dt.month))
                month = dt.strftime("%b")
                buckets.add((pair, session, weekday, quarter, month))

                entry = float(close.iloc[i])
                atr_i = float(atr.iloc[i])
                atr_exec_pips = atr_i * pipf
                p33, p66 = atr_cutpoints.get((pair, session), (atr_exec_pips, atr_exec_pips))
                if atr_exec_pips <= p33:
                    atr_bucket = "ATR_LOW"
                elif atr_exec_pips <= p66:
                    atr_bucket = "ATR_MID"
                else:
                    atr_bucket = "ATR_HIGH"
                mom = float(close.iloc[i] - close.iloc[i - 3])
                is_long = mom >= 0.0
                side = "LONG" if is_long else "SHORT"
                trade_key = f"{pair}|{int(dt.timestamp())}|{side}|sim"
                n_trades += 1

                mfe = 0.0
                mae = 0.0
                fail_w = 0
                eval_count = 0
                decision_prev = None
                prev_giveback = 0.0
                prev_eval_ts = None
                stall_start_ts = None
                for j in range(i + 1, i + h + 1, eval_step):
                    if j >= len(df):
                        break
                    bar_open = float(close.iloc[j - 1]) if j > 0 else float(close.iloc[j])
                    bar_close = float(close.iloc[j])
                    hi = float(high.iloc[j])
                    lo = float(low.iloc[j])
                    bar_ts = float(ts.iloc[j].timestamp())
                    prev_ts = float(ts.iloc[j - 1].timestamp()) if j > 0 else bar_ts - 60.0

                    for sidx in range(1, substeps + 1):
                        frac = sidx / float(substeps)
                        eval_count += 1
                        px = bar_open + (bar_close - bar_open) * frac
                        t_eval = prev_ts + (bar_ts - prev_ts) * frac
                        pnl = (px - entry) / atr_i if is_long else (entry - px) / atr_i
                        fav = (hi - entry) / atr_i if is_long else (entry - lo) / atr_i
                        adv = (lo - entry) / atr_i if is_long else (entry - hi) / atr_i
                        mfe = max(mfe, fav)
                        mae = min(mae, adv)
                        giveback = ((mfe - pnl) / mfe) if mfe > 1e-9 else 0.0
                        expected = max(0.08, 0.08 * eval_count)
                        energy = pnl / expected if expected > 1e-9 else 0.0
                        if energy < 0.8:
                            fail_w += 1
                        else:
                            fail_w = 0

                        # simple deterministic baseline policy for change flags
                        if pnl <= -0.5:
                            decision = "CLOSE"
                        elif fail_w >= 2 and energy < 0.8:
                            decision = "CLOSE"
                        elif mfe >= 1.0 and energy >= 1.0:
                            decision = "RUNNER"
                        else:
                            decision = "HOLD"
                        decision_changed = (decision_prev is not None and decision != decision_prev)

                        stall_prox = float(max(0.0, min(1.0, abs(expected - pnl))))
                        if stall_prox >= stall_band_min:
                            if stall_start_ts is None:
                                stall_start_ts = t_eval
                            stall_dur = t_eval - stall_start_ts
                        else:
                            stall_start_ts = None
                            stall_dur = 0.0

                        if prev_eval_ts is None:
                            giveback_speed = 0.0
                        else:
                            dt_sec = max(1e-3, t_eval - prev_eval_ts)
                            giveback_speed = (giveback - prev_giveback) / dt_sec

                        # spread proxy: base + volatility scaler + intrabar stress
                        vol_scale = 0.0
                        if p66 > 1e-9:
                            vol_scale = max(0.0, min(2.0, atr_exec_pips / max(1e-9, p66)))
                        intrabar = abs(hi - lo) * pipf
                        spread_pips = max(0.1, min(8.0, base_spread + 0.35 * vol_scale + 0.04 * intrabar))
                        spread_bucket = _spread_bucket(spread_pips)
                        spread_bucket_counts[spread_bucket] += 1

                        row = {
                            "trade_key": trade_key,
                            "eval_seq": eval_count,
                            "pair": pair,
                            "session": session,
                            "weekday": weekday,
                            "quarter": quarter,
                            "month": month,
                            "regime": "unknown",
                            "ts_utc": float(t_eval),
                            "time_in_trade_sec": float(t_eval - float(ts.iloc[i].timestamp())),
                            "pnl_atr": float(pnl),
                            "pnl_pips": float(pnl * atr_exec_pips),
                            "mfe_atr": float(mfe),
                            "mae_atr": float(mae),
                            "energy_ratio": float(energy),
                            "velocity": float((bar_close - bar_open) / atr_i) if atr_i > 1e-9 else 0.0,
                            "giveback_ratio": float(max(0.0, min(1.0, giveback))),
                            "stall_proximity": stall_prox,
                            "dist_to_tp_bin": "NEAR" if pnl >= 0.7 else ("MID" if pnl >= 0.3 else "FAR"),
                            "runner_mode": bool(mfe >= 1.0),
                            "aee_phase": "RUNNER" if mfe >= 1.0 else "PROTECT",
                            "consecutive_fail_windows": int(fail_w),
                            "tp_dist_atr": 1.0,
                            "sl_dist_atr": 0.6,
                            "baseline_decision": decision,
                            # v2 required fields
                            "spread_pips": float(spread_pips),
                            "spread_bucket": spread_bucket,
                            "atr_exec_pips": float(atr_exec_pips),
                            "atr_bucket": atr_bucket,
                            "stall_duration_sec": float(max(0.0, stall_dur)),
                            "giveback_speed": float(giveback_speed),
                            "decision_prev": decision_prev,
                            "decision_changed": bool(decision_changed),
                        }
                        gz.write(json.dumps(row) + "\n")
                        n_rows += 1
                        prev_eval_ts = t_eval
                        prev_giveback = giveback
                        decision_prev = decision

                rows_per_trade.append(eval_count)

    rows_sorted = sorted(rows_per_trade) if rows_per_trade else [0]
    nrt = len(rows_sorted)
    p50 = rows_sorted[int(0.50 * (nrt - 1))] if nrt else 0
    p90 = rows_sorted[int(0.90 * (nrt - 1))] if nrt else 0
    mean_rows = (sum(rows_sorted) / nrt) if nrt else 0.0
    atr_cutpoints_json = {
        f"{k[0]}|{k[1]}": {"p33": v[0], "p66": v[1]} for k, v in sorted(atr_cutpoints.items())
    }
    manifest = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "trace_schema_version": 2,
        "enums": {
            "spread_bucket": ["SB_0_1p5", "SB_1p5_3", "SB_3_5", "SB_GT_5"],
            "atr_bucket": ["ATR_LOW", "ATR_MID", "ATR_HIGH"],
        },
        "source": {
            "tape_root": str(tape_root),
            "pairs": pairs,
            "start_utc": args.start_utc,
            "end_utc": args.end_utc,
            "horizon_bars": h,
            "eval_step": eval_step,
            "substeps_per_bar": substeps,
            "stall_band_min": stall_band_min,
        },
        "trace": {
            "path": str(out_path),
            "rows": n_rows,
            "trades": n_trades,
            "buckets_covered": len(buckets),
            "rows_per_trade_mean": mean_rows,
            "rows_per_trade_p50": p50,
            "rows_per_trade_p90": p90,
            "spread_bucket_distribution": spread_bucket_counts,
            "atr_bucket_cutpoints_pair_session": atr_cutpoints_json,
        },
    }
    mpath = Path(args.manifest_out)
    mpath.parent.mkdir(parents=True, exist_ok=True)
    mpath.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"TRACE_OK {out_path} rows={n_rows} trades={n_trades} buckets={len(buckets)}")
    print(f"MANIFEST_OK {mpath}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
