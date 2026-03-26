#!/usr/bin/env python3
"""Extract V1 market behavior stats for synthetic session generation.

Inputs: CSV with at least ts + one of (mid) or (bid,ask).
Outputs: JSON stats/session_<SESSION>.json
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


def _bucketize(v: float, edges: List[float]) -> str:
    for i in range(len(edges) - 1):
        lo, hi = edges[i], edges[i + 1]
        if lo <= v < hi:
            return f"{lo:.2f}-{hi:.2f}"
    return f"{edges[-2]:.2f}+"


def _norm_counter(counter: Counter) -> Dict[str, float]:
    total = float(sum(counter.values()))
    if total <= 0:
        return {}
    return {k: round(v / total, 6) for k, v in sorted(counter.items())}


def _pct(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    idx = int(max(0, min(len(vals) - 1, round((len(vals) - 1) * q))))
    return float(vals[idx])


def _session_from_hour(hour_utc: int) -> str:
    if 0 <= hour_utc < 8:
        return "ASIA"
    if 8 <= hour_utc < 16:
        return "LONDON"
    return "NY"


def _load_rows(csv_path: Path) -> List[dict]:
    rows: List[dict] = []
    with csv_path.open("r", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for r in rd:
            try:
                ts = float(r.get("ts") or r.get("timestamp") or 0.0)
                mid = float(r.get("mid") or 0.0)
                bid = float(r.get("bid") or 0.0)
                ask = float(r.get("ask") or 0.0)
                if mid <= 0.0 and bid > 0.0 and ask > 0.0:
                    mid = 0.5 * (bid + ask)
                if bid <= 0.0 and ask > 0.0 and mid > 0.0:
                    bid = mid
                if ask <= 0.0 and bid > 0.0 and mid > 0.0:
                    ask = mid
                if ts <= 0.0 or mid <= 0.0:
                    continue
                spread_pips = float(r.get("spread_pips") or 0.0)
                if spread_pips <= 0.0 and ask > bid > 0.0:
                    pip = 0.01 if "JPY" in str(r.get("pair") or r.get("instrument") or "") else 0.0001
                    spread_pips = (ask - bid) / pip
                rows.append(
                    {
                        "ts": ts,
                        "mid": mid,
                        "bid": bid,
                        "ask": ask,
                        "pair": str(r.get("pair") or r.get("instrument") or ""),
                        "session": str(r.get("session") or ""),
                        "spread_pips": max(0.0, spread_pips),
                    }
                )
            except Exception:
                continue
    rows.sort(key=lambda x: x["ts"])
    return rows


def _compute_atr_proxy(rows: List[dict], window: int = 14) -> List[float]:
    tr: List[float] = [0.0]
    for i in range(1, len(rows)):
        tr.append(abs(rows[i]["mid"] - rows[i - 1]["mid"]))
    atr: List[float] = []
    acc = 0.0
    q: List[float] = []
    for x in tr:
        q.append(x)
        acc += x
        if len(q) > window:
            acc -= q.pop(0)
        atr.append((acc / max(1, len(q))) if q else 0.0)
    return atr


def _detect_impulses(rows: List[dict], atr: List[float], min_move_atr: float = 0.3, max_len: int = 40) -> List[Tuple[int, int, int, float]]:
    out: List[Tuple[int, int, int, float]] = []
    i = 1
    n = len(rows)
    while i < n - 2:
        base_atr = max(1e-12, atr[i])
        j = min(n - 1, i + max_len)
        best_k = -1
        best_score = 0.0
        best_dir = 0
        for k in range(i + 1, j + 1):
            move = rows[k]["mid"] - rows[i]["mid"]
            move_atr = abs(move) / base_atr
            if move_atr < min_move_atr:
                continue
            path = 0.0
            for m in range(i + 1, k + 1):
                path += abs(rows[m]["mid"] - rows[m - 1]["mid"])
            eff = abs(move) / max(1e-12, path)
            score = move_atr * eff
            if eff >= 0.7 and score > best_score:
                best_score = score
                best_k = k
                best_dir = 1 if move > 0 else -1
        if best_k > i:
            move_atr = abs(rows[best_k]["mid"] - rows[i]["mid"]) / base_atr
            out.append((i, best_k, best_dir, move_atr))
            i = best_k + 1
        else:
            i += 1
    return out


def _post_pullback_outcome(rows: List[dict], atr: List[float], imp: Tuple[int, int, int, float], timeout: int = 50) -> Tuple[float, str]:
    i0, i1, idir, _ = imp
    start = i1 + 1
    if start >= len(rows):
        return 0.0, "stall"
    imp_hi = max(rows[k]["mid"] for k in range(i0, i1 + 1))
    imp_lo = min(rows[k]["mid"] for k in range(i0, i1 + 1))
    base_atr = max(1e-12, atr[i1])
    pb_max = 0.0
    end = min(len(rows) - 1, start + timeout)
    for k in range(start, end + 1):
        px = rows[k]["mid"]
        if idir > 0:
            pb = max(0.0, imp_hi - px)
            pb_max = max(pb_max, pb)
            if px >= imp_hi + (0.15 * base_atr):
                return pb_max / max(1e-12, (imp_hi - rows[i0]["mid"])), "continue"
            if px <= imp_lo - (0.15 * base_atr):
                return pb_max / max(1e-12, (imp_hi - rows[i0]["mid"])), "reverse"
        else:
            pb = max(0.0, px - imp_lo)
            pb_max = max(pb_max, pb)
            if px <= imp_lo - (0.15 * base_atr):
                return pb_max / max(1e-12, (rows[i0]["mid"] - imp_lo)), "continue"
            if px >= imp_hi + (0.15 * base_atr):
                return pb_max / max(1e-12, (rows[i0]["mid"] - imp_lo)), "reverse"
    return pb_max / max(1e-12, abs(rows[i1]["mid"] - rows[i0]["mid"])), "stall"


def extract_stats(rows: List[dict], session: str) -> dict:
    if len(rows) < 100:
        return {"session": session, "error": "not_enough_rows", "n_rows": len(rows)}
    atr = _compute_atr_proxy(rows)
    atr_nonzero = [x for x in atr if x > 0.0]
    atr_med = _pct(atr_nonzero, 0.5)

    phase_edges = [0.0, 0.1, 0.3, 0.7, 1.01]
    phase_atr: Dict[str, List[float]] = defaultdict(list)
    phase_spread: Dict[str, List[float]] = defaultdict(list)
    n = len(rows)
    for i, r in enumerate(rows):
        phase = i / max(1, (n - 1))
        pb = _bucketize(phase, phase_edges)
        phase_atr[pb].append(atr[i])
        phase_spread[pb].append(float(r.get("spread_pips", 0.0) or 0.0))

    imps = _detect_impulses(rows, atr)
    imp_bins = Counter()
    pb_bins = Counter()
    outcomes = Counter()
    imp_edges = [0.1, 0.3, 0.6, 1.0, 2.0]
    pb_edges = [0.0, 0.2, 0.4, 0.6, 1.0]
    for imp in imps:
        _, _, _, size_atr = imp
        imp_bins[_bucketize(size_atr, imp_edges)] += 1
        pb_ratio, outcome = _post_pullback_outcome(rows, atr, imp)
        pb_bins[_bucketize(max(0.0, min(pb_ratio, 5.0)), pb_edges)] += 1
        outcomes[outcome] += 1

    vol_profile = {}
    for k, vals in sorted(phase_atr.items()):
        med = _pct(vals, 0.5)
        vol_profile[k] = round((med / atr_med), 6) if atr_med > 0 else 1.0
    spread_profile = {k: round(_pct(v, 0.5), 6) for k, v in sorted(phase_spread.items())}
    spread_all = [float(r.get("spread_pips", 0.0) or 0.0) for r in rows]
    atr_p90 = _pct(atr_nonzero, 0.9)
    spread_p50 = _pct(spread_all, 0.5)
    high_vol_spreads = [spread_all[i] for i in range(len(rows)) if atr[i] >= atr_p90]
    atr_p90_mult = (_pct(high_vol_spreads, 0.5) / spread_p50) if spread_p50 > 0 and high_vol_spreads else 1.0
    open_mult = (spread_profile.get("0.00-0.10", spread_p50) / spread_p50) if spread_p50 > 0 else 1.0

    return {
        "session": session,
        "n_rows": len(rows),
        "n_impulses": len(imps),
        "impulse_atr_bins": _norm_counter(imp_bins),
        "pullback_ratio_bins": _norm_counter(pb_bins),
        "post_pullback_outcome": _norm_counter(outcomes),
        "session_vol_profile": vol_profile,
        "session_spread_profile": spread_profile,
        "spread_profile": {
            "base_median_pips": round(spread_p50, 6),
            "atr_p90_multiplier": round(float(atr_p90_mult), 6),
            "open_phase_multiplier": round(float(open_mult), 6),
        },
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Extract V1 market stats from historical CSV.")
    ap.add_argument("--input", required=True, help="Input CSV path (ts,mid or bid/ask columns).")
    ap.add_argument("--session", default="LONDON", choices=["ASIA", "LONDON", "NY"], help="Session to extract.")
    ap.add_argument("--outdir", default="stats", help="Output directory.")
    args = ap.parse_args()

    rows = _load_rows(Path(args.input))
    rows_sess = []
    for r in rows:
        sess = str(r.get("session") or "").upper()
        if not sess:
            hour = int((r["ts"] // 3600) % 24)
            sess = _session_from_hour(hour)
        if sess == args.session:
            rows_sess.append(r)

    stats = extract_stats(rows_sess, args.session)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    out_path = outdir / f"session_{args.session}.json"
    out_path.write_text(json.dumps(stats, indent=2), encoding="utf-8")
    print(str(out_path))


if __name__ == "__main__":
    main()
