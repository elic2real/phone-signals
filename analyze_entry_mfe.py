#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median
from typing import Iterable


@dataclass
class TradeMFE:
    pair: str
    day: str
    session: str
    trade_id: str
    sl_pips: float
    mfe_pips: float
    mfe_r: float
    time_to_mfe_sec: float


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _percentile(sorted_vals: list[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    if p <= 0:
        return sorted_vals[0]
    if p >= 100:
        return sorted_vals[-1]
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def _context_from_path(path: Path) -> tuple[str, str, str]:
    # compiled_market_nodes/PAIR__day__session/aee_stage/aee_state_stream/aee_state_stream.csv
    node = path.parts[path.parts.index("compiled_market_nodes") + 1]
    bits = node.split("__")
    pair = bits[0] if len(bits) > 0 else "UNKNOWN"
    day = bits[1] if len(bits) > 1 else "unknown"
    session = bits[2] if len(bits) > 2 else "unknown"
    return pair, day, session


def _stream_paths(root: Path, max_streams: int) -> list[Path]:
    globs = [
        "compiled_market_nodes/EUR_USD__*/aee_stage/aee_state_stream/aee_state_stream.csv",
        "compiled_market_nodes/EUR_CHF__*/aee_stage/aee_state_stream/aee_state_stream.csv",
        "compiled_market_nodes/USD_CAD__*/aee_stage/aee_state_stream/aee_state_stream.csv",
        "compiled_market_nodes/EUR_GBP__*/aee_stage/aee_state_stream/aee_state_stream.csv",
    ]
    out: list[Path] = []
    for g in globs:
        out.extend([p.resolve() for p in root.glob(g) if p.is_file()])
    out = sorted(set(out))
    return out[: max(1, int(max_streams))]


def _calc_mfe_for_trade(rows: list[dict[str, str]], pair: str, day: str, session: str, trade_id: str) -> TradeMFE:
    rows.sort(key=lambda r: int(_safe_float(r.get("bar_index", 0), 0)))
    sl_pips = max(0.1, _safe_float(rows[0].get("target_distance", 1.0), 1.0))

    profits = [_safe_float(r.get("profit_now", 0.0), 0.0) for r in rows]
    bars = [max(1, int(_safe_float(r.get("bar_index", i + 1), i + 1))) for i, r in enumerate(rows)]

    if not profits:
        return TradeMFE(pair, day, session, trade_id, sl_pips, 0.0, 0.0, 0.0)

    mfe_pips = max(profits)
    mfe_idx = profits.index(mfe_pips)
    time_to_mfe_sec = float(bars[mfe_idx]) * 60.0
    mfe_r = mfe_pips / sl_pips

    return TradeMFE(pair, day, session, trade_id, sl_pips, mfe_pips, mfe_r, time_to_mfe_sec)


def _iter_trades(stream: Path) -> Iterable[TradeMFE]:
    pair, day, session = _context_from_path(stream)
    by_trade: dict[str, list[dict[str, str]]] = defaultdict(list)
    with stream.open(newline="", encoding="utf-8", errors="ignore") as f:
        for row in csv.DictReader(f):
            tid = str(row.get("trade_id", "")).strip()
            if tid:
                by_trade[tid].append(row)

    for tid, rows in by_trade.items():
        if rows:
            yield _calc_mfe_for_trade(rows, pair, day, session, tid)


def _summarize(trades: list[TradeMFE], friction_pips: float) -> dict:
    if not trades:
        return {
            "total_trades": 0,
            "avg_mfe_r": 0.0,
            "median_mfe_r": 0.0,
            "p50_mfe_r": 0.0,
            "p70_mfe_r": 0.0,
            "p80_mfe_r": 0.0,
            "p90_mfe_r": 0.0,
            "p95_mfe_r": 0.0,
            "avg_mfe_pips": 0.0,
            "pct_above_friction": 0.0,
            "avg_time_to_mfe_sec": 0.0,
        }

    mfe_r = sorted([t.mfe_r for t in trades])
    mfe_pips = [t.mfe_pips for t in trades]
    ttm = [t.time_to_mfe_sec for t in trades]
    pct_above = sum(1 for v in mfe_pips if v > friction_pips) / len(mfe_pips)

    return {
        "total_trades": len(trades),
        "avg_mfe_r": mean(mfe_r),
        "median_mfe_r": median(mfe_r),
        "p50_mfe_r": _percentile(mfe_r, 50),
        "p70_mfe_r": _percentile(mfe_r, 70),
        "p80_mfe_r": _percentile(mfe_r, 80),
        "p90_mfe_r": _percentile(mfe_r, 90),
        "p95_mfe_r": _percentile(mfe_r, 95),
        "avg_mfe_pips": mean(mfe_pips),
        "pct_above_friction": 100.0 * pct_above,
        "avg_time_to_mfe_sec": mean(ttm),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Measure entry quality by MFE in R")
    ap.add_argument("--max-streams", type=int, default=48)
    ap.add_argument("--spread-pips", type=float, default=0.8)
    ap.add_argument("--slippage-pips-per-side", type=float, default=0.15)
    ap.add_argument("--commission-pips-roundtrip", type=float, default=0.0)
    ap.add_argument("--latency-penalty-pips", type=float, default=0.0)
    ap.add_argument("--out", default="analyze_entry_mfe_report.json")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    streams = _stream_paths(root, args.max_streams)
    if not streams:
        raise SystemExit("No state streams found")

    friction_pips = (
        max(0.0, float(args.spread_pips))
        + (2.0 * max(0.0, float(args.slippage_pips_per_side)))
        + max(0.0, float(args.commission_pips_roundtrip))
        + max(0.0, float(args.latency_penalty_pips))
    )

    trades: list[TradeMFE] = []
    for s in streams:
        trades.extend(list(_iter_trades(s)))

    by_pair: dict[str, list[TradeMFE]] = defaultdict(list)
    by_session: dict[str, list[TradeMFE]] = defaultdict(list)
    by_context: dict[str, list[TradeMFE]] = defaultdict(list)
    for t in trades:
        by_pair[t.pair].append(t)
        by_session[t.session].append(t)
        by_context[f"{t.pair}__{t.day}__{t.session}"].append(t)

    report = {
        "name": "analyze_entry_mfe_report",
        "streams_used": len(streams),
        "friction_pips": friction_pips,
        "overall": _summarize(trades, friction_pips),
        "by_pair": {k: _summarize(v, friction_pips) for k, v in sorted(by_pair.items())},
        "by_session": {k: _summarize(v, friction_pips) for k, v in sorted(by_session.items())},
        "by_context": {k: _summarize(v, friction_pips) for k, v in sorted(by_context.items())},
    }

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = root / out_path
    out_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    print(json.dumps({
        "out": str(out_path),
        "streams_used": len(streams),
        "friction_pips": friction_pips,
        **report["overall"],
    }, indent=2))


if __name__ == "__main__":
    main()
