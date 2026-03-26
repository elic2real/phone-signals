#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _parse_ts(ts: str) -> float | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        return list(csv.DictReader(f))


def _mean(values: list[float]) -> float:
    return (sum(values) / len(values)) if values else 0.0


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    m = _mean(values)
    var = sum((x - m) ** 2 for x in values) / len(values)
    return math.sqrt(max(0.0, var))


def _effect_size(pos: list[float], neg: list[float]) -> float:
    if not pos or not neg:
        return 0.0
    m1, m2 = _mean(pos), _mean(neg)
    s1, s2 = _std(pos), _std(neg)
    pooled = math.sqrt((s1 * s1 + s2 * s2) / 2.0)
    if pooled <= 1e-9:
        return 0.0
    return (m1 - m2) / pooled


def _node_name_from_stream(path: Path) -> str:
    parts = path.parts
    if "compiled_market_nodes" in parts:
        i = parts.index("compiled_market_nodes")
        if i + 1 < len(parts):
            return parts[i + 1]
    return path.parent.parent.parent.name


def _screen_stream(path: Path, min_trades: int) -> dict[str, Any] | None:
    rows = _load_csv_rows(path)
    if not rows:
        return None

    by_trade: dict[str, list[dict[str, str]]] = defaultdict(list)
    ts_values: list[float] = []
    for row in rows:
        tid = str(row.get("trade_id", "")).strip()
        if not tid:
            continue
        by_trade[tid].append(row)
        ts = _parse_ts(str(row.get("timestamp", "")))
        if ts is not None:
            ts_values.append(ts)

    for trows in by_trade.values():
        trows.sort(key=lambda r: int(float(r.get("bar_index", "0") or "0")))

    trades = [trows for trows in by_trade.values() if trows]
    if len(trades) < min_trades:
        return None

    static_pips: list[float] = []
    static_r: list[float] = []
    win_flags: list[int] = []
    sl_flags: list[int] = []

    tp_pre_build_slope: list[float] = []
    sl_pre_build_slope: list[float] = []
    tp_pre_align: list[float] = []
    sl_pre_align: list[float] = []
    tp_opp: list[float] = []
    sl_opp: list[float] = []

    for trows in trades:
        first = trows[0]
        last = trows[-1]
        reason = str(last.get("static_reason", ""))
        pips = _safe_float(last.get("static_pips", 0.0))
        r = _safe_float(last.get("static_R", 0.0))
        static_pips.append(pips)
        static_r.append(r)
        win_flags.append(1 if reason == "TP_HIT" else 0)
        sl_flags.append(1 if reason == "SL_HIT" or pips < 0.0 else 0)

        pre_build_slope = _safe_float(first.get("pre_build_slope", 0.0))
        pre_align = _safe_float(first.get("pre_macro_micro_alignment", 0.0))
        opp_strength = _safe_float(first.get("opposite_direction_strength", 0.0))

        if reason == "TP_HIT":
            tp_pre_build_slope.append(pre_build_slope)
            tp_pre_align.append(pre_align)
            tp_opp.append(opp_strength)
        else:
            sl_pre_build_slope.append(pre_build_slope)
            sl_pre_align.append(pre_align)
            sl_opp.append(opp_strength)

    duration_hr = 1.0
    if len(ts_values) >= 2:
        duration_hr = max(1.0 / 60.0, (max(ts_values) - min(ts_values)) / 3600.0)

    pph = sum(static_pips) / duration_hr
    avg_r = _mean(static_r)
    win_rate = _mean([float(x) for x in win_flags])
    sl_rate = _mean([float(x) for x in sl_flags])

    sep_build = _effect_size(tp_pre_build_slope, sl_pre_build_slope)
    sep_align = _effect_size(tp_pre_align, sl_pre_align)
    sep_opp = _effect_size(tp_opp, sl_opp)
    separability = abs(sep_build) + abs(sep_align) + abs(sep_opp)

    quality_score = (
        pph * 0.35
        + avg_r * 1.6
        + win_rate * 1.2
        + separability * 0.35
        - sl_rate * 0.9
    )

    return {
        "node": _node_name_from_stream(path),
        "stream_path": str(path),
        "trade_count": len(trades),
        "window_duration_hr": duration_hr,
        "static_pips_per_hour": pph,
        "static_avg_r": avg_r,
        "static_win_rate": win_rate,
        "static_sl_rate": sl_rate,
        "separability": {
            "pre_build_slope_d": sep_build,
            "pre_macro_micro_alignment_d": sep_align,
            "opposite_direction_strength_d": sep_opp,
            "total": separability,
        },
        "quality_score": quality_score,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Screen node-level entry populations for quality/separability before AEE optimization.")
    ap.add_argument("--stream-glob", default="compiled_market_nodes/**/aee_stage/aee_state_stream/aee_state_stream.csv")
    ap.add_argument("--min-trades", type=int, default=120)
    ap.add_argument("--max-streams", type=int, default=0, help="Optional cap on number of streams to scan (0 = all).")
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--out", default="aee_entry_population_quality_screen.json")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    streams = sorted([p.resolve() for p in root.glob(args.stream_glob) if p.is_file()])
    if not streams:
        raise SystemExit("no streams found for pattern")
    if int(args.max_streams) > 0:
        streams = streams[: int(args.max_streams)]

    rows: list[dict[str, Any]] = []
    for sp in streams:
        item = _screen_stream(sp, int(args.min_trades))
        if item is not None:
            rows.append(item)

    if not rows:
        raise SystemExit("no streams met minimum trade count")

    ranked = sorted(rows, key=lambda r: r.get("quality_score", 0.0), reverse=True)
    top_rows = ranked[: max(1, int(args.top))]

    payload = {
        "generated_at": _iso_now(),
        "stream_glob": args.stream_glob,
        "min_trades": int(args.min_trades),
        "max_streams": int(args.max_streams),
        "total_screened": len(ranked),
        "top_count": len(top_rows),
        "top_nodes": top_rows,
    }

    out = Path(args.out)
    if not out.is_absolute():
        out = (root / out).resolve()
    out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    print(
        json.dumps(
            {
                "out": str(out),
                "total_screened": len(ranked),
                "top_node": top_rows[0]["node"] if top_rows else None,
                "top_quality_score": top_rows[0]["quality_score"] if top_rows else None,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
