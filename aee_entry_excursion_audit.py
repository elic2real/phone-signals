#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import mean


@dataclass
class TradeStats:
    pair: str
    day: str
    session: str
    trade_id: str
    target_distance: float
    final_pips: float
    mfe_pips: float
    mae_pips: float
    mfe_1bar_pips: float
    mfe_2bar_pips: float
    mfe_3bar_pips: float
    first_clear_bar: int


def _safe_float(v: str | float | int, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _parse_context_from_path(path: Path) -> tuple[str, str, str]:
    # expected: compiled_market_nodes/PAIR__day__session/aee_stage/aee_state_stream/aee_state_stream.csv
    node = path.parts[path.parts.index("compiled_market_nodes") + 1]
    bits = node.split("__")
    pair = bits[0] if len(bits) > 0 else "UNKNOWN"
    day = bits[1] if len(bits) > 1 else "unknown"
    session = bits[2] if len(bits) > 2 else "unknown"
    return pair, day, session


def _iter_streams(root: Path, globs: list[str], max_streams: int) -> list[Path]:
    paths: list[Path] = []
    for g in globs:
        paths.extend([p.resolve() for p in root.glob(g) if p.is_file()])
    paths = sorted(set(paths))
    return paths[: max(1, max_streams)]


def _load_trade_stats(stream_path: Path) -> list[TradeStats]:
    pair, day, session = _parse_context_from_path(stream_path)
    by_trade: dict[str, list[dict[str, str]]] = defaultdict(list)
    with stream_path.open(newline="", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            by_trade[str(row.get("trade_id", ""))].append(row)

    out: list[TradeStats] = []
    for trade_id, rows in by_trade.items():
        if not trade_id or not rows:
            continue
        rows.sort(key=lambda r: int(_safe_float(r.get("bar_index", 0), 0)))
        first = rows[0]
        last = rows[-1]
        td = max(0.1, _safe_float(first.get("target_distance", 1.0), 1.0))

        profits = [_safe_float(r.get("profit_now", 0.0), 0.0) for r in rows]
        bar_idx = [int(_safe_float(r.get("bar_index", i + 1), i + 1)) for i, r in enumerate(rows)]
        mfe = max(profits) if profits else 0.0
        mae = min(profits) if profits else 0.0
        final_pips = _safe_float(last.get("static_pips", profits[-1] if profits else 0.0), 0.0)

        mfe_1 = max((p for p, b in zip(profits, bar_idx) if b <= 1), default=(profits[0] if profits else 0.0))
        mfe_2 = max((p for p, b in zip(profits, bar_idx) if b <= 2), default=mfe_1)
        mfe_3 = max((p for p, b in zip(profits, bar_idx) if b <= 3), default=mfe_2)

        out.append(
            TradeStats(
                pair=pair,
                day=day,
                session=session,
                trade_id=trade_id,
                target_distance=td,
                final_pips=final_pips,
                mfe_pips=mfe,
                mae_pips=mae,
                mfe_1bar_pips=mfe_1,
                mfe_2bar_pips=mfe_2,
                mfe_3bar_pips=mfe_3,
                first_clear_bar=0,
            )
        )
    return out


def _pct(vals: list[bool]) -> float:
    return (sum(1 for v in vals if v) / len(vals)) if vals else 0.0


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit entry quality via post-entry excursion vs friction floor")
    ap.add_argument("--max-streams", type=int, default=48)
    ap.add_argument("--spread-pips", type=float, default=0.8)
    ap.add_argument("--slippage-pips-per-side", type=float, default=0.15)
    ap.add_argument("--commission-pips-roundtrip", type=float, default=0.0)
    ap.add_argument("--latency-penalty-pips", type=float, default=0.0)
    ap.add_argument("--viability-mult", type=float, default=1.10)
    ap.add_argument("--kill-min-trades", type=int, default=200)
    ap.add_argument("--kill-clearance-threshold", type=float, default=0.20)
    ap.add_argument("--out", default="aee_entry_excursion_audit_report.json")
    ap.add_argument("--kill-out", default="aee_entry_excursion_kill_contexts.json")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    globs = [
        "compiled_market_nodes/EUR_USD__*/aee_stage/aee_state_stream/aee_state_stream.csv",
        "compiled_market_nodes/EUR_CHF__*/aee_stage/aee_state_stream/aee_state_stream.csv",
        "compiled_market_nodes/USD_CAD__*/aee_stage/aee_state_stream/aee_state_stream.csv",
        "compiled_market_nodes/EUR_GBP__*/aee_stage/aee_state_stream/aee_state_stream.csv",
    ]

    streams = _iter_streams(root, globs, args.max_streams)
    if not streams:
        raise SystemExit("No streams found")

    friction_per_trade = (
        max(0.0, float(args.spread_pips))
        + (2.0 * max(0.0, float(args.slippage_pips_per_side)))
        + max(0.0, float(args.commission_pips_roundtrip))
        + max(0.0, float(args.latency_penalty_pips))
    )
    required_min_pips = friction_per_trade * max(1.0, float(args.viability_mult))

    trades: list[TradeStats] = []
    for s in streams:
        trades.extend(_load_trade_stats(s))

    by_context: dict[str, list[TradeStats]] = defaultdict(list)
    by_pair: dict[str, list[TradeStats]] = defaultdict(list)
    for t in trades:
        key = f"{t.pair}__{t.day}__{t.session}"
        by_context[key].append(t)
        by_pair[t.pair].append(t)

    def summarize(group: list[TradeStats]) -> dict:
        mfe_clear = [t.mfe_pips >= required_min_pips for t in group]
        final_clear = [t.final_pips >= required_min_pips for t in group]
        early1_clear = [t.mfe_1bar_pips >= required_min_pips for t in group]
        early2_clear = [t.mfe_2bar_pips >= required_min_pips for t in group]
        early3_clear = [t.mfe_3bar_pips >= required_min_pips for t in group]
        mfe_r = [t.mfe_pips / max(0.1, t.target_distance) for t in group]
        final_r = [t.final_pips / max(0.1, t.target_distance) for t in group]
        return {
            "trades": len(group),
            "avg_mfe_pips": mean([t.mfe_pips for t in group]) if group else 0.0,
            "avg_mfe_1bar_pips": mean([t.mfe_1bar_pips for t in group]) if group else 0.0,
            "avg_mfe_2bar_pips": mean([t.mfe_2bar_pips for t in group]) if group else 0.0,
            "avg_mfe_3bar_pips": mean([t.mfe_3bar_pips for t in group]) if group else 0.0,
            "avg_final_pips": mean([t.final_pips for t in group]) if group else 0.0,
            "avg_mfe_r": mean(mfe_r) if mfe_r else 0.0,
            "avg_final_r": mean(final_r) if final_r else 0.0,
            "mfe_clearance_rate": _pct(mfe_clear),
            "mfe_1bar_clearance_rate": _pct(early1_clear),
            "mfe_2bar_clearance_rate": _pct(early2_clear),
            "mfe_3bar_clearance_rate": _pct(early3_clear),
            "final_clearance_rate": _pct(final_clear),
        }

    pair_summary = {k: summarize(v) for k, v in sorted(by_pair.items())}
    context_summary = {k: summarize(v) for k, v in sorted(by_context.items())}

    kill_contexts = []
    for k, v in context_summary.items():
        if v["trades"] < int(args.kill_min_trades):
            continue
        if v["mfe_3bar_clearance_rate"] < float(args.kill_clearance_threshold):
            kill_contexts.append(
                {
                    "context": k,
                    "trades": v["trades"],
                    "mfe_3bar_clearance_rate": v["mfe_3bar_clearance_rate"],
                    "mfe_full_clearance_rate": v["mfe_clearance_rate"],
                    "avg_mfe_pips": v["avg_mfe_pips"],
                    "avg_mfe_3bar_pips": v["avg_mfe_3bar_pips"],
                    "required_min_pips": required_min_pips,
                }
            )
    kill_contexts.sort(key=lambda x: (x["mfe_3bar_clearance_rate"], -x["trades"]))

    report = {
        "name": "aee_entry_excursion_audit_report",
        "streams_used": len(streams),
        "trade_count": len(trades),
        "friction_per_trade_pips": friction_per_trade,
        "required_min_pips": required_min_pips,
        "overall": summarize(trades),
        "pair_summary": pair_summary,
        "context_summary": context_summary,
    }

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = root / out_path
    out_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    kill_out = Path(args.kill_out)
    if not kill_out.is_absolute():
        kill_out = root / kill_out
    kill_payload = {
        "name": "aee_entry_excursion_kill_contexts",
        "kill_min_trades": int(args.kill_min_trades),
        "kill_clearance_threshold": float(args.kill_clearance_threshold),
        "required_min_pips": required_min_pips,
        "count": len(kill_contexts),
        "contexts": kill_contexts,
    }
    kill_out.write_text(json.dumps(kill_payload, indent=2) + "\n", encoding="utf-8")

    print(json.dumps({
        "out": str(out_path),
        "kill_out": str(kill_out),
        "streams_used": len(streams),
        "trade_count": len(trades),
        "overall_mfe_clearance_rate": report["overall"]["mfe_clearance_rate"],
        "overall_mfe_1bar_clearance_rate": report["overall"]["mfe_1bar_clearance_rate"],
        "overall_mfe_2bar_clearance_rate": report["overall"]["mfe_2bar_clearance_rate"],
        "overall_mfe_3bar_clearance_rate": report["overall"]["mfe_3bar_clearance_rate"],
        "overall_avg_mfe_pips": report["overall"]["avg_mfe_pips"],
        "overall_avg_mfe_3bar_pips": report["overall"]["avg_mfe_3bar_pips"],
        "required_min_pips": required_min_pips,
        "kill_contexts": len(kill_contexts),
    }, indent=2))


if __name__ == "__main__":
    main()
