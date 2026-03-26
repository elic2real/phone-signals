#!/usr/bin/env python3
"""
Multi-Strategy Firehose Evaluator — v1 Discovery Mode

Runs ALL 4 families (EXPANSION_BREAKOUT, PULLBACK_CONTINUATION,
RECLAIM_CONTINUATION, RANGE_ESCAPE) with gates removed on EUR_USD.

Outputs:
  strategy_performance_report.json  — per-family ranked by net pph
  strategy_performance_by_context.json — per-family x context breakdown

Usage:
  python3 run_multi_strategy_firehose.py \
      --config entry_v36_firehose_all_families.json \
      --pair EUR_USD \
      --out strategy_performance_report.json
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from statistics import median

# Re-use all proven infrastructure from the baseline evaluator
sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_aee_band_floor_baseline import (
    _eval_trade_baseline,
    _eval_trade_static,
    _entry_filter_evaluate,
    _infer_trade_family,
    _load_rows,
    _stream_duration_hours,
    _context_from_stream,
    _safe_float,
    _safe_int,
)

def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_ts_seconds(raw: Any) -> float | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None


def _normalize_ts_utc(raw: Any) -> str:
    sec = _parse_ts_seconds(raw)
    if sec is None:
        return ""
    return datetime.fromtimestamp(sec, tz=timezone.utc).isoformat()


def _trade_lifecycle_metrics(trows: list[dict[str, Any]]) -> dict[str, Any]:
    if not trows:
        return {
            "entry_timestamp": "",
            "close_timestamp": "",
            "trade_life_seconds": 0.0,
            "time_to_first_profit_seconds": None,
            "time_in_drawdown_seconds": 0.0,
            "time_from_entry_to_close_seconds": 0.0,
            "time_from_peak_to_close_seconds": 0.0,
        }

    times: list[float] = []
    profits: list[float] = []
    for idx, row in enumerate(trows):
        ts = (
            _parse_ts_seconds(row.get("timestamp"))
            or _parse_ts_seconds(row.get("entry_time"))
        )
        if ts is None:
            ts = float(idx * 60)
        times.append(ts)
        profits.append(_safe_float(row.get("profit_now", 0.0), 0.0))

    entry_sec = times[0]
    close_sec = times[-1]
    trade_life = max(0.0, close_sec - entry_sec)

    first_profit_sec = None
    for ts, p in zip(times, profits):
        if p > 0.0:
            first_profit_sec = ts
            break

    time_in_drawdown = 0.0
    for i in range(len(times) - 1):
        if profits[i] < 0.0:
            dt = max(0.0, times[i + 1] - times[i])
            if dt <= 0.0:
                dt = 60.0
            time_in_drawdown += dt

    peak_idx = 0
    peak_val = profits[0]
    for i, p in enumerate(profits):
        if p > peak_val:
            peak_val = p
            peak_idx = i

    entry_ts_norm = _normalize_ts_utc(trows[0].get("timestamp") or trows[0].get("entry_time"))
    close_ts_norm = _normalize_ts_utc(trows[-1].get("timestamp") or trows[-1].get("entry_time"))

    return {
        "entry_timestamp": entry_ts_norm,
        "close_timestamp": close_ts_norm,
        "trade_life_seconds": trade_life,
        "time_to_first_profit_seconds": None if first_profit_sec is None else max(0.0, first_profit_sec - entry_sec),
        "time_in_drawdown_seconds": time_in_drawdown,
        "time_from_entry_to_close_seconds": trade_life,
        "time_from_peak_to_close_seconds": max(0.0, close_sec - times[peak_idx]),
    }


def _build_priority_telemetry(
    root: Path,
    pair: str,
    include_sessions: set[str],
    stream_days: set[str],
    selected_keys: set[tuple[str, str]],
    top_n: int = 5,
    max_cycles: int = 300,
) -> dict[str, Any]:
    parquet_path = (root / "global_true_candidate_stream.parquet").resolve()
    if not parquet_path.exists():
        return {
            "available": False,
            "reason": "global_true_candidate_stream.parquet missing",
        }

    try:
        import pandas as pd  # type: ignore
    except Exception as exc:
        return {
            "available": False,
            "reason": f"pandas unavailable: {exc}",
        }

    try:
        df = pd.read_parquet(
            parquet_path,
            columns=["timestamp", "direction_assumed", "node", "composite_score"],
        )
    except Exception as exc:
        return {
            "available": False,
            "reason": f"failed to read candidate stream: {exc}",
        }

    if df.empty:
        return {
            "available": False,
            "reason": "candidate stream is empty",
        }

    node = df["node"].astype(str)
    pair_mask = node.str.startswith(f"{pair}__", na=False)
    df = df[pair_mask]
    if df.empty:
        return {
            "available": False,
            "reason": "no candidate rows for pair",
        }

    if include_sessions:
        node = df["node"].astype(str)
        session_tokens = {f"__{s.lower()}" for s in include_sessions}
        sess_mask = node.str.lower().apply(lambda n: any(tok in n for tok in session_tokens))
        df = df[sess_mask]
    if df.empty:
        return {
            "available": False,
            "reason": "no candidate rows after session lock",
        }

    if stream_days:
        node = df["node"].astype(str)
        days = {d.lower() for d in stream_days}
        day_mask = node.str.lower().apply(lambda n: any(f"__{d}__" in n for d in days))
        df = df[day_mask]
    if df.empty:
        return {
            "available": False,
            "reason": "no candidate rows after stream-day filter",
        }

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["timestamp"])
    if df.empty:
        return {
            "available": False,
            "reason": "no valid timestamp rows after parsing",
        }

    df["timestamp_iso"] = df["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    df["direction"] = df["direction_assumed"].astype(str).str.upper()
    df["priority_score"] = df["composite_score"].astype(float)

    cycles: list[dict[str, Any]] = []
    selected_ranks: list[int] = []
    selected_count = 0

    grouped = df.groupby("timestamp_iso", sort=True)
    for ts_iso, g in grouped:
        g2 = g.sort_values("priority_score", ascending=False)
        rows: list[dict[str, Any]] = []
        selected_marked = False
        for idx, rec in enumerate(g2.itertuples(index=False), start=1):
            key = (ts_iso, str(rec.direction).upper())
            is_selected = (key in selected_keys) and (not selected_marked)
            if is_selected:
                selected_marked = True
                selected_count += 1
                selected_ranks.append(idx)
            rows.append(
                {
                    "priority_score": round(float(rec.priority_score), 6),
                    "rank": idx,
                    "selected": is_selected,
                    "direction": str(rec.direction),
                    "node": str(rec.node),
                }
            )

        top_rows = rows[: max(1, top_n)]
        cycles.append(
            {
                "timestamp": ts_iso,
                "candidate_count": len(rows),
                "top_ranked_candidates": top_rows,
                "selected_present_in_top_n": any(r["selected"] for r in top_rows),
            }
        )

        if len(cycles) >= max_cycles:
            break

    return {
        "available": True,
        "source": str(parquet_path),
        "cycle_count_evaluated": len(cycles),
        "top_n": top_n,
        "selected_count": selected_count,
        "avg_selected_rank": round((sum(selected_ranks) / len(selected_ranks)) if selected_ranks else 0.0, 4),
        "median_selected_rank": float(median(selected_ranks)) if selected_ranks else None,
        "cycles": cycles,
    }


def _build_family_stats(
    records: list[dict[str, Any]],
    total_hours: float,
    friction_per_trade: float,
) -> dict[str, Any]:
    """Aggregate per-family performance from raw trade records."""
    fam_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for rec in records:
        fam_groups[rec["family"]].append(rec)

    results = {}
    for fam, recs in fam_groups.items():
        net_pips_list = [r["net_pips"] for r in recs]
        gross_pips_list = [r["gross_pips"] for r in recs]
        n = len(recs)
        wins = sum(1 for p in net_pips_list if p > 0)
        total_net = sum(net_pips_list)
        total_gross = sum(gross_pips_list)
        avg_ppt = total_gross / n if n else 0.0
        avg_net_ppt = total_net / n if n else 0.0
        win_rate = wins / n if n else 0.0
        trades_per_hour = n / total_hours if total_hours > 0 else 0.0
        net_pph = total_net / total_hours if total_hours > 0 else 0.0
        gross_pph = total_gross / total_hours if total_hours > 0 else 0.0

        mfe_list = [r.get("mfe", 0.0) for r in recs]
        avg_mfe = sum(mfe_list) / len(mfe_list) if mfe_list else 0.0

        results[fam] = {
            "family": fam,
            "trade_count": n,
            "trades_per_hour": round(trades_per_hour, 4),
            "win_count": wins,
            "win_rate": round(win_rate, 4),
            "avg_gross_pips_per_trade": round(avg_ppt, 4),
            "avg_net_pips_per_trade": round(avg_net_ppt, 4),
            "avg_mfe_pips": round(avg_mfe, 4),
            "gross_pips_per_hour": round(gross_pph, 4),
            "net_pips_per_hour": round(net_pph, 4),
            "total_net_pips": round(total_net, 4),
            "total_gross_pips": round(total_gross, 4),
            "verdict": _verdict(net_pph),
        }
    return results


def _verdict(net_pph: float) -> str:
    if net_pph > 0.02:
        return "KEEP"
    if net_pph > -0.02:
        return "TUNE"
    return "KILL"


def _build_context_stats(
    records: list[dict[str, Any]],
    hours_by_context: dict[str, float],
) -> dict[str, Any]:
    groups: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for rec in records:
        key = f"{rec['family']}|{rec['context']}"
        groups[key]["net_pips"].append(rec["net_pips"])
        groups[key]["gross_pips"].append(rec["gross_pips"])
        groups[key]["family"].append(rec["family"])
        groups[key]["context"].append(rec["context"])

    results = {}
    for key, data in groups.items():
        fam = data["family"][0]
        ctx = data["context"][0]
        n = len(data["net_pips"])
        total_net = sum(data["net_pips"])
        total_gross = sum(data["gross_pips"])
        ctx_hours = hours_by_context.get(ctx, 1.0)
        results[key] = {
            "family": fam,
            "context": ctx,
            "trade_count": n,
            "win_rate": round(sum(1 for p in data["net_pips"] if p > 0) / n, 4),
            "avg_net_pips_per_trade": round(total_net / n, 4),
            "net_pips_per_hour": round(total_net / ctx_hours, 4),
        }
    return results


def _sorted_top(counter_like: dict[str, int], top_n: int = 8) -> list[dict[str, Any]]:
    rows = [{"name": k, "count": int(v)} for k, v in counter_like.items()]
    rows.sort(key=lambda x: (-x["count"], x["name"]))
    return rows[:top_n]


def _safe_round(v: float, n: int = 6) -> float:
    return round(float(v), n)


def _sample_by_bucket(records: list[dict[str, Any]], bucket: str, max_rows: int = 20) -> list[dict[str, Any]]:
    if not records:
        return []
    if bucket == "winners":
        rows = [r for r in records if r.get("net_pips", 0.0) > 0.0]
    elif bucket == "losers":
        rows = [r for r in records if r.get("net_pips", 0.0) < 0.0]
    else:
        rows = [r for r in records if abs(float(r.get("net_pips", 0.0))) <= 0.25]

    rows.sort(key=lambda r: (str(r.get("family", "")), str(r.get("trade_id", ""))))
    return rows[:max_rows]


def _classify_failure_layer(
    total_entry_only_pph: float,
    total_realized_pph: float,
    top_exit_branches: list[dict[str, Any]],
) -> dict[str, Any]:
    gap = total_entry_only_pph - total_realized_pph
    if gap > 0.03:
        primary = top_exit_branches[0]["name"] if top_exit_branches else "UNKNOWN"
        return {
            "failure_layer": "AEE",
            "confidence": 0.87,
            "reason": f"Entry-only edge remains above realized by {gap:.4f} pph with dominant exit branch {primary}",
        }
    if total_realized_pph <= 0.0:
        return {
            "failure_layer": "ENTRY",
            "confidence": 0.72,
            "reason": "Realized edge is non-positive and no strong AEE-shaped gap signal dominates",
        }
    return {
        "failure_layer": "MIXED",
        "confidence": 0.6,
        "reason": "No single layer dominates; inspect branch and family deltas",
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="entry_v36_firehose_all_families.json")
    ap.add_argument("--pair", default="EUR_USD", help="Pair to run (default EUR_USD)")
    ap.add_argument("--spread-pips", type=float, default=0.8)
    ap.add_argument("--slippage-pips-per-side", type=float, default=0.15)
    ap.add_argument("--commission-pips-roundtrip", type=float, default=0.0)
    ap.add_argument("--latency-penalty-pips", type=float, default=0.0)
    ap.add_argument("--economic-viability-mult", type=float, default=1.10)
    ap.add_argument("--out", default="strategy_performance_report.json")
    ap.add_argument("--context-out", default="strategy_performance_by_context.json")
    ap.add_argument("--telemetry-out", default="", help="Optional telemetry artifact path (json)")
    ap.add_argument("--telemetry-top-n", type=int, default=5, help="Top-N ranked candidates per cycle")
    ap.add_argument("--max-streams", type=int, default=999)
    ap.add_argument("--raw-core-mode", action="store_true", help="Hard-disable non-core filters and include all families")
    ap.add_argument("--run-id", default="", help="Optional run id; defaults to timestamp id")
    ap.add_argument("--champion-reference", default="strategy_performance_report_raw.json")
    ap.add_argument("--intervention-class", default="AEE_RUN")
    ap.add_argument("--strategy-form", default="entry_v36_firehose_all_families")
    ap.add_argument("--aee-version", default="AUTO_FROM_CONFIG")
    ap.add_argument("--simulation-mode", default="RAW_STREAM_REPLAY")
    ap.add_argument("--dataset-window-id", default="EUR_USD_RAW_CORE_FULL")
    ap.add_argument("--artifact-version", default="run_multi_strategy_firehose_v2")
    ap.add_argument("--code-version", default="workspace-local")
    ap.add_argument("--result-dir", default="control")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = (root / cfg_path).resolve()
    cfg = json.loads(cfg_path.read_text(encoding="utf-8"))

    entry_filters = cfg.get("entry_filters") or {}
    include_families = {
        str(x).upper().strip()
        for x in (entry_filters.get("include_entry_families") or [])
        if str(x).strip()
    }
    exclude_families = {
        str(x).upper().strip()
        for x in (entry_filters.get("exclude_entry_families") or [])
        if str(x).strip()
    }
    exclude_contexts_lc = {str(x).lower().strip() for x in entry_filters.get("exclude_contexts", [])}
    include_pairs = {
        str(x).upper().strip()
        for x in entry_filters.get("include_pairs", [])
        if str(x).strip()
    }
    include_sessions = {
        str(x).upper().strip()
        for x in entry_filters.get("include_sessions", [])
        if str(x).strip()
    }
    min_profit_now_pips_by_bar = list(entry_filters.get("min_profit_now_pips_by_bar", []))
    min_progress_ratio_by_bar = list(entry_filters.get("min_progress_ratio_by_bar", []))
    min_release_quality_by_bar = list(entry_filters.get("min_release_quality_by_bar", []))
    max_noise_by_bar = list(entry_filters.get("max_noise_by_bar", []))
    micro_confirm = dict(entry_filters.get("micro_confirm", {}))

    if args.raw_core_mode:
        # Raw core validation mode: no context/session/pair filtering, no quality overlays.
        include_families = {
            "EXPANSION_BREAKOUT",
            "RECLAIM_CONTINUATION",
            "PULLBACK_CONTINUATION",
            "RANGE_ESCAPE",
            "OTHER",
        }
        exclude_families = set()
        exclude_contexts_lc = set()
        include_pairs = set()
        include_sessions = set()
        min_profit_now_pips_by_bar = []
        min_progress_ratio_by_bar = []
        min_release_quality_by_bar = []
        max_noise_by_bar = []
        micro_confirm = {"enabled": False}

    friction_per_trade = (
        max(0.0, float(args.spread_pips))
        + (2.0 * max(0.0, float(args.slippage_pips_per_side)))
        + max(0.0, float(args.commission_pips_roundtrip))
        + max(0.0, float(args.latency_penalty_pips))
    )

    pair = args.pair.upper().replace("/", "_")
    stream_glob = f"compiled_market_nodes/{pair}__*/aee_stage/aee_state_stream/aee_state_stream.csv"
    streams = sorted({p.resolve() for p in root.glob(stream_glob) if p.is_file()})
    streams = streams[: max(1, args.max_streams)]

    if not streams:
        msg = f"No streams found for pair={pair} (glob: {stream_glob})"
        print(f"ERROR: {msg}", file=sys.stderr)
        raise SystemExit(1)

    print(f"Firehose — pair={pair}  streams={len(streams)}  config={cfg_path.name}")
    print(f"Raw core mode: {'ON' if args.raw_core_mode else 'OFF'}")
    print(f"Families: {sorted(include_families)}")
    print(f"Gates: micro_confirm={'enabled' if micro_confirm.get('enabled') else 'OFF'}  "
          f"displacement_rules={len(min_profit_now_pips_by_bar)}  "
          f"noise_rules={len(max_noise_by_bar)}")
    print()

    all_records: list[dict[str, Any]] = []
    total_hours = 0.0
    hours_by_context: dict[str, float] = {}
    skipped_entry = Counter()
    family_seen = Counter()
    sessions_seen = Counter()
    quarters_seen = Counter()
    unique_days: set[str] = set()
    unique_pairs: set[str] = set()
    selected_keys: set[tuple[str, str]] = set()

    for sp in streams:
        rows = _load_rows(sp)
        if not rows:
            continue
        pair_str, day, session, context = _context_from_stream(root, sp)
        context_lc = context.lower()
        unique_days.add(day)
        unique_pairs.add(pair_str)
        sessions_seen[session] += 1
        if day:
            quarter = f"Q{(((int(day[5:7]) - 1) // 3) + 1)}" if len(day) >= 7 and day[5:7].isdigit() else "UNKNOWN"
            quarters_seen[quarter] += 1
        stream_hours = _stream_duration_hours(rows)
        total_hours += stream_hours
        hours_by_context[context] = hours_by_context.get(context, 0.0) + stream_hours

        by_trade: dict[str, list] = {}
        for r in rows:
            tid = str(r.get("trade_id", ""))
            by_trade.setdefault(tid, []).append(r)

        for trade_id, trows in by_trade.items():
            trows.sort(key=lambda x: _safe_int(x.get("bar_index", 0), 0))
            if not trows:
                continue

            inferred_family = _infer_trade_family(trows)
            family_seen[inferred_family] += 1

            filter_eval = _entry_filter_evaluate(
                trows,
                pair_str,
                context_lc,
                include_families,
                exclude_families,
                exclude_contexts_lc,
                min_profit_now_pips_by_bar,
                min_progress_ratio_by_bar,
                min_release_quality_by_bar,
                max_noise_by_bar,
                micro_confirm,
                include_pairs=include_pairs,
                include_sessions=include_sessions,
                family_specific_filters=None,
                inferred_family=inferred_family,
            )
            if filter_eval.get("blocked"):
                skipped_entry[str(filter_eval.get("reason"))] += 1
                continue

            aee = _eval_trade_baseline(
                trows,
                cfg,
                friction_per_trade_pips=friction_per_trade,
                economic_value_margin_mult=float(args.economic_viability_mult),
                spread_fallback_pips=max(0.0, float(args.spread_pips)),
            )

            lifecycle = _trade_lifecycle_metrics(trows)
            direction = str(trows[0].get("direction", "")).upper().strip()
            entry_ts_norm = str(lifecycle.get("entry_timestamp", ""))
            if entry_ts_norm and direction:
                selected_keys.add((entry_ts_norm, direction))

            gross_pips = _safe_float(aee.get("gross_pips", aee.get("pips", 0.0)), 0.0)
            net_pips = gross_pips - friction_per_trade
            entry_only_pips = max(0.0, max((_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows), default=0.0) - friction_per_trade)

            # MFE: best profit seen during the trade
            mfe = max(
                (_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows),
                default=0.0,
            )

            all_records.append({
                "trade_id": trade_id,
                "pair": pair_str,
                "context": context,
                "day": day,
                "session": session,
                "family": inferred_family,
                "direction": direction,
                "gross_pips": gross_pips,
                "net_pips": net_pips,
                "mfe": mfe,
                "mae": min((_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows), default=0.0),
                "entry_only_pips": entry_only_pips,
                "realized_pips": net_pips,
                "hold_sec": _safe_float(aee.get("hold_sec", len(trows) * 60.0), len(trows) * 60.0),
                "exit_reason": str(aee.get("reason", "UNKNOWN")),
                "aee_branch": str(aee.get("reason", "UNKNOWN")),
                "state": str(aee.get("state", "UNKNOWN")),
                "entry_timestamp": lifecycle["entry_timestamp"],
                "close_timestamp": lifecycle["close_timestamp"],
                "trade_life_seconds": lifecycle["trade_life_seconds"],
                "time_to_first_profit_seconds": lifecycle["time_to_first_profit_seconds"],
                "time_in_drawdown_seconds": lifecycle["time_in_drawdown_seconds"],
                "time_from_entry_to_close_seconds": lifecycle["time_from_entry_to_close_seconds"],
                "time_from_peak_to_close_seconds": lifecycle["time_from_peak_to_close_seconds"],
                "logic_path": {
                    "detector": f"FAMILY::{inferred_family}",
                    "gating": "ENTRY_FILTER_EVALUATE",
                    "exit": str(aee.get("reason", "UNKNOWN")),
                },
            })

    if not all_records:
        print("No trades passed entry filter — check config or stream paths.")
        raise SystemExit(1)

    print(f"Streams processed: {len(streams)}")
    print(f"Total hours: {total_hours:.1f}")
    print(f"Total trades accepted: {len(all_records)}")
    print(f"Family distribution in data (before filter): {dict(family_seen.most_common())}")
    print(f"Skipped by filter: {dict(skipped_entry)}")
    print()

    family_stats = _build_family_stats(all_records, total_hours, friction_per_trade)
    context_stats = _build_context_stats(all_records, hours_by_context)

    # Rank families by net pph descending
    ranked = sorted(family_stats.values(), key=lambda x: -x["net_pips_per_hour"])

    print("=" * 65)
    print("STRATEGY PERFORMANCE REPORT — RANKED BY NET PPH")
    print("=" * 65)
    print(f"  {'Family':<26} {'Trades':>7} {'T/hr':>6} {'Avg ppt':>8} {'Win%':>6} {'Net pph':>9} {'Verdict':>7}")
    print("-" * 65)
    for s in ranked:
        bar = "█" * min(40, max(0, int(s["net_pips_per_hour"] * 400)))
        verdict_mark = {"KEEP": "✓✓", "TUNE": "~", "KILL": "✗"}[s["verdict"]]
        print(f"  {s['family']:<26} {s['trade_count']:>7,} {s['trades_per_hour']:>6.3f} "
              f"{s['avg_net_pips_per_trade']:>8.3f} {s['win_rate']:>5.1%} "
              f"{s['net_pips_per_hour']:>9.5f}  {verdict_mark}")
    print()

    # Total system if all KEEP+TUNE families combined
    keep_families = [s for s in ranked if s["verdict"] in ("KEEP", "TUNE")]
    combined_net_pph = sum(s["net_pips_per_hour"] for s in keep_families)
    combined_trades = sum(s["trade_count"] for s in keep_families)
    print(f"  Combined KEEP+TUNE net pph: {combined_net_pph:+.5f}")
    print(f"  Combined KEEP+TUNE trades:  {combined_trades:,}")
    print()
    print(f"  Baseline (EXPANSION_BREAKOUT only): ~0.04200 net pph (reference)")
    breakout = family_stats.get("EXPANSION_BREAKOUT", {})
    if breakout:
        print(f"  EXPANSION_BREAKOUT (this run): {breakout['net_pips_per_hour']:+.5f} net pph  "
              f"({breakout['trade_count']:,} trades)")
    print()

    # Write outputs
    life_vals = [float(r.get("trade_life_seconds", 0.0)) for r in all_records]
    close_vals = [float(r.get("net_pips", 0.0)) for r in all_records]
    mfe_vals = [float(r.get("mfe", 0.0)) for r in all_records]
    first_profit_vals = [float(r.get("time_to_first_profit_seconds", 0.0)) for r in all_records if r.get("time_to_first_profit_seconds") is not None]
    drawdown_vals = [float(r.get("time_in_drawdown_seconds", 0.0)) for r in all_records]
    peak_to_close_vals = [float(r.get("time_from_peak_to_close_seconds", 0.0)) for r in all_records]

    priority_telemetry = _build_priority_telemetry(
        root=root,
        pair=pair,
        include_sessions=include_sessions,
        stream_days=unique_days,
        selected_keys=selected_keys,
        top_n=max(1, int(args.telemetry_top_n)),
    )

    trade_lifecycle_summary = {
        "avg_trade_life_seconds": round((sum(life_vals) / len(life_vals)) if life_vals else 0.0, 4),
        "median_trade_life_seconds": float(median(life_vals)) if life_vals else 0.0,
        "avg_close_value_pips": round((sum(close_vals) / len(close_vals)) if close_vals else 0.0, 6),
        "avg_mfe_pips": round((sum(mfe_vals) / len(mfe_vals)) if mfe_vals else 0.0, 6),
        "avg_time_to_first_profit_seconds": round((sum(first_profit_vals) / len(first_profit_vals)) if first_profit_vals else 0.0, 4),
        "median_time_to_first_profit_seconds": float(median(first_profit_vals)) if first_profit_vals else None,
        "avg_time_in_drawdown_seconds": round((sum(drawdown_vals) / len(drawdown_vals)) if drawdown_vals else 0.0, 4),
        "median_time_in_drawdown_seconds": float(median(drawdown_vals)) if drawdown_vals else 0.0,
        "avg_time_from_peak_to_close_seconds": round((sum(peak_to_close_vals) / len(peak_to_close_vals)) if peak_to_close_vals else 0.0, 4),
        "median_time_from_peak_to_close_seconds": float(median(peak_to_close_vals)) if peak_to_close_vals else 0.0,
    }

    report = {
        "generated_at": _iso_now(),
        "config": str(cfg_path),
        "pair": pair,
        "total_streams": len(streams),
        "total_hours": round(total_hours, 2),
        "total_accepted_trades": len(all_records),
        "friction_per_trade_pips": friction_per_trade,
        "family_distribution_in_data": dict(family_seen),
        "skipped_by_entry_filter": dict(skipped_entry),
        "ranked_families": ranked,
        "combined_keep_tune_net_pph": round(combined_net_pph, 6),
        "trade_lifecycle_summary": trade_lifecycle_summary,
        "priority_telemetry_summary": {
            "available": bool(priority_telemetry.get("available")),
            "cycle_count_evaluated": int(priority_telemetry.get("cycle_count_evaluated", 0) or 0),
            "selected_count": int(priority_telemetry.get("selected_count", 0) or 0),
            "avg_selected_rank": priority_telemetry.get("avg_selected_rank"),
            "median_selected_rank": priority_telemetry.get("median_selected_rank"),
            "reason": priority_telemetry.get("reason", ""),
        },
    }
    out_path = root / args.out
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"  Wrote: {out_path.name}")

    ctx_path = root / args.context_out
    ctx_path.write_text(json.dumps({
        "generated_at": _iso_now(),
        "pair": pair,
        "family_x_context": context_stats,
    }, indent=2), encoding="utf-8")
    print(f"  Wrote: {ctx_path.name}")

    if args.telemetry_out:
        tele_path = Path(args.telemetry_out)
        if not tele_path.is_absolute():
            tele_path = (root / tele_path).resolve()
        tele_path.parent.mkdir(parents=True, exist_ok=True)
        telemetry_payload = {
            "generated_at": _iso_now(),
            "pair": pair,
            "scope": {
                "include_pairs": sorted(include_pairs),
                "include_sessions": sorted(include_sessions),
            },
            "priority_telemetry": priority_telemetry,
            "trade_lifecycle_summary": trade_lifecycle_summary,
            "trade_lifecycle_samples": [
                {
                    "trade_id": r.get("trade_id"),
                    "family": r.get("family"),
                    "exit_reason": r.get("exit_reason"),
                    "direction": r.get("direction"),
                    "entry_timestamp": r.get("entry_timestamp"),
                    "close_timestamp": r.get("close_timestamp"),
                    "trade_life_seconds": r.get("trade_life_seconds"),
                    "net_pips": r.get("net_pips"),
                    "mfe": r.get("mfe"),
                    "loss_after_peak_pips": max(0.0, float(r.get("mfe", 0.0)) - float(r.get("net_pips", 0.0))),
                    "outcome_label": (
                        "WIN"
                        if float(r.get("net_pips", 0.0)) > 0.0
                        else ("LOSS" if float(r.get("net_pips", 0.0)) < 0.0 else "FLAT")
                    ),
                    "time_to_first_profit_seconds": r.get("time_to_first_profit_seconds"),
                    "time_in_drawdown_seconds": r.get("time_in_drawdown_seconds"),
                    "time_from_entry_to_close_seconds": r.get("time_from_entry_to_close_seconds"),
                    "time_from_peak_to_close_seconds": r.get("time_from_peak_to_close_seconds"),
                }
                for r in all_records[:200]
            ],
        }
        tele_path.write_text(json.dumps(telemetry_payload, indent=2), encoding="utf-8")
        print(f"  Wrote: {tele_path.name}")

    # Full evidence pack artifacts.
    run_id = args.run_id.strip() or f"AEE_RUN_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    result_dir = (root / args.result_dir).resolve()
    result_dir.mkdir(parents=True, exist_ok=True)

    exit_reason_counts = Counter(str(r.get("exit_reason", "UNKNOWN")) for r in all_records)
    state_counts = Counter(str(r.get("state", "UNKNOWN")) for r in all_records)
    detector_counts = Counter(str(r.get("family", "UNKNOWN")) for r in all_records)

    total_entry_only = sum(float(r.get("entry_only_pips", 0.0)) for r in all_records)
    total_realized = sum(float(r.get("realized_pips", 0.0)) for r in all_records)
    total_entry_only_pph = total_entry_only / total_hours if total_hours > 0 else 0.0
    total_realized_pph = total_realized / total_hours if total_hours > 0 else 0.0
    gap_pph = total_entry_only_pph - total_realized_pph
    extraction_efficiency = (total_realized / total_entry_only) if total_entry_only > 0 else 0.0

    champion_path = Path(args.champion_reference)
    if not champion_path.is_absolute():
        champion_path = (root / champion_path).resolve()
    champion_payload = None
    if champion_path.exists():
        try:
            champion_payload = json.loads(champion_path.read_text(encoding="utf-8"))
        except Exception:
            champion_payload = None

    top_entry_branches = _sorted_top(dict(detector_counts), top_n=5)
    top_exit_branches = _sorted_top(dict(exit_reason_counts), top_n=8)

    failure_layer = _classify_failure_layer(total_entry_only_pph, total_realized_pph, top_exit_branches)

    data_coverage = {
        "pair_coverage": sorted(unique_pairs),
        "streams": len(streams),
        "hours": _safe_round(total_hours, 4),
        "unique_days": len(unique_days),
        "sessions_represented": dict(sessions_seen),
        "session_quarter_distribution": dict(quarters_seen),
        "contexts_represented": {k: _safe_round(v, 4) for k, v in sorted(hours_by_context.items())},
        "regime_distribution": {k: int(v) for k, v in family_seen.items()},
        "dominance_concentration": {
            "top_family": ranked[0]["family"] if ranked else "UNKNOWN",
            "top_family_trade_share": _safe_round((ranked[0]["trade_count"] / len(all_records)) if all_records and ranked else 0.0, 6),
        },
    }

    aee_mode_cfg = str((cfg.get("extraction") or {}).get("aee_version", cfg.get("aee_version", "v2"))).strip()
    aee_version = args.aee_version if args.aee_version != "AUTO_FROM_CONFIG" else aee_mode_cfg

    config_snapshot = {
        "run_id": run_id,
        "strategy_family": sorted(include_families),
        "strategy_form_id": args.strategy_form,
        "thresholds": {
            "spread_pips": float(args.spread_pips),
            "slippage_pips_per_side": float(args.slippage_pips_per_side),
            "commission_pips_roundtrip": float(args.commission_pips_roundtrip),
            "latency_penalty_pips": float(args.latency_penalty_pips),
            "economic_viability_mult": float(args.economic_viability_mult),
        },
        "enabled_gates": {
            "micro_confirm": bool(micro_confirm.get("enabled")),
            "displacement_rules": len(min_profit_now_pips_by_bar),
            "progress_rules": len(min_progress_ratio_by_bar),
            "release_quality_rules": len(min_release_quality_by_bar),
            "noise_rules": len(max_noise_by_bar),
        },
        "disabled_gates": {
            "raw_core_mode": bool(args.raw_core_mode),
            "excluded_contexts": sorted(exclude_contexts_lc),
        },
        "aee_version": aee_version,
        "simulation_mode": args.simulation_mode,
        "dataset_window_id": args.dataset_window_id,
        "pair_session_coverage": {
            "pairs": sorted(unique_pairs),
            "sessions": dict(sessions_seen),
        },
        "code_version": args.code_version,
        "artifact_version": args.artifact_version,
        "config_path": str(cfg_path),
    }

    logic_trace_summary = {
        "run_id": run_id,
        "detector_logic_path": top_entry_branches,
        "gating_path": {
            "top_block_reasons": _sorted_top(dict(skipped_entry), top_n=8),
            "filter_config": {
                "include_families": sorted(include_families),
                "exclude_families": sorted(exclude_families),
                "exclude_contexts": sorted(exclude_contexts_lc),
            },
        },
        "exit_logic_path": top_exit_branches,
        "state_transitions_used": _sorted_top(dict(state_counts), top_n=8),
        "overrides_triggered": [
            {
                "name": "raw_core_mode",
                "enabled": bool(args.raw_core_mode),
                "effect": "disables non-core entry gates when true",
            }
        ],
    }

    trade_samples = {
        "run_id": run_id,
        "winners": _sample_by_bucket(all_records, "winners", max_rows=20),
        "losers": _sample_by_bucket(all_records, "losers", max_rows=20),
        "ambiguous": _sample_by_bucket(all_records, "ambiguous", max_rows=20),
    }

    expected_vs_actual = {
        "run_id": run_id,
        "champion_reference": str(champion_path.name if champion_payload else "UNAVAILABLE"),
        "intervention_class": args.intervention_class,
        "expected_signature": {
            "realized_pph": "improve_or_hold",
            "entry_only_vs_realized_gap": "shrink_or_hold",
            "extraction_efficiency": "improve_or_hold",
        },
        "actual_signature": {
            "trade_count": len(all_records),
            "trades_per_hour": _safe_round(len(all_records) / total_hours if total_hours > 0 else 0.0, 6),
            "avg_pips_per_trade": _safe_round(total_realized / len(all_records) if all_records else 0.0, 6),
            "entry_only_pph": _safe_round(total_entry_only_pph, 6),
            "realized_pph": _safe_round(total_realized_pph, 6),
            "gap": _safe_round(gap_pph, 6),
            "extraction_efficiency": _safe_round(extraction_efficiency, 6),
        },
        "delta_vs_champion": {},
        "matches_expected_model": gap_pph <= 0.0 or total_realized_pph > 0.0,
    }

    if champion_payload and isinstance(champion_payload, dict):
        champ_breakout = 0.0
        for row in champion_payload.get("ranked_families", []):
            if str(row.get("family", "")) == "EXPANSION_BREAKOUT":
                champ_breakout = _safe_float(row.get("net_pips_per_hour", 0.0), 0.0)
                break
        run_breakout = 0.0
        for row in ranked:
            if str(row.get("family", "")) == "EXPANSION_BREAKOUT":
                run_breakout = _safe_float(row.get("net_pips_per_hour", 0.0), 0.0)
                break
        expected_vs_actual["delta_vs_champion"] = {
            "breakout_net_pph": _safe_round(run_breakout - champ_breakout, 6),
            "combined_keep_tune_net_pph": _safe_round(combined_net_pph - _safe_float(champion_payload.get("combined_keep_tune_net_pph", 0.0), 0.0), 6),
        }

    run_summary = {
        "run_id": run_id,
        "generated_at": _iso_now(),
        "champion_reference": str(champion_path.name if champion_payload else "UNAVAILABLE"),
        "intervention_class": args.intervention_class,
        "strategy_form": args.strategy_form,
        "aee_version": aee_version,
        "simulation_mode": args.simulation_mode,
        "data_coverage": data_coverage,
        "results": expected_vs_actual["actual_signature"],
        "top_logic_paths": {
            "entry": top_entry_branches,
            "exit": top_exit_branches,
        },
        "top_damage": {
            "primary_destructive_branch": top_exit_branches[0] if top_exit_branches else None,
            "secondary_destructive_branch": top_exit_branches[1] if len(top_exit_branches) > 1 else None,
        },
        "signature_check": {
            "expected": expected_vs_actual["expected_signature"],
            "actual": expected_vs_actual["actual_signature"],
            "delta_vs_champion": expected_vs_actual["delta_vs_champion"],
            "matches_expected_model": expected_vs_actual["matches_expected_model"],
        },
        "verdict": "PROMOTE" if total_realized_pph > 0 and expected_vs_actual["matches_expected_model"] else "REJECT",
    }

    (result_dir / "run_summary.json").write_text(json.dumps(run_summary, indent=2) + "\n", encoding="utf-8")
    (result_dir / "config_snapshot.json").write_text(json.dumps(config_snapshot, indent=2) + "\n", encoding="utf-8")
    (result_dir / "logic_trace_summary.json").write_text(json.dumps(logic_trace_summary, indent=2) + "\n", encoding="utf-8")
    (result_dir / "trade_evidence_sample.json").write_text(json.dumps(trade_samples, indent=2) + "\n", encoding="utf-8")
    (result_dir / "data_coverage_report.json").write_text(json.dumps(data_coverage, indent=2) + "\n", encoding="utf-8")
    (result_dir / "expected_vs_actual_signature.json").write_text(json.dumps(expected_vs_actual, indent=2) + "\n", encoding="utf-8")
    (result_dir / "failure_layer_classification.json").write_text(json.dumps(failure_layer, indent=2) + "\n", encoding="utf-8")

    print("\n" + "=" * 65)
    print("RESULT PACKAGE")
    print("=" * 65)
    print(f"RUN_ID: {run_id}")
    print(f"CHAMPION_REFERENCE: {run_summary['champion_reference']}")
    print(f"INTERVENTION_CLASS: {args.intervention_class}")
    print(f"STRATEGY_FORM: {args.strategy_form}")
    print(f"AEE_VERSION: {aee_version}")
    print(f"SIMULATION_MODE: {args.simulation_mode}")
    print("DATA_COVERAGE:")
    print(f"- pair(s): {', '.join(data_coverage['pair_coverage'])}")
    print(f"- sessions: {data_coverage['sessions_represented']}")
    print(f"- unique days: {data_coverage['unique_days']}")
    print(f"- streams: {data_coverage['streams']}")
    print(f"- hours: {data_coverage['hours']}")
    print("CONFIG:")
    print(f"- exact thresholds: {config_snapshot['thresholds']}")
    print(f"- enabled gates: {config_snapshot['enabled_gates']}")
    print(f"- disabled gates: {config_snapshot['disabled_gates']}")
    print("RESULTS:")
    print(f"- trade_count: {run_summary['results']['trade_count']}")
    print(f"- trades_per_hour: {run_summary['results']['trades_per_hour']}")
    print(f"- avg_pips_per_trade: {run_summary['results']['avg_pips_per_trade']}")
    print(f"- entry_only_pph: {run_summary['results']['entry_only_pph']}")
    print(f"- realized_pph: {run_summary['results']['realized_pph']}")
    print(f"- gap: {run_summary['results']['gap']}")
    print(f"- extraction_efficiency: {run_summary['results']['extraction_efficiency']}")
    print("TOP LOGIC PATHS:")
    print(f"- top entry branches: {top_entry_branches[:3]}")
    print(f"- top exit branches: {top_exit_branches[:3]}")
    print("TOP DAMAGE:")
    print(f"- primary destructive branch: {run_summary['top_damage']['primary_destructive_branch']}")
    print(f"- secondary destructive branch: {run_summary['top_damage']['secondary_destructive_branch']}")
    print("SIGNATURE CHECK:")
    print(f"- expected vs actual: {run_summary['signature_check']}")
    print(f"VERDICT: {run_summary['verdict']}")
    print()
    print(f"  Wrote: {(result_dir / 'run_summary.json').name}")
    print(f"  Wrote: {(result_dir / 'config_snapshot.json').name}")
    print(f"  Wrote: {(result_dir / 'logic_trace_summary.json').name}")
    print(f"  Wrote: {(result_dir / 'trade_evidence_sample.json').name}")
    print(f"  Wrote: {(result_dir / 'data_coverage_report.json').name}")
    print(f"  Wrote: {(result_dir / 'expected_vs_actual_signature.json').name}")
    print(f"  Wrote: {(result_dir / 'failure_layer_classification.json').name}")
    print()
    print("Decision rule:")
    print("  KEEP  (net pph > +0.020) — wire into live system")
    print("  TUNE  (net pph > -0.020) — potential edge, needs gating")
    print("  KILL  (net pph < -0.020) — discard")


if __name__ == "__main__":
    main()
