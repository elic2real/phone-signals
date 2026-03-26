#!/usr/bin/env python3
"""TASK-011: Global Simulation Realism Audit.

Five audit sections (NO strategy/AEE/entry changes):
  A. Path Realism
  B. Fill Realism
  C. Sample Diversity
  D. Sequence Fidelity
  E. Global Bias Test (breakout vs multi-stage degradation)
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, stdev
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from run_aee_band_floor_baseline import (
    _context_from_stream,
    _eval_trade_baseline,
    _infer_trade_family,
    _load_rows,
    _safe_float,
    _safe_int,
    _stream_duration_hours,
)

FAMILIES = [
    "EXPANSION_BREAKOUT",
    "RECLAIM_CONTINUATION",
    "PULLBACK_CONTINUATION",
    "RANGE_ESCAPE",
    "OTHER",
]

FRICTION = 0.8 + (2.0 * 0.15)  # 1.10 pips


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _bucket_regime(macro: float, compression: float) -> str:
    if compression >= 0.55:
        return "compressed"
    if macro >= 0.6:
        return "strong_bullish"
    if macro <= 0.4:
        return "strong_bearish"
    return "neutral"


def _directional_changes(values: list[float], threshold: float = 0.05) -> int:
    """Count number of direction reversals in a value sequence."""
    if len(values) < 3:
        return 0
    changes = 0
    direction = 0  # 0=init, 1=up, -1=down
    for i in range(1, len(values)):
        delta = values[i] - values[i - 1]
        if abs(delta) < threshold:
            continue
        new_dir = 1 if delta > 0 else -1
        if direction != 0 and new_dir != direction:
            changes += 1
        direction = new_dir
    return changes


def _has_reversal_before_mfe(profits: list[float]) -> bool:
    """True if profit went negative (below -0.1) before MFE point."""
    if not profits:
        return False
    mfe_idx = profits.index(max(profits))
    pre_mfe = profits[:mfe_idx + 1]
    return any(p < -0.1 for p in pre_mfe)


def _has_multi_stage(profits: list[float]) -> bool:
    """True if profit crosses 0 at least twice suggesting multi-stage movement."""
    if len(profits) < 4:
        return False
    crossings = 0
    for i in range(1, len(profits)):
        if (profits[i - 1] < 0 and profits[i] >= 0) or (profits[i - 1] >= 0 and profits[i] < 0):
            crossings += 1
    return crossings >= 2


def _fill_consistency_score(profits: list[float], direction: str) -> dict[str, Any]:
    """
    Assess fill realism from bar-1 profit.
    Ideal: bar 1 profit_now should be near 0 (just entered, spread not yet paid in P&L)
    or slightly negative (spread applied). Large positive at bar 1 = backdated fill.
    """
    if not profits:
        return {"bar1_profit": None, "suspicious_fill": False, "fill_bias": 0.0}
    bar1 = profits[0]
    # A >2 pip gain on bar 1 is suspicious (would indicate unrealistic fill)
    suspicious = bar1 > 2.0
    return {
        "bar1_profit": round(bar1, 4),
        "suspicious_fill": suspicious,
        "fill_bias_pips": round(bar1, 4),
    }


def main() -> None:
    root = ROOT
    cfg = json.loads((root / "entry_v36_firehose_all_families.json").read_text(encoding="utf-8"))

    streams = sorted({
        p.resolve()
        for p in root.glob("compiled_market_nodes/EUR_USD__*/aee_stage/aee_state_stream/aee_state_stream.csv")
        if p.is_file()
    })
    if not streams:
        raise SystemExit("No EUR_USD streams found")

    # ─── accumulators ─────────────────────────────────────────────────────────
    total_hours = 0.0
    all_days: set[str] = set()
    all_sessions: set[str] = set()
    all_quarters: set[str] = set()
    regime_counter: Counter = Counter()
    session_counter: Counter = Counter()
    day_of_week_counter: Counter = Counter()

    # Path realism
    reversal_count = 0
    multi_stage_count = 0
    total_trades = 0
    path_complexity_scores: list[float] = []
    bars_per_trade: list[int] = []

    # Fill realism
    fill_biases: list[float] = []
    suspicious_fill_count = 0
    bar1_negative_count = 0  # trades where bar 1 shows immediate adverse P&L (spread applied correctly)
    bar1_neutral_count = 0  # bar 1 near 0

    # Sequence fidelity (per-family)
    family_total: Counter = Counter()
    family_pullback_representable: Counter = Counter()  # trades where early adverse observed
    family_reclaim_representable: Counter = Counter()   # trades with non-monotonic progress

    # Global bias (per-family)
    family_entry_only_pips: dict[str, list[float]] = defaultdict(list)
    family_realized_pips: dict[str, list[float]] = defaultdict(list)
    family_hours: dict[str, float] = defaultdict(float)
    family_trade_counts: Counter = Counter()

    # Segment coverage (unique trade segments vs total bars)
    segment_ids: set[str] = set()

    # ─── stream loop ──────────────────────────────────────────────────────────
    for sp in streams:
        rows = _load_rows(sp)
        if not rows:
            continue

        pair, day, session, context = _context_from_stream(root, sp)
        stream_hrs = _stream_duration_hours(rows)
        total_hours += stream_hrs
        all_sessions.add(session)

        # Group rows by trade_id
        by_trade: dict[str, list[dict[str, str]]] = defaultdict(list)
        for r in rows:
            tid = str(r.get("trade_id", ""))
            by_trade[tid].append(r)
            all_days.add(str(r.get("session_id", "")))
            all_quarters.add(str(r.get("quarter", "")))
            seg = str(r.get("segment_id", ""))
            if seg:
                segment_ids.add(seg)
            session_counter[str(r.get("session_id", "XXX")).split("-")[0][:3]] += 1  # dow stub

        for tid, trows in by_trade.items():
            trows.sort(key=lambda x: _safe_int(x.get("bar_index", 0), 0))
            if not trows:
                continue

            first = trows[0]
            fam = _infer_trade_family(trows)
            if fam not in FAMILIES:
                fam = "OTHER"

            profits = [_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows]
            bar_count = len(trows)
            bars_per_trade.append(bar_count)

            # Regime classification from first bar
            macro = _safe_float(first.get("macro_dir_score", 0.5), 0.5)
            compression = _safe_float(first.get("compression", 0.5), 0.5)
            regime = _bucket_regime(macro, compression)
            regime_counter[regime] += 1

            quarter = str(first.get("quarter", "UNKNOWN"))
            day_of_week_counter[quarter] += 1

            # ── A. Path realism ───────────────────────────────────────────────
            total_trades += 1
            if _has_reversal_before_mfe(profits):
                reversal_count += 1
            if _has_multi_stage(profits):
                multi_stage_count += 1
            changes = _directional_changes(profits)
            path_complexity_scores.append(float(changes))

            # ── B. Fill realism ───────────────────────────────────────────────
            fill_info = _fill_consistency_score(profits, str(first.get("direction", "LONG")))
            bar1_p = fill_info.get("bar1_profit")
            if bar1_p is not None:
                fill_biases.append(float(bar1_p))
                if fill_info.get("suspicious_fill", False):
                    suspicious_fill_count += 1
                if float(bar1_p) < 0.0:
                    bar1_negative_count += 1
                elif abs(float(bar1_p)) <= 0.2:
                    bar1_neutral_count += 1

            # ── D. Sequence fidelity ──────────────────────────────────────────
            family_total[fam] += 1

            # Pullback representable: early adverse observed (bar 2-5 profit < 0)
            early_profits = [_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows if 2 <= _safe_int(r.get("bar_index", 0), 0) <= 5]
            if any(p < -0.1 for p in early_profits):
                family_pullback_representable[fam] += 1

            # Reclaim representable: progress_ratio non-monotonic in first 8 bars
            prog_vals = [_safe_float(r.get("progress_ratio", 0.0), 0.0) for r in trows if _safe_int(r.get("bar_index", 0), 0) <= 8]
            if len(prog_vals) >= 3:
                dips = sum(1 for i in range(1, len(prog_vals)) if prog_vals[i] < prog_vals[i - 1] - 0.03)
                if dips >= 2:
                    family_reclaim_representable[fam] += 1

            # ── E. Global bias ────────────────────────────────────────────────
            aee = _eval_trade_baseline(
                trows, cfg,
                friction_per_trade_pips=FRICTION,
                economic_value_margin_mult=1.10,
                spread_fallback_pips=0.8,
            )
            gross_pips = _safe_float(aee.get("gross_pips", aee.get("pips", 0.0)), 0.0)
            realized_net = gross_pips - FRICTION

            mfe = max((_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows), default=0.0)
            entry_only = max(0.0, mfe - FRICTION)

            family_entry_only_pips[fam].append(entry_only)
            family_realized_pips[fam].append(realized_net)
            family_trade_counts[fam] += 1

        family_hours[session] += stream_hrs

    # ─── compute stream-level hours per family (approximate by trade share) ──
    fam_hours: dict[str, float] = {}
    for fam in FAMILIES:
        share = (family_trade_counts[fam] / max(1, total_trades))
        fam_hours[fam] = total_hours * share

    # ─── A. Path Realism Report ───────────────────────────────────────────────
    avg_complexity = mean(path_complexity_scores) if path_complexity_scores else 0.0
    reversal_rate = reversal_count / max(1, total_trades)
    multi_stage_rate = multi_stage_count / max(1, total_trades)
    avg_bars = mean(bars_per_trade) if bars_per_trade else 0.0
    path_complexity_score = min(1.0, avg_complexity / 4.0)  # normalised to 0-1

    path_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-011",
        "section": "A_PATH_REALISM",
        "scope": "audit_only_no_changes",
        "metrics": {
            "total_trades_analysed": total_trades,
            "total_hours": round(total_hours, 1),
            "avg_bars_per_trade": round(avg_bars, 2),
            "reversal_presence_rate": round(reversal_rate, 4),
            "multi_stage_presence_rate": round(multi_stage_rate, 4),
            "avg_directional_changes_per_trade": round(avg_complexity, 3),
            "path_complexity_score": round(path_complexity_score, 4),
        },
        "interpretation": {
            "reversal_presence_rate": "fraction of trades where price moved against before reaching MFE",
            "multi_stage_presence_rate": "fraction of trades where P&L crossed zero at least twice",
            "path_complexity_score": "normalised 0-1 from avg directional changes (4+ = 1.0)",
        },
        "finding": (
            "PATH_SUPPORTS_MULTI_STAGE" if reversal_rate > 0.30 else "PATH_BIASED_TOWARD_DIRECT_MOVES"
        ),
    }

    # ─── B. Fill Realism Report ───────────────────────────────────────────────
    avg_fill_bias = mean(fill_biases) if fill_biases else 0.0
    fill_bias_std = stdev(fill_biases) if len(fill_biases) > 1 else 0.0
    suspicious_rate = suspicious_fill_count / max(1, total_trades)
    spread_applied_rate = bar1_negative_count / max(1, total_trades)
    bar1_neutral_rate = bar1_neutral_count / max(1, total_trades)

    # fill_realism_score: 1.0 = perfectly realistic; lower if many suspicious fills or high bias
    fill_realism_score = max(0.0, 1.0 - (suspicious_rate * 2.0) - abs(avg_fill_bias) / 5.0)
    pricing_consistency_score = max(0.0, 1.0 - fill_bias_std / 3.0)

    fill_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-011",
        "section": "B_FILL_REALISM",
        "scope": "audit_only_no_changes",
        "metrics": {
            "total_trades": total_trades,
            "avg_bar1_profit_pips": round(avg_fill_bias, 4),
            "bar1_profit_stdev": round(fill_bias_std, 4),
            "suspicious_fill_rate": round(suspicious_rate, 4),
            "spread_applied_at_bar1_rate": round(spread_applied_rate, 4),
            "bar1_neutral_rate": round(bar1_neutral_rate, 4),
            "fill_realism_score": round(fill_realism_score, 4),
            "pricing_consistency_score": round(pricing_consistency_score, 4),
        },
        "interpretation": {
            "avg_bar1_profit_pips": ">0.5 suggests backdated fills or look-ahead bias; <0 confirms spread applied up-front",
            "suspicious_fill_rate": "rate of trades where bar-1 profit > 2 pips (unrealistic instantaneous gain)",
            "spread_applied_at_bar1_rate": "rate where bar-1 profit is negative — consistent with correct spread application",
        },
        "finding": (
            "FILL_REALISM_OK" if suspicious_rate < 0.05 else "FILL_REALISM_SUSPECT"
        ),
    }

    # ─── C. Sample Diversity Report ──────────────────────────────────────────
    total_regime_events = sum(regime_counter.values())
    regime_shares = {k: round(v / max(1, total_regime_events), 4) for k, v in regime_counter.most_common()}
    top_regime_share = max(regime_shares.values()) if regime_shares else 0.0
    regime_balance_score = max(0.0, 1.0 - (top_regime_share - 0.25) / 0.75)  # perfectly balanced = 0.25 each → 1.0

    total_q = sum(day_of_week_counter.values())
    q_shares = {k: round(v / max(1, total_q), 4) for k, v in day_of_week_counter.most_common()}
    top_q_share = max(q_shares.values()) if q_shares else 0.0
    session_imbalance_ratio = top_q_share

    diversity_score = 0.5 * regime_balance_score + 0.5 * (1.0 - max(0.0, (top_q_share - 0.25) / 0.75))

    diversity_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-011",
        "section": "C_SAMPLE_DIVERSITY",
        "scope": "audit_only_no_changes",
        "metrics": {
            "unique_streams": len(streams),
            "unique_days": len(all_days),
            "unique_sessions": len(all_sessions),
            "unique_quarters": len(all_quarters),
            "total_hours": round(total_hours, 1),
            "regime_distribution": regime_shares,
            "regime_concentration_ratio_top": round(top_regime_share, 4),
            "regime_balance_score": round(regime_balance_score, 4),
            "quarter_distribution": q_shares,
            "session_imbalance_ratio": round(session_imbalance_ratio, 4),
            "diversity_score": round(diversity_score, 4),
        },
        "interpretation": {
            "regime_balance_score": "1.0 = perfectly even across regimes; <0.5 = heavily concentrated",
            "diversity_score": "composite of regime and quarter balance",
        },
        "finding": (
            "SAMPLE_DIVERSE" if diversity_score >= 0.5 else "SAMPLE_REGIME_CONCENTRATED"
        ),
    }

    # ─── D. Sequence Fidelity Report ─────────────────────────────────────────
    fidelity_rows = []
    global_pullback_representable = 0
    global_reclaim_representable = 0
    for fam in FAMILIES:
        n = family_total[fam]
        pb_rate = family_pullback_representable[fam] / max(1, n)
        rc_rate = family_reclaim_representable[fam] / max(1, n)
        global_pullback_representable += family_pullback_representable[fam]
        global_reclaim_representable += family_reclaim_representable[fam]
        fidelity_rows.append({
            "family": fam,
            "trade_count": n,
            "pullback_pattern_representable_rate": round(pb_rate, 4),
            "reclaim_pattern_representable_rate": round(rc_rate, 4),
        })

    overall_pb_rate = global_pullback_representable / max(1, total_trades)
    overall_rc_rate = global_reclaim_representable / max(1, total_trades)
    sequence_resolution_score = min(1.0, avg_bars / 20.0)  # 20+ bars = full resolution
    sequence_fidelity_score = 0.5 * sequence_resolution_score + 0.25 * overall_pb_rate + 0.25 * overall_rc_rate

    seq_fidelity_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-011",
        "section": "D_SEQUENCE_FIDELITY",
        "scope": "audit_only_no_changes",
        "metrics": {
            "avg_bars_per_trade": round(avg_bars, 2),
            "sequence_resolution_score": round(sequence_resolution_score, 4),
            "overall_pullback_representable_rate": round(overall_pb_rate, 4),
            "overall_reclaim_representable_rate": round(overall_rc_rate, 4),
            "sequence_fidelity_score": round(sequence_fidelity_score, 4),
            "per_family": fidelity_rows,
        },
        "interpretation": {
            "pullback_pattern_representable_rate": "fraction of trades with early adverse P&L observable (bars 2-5), required for pullback strategy evaluation",
            "reclaim_pattern_representable_rate": "fraction of trades where progress_ratio shows non-monotonic dips for reclaim detection",
            "sequence_resolution_score": "normalised bar count; 20 bars = 1.0",
        },
        "finding": (
            "SEQUENCE_FIDELITY_ADEQUATE" if sequence_fidelity_score >= 0.40 else "SEQUENCE_FIDELITY_INSUFFICIENT"
        ),
    }

    # ─── E. Global Bias Report ────────────────────────────────────────────────
    family_degradation_matrix = {}
    all_degradation_ratios = []
    for fam in FAMILIES:
        e_vals = family_entry_only_pips[fam]
        r_vals = family_realized_pips[fam]
        n = len(r_vals)
        fam_hrs = fam_hours.get(fam, 1.0)

        avg_entry = mean(e_vals) if e_vals else 0.0
        avg_real = mean(r_vals) if r_vals else 0.0
        entry_pph = (sum(e_vals) / fam_hrs) if fam_hrs > 0 else 0.0
        realized_pph = (sum(r_vals) / fam_hrs) if fam_hrs > 0 else 0.0
        degradation_ratio = (avg_real / avg_entry) if avg_entry > 0.0 else 0.0
        gap_pips = avg_entry - avg_real

        family_degradation_matrix[fam] = {
            "trade_count": n,
            "avg_entry_only_potential_pips": round(avg_entry, 4),
            "avg_realized_net_pips": round(avg_real, 4),
            "entry_only_pph": round(entry_pph, 6),
            "realized_pph": round(realized_pph, 6),
            "degradation_ratio": round(degradation_ratio, 4),
            "realization_gap_pips": round(gap_pips, 4),
        }
        if fam != "EXPANSION_BREAKOUT":
            all_degradation_ratios.append(degradation_ratio)

    # breakout_bias_score: how much better breakout degrades vs all others
    bo = family_degradation_matrix.get("EXPANSION_BREAKOUT", {})
    bo_ratio = bo.get("degradation_ratio", 0.0)
    others_mean = mean(all_degradation_ratios) if all_degradation_ratios else 0.0
    breakout_bias_score = round(bo_ratio - others_mean, 4)  # >0 means breakout degrades less (is favoured)

    # Determine bias finding
    if breakout_bias_score > 0.10:
        bias_finding = "SIMULATOR_BIASED_TOWARD_BREAKOUT"
    elif breakout_bias_score < -0.10:
        bias_finding = "SIMULATOR_BIASED_AGAINST_BREAKOUT"
    else:
        bias_finding = "NO_SIGNIFICANT_BREAKOUT_BIAS_DETECTED"

    global_bias_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-011",
        "section": "E_GLOBAL_BIAS",
        "scope": "audit_only_no_changes",
        "metrics": {
            "total_hours": round(total_hours, 1),
            "total_trades": total_trades,
            "family_degradation_matrix": family_degradation_matrix,
            "breakout_degradation_ratio": bo_ratio,
            "non_breakout_avg_degradation_ratio": round(others_mean, 4),
            "breakout_bias_score": breakout_bias_score,
        },
        "interpretation": {
            "degradation_ratio": "realized_net / entry_only_potential; 1.0 = no degradation; 0.0 = all potential destroyed; <0 = AEE creates loss",
            "breakout_bias_score": "breakout_ratio - mean(other_ratios); >0.10 = simulator favours breakout; < -0.10 = simulator penalises breakout",
        },
        "finding": bias_finding,
    }

    # ─── write artifacts ──────────────────────────────────────────────────────
    ctrl = root / "control"
    (ctrl / "simulation_path_realism_report.json").write_text(json.dumps(path_report, indent=2) + "\n", encoding="utf-8")
    (ctrl / "simulation_fill_realism_report.json").write_text(json.dumps(fill_report, indent=2) + "\n", encoding="utf-8")
    (ctrl / "simulation_diversity_report.json").write_text(json.dumps(diversity_report, indent=2) + "\n", encoding="utf-8")
    (ctrl / "simulation_sequence_fidelity_report.json").write_text(json.dumps(seq_fidelity_report, indent=2) + "\n", encoding="utf-8")
    (ctrl / "simulation_global_bias_report.json").write_text(json.dumps(global_bias_report, indent=2) + "\n", encoding="utf-8")

    print("wrote control/simulation_path_realism_report.json")
    print("wrote control/simulation_fill_realism_report.json")
    print("wrote control/simulation_diversity_report.json")
    print("wrote control/simulation_sequence_fidelity_report.json")
    print("wrote control/simulation_global_bias_report.json")

    # ─── print summary ────────────────────────────────────────────────────────
    print("\n=== TASK-011 SIMULATION REALISM AUDIT SUMMARY ===")
    print(f"  Streams: {len(streams)} | Hours: {total_hours:.0f} | Trades: {total_trades}")
    print(f"\n  A. PATH REALISM")
    print(f"     reversal_presence_rate:  {reversal_rate:.3f}")
    print(f"     multi_stage_rate:        {multi_stage_rate:.3f}")
    print(f"     path_complexity_score:   {path_complexity_score:.4f}")
    print(f"     finding: {path_report['finding']}")
    print(f"\n  B. FILL REALISM")
    print(f"     avg_bar1_profit:         {avg_fill_bias:.4f} pips")
    print(f"     suspicious_fill_rate:    {suspicious_rate:.4f}")
    print(f"     fill_realism_score:      {fill_realism_score:.4f}")
    print(f"     finding: {fill_report['finding']}")
    print(f"\n  C. SAMPLE DIVERSITY")
    print(f"     unique_days:             {len(all_days)}")
    print(f"     regime_concentration:    {top_regime_share:.3f}")
    print(f"     diversity_score:         {diversity_score:.4f}")
    print(f"     finding: {diversity_report['finding']}")
    print(f"\n  D. SEQUENCE FIDELITY")
    print(f"     avg_bars_per_trade:      {avg_bars:.1f}")
    print(f"     pullback_representable:  {overall_pb_rate:.3f}")
    print(f"     reclaim_representable:   {overall_rc_rate:.3f}")
    print(f"     sequence_fidelity_score: {sequence_fidelity_score:.4f}")
    print(f"     finding: {seq_fidelity_report['finding']}")
    print(f"\n  E. GLOBAL BIAS TEST")
    for fam in FAMILIES:
        row = family_degradation_matrix[fam]
        print(f"     {fam:<30} degradation_ratio={row['degradation_ratio']:.4f}  realized_pph={row['realized_pph']:.5f}")
    print(f"     breakout_bias_score:     {breakout_bias_score:.4f}")
    print(f"     finding: {global_bias_report['finding']}")


if __name__ == "__main__":
    main()
