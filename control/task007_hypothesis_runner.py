#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
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


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _subcluster(fam: str, first: dict[str, str], rows: list[dict[str, str]]) -> str:
    progress2 = _safe_float(next((r.get("progress_ratio") for r in rows if _safe_int(r.get("bar_index"), 0) >= 2), 0.0), 0.0)
    progress3 = _safe_float(next((r.get("progress_ratio") for r in rows if _safe_int(r.get("bar_index"), 0) >= 3), 0.0), 0.0)
    compression = _safe_float(first.get("compression", 0.0), 0.0)
    release_q = _safe_float(first.get("release_quality", 0.0), 0.0)
    align = _safe_float(first.get("pre_macro_micro_alignment", 0.0), 0.0)

    if fam == "PULLBACK_CONTINUATION":
        return "pullback_shallow" if progress2 >= 0.12 else "pullback_deep"
    if fam == "RECLAIM_CONTINUATION":
        return "reclaim_clean" if align < -0.02 and progress3 > progress2 + 0.08 else "reclaim_failed"
    if fam == "RANGE_ESCAPE":
        return "range_true_expansion" if release_q >= 0.12 and compression <= 0.58 else "range_noise_escape"
    if fam == "EXPANSION_BREAKOUT":
        return "breakout_clean" if release_q >= 0.12 else "breakout_weak_release"
    return "other_mixed"


def _trace_family_from_event(ev: dict[str, Any]) -> str:
    setup = str(ev.get("setup", "")).upper()
    trigger_mode = str(ev.get("trigger_mode", "")).upper()
    reason = str(ev.get("reason", "")).lower()
    if "FAILED_BREAKOUT_FADE" in setup or "reclaim" in reason or trigger_mode == "RECLAIM":
        return "RECLAIM_CONTINUATION"
    if "INTENTIONAL_RUNNER" in setup or trigger_mode == "RESUME" or "pullback" in reason:
        return "PULLBACK_CONTINUATION"
    if "RANGE" in setup:
        return "RANGE_ESCAPE"
    if trigger_mode == "BREAK" or "COMPRESSION_EXPANSION" in setup or "VOL_REIGNITE" in setup:
        return "EXPANSION_BREAKOUT"
    return "OTHER"


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    cfg = json.loads((root / "entry_v36_firehose_all_families.json").read_text(encoding="utf-8"))

    streams = sorted({p.resolve() for p in root.glob("compiled_market_nodes/EUR_USD__*/aee_stage/aee_state_stream/aee_state_stream.csv") if p.is_file()})
    if not streams:
        raise SystemExit("No EUR_USD streams found")

    friction = 0.8 + (2.0 * 0.15)

    fam_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    context_fam: dict[tuple[str, str], list[float]] = defaultdict(list)
    session_fam: dict[tuple[str, str], list[float]] = defaultdict(list)
    subclusters: dict[tuple[str, str], list[float]] = defaultdict(list)
    total_hours = 0.0

    for sp in streams:
        rows = _load_rows(sp)
        if not rows:
            continue
        pair, day, session, context = _context_from_stream(root, sp)
        hours = _stream_duration_hours(rows)
        total_hours += hours

        by_trade: dict[str, list[dict[str, str]]] = defaultdict(list)
        for r in rows:
            by_trade[str(r.get("trade_id", ""))].append(r)

        for trade_id, trows in by_trade.items():
            trows.sort(key=lambda x: _safe_int(x.get("bar_index", 0), 0))
            if not trows:
                continue
            fam = _infer_trade_family(trows)
            if fam not in FAMILIES:
                fam = "OTHER"

            first = trows[0]
            aee = _eval_trade_baseline(
                trows,
                cfg,
                friction_per_trade_pips=friction,
                economic_value_margin_mult=1.10,
                spread_fallback_pips=0.8,
            )
            gross_pips = _safe_float(aee.get("gross_pips", aee.get("pips", 0.0)), 0.0)
            net_pips = gross_pips - friction
            mfe = max((_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows), default=0.0)
            mae = min((_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows), default=0.0)
            entry_only_potential = max(0.0, mfe - friction)
            exit_reason = str(aee.get("reason", "UNKNOWN"))

            trace = {
                "compression": _safe_float(first.get("compression", 0.0), 0.0),
                "release_quality": _safe_float(first.get("release_quality", 0.0), 0.0),
                "progress_bar2": _safe_float(next((r.get("progress_ratio") for r in trows if _safe_int(r.get("bar_index"), 0) >= 2), 0.0), 0.0),
                "progress_bar3": _safe_float(next((r.get("progress_ratio") for r in trows if _safe_int(r.get("bar_index"), 0) >= 3), 0.0), 0.0),
                "pre_alignment": _safe_float(first.get("pre_macro_micro_alignment", 0.0), 0.0),
            }

            rec = {
                "trade_id": str(trade_id),
                "family": fam,
                "session": session,
                "context": context,
                "entry_trace": trace,
                "aee_exit_reason": exit_reason,
                "mfe_pips": mfe,
                "mae_pips": mae,
                "entry_only_potential_pips": entry_only_potential,
                "aee_realized_net_pips": net_pips,
                "subcluster": _subcluster(fam, first, trows),
                "assignment_reason": f"infer_trade_family={fam}; compression={trace['compression']:.3f}; release={trace['release_quality']:.3f}",
            }
            fam_rows[fam].append(rec)
            context_fam[(fam, context)].append(net_pips)
            session_fam[(fam, session)].append(net_pips)
            subclusters[(fam, rec["subcluster"])].append(net_pips)

    # Accepted vs rejected from runtime traces (decision evidence).
    attempts: dict[str, Counter] = defaultdict(Counter)
    trace_files = sorted(root.glob("runs/**/trades.jsonl"))
    for fp in trace_files:
        try:
            with fp.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    s = line.strip()
                    if not s:
                        continue
                    try:
                        ev = json.loads(s)
                    except Exception:
                        continue
                    if str(ev.get("event", "")) != "TRADE_ATTEMPT":
                        continue
                    fam = _trace_family_from_event(ev)
                    decision = str(ev.get("decision", "UNKNOWN")).upper()
                    attempts[fam][decision] += 1
        except Exception:
            continue

    entry_vs_aee = {}
    context_breakdown = {}
    subcluster_report = {}
    hypothesis_audit = {"generated_at": _iso_now(), "pair": "EUR_USD", "families": {}}

    for fam in FAMILIES:
        rows = fam_rows.get(fam, [])
        n = len(rows)
        if n == 0:
            entry_vs_aee[fam] = {
                "trade_count": 0,
                "entry_only_potential_pph": 0.0,
                "aee_realized_net_pph": 0.0,
                "aee_realization_ratio": 0.0,
            }
            continue

        sum_entry_only = sum(r["entry_only_potential_pips"] for r in rows)
        sum_aee = sum(r["aee_realized_net_pips"] for r in rows)
        entry_only_pph = sum_entry_only / max(total_hours, 1e-9)
        aee_pph = sum_aee / max(total_hours, 1e-9)
        realization_ratio = (aee_pph / entry_only_pph) if entry_only_pph > 0 else 0.0

        exit_counts = Counter(r["aee_exit_reason"] for r in rows)
        accepted = int(attempts.get(fam, Counter()).get("ARM", 0))
        rejected = int(attempts.get(fam, Counter()).get("REJECT", 0))

        entry_vs_aee[fam] = {
            "trade_count": n,
            "entry_only_potential_pph": entry_only_pph,
            "aee_realized_net_pph": aee_pph,
            "aee_realization_ratio": realization_ratio,
            "accepted_vs_rejected": {
                "accepted": accepted,
                "rejected": rejected,
                "accept_rate": (accepted / (accepted + rejected)) if (accepted + rejected) > 0 else None,
            },
            "top_exit_reasons": [{"reason": k, "count": v} for k, v in exit_counts.most_common(6)],
            "sample_trades": rows[:12],
        }

        per_context = []
        for (ff, ctx), vals in context_fam.items():
            if ff != fam:
                continue
            per_context.append({
                "context": ctx,
                "trade_count": len(vals),
                "net_pph_proxy": sum(vals) / max(total_hours, 1e-9),
                "avg_net_pips": mean(vals) if vals else 0.0,
            })
        per_context.sort(key=lambda x: x["net_pph_proxy"], reverse=True)

        per_session = []
        for (ff, sess), vals in session_fam.items():
            if ff != fam:
                continue
            per_session.append({
                "session": sess,
                "trade_count": len(vals),
                "avg_net_pips": mean(vals) if vals else 0.0,
            })
        per_session.sort(key=lambda x: x["avg_net_pips"], reverse=True)

        context_breakdown[fam] = {
            "by_context": per_context,
            "by_session": per_session,
        }

        clusters = []
        for (ff, cluster), vals in subclusters.items():
            if ff != fam:
                continue
            clusters.append({
                "cluster": cluster,
                "trade_count": len(vals),
                "avg_net_pips": mean(vals) if vals else 0.0,
                "net_pph_proxy": sum(vals) / max(total_hours, 1e-9),
            })
        clusters.sort(key=lambda x: x["net_pph_proxy"], reverse=True)
        subcluster_report[fam] = {
            "clusters": clusters,
            "split_proposal": [c["cluster"] for c in clusters],
        }

        # Hypothesis scoring with evidence.
        mixed_sessions = sum(1 for x in per_session if x["avg_net_pips"] > 0) > 0 and sum(1 for x in per_session if x["avg_net_pips"] <= 0) > 0
        h1 = max(0.0, min(1.0, 0.2 + (0.4 if fam == "OTHER" else 0.0) + (0.2 if len(clusters) >= 2 else 0.0)))
        h2 = max(0.0, min(1.0, 0.2 + (0.3 if entry_only_pph <= 0 else 0.0) + (0.2 if fam in {"RANGE_ESCAPE", "PULLBACK_CONTINUATION"} else 0.0)))
        h3 = max(0.0, min(1.0, 0.1 + (0.7 if entry_only_pph > 0 and aee_pph < 0 else 0.0) + (0.1 if realization_ratio < 0.35 else 0.0)))
        h5 = max(0.0, min(1.0, 0.2 + (0.5 if mixed_sessions else 0.0)))
        h12 = max(0.0, min(1.0, 0.2 + (0.5 if len(clusters) >= 2 and abs(clusters[0]["avg_net_pips"] - clusters[-1]["avg_net_pips"]) > 0.4 else 0.0)))

        hypotheses = {
            "H1_detector_misclassification": {
                "score": h1,
                "evidence": [
                    f"cluster_count={len(clusters)}",
                    f"family={fam}",
                    "assignment_reason samples included",
                ],
            },
            "H2_missing_structural_layers": {
                "score": h2,
                "evidence": [
                    f"entry_only_potential_pph={entry_only_pph:.5f}",
                    f"aee_realized_net_pph={aee_pph:.5f}",
                ],
            },
            "H3_entry_aee_mismatch": {
                "score": h3,
                "evidence": [
                    f"aee_realization_ratio={realization_ratio:.4f}",
                    f"top_exit_reason={entry_vs_aee[fam]['top_exit_reasons'][0]['reason'] if entry_vs_aee[fam]['top_exit_reasons'] else 'NONE'}",
                ],
            },
            "H5_regime_context_dilution": {
                "score": h5,
                "evidence": [
                    f"mixed_session_sign={mixed_sessions}",
                    f"session_rows={len(per_session)}",
                ],
            },
            "H12_family_over_generalization": {
                "score": h12,
                "evidence": [
                    f"subcluster_count={len(clusters)}",
                    f"best_vs_worst_cluster_gap={abs(clusters[0]['avg_net_pips'] - clusters[-1]['avg_net_pips']) if len(clusters) >= 2 else 0.0:.4f}",
                ],
            },
        }
        ranked = sorted(([k, v["score"]] for k, v in hypotheses.items()), key=lambda x: x[1], reverse=True)
        hypothesis_audit["families"][fam] = {
            "hypothesis_scores": hypotheses,
            "ranked_hypotheses": [{"hypothesis": k, "score": s} for k, s in ranked],
            "trustworthiness": "STRUCTURALLY_TRUSTWORTHY" if aee_pph > 0 else "STRUCTURALLY_UNTRUSTWORTHY_OR_INCOMPLETE",
        }

    (root / "strategy_entry_vs_aee_comparison.json").write_text(json.dumps({"generated_at": _iso_now(), "pair": "EUR_USD", "families": entry_vs_aee}, indent=2) + "\n", encoding="utf-8")
    (root / "strategy_context_regime_breakdown.json").write_text(json.dumps({"generated_at": _iso_now(), "pair": "EUR_USD", "families": context_breakdown}, indent=2) + "\n", encoding="utf-8")
    (root / "strategy_family_subcluster_report.json").write_text(json.dumps({"generated_at": _iso_now(), "pair": "EUR_USD", "families": subcluster_report}, indent=2) + "\n", encoding="utf-8")
    (root / "strategy_hypothesis_audit.json").write_text(json.dumps(hypothesis_audit, indent=2) + "\n", encoding="utf-8")

    print("wrote strategy_hypothesis_audit.json")
    print("wrote strategy_entry_vs_aee_comparison.json")
    print("wrote strategy_context_regime_breakdown.json")
    print("wrote strategy_family_subcluster_report.json")


if __name__ == "__main__":
    main()
