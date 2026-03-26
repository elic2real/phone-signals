#!/usr/bin/env python3
from __future__ import annotations

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
    release_q = _safe_float(first.get("release_quality", 0.0), 0.0)
    align = _safe_float(first.get("pre_macro_micro_alignment", 0.0), 0.0)

    if fam == "PULLBACK_CONTINUATION":
        return "pullback_shallow" if progress2 >= 0.12 else "pullback_deep"
    if fam == "RECLAIM_CONTINUATION":
        return "reclaim_clean" if align < -0.02 and progress3 > progress2 + 0.08 else "reclaim_failed"
    if fam == "RANGE_ESCAPE":
        return "range_true_expansion" if release_q >= 0.12 else "range_noise_escape"
    if fam == "EXPANSION_BREAKOUT":
        return "breakout_clean" if release_q >= 0.12 else "breakout_weak_release"
    return "other_mixed"


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    cfg = json.loads((root / "entry_v36_firehose_all_families.json").read_text(encoding="utf-8"))

    streams = sorted({p.resolve() for p in root.glob("compiled_market_nodes/EUR_USD__*/aee_stage/aee_state_stream/aee_state_stream.csv") if p.is_file()})
    if not streams:
        raise SystemExit("No EUR_USD streams found")

    friction = 0.8 + (2.0 * 0.15)

    family_exit_counts: dict[str, Counter] = {f: Counter() for f in FAMILIES}
    family_trade_counts: Counter = Counter()
    family_entry_only: dict[str, list[float]] = defaultdict(list)
    family_realized: dict[str, list[float]] = defaultdict(list)

    cluster_exit_counts: dict[tuple[str, str], Counter] = defaultdict(Counter)
    cluster_entry_only: dict[tuple[str, str], list[float]] = defaultdict(list)
    cluster_realized: dict[tuple[str, str], list[float]] = defaultdict(list)

    for sp in streams:
        rows = _load_rows(sp)
        if not rows:
            continue
        _pair, _day, _session, _context = _context_from_stream(root, sp)

        by_trade: dict[str, list[dict[str, str]]] = defaultdict(list)
        for r in rows:
            by_trade[str(r.get("trade_id", ""))].append(r)

        for _, trows in by_trade.items():
            trows.sort(key=lambda x: _safe_int(x.get("bar_index", 0), 0))
            if not trows:
                continue

            fam = _infer_trade_family(trows)
            if fam not in FAMILIES:
                fam = "OTHER"

            first = trows[0]
            cluster = _subcluster(fam, first, trows)

            aee = _eval_trade_baseline(
                trows,
                cfg,
                friction_per_trade_pips=friction,
                economic_value_margin_mult=1.10,
                spread_fallback_pips=0.8,
            )

            gross_pips = _safe_float(aee.get("gross_pips", aee.get("pips", 0.0)), 0.0)
            net_pips = gross_pips - friction
            exit_reason = str(aee.get("reason", "UNKNOWN"))

            mfe = max((_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows), default=0.0)
            entry_only_potential = max(0.0, mfe - friction)

            family_trade_counts[fam] += 1
            family_exit_counts[fam][exit_reason] += 1
            family_entry_only[fam].append(entry_only_potential)
            family_realized[fam].append(net_pips)

            key = (fam, cluster)
            cluster_exit_counts[key][exit_reason] += 1
            cluster_entry_only[key].append(entry_only_potential)
            cluster_realized[key].append(net_pips)

    family_report = {}
    for fam in FAMILIES:
        n = int(family_trade_counts.get(fam, 0))
        exits = family_exit_counts.get(fam, Counter())
        top_exits = [{"reason": k, "count": int(v), "share": (v / n if n > 0 else 0.0)} for k, v in exits.most_common(8)]
        avg_entry = mean(family_entry_only[fam]) if family_entry_only[fam] else 0.0
        avg_realized = mean(family_realized[fam]) if family_realized[fam] else 0.0

        loss_channel = "AEE_EXIT_PATH" if avg_entry > 0.0 and avg_realized < 0.0 else ("ENTRY_STRUCTURE" if avg_entry <= 0.0 else "MIXED")
        family_report[fam] = {
            "trade_count": n,
            "avg_entry_only_potential_pips": avg_entry,
            "avg_realized_net_pips": avg_realized,
            "realization_gap_pips": avg_entry - avg_realized,
            "dominant_exit_reasons": top_exits,
            "inferred_loss_channel": loss_channel,
        }

    cluster_rows = []
    for (fam, cluster), exits in cluster_exit_counts.items():
        vals_e = cluster_entry_only[(fam, cluster)]
        vals_r = cluster_realized[(fam, cluster)]
        n = len(vals_r)
        top_exits = [{"reason": k, "count": int(v), "share": (v / n if n > 0 else 0.0)} for k, v in exits.most_common(6)]
        avg_entry = mean(vals_e) if vals_e else 0.0
        avg_realized = mean(vals_r) if vals_r else 0.0
        cluster_rows.append(
            {
                "family": fam,
                "subcluster": cluster,
                "trade_count": n,
                "avg_entry_only_potential_pips": avg_entry,
                "avg_realized_net_pips": avg_realized,
                "realization_gap_pips": avg_entry - avg_realized,
                "dominant_exit_reasons": top_exits,
            }
        )

    cluster_rows.sort(key=lambda x: (x["family"], -x["trade_count"]))

    attribution = {
        "generated_at": _iso_now(),
        "pair": "EUR_USD",
        "scope": "audit_only_no_tuning_no_routing_changes",
        "families": family_report,
        "notes": [
            "entry-only potential uses MFE minus friction proxy",
            "realized net uses baseline AEE evaluation path",
            "dominant exits identify where potential is lost",
        ],
    }

    matrix = {
        "generated_at": _iso_now(),
        "pair": "EUR_USD",
        "rows": cluster_rows,
    }

    (root / "control" / "aee_exit_path_attribution.json").write_text(json.dumps(attribution, indent=2) + "\n", encoding="utf-8")
    (root / "control" / "aee_exit_path_subcluster_matrix.json").write_text(json.dumps(matrix, indent=2) + "\n", encoding="utf-8")

    print("wrote control/aee_exit_path_attribution.json")
    print("wrote control/aee_exit_path_subcluster_matrix.json")


if __name__ == "__main__":
    main()
