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

from run_aee_band_floor_baseline import (  # noqa: E402
    _context_from_stream,
    _eval_trade_baseline,
    _infer_trade_family,
    _load_rows,
    _safe_float,
    _stream_duration_hours,
)

FAMILIES = [
    "EXPANSION_BREAKOUT",
    "RECLAIM_CONTINUATION",
    "PULLBACK_CONTINUATION",
    "RANGE_ESCAPE",
    "OTHER",
]

FRICTION = 1.1


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _branch_damage_pph(realized_vals: list[float], entry_vals: list[float], reasons: list[str], hold_vals: list[float], hours: float) -> list[dict[str, Any]]:
    by_idx: dict[str, list[int]] = defaultdict(list)
    for i, r in enumerate(reasons):
        by_idx[r].append(i)

    realized_pph = (sum(realized_vals) / hours) if hours > 0 else 0.0
    rows = []
    for reason, idxs in by_idx.items():
        rs = [realized_vals[i] for i in idxs]
        hs = [hold_vals[i] for i in idxs]
        branch_count = len(idxs)
        avg_entry = mean(entry_vals) if entry_vals else 0.0

        counterfactual = sum(realized_vals) - sum(rs) + (avg_entry * branch_count)
        without_pph = (counterfactual / hours) if hours > 0 else 0.0
        delta_pph = without_pph - realized_pph

        rows.append(
            {
                "exit_branch": reason,
                "count": branch_count,
                "share": branch_count / max(1, len(realized_vals)),
                "avg_hold_sec": mean(hs) if hs else 0.0,
                "branch_removal_delta_pph": delta_pph,
            }
        )

    rows.sort(key=lambda x: x["branch_removal_delta_pph"], reverse=True)
    return rows


def main() -> None:
    root = ROOT
    control = root / "control"

    cfg_v2 = json.loads((root / "entry_v36_firehose_all_families.json").read_text(encoding="utf-8"))
    cfg_v3 = json.loads((root / "control" / "entry_v36_firehose_all_families_v3.json").read_text(encoding="utf-8"))

    streams = sorted({
        p.resolve()
        for p in root.glob("compiled_market_nodes/EUR_USD__*/aee_stage/aee_state_stream/aee_state_stream.csv")
        if p.is_file()
    })

    total_hours = 0.0
    fam_entry: dict[str, list[float]] = defaultdict(list)
    fam_v2_real: dict[str, list[float]] = defaultdict(list)
    fam_v3_real: dict[str, list[float]] = defaultdict(list)
    fam_v2_reason: dict[str, list[str]] = defaultdict(list)
    fam_v3_reason: dict[str, list[str]] = defaultdict(list)
    fam_v2_hold: dict[str, list[float]] = defaultdict(list)
    fam_v3_hold: dict[str, list[float]] = defaultdict(list)

    for sp in streams:
        rows = _load_rows(sp)
        if not rows:
            continue
        total_hours += _stream_duration_hours(rows)
        _pair, _day, _session, _ctx = _context_from_stream(root, sp)

        by_trade: dict[str, list[dict[str, str]]] = defaultdict(list)
        for r in rows:
            by_trade[str(r.get("trade_id", ""))].append(r)

        for _, trows in by_trade.items():
            trows.sort(key=lambda x: int(float(x.get("bar_index", 0))))
            if not trows:
                continue

            fam = _infer_trade_family(trows)
            if fam not in FAMILIES:
                fam = "OTHER"

            mfe = max((_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows), default=0.0)
            entry_only = max(0.0, mfe - FRICTION)

            r2 = _eval_trade_baseline(trows, cfg_v2, friction_per_trade_pips=FRICTION, economic_value_margin_mult=1.10, spread_fallback_pips=0.8)
            r3 = _eval_trade_baseline(trows, cfg_v3, friction_per_trade_pips=FRICTION, economic_value_margin_mult=1.10, spread_fallback_pips=0.8)

            g2 = _safe_float(r2.get("gross_pips", r2.get("pips", 0.0)), 0.0) - FRICTION
            g3 = _safe_float(r3.get("gross_pips", r3.get("pips", 0.0)), 0.0) - FRICTION

            fam_entry[fam].append(entry_only)
            fam_v2_real[fam].append(g2)
            fam_v3_real[fam].append(g3)
            fam_v2_reason[fam].append(str(r2.get("reason", "UNKNOWN")))
            fam_v3_reason[fam].append(str(r3.get("reason", "UNKNOWN")))
            fam_v2_hold[fam].append(_safe_float(r2.get("hold_sec", 0.0), 0.0))
            fam_v3_hold[fam].append(_safe_float(r3.get("hold_sec", 0.0), 0.0))

    report_rows = []
    transition_fault_map = []
    breakout_transition_blame = []

    for fam in FAMILIES:
        n = len(fam_entry[fam])
        if n == 0:
            continue
        fam_hours = total_hours * (n / max(1, sum(len(fam_entry[x]) for x in FAMILIES)))
        entry_pph = sum(fam_entry[fam]) / fam_hours if fam_hours > 0 else 0.0
        v2_pph = sum(fam_v2_real[fam]) / fam_hours if fam_hours > 0 else 0.0
        v3_pph = sum(fam_v3_real[fam]) / fam_hours if fam_hours > 0 else 0.0

        v2_gap = (mean(fam_entry[fam]) - mean(fam_v2_real[fam])) if fam_v2_real[fam] else 0.0
        v3_gap = (mean(fam_entry[fam]) - mean(fam_v3_real[fam])) if fam_v3_real[fam] else 0.0

        c2 = Counter(fam_v2_reason[fam])
        c3 = Counter(fam_v3_reason[fam])

        hold_v2 = mean(fam_v2_hold[fam]) if fam_v2_hold[fam] else 0.0
        hold_v3 = mean(fam_v3_hold[fam]) if fam_v3_hold[fam] else 0.0

        branch_damage_v2 = _branch_damage_pph(fam_v2_real[fam], fam_entry[fam], fam_v2_reason[fam], fam_v2_hold[fam], fam_hours)
        branch_damage_v3 = _branch_damage_pph(fam_v3_real[fam], fam_entry[fam], fam_v3_reason[fam], fam_v3_hold[fam], fam_hours)

        report_rows.append(
            {
                "family": fam,
                "trade_count": n,
                "entry_only_pph": entry_pph,
                "v2_realized_pph": v2_pph,
                "v3_realized_pph": v3_pph,
                "delta_realized_pph": v3_pph - v2_pph,
                "v2_gap_pips": v2_gap,
                "v3_gap_pips": v3_gap,
                "delta_gap_pips": v3_gap - v2_gap,
                "v2_avg_hold_sec": hold_v2,
                "v3_avg_hold_sec": hold_v3,
                "delta_hold_sec": hold_v3 - hold_v2,
                "v2_exit_reason_distribution": {k: v / n for k, v in c2.items()},
                "v3_exit_reason_distribution": {k: v / n for k, v in c3.items()},
                "branch_damage_v2": branch_damage_v2[:8],
                "branch_damage_v3": branch_damage_v3[:8],
            }
        )

        for reason in sorted(set(c2) | set(c3)):
            transition_fault_map.append(
                {
                    "family": fam,
                    "reason": reason,
                    "v2_share": c2.get(reason, 0) / n,
                    "v3_share": c3.get(reason, 0) / n,
                    "delta_share": (c3.get(reason, 0) - c2.get(reason, 0)) / n,
                }
            )

        if fam == "EXPANSION_BREAKOUT":
            v2_map = {x["exit_branch"]: x for x in branch_damage_v2}
            v3_map = {x["exit_branch"]: x for x in branch_damage_v3}
            for reason in sorted(set(v2_map) | set(v3_map)):
                r2 = v2_map.get(reason, {"branch_removal_delta_pph": 0.0, "share": 0.0, "avg_hold_sec": 0.0})
                r3 = v3_map.get(reason, {"branch_removal_delta_pph": 0.0, "share": 0.0, "avg_hold_sec": 0.0})
                breakout_transition_blame.append(
                    {
                        "reason": reason,
                        "v2_share": r2["share"],
                        "v3_share": r3["share"],
                        "delta_share": r3["share"] - r2["share"],
                        "v2_branch_damage_delta_pph": r2["branch_removal_delta_pph"],
                        "v3_branch_damage_delta_pph": r3["branch_removal_delta_pph"],
                        "delta_branch_damage_delta_pph": r3["branch_removal_delta_pph"] - r2["branch_removal_delta_pph"],
                        "v2_avg_hold_sec": r2["avg_hold_sec"],
                        "v3_avg_hold_sec": r3["avg_hold_sec"],
                        "delta_avg_hold_sec": r3["avg_hold_sec"] - r2["avg_hold_sec"],
                    }
                )

    report_rows.sort(key=lambda x: x["family"])
    transition_fault_map.sort(key=lambda x: (x["family"], -abs(x["delta_share"])))
    breakout_transition_blame.sort(key=lambda x: x["delta_branch_damage_delta_pph"], reverse=True)

    diagnosis = {
        "generated_at": _iso_now(),
        "task": "AEE_V3_DELTA_FORENSIC_AUDIT",
        "pair": "EUR_USD",
        "family_rows": report_rows,
        "transition_fault_map": transition_fault_map,
        "breakout_transition_blame": breakout_transition_blame,
    }

    summary = {
        "generated_at": _iso_now(),
        "pair": "EUR_USD",
        "headline": {
            "breakout_delta_realized_pph": next((r["delta_realized_pph"] for r in report_rows if r["family"] == "EXPANSION_BREAKOUT"), 0.0),
            "breakout_delta_gap_pips": next((r["delta_gap_pips"] for r in report_rows if r["family"] == "EXPANSION_BREAKOUT"), 0.0),
            "families_gap_improved": [r["family"] for r in report_rows if r["delta_gap_pips"] < 0],
            "families_gap_worsened": [r["family"] for r in report_rows if r["delta_gap_pips"] > 0],
        },
        "primary_breakout_regression_reason": breakout_transition_blame[0] if breakout_transition_blame else None,
    }

    (control / "aee_v3_delta_forensic_audit.json").write_text(json.dumps(diagnosis, indent=2) + "\n", encoding="utf-8")
    (control / "aee_v3_transition_fault_summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")

    print("wrote control/aee_v3_delta_forensic_audit.json")
    print("wrote control/aee_v3_transition_fault_summary.json")


if __name__ == "__main__":
    main()
