#!/usr/bin/env python3
from __future__ import annotations

import json
import math
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

FRICTION = 0.8 + (2.0 * 0.15)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _regime(first: dict[str, str]) -> str:
    macro = _safe_float(first.get("macro_dir_score", 0.5), 0.5)
    comp = _safe_float(first.get("compression", 0.5), 0.5)
    if comp >= 0.55:
        return "compressed"
    if macro >= 0.6:
        return "strong_bullish"
    if macro <= 0.4:
        return "strong_bearish"
    return "neutral"


def _swing_transitions(profits: list[float], threshold: float = 0.05) -> list[str]:
    transitions: list[str] = []
    direction = 0
    for i in range(1, len(profits)):
        d = profits[i] - profits[i - 1]
        if abs(d) < threshold:
            continue
        new_dir = 1 if d > 0 else -1
        if direction == 0:
            direction = new_dir
            transitions.append("UP" if new_dir > 0 else "DOWN")
            continue
        if new_dir != direction:
            transitions.append("UP" if new_dir > 0 else "DOWN")
            direction = new_dir
    return transitions


def _reclaim_representable(profits: list[float]) -> bool:
    """Detect reclaim-like sequence from path only: down leg then recovery above entry region."""
    if len(profits) < 8:
        return False
    first = profits[0]
    window = profits[: min(20, len(profits))]
    trough = min(window)
    trough_idx = window.index(trough)
    if trough_idx < 2 or trough > first - 0.25:
        return False
    after = window[trough_idx + 1 :]
    if not after:
        return False
    rebound_peak = max(after)
    # Requires clear recovery and recapture of entry region.
    return rebound_peak >= max(first + 0.15, 0.0)


def _eff_band(eff: float) -> str:
    if eff < 0.10:
        return "broken"
    if eff < 0.30:
        return "weak"
    if eff < 0.60:
        return "viable"
    return "strong"


def main() -> None:
    root = ROOT
    control = root / "control"
    cfg = json.loads((root / "entry_v36_firehose_all_families.json").read_text(encoding="utf-8"))

    streams = sorted({
        p.resolve()
        for p in root.glob("compiled_market_nodes/EUR_USD__*/aee_stage/aee_state_stream/aee_state_stream.csv")
        if p.is_file()
    })
    if not streams:
        raise SystemExit("No EUR_USD state streams found")

    by_family = {
        fam: {
            "trades": 0,
            "available": [],
            "entry_only": [],
            "realized": [],
            "aee_damage": [],
            "eff": [],
            "reclaim_repr": 0,
            "transition_count": [],
            "suspicious_fill": 0,
            "bar1_negative": 0,
            "bar1_profit": [],
            "by_regime": defaultdict(list),
            "by_quarter": defaultdict(list),
        }
        for fam in FAMILIES
    }

    regime_counter: Counter = Counter()
    quarter_counter: Counter = Counter()
    total_trades = 0
    total_hours = 0.0

    for sp in streams:
        rows = _load_rows(sp)
        if not rows:
            continue
        total_hours += _stream_duration_hours(rows)

        by_trade: dict[str, list[dict[str, str]]] = defaultdict(list)
        for r in rows:
            by_trade[str(r.get("trade_id", ""))].append(r)

        for _, trows in by_trade.items():
            trows.sort(key=lambda x: _safe_int(x.get("bar_index", 0), 0))
            if not trows:
                continue
            total_trades += 1

            fam = _infer_trade_family(trows)
            if fam not in FAMILIES:
                fam = "OTHER"
            f = by_family[fam]

            first = trows[0]
            regime = _regime(first)
            quarter = str(first.get("quarter", "UNKNOWN"))
            regime_counter[regime] += 1
            quarter_counter[quarter] += 1

            profits = [_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows]
            mfe = max(profits) if profits else 0.0

            total_available = max(0.0, mfe)
            entry_only_capture = max(0.0, total_available - FRICTION)

            aee = _eval_trade_baseline(
                trows,
                cfg,
                friction_per_trade_pips=FRICTION,
                economic_value_margin_mult=1.10,
                spread_fallback_pips=0.8,
            )
            gross = _safe_float(aee.get("gross_pips", aee.get("pips", 0.0)), 0.0)
            realized = gross - FRICTION

            eff = realized / total_available if total_available > 1e-9 else 0.0
            damage = entry_only_capture - realized

            f["trades"] += 1
            f["available"].append(total_available)
            f["entry_only"].append(entry_only_capture)
            f["realized"].append(realized)
            f["aee_damage"].append(damage)
            f["eff"].append(eff)
            f["by_regime"][regime].append(realized)
            f["by_quarter"][quarter].append(realized)

            transitions = _swing_transitions(profits)
            f["transition_count"].append(len(transitions))
            if _reclaim_representable(profits):
                f["reclaim_repr"] += 1

            bar1 = profits[0] if profits else 0.0
            f["bar1_profit"].append(bar1)
            if bar1 > 2.0:
                f["suspicious_fill"] += 1
            if bar1 < 0.0:
                f["bar1_negative"] += 1

    if total_trades == 0:
        raise SystemExit("No trades found across streams")

    # Approximate family hours by trade share.
    family_hours = {
        fam: total_hours * (vals["trades"] / total_trades if total_trades > 0 else 0.0)
        for fam, vals in by_family.items()
    }

    truth_rows = []
    seq_rows = []
    pricing_rows = []
    balanced_rows = []

    for fam in FAMILIES:
        vals = by_family[fam]
        n = vals["trades"]
        if n == 0:
            continue

        available_avg = mean(vals["available"])
        entry_avg = mean(vals["entry_only"])
        realized_avg = mean(vals["realized"])
        eff_avg = mean(vals["eff"])
        damage_avg = mean(vals["aee_damage"])

        hrs = max(1e-9, family_hours.get(fam, 0.0))
        entry_pph = sum(vals["entry_only"]) / hrs
        realized_pph = sum(vals["realized"]) / hrs

        truth_rows.append(
            {
                "family": fam,
                "trade_count": n,
                "total_available_pips_avg": round(available_avg, 4),
                "entry_only_capture_avg": round(entry_avg, 4),
                "realized_capture_avg": round(realized_avg, 4),
                "entry_only_pph": round(entry_pph, 6),
                "realized_pph": round(realized_pph, 6),
                "aee_damage_avg": round(damage_avg, 4),
                "extraction_efficiency_avg": round(eff_avg, 4),
                "efficiency_band": _eff_band(eff_avg),
            }
        )

        reclaim_rate = vals["reclaim_repr"] / n
        seq_rows.append(
            {
                "family": fam,
                "trade_count": n,
                "avg_swing_transitions": round(mean(vals["transition_count"]) if vals["transition_count"] else 0.0, 3),
                "reclaim_representable_rate": round(reclaim_rate, 4),
            }
        )

        suspicious_rate = vals["suspicious_fill"] / n
        bar1_neg_rate = vals["bar1_negative"] / n
        pricing_rows.append(
            {
                "family": fam,
                "trade_count": n,
                "avg_bar1_profit_pips": round(mean(vals["bar1_profit"]) if vals["bar1_profit"] else 0.0, 4),
                "suspicious_fill_rate": round(suspicious_rate, 4),
                "spread_applied_bar1_rate": round(bar1_neg_rate, 4),
            }
        )

        # Balanced benchmark: equalized weighting across regime and quarter buckets.
        raw_realized = realized_avg
        regime_means = []
        for _, rv in vals["by_regime"].items():
            if rv:
                regime_means.append(mean(rv))
        quarter_means = []
        for _, qv in vals["by_quarter"].items():
            if qv:
                quarter_means.append(mean(qv))
        balanced_realized = 0.5 * (mean(regime_means) if regime_means else raw_realized) + 0.5 * (
            mean(quarter_means) if quarter_means else raw_realized
        )

        balanced_rows.append(
            {
                "family": fam,
                "trade_count": n,
                "raw_realized_avg_pips": round(raw_realized, 4),
                "balanced_realized_avg_pips": round(balanced_realized, 4),
                "balance_delta_pips": round(balanced_realized - raw_realized, 4),
            }
        )

    truth_rows.sort(key=lambda x: x["family"])
    seq_rows.sort(key=lambda x: x["family"])
    pricing_rows.sort(key=lambda x: x["family"])
    balanced_rows.sort(key=lambda x: x["family"])

    # Champion delta against TASK-011 global bias report.
    champion = json.loads((control / "simulation_global_bias_report.json").read_text(encoding="utf-8"))
    champion_fam = champion.get("metrics", {}).get("family_degradation_matrix", {})

    retest_rows = []
    for row in truth_rows:
        fam = row["family"]
        base = champion_fam.get(fam, {})
        retest_rows.append(
            {
                "family": fam,
                "baseline_realized_pph": _safe_float(base.get("realized_pph", 0.0), 0.0),
                "task014_realized_pph": row["realized_pph"],
                "delta_realized_pph": round(row["realized_pph"] - _safe_float(base.get("realized_pph", 0.0), 0.0), 6),
                "baseline_degradation_ratio": _safe_float(base.get("degradation_ratio", 0.0), 0.0),
                "task014_extraction_efficiency": row["extraction_efficiency_avg"],
            }
        )

    impl_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-014",
        "scope": "simulation_reconstruction_implementation",
        "streams": len(streams),
        "total_trades": total_trades,
        "total_hours": round(total_hours, 2),
        "implemented_modules": [
            "truth_anchor_metrics",
            "extraction_efficiency",
            "sequence_preservation_diagnostics",
            "pricing_realism_diagnostics",
            "balanced_benchmark_windows",
            "family_retest_vs_champion"
        ],
        "regime_distribution": {k: round(v / total_trades, 4) for k, v in regime_counter.items()},
        "quarter_distribution": {k: round(v / total_trades, 4) for k, v in quarter_counter.items()},
        "headline_findings": [
            "truth-anchor metrics are now explicit for every family",
            "extraction efficiency is measured and banded (broken/weak/viable/strong)",
            "reclaim representability now uses path-sequence diagnostics independent of progress_ratio monotonicity",
            "pricing realism now reports suspicious bar1 fill rates per family",
            "balanced benchmark outputs are produced alongside raw metrics"
        ]
    }

    truth_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-014",
        "rows": truth_rows,
    }

    seq_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-014",
        "rows": seq_rows,
        "global_reclaim_representable_rate": round(
            sum(by_family[f]["reclaim_repr"] for f in FAMILIES) / total_trades,
            4,
        ),
    }

    pricing_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-014",
        "rows": pricing_rows,
        "global_suspicious_fill_rate": round(
            sum(by_family[f]["suspicious_fill"] for f in FAMILIES) / total_trades,
            4,
        ),
    }

    balanced_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-014",
        "rows": balanced_rows,
        "method": "equalized mean over available regime and quarter slices per family",
    }

    retest_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-014",
        "rows": retest_rows,
        "focus_families": [
            "RECLAIM_CONTINUATION",
            "PULLBACK_CONTINUATION",
            "RANGE_ESCAPE",
            "EXPANSION_BREAKOUT"
        ],
    }

    (control / "simulation_reconstruction_implementation_report.json").write_text(
        json.dumps(impl_report, indent=2) + "\n", encoding="utf-8"
    )
    (control / "simulation_truth_anchor_metrics_report.json").write_text(
        json.dumps(truth_report, indent=2) + "\n", encoding="utf-8"
    )
    (control / "simulation_sequence_preservation_report.json").write_text(
        json.dumps(seq_report, indent=2) + "\n", encoding="utf-8"
    )
    (control / "simulation_pricing_realism_diagnostics_report.json").write_text(
        json.dumps(pricing_report, indent=2) + "\n", encoding="utf-8"
    )
    (control / "simulation_balanced_benchmark_report.json").write_text(
        json.dumps(balanced_report, indent=2) + "\n", encoding="utf-8"
    )
    (control / "simulation_retest_family_report.json").write_text(
        json.dumps(retest_report, indent=2) + "\n", encoding="utf-8"
    )

    print("wrote control/simulation_reconstruction_implementation_report.json")
    print("wrote control/simulation_truth_anchor_metrics_report.json")
    print("wrote control/simulation_sequence_preservation_report.json")
    print("wrote control/simulation_pricing_realism_diagnostics_report.json")
    print("wrote control/simulation_balanced_benchmark_report.json")
    print("wrote control/simulation_retest_family_report.json")


if __name__ == "__main__":
    main()
