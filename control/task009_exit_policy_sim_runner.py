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


def _subcluster(fam: str, first: dict[str, str], rows: list[dict[str, str]]) -> str:
    rel = _safe_float(first.get("release_quality", 0.0), 0.0)
    align = _safe_float(first.get("pre_macro_micro_alignment", 0.0), 0.0)
    p2 = _safe_float(next((r.get("progress_ratio") for r in rows if int(float(r.get("bar_index", 0))) >= 2), 0.0), 0.0)

    if fam == "EXPANSION_BREAKOUT":
        return "breakout_clean" if rel >= 0.12 else "breakout_weak_release"
    if fam == "PULLBACK_CONTINUATION":
        return "pullback_shallow" if p2 >= 0.12 else "pullback_deep"
    if fam == "RECLAIM_CONTINUATION":
        return "reclaim_clean" if align <= -0.02 else "reclaim_failed"
    if fam == "RANGE_ESCAPE":
        return "range_true_expansion" if rel >= 0.12 else "range_noise_escape"
    return "other_mixed"


def _family_row(
    family: str,
    trade_count: int,
    hours: float,
    entry_vals: list[float],
    realized_vals: list[float],
    exit_vals: dict[str, list[float]],
) -> dict[str, Any]:
    entry_only_pph = (sum(entry_vals) / hours) if hours > 0 else 0.0
    realized_pph = (sum(realized_vals) / hours) if hours > 0 else 0.0

    branch_rows = []
    for reason, vals in exit_vals.items():
        branch_sum = sum(vals)
        branch_count = len(vals)
        branch_pph = (branch_sum / hours) if hours > 0 else 0.0

        counterfactual_sum = sum(realized_vals) - branch_sum
        # Branch-removal counterfactual assumes those trades realize entry-only potential.
        if branch_count > 0:
            avg_entry = mean(entry_vals) if entry_vals else 0.0
            counterfactual_sum += avg_entry * branch_count
        without_branch_pph = (counterfactual_sum / hours) if hours > 0 else 0.0
        delta_pph = without_branch_pph - realized_pph

        branch_rows.append(
            {
                "exit_branch": reason,
                "exit_branch_count": branch_count,
                "exit_branch_pct": (branch_count / trade_count) if trade_count > 0 else 0.0,
                "exit_branch_net_contribution_pph": branch_pph,
                "realized_without_branch_pph": without_branch_pph,
                "branch_removal_delta_pph": delta_pph,
            }
        )

    branch_rows.sort(key=lambda x: x["branch_removal_delta_pph"], reverse=True)

    return {
        "family": family,
        "trade_count": trade_count,
        "trades_per_hour": (trade_count / hours) if hours > 0 else 0.0,
        "entry_only_avg_pips": mean(entry_vals) if entry_vals else 0.0,
        "realized_avg_pips": mean(realized_vals) if realized_vals else 0.0,
        "entry_only_net_pph": entry_only_pph,
        "realized_net_pph": realized_pph,
        "realized_minus_entry_only_pph": realized_pph - entry_only_pph,
        "exit_branches": branch_rows,
    }


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
        raise SystemExit("No EUR_USD streams found")

    family_trade_count: Counter = Counter()
    family_entry: dict[str, list[float]] = defaultdict(list)
    family_realized: dict[str, list[float]] = defaultdict(list)
    family_exits: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

    cluster_trade_count: Counter = Counter()
    cluster_entry: dict[tuple[str, str], list[float]] = defaultdict(list)
    cluster_realized: dict[tuple[str, str], list[float]] = defaultdict(list)
    cluster_exits: dict[tuple[str, str], dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))

    total_hours = 0.0
    for sp in streams:
        rows = _load_rows(sp)
        if not rows:
            continue
        _pair, _day, _session, _ctx = _context_from_stream(root, sp)
        # Use stream span as denominator for pph-like quantities.
        from run_aee_band_floor_baseline import _stream_duration_hours  # local import to avoid lint cross-order

        total_hours += _stream_duration_hours(rows)

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
            first = trows[0]
            cluster = _subcluster(fam, first, trows)

            aee = _eval_trade_baseline(
                trows,
                cfg,
                friction_per_trade_pips=FRICTION,
                economic_value_margin_mult=1.10,
                spread_fallback_pips=0.8,
            )

            gross = _safe_float(aee.get("gross_pips", aee.get("pips", 0.0)), 0.0)
            realized = gross - FRICTION
            reason = str(aee.get("reason", "UNKNOWN"))

            mfe = max((_safe_float(r.get("profit_now", 0.0), 0.0) for r in trows), default=0.0)
            entry_only = max(0.0, mfe - FRICTION)

            family_trade_count[fam] += 1
            family_entry[fam].append(entry_only)
            family_realized[fam].append(realized)
            family_exits[fam][reason].append(realized)

            key = (fam, cluster)
            cluster_trade_count[key] += 1
            cluster_entry[key].append(entry_only)
            cluster_realized[key].append(realized)
            cluster_exits[key][reason].append(realized)

    family_rows = []
    for fam in FAMILIES:
        n = family_trade_count[fam]
        if n == 0:
            continue
        # allocate hours proportionally for fair family pph comparisons
        fam_hours = total_hours * (n / max(1, sum(family_trade_count.values())))
        row = _family_row(fam, n, fam_hours, family_entry[fam], family_realized[fam], family_exits[fam])
        family_rows.append(row)

    family_rows.sort(key=lambda x: x["family"])

    subcluster_rows = []
    for (fam, cluster), n in cluster_trade_count.items():
        hours = total_hours * (n / max(1, sum(cluster_trade_count.values())))
        row = _family_row(fam, n, hours, cluster_entry[(fam, cluster)], cluster_realized[(fam, cluster)], cluster_exits[(fam, cluster)])
        row["subcluster"] = cluster
        subcluster_rows.append(row)

    subcluster_rows.sort(key=lambda x: (x["family"], x.get("subcluster", "")))

    # Global branch damage ranking by pph suppression magnitude.
    damage_agg: dict[str, dict[str, float]] = defaultdict(lambda: {"count": 0.0, "suppression_pph": 0.0})
    for fam_row in family_rows:
        for b in fam_row["exit_branches"]:
            name = b["exit_branch"]
            damage_agg[name]["count"] += b["exit_branch_count"]
            damage_agg[name]["suppression_pph"] += b["branch_removal_delta_pph"]

    damage_rank = [
        {
            "exit_branch": k,
            "total_count": int(v["count"]),
            "total_branch_removal_delta_pph": v["suppression_pph"],
        }
        for k, v in damage_agg.items()
    ]
    damage_rank.sort(key=lambda x: x["total_branch_removal_delta_pph"], reverse=True)

    recommendations = []
    for fam_row in family_rows:
        branches = fam_row["exit_branches"]
        primary = branches[0] if branches else None
        secondary = branches[1] if len(branches) > 1 else None
        protective = sorted(branches, key=lambda x: x["branch_removal_delta_pph"])[:2]
        recommendations.append(
            {
                "family": fam_row["family"],
                "trade_count": fam_row["trade_count"],
                "primary_destructive_branch": primary,
                "secondary_destructive_branch": secondary,
                "neutral_or_protective_branches": protective,
            }
        )

    family_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-009",
        "pair": "EUR_USD",
        "rows": family_rows,
    }
    subcluster_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-009",
        "pair": "EUR_USD",
        "rows": subcluster_rows,
    }
    damage_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-009",
        "pair": "EUR_USD",
        "rows": damage_rank,
    }
    recommendation_report = {
        "generated_at": _iso_now(),
        "task_id": "TASK-009",
        "pair": "EUR_USD",
        "rows": recommendations,
    }

    (control / "aee_family_exit_policy_simulation_report.json").write_text(json.dumps(family_report, indent=2) + "\n", encoding="utf-8")
    (control / "aee_subcluster_exit_policy_simulation_report.json").write_text(json.dumps(subcluster_report, indent=2) + "\n", encoding="utf-8")
    (control / "aee_exit_branch_damage_rank.json").write_text(json.dumps(damage_report, indent=2) + "\n", encoding="utf-8")
    (control / "aee_family_branch_recommendation.json").write_text(json.dumps(recommendation_report, indent=2) + "\n", encoding="utf-8")

    print("wrote control/aee_family_exit_policy_simulation_report.json")
    print("wrote control/aee_subcluster_exit_policy_simulation_report.json")
    print("wrote control/aee_exit_branch_damage_rank.json")
    print("wrote control/aee_family_branch_recommendation.json")


if __name__ == "__main__":
    main()
