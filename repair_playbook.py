#!/usr/bin/env python3
from __future__ import annotations

from typing import Any, Iterable


REPAIR_PLAYBOOK: dict[str, dict[str, Any]] = {
    "weak_edge_near_break_even": {
        "repair_route": "quality_repair",
        "verification_checks": [
            "effective_win_rate_on_all_selected_sides_gte_0_51",
        ],
        "fixes_that_worked": [
            "Treat 51% as a warning band, not a hard fail, and prioritize local quality improvement on the weak side.",
            "Tighten entries that are barely positive without collapsing supply.",
            "Use historical pair-session bias and trend state to push the side further away from break-even.",
        ],
    },
    "below_symmetric_break_even": {
        "repair_route": "quality_repair",
        "verification_checks": [
            "effective_win_rate_on_all_selected_sides_gte_0_51",
            "selected_population_nonempty",
        ],
        "fixes_that_worked": [
            "Borrow Monday/Tuesday/Wednesday same pair-session rule surfaces before local optimization.",
            "Switch no-timeout optimization to winrate_first for the failing node.",
            "Tighten the weak direction instead of broadening both sides equally.",
            "Rebuild contextual truth locally when borrowed rules produce stale or mismatched behavior.",
            "Re-run downstream calibration after no-timeout repair to verify effective win rate, not just entry win rate.",
        ],
    },
    "side_trade_count_too_low": {
        "repair_route": "supply_expand",
        "verification_checks": [
            "selected_count_per_active_side_gte_25",
            "best_class_trade_count_gt_5",
        ],
        "fixes_that_worked": [
            "Use cross-day same pair-session rule borrowing to widen the active state surface.",
            "Lower over-pruning by using expand_quality_entries instead of winrate_first.",
            "Reduce thin-zone sensitivity so small but valid quarter-direction-target pockets survive.",
            "Expand small/mid target ladders first before trying runner targets.",
        ],
    },
    "side_density_too_low": {
        "repair_route": "supply_expand",
        "verification_checks": [
            "trades_per_hour_per_active_side_gte_0_20",
            "best_class_trade_count_gt_5",
        ],
        "fixes_that_worked": [
            "Use expand_quality_entries to widen accepted zones without reverting to all-trades selection.",
            "Borrow same pair-session templates from earlier weekdays to restore quarter coverage quickly.",
            "Promote valid small-target classes from prior weekdays into Thursday/Friday before local tuning.",
        ],
    },
    "total_opportunity_count_too_low": {
        "repair_route": "supply_expand",
        "verification_checks": [
            "opportunity_count_gte_100",
            "selected_population_nonempty",
        ],
        "fixes_that_worked": [
            "Rebuild the node locally instead of relying on stale template-applied truth.",
            "Repair or replace borrowed rule surfaces when they collapse to near-zero selected rows.",
            "Use cross-day same pair-session baselines to rebuild missing state coverage.",
        ],
    },
    "pathological_total_opportunity_count": {
        "repair_route": "state_surface_rebuild",
        "verification_checks": [
            "opportunity_count_gt_15",
            "selected_population_nonempty",
        ],
        "fixes_that_worked": [
            "Treat extremely low opportunity count as a broken local state surface, not a mild supply miss.",
            "Force a local target-stage rebuild instead of relying on borrowed weekday surfaces.",
            "Rebuild small and mid targets first, then recheck opportunity supply before any deeper tuning.",
        ],
    },
    "missing_directional_coverage": {
        "repair_route": "state_surface_rebuild",
        "verification_checks": [
            "long_and_short_directional_coverage_present",
            "selected_population_nonempty",
        ],
        "fixes_that_worked": [
            "Rebuild the missing side from a valid earlier weekday in the same pair-session family.",
            "Force a local target-stage/state-surface rebuild when borrowed rules still leave one side empty.",
            "Check same-side overfit and underutilized expected direction before adding more of the dominant side.",
        ],
    },
    "pathological_best_class_trade_count": {
        "repair_route": "state_surface_rebuild",
        "verification_checks": [
            "best_class_trade_count_gt_1",
            "selected_population_nonempty",
        ],
        "fixes_that_worked": [
            "Treat the node as an invalid optimized surface, not a normal weak node.",
            "Rebuild target-entry contextual truth locally and discard borrowed rule surfaces that select 0/1 trades.",
            "Use supply_expand/state-surface repair instead of quality-only win-rate tuning.",
        ],
    },
    "ultra_thin_best_class_trade_count": {
        "repair_route": "state_surface_rebuild",
        "verification_checks": [
            "best_class_trade_count_gt_5",
            "selected_population_nonempty",
        ],
        "fixes_that_worked": [
            "Broaden from earlier same pair-session weekdays before local fine-tuning.",
            "Promote small-target classes first and only then rebuild larger targets.",
            "Do not accept the node as near-ceiling while the best class is ultra-thin.",
        ],
    },
    "directional_overfit": {
        "repair_route": "state_surface_rebuild",
        "verification_checks": [
            "weak_expected_direction_utilization_improved",
            "selected_count_ratio_below_overfit_threshold",
        ],
        "fixes_that_worked": [
            "Borrow the weaker side from a healthier earlier weekday in the same pair-session family.",
            "Force a local target-stage/state-surface rebuild if the borrowed baseline still collapses the weak side.",
            "Use expected opportunity/utilization metrics to rebuild the underused side instead of further optimizing the dominant side.",
            "Check quarter-relative bias and trend-state alignment for the weak side before expanding it.",
        ],
    },
    "underutilized_expected_direction": {
        "repair_route": "state_surface_rebuild",
        "verification_checks": [
            "expected_direction_trades_per_hour_gte_0_20",
            "expected_direction_utilization_gt_0_10",
        ],
        "fixes_that_worked": [
            "Use session_potential expected opportunities/hour to target the underused side directly.",
            "Discard borrowed surfaces that still suppress the expected side and rebuild locally from node truth.",
            "Rebuild small and mid targets on the underused side before runners.",
            "Use cross-day baseline rules as the starting point for the underused side.",
        ],
    },
    "INVALID_OPPORTUNITY_SANITY": {
        "repair_route": "truth_rebuild",
        "verification_checks": [
            "opportunity_sanity_valid",
            "truth_matches_dataset_lock_dates",
        ],
        "fixes_that_worked": [
            "Reject borrowed or stale truth artifacts whose dates/session_id do not match the dataset lock.",
            "Rebuild target-entry contextual truth locally from the node's own dataset lock.",
            "Repair contextual joins when quarter/session keys collapse valid truth rows to zero.",
        ],
    },
    "missing_aee_trade_rows": {
        "repair_route": "manual_review",
        "verification_checks": [
            "nonempty_aee_trade_rows_present",
            "aee_family_outputs_materialized",
        ],
        "fixes_that_worked": [
            "Run downstream-only closure for the node so AEE-managed trade rows are materialized.",
            "Reject production-ready status when trade-level AEE output is missing or empty.",
            "Compare static versus AEE only on nodes with concrete AEE trade rows.",
        ],
    },
}

ROUTE_PRIORITY = {
    "truth_rebuild": 4,
    "state_surface_rebuild": 3,
    "quality_repair": 2,
    "supply_expand": 1,
    "manual_review": 0,
}


def playbook_for(issue: str) -> dict[str, Any]:
    return REPAIR_PLAYBOOK.get(
        issue,
        {
            "repair_route": "manual_review",
            "verification_checks": [
                "manual_validation_required",
            ],
            "fixes_that_worked": [
                "No documented repair yet. Inspect node truth, state surface, and cross-day template fit."
            ],
        },
    )


def route_for_issues(issue_names: Iterable[str]) -> str:
    best_route = "manual_review"
    best_priority = ROUTE_PRIORITY[best_route]
    for issue_name in issue_names:
        route = str(playbook_for(str(issue_name)).get("repair_route", "manual_review"))
        priority = ROUTE_PRIORITY.get(route, 0)
        if priority > best_priority:
            best_route = route
            best_priority = priority
    return best_route


def verification_checks_for_issues(issue_names: Iterable[str]) -> list[str]:
    checks: list[str] = []
    seen: set[str] = set()
    for issue_name in issue_names:
        for check in playbook_for(str(issue_name)).get("verification_checks", []):
            check_name = str(check)
            if check_name not in seen:
                checks.append(check_name)
                seen.add(check_name)
    return checks
