#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import session_opportunity_map
import session_performance_check
import session_template


ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = ROOT / "compiled_market_nodes"
DEFAULT_TEMPLATE_ROOT = ROOT / "compiled_session_templates"
DEFAULT_MAJOR_PAIRS = [
    "AUD_USD",
    "EUR_USD",
    "GBP_USD",
    "NZD_USD",
    "USD_CAD",
    "USD_CHF",
    "USD_JPY",
]


def node_dir(output_root: Path, pair: str, weekday: str, session: str) -> Path:
    return output_root / f"{pair}__{weekday}__{session}"


def maybe_refresh_performance(node_root: Path, dataset_lock: Path) -> bool:
    entry_population_csv = node_root / "target_entry_no_timeouts" / "target_entry_population.csv"
    if not entry_population_csv.exists():
        return False
    session_performance_check.run(
        dataset_lock=dataset_lock,
        entry_population_csv=entry_population_csv,
        output_dir=node_root / "session_performance_check",
        trade_rows_json=(node_root / "aee_target_local_fixedpop" / "target_local_fixedpop_aee_trade_rows.json"),
        session_potential_json=(node_root / "session_potential" / "session_potential_report.json"),
        session_opportunity_map_json=(node_root / "session_opportunity_map" / "session_opportunity_map_report.json"),
        session_calibration_json=(node_root / "session_calibration" / "session_calibration_report.json"),
        symmetric_break_even=0.505,
        min_side_trades=25,
        min_side_trades_per_hour=0.20,
        min_opportunities=100,
    )
    return True


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    ap.add_argument("--template-root", type=Path, default=DEFAULT_TEMPLATE_ROOT)
    ap.add_argument("--pairs", nargs="*", default=DEFAULT_MAJOR_PAIRS)
    ap.add_argument("--weekdays", nargs="*", default=["thursday", "friday"])
    ap.add_argument("--sessions", nargs="*", default=["london", "new_york", "asia", "sydney"])
    ap.add_argument("--refresh-templates", action="store_true")
    args = ap.parse_args()

    results: list[dict[str, object]] = []
    touched_pair_sessions: set[tuple[str, str]] = set()
    for pair in args.pairs:
        for weekday in args.weekdays:
            for session in args.sessions:
                root = node_dir(args.output_root, pair, weekday, session)
                dataset_lock = ROOT / f"dataset_lock__{pair.lower()}__{weekday}__{session}__11.json"
                truth_csv = root / "target_entry_stage" / "target_contextual_v2" / "target_entry_truth_table.csv"
                if not dataset_lock.exists() or not truth_csv.exists():
                    results.append(
                        {
                            "pair": pair,
                            "weekday": weekday,
                            "session": session,
                            "status": "SKIP",
                            "reason": "missing_dataset_lock_or_truth",
                        }
                    )
                    continue
                report = session_opportunity_map.run(
                    dataset_lock=dataset_lock,
                    truth_csv=truth_csv,
                    output_dir=root / "session_opportunity_map",
                )
                maybe_refresh_performance(root, dataset_lock)
                touched_pair_sessions.add((pair, session))
                results.append(
                    {
                        "pair": pair,
                        "weekday": weekday,
                        "session": session,
                        "status": report.get("status", "UNKNOWN"),
                        "opportunities": report.get("pair_rollup", {}).get("total_opportunities", 0),
                    }
                )

    template_results: list[dict[str, object]] = []
    if args.refresh_templates:
        for pair, session in sorted(touched_pair_sessions):
            report = session_template.ensure_template(args.output_root, pair, session, args.template_root)
            template_results.append(
                {
                    "pair": pair,
                    "session": session,
                    "status": report.get("status", "UNKNOWN"),
                    "source_count": report.get("source_count", 0),
                }
            )

    print(
        json.dumps(
            {
                "status": "PASS",
                "results": results,
                "template_results": template_results,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
