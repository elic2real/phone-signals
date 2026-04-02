from __future__ import annotations

import argparse
import json
import math
import os
import statistics
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

WORKSPACE = Path(__file__).resolve().parents[1]
CONTROL_DIR = WORKSPACE / "control" / "v2_engine"
BLITZ_DIR = CONTROL_DIR / "blitz"
PHASE2_DIR = CONTROL_DIR / "phase2"
PHASE4_DIR = CONTROL_DIR / "phase4"
PHASE5_DIR = CONTROL_DIR / "phase5"


def read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _safe_round(value: float) -> float:
    if math.isinf(value):
        return value
    return round(value, 6)


def _scenario_metrics(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    filled = [row for row in rows if str(row.get("status", "") or "").upper() == "FILLED"]
    pnl_rows = [float(row.get("pnl_pips", 0.0) or 0.0) for row in filled]
    gross_profit = sum(value for value in pnl_rows if value > 0.0)
    gross_loss = abs(sum(value for value in pnl_rows if value < 0.0))
    mean_pnl = statistics.fmean(pnl_rows) if pnl_rows else 0.0
    stdev = statistics.pstdev(pnl_rows) if len(pnl_rows) >= 2 else 0.0
    downside = [value for value in pnl_rows if value < 0.0]
    downside_stdev = statistics.pstdev(downside) if len(downside) >= 2 else 0.0
    sharpe = (mean_pnl / stdev) * math.sqrt(len(pnl_rows)) if pnl_rows and stdev > 0.0 else 0.0
    if pnl_rows and not downside and mean_pnl > 0.0:
        sortino = math.inf
    else:
        sortino = (mean_pnl / downside_stdev) * math.sqrt(len(pnl_rows)) if pnl_rows and downside_stdev > 0.0 else 0.0
    return {
        "trade_count": len(filled),
        "aborted_count": len(rows) - len(filled),
        "result_count": len(rows),
        "capture_rate": _safe_round(len(filled) / max(len(rows), 1)),
        "win_rate": _safe_round(sum(1 for value in pnl_rows if value > 0.0) / max(len(pnl_rows), 1)),
        "expectancy_pips": _safe_round(mean_pnl),
        "net_pnl_pips": _safe_round(sum(pnl_rows)),
        "profit_factor": math.inf if gross_loss == 0.0 and gross_profit > 0.0 else _safe_round(gross_profit / gross_loss) if gross_loss > 0.0 else 0.0,
        "sharpe": _safe_round(sharpe),
        "sortino": _safe_round(sortino),
        "gross_profit_pips": _safe_round(gross_profit),
        "gross_loss_pips": _safe_round(gross_loss),
    }


def _window_report(rows: List[Dict[str, Any]], scenarios: List[str]) -> Dict[str, Any]:
    scenario_set = {str(item) for item in scenarios}
    scoped = [row for row in rows if str(row.get("scenario", "") or "") in scenario_set]
    scenario_breakdown = []
    by_scenario: Dict[str, List[Dict[str, Any]]] = {}
    for row in scoped:
        by_scenario.setdefault(str(row.get("scenario", "") or ""), []).append(row)
    for scenario_name in scenarios:
        scenario_rows = by_scenario.get(scenario_name, [])
        scenario_breakdown.append(
            {
                "scenario": scenario_name,
                **_scenario_metrics(scenario_rows),
            }
        )
    return {
        "scenarios": scenarios,
        **_scenario_metrics(scoped),
        "scenario_breakdown": scenario_breakdown,
    }


def _correlation(values_a: List[float], values_b: List[float]) -> float:
    if len(values_a) != len(values_b) or len(values_a) < 2:
        return 0.0
    mean_a = statistics.fmean(values_a)
    mean_b = statistics.fmean(values_b)
    centered_a = [value - mean_a for value in values_a]
    centered_b = [value - mean_b for value in values_b]
    denom_a = math.sqrt(sum(value * value for value in centered_a))
    denom_b = math.sqrt(sum(value * value for value in centered_b))
    if denom_a == 0.0 or denom_b == 0.0:
        return 0.0
    return round(sum(a * b for a, b in zip(centered_a, centered_b)) / (denom_a * denom_b), 6)


def _baseline_reference(doctrine_id: str, baseline_report: Dict[str, Any]) -> Dict[str, Any]:
    row = next((item for item in baseline_report.get("strategies", []) if str(item.get("strategy_id", "") or "") == doctrine_id), {})
    if not row:
        return {
            "baseline_available": False,
            "source": "baseline_summary_report",
            "strategy_id": doctrine_id,
            "trade_count": 0,
            "aborted_count": 0,
            "result_count": 0,
            "capture_rate": 0.0,
            "win_rate": 0.0,
            "expectancy_pips": 0.0,
            "net_pnl_pips": 0.0,
            "sharpe": 0.0,
            "sortino": 0.0,
            "phase5_gate_passed": False,
            "scenarios": [],
        }
    return {
        "baseline_available": True,
        "source": "baseline_summary_report",
        "strategy_id": doctrine_id,
        "trade_count": int(row.get("trade_count", 0) or 0),
        "aborted_count": int(row.get("aborted_count", 0) or 0),
        "result_count": int(row.get("result_count", 0) or 0),
        "capture_rate": _safe_round(int(row.get("trade_count", 0) or 0) / max(int(row.get("result_count", 0) or 0), 1)),
        "win_rate": float(row.get("win_rate", 0.0) or 0.0),
        "expectancy_pips": float(row.get("expectancy_pips", 0.0) or 0.0),
        "net_pnl_pips": float(row.get("net_pnl_pips", 0.0) or 0.0),
        "sharpe": float(row.get("sharpe", 0.0) or 0.0),
        "sortino": float(row.get("sortino", 0.0) or 0.0),
        "phase5_gate_passed": bool(row.get("phase5_gate_passed")),
        "scenarios": row.get("scenarios", []),
    }


def _focused_phase5_report_path(doctrine_id: str) -> Path:
    return PHASE5_DIR / f"v2_phase5_evaluation_report_{str(doctrine_id).lower()}.json"


def _focused_phase5_rows_path(doctrine_id: str) -> Path:
    return PHASE5_DIR / f"phase5_evaluation_rows_{str(doctrine_id).lower()}.json"


def _focused_baseline_comparison_path(doctrine_id: str) -> Path:
    return PHASE5_DIR / f"v2_focus_baseline_comparison_{str(doctrine_id).lower()}.json"


def _default_branch_mutation(contract: Dict[str, Any]) -> Dict[str, Any]:
    contract_id = str(contract.get("contract_id", "") or "")
    if contract_id == "C1_FLOW_DRIFT_SHORT":
        return {
            "phase4_option_mutation": {
                "FLOW_DRIFT_SHORT": {
                    "route_modes": {
                        "DRIFT_REACCEL": {"ttl_scale_multiplier": 1.1},
                        "DRIFT_CONFIRM": {"ttl_scale_multiplier": 1.08},
                        "DRIFT_HARVEST": {"ttl_scale_multiplier": 1.05},
                    }
                }
            }
        }
    if contract_id == "C2_TRANSITION_RELEASE_SHORT_STANDARD":
        return {
            "route_selection": {
                "TRANSITION_RELEASE_SHORT_STANDARD": {
                    "mode_bias": {
                        "IGNITION_RELEASE_FAST": 0.4,
                        "RELEASE_CONFIRM": -0.1,
                        "RELEASE_EXTENSION": -0.4,
                    },
                    "variant_bias": {
                        "TRANSITION_FRONT_RUN": 0.5,
                        "CAPTURE_NEAR": 0.2,
                        "TRANSITION_CONFIRM": -0.1,
                    },
                }
            }
        }
    if contract_id == "C3_OSCILLATION_EDGE_LONG_SCALP":
        return {
            "phase2_survivor_override": {
                "OSCILLATION_EDGE_LONG_SCALP": {
                    "allow_relaxed_fragile_scalp_lane": True,
                    "minimum_cluster_size": 12,
                    "minimum_trade_count": 3,
                    "minimum_selected_expression_count": 1,
                    "minimum_positive_route_count": 1,
                    "minimum_positive_route_trade_count": 3,
                    "minimum_positive_route_expectancy_pips": 0.0,
                }
            },
            "regime_filter": {
                "OSCILLATION_EDGE_LONG_SCALP": {
                    "exclude_regime_states": ["EXPANSION"],
                    "max_volatility_percentile": 0.67,
                    "max_anchor_velocity_pips_per_sec": 0.35,
                }
            },
        }
    return {}


def _doctrine_phase2_snapshot(doctrine_id: str, phase2_report: Dict[str, Any]) -> Dict[str, Any]:
    row = next((item for item in phase2_report.get("clusters", []) if str(item.get("doctrine_id", "") or "") == doctrine_id), None)
    if not row:
        return {
            "found": False,
            "doctrine_id": doctrine_id,
            "tier1_survivor": False,
            "doctrine_runtime_status": "UNKNOWN",
        }
    return {
        "found": True,
        "doctrine_id": doctrine_id,
        "tier1_survivor": bool(row.get("tier1_survivor")),
        "doctrine_runtime_status": str(row.get("doctrine_runtime_status", "") or ""),
        "cluster_size": int(row.get("cluster_size", 0) or 0),
        "tier1_extraction_summary": dict(row.get("tier1_extraction_summary", {}) or {}),
        "tier1_selected_expression_ids": list(row.get("tier1_selected_expression_ids", []) or []),
        "doctrine_operating_tier": str(row.get("doctrine_operating_tier", "") or ""),
        "doctrine_runtime_contract": dict(row.get("doctrine_runtime_contract", {}) or {}),
    }


def _metric_contract_pass(current: float, baseline: float, gain_min: float) -> bool:
    if math.isinf(current):
        return True
    if math.isinf(baseline):
        return math.isinf(current)
    if baseline <= 0.0:
        return current > 0.0
    return current >= baseline * (1.0 + gain_min)


def _contract_decision(
    contract: Dict[str, Any],
    baseline_reference: Dict[str, Any],
    is_report: Dict[str, Any],
    oos_report: Dict[str, Any],
    phase2_snapshot: Dict[str, Any] | None,
) -> Dict[str, Any]:
    doctrine_id = str(contract["doctrine_id"])
    success = contract.get("success_contract", {})
    deadline_utc = (datetime.now(UTC) + timedelta(hours=48)).isoformat().replace("+00:00", "Z")

    if doctrine_id == "OSCILLATION_EDGE_LONG_SCALP":
        if not bool((phase2_snapshot or {}).get("tier1_survivor")):
            return {
                "contract_id": contract["contract_id"],
                "doctrine_id": doctrine_id,
                "status": "BLOCKED_BY_PHASE2_ADMISSION",
                "deadline_utc": deadline_utc,
                "failure_reason": "Doctrine is still failing runtime admission after the branch mutation.",
                "fail_action": contract["fail_action"],
                "success_contract": success,
            }
        sharpe_ok = _metric_contract_pass(
            float(is_report.get("sharpe", 0.0) or 0.0),
            float(baseline_reference.get("sharpe", 0.0) or 0.0),
            float(success["sharpe_ratio_gain_min"]),
        )
        sortino_ok = _metric_contract_pass(
            float(is_report.get("sortino", 0.0) or 0.0),
            float(baseline_reference.get("sortino", 0.0) or 0.0),
            float(success["sortino_ratio_gain_min"]),
        )
        status = "FAIL_IS"
        failure_reason = ""
        if sharpe_ok and sortino_ok and int(is_report.get("trade_count", 0) or 0) > 0:
            status = "PASS_IS"
        else:
            failure_reason = "Sharpe / Sortino admission contract not met after runtime unlock."
        if status == "PASS_IS":
            status = "PASS_OOS" if int(oos_report.get("trade_count", 0) or 0) > 0 else "PENDING_OOS_DATA"
        return {
            "contract_id": contract["contract_id"],
            "doctrine_id": doctrine_id,
            "status": status,
            "deadline_utc": deadline_utc,
            "failure_reason": failure_reason,
            "fail_action": contract["fail_action"],
            "success_contract": success,
        }

    if not baseline_reference.get("baseline_available"):
        return {
            "contract_id": contract["contract_id"],
            "doctrine_id": doctrine_id,
            "status": "BASELINE_UNAVAILABLE",
            "deadline_utc": deadline_utc,
            "failure_reason": "No baseline summary row available.",
            "fail_action": contract["fail_action"],
            "success_contract": success,
        }

    status = "FAIL_IS"
    failure_reason = ""
    if contract["contract_id"] == "C1_FLOW_DRIFT_SHORT":
        capture_delta = float(is_report.get("capture_rate", 0.0) or 0.0) - float(baseline_reference.get("capture_rate", 0.0) or 0.0)
        expectancy_ok = float(is_report.get("expectancy_pips", 0.0) or 0.0) >= float(success["expectancy_pips_min"])
        win_rate_ok = float(is_report.get("win_rate", 0.0) or 0.0) >= float(baseline_reference.get("win_rate", 0.0) or 0.0) - float(success["win_rate_drawdown_max"])
        if capture_delta >= float(success["capture_rate_delta_min"]) and expectancy_ok and win_rate_ok:
            status = "PASS_IS"
        else:
            failure_reason = "Capture lift / expectancy / win-rate contract not met."
    elif contract["contract_id"] == "C2_TRANSITION_RELEASE_SHORT_STANDARD":
        pnl_ok = float(is_report.get("net_pnl_pips", 0.0) or 0.0) >= float(baseline_reference.get("net_pnl_pips", 0.0) or 0.0) * float(success["net_pnl_ratio_min"])
        pf_value = float(is_report.get("profit_factor", 0.0) or 0.0)
        pf_ok = math.isinf(pf_value) or pf_value >= float(success["profit_factor_min"])
        if pnl_ok and pf_ok:
            status = "PASS_IS"
        else:
            failure_reason = "Net PnL retention / profit-factor contract not met."

    if status == "PASS_IS":
        status = "PASS_OOS" if int(oos_report.get("trade_count", 0) or 0) > 0 else "PENDING_OOS_DATA"

    return {
        "contract_id": contract["contract_id"],
        "doctrine_id": doctrine_id,
        "status": status,
        "deadline_utc": deadline_utc,
        "failure_reason": failure_reason,
        "fail_action": contract["fail_action"],
        "success_contract": success,
    }


def _portfolio_conflicts(
    protocol: Dict[str, Any],
    contract_metrics: Dict[str, Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    oos_scenarios = protocol["shared_harness"]["oos_scenarios"]
    threshold = float(protocol["shared_harness"]["portfolio_gate"]["correlation_threshold"])
    conflicts: Dict[str, List[Dict[str, Any]]] = {key: [] for key in contract_metrics}
    ids = sorted(contract_metrics)
    for idx, left_id in enumerate(ids):
        for right_id in ids[idx + 1:]:
            left = contract_metrics[left_id]
            right = contract_metrics[right_id]
            left_vector = [float(item.get("net_pnl_pips", 0.0) or 0.0) for item in left["oos_report"].get("scenario_breakdown", []) if item.get("scenario") in oos_scenarios]
            right_vector = [float(item.get("net_pnl_pips", 0.0) or 0.0) for item in right["oos_report"].get("scenario_breakdown", []) if item.get("scenario") in oos_scenarios]
            corr = _correlation(left_vector, right_vector)
            left_positive = {item["scenario"] for item in left["oos_report"].get("scenario_breakdown", []) if float(item.get("net_pnl_pips", 0.0) or 0.0) > 0.0}
            right_positive = {item["scenario"] for item in right["oos_report"].get("scenario_breakdown", []) if float(item.get("net_pnl_pips", 0.0) or 0.0) > 0.0}
            overlap = sorted(left_positive & right_positive)
            flag = {
                "peer_contract_id": right_id,
                "oos_pnl_correlation": corr,
                "positive_scenario_overlap": overlap,
                "conflict_flag": abs(corr) >= threshold,
            }
            conflicts[left_id].append(flag)
            conflicts[right_id].append(
                {
                    **flag,
                    "peer_contract_id": left_id,
                }
            )
    return conflicts


def _portfolio_gate_report(
    protocol: Dict[str, Any],
    contract_metrics: Dict[str, Dict[str, Any]],
    conflicts: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    passing = [
        contract_id
        for contract_id, payload in contract_metrics.items()
        if str(payload["contract_decision"].get("status", "") or "") in {"PASS_IS", "PASS_OOS"}
    ]
    positive_oos = [
        contract_id
        for contract_id, payload in contract_metrics.items()
        if contract_id in passing and float(payload["oos_report"].get("net_pnl_pips", 0.0) or 0.0) > 0.0
    ]
    conflict_pairs = [
        {
            "contract_id": contract_id,
            "peer_contract_id": flag["peer_contract_id"],
            "oos_pnl_correlation": flag["oos_pnl_correlation"],
        }
        for contract_id, flags in conflicts.items()
        for flag in flags
        if flag["conflict_flag"]
    ]
    return {
        "artifact_id": "V2_BLITZ_PORTFOLIO_GATE_REPORT",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "passing_contract_ids": passing,
        "positive_oos_contract_ids": positive_oos,
        "positive_oos_contract_count": len(positive_oos),
        "minimum_positive_oos_strategies": int(protocol["shared_harness"]["portfolio_gate"]["minimum_positive_oos_strategies"]),
        "conflict_pairs": conflict_pairs,
        "portfolio_gate_passed": bool(passing) and len(positive_oos) >= int(protocol["shared_harness"]["portfolio_gate"]["minimum_positive_oos_strategies"]) and not conflict_pairs,
    }


def _shared_harness(protocol: Dict[str, Any], determinism: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "artifact_id": "V2_BLITZ_SHARED_HARNESS",
        "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "dataset_fixture": {
            "seed": int(determinism["seed"]),
            "scenario_source": str(CONTROL_DIR / "v2_determinism_lock.json"),
            "scenario_names": determinism["sampling_policy"]["fixed_phase1_scenarios"],
            "max_profiles_per_scenario": int(determinism["sampling_policy"]["fixed_max_profiles_per_scenario"]),
            "profile_stride": int(determinism["sampling_policy"]["fixed_profile_stride"]),
        },
        "is_window": protocol["shared_harness"]["is_scenarios"],
        "oos_window": protocol["shared_harness"]["oos_scenarios"],
        "metric_set": protocol["shared_harness"]["metric_set"],
        "baseline_report": protocol["shared_harness"]["baseline_report"],
        "phase5_rows_report": protocol["shared_harness"]["phase5_rows_report"],
    }


def _run_contract_branch(contract: Dict[str, Any], baseline_report_path: Path) -> Dict[str, Any]:
    doctrine_id = str(contract["doctrine_id"])
    namespace = str(contract["namespace"])
    contract_dir = BLITZ_DIR / namespace
    contract_dir.mkdir(parents=True, exist_ok=True)

    mutation_payload = _default_branch_mutation(contract)
    mutation_path = contract_dir / "branch_mutation.json"
    write_json(mutation_path, mutation_payload)

    env = dict(os.environ)
    env["V2_BLITZ_CONFIG"] = str(mutation_path)
    completed = subprocess.run(
        [
            sys.executable,
            str(WORKSPACE / "tools" / "run_v2_entry_stack.py"),
            "--focus-strategy",
            doctrine_id,
            "--baseline-report",
            str(baseline_report_path),
        ],
        check=False,
        cwd=str(WORKSPACE),
        env=env,
    )

    focused_report_path = _focused_phase5_report_path(doctrine_id)
    focused_rows_path = _focused_phase5_rows_path(doctrine_id)
    if not focused_report_path.exists() or not focused_rows_path.exists():
        raise RuntimeError(
            f"Focused branch run for {doctrine_id} exited with code {completed.returncode} before writing required artifacts."
        )

    focused_report = read_json(focused_report_path)
    focused_rows_payload = read_json(focused_rows_path)
    phase2_report = read_json(PHASE2_DIR / "v2_phase2_cluster_report.json")
    phase2_snapshot = _doctrine_phase2_snapshot(doctrine_id, phase2_report)
    phase4_trigger_report = read_json(PHASE4_DIR / "v2_phase4_trigger_report.json")
    phase4_option_report = read_json(PHASE4_DIR / "phase4_adjustment_option_report.json")

    write_json(contract_dir / "focused_phase5_report.json", focused_report)
    write_json(contract_dir / "focused_phase5_rows.json", focused_rows_payload)
    write_json(contract_dir / "phase2_runtime_snapshot.json", phase2_snapshot)
    write_json(contract_dir / "phase4_trigger_snapshot.json", phase4_trigger_report)
    write_json(contract_dir / "phase4_adjustment_option_snapshot.json", phase4_option_report)
    focused_baseline_comparison_path = _focused_baseline_comparison_path(doctrine_id)
    if focused_baseline_comparison_path.exists():
        write_json(contract_dir / "focus_baseline_comparison.json", read_json(focused_baseline_comparison_path))
    write_json(
        contract_dir / "branch_run_metadata.json",
        {
            "artifact_id": "V2_BLITZ_BRANCH_RUN_METADATA",
            "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "contract_id": contract["contract_id"],
            "doctrine_id": doctrine_id,
            "branch_mutation_path": str(mutation_path),
            "focus_report_path": str(focused_report_path),
            "focus_rows_path": str(focused_rows_path),
            "focus_baseline_comparison_path": str(focused_baseline_comparison_path) if focused_baseline_comparison_path.exists() else None,
            "entry_stack_return_code": int(completed.returncode),
        },
    )
    return {
        "focused_report": focused_report,
        "focused_rows_payload": focused_rows_payload,
        "phase2_snapshot": phase2_snapshot,
        "mutation_payload": mutation_payload,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the repo-native parallel extraction blitz harness.")
    parser.add_argument("--refresh-stack", action="store_true", help="Refresh V2 stack outputs before computing blitz artifacts.")
    parser.add_argument("--initialize-baseline", action="store_true", help="Write shared baseline references into each contract namespace.")
    args = parser.parse_args()

    baseline_report_path = WORKSPACE / "control" / "v2_engine" / "phase5" / "v2_phase5_evaluation_report.before_wave1.json"
    if args.refresh_stack:
        subprocess.run(
            [
                sys.executable,
                str(WORKSPACE / "tools" / "run_v2_entry_stack.py"),
                "--baseline-report",
                str(baseline_report_path),
            ],
            check=True,
            cwd=str(WORKSPACE),
        )

    protocol = read_json(BLITZ_DIR / "blitz_protocol.json")
    determinism = read_json(CONTROL_DIR / "v2_determinism_lock.json")
    shared_harness = _shared_harness(protocol, determinism)
    write_json(BLITZ_DIR / "shared_harness.json", shared_harness)

    baseline_report = read_json(WORKSPACE / protocol["shared_harness"]["baseline_report"])
    contract_metrics: Dict[str, Dict[str, Any]] = {}

    for contract in protocol["contracts"]:
        contract_id = str(contract["contract_id"])
        doctrine_id = str(contract["doctrine_id"])
        contract_dir = BLITZ_DIR / str(contract["namespace"])

        baseline_reference = _baseline_reference(doctrine_id, baseline_report)
        write_json(contract_dir / "contract_spec.json", contract)
        if args.initialize_baseline or not (contract_dir / "baseline_reference.json").exists():
            write_json(contract_dir / "baseline_reference.json", baseline_reference)

        branch_result = _run_contract_branch(contract, baseline_report_path)
        branch_rows = list(branch_result["focused_rows_payload"].get("rows", []))
        is_report = {
            "artifact_id": "V2_BLITZ_IS_REPORT",
            "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "contract_id": contract_id,
            "doctrine_id": doctrine_id,
            **_window_report(branch_rows, protocol["shared_harness"]["is_scenarios"]),
        }
        oos_report = {
            "artifact_id": "V2_BLITZ_OOS_REPORT",
            "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "contract_id": contract_id,
            "doctrine_id": doctrine_id,
            **_window_report(branch_rows, protocol["shared_harness"]["oos_scenarios"]),
        }
        contract_decision = _contract_decision(contract, baseline_reference, is_report, oos_report, branch_result["phase2_snapshot"])

        write_json(contract_dir / "is_report.json", is_report)
        write_json(contract_dir / "oos_report.json", oos_report)
        write_json(contract_dir / "contract_decision.json", contract_decision)

        contract_metrics[contract_id] = {
            "contract": contract,
            "baseline_reference": baseline_reference,
            "is_report": is_report,
            "oos_report": oos_report,
            "contract_decision": contract_decision,
            "phase2_snapshot": branch_result["phase2_snapshot"],
        }

    conflicts = _portfolio_conflicts(protocol, contract_metrics)
    for contract_id, payload in contract_metrics.items():
        contract_dir = BLITZ_DIR / str(payload["contract"]["namespace"])
        write_json(
            contract_dir / "portfolio_conflict_flags.json",
            {
                "artifact_id": "V2_BLITZ_PORTFOLIO_CONFLICT_FLAGS",
                "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                "contract_id": contract_id,
                "flags": conflicts.get(contract_id, []),
            },
        )

    portfolio_gate = _portfolio_gate_report(protocol, contract_metrics, conflicts)
    write_json(BLITZ_DIR / "portfolio_gate_report.json", portfolio_gate)
    write_json(
        BLITZ_DIR / "blitz_run_summary.json",
        {
            "artifact_id": "V2_BLITZ_RUN_SUMMARY",
            "generated_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "contracts": [
                {
                    "contract_id": contract_id,
                    "status": payload["contract_decision"]["status"],
                    "namespace": payload["contract"]["namespace"],
                }
                for contract_id, payload in sorted(contract_metrics.items())
            ],
            "portfolio_gate": portfolio_gate,
            "shared_harness": str(BLITZ_DIR / "shared_harness.json"),
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
