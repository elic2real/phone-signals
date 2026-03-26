#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent
REPO_ROOT = ROOT.parent
PROMPTS_DIR = ROOT / "prompts"
REQUIRED_CONTROL_FILES = [
	"system_state.json",
	"execution_plan.json",
	"task_queue.json",
	"validation_rules.json",
	"run_log.jsonl",
	"preflight_report.json",
	"validation_result.json",
	"adjudication.json",
	"prompts/current_task.md",
]
MECHANICAL_VERDICTS = {
	"PROMOTE",
	"REJECT",
	"NO_OP",
	"CONDITIONALLY_POSITIVE_BUT_STRUCTURALLY_BLOCKED",
	"INCOMPLETE",
	"BLOCKED",
}


def _utc_now() -> str:
	return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_write(path: Path, data: dict[str, Any]) -> None:
	path.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")


def load_json(name: str) -> dict[str, Any]:
	path = ROOT / name
	return json.loads(path.read_text(encoding="utf-8"))


def save_json(name: str, data: dict[str, Any]) -> None:
	_json_write(ROOT / name, data)


def append_run_log(task_id: str, action: str, notes: str = "", extra: dict[str, Any] | None = None) -> None:
	path = ROOT / "run_log.jsonl"
	row: dict[str, Any] = {
		"ts": _utc_now(),
		"task_id": task_id,
		"action": action,
		"notes": notes,
	}
	if extra:
		row.update(extra)
	with path.open("a", encoding="utf-8") as handle:
		handle.write(json.dumps(row, sort_keys=True) + "\n")


def _default_json_for(name: str) -> dict[str, Any] | None:
	if name == "preflight_report.json":
		return {"protocol": "RCP", "status": "UNSET", "task_id": "", "generated_at": ""}
	if name == "validation_result.json":
		return {"protocol": "RCP", "status": "UNSET", "task_id": "", "generated_at": ""}
	if name == "adjudication.json":
		return {"protocol": "RCP", "status": "UNSET", "task_id": "", "generated_at": ""}
	if name == "system_state.json":
		return {
			"protocol": "RCP",
			"entry_structure": "UNKNOWN",
			"aee_structure": "UNKNOWN",
			"routing": "UNKNOWN",
			"context": "UNKNOWN",
			"calibration": "UNKNOWN",
			"mapping": "UNKNOWN",
			"macro": "UNKNOWN",
			"viability": "UNKNOWN",
			"scoring": "UNKNOWN",
			"quality": "UNKNOWN",
			"baseline_config": "entry_v23_policy_strict_active_only.json",
			"baseline_status": "UNKNOWN",
			"baseline_read_only": True,
			"active_task_id": "",
			"last_updated_by": "rcp-stage0",
		}
	if name == "execution_plan.json":
		return {
			"protocol": "RCP",
			"priority_order": ["entry_structure", "aee_structure", "routing", "context", "calibration"],
			"dependencies": {
				"entry_structure": [],
				"aee_structure": ["entry_structure"],
				"routing": ["entry_structure"],
				"context": ["entry_structure", "routing"],
				"calibration": ["entry_structure", "aee_structure"],
			},
		}
	if name == "task_queue.json":
		return {"active_task_id": "", "tasks": []}
	if name == "validation_rules.json":
		return {
			"protocol_version": "RCP_V2_HARD",
			"global_rules": [
				"One active task only",
				"One component per iteration",
				"No tuning during structural rebuild tasks",
				"No AEE edits during entry tasks unless task explicitly allows it",
				"No baseline replacement without PROMOTE verdict",
				"Every iteration must write control/preflight_report.json",
				"Every iteration must write control/validation_result.json",
				"Every iteration must write control/adjudication.json",
				"Every RCP_V2 task must declare intervention class, research layer, champion reference, expected signature, and reverse-engineering outputs",
				"Broad concept failure blocks tuning and escalates to upstream layer audit",
				"Parallel form search precedes local refinement",
				"Extraction efficiency and entry-only vs realized capture must be measurable where applicable",
				"Hard-mode tasks must provide variable_contract, falsifiable_signature, min_effect_size, and post_run_classification",
				"Hard-mode tasks must pass layer-metric-lock and parallel-isolation checks",
				"Primary edge protection and regression dominance rules are mandatory in hard mode",
				"AEE is an asymmetry engine that must be audited as distribution shaping, not generic exit management",
				"AEE audits must measure the three required transformations: loss compression, failure-to-win conversion, continuation capture",
			]
			,
			"aee_transformation_contract": {
				"purpose": "AEE reshapes post-entry outcome distribution to maximize retained extraction",
				"required_transformations": {
					"loss_compression": {
						"frequency": "VERY_HIGH",
						"role": "survival",
						"mandatory": True,
						"definition": "loser -> smaller loser"
					},
					"failure_to_win_conversion": {
						"frequency": "HIGH",
						"role": "edge_creation",
						"mandatory": True,
						"definition": "slow_or_borderline_loser -> small_or_medium_winner"
					},
					"continuation_capture": {
						"frequency": "LOW",
						"role": "edge_amplification",
						"mandatory": True,
						"definition": "normal_win -> extended_win"
					}
				},
				"required_metrics": [
					"loss_compression_rate",
					"failure_to_win_conversion_rate",
					"continuation_capture_rate",
					"giveback_ratio"
				],
				"hard_adjudication_gates": {
					"promote_requires": [
						"loss_compression_rate_pass",
						"failure_to_win_conversion_rate_pass",
						"continuation_capture_rate_pass",
						"giveback_ratio_pass",
						"blocking_overfire_absent_pass"
					],
					"conditional_structural_block_rule": {
						"when": "realized_pph_positive_and_continuation_capture_fail_and_giveback_fail",
						"verdict": "CONDITIONALLY_POSITIVE_BUT_STRUCTURALLY_BLOCKED"
					}
				},
				"overfire_severity_policy": {
					"blocking_flags": [
						"AEE_GIVEBACK_EXIT_OVERFIRE",
						"HIGH_GIVEBACK_RATIO"
					],
					"non_blocking_flags": [
						"AEE_BAND_FAST_FAILURE_EARLY_FIRE"
					]
				},
				"profit_amplifier_contract": {
					"statement": "AEE must increase realized extraction versus simpler alternatives",
					"required_baselines": [
						"static_tp_sl",
						"minimal_protective_only",
						"no_aee_loose",
						"aee_candidate"
					],
					"hard_reject_baselines": [
						"minimal_protective_only",
						"no_aee_loose"
					],
					"hard_reject_no_override": True,
					"hard_reject_condition": "If candidate fails to beat minimal_protective_only OR no_aee_loose on realized_pph OR avg_pips_per_trade, verdict must be REJECT",
					"primary_judgment_metrics": [
						"realized_pph_delta_vs_baseline_exit",
						"realized_avg_pips_per_trade_delta_vs_baseline_exit",
						"loss_compression_benefit",
						"continuation_capture_benefit",
						"net_expectancy_shift"
					],
					"promote_requires_outperform_all_simpler_baselines": True
				},
				"bounded_performance_expectations": {
					"global_expected_ranges": {
						"realized_pph_change_pct": [0.20, 0.80],
						"gap_change_pct": [-0.40, -0.15],
						"efficiency_abs_change": [0.05, 0.25]
					},
					"giveback_fix_expected_ranges": {
						"giveback_ratio_change_pct": [-0.30, -0.10],
						"giveback_exit_share_change_pct": [-0.30, -0.10],
						"continuation_capture_change_pct": [0.05, 0.20],
						"breakout_pph_change_pct": [0.10, 0.40]
					},
					"bankable_green_loss_red_rate_target_max": 0.05,
					"throughput_expected_directions": {
						"trades_per_hour": "up",
						"avg_loser_hold_sec": "down",
						"avg_weak_winner_hold_sec": "down"
					},
					"loss_compression_floor": 0.85,
					"core_rule_all_four_required": True,
					"auto_reject_patterns": [
						"COSMETIC_IMPROVEMENT",
						"BANKABLE_GREEN_PROTECTION_FAILED",
						"GAP_NOT_MATERIALLY_REDUCED",
						"OVER_HOLD_BROKE_LOSS_COMPRESSION",
						"MORE_CONTINUATION_WORSE_PPH",
						"THROUGHPUT_STALLED",
						"SHIFTED_DAMAGE_GIVEBACK_TO_FAST_FAILURE",
						"BASELINE_STILL_BETTER"
					]
				},
				"reverse_engineering_prerun_basis": {
					"required": True,
					"artifact": "control/aee_intervention_basis.json",
					"required_fields": [
						"economic_objective",
						"green_tier_definition",
						"state_framework",
						"module_order",
						"source_runs_used",
						"source_logs_used",
						"dominant_damaging_branches",
						"affected_families",
						"affected_subclusters",
						"evidence_samples",
						"proposed_variable_or_transition",
						"expected_signature",
						"success_criteria"
					]
				}
			},
			"task_required_fields_v2": [
				"protocol_version",
				"intervention_class",
				"research_layer",
				"champion_reference",
				"expected_signature",
				"reverse_engineering_outputs",
				"ablatable_components",
				"fail_conditions",
				"output_requirements"
			],
			"strict_task_required_fields": [
				"variable_contract",
				"falsifiable_signature",
				"evaluation_metrics",
				"parallel_isolation",
				"min_effect_size",
				"dead_path_policy",
				"protected_metric",
				"regression_dominance_rule",
				"concept_lock",
				"post_run_classification",
			],
			"intervention_classes": [
				"FORM_SEARCH",
				"PARAM_TUNE",
				"ABLATION",
				"SIMULATION_AUDIT",
				"EXIT_AUDIT",
				"CLASSIFIER_AUDIT",
				"PRODUCTION_HARDENING",
				"IMPLEMENTATION",
				"SPEC_ONLY"
			],
			"research_layers": [
				"CONCEPT_FORM",
				"IMPLEMENTATION",
				"SIMULATION",
				"AEE_INTERACTION",
				"FILTERING",
				"EXPANSION"
			],
			"layer_metric_lock": {
				"CONCEPT_FORM": ["trade_count", "entry_only_pph", "avg_pips_per_trade"],
				"AEE_INTERACTION": [
					"realized_pph",
					"entry_only_vs_realized_gap",
					"exit_reason_distribution",
					"avg_hold_time",
					"loss_compression_rate",
					"failure_to_win_conversion_rate",
					"continuation_capture_rate",
					"giveback_ratio"
				],
				"SIMULATION": ["family_rank_shift", "sequence_fidelity_score", "extraction_efficiency", "breakout_bias_score", "pricing_consistency_score"],
				"FILTERING": ["trade_count", "family_distribution", "realized_pph", "raw_vs_filtered_delta"],
				"IMPLEMENTATION": ["test_pass_rate", "validation_command_pass_rate", "artifact_completeness"],
				"EXPANSION": ["total_available_pips", "entry_only_capture", "realized_capture", "extraction_efficiency"],
			},
			"noise_floor": {"pph_abs": 0.005, "gap_abs": 0.005, "share_abs": 0.02},
			"protected_edge_policy": {
				"default_metric": "EXPANSION_BREAKOUT_pph",
				"minimum_allowed_ratio": 0.9,
				"rule": "Auto-fail if protected edge drops below threshold",
			},
			"regression_dominance_rule": {
				"rule": "Fail if primary edge degrades while secondary gains are used as justification",
				"required": True,
			},
			"concept_lock": {
				"concept_validity": "ASSUMED_TRUE",
				"failure_interpretation": "IMPLEMENTATION_OR_SIMULATION_ERROR",
			},
			"required_result_pack_artifacts": [
				"control/run_summary.json",
				"control/config_snapshot.json",
				"control/logic_trace_summary.json",
				"control/trade_evidence_sample.json",
				"control/data_coverage_report.json",
				"control/expected_vs_actual_signature.json",
				"control/failure_layer_classification.json",
				"control/aee_transformation_audit.json",
				"control/aee_baseline_ab_comparison.json",
				"control/aee_performance_signature.json",
				"control/aee_green_loss_audit.json",
				"control/aee_intervention_basis.json",
				"control/auto_adjudication.json",
				"control/next_task_recommendation.json",
				"control/champion_dual_status.json",
			],
			"required_result_pack_sections": {
				"control/run_summary.json": [
					"run_id",
					"champion_reference",
					"intervention_class",
					"strategy_form",
					"aee_version",
					"simulation_mode",
					"data_coverage",
					"results",
					"aee_transformation_audit",
					"top_logic_paths",
					"top_damage",
					"signature_check",
					"verdict",
				],
				"control/config_snapshot.json": [
					"strategy_family",
					"strategy_form_id",
					"thresholds",
					"enabled_gates",
					"disabled_gates",
					"aee_version",
					"simulation_mode",
					"dataset_window_id",
					"pair_session_coverage",
					"code_version",
					"artifact_version",
				],
				"control/logic_trace_summary.json": [
					"detector_logic_path",
					"gating_path",
					"exit_logic_path",
					"state_transitions_used",
				],
				"control/trade_evidence_sample.json": ["winners", "losers", "ambiguous"],
				"control/data_coverage_report.json": [
					"pair_coverage",
					"streams",
					"unique_days",
					"sessions_represented",
					"session_quarter_distribution",
					"regime_distribution",
					"hours",
					"dominance_concentration",
				],
				"control/expected_vs_actual_signature.json": [
					"expected_signature",
					"actual_signature",
					"delta_vs_champion",
					"matches_expected_model",
				],
				"control/failure_layer_classification.json": ["failure_layer", "confidence", "reason"],
				"control/aee_transformation_audit.json": [
					"loss_compression",
					"failure_to_win_conversion",
					"continuation_capture",
					"giveback_ratio",
					"branch_role_audit",
					"branch_overfire_flags"
				],
				"control/aee_baseline_ab_comparison.json": [
					"baselines",
					"candidate_vs_baselines"
				],
				"control/aee_performance_signature.json": [
					"immediate_signature",
					"global_expectation",
					"bankable_green_protection",
					"throughput_signature",
					"baseline_flip_signature",
					"gap_reduction_signature",
					"giveback_fix_signature",
					"continuation_signature",
					"loss_compression_stability",
					"core_rule_all_four",
					"auto_reject_patterns"
				],
				"control/aee_green_loss_audit.json": [
					"thresholds",
					"summary",
					"green_tier_distribution",
					"dead_trade_subtype_distribution",
					"economic_state_distribution",
					"family_distribution",
					"exit_reason_distribution",
					"path_shape_distribution",
					"throughput",
					"green_then_loss_samples",
					"bankable_green_loss_samples",
					"dead_trade_samples",
					"never_green_samples"
				],
				"control/aee_intervention_basis.json": [
					"economic_objective",
					"green_tier_definition",
					"state_framework",
					"module_order",
					"source_runs_used",
					"source_logs_used",
					"dominant_damaging_branches",
					"affected_families",
					"affected_subclusters",
					"evidence_samples",
					"proposed_variable_or_transition",
					"expected_signature",
					"success_criteria"
				],
				"control/auto_adjudication.json": [
					"verdict",
					"reason",
					"gates",
					"blocking_overfire_flags",
					"is_structurally_healthy"
				],
				"control/next_task_recommendation.json": [
					"intervention_class",
					"scope",
					"bounded_constraints",
					"target_branches"
				],
				"control/champion_dual_status.json": [
					"performance_champion",
					"structural_champion",
					"comparison"
				],
			},
			"benchmark_stack_required": [
				"control/concept_registry.json",
				"control/strategy_form_registry.json",
				"control/champion_registry.json",
				"control/gold_cases.json",
				"control/benchmark_suite.json",
				"control/degradation_waterfall_template.json",
				"control/family_confusion_report_template.json",
				"control/ablation_matrix_template.json"
			],
			"forbidden_behaviors": [
				"apply all fixes in one pass",
				"silently broaden scope",
				"mix rebuild and tuning in one iteration",
				"run new candidate before reading prior adjudication",
				"overwrite baseline without promote verdict",
				"use chat memory as source of truth over control files",
				"treat absence of evidence as permission",
				"conclude concept failure before checking simulator and AEE interaction",
				"optimize local winner before multi-form ranking"
			]
		}
	return None


def stage0(create_missing: bool = True) -> int:
	missing: list[str] = []
	for rel in REQUIRED_CONTROL_FILES:
		path = ROOT / rel
		if not path.exists():
			missing.append(rel)

	if missing and create_missing:
		for rel in missing:
			path = ROOT / rel
			path.parent.mkdir(parents=True, exist_ok=True)
			if rel.endswith(".json"):
				default_data = _default_json_for(Path(rel).name)
				if default_data is None:
					default_data = {"protocol": "RCP", "status": "UNSET"}
				_json_write(path, default_data)
			elif rel.endswith(".jsonl"):
				path.write_text("", encoding="utf-8")
			else:
				path.write_text("# Active Task\n\nRun python3 control/orchestrator.py render\n", encoding="utf-8")
		append_run_log("SYSTEM", "STAGE0_MISSING_CREATED", "Created missing control files", {"missing": missing})
		print(json.dumps({"stage": 0, "status": "MISSING_CREATED", "missing": missing}, indent=2))
		return 2

	if missing:
		print(json.dumps({"stage": 0, "status": "MISSING", "missing": missing}, indent=2))
		return 1

	print(json.dumps({"stage": 0, "status": "OK"}, indent=2))
	return 0


def get_active_task(queue: dict[str, Any]) -> dict[str, Any]:
	active_id = str(queue.get("active_task_id", "")).strip()
	if not active_id:
		raise ValueError("task_queue.active_task_id is empty")
	for task in queue.get("tasks", []):
		if str(task.get("id")) == active_id:
			return task
	raise ValueError(f"Active task not found: {active_id}")


def get_task(queue: dict[str, Any], task_id: str) -> dict[str, Any]:
	for task in queue.get("tasks", []):
		if str(task.get("id")) == task_id:
			return task
	raise ValueError(f"Task not found: {task_id}")


def _task_index(queue: dict[str, Any], task_id: str) -> int:
	for idx, task in enumerate(queue.get("tasks", [])):
		if str(task.get("id")) == task_id:
			return idx
	raise ValueError(f"Task not found: {task_id}")


def _normalize_task(task: dict[str, Any]) -> dict[str, Any]:
	if "validation_commands" not in task:
		task["validation_commands"] = list(task.get("validation", []))
	if "allowed_files" not in task:
		task["allowed_files"] = ["control/*"]
	if "protocol_version" not in task:
		task["protocol_version"] = "RCP_V1"
	task.setdefault("enforcement_mode", "")
	task.setdefault("intervention_class", "")
	task.setdefault("research_layer", "")
	task.setdefault("champion_reference", [])
	task.setdefault("expected_signature", {})
	task.setdefault("reverse_engineering_outputs", [])
	task.setdefault("ablatable_components", [])
	task.setdefault("variable_contract", {})
	task.setdefault("falsifiable_signature", {})
	task.setdefault("evaluation_metrics", [])
	task.setdefault("parallel_isolation", {})
	task.setdefault("min_effect_size", {})
	task.setdefault("dead_path_policy", {})
	task.setdefault("protected_metric", {})
	task.setdefault("regression_dominance_rule", {})
	task.setdefault("concept_lock", {})
	task.setdefault("post_run_classification", {})
	task.setdefault("fail_conditions", [])
	task.setdefault("output_requirements", {"must_produce": list(task.get("deliverables", []))})
	task.setdefault("adjudication_rules", sorted(MECHANICAL_VERDICTS))
	return task


def _as_list(value: Any) -> list[str]:
	if value is None:
		return []
	if isinstance(value, list):
		return [str(item) for item in value if str(item).strip()]
	if isinstance(value, str):
		return [value] if value.strip() else []
	return [str(value)]


def _protocol_is_hard(task: dict[str, Any]) -> bool:
	version = str(task.get("protocol_version", "")).strip().upper()
	mode = str(task.get("enforcement_mode", "")).strip().upper()
	return version in {"RCP_V2_HARD", "RCP_V3", "RCP_V3_HARD"} or mode == "STRICT"


def _requires_full_evidence_pack(task: dict[str, Any], rules: dict[str, Any]) -> bool:
	if not _protocol_is_hard(task):
		return False
	# Spec-only or explicit opt-out tasks can skip runtime evidence packs.
	if str(task.get("intervention_class", "")).strip().upper() == "SPEC_ONLY":
		return False
	if task.get("require_full_evidence_pack") is False:
		return False
	return bool(rules.get("required_result_pack_artifacts", []))


def _is_falsifiable_expr(value: Any) -> bool:
	text = str(value).strip().lower()
	if not text:
		return False
	if len(text) < 4:
		return False
	# Require directional/comparator/magnitude tokens to avoid vague signatures.
	tokens = [
		"<=", ">=", "<", ">", " to ", "baseline", "%", "+", "-", "delta", "range", "between"
	]
	if not any(token in text for token in tokens):
		return False
	if re.fullmatch(r"[a-z\s]+", text):
		return False
	return True


def _task_schema_failures(task: dict[str, Any], rules: dict[str, Any]) -> list[dict[str, Any]]:
	if str(task.get("protocol_version", "RCP_V1")).upper() != "RCP_V2":
		if not _protocol_is_hard(task):
			return []

	failures: list[dict[str, Any]] = []
	required_fields = [str(x) for x in rules.get("task_required_fields_v2", [])]
	for field in required_fields:
		value = task.get(field)
		if value in (None, "", [], {}):
			failures.append({"type": "task_schema_missing_field", "field": field})

	allowed_interventions = {str(x) for x in rules.get("intervention_classes", [])}
	allowed_layers = {str(x) for x in rules.get("research_layers", [])}
	intervention = str(task.get("intervention_class", "")).strip()
	research_layer = str(task.get("research_layer", "")).strip()
	if intervention and allowed_interventions and intervention not in allowed_interventions:
		failures.append({"type": "task_schema_invalid_enum", "field": "intervention_class", "value": intervention})
	if research_layer and allowed_layers and research_layer not in allowed_layers:
		failures.append({"type": "task_schema_invalid_enum", "field": "research_layer", "value": research_layer})

	champion_reference = _as_list(task.get("champion_reference"))
	if not champion_reference:
		failures.append({"type": "task_schema_missing_field", "field": "champion_reference"})

	expected_signature = task.get("expected_signature", {})
	if not isinstance(expected_signature, dict):
		failures.append({"type": "task_schema_invalid_type", "field": "expected_signature", "expected": "object"})
	else:
		for key in [
			"trade_count",
			"trades_per_hour",
			"avg_pips_per_trade",
			"net_pips_per_hour",
			"entry_only_vs_realized_gap",
		]:
			if str(expected_signature.get(key, "")).strip() == "":
				failures.append({"type": "task_schema_missing_signature", "field": key})

	output_requirements = task.get("output_requirements", {})
	if not isinstance(output_requirements, dict):
		failures.append({"type": "task_schema_invalid_type", "field": "output_requirements", "expected": "object"})
	else:
		must_produce = _as_list(output_requirements.get("must_produce"))
		if not must_produce:
			failures.append({"type": "task_schema_missing_field", "field": "output_requirements.must_produce"})

	if not _protocol_is_hard(task):
		return failures

	strict_required = [str(x) for x in rules.get("strict_task_required_fields", [])]
	for field in strict_required:
		value = task.get(field)
		if value in (None, "", [], {}):
			failures.append({"type": "strict_schema_missing_field", "field": field})

	variable_contract = task.get("variable_contract", {})
	if not isinstance(variable_contract, dict):
		failures.append({"type": "strict_schema_invalid_type", "field": "variable_contract", "expected": "object"})
	else:
		for key in ["variable", "layer", "intervention", "scope"]:
			if str(variable_contract.get(key, "")).strip() == "":
				failures.append({"type": "variable_contract_missing_field", "field": key})

	falsifiable_signature = task.get("falsifiable_signature", {})
	if not isinstance(falsifiable_signature, dict):
		failures.append({"type": "strict_schema_invalid_type", "field": "falsifiable_signature", "expected": "object"})
	else:
		expected = falsifiable_signature.get("expected", {})
		failure = falsifiable_signature.get("failure", {})
		if not isinstance(expected, dict) or not expected:
			failures.append({"type": "falsifiability_missing_block", "field": "falsifiable_signature.expected"})
		if not isinstance(failure, dict) or not failure:
			failures.append({"type": "falsifiability_missing_block", "field": "falsifiable_signature.failure"})
		if isinstance(expected, dict):
			for key, value in expected.items():
				if not _is_falsifiable_expr(value):
					failures.append({"type": "falsifiability_non_specific", "field": f"expected.{key}", "value": value})
		if isinstance(failure, dict):
			for key, value in failure.items():
				if not _is_falsifiable_expr(value):
					failures.append({"type": "falsifiability_non_specific", "field": f"failure.{key}", "value": value})

	layer_metric_lock = {str(k): [str(x) for x in v] for k, v in (rules.get("layer_metric_lock", {}) or {}).items()}
	research_layer = str(task.get("research_layer", "")).strip()
	evaluation_metrics = _as_list(task.get("evaluation_metrics"))
	allowed_metrics = set(layer_metric_lock.get(research_layer, []))
	if evaluation_metrics and allowed_metrics:
		invalid_metrics = [m for m in evaluation_metrics if m not in allowed_metrics]
		if invalid_metrics:
			failures.append(
				{
					"type": "layer_metric_lock_violation",
					"research_layer": research_layer,
					"invalid_metrics": invalid_metrics,
					"allowed_metrics": sorted(allowed_metrics),
				}
			)

	parallel_isolation = task.get("parallel_isolation", {})
	if not isinstance(parallel_isolation, dict):
		failures.append({"type": "strict_schema_invalid_type", "field": "parallel_isolation", "expected": "object"})
	else:
		max_variables = parallel_isolation.get("max_variables")
		independent = bool(parallel_isolation.get("variables_independent", False))
		if max_variables is None and not independent:
			failures.append({"type": "parallel_isolation_missing", "field": "parallel_isolation"})
		if isinstance(max_variables, int) and max_variables > 1 and not independent:
			failures.append(
				{
					"type": "parallel_isolation_violation",
					"max_variables": max_variables,
					"variables_independent": independent,
				}
			)

	min_effect_size = task.get("min_effect_size", {})
	if not isinstance(min_effect_size, dict) or not min_effect_size:
		failures.append({"type": "min_effect_size_missing", "field": "min_effect_size"})

	dead_path_policy = task.get("dead_path_policy", {})
	if not isinstance(dead_path_policy, dict):
		failures.append({"type": "strict_schema_invalid_type", "field": "dead_path_policy", "expected": "object"})
	else:
		for key in ["history_artifact", "max_attempts", "unblock_requires"]:
			if dead_path_policy.get(key) in (None, "", [], {}):
				failures.append({"type": "dead_path_policy_missing", "field": key})

	protected_metric = task.get("protected_metric", {})
	if not isinstance(protected_metric, dict):
		failures.append({"type": "strict_schema_invalid_type", "field": "protected_metric", "expected": "object"})
	else:
		for key in ["name", "minimum_allowed"]:
			if str(protected_metric.get(key, "")).strip() == "":
				failures.append({"type": "protected_metric_missing", "field": key})

	if not isinstance(task.get("regression_dominance_rule", {}), dict) or not task.get("regression_dominance_rule", {}):
		failures.append({"type": "strict_schema_missing_field", "field": "regression_dominance_rule"})

	concept_lock = task.get("concept_lock", {})
	if not isinstance(concept_lock, dict):
		failures.append({"type": "strict_schema_invalid_type", "field": "concept_lock", "expected": "object"})
	else:
		if str(concept_lock.get("concept_validity", "")).strip().upper() != "ASSUMED_TRUE":
			failures.append({"type": "concept_lock_invalid", "field": "concept_validity"})
		if str(concept_lock.get("failure_interpretation", "")).strip() == "":
			failures.append({"type": "concept_lock_missing", "field": "failure_interpretation"})

	post_run = task.get("post_run_classification", {})
	if not isinstance(post_run, dict):
		failures.append({"type": "strict_schema_invalid_type", "field": "post_run_classification", "expected": "object"})
	else:
		for key in ["artifact", "required_fields"]:
			if post_run.get(key) in (None, "", [], {}):
				failures.append({"type": "post_run_classification_missing", "field": key})

	if _requires_full_evidence_pack(task, rules):
		required_artifacts = _as_list(rules.get("required_result_pack_artifacts"))
		must_produce = _as_list((task.get("output_requirements") or {}).get("must_produce"))
		missing_artifacts = [x for x in required_artifacts if x not in must_produce]
		if missing_artifacts:
			failures.append(
				{
					"type": "required_result_pack_missing_in_task",
					"missing_artifacts": missing_artifacts,
				}
			)

	return failures


def _load_json_file(rel_or_abs: str) -> dict[str, Any] | None:
	path = (REPO_ROOT / rel_or_abs).resolve() if not Path(rel_or_abs).is_absolute() else Path(rel_or_abs).resolve()
	if not path.exists():
		return None
	try:
		return json.loads(path.read_text(encoding="utf-8"))
	except Exception:
		return None


def _lookup_variable_history(payload: dict[str, Any], variable_name: str) -> dict[str, Any] | None:
	if not payload:
		return None
	vh = payload.get("variable_history")
	if isinstance(vh, dict) and isinstance(vh.get(variable_name), dict):
		return vh.get(variable_name)
	for key in ["variables", "rows"]:
		items = payload.get(key)
		if isinstance(items, list):
			for item in items:
				if not isinstance(item, dict):
					continue
				name = str(item.get("name") or item.get("variable") or "").strip()
				if name == variable_name:
					return item
	return None


def _dead_path_gate_failures(task: dict[str, Any]) -> list[dict[str, Any]]:
	if not _protocol_is_hard(task):
		return []
	policy = task.get("dead_path_policy", {})
	if not isinstance(policy, dict) or not policy:
		return []
	history_artifact = str(policy.get("history_artifact", "")).strip()
	variable_name = str((task.get("variable_contract") or {}).get("variable", "")).strip()
	if not history_artifact or not variable_name:
		return []
	payload = _load_json_file(history_artifact)
	if payload is None:
		return [{"type": "dead_path_history_missing", "history_artifact": history_artifact}]

	row = _lookup_variable_history(payload, variable_name)
	if not isinstance(row, dict):
		return []

	status = str(row.get("status") or row.get("current_status") or row.get("current_belief") or "").upper()
	attempts = row.get("attempts")
	max_attempts = policy.get("max_attempts")
	if not isinstance(attempts, int) or not isinstance(max_attempts, int):
		return []

	blocked_statuses = {
		"NON_BINDING",
		"PROVEN_NONBINDING",
		"PROVEN_NON_BINDING",
		"SUSPECT_NONBINDING",
		"SUSPECT_NON_BINDING",
	}
	if attempts >= max_attempts and status in blocked_statuses:
		override = task.get("dead_path_override", {})
		if not isinstance(override, dict) or not override:
			return [
				{
					"type": "dead_path_kill_switch",
					"variable": variable_name,
					"attempts": attempts,
					"max_attempts": max_attempts,
					"status": status,
				}
			]
		if str(override.get("reason", "")).strip() == "":
			return [{"type": "dead_path_override_missing_reason", "variable": variable_name}]
	return []


def _dependency_check(task: dict[str, Any], queue: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
	component = str(task.get("component", "")).strip()
	deps = list((plan.get("dependencies") or {}).get(component, []))
	incomplete: list[str] = []
	for dep_component in deps:
		dep_tasks = [t for t in queue.get("tasks", []) if str(t.get("component", "")).strip() == dep_component]
		if dep_tasks and not all(str(t.get("status", "")).upper() == "DONE" for t in dep_tasks):
			incomplete.append(dep_component)

	task_deps = [str(x).strip() for x in task.get("depends_on_task_ids", []) if str(x).strip()]
	incomplete_tasks: list[str] = []
	for dep_task_id in task_deps:
		try:
			dep_task = get_task(queue, dep_task_id)
		except ValueError:
			incomplete_tasks.append(f"missing:{dep_task_id}")
			continue
		if str(dep_task.get("status", "")).upper() != "DONE":
			incomplete_tasks.append(dep_task_id)

	return {
		"dependencies": deps,
		"task_dependencies": task_deps,
		"all_met": len(incomplete) == 0 and len(incomplete_tasks) == 0,
		"incomplete_components": incomplete,
		"incomplete_tasks": incomplete_tasks,
	}


def _git_changed_files() -> set[str]:
	proc = subprocess.run(
		"git status --porcelain",
		shell=True,
		cwd=REPO_ROOT,
		capture_output=True,
		text=True,
	)
	if proc.returncode != 0:
		return set()
	out: set[str] = set()
	for line in proc.stdout.splitlines():
		if not line.strip():
			continue
		raw = line[3:] if len(line) > 3 else line
		if " -> " in raw:
			raw = raw.split(" -> ", 1)[1]
		out.add(raw.strip())
	return out


def _expand_allowed(task: dict[str, Any]) -> list[str]:
	allowed = [str(x).replace("\\", "/") for x in task.get("allowed_files", [])]
	if "control/*" not in allowed:
		allowed.append("control/*")
	return allowed


def _matches_allowed(path: str, allowed_patterns: list[str]) -> bool:
	normalized = str(path).replace("\\", "/")
	return any(fnmatch.fnmatch(normalized, pattern) for pattern in allowed_patterns)


def _hash_file(path: Path) -> str:
	h = hashlib.sha256()
	with path.open("rb") as handle:
		while True:
			chunk = handle.read(65536)
			if not chunk:
				break
			h.update(chunk)
	return h.hexdigest()


def _scope_hash_snapshot(allowed_patterns: list[str]) -> dict[str, str]:
	snapshot: dict[str, str] = {}
	for pattern in allowed_patterns:
		for p in REPO_ROOT.glob(pattern):
			if p.is_file():
				rel = p.relative_to(REPO_ROOT).as_posix()
				snapshot[rel] = _hash_file(p)
	return dict(sorted(snapshot.items()))


def render_current_task() -> Path:
	stage0(create_missing=True)
	queue = load_json("task_queue.json")
	plan = load_json("execution_plan.json")
	rules = load_json("validation_rules.json")
	task = _normalize_task(get_active_task(queue))
	dep = _dependency_check(task, queue, plan)

	lines: list[str] = []
	lines.append(f"# Active Task: {task.get('id')}")
	lines.append("")
	lines.append(f"Title: {task.get('title', '')}")
	lines.append(f"Component: {task.get('component', '')}")
	lines.append(f"Status: {task.get('status', '')}")
	lines.append(f"Protocol Version: {task.get('protocol_version', '')}")
	if str(task.get("intervention_class", "")).strip():
		lines.append(f"Intervention Class: {task.get('intervention_class', '')}")
	if str(task.get("research_layer", "")).strip():
		lines.append(f"Research Layer: {task.get('research_layer', '')}")
	lines.append("")
	lines.append("## Protocol")
	lines.append("- You are not allowed to operate directly from chat intent.")
	lines.append("- You must operate through the Repo Control Protocol (RCP).")
	lines.append("- Read only the active task and stay in task scope.")
	lines.append("")
	lines.append("## Rules")
	for rule in rules.get("global_rules", []):
		lines.append(f"- {rule}")
	lines.append("")
	lines.append("## Inputs")
	for item in task.get("inputs", []):
		lines.append(f"- {item}")
	lines.append("")
	champion_reference = _as_list(task.get("champion_reference"))
	if champion_reference:
		lines.append("## Champion Reference")
		for item in champion_reference:
			lines.append(f"- {item}")
		lines.append("")
	expected_signature = task.get("expected_signature", {})
	if isinstance(expected_signature, dict) and expected_signature:
		lines.append("## Expected Signature")
		for key, value in expected_signature.items():
			lines.append(f"- {key}: {value}")
		lines.append("")
	falsifiable_signature = task.get("falsifiable_signature", {})
	if isinstance(falsifiable_signature, dict) and falsifiable_signature:
		lines.append("## Falsifiable Signature")
		for block in ["expected", "failure"]:
			payload = falsifiable_signature.get(block, {})
			if isinstance(payload, dict) and payload:
				lines.append(f"- {block}:")
				for key, value in payload.items():
					lines.append(f"- {key}: {value}")
		lines.append("")
	variable_contract = task.get("variable_contract", {})
	if isinstance(variable_contract, dict) and variable_contract:
		lines.append("## Variable Contract")
		for key, value in variable_contract.items():
			lines.append(f"- {key}: {value}")
		lines.append("")
	evaluation_metrics = _as_list(task.get("evaluation_metrics"))
	if evaluation_metrics:
		lines.append("## Evaluation Metrics")
		for item in evaluation_metrics:
			lines.append(f"- {item}")
		lines.append("")
	parallel_isolation = task.get("parallel_isolation", {})
	if isinstance(parallel_isolation, dict) and parallel_isolation:
		lines.append("## Parallel Isolation")
		for key, value in parallel_isolation.items():
			lines.append(f"- {key}: {value}")
		lines.append("")
	min_effect_size = task.get("min_effect_size", {})
	if isinstance(min_effect_size, dict) and min_effect_size:
		lines.append("## Minimum Effect Size")
		for key, value in min_effect_size.items():
			lines.append(f"- {key}: {value}")
		lines.append("")
	protected_metric = task.get("protected_metric", {})
	if isinstance(protected_metric, dict) and protected_metric:
		lines.append("## Protected Metric")
		for key, value in protected_metric.items():
			lines.append(f"- {key}: {value}")
		lines.append("")
	post_run = task.get("post_run_classification", {})
	if isinstance(post_run, dict) and post_run:
		lines.append("## Post Run Classification")
		for key, value in post_run.items():
			lines.append(f"- {key}: {value}")
		lines.append("")
	reverse_outputs = _as_list(task.get("reverse_engineering_outputs"))
	if reverse_outputs:
		lines.append("## Reverse Engineering Outputs")
		for item in reverse_outputs:
			lines.append(f"- {item}")
		lines.append("")
	ablatable = _as_list(task.get("ablatable_components"))
	if ablatable:
		lines.append("## Ablatable Components")
		for item in ablatable:
			lines.append(f"- {item}")
		lines.append("")
	lines.append("## Allowed Files")
	for item in _expand_allowed(task):
		lines.append(f"- {item}")
	lines.append("")
	lines.append("## Deliverables")
	for item in task.get("deliverables", []):
		lines.append(f"- {item}")
	lines.append("")
	lines.append("## Validation Commands")
	for item in task.get("validation_commands", []):
		lines.append(f"- {item}")
	lines.append("")
	lines.append("## Done When")
	for item in task.get("done_when", []):
		lines.append(f"- {item}")
	lines.append("")
	fail_conditions = _as_list(task.get("fail_conditions"))
	if fail_conditions:
		lines.append("## Fail Conditions")
		for item in fail_conditions:
			lines.append(f"- {item}")
		lines.append("")
	output_requirements = task.get("output_requirements", {})
	if isinstance(output_requirements, dict) and output_requirements:
		lines.append("## Output Requirements")
		for key, value in output_requirements.items():
			if isinstance(value, list):
				lines.append(f"- {key}:")
				for item in value:
					lines.append(f"- {item}")
			else:
				lines.append(f"- {key}: {value}")
		lines.append("")
	lines.append("## Dependency Check")
	lines.append(f"- all_met: {dep['all_met']}")
	if dep["incomplete_components"]:
		lines.append("- incomplete_components:")
		for item in dep["incomplete_components"]:
			lines.append(f"- {item}")
	if dep.get("incomplete_tasks"):
		lines.append("- incomplete_tasks:")
		for item in dep.get("incomplete_tasks", []):
			lines.append(f"- {item}")

	out_path = PROMPTS_DIR / "current_task.md"
	out_path.parent.mkdir(parents=True, exist_ok=True)
	out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
	append_run_log(str(task.get("id", "UNKNOWN")), "STAGE0_STAGE1_READY", "Rendered current task prompt")
	return out_path


def write_preflight_report() -> int:
	rc = stage0(create_missing=True)
	if rc == 1:
		return rc
	queue = load_json("task_queue.json")
	plan = load_json("execution_plan.json")
	task = _normalize_task(get_active_task(queue))
	dep = _dependency_check(task, queue, plan)
	allowed = _expand_allowed(task)
	baseline = str(load_json("system_state.json").get("baseline_config", "")).strip()

	planned_reads = [
		"control/system_state.json",
		"control/execution_plan.json",
		"control/task_queue.json",
		"control/validation_rules.json",
		"control/run_log.jsonl",
		"control/prompts/current_task.md",
	] + [str(x) for x in task.get("inputs", [])]

	planned_modify = [str(x) for x in task.get("deliverables", [])] + [
		"control/preflight_report.json",
		"control/validation_result.json",
		"control/adjudication.json",
		"control/system_state.json",
		"control/task_queue.json",
		"control/run_log.jsonl",
	]

	changed_before = sorted(_git_changed_files())
	scope_hash_before = _scope_hash_snapshot(allowed)

	task_status = str(task.get("status", "")).upper()
	risk_flags: list[str] = []
	if task_status not in {"READY", "IN_PROGRESS"}:
		risk_flags.append(f"active_task_status_not_ready:{task_status}")
	if not dep["all_met"]:
		risk_flags.append("dependency_not_met")

	report = {
		"protocol": "RCP",
		"protocol_version": task.get("protocol_version", "RCP_V1"),
		"stage": 2,
		"generated_at": _utc_now(),
		"task_id": task.get("id"),
		"component": task.get("component"),
		"intervention_class": task.get("intervention_class", ""),
		"research_layer": task.get("research_layer", ""),
		"champion_reference": _as_list(task.get("champion_reference")),
		"expected_signature": task.get("expected_signature", {}),
		"reverse_engineering_outputs": _as_list(task.get("reverse_engineering_outputs")),
		"ablatable_components": _as_list(task.get("ablatable_components")),
		"files_planned_to_read": sorted(set(planned_reads)),
		"files_planned_to_modify": sorted(set(planned_modify)),
		"files_promised_not_to_modify": [
			"all files outside task.allowed_files",
			baseline if baseline else "baseline_config_not_set",
		],
		"allowed_files": allowed,
		"dependency_check": dep,
		"risk_check": {
			"has_risk": len(risk_flags) > 0,
			"flags": risk_flags,
		},
		"success_criteria": list(task.get("done_when", [])),
		"validation_commands": list(task.get("validation_commands", [])),
		"scope_hash": {
			"algorithm": "sha256",
			"changed_files_before": changed_before,
			"allowed_snapshot_before": scope_hash_before,
		},
	}

	save_json("preflight_report.json", report)
	append_run_log(str(task.get("id", "UNKNOWN")), "PREFLIGHT_WRITTEN", "Wrote Stage 2 preflight report")
	print(json.dumps({"stage": 2, "status": "OK", "task_id": task.get("id")}, indent=2))
	return 0


def validate_task(task_id: str | None = None) -> int:
	stage0(create_missing=True)
	queue = load_json("task_queue.json")
	rules = load_json("validation_rules.json")
	task = _normalize_task(get_task(queue, task_id)) if task_id else _normalize_task(get_active_task(queue))
	preflight = load_json("preflight_report.json")
	if str(preflight.get("task_id", "")) != str(task.get("id", "")):
		msg = "preflight_report.json task_id mismatch or missing preflight for active task"
		print(json.dumps({"stage": 4, "status": "FAILED", "error": msg}, indent=2))
		return 1

	commands = list(task.get("validation_commands", []))
	command_results: list[dict[str, Any]] = []
	failures: list[dict[str, Any]] = []
	failures.extend(_task_schema_failures(task, rules))
	failures.extend(_dead_path_gate_failures(task))

	for command in commands:
		proc = subprocess.run(command, shell=True, cwd=REPO_ROOT, capture_output=True, text=True)
		row = {
			"command": command,
			"returncode": proc.returncode,
			"pass": proc.returncode == 0,
			"stdout": proc.stdout.strip(),
			"stderr": proc.stderr.strip(),
		}
		command_results.append(row)
		if proc.returncode != 0:
			failures.append({"type": "validation_command_failed", **row})

	produced: list[str] = []
	missing: list[str] = []
	for item in task.get("deliverables", []):
		path = (REPO_ROOT / item).resolve() if not Path(item).is_absolute() else Path(item).resolve()
		rel = path.relative_to(REPO_ROOT).as_posix() if str(path).startswith(str(REPO_ROOT)) else str(path)
		if path.exists():
			produced.append(rel)
		else:
			missing.append(rel)

	changed_before = set(preflight.get("scope_hash", {}).get("changed_files_before", []))
	changed_after = _git_changed_files()
	newly_changed = sorted(changed_after - changed_before)
	allowed_patterns = list(preflight.get("allowed_files", []))
	unexpected = [p for p in newly_changed if not _matches_allowed(p, allowed_patterns)]
	if unexpected:
		failures.append({"type": "scope_violation", "unexpected_files": unexpected})

	baseline_config = str(load_json("system_state.json").get("baseline_config", "")).strip()
	baseline_touched = baseline_config in newly_changed if baseline_config else False
	if baseline_touched:
		failures.append({"type": "baseline_touched", "baseline_config": baseline_config})

	if _protocol_is_hard(task):
		post_run = task.get("post_run_classification", {})
		if isinstance(post_run, dict) and post_run:
			artifact = str(post_run.get("artifact", "")).strip()
			required_fields = _as_list(post_run.get("required_fields"))
			if artifact:
				payload = _load_json_file(artifact)
				if payload is None:
					failures.append({"type": "post_run_classification_missing_artifact", "artifact": artifact})
				else:
					for field in required_fields:
						if payload.get(field) in (None, "", [], {}):
							failures.append(
								{
									"type": "post_run_classification_missing_field",
									"artifact": artifact,
									"field": field,
								}
							)
					confidence = payload.get("confidence")
					if confidence is not None and not (isinstance(confidence, (int, float)) and 0.0 <= float(confidence) <= 1.0):
						failures.append(
							{
								"type": "post_run_classification_invalid_confidence",
								"artifact": artifact,
								"value": confidence,
							}
						)

	if _requires_full_evidence_pack(task, rules):
		required_artifacts = _as_list(rules.get("required_result_pack_artifacts"))
		required_sections = rules.get("required_result_pack_sections", {}) or {}
		for artifact in required_artifacts:
			payload = _load_json_file(artifact)
			if payload is None:
				failures.append({"type": "result_pack_missing_artifact", "artifact": artifact})
				continue
			sections = _as_list(required_sections.get(artifact))
			for section in sections:
				if payload.get(section) in (None, "", [], {}):
					failures.append(
						{
							"type": "result_pack_missing_section",
							"artifact": artifact,
							"section": section,
						}
					)

	success = len(failures) == 0 and len(missing) == 0

	report = {
		"protocol": "RCP",
		"protocol_version": task.get("protocol_version", "RCP_V1"),
		"stage": 4,
		"generated_at": _utc_now(),
		"task_id": task.get("id"),
		"commands_run": command_results,
		"artifacts_produced": produced,
		"missing_artifacts": missing,
		"changed_files_new": newly_changed,
		"scope_violation": unexpected,
		"baseline_touched": baseline_touched,
		"success_criteria": list(task.get("done_when", [])),
		"success_criteria_met": success,
		"status": "PASS" if success else "FAIL",
		"failures": failures,
	}
	save_json("validation_result.json", report)
	append_run_log(str(task.get("id", "UNKNOWN")), "VALIDATION_WRITTEN", "Wrote Stage 4 validation result", {"status": report["status"]})
	print(json.dumps({"stage": 4, "status": report["status"], "task_id": task.get("id")}, indent=2))
	return 0 if success else 1


def write_adjudication(verdict: str, why: str, evidence: list[str], next_task: str, do_not_try: list[str]) -> int:
	stage0(create_missing=True)
	queue = load_json("task_queue.json")
	task = _normalize_task(get_active_task(queue))
	normalized = str(verdict).strip().upper()
	if normalized not in MECHANICAL_VERDICTS:
		print(json.dumps({"stage": 5, "status": "FAILED", "error": f"invalid verdict: {verdict}"}, indent=2))
		return 1

	val = load_json("validation_result.json")
	if str(val.get("task_id", "")) != str(task.get("id", "")):
		print(json.dumps({"stage": 5, "status": "FAILED", "error": "validation_result task mismatch"}, indent=2))
		return 1

	payload = {
		"protocol": "RCP",
		"stage": 5,
		"generated_at": _utc_now(),
		"task_id": task.get("id"),
		"verdict": normalized,
		"why": why,
		"evidence": evidence,
		"next_recommended_task": next_task,
		"do_not_try_next": do_not_try,
	}
	save_json("adjudication.json", payload)
	append_run_log(str(task.get("id", "UNKNOWN")), "ADJUDICATION_WRITTEN", f"Verdict={normalized}")
	print(json.dumps({"stage": 5, "status": "OK", "verdict": normalized}, indent=2))
	return 0


def _dependencies_met_for_task(task: dict[str, Any], queue: dict[str, Any], plan: dict[str, Any]) -> bool:
	return _dependency_check(task, queue, plan)["all_met"]


def _find_next_dependency_safe_task(queue: dict[str, Any], plan: dict[str, Any], current_task_id: str) -> str | None:
	start = _task_index(queue, current_task_id) + 1
	for task in queue.get("tasks", [])[start:]:
		status = str(task.get("status", "")).upper()
		if status in {"READY", "BLOCKED", "NO_OP", "REJECTED"} and _dependencies_met_for_task(task, queue, plan):
			return str(task.get("id", ""))
	return None


def persist_iteration() -> int:
	stage0(create_missing=True)
	queue = load_json("task_queue.json")
	plan = load_json("execution_plan.json")
	state = load_json("system_state.json")
	task = _normalize_task(get_active_task(queue))
	task_id = str(task.get("id", ""))

	val = load_json("validation_result.json")
	adj = load_json("adjudication.json")

	if str(val.get("task_id", "")) != task_id or str(adj.get("task_id", "")) != task_id:
		print(json.dumps({"stage": 6, "status": "FAILED", "error": "validation/adjudication task mismatch"}, indent=2))
		return 1

	verdict = str(adj.get("verdict", "")).upper().strip()
	if verdict not in MECHANICAL_VERDICTS:
		print(json.dumps({"stage": 6, "status": "FAILED", "error": "invalid adjudication verdict"}, indent=2))
		return 1

	baseline_touched = bool(val.get("baseline_touched", False))
	if baseline_touched and verdict != "PROMOTE":
		print(json.dumps({"stage": 6, "status": "FAILED", "error": "baseline touched without PROMOTE verdict"}, indent=2))
		return 1

	task_status_update = {
		"PROMOTE": "DONE",
		"REJECT": "REJECTED",
		"NO_OP": "NO_OP",
		"INCOMPLETE": "IN_PROGRESS",
		"BLOCKED": "BLOCKED",
	}[verdict]
	task["status"] = task_status_update

	next_active = task_id
	if verdict == "PROMOTE":
		next_id = _find_next_dependency_safe_task(queue, plan, task_id)
		if next_id:
			next_active = next_id
			next_task = get_task(queue, next_id)
			if str(next_task.get("status", "")).upper() == "BLOCKED":
				next_task["status"] = "READY"
	elif verdict == "REJECT":
		fallback = str(task.get("on_reject_task_id", "")).strip()
		if fallback:
			next_active = fallback
	elif verdict == "NO_OP":
		nxt = str(task.get("on_noop_task_id", "")).strip()
		if nxt:
			next_active = nxt
	elif verdict == "BLOCKED":
		nxt = str(task.get("on_blocked_task_id", "")).strip()
		if nxt:
			next_active = nxt

	queue["active_task_id"] = next_active
	save_json("task_queue.json", queue)

	state["active_task_id"] = next_active
	state["last_adjudication"] = {
		"task_id": task_id,
		"verdict": verdict,
		"ts": _utc_now(),
		"why": str(adj.get("why", "")),
	}
	state["last_updated_by"] = "rcp-persist"
	save_json("system_state.json", state)

	append_run_log(task_id, "STATE_PERSISTED", f"Stage 6 persisted with verdict={verdict}", {"next_active_task_id": next_active})
	render_current_task()
	print(json.dumps({"stage": 6, "status": "OK", "task_id": task_id, "verdict": verdict, "next_active_task_id": next_active}, indent=2))
	return 0


def status() -> int:
	stage0(create_missing=True)
	queue = load_json("task_queue.json")
	task = _normalize_task(get_active_task(queue))
	print(
		json.dumps(
			{
				"protocol": "RCP",
				"active_task_id": queue.get("active_task_id"),
				"active_task_title": task.get("title"),
				"active_task_status": task.get("status"),
				"component": task.get("component"),
			},
			indent=2,
		)
	)
	return 0


def parse_csv_list(value: str) -> list[str]:
	if not value.strip():
		return []
	return [item.strip() for item in value.split(",") if item.strip()]


def main(argv: list[str] | None = None) -> int:
	parser = argparse.ArgumentParser(description="Repo Control Protocol (RCP) orchestrator")
	sub = parser.add_subparsers(dest="command", required=True)

	stg0 = sub.add_parser("stage0", help="Stage 0: load/check required control files")
	stg0.add_argument("--no-create", action="store_true", help="Fail if missing files are found")

	sub.add_parser("render", help="Render active task prompt")
	sub.add_parser("status", help="Print current active task status")
	sub.add_parser("preflight", help="Stage 2: write control/preflight_report.json")

	v = sub.add_parser("validate", help="Stage 4: run task validation and write control/validation_result.json")
	v.add_argument("--task-id", default=None)

	a = sub.add_parser("adjudicate", help="Stage 5: write control/adjudication.json")
	a.add_argument("--verdict", required=True, choices=sorted(MECHANICAL_VERDICTS))
	a.add_argument("--why", required=True)
	a.add_argument("--evidence", default="")
	a.add_argument("--next-task", default="")
	a.add_argument("--do-not-try", default="")

	sub.add_parser("persist", help="Stage 6: persist adjudication into state and queue")

	args = parser.parse_args(argv)

	if args.command == "stage0":
		return stage0(create_missing=not args.no_create)
	if args.command == "render":
		path = render_current_task()
		print(str(path))
		return 0
	if args.command == "status":
		return status()
	if args.command == "preflight":
		return write_preflight_report()
	if args.command == "validate":
		return validate_task(args.task_id)
	if args.command == "adjudicate":
		return write_adjudication(
			args.verdict,
			args.why,
			parse_csv_list(args.evidence),
			args.next_task,
			parse_csv_list(args.do_not_try),
		)
	if args.command == "persist":
		return persist_iteration()

	return 1


if __name__ == "__main__":
	raise SystemExit(main())
