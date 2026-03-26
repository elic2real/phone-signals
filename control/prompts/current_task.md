# Active Task: TASK-016

Title: Post-implementation raw-core firehose validation
Component: aee_v3_validation
Status: DONE
Protocol Version: RCP_V2
Intervention Class: ABLATION
Research Layer: AEE_INTERACTION

## Protocol
- You are not allowed to operate directly from chat intent.
- You must operate through the Repo Control Protocol (RCP).
- Read only the active task and stay in task scope.

## Rules
- One active task only
- One component per iteration
- No tuning during structural rebuild tasks
- No AEE edits during entry tasks unless task explicitly allows it
- No baseline replacement without PROMOTE verdict
- Every iteration must write control/preflight_report.json
- Every iteration must write control/validation_result.json
- Every iteration must write control/adjudication.json
- Every RCP_V2 task must declare intervention class, research layer, champion reference, expected signature, and reverse-engineering outputs
- Broad concept failure blocks tuning and escalates to upstream layer audit
- Parallel form search precedes local refinement
- Extraction efficiency and entry-only vs realized capture must be measurable where applicable
- Hard-mode tasks must provide variable_contract, falsifiable_signature, min_effect_size, and post_run_classification
- Hard-mode tasks must pass layer-metric-lock and parallel-isolation checks
- Primary edge protection and regression dominance rules are mandatory in hard mode

## Inputs
- aee_family_state_machine_v3.py
- strategy_performance_report_raw.json
- control/simulation_truth_anchor_metrics_report.json
- run_multi_strategy_firehose.py

## Champion Reference
- strategy_performance_report_raw.json

## Expected Signature
- trade_count: stable
- trades_per_hour: stable
- avg_pips_per_trade: improve on non-breakout families
- net_pips_per_hour: improve
- entry_only_vs_realized_gap: shrink

## Reverse Engineering Outputs
- control/degradation_waterfall_template.json
- control/family_confusion_report_template.json
- control/expected_vs_actual_signature_template.json

## Ablatable Components
- aee_v2_vs_v3
- family_policy_sets

## Allowed Files
- control/*
- strategy_performance_report_raw.json
- run_multi_strategy_firehose.py
- run_aee_band_floor_baseline.py

## Deliverables
- control/aee_v3_raw_core_firehose_report.json
- control/aee_v3_vs_baseline_comparison.json

## Validation Commands
- python3 -m py_compile run_multi_strategy_firehose.py

## Done When
- raw-core v3 run report exists
- comparison against raw-core baseline exists
- breakout net pph and family gap deltas are explicitly reported

## Fail Conditions
- missing either required deliverable
- no baseline comparison
- no gap or extraction delta reporting

## Output Requirements
- must_produce:
- control/preflight_report.json
- control/validation_result.json
- control/adjudication.json
- control/aee_v3_vs_baseline_comparison.json

## Dependency Check
- all_met: True
