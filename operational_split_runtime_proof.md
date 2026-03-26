# Operational Split Runtime Proof

Generated UTC: `2026-03-16T02:25:06.461476+00:00`
Overall pass: `True`

## Working System Checks
- `entries_detected`: `True`
- `priority_ranking_present`: `True`
- `trades_opened`: `True`
- `enter_to_managing_seen`: `True`
- `aee_managing_actions_seen`: `True`
- `trades_closed`: `True`
- `close_side_effect_seen`: `True`
- `harvester_banked_seen`: `True`
- `account_physics_traces_present`: `True`
- `runner_has_main_pairing`: `True`

## Mapping Split Evidence
- `baseline_compiled_source_present`: `True`

## Counts
- `signal_generated`: `11269`
- `entry_gate_eval`: `5130`
- `trade_attempt`: `5130`
- `entry_result`: `971`
- `exit_result`: `178`
- `harvester_banked`: `86`
- `signal_with_priority_fields`: `11269`
- `signal_total`: `11269`

## Sample Evidence
- `signal_generated`: `logs/trades.jsonl:26`
- `entry_gate_eval_allow`: `logs/trades.jsonl:8`
- `trade_attempt`: `logs/trades.jsonl:9`
- `entry_result_filled`: `logs/trades.jsonl:13`
- `aee_exit_action`: `logs/trades.jsonl.5:295`
- `exit_result`: `logs/trades.jsonl.5:297`
- `pair_close_complete`: `logs/trades.jsonl.5:296`
- `harvester_banked`: `logs/trades.jsonl.5:299`

## Interpretation
- `working_system_operational`: `True`
- `mapping_separate_from_runtime`: `True`
- Note: This verifies observed runtime behavior from production logs. It is operational proof, not a ceiling-optimization proof.
