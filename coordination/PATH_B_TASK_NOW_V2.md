# Path B Instructions (V2)

Owner: external-ai  
Priority: High  
Do not edit Path A owned files.

## Scope
Live/runtime quality and teacher dataset reliability only.

## Owned files
- `phone_bot.py`
- `entry_logic.py`
- `aee_engine.py`
- `tools/run_baseline_tune_probe.py`
- `tools/funnel_report.py`
- `tools/trade_rollback.py`

## Do not edit
- `tools/state_replay_metrics.py`
- `tools/policy_sweep_cached.py`
- `tools/build_tier0_patch_batch.py`
- `tools/build_tier1_patch_from_cached.py`
- `tools/promotion_gate.py`

## Required deliverables
1. `proof_artifacts/LANE_B_PROBE_10M.json`
2. `proof_artifacts/LANE_B_FUNNEL_10M.json`
3. `proof_artifacts/LANE_B_TEACHER_HEALTH.json`
4. `proof_artifacts/LANE_B_ROLLBACK_10.json`

## Required metrics in outputs
### Probe
- `orders_sent_per_h`
- `fills_per_h`
- `entry_result_per_h`
- `exit_result_per_h`

### Funnel
- `median_hold_sec`, `p75_hold_sec`, `p90_hold_sec`
- top 10 `ENTRY_GATE_EVAL.block_reason`
- top 10 `EXIT_RESULT.exit_reason`

### Teacher health
- `state_complete_ok_rate`
- `teacher_emit_emitted_count`
- `teacher_emit_skipped_incomplete_count`
- heartbeat interval stats per-trade (median, p90)

### Rollback
Each trade row must include:
- `entry_knobs_eff`, `aee_knobs_eff`
- `patch_version`, `manual_version`
- `aee_reason`, `exit_reason`
- `pnl_atr`, `MFE_atr`, `MAE_atr`, `GB`, `hold_sec`

## Run commands
```bash
python3 tools/run_baseline_tune_probe.py --run-minutes 10 --artifact proof_artifacts/LANE_B_PROBE_10M.json
python3 tools/funnel_report.py --log logs/trades.jsonl > proof_artifacts/LANE_B_FUNNEL_10M.json
python3 tools/trade_rollback.py --last 10 --out proof_artifacts/LANE_B_ROLLBACK_10.json
```

## Acceptance
- All four artifacts exist and are non-empty.
- All required fields above are present.
- No edits outside Path B owned files.
