# Lane B Task (Start Now)

Owner: external-ai  
Priority: High  
Do not edit Lane A files.

## Goal
Increase live observability quality and supervised dataset readiness without changing mapping/calibration internals.

## Files (Lane B only)
- `phone_bot.py`
- `tools/run_baseline_tune_probe.py`
- `tools/funnel_report.py`
- `tools/trade_rollback.py`

## Required changes

1. Add `orders_sent_per_h` and `fills_per_h` to probe artifact output.
2. Add hold-time quantiles to funnel output:
   - `median_hold_sec`, `p75_hold_sec`, `p90_hold_sec`.
3. Add top 10 entry block reasons from `ENTRY_GATE_EVAL`.
4. Add top 10 exit reasons from `EXIT_RESULT`.
5. Ensure teacher dataset monitor reports:
   - `state_complete_ok_rate`
   - `teacher_emit_skipped_incomplete_count`
   - `teacher_emit_emitted_count`

## Artifacts
- `proof_artifacts/LANE_B_FUNNEL_*.json`
- `proof_artifacts/LANE_B_PROBE_*.json`

## Acceptance

Run:

```bash
python3 tools/run_baseline_tune_probe.py --run-minutes 10 --artifact proof_artifacts/LANE_B_PROBE_10M.json
python3 tools/funnel_report.py --log logs/trades.jsonl > proof_artifacts/LANE_B_FUNNEL_10M.json
```

Must include all fields listed above.
