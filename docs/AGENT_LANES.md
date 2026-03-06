# Agent Lanes (Parallel Execution Contract)

This file prevents overlap when two coding assistants work at the same time.

## Global Rules

1. Do not edit files owned by another lane.
2. Before any edit, check `coordination/ownership.json`.
3. If a shared file must change, open a handoff request in `coordination/handoffs.md` first.
4. Every lane writes artifacts with lane prefix:
   - Lane A: `proof_artifacts/LANE_A_*`
   - Lane B: `proof_artifacts/LANE_B_*`
5. Never rewrite or delete another lane's artifact files.

## Lane A (this assistant)

Scope:
- Historical mapping/calibration engine performance and cache pipeline
- State replay artifacts and promotion reports from cached runs

Owned files:
- `tools/state_replay_metrics.py`
- `tools/policy_sweep_cached.py`
- `tools/build_tier0_patch_batch.py`
- `tools/build_tier1_patch_from_cached.py`
- `tools/promotion_gate.py`
- `proof_artifacts/LANE_A_*`

Current objective:
- Maximize sweep throughput with cache reuse and produce promotable patches.

## Lane B (external assistant)

Scope:
- Runtime/live execution instrumentation and operator tooling
- Teacher/heartbeat data quality and live dashboard/report ergonomics

Owned files:
- `phone_bot.py`
- `entry_logic.py`
- `aee_engine.py`
- `tools/run_baseline_tune_probe.py`
- `tools/funnel_report.py`
- `tools/trade_rollback.py`
- `proof_artifacts/LANE_B_*`

Current objective:
- Improve live funnel observability and produce reliable supervised dataset metrics.

## Shared files (request required)

- `tune_map_generate.py`
- `tune_map.py`
- `tune_apply.py`
- `state_key.py`
- `calibration/tune_map_patch.json`
- `tunes/manual_overrides.json`

Shared-file change flow:
1. Add request to `coordination/handoffs.md` with rationale.
2. Wait for owner acknowledgment.
3. Apply change.
4. Record completion in the same handoff entry.
