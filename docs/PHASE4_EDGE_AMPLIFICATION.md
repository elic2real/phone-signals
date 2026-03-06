# Phase 4: Edge Amplification (Historical-Only)

Prerequisite: baseline manifest exists and decision is PASS.

## Baseline

- `proof_artifacts/LANE_A_BASELINE_V1_MANIFEST.json`
- `proof_artifacts/LANE_A_STABILITY_4SLICE.json`
- active patch snapshot: `calibration/tune_map_patch_active_v1_*.json`

## Objective

Improve residual weak states only (do not re-sweep everything).

## Step 1: Build residual state list

Residual = states with `n > 0` and either:
- `delta_expected_extraction_atr <= 0`, or
- `delta_capture_to_ceiling < 0.02`

Use latest slice artifacts:
- `proof_artifacts/LANE_A_S1_PATCH.json`
- `proof_artifacts/LANE_A_S2_PATCH.json`
- `proof_artifacts/LANE_A_S3_PATCH.json`
- `proof_artifacts/LANE_A_S4_PATCH.json`

## Step 2: Candidate generation

Generate 3-6 candidate narrow patches targeted to residual clusters:
- by `pair+session` first
- then by `pair+session+quarter`

Keep patch count small and sparse.

## Step 3: Cached sweep

Evaluate candidates with:
- `tools/policy_sweep_cached.py`

Rank by:
- `delta_expected_extraction_atr`
- `delta_capture_to_ceiling`

## Step 4: Promotion gate (historical only)

Require 4-slice pass using same thresholds from baseline stabilization.

## Step 5: Merge on pass

If candidate passes:
- merge into active patch
- snapshot `calibration/tune_map_patch_active_v2_*.json`
- write new manifest

