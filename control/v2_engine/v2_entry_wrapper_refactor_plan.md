# V2 Entry Wrapper Recovery And Refactor Plan

## Current Read
The strongest surviving testing layer is the `V2 parallel extraction blitz` harness in the sibling repo:

- `C:\Users\mawil\phone_signals\phone-signals\tools\run_v2_parallel_extraction_blitz.py`
- `C:\Users\mawil\phone_signals\phone-signals\control\v2_engine\blitz\...`

`publish-clean` has the cleaned downstream artifacts and code hooks, but not the full saved blitz tree.

## What The Wrapper Was Doing
- Running doctrine-scoped mutation branches against the V2 entry stack.
- Using a shared deterministic harness with fixed IS/OOS windows.
- Evaluating success by doctrine-specific contracts, not only generic Phase 5 gates.
- Saving:
  - branch mutations
  - contract specs and decisions
  - focused Phase 5 rows/reports
  - phase2 runtime snapshots
  - phase4 snapshots
  - portfolio conflict flags

## Surviving Wrapper Contracts
- `C1_FLOW_DRIFT_SHORT`
  - focus: `T6_DECAY_ONLY`
  - mutation: TTL scale increases by route mode
  - result: `FAIL_IS`

- `C2_TRANSITION_RELEASE_SHORT_STANDARD`
  - focus: `T1_CONFIRMATION_VS_DIRECT_ONLY`
  - mutation: route/variant bias toward faster direct-release paths
  - result: `FAIL_IS`

- `C3_OSCILLATION_EDGE_LONG_SCALP`
  - focus: `PHASE2_ADMISSION_PLUS_T2_REGIME_ONLY`
  - mutation: relaxed fragile admission plus regime filter
  - result: `BLOCKED_BY_PHASE2_ADMISSION`

## Immediate Recovery Work
1. Mirror the full `control/v2_engine/blitz` artifact tree from `phone-signals` into `publish-clean` as read-only recovery evidence.
2. Copy or port `tools/run_v2_parallel_extraction_blitz.py` into `publish-clean/tools/` so the wrapper owner script is no longer orphaned.
3. Formalize the wrapper schema into canonical config objects:
   - `phase2_survivor_override`
   - `route_selection`
   - `regime_filter`
   - `phase4_option_mutation`
4. Replace env-only dependence on `V2_BLITZ_CONFIG` with a canonical checked-in config directory under `control/v2_engine/blitz/`.

## Entry Refactor Work After Recovery
1. Repair Tier 0 to match `v2_tier0_recovery_notes.md`:
   - raw movement discovery
   - cost-cover movement filter
   - precondition capture
   - independent market mapping
   - opportunity-fit layer
   - Tier 1 handoff compiler
2. Stop Phase 1 from doing early doctrine naming.
3. Promote global truth from soft modifier to hard doctrine-shaping admission gate.
4. Convert doctrine-local T6 diagnosis into explicit per-doctrine trigger contracts.
5. Promote profitable pocket/window structure into native code rather than passive pruning artifacts.

## Grammar Rebuild Anchors
- `control/v2_engine/v2_tier0_recovery_notes.md`
- `control/v2_engine/phase1/trigger_state_expansion_plan.json`
- `control/v2_engine/phase1/trigger_state_gap_clusters.json`
- `control/v2_engine/phase1/trigger_reject_surface_map.json`
- `control/v2_engine/phase2/phase2_doctrine_operating_model.json`
- `control/v2_engine/tier3/v2_doctrine_layer_contracts.json`
- `control/v2_engine/tier3/v2_tier3_pocket_coverage_report.json`
- `control/v2_engine/v2_blitz_wrapper_recovery_manifest.json`

## Decision Rule
If the mirrored blitz surface proves to be the exact last-used harness, recover it first and port it into canonical config.

If it proves incomplete, use the mirrored artifacts plus the Tier 0 notes as the basis for the full rebuild.
