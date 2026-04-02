# V2 Tier 0 Rebuild Plan

Anchored to:

- [v2_tier0_recovery_notes.md](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/v2_tier0_recovery_notes.md)
- [v2_phase_contracts.json](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/v2_phase_contracts.json)
- [v2_phase1_discovery_map.json](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/phase1/v2_phase1_discovery_map.json)
- [v2_phase1_sample_profiles.json](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/phase1/v2_phase1_sample_profiles.json)
- [trigger_state_expansion_plan.json](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/phase1/trigger_state_expansion_plan.json)
- [trigger_state_gap_clusters.json](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/phase1/trigger_state_gap_clusters.json)
- [trigger_reject_surface_map.json](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/phase1/trigger_reject_surface_map.json)
- [v2_structural_failure_map.json](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/tier3/v2_structural_failure_map.json)
- [v2_blitz_wrapper_recovery_manifest.json](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/v2_blitz_wrapper_recovery_manifest.json)

## Current Read

The current V2 stack is profitable enough to produce useful entry survivors, but it is still upstream of the intended Tier 0 architecture.

What is already present:
- move discovery
- cost-cover logic
- precursor measurement
- structural fields like zone, compression, energy, distance

What is still wrong:
- Phase 1 performs early recognition and doctrine-like interpretation
- Tier 0 mapping is not independent from trade interpretation
- Tier 1 is not the first true interpretation layer
- global truth is still soft-gated downstream rather than hard-gated upstream

## The Core Leak

Phase 1 currently emits interpretation-bearing fields that should not exist yet at Tier 0:

- `pattern_match_state`
- `live_recognition_state`
- `trigger_state`
- `extraction_signature`
- `doctrine_family_id`

These are present in [v2_phase1_physics_engine.py](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/tools/v2_phase1_physics_engine.py) and get propagated into Tier 1 `event_kernel` in [v2_tier1_truth_kernel.py](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/tools/v2_tier1_truth_kernel.py).

That means the current stack is:

`physics + early naming -> truth wrapper -> doctrine formalization`

instead of:

`physics -> independent mapping -> opportunity fit -> handoff -> interpretation`

## Target Tier 0 Structure

### `T0.1` Move Discovery
Question:

`Where did price move?`

Allowed outputs:
- direction
- gross movement
- net movement
- discovered distance

Not allowed:
- doctrine ids
- trigger names
- extraction signatures

### `T0.2` Cost-Cover Layer
Question:

`Which moves clear friction and execution cost?`

Allowed outputs:
- `friction_threshold_pips`
- `usable_available_pips`
- `cost_covering_state`
- `extractable`

### `T0.3` Precondition Capture
Question:

`What existed before the move happened?`

Allowed outputs:
- precursor state
- precursor pressure
- precursor duration
- precursor width
- clean-book and alignment primitives

### `T0.4` Independent Market Mapping
Question:

`What structurally real patterns and containers existed in the same market slice, independent of the event stream?`

This layer must map:
- support
- resistance
- ranges
- zones
- oscillations
- recurring energy patterns
- repeated structural movement forms

This layer must be parallel to move discovery, not a function of whether a trade ended up looking interesting.

### `T0.5` Opportunity Fit
Question:

`Where do the discovered opportunities fit inside the independent market map?`

This is where move discoveries get attached to structural map elements.

### `T0.6` Handoff Compiler
Question:

`What notable and quantifiable facts should Tier 1 receive?`

Tier 0 handoff should contain:
- event truth
- market-map truth
- fit truth
- direction
- discovered distance
- energy and precursor facts

Tier 0 handoff should not contain:
- doctrine labels
- trigger family names
- strategy names
- profitability conclusions

## File Ownership Changes

### Keep in Phase 1
File:
- [v2_phase1_physics_engine.py](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/tools/v2_phase1_physics_engine.py)

Keep:
- sanitizer
- move detection
- cost-cover evaluation
- precursor measurement
- raw structural measurements

Move out of Phase 1:
- `_match_pattern`
- `_trigger_state`
- `_doctrine_family_id`
- `live_recognition_state`
- doctrine-bearing `extraction_signature`

### Split Phase 1 Output
Replace one mixed profile object with two explicit outputs:

1. `event_discovery_rows`
- move, cost, precursor, direction, discovered distance

2. `market_mapping_rows`
- structural map, zones, oscillation state, repeated energy forms, boundaries

Then create:

3. `opportunity_fit_rows`
- link discovered event opportunities to the parallel map

4. `tier0_handoff_rows`
- compiled notable facts for Tier 1

### Tighten Tier 1
File:
- [v2_tier1_truth_kernel.py](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/tools/v2_tier1_truth_kernel.py)

Tier 1 should build kernels only from Tier 0 handoff truth.

Remove from Tier 1 input assumptions:
- `pattern_match_state`
- `live_recognition_state`

Keep `trigger_state` out of Tier 1 until it is created by interpretation logic, not Phase 1 shortcuts.

## Artifact Reclassification

### Use As Salvage Inputs
These are useful, but they are not Tier 0 architecture:

- [trigger_state_expansion_plan.json](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/phase1/trigger_state_expansion_plan.json)
- [trigger_state_gap_clusters.json](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/phase1/trigger_state_gap_clusters.json)
- [trigger_reject_surface_map.json](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/phase1/trigger_reject_surface_map.json)
- [blitz_protocol.json](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/blitz/blitz_protocol.json)
- [v2_blitz_wrapper_recovery_manifest.json](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/v2_blitz_wrapper_recovery_manifest.json)

They tell us:
- which clusters were economically worth covering
- which downstream mutations were attempted
- which doctrine-local windows were promising

They do not justify keeping early naming inside Phase 1.

### Keep As Structural Warning
- [v2_structural_failure_map.json](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/control/v2_engine/tier3/v2_structural_failure_map.json)

This confirms that global truth exists but is not acting as hard doctrine-shaping admission truth.

## Concrete Build Order

### Step 1
Refactor [v2_phase1_physics_engine.py](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/tools/v2_phase1_physics_engine.py) into explicit internal functions:
- `discover_moves`
- `filter_cost_covering_moves`
- `measure_precursors`
- `build_independent_market_map`
- `fit_opportunities_to_map`
- `compile_tier0_handoff`

### Step 2
Strip these fields from canonical Phase 1 output:
- `pattern_match_state`
- `live_recognition_state`
- doctrine-bearing `extraction_signature`
- `doctrine_family_id`

### Step 3
Publish new Phase 1 artifacts:
- `phase1_event_discovery_rows.json`
- `phase1_market_mapping_rows.json`
- `phase1_opportunity_fit_rows.json`
- `phase1_tier0_handoff_rows.json`

### Step 4
Rewrite [v2_tier1_truth_kernel.py](/c:/Users/mawil/phone_signals/phone-signals-publish-clean/tools/v2_tier1_truth_kernel.py) so Tier 1 consumes only `phase1_tier0_handoff_rows.json`.

### Step 5
Move trigger and doctrine creation into Tier 1 and Phase 2, where they belong.

### Step 6
After Tier 0 is clean, re-run the current salvage winners through the repaired stack:
- `COMPRESSION_PRESSURE_LIFT_LONG`
- `PRESSURE_DRIVE_LONG`
- `FLOW_DRIFT_LONG`
- `FLOW_DRIFT_SHORT`
- `COILED_COMPRESSION_LONG`

## New Success Criteria

Tier 0 rebuild is complete only when:

1. Phase 1 no longer emits doctrine-bearing identity.
2. Independent market mapping is published as its own artifact.
3. Opportunity-fit is separate from raw market mapping.
4. Tier 1 is the first layer that interprets Tier 0 truth into reusable trade meaning.
5. Salvaged trigger/doctrine findings can be reintroduced without recontaminating Tier 0.

## Final Read

The current stack is recoverable, but not architecturally correct yet.

The profitable wrapper and doctrine-local artifacts should be treated as salvage memory for thresholds, windows, and doctrine behavior.

They should not be mistaken for proof that the upstream Tier 0 design is already finished.
