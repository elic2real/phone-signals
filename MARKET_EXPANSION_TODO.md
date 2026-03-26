# Market Expansion Todo

Purpose: turn the calibrated EUR/USD engine into a repeatable ceiling compiler for all 15 pairs, all weekdays, all sessions, and all quarters.

Status key:
- `[ ]` not started
- `[-]` in progress
- `[x]` complete

## Phase 0: Lock The Rapid Ceiling Compiler

### 0.1 Compiler structure
- [ ] Create `price_extractor.py`
- [ ] Create `opportunity_mapper.py`
- [ ] Create `cluster_compiler.py`
- [ ] Create `entry_window_compiler.py`
- [ ] Create `scenario_library_builder.py`
- [ ] Create `entry_ceiling_compiler.py`
- [ ] Create `aee_ceiling_compiler.py`
- [ ] Create `quarter_expander.py`
- [ ] Create `settings_inheritance_engine.py`
- [ ] Create `market_grid_tracker.py`

### 0.2 Compiler contract
- [ ] Define locked compiler inputs: `timestamp`, `price`, `pair`, `session`, `weekday`
- [ ] Define locked compiler outputs:
  - [ ] `compiled_entry_thresholds.json`
  - [ ] `compiled_partial_runner_thresholds.json`
  - [ ] `compiled_aee_thresholds.json`
  - [ ] `compiled_ceiling_report.json`
  - [ ] `threshold_derivation_report.json`
- [ ] Define compiler run manifest with dataset hash, pair, weekday, session, quarter, source config
- [ ] Make compiler deterministic from locked inputs and config

### 0.3 Compiler derivation logic
- [ ] Replace manual threshold nudges with quantile-based derivation
- [ ] Replace manual scenario boundaries with scenario-distribution boundaries
- [ ] Record threshold origin for every compiled threshold
- [ ] Emit first-pass ceiling without manual rescue tuning
- [ ] Emit pass/fail diagnostics if first-pass ceiling is weak

### 0.4 Compiler validation
- [ ] Reproduce current EUR/USD single-session ceiling from compiler-only run
- [ ] Reproduce current entry ceiling from compiler-only run
- [ ] Reproduce current AEE state-machine performance from compiler-only run
- [ ] Confirm raw price -> ceiling path is fully automated

## Phase 1: EUR/USD Base Pair Ceiling Path

### 1.1 Synthetic shakedown
- [ ] Use synthetic data only to test compiler wiring and runtime
- [ ] Verify opportunity mapping on synthetic data
- [ ] Verify clustering on synthetic data
- [ ] Verify entry-window extraction on synthetic data
- [ ] Verify scenario-library generation on synthetic data
- [ ] Verify compiled outputs emit cleanly on synthetic data

### 1.2 First real session
- [ ] Lock one real EUR/USD London session on one weekday
- [ ] Extract raw price only
- [ ] Run opportunity map
- [ ] Run clustering
- [ ] Run entry-window extraction
- [ ] Run scenario compilation
- [ ] Run compiled entry ceiling
- [ ] Run compiled AEE ceiling
- [ ] Run ceiling replay
- [ ] Emit:
  - [ ] pips/hour ceiling
  - [ ] R/hour ceiling
  - [ ] equity/hour ceiling

### 1.3 Stabilize the base process
- [ ] Ensure repeated runs on the same locked dataset reproduce the same outputs
- [ ] Ensure threshold derivation report explains all thresholds
- [ ] Ensure compiler does not need post-hoc feature additions for this base case

## Phase 2: Quarter-By-Quarter Ceilinging For EUR/USD

### 2.1 Quarter slicing
- [ ] Define quarter boundaries for the London session
- [ ] Add quarter tags to compiler manifests
- [ ] Run quarter-specific opportunity mapping
- [ ] Run quarter-specific clustering
- [ ] Run quarter-specific entry windows
- [ ] Run quarter-specific scenario libraries

### 2.2 Quarter ceilings
- [ ] Compile Q1 EUR/USD London ceiling
- [ ] Compile Q2 EUR/USD London ceiling
- [ ] Compile Q3 EUR/USD London ceiling
- [ ] Compile Q4 EUR/USD London ceiling
- [ ] Emit quarter-specific threshold families
- [ ] Emit quarter-specific entry ceiling reports
- [ ] Emit quarter-specific AEE ceiling reports

### 2.3 Quarter comparison
- [ ] Compare opportunity density by quarter
- [ ] Compare entry thresholds by quarter
- [ ] Compare AEE thresholds by quarter
- [ ] Compare pips/hour, R/hour, and equity/hour by quarter
- [ ] Record quarter-specific inheritance seed settings

## Phase 3: Expand Same Pair, Same Weekday, Same Session

### 3.1 Same-quarter expansion across more sessions
- [ ] Add another EUR/USD London session for the same weekday
- [ ] Run Q1 across multiple same-weekday sessions
- [ ] Run Q2 across multiple same-weekday sessions
- [ ] Run Q3 across multiple same-weekday sessions
- [ ] Run Q4 across multiple same-weekday sessions

### 3.2 Same-weekday stabilization
- [ ] Build quarter-specific stability report for EUR/USD same weekday
- [ ] Detect threshold drift across same-weekday sessions
- [ ] Compile stable starting settings per quarter
- [ ] Lock EUR/USD same-weekday London inheritance seeds

## Phase 4: Expand To Related USD Pairs

### 4.1 Pair expansion order
- [ ] Define the 15-pair universe
- [ ] Lock related-pair transfer order starting from EUR/USD
- [ ] Recommended first expansion set:
  - [ ] `GBPUSD`
  - [ ] `AUDUSD`
  - [ ] `NZDUSD`
  - [ ] `USDCAD`
  - [ ] `USDCHF`
  - [ ] `USDJPY`

### 4.2 Inheritance engine
- [ ] Implement nearest solved case lookup:
  - [ ] same pair, nearest weekday, same session, same quarter
  - [ ] nearest related pair, same weekday, same session, same quarter
  - [ ] same pair, same weekday, neighboring quarter
- [ ] Record inherited source for every new compile
- [ ] Emit inheritance confidence score
- [ ] Emit inherited-start vs compiled-end comparison

### 4.3 Pair-by-pair execution
- [ ] Compile `GBPUSD` for the locked weekday, London, all 4 quarters
- [ ] Compile `AUDUSD` for the locked weekday, London, all 4 quarters
- [ ] Compile `NZDUSD` for the locked weekday, London, all 4 quarters
- [ ] Compile `USDCAD` for the locked weekday, London, all 4 quarters
- [ ] Compile `USDCHF` for the locked weekday, London, all 4 quarters
- [ ] Compile `USDJPY` for the locked weekday, London, all 4 quarters

## Phase 5: Complete All 15 Pairs For One Weekday, London Only

### 5.1 Full pair coverage
- [ ] Compile remaining 9 pairs for the locked weekday, London, all 4 quarters
- [ ] Ensure every pair has quarter-specific ceilings
- [ ] Ensure every pair has inherited starting settings recorded
- [ ] Ensure every pair has final compiled settings recorded

### 5.2 London market grid
- [ ] Build one-weekday London market grid report across all 15 pairs
- [ ] Compare pair opportunity density
- [ ] Compare pair entry ceilings
- [ ] Compare pair AEE ceilings
- [ ] Compare pair pips/hour, R/hour, and equity/hour
- [ ] Rank pairs by ceiling strength

## Phase 6: Fill All Weekdays For London

### 6.1 Weekday expansion order
- [ ] Expand to Tuesday
- [ ] Expand to Wednesday
- [ ] Expand to Thursday
- [ ] Expand to Friday
- [ ] Expand to Monday if not used as the base weekday

### 6.2 Weekday inheritance
- [ ] For every new weekday compile, inherit from nearest solved case
- [ ] Prefer same pair, nearest weekday, same session, same quarter
- [ ] Fall back to related pair if same-pair weekday not solved
- [ ] Record inheritance source for every weekday expansion

### 6.3 London completion
- [ ] Complete all 15 pairs for all weekdays in London Q1
- [ ] Complete all 15 pairs for all weekdays in London Q2
- [ ] Complete all 15 pairs for all weekdays in London Q3
- [ ] Complete all 15 pairs for all weekdays in London Q4
- [ ] Emit full London market grid tracker

## Phase 7: Expand Remaining Sessions

### 7.1 Session order
- [ ] Define remaining sessions after London
- [ ] Expand to New York
- [ ] Expand to Asia
- [ ] Expand to any additional session buckets used by the engine

### 7.2 Session inheritance
- [ ] Inherit from same pair, same weekday, nearest solved session
- [ ] If unavailable, inherit from same pair, same session type, nearest weekday
- [ ] If unavailable, inherit from related pair, same weekday, same session
- [ ] Record every session transfer source

### 7.3 Session completion
- [ ] Complete all 15 pairs for all weekdays in New York Q1-Q4
- [ ] Complete all 15 pairs for all weekdays in Asia Q1-Q4
- [ ] Complete all 15 pairs for all weekdays in all defined session buckets

## Phase 8: Final Market Coverage

### 8.1 Full grid completion
- [ ] Verify all 15 pairs are covered
- [ ] Verify all weekdays are covered
- [ ] Verify all sessions are covered
- [ ] Verify all 4 quarters are covered for every pair/weekday/session node

### 8.2 Final grid artifacts
- [ ] Emit master market grid tracker
- [ ] Emit per-node compiled settings store
- [ ] Emit per-node compiled ceiling store
- [ ] Emit inheritance graph
- [ ] Emit unresolved weak-node report

### 8.3 Maintenance layer
- [ ] Add rolling recalibration support
- [ ] Add nearest solved case refresh logic
- [ ] Add stale-node detection
- [ ] Add recompile queue for weak or drifted nodes

## Tracker Schema Todo

- [ ] Define market grid node key: `pair`, `weekday`, `session`, `quarter`
- [ ] Track dataset size per node
- [ ] Track best entry settings per node
- [ ] Track best AEE settings per node
- [ ] Track compiled ceilings per node
- [ ] Track inherited source node
- [ ] Track inheritance confidence
- [ ] Track last compile time
- [ ] Track drift status
- [ ] Track validation status

## Locked Process Rules

- [ ] Start from raw price only
- [ ] Map opportunities before inventing entry rules
- [ ] Keep overlapping observations
- [ ] Resolve executable clusters
- [ ] Train on entry windows, not single bars
- [ ] Keep decision features separate from future outcome fields
- [ ] Build AEE from winning paths plus reversed bad-trade geometry
- [ ] Share bad-trade intelligence between entry and AEE
- [ ] Treat quarter as a first-class axis
- [ ] Transfer by nearest solved case, never restart from zero without cause

## Immediate Execution Queue

1. [ ] Build `ceiling_compiler.py`
2. [ ] Build `market_grid_tracker.py`
3. [ ] Reproduce the current EUR/USD ceiling via compiler-only run
4. [ ] Add quarter slicing and quarter-specific compiler outputs
5. [ ] Compile EUR/USD Q1-Q4 for the locked London weekday
6. [ ] Add settings inheritance engine
7. [ ] Expand EUR/USD across more same-weekday London sessions
8. [ ] Expand to related USD pairs
9. [ ] Complete all 15 pairs for one weekday in London
10. [ ] Fill the remaining London weekdays
11. [ ] Expand to New York and Asia
12. [ ] Close the full pair-weekday-session-quarter grid
