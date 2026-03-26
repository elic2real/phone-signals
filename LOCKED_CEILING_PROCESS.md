# Locked Ceiling Process

## Objective
Build a deterministic, repeatable process that converts a locked real dataset into:
- opportunity map
- executable clusters
- entry windows
- labeled opportunity surface
- separability proof
- ODM ceiling
- stream-centered trigger machine
- context-aware trigger machine
- point-trajectory-aware trigger machine

Current highest verified objective:
- maximize verified `pips/hour`
- convert to estimated `equity/hour` using `2.5` pip risk basis and `2%` risk

Current champion:
- island-specific regime gate + island-specific point-trajectory compatibility
- `10.6534` pips/hour
- `8.5227%` estimated equity/hour

## Non-Negotiable Rules
- Use a locked dataset.
- Use one canonical payoff engine.
- Never mix benchmark bucketing with quarter-native recompilation.
- Never trust downstream results until upstream invariants pass.
- Record source artifacts for every stage.
- Treat the market as one continuous energy stream, not isolated trades.
- Use future-only fields for ceiling / reverse-engineering analysis only, not as live-safe trigger logic.
- Never start expanded runs from scratch if a closer solved case exists.
- Inheritance protocol is mandatory:
  - more sessions on same pair/day/session start from the last successful smaller-session rules
  - new weekday starts from the closest solved weekday on the same pair/session
  - new pair starts from the closest related solved pair before cold-starting
  - later stages inherit from the last successful settings of the closest related earlier run

## Step 0: Lock Dataset
Input:
- real OANDA minute data only
- pair
- session
- weekday
- session list

Artifacts:
- `dataset_lock*.json`

Required:
- pair, session, weekday, row_count, session_count, source hash

Current locked expansion dataset:
- `EUR_USD`
- `Monday`
- `London`
- `11` sessions
- `5280` rows

## Step 1: Opportunity Mapping
Method:
- run ODE independently per session
- every minute is a candidate origin
- detect first-hit directional opportunities
- aggregate after session-local detection

Artifacts:
- `opportunity_map_raw.csv`
- `opportunity_map_summary.json`
- `opportunity_map_audit.json`

Why:
- avoids false missing-minute failures across session gaps

Current 11-session result:
- directional opportunities: `7660`

## Step 2: Executable Clustering
Method:
- merge overlapping same-direction opportunities within session scope
- no cross-session clustering
- preserve time order and cluster membership lineage

Artifacts:
- `opportunity_clusters.csv`
- `cluster_summary.json`
- `cluster_audit.json`

Current 11-session result:
- executable clusters: `116`

## Step 3: Entry Windows
Method:
- derive valid entry timestamps from cluster-linked directional rows
- require cluster mapping for every entry window row
- no free-floating entry states

Artifacts:
- `entry_window_states.csv`
- `entry_window_summary.json`
- `entry_window_audit.json`

Current 11-session result:
- valid entry states: `5193`

## Step 4: OAE / Labels
Method:
- use one deterministic quantile-based label path
- no collapsed legacy branch
- label all directional rows as `GOOD/BAD/NOISE`

Artifacts:
- `opportunity_zones_labeled.csv`
- `zone_label_summary.json`
- `zone_label_audit.json`

Current 11-session result:
- `GOOD = 1913`
- `BAD = 2681`
- `NOISE = 3066`

## Step 5: Separability
Method:
- compute `GOOD` vs `BAD` feature separability from the same stage-4 labeled CSV
- do not use stale or alternate branch files

Artifacts:
- `zone_label_separability.json`

Current 11-session result:
- `PASS`

## Step 6: ODM / Ceiling Substrate
Method:
- convert directional labels to cluster-resolved labels
- compute movement supply and extraction ceiling from executable clusters

Artifacts:
- `odm_ceiling_report.json`
- `odm_audit.json`
- `cluster_resolved_labels.csv`

Current 11-session result:
- executable GOOD clusters: `76`
- theoretical pips/hour ceiling: `7.1171`

## Step 7A: Benchmark Entry Work
Method:
- do not confuse:
  - benchmark trade bucketing
  - quarter-native reselection
- benchmark existing selected trades by time only
- reconcile totals exactly

Artifacts:
- `session_trades.csv`
- `quarter_bucketed_trades.csv`
- `quarter_reconciliation_report.json`

Invariant:
- same trade, same timestamp, same payoff everywhere

## Inheritance Protocol
Method:
- seed every expanded compiler run from the closest related successful settings
- compile from the seeded settings, then adapt deterministically to the larger sample
- always record:
  - seed source artifact
  - seed hash
  - inherited vs compiled-end comparison

Examples:
- 1 session -> 11 sessions on same `EUR_USD / Monday / London`:
  - AEE stage seeds from 1-session AEE rules
- Monday -> Tuesday:
  - start from Monday rules on the same pair/session
- `EUR_USD` -> `GBP_USD`:
  - start from closest related USD-pair rules

## Step 7B: Canonical Outcome Layer
Method:
- unify harvester and runner payoff math
- compile outcomes from the exact selected replay engine
- audit every shared `cluster_id + timestamp (+ mode)` row

Artifacts:
- `entry_outcomes.csv`
- `entry_outcomes_consistency_audit.json`

Invariant:
- `mismatch_count = 0`

## Step 7C: Honest Entry Protocol
Method:
- freeze truth rows
- run separability on timestamp-level candidate states
- test descriptor families
- verify whether pre-entry descriptors alone recover edge honestly

Artifacts:
- `entry_state_truth_table.csv`
- `feature_separability_report.json`
- `interaction_separability_report.json`
- `reduced_feature_set.json`
- `entry_static_replay_report.json`
- `entry_surface_stability_report.json`
- `entry_ceiling_verification.json`

What we learned:
- snapshot-only pre-entry descriptor families were too weak
- protocol was honest, fitter was not the main bottleneck

## Step 8: Continuous Session Stream
Method:
- stop treating trades as isolated objects
- model the full session as one energy stream
- create one row per timestamp and assumed direction
- attach action truth over the continuous stream

Artifacts:
- `session_energy_state_stream.csv`
- `state_action_truth_table.csv`
- `state_transition_report.json`
- `unified_action_surface.json`

Why:
- entry and AEE are decisions on the same stream

## Step 9: Stream Trigger Machine
Method:
- cluster `ENTER/HOLD` truth rows into path classes
- build quarter-aware trigger islands
- replay on the full stream
- keep profitable islands only

Artifacts:
- `path_class_clusters.json`
- `entry_trigger_state_machine.json`
- `entry_trigger_replay_report.json`
- `entry_trigger_population.csv`

Current raw full-blend trigger machine:
- trades: `1241`
- win rate: `57.94%`
- expectancy: `0.4492`
- pips/hour: `6.3352`
- estimated equity/hour: `5.0682%`

## Step 10: Split by Contribution
Method:
- split trigger machine into:
  - core
  - expansion
  - research
- then compute island-level marginal contribution

Artifacts:
- `entry_ruleset_split_report.json`
- `core_ruleset.json`
- `expansion_ruleset.json`
- `research_ruleset.json`
- `island_marginal_contribution_report.json`

Why:
- purity is diagnostic
- throughput is objective
- identify hitchhiker islands and throughput engines

## Step 11: Energy Context Engine
Method:
- compute rolling market context vector:
  - macro direction
  - micro direction
  - compression
  - release quality
  - exhaustion
  - noise
  - remaining budget
- classify energy regimes

Artifacts:
- `session_energy_context_stream.csv`
- `energy_context_report.json`
- `island_energy_context_audit.json`
- `full_stream_regimes.csv`
- `selected_trade_regimes.csv`
- `island_regime_matrix.csv`
- `energy_regime_report.json`

Important lesson:
- blanket hard global context gate killed throughput
- context must be used as island-specific regime compatibility

## Step 12: Island-Specific Regime Compatibility
Method:
- assign allowed regimes per island from the compatibility matrix
- gate each island by its own allowed regime set
- replay against full-blend baseline

Artifacts:
- `apply_island_regime_gate.py`
- `island_regime_gate_report.json`

Current regime-gated champion:
- trades: `989`
- win rate: `62.29%`
- expectancy: `0.6800`
- pips/hour: `7.6420`
- estimated equity/hour: `6.1136%`

Why it worked:
- context is real signal
- universal fresh-only gate was wrong
- island-specific regime compatibility was correct

## Step 13: Point-Centered Energy Trajectory Layer
Method:
- go below trade buckets and regime buckets
- compute point-level trajectory around each timestamp:
  - pre-build slope
  - pre-build accel
  - compression-release delta
  - macro/micro alignment
  - release-to-exhaustion delta
  - continuation persistence
  - noise rise
  - exhaustion rise
  - budget decay

Artifacts:
- `point_energy_trajectory.csv`
- `point_energy_transition_report.json`
- `point_trigger_curvature_report.json`

Important lesson:
- universal hard point gate was too strict and killed all trades
- point trajectories must be applied softly per island

## Step 14: Island-Specific Point-Trajectory Compatibility
Method:
- start from the island-regime-gated champion
- for each island, derive soft trajectory thresholds from its own winning rows
- score compatibility per island instead of using one universal gate
- replay against the regime-gated baseline

Artifacts:
- `apply_island_point_trajectory_gate.py`
- `island_point_trajectory_gate_report.json`

Current top verified champion:
- baseline before point layer:
  - trades: `989`
  - expectancy: `0.6800`
  - pips/hour: `7.6420`
  - estimated equity/hour: `6.1136%`
- after point-trajectory layer:
  - trades: `585`
  - win rate: `80.85%`
  - expectancy: `1.6026`
  - avg `R`: `0.6410`
  - pips/hour: `10.6534`
  - estimated equity/hour: `8.5227%`
  - good capture: `9.79%`
  - bad trigger: `2.03%`
  - noise trigger: `5.79%`

This is the current champion because the objective is:
- highest verified equity/hour

## What Must Stay Locked
- Do not mix benchmark bucketing with quarter-native recompilation.
- Do not trust any outcome layer until consistency audit passes.
- Do not use blanket global gates when island-specific compatibility is required.
- Do not optimize to purity alone when objective is equity/hour.
- Keep:
  - raw full blend
  - regime-gated champion
  - point-trajectory-gated champion
as comparable baselines.

## Champion Baselines
1. Raw full blend
- pips/hour `6.3352`
- equity/hour `5.0682%`

2. Island-specific regime compatibility
- pips/hour `7.6420`
- equity/hour `6.1136%`

3. Island-specific point-trajectory compatibility
- pips/hour `10.6534`
- equity/hour `8.5227%`

Current best:
- `3`

## Step 15: Independent Target-Class Entry Surfaces
Method:
- treat each target as its own independent entry class
- do **not** suppress small targets because larger targets also exist
- do **not** require cross-target nesting for entry selection
- only the target's own conditions may block a trade
- remove hidden quarter-dominance choke points across target classes

Target ladder currently used:
- `1.5`
- `2.5`
- `4.5`
- `6`
- `7`
- `8`
- `9`
- `11`
- `13`
- `15`

Artifacts:
- `target_entry_truth_table.csv`
- `target_entry_classes.json`
- `target_entry_class_report.json`
- `target_entry_class_summary.csv`
- `target_blocker_audit.json`

Important lesson:
- small targets were initially underfiring because of logic conflict, especially quarter-scope narrowing
- after removing that hidden blocker, small targets expanded sharply and behaved more like independent throughput classes
- target classes must be evaluated independently on the same fixed truth sample

## Step 16: Static-Sample / No-Recompile Rule
Method:
- once the target truth sample is compiled, do not keep recompiling the same resolved trades
- reuse the same fixed sample
- `TP_HIT` rows stay fixed
- `SL_HIT` rows stay fixed
- only `TIMEOUT` rows are unresolved populations

Artifacts:
- `compiled_target_timeout_populations_11_sessions/all_timeouts.csv`
- `compiled_target_timeout_populations_11_sessions/timeout_population_report.json`

Important lesson:
- a low TP-hit rate can only coexist with positive `pips/hour` if a large share of non-wins are `TIMEOUT`
- timeout counts must always be exposed explicitly
- never present timeout-heavy classes as if they were pure TP/SL outcome sets

## Step 17: No-Timeout Realistic Entry Surface
Method:
- optimize each `direction x target` class on the same fixed truth sample
- hard constraint: `timeouts = 0`
- objective:
  1. zero timeouts
  2. maximize `pips/hour`
  3. maximize expectancy
  4. maximize TP-hit rate
  5. maximize trade count

Canonical runner:
- `python3 optimize_target_entry_classes_no_timeouts.py`

Automatic compiler runner ending at this stage:
- `python3 run_target_entry_stage_compiler.py`

Canonical outputs:
- `compiled_target_entry_classes_no_timeouts_11_sessions/target_entry_class_report.json`
- `compiled_target_entry_classes_no_timeouts_11_sessions/target_entry_class_summary.csv`
- `compiled_target_entry_classes_no_timeouts_11_sessions/target_entry_classes.json`

Current no-timeout leaders:
- `LONG 2.5`
  - trades `273`
  - `TP_HIT 190`
  - `SL_HIT 83`
  - `TIMEOUT 0`
  - `3.0398` pips/hour
- `SHORT 2.5`
  - trades `200`
  - `TP_HIT 143`
  - `SL_HIT 57`
  - `TIMEOUT 0`
  - `2.4432` pips/hour
- `LONG 1.5`
  - trades `430`
  - `TP_HIT 272`
  - `SL_HIT 158`
  - `TIMEOUT 0`
  - `1.9432` pips/hour
- `SHORT 1.5`
  - trades `345`
  - `TP_HIT 213`
  - `SL_HIT 132`
  - `TIMEOUT 0`
  - `1.3807` pips/hour

Current rule:
- if the sample contains timeouts, it is incomplete for the realistic target-class report
- the no-timeout report is the current realistic entry-only target surface

Compiler terminal stage at this point:
- deterministic stages `1–6`
- target contextual class build
- targeted class rescue for historically quarter-choked classes
- final no-timeout target-class surface

Terminal artifacts:
- `compiled_target_entry_stage_11_sessions/target_stage_manifest.json`
- `compiled_target_entry_stage_11_sessions/target_stage_report.json`
- `compiled_target_entry_stage_11_sessions/target_no_timeouts/target_entry_class_report.json`

## Next-Step Rule
Every new change must beat the current champion on:
- verified `pips/hour`
- estimated `equity/hour`

Or preserve those while materially reducing contamination or improving stability.


## Market Node Compiler

Canonical node runner:
- `python3 run_market_node_compiler.py --dataset-lock <lock.json>`

This runner locks the full node process for one:
- pair
- weekday
- session

It reproduces, in order:
1. deterministic stages 1-6
2. target entry stage
3. no-timeout target entry endpoint
4. trade-type truth build
5. canonical AEE stage
6. fixed-pop target-local AEE
7. exact-class theoretical AEE ceiling pass

Node outputs live under:
- `compiled_market_nodes/<PAIR>__<weekday>__<session>/`

Canonical tracker:
- `python3 build_market_node_tracker.py`

Tracker outputs:
- `market_node_tracker_v2.json`
- `market_node_tracker_v2.csv`

## Expansion Rule

Always start a new node from the closest solved successful node:
1. same pair, nearest weekday, same session
2. else nearest related pair, same weekday, same session
3. else same pair, nearest solved session

Never restart cold if a closer solved node exists.
