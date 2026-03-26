# Entry State Reverse-Engineering Plan

## Core Reframe

The market is one continuous energy stream. Trades are not the primary object.
The primary object is the session state at each timestamp.

We are solving:

- when the stream is in `DO_NOT_ENTER`
- when the stream transitions into `ENTER_LONG` or `ENTER_SHORT`
- when the stream should stay in `HOLD`
- when the stream should `HARVEST`, `EXTEND`, or `PANIC`

## Current Problem

Stage 7 is no longer blocked by:

- accounting
- clustering
- entry-window extraction
- label creation
- label separability
- replay plumbing

Stage 7 is blocked by weak causal state description.

The current past-only descriptor families are too weak to isolate a ceiling-quality
entry surface on the 11-session EUR/USD Monday London truth set.

## Known Variables

These are proven:

- dataset lock is valid
- deterministic stage 1-6 compiler passes
- stage 4 GOOD/BAD/NOISE labels exist
- stage 5 separability passes at the opportunity level
- canonical payoff logic is reconciled
- quarter bucketing is reconciled
- directional bias exists by quarter
- AEE improves once a viable trade population exists

## Unknown Variables

These remain unresolved:

- what exact state transition precedes a GOOD triggerable state
- how much signal exists before entry vs only after early confirmation
- whether one entry surface exists or multiple path-class surfaces are needed
- which parts of the stream matter most:
  - structural context
  - transition dynamics
  - early-trade validation

## First Failing Layer

The first failing layer is the timestamp-level causal state surface.

Evidence:

- stage 7 live-safe protocol is honest
- protocol found weak or no strong-signal features
- broadening descriptor families raised throughput but also BAD/NOISE trigger
- current pre-entry-only state snapshots do not recover the edge cleanly

## New Canonical Object

We will build:

- `session_energy_state_stream.csv`

Each row is one timestamp in one session, for one assumed direction, with:

- timestamp
- session_id
- quarter
- direction_assumed
- structural state descriptors
- transition state descriptors
- current path state descriptors
- future-scoring fields for diagnostics only

## New Canonical Truth Layer

We will build:

- `state_action_truth_table.csv`

Each row will contain the correct action label implied by the stream state:

- `DO_NOT_ENTER`
- `ENTER_LONG`
- `ENTER_SHORT`
- `HOLD_LONG`
- `HOLD_SHORT`
- `HARVEST_LONG`
- `HARVEST_SHORT`
- `EXTEND_LONG`
- `EXTEND_SHORT`
- `PANIC_LONG`
- `PANIC_SHORT`

## Descriptor Families To Build Next

### 1. Structural Context

- session-relative bias
- quarter-relative bias
- local box position
- distance to swing highs/lows
- directional dominance so far in session

### 2. Transition Dynamics

- short vs medium pressure acceleration
- pullback resolution
- reclaim strength
- compression release
- breakout acceptance / failure

### 3. Impulse Geometry

- last impulse size
- pullback depth as fraction of last impulse
- bars since impulse peak
- entry vs impulse origin

### 4. Early-Trade Validation

- first 1-3 bars after trigger
- early MFE / MAE evolution
- opposite-pressure response
- continuation persistence

### 5. Path-Class Context

- breakout continuation
- pullback continuation
- reclaim continuation
- squeeze release
- grind trend continuation

## Deterministic Build Order

1. Build `session_energy_state_stream.csv`
2. Build `state_action_truth_table.csv`
3. Build `state_transition_report.json`
4. Build matched comparisons between:
   - pre-trigger GOOD
   - triggerable GOOD
   - BAD
   - NOISE
5. Cluster GOOD rows into path classes
6. Derive action surfaces from transitions, not static snapshots
7. Replay the derived trigger-state machine causally

## Hard Rules

- no future leakage in decision features
- future fields may only be used for scoring or truth labeling
- no threshold added without a measured state or transition justification
- no new entry logic accepted unless it beats the current protocol baseline on:
  - good capture
  - bad trigger
  - noise trigger
  - expectancy
  - pips/hour

## Acceptance Target

Entry is not considered ceiling-ready until:

- good capture >= 60%
- bad trigger <= 6%
- noise trigger <= 30%
- expectancy > 0
- trade_count >= 1000

## Immediate Next Artifacts

- `session_energy_state_stream.csv`
- `state_action_truth_table.csv`
- `state_transition_report.json`
- `path_class_clusters.json`
- `entry_trigger_state_machine.json`
- `entry_trigger_replay_report.json`

