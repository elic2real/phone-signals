TITLE
PHASE_0_FULL_INSTRUMENTED_BASELINE

OBJECTIVE
Start every future system with:

- full telemetry
- universal analysis capability
- parallel test readiness
- zero blind spots

Eliminate sequential discovery.

CORE PRINCIPLE

Do not build system then analyze.
Build system with full observability so one run supports full diagnosis.

SECTION 1 - FULL TELEMETRY (MANDATORY AT START)

All fields must exist before first run.

1) Candidate-Level Telemetry

- pair
- timestamp
- direction
- session
- candidate_id
- priority_score
- rank
- selected (true/false)
- entry_zone_low
- entry_zone_high

2) Selection-Level Telemetry

Per cycle:

- full ranked list (top N minimum)
- selected candidate(s)
- rejected candidates (top N)

3) Trade Lifecycle Telemetry

Per trade:

- trade_id
- entry_timestamp
- close_timestamp
- trade_life_seconds

4) Profit Path Telemetry

- time_to_first_profit_seconds
- time_in_drawdown_seconds
- max_drawdown
- max_favorable_excursion (MFE)
- current_pnl at each step

5) Timing Telemetry (CRITICAL)

- time_from_entry_to_peak
- time_from_peak_to_close
- time_from_entry_to_close

6) AEE Decision Telemetry

Per decision:

- decision_type (HOLD / CLOSE / PARTIAL)
- reason
- timestamp
- pnl_at_decision

7) Blocker Telemetry

- blocker_reason
- blocker_count
- suppression counts

Pass Criteria:
- all fields present in first run

Fail Condition:
- any missing telemetry -> STOP

SECTION 2 - UNIVERSAL ANALYSIS ENGINE

One script only. It must always output:

A) Candidate Metrics
- candidates per cycle
- A/B/C distribution

B) Rank vs Outcome
- win rate by rank
- avg pnl by rank
- avg trade life by rank

C) Selected vs Rejected
- avg score selected vs rejected
- avg outcome selected vs rejected

D) Trade Life Distributions
- histogram of trade duration
- fast vs slow trade ratios

E) Timing Metrics
- time_to_first_profit distribution
- time_in_drawdown distribution
- peak_to_close delay distribution

F) AEE Behavior
- HOLD / CLOSE / PARTIAL distribution
- decision timing vs outcome

G) Blocker Analysis
- blocker frequency
- missed opportunity estimate

Pass Criteria:
- all outputs generated from one run

Fail Condition:
- multiple scripts required to understand system

SECTION 3 - FAILURE ARCHETYPE MAP (PREDEFINED)

Define before running:

- rank_inversion
- slow_recycler
- prolonged_drawdown
- peak_delay
- dead_on_arrival
- late_entry

Pass Criteria:
- every trade assigned to a failure or success archetype

SECTION 4 - PARALLEL VARIANT SYSTEM

Minimum three variants per run:

- BASELINE
- VARIANT_A
- VARIANT_B

Each variant must:

- run on same data
- produce same telemetry schema
- be directly comparable

Pass Criteria:
- side-by-side comparison possible in one analysis pass

SECTION 5 - MICRO-SLICE TESTING

Before full runs, run on:

- high-volatility slice
- known failure slice
- known success slice

Pass Criteria:
- bad ideas fail in micro-slice

Fail Condition:
- full run required to detect failure

SECTION 6 - KILL RULES (PREDEFINED)

Before running, define:

- if peak delay not reduced -> discard
- if drawdown time not reduced -> discard
- if rank vs outcome not improved -> discard

Pass Criteria:
- bad variants killed immediately

SECTION 7 - BENCHMARK DATASET

Maintain fixed benchmark set:

- top winners
- top losers
- whipsaw events
- drawdown events

Pass Criteria:
- every change tested against benchmark first

SECTION 8 - LOOP STRUCTURE

RUN -> ANALYZE -> DECIDE -> MODIFY -> RE-RUN

Maximum diagnostic depth:
- 2 to 3 analysis passes before intervention

Fail Condition:
- more than three analysis phases without action

SECTION 9 - PROMOTION RULE

Promote only when:

- net positive
- stable across windows
- failure archetype reduced
- no new major regression

SECTION 10 - HARD RULES

- no blind spots
- no silent failures
- no sequential discovery
- no guessing
- no tuning without attribution

FINAL STATE

Phase 0 is complete when one run can fully explain system behavior without additional diagnostic phases.
