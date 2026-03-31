# CODESPACES RCP — Post-Consolidation Entry Discovery Scale Plan

**Status**: March 31, 2026  
**IDE**: Codespaces (unified, single active)  
**Branch**: integration/entry-enforcement-sync  
**Data Home**: PC2 (support/storage)  

---

## Overview

This plan outlines the phased scaling of entry discovery across more pairs, sessions, directions, and target buckets while maintaining validator discipline and preventing inflation.

**Key Principle**: Reuse existing discovery code, cache layers, and validators. Scale in small, parallel batches. Validate continuously. No architecture redesign.

---

## Current State (Stage A)

### Coverage by (Pair, Direction)

| Pair | Direction | Buckets | Setups | Triggers | Status |
|------|-----------|---------|--------|----------|--------|
| EUR_USD | LONG | 2, 3, 10 | 3 | 3 | **ESTABLISHED** |
| EUR_USD | SHORT | 2, 3, 5, 10 | 4 | 4 | **ESTABLISHED** |
| AUD_USD | SHORT | 5, 10 | 2 | 2 | **ESTABLISHED** |
| AUD_USD | LONG | — | — | — | **GAP** |

### Viability Analysis (Phases 0-2)

**EUR_USD:**
- Viable buckets SHORT: 2, 3, 5, 8, 10 ✓ (all tested)
- Viable buckets LONG: 2, 3, 10 (buckets 5, 8 not viable)

**AUD_USD:**
- Viable buckets SHORT: 5, 10 (buckets 2, 3, 8 not viable at this moment)
- Viable buckets LONG: 8 only (insufficient for multi-bucket family)

### Phases Completed

- Phase 0 (Business Viability): ✓ All buckets 2-10
- Phase 1 (Path Family): ✓ Stage A pairs/directions
- Phase 2 (Structure Truth): ✓ Stage A pairs/directions
- Phase 3 (Setup Discovery): ✓ Stage A, 9 setups
- Phase 4 (Trigger Discovery): ✓ Stage A, 9 triggers
- Phase 6 (Ceiling): ✓ Stage A

### Enforcement Status

| Gate | Status |
|------|--------|
| Discovery-stage setup validation | ✓ PASS (9/9) |
| Promotion-stage setup validation | ✗ FAIL (0/9 — sample floor) |
| Trigger sibling distinctness | ✓ PASS (0 fake variants) |

---

## Scaling Strategy

### Principle: Staged, Validated Expansion

**Philosophy**: 
- Increase real discovery candidates
- Close obvious coverage gaps
- Maintain validator honesty (no fake inflation)
- Test stability under expanded conditions

**Rules**:
1. Each batch is discrete (separate output path)
2. Each batch runs immediate validation after discovery
3. Batches run independently (can parallelize)
4. Stop immediately if validation errors appear
5. No promotion claims - discovery candidates only

---

## Phase S1: Deepen EUR_USD / London / SHORT

**Goal**: Increase sample support and business complexity for the strongest family.

**Scope**:
- Pair: EUR_USD
- Session: London
- Direction: SHORT
- Buckets: 2, 3, 5, 10 (established), + 8 (investigate)
- Sample strategy: Extend to 60-100 samples per bucket (vs current 50)
- Additional structure variants: Look for sweep vs continuation vs breakout variants

**Outputs** (per bucket):
- business_viability_report.json (extended sample)
- path_family_report.json (deeper family analysis)
- structure_truth.json (additional structure variants)
- setup_truth.json (expanded setup family)
- trigger_truth.json (expanded trigger variants)
- ceiling_report.json (quality metrics on expanded set)

**Validation**:
- ✓ No schema/ownership/dependency errors
- ✓ Setup-phase discovery validation PASS
- ✓ Setup-phase promotion validation (may still FAIL on sample floor)
- ✓ Trigger sibling distinctness PASS (no new fake variants)

**Success Criteria**:
- Setup count increases from 4 to ≥6 (with bucket 8)
- Trigger count increases from 4 to ≥6
- All new triggers pass distinctness check
- No regression in validator results

**Output Path**: `control/batches/eurusd_london_short/`

---

## Phase S2: Fill EUR_USD / London / LONG Gap

**Goal**: Complete the missing LONG-side coverage for strongest pair.

**Scope**:
- Pair: EUR_USD
- Session: London
- Direction: LONG
- Buckets: 2, 3, 10 (established), + 5, 8 (fill gap)
- Sample strategy: Full discovery on 5, 8 (currently marked not viable—retest with extended samples)
- No assumption of LONG/SHORT symmetry

**Outputs** (same structure as S1):
- Expanded setup_truth.json (fill 5, 8 if viable)
- Expanded trigger_truth.json (fill 5, 8 if viable)

**Validation**:
- ✓ Same validation gates as S1
- ✓ Ensure LONG triggers are distinct from SHORT (no accidental sibling collision)

**Success Criteria**:
- Discover at least 1 viable LONG setup for bucket 5 or 8
- Total EUR_USD LONG triggers ≥5
- Validator discipline maintained

**Output Path**: `control/batches/eurusd_london_long/`

---

## Phase S3: Broaden AUD_USD / London / SHORT

**Goal**: Increase coverage of secondary pair.

**Scope**:
- Pair: AUD_USD
- Session: London
- Direction: SHORT
- Buckets: 5, 10 (established), + 2, 3, 8 (fill gaps)
- Sample strategy: Full rediscovery on 2, 3, 8 (currently not viable—extend samples)

**Outputs** (same as S1/S2):
- Expanded viability report
- Expanded setup/trigger truth

**Validation**:
- ✓ Same gates, but with vigilance on fake variants (pairs can mask inflation)

**Success Criteria**:
- Discover at least 1 new viable AUD_USD SHORT setup
- Trigger sibling distinctness remains clean
- No pairing of low-quality duplicates

**Output Path**: `control/batches/audusd_london_short/`

---

## Phase S4: Discover AUD_USD / London / LONG

**Goal**: Close the major LONG/SHORT asymmetry for AUD_USD.

**Scope**:
- Pair: AUD_USD
- Session: London
- Direction: LONG
- Buckets: 2, 3, 5, 8, 10 (full range)
- Sample strategy: Full discovery
- Expect lower density (LONG historically weaker on AUD_USD in first run)

**Outputs** (same structure):
- Full discovery set for LONG

**Validation**:
- ✓ All standard gates
- ✓ Trigger distinctness

**Success Criteria**:
- Discover at least 1 viable AUD_USD LONG setup
- Overall AUD_USD triggers ≥3 for both directions combined

**Output Path**: `control/batches/audusd_london_long/`

---

## Phase S5: Test New Session (IF S1-S4 Stable)

**Scope**: Not yet run—contingent on S1-S4 success.

**Options**:
- **New York**: EUR_USD / AUD_USD on US morning
- **Asia**: EUR_USD / AUD_USD on Asian evening

**Rule**: Only one new session. Test stability under different execution morphology.

Would expand to:
- EUR_USD / NewYork / SHORT & LONG
- AUD_USD / NewYork / SHORT & LONG

---

## Phase S6: Additional Pairs (IF Sessions Stable)

**Candidates** (in order of volume/liquidity):
1. GBP_USD
2. USD_CAD
3. USD_JPY

**Rule**: Add pairs in groups of 2-3 at a time. Do NOT expand session and pair simultaneously if failures become hard to interpret.

---

## Required Outputs Per Batch

Each batch must produce:

**Discovery Artifacts**:
- `business_viability_report.json`
- `path_family_report.json`
- `structure_truth.json`
- `setup_truth.json`
- `trigger_truth.json`
- `ceiling_report.json`

**Validation Outputs**:
- `control/codespaces_enforcement_validation.json`
- `control/setup_phase_reports_discovery/*`
- `control/setup_phase_reports_promotion/*`
- `control/trigger_validation_reports/*`

**Summary Table**:
- Pair, session, direction, buckets covered
- Setup count, trigger count
- Viable business count
- Validator status (PASS/FAIL on each gate)
- Strongest setup by expectancy
- Strongest trigger by quality

---

## Execution Timeline

| Phase | Pair | Session | Direction | Buckets | Status | Target Date |
|-------|------|---------|-----------|---------|--------|-------------|
| S1 | EUR_USD | London | SHORT | 2,3,5,10 (+8) | READY | Immediate |
| S2 | EUR_USD | London | LONG | 2,3,10 (+5,8) | READY | After S1 pass |
| S3 | AUD_USD | London | SHORT | 5,10 (+2,3,8) | READY | After S1 pass |
| S4 | AUD_USD | London | LONG | 2-10 | READY | After S3 pass |
| S5 | EUR/AUD | NewYork | LONG/SHORT | TBD | BLOCKED | After S4 pass |
| S6 | GBP/USD/CAD | London | LONG/SHORT | TBD | BLOCKED | After S5 pass |

---

## Validation Checkpoints

**After Each Batch**:
1. ✓ Schema/ownership/dependency: ZERO errors
2. ✓ Setup discovery validation: PASS
3. ✓ Trigger sibling distinctness: PASS (zero fake variants)
4. ✓ No setup inflation without real business
5. ✓ No trigger family explosion

**Cumulative Checkpoints**:
- After S1 + S2 + S3: EUR_USD stable, AUD_USD SHORT stable
- Before S4: Decide if LONG discovery for AUD_USD is necessary
- Before S5: Confirm all London batches maintain validator discipline
- Before S6: Confirm new session (NY/Asia) doesn't regress existing

---

## Stop Conditions

**Immediate stop if**:
1. Schema/ownership/dependency errors reappear
2. Trigger sibling distinctness fails
3. Setup inflation without real new business (e.g., 20 setups from 2 viable businesses)
4. Path families become random/noisy
5. Discovered domains all negative expectancy

---

## Data Availability Note

**Current Constraint**: Raw phase1 data (compiled_nodes) is stored on PC2 machine. Codespaces has:
- ✓ Discovery code and phases
- ✓ Stage A outputs
- ✓ Enforcement validators
- ✓ Cached templates

**For Phase S1-S4**: Requires access to raw phase1 data from PC2, OR reuse existing viability reports and extend phases 3-6.

**Action**: Confirm data availability before running batches. May require:
1. Syncing compiled_nodes from PC2, OR
2. Using Stage A viability as baseline and extending with phase 3-6 logic

---

## Success Definition

This RCP succeeds if:
1. Codespaces remains single active IDE
2. Scaled batches run without infrastructure drift
3. Discovery validation stays clean
4. Promotion validation remains honest
5. Trigger distinctness gate stays green
6. Real discovery candidates increase without family inflation

**Target**: Increase Stage A candidate count from 6 unique (pair, direction) + 9 triggers to ≥4 new domains with ≥5 new high-quality triggers, all passing validation.

