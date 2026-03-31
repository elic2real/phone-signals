# Batch Execution Manifest — Entry Discovery Scale Phase 1

**Created**: 2026-03-31  
**Status**: READY TO EXECUTE (pending phase1 data sync from PC2)  
**Duration**: ~2-4 hours per complete batch (with full discovery)  

---

## Command Sequence: Run 1 (EUR_USD / London / SHORT)

### Prerequisites

Ensure phase1 data is available from PC2:

```bash
# On PC2 machine, verify:
ls PC2/compiled_nodes/EUR_USD__Thursday__London/phase1/opportunity_map_raw.csv

# If available, sync to Codespaces:
rsync -av PC2/compiled_nodes/ /codespaces/phone-signals/PC2/compiled_nodes/
```

### Run 1: EUR_USD / London / SHORT (Extend Existing Family)

```bash
cd /workspaces/phone-signals

# Step 1: Run phases 0-2 discovery with extended sample (100 samples vs 50)
python tools/scaled_discovery_batch.py \
  --pair EUR_USD \
  --session London \
  --direction SHORT \
  --buckets 2 3 5 8 10 \
  --output-dir control/batches/eurusd_london_short \
  --sample-size 100 \
  --weekday Thursday

# Step 2: Run phases 3-6 (setup, trigger, ceiling)
# [Would import phase 3-6 functions here]

# Step 3: Run main enforcement validator
python -m codespaces_rcp.validator_runner \
  --report-dir control/batches/eurusd_london_short \
  --schema-dir codespaces_rcp/schemas \
  --out control/batches/eurusd_london_short/codespaces_enforcement_validation.json

# Step 4: Run setup-phase validation (discovery mode)
python enforcement/setup_phase_validation.py \
  --artifact-dir control/batches/eurusd_london_short \
  --output-dir control/batches/eurusd_london_short/setup_phase_reports_discovery \
  --stage discovery \
  --min-sample-size 30 \
  --discovery-sample-floor 15

# Step 5: Run setup-phase validation (promotion mode)
python enforcement/setup_phase_validation.py \
  --artifact-dir control/batches/eurusd_london_short \
  --output-dir control/batches/eurusd_london_short/setup_phase_reports_promotion \
  --stage promotion \
  --min-sample-size 30 \
  --discovery-sample-floor 15

# Step 6: Run trigger distinctness validation
python enforcement/trigger_validation_runner.py \
  --trigger-dir control/batches/eurusd_london_short \
  --schema-dir enforcement/schemas \
  --output-dir control/batches/eurusd_london_short/trigger_validation_reports

# Step 7: Inspect results
python - <<'INSPECT'
import json

batch_key = "eurusd_london_short"
prefix = f"control/batches/{batch_key}"

# Summary table
print("=== BATCH EXECUTION SUMMARY ===\n")
print(f"Batch: {batch_key}")
print(f"EUR_USD / London / SHORT\n")

# Viability
try:
    vr = json.load(open(f"{prefix}/business_viability_report.json"))
    viable_count = sum(1 for r in vr['records'] if r.get('viable'))
    print(f"Viable count: {viable_count}/{len(vr['records'])}")
except:
    print("Viability report: NOT FOUND")

# Setup
try:
    sr = json.load(open(f"{prefix}/setup_truth.json"))
    print(f"Setup count: {len(sr['records'])}")
except:
    print("Setup report: NOT FOUND")

# Trigger
try:
    tr = json.load(open(f"{prefix}/trigger_truth.json"))
    print(f"Trigger count: {len(tr['records'])}")
    
    # Sibling analysis
    siblings = {}
    for t in tr['records']:
        key = (t['pair'], t['session'], t['direction'], t['structure_label'], t['path_family'])
        if key not in siblings:
            siblings[key] = []
        siblings[key].append(t['target_bucket'])
    
    print(f"\nSibling groups: {len(siblings)}")
    for key, buckets in sorted(siblings.items()):
        print(f"  {key[3]:15} {key[4]:12}: buckets {sorted(buckets)}")
except:
    print("Trigger report: NOT FOUND")

# Enforcement
try:
    ev = json.load(open(f"{prefix}/codespaces_enforcement_validation.json"))
    print(f"\nEnforcement status: {ev.get('promotion_gate', {}).get('status')}")
except:
    print("Enforcement report: NOT FOUND")

# Setup validation
try:
    sv = json.load(open(f"{prefix}/setup_phase_reports_discovery/validation_report.json"))
    print(f"Setup discovery validation: {sv.get('status')}")
except:
    print("Setup discovery validation: NOT FOUND")

# Trigger validation
try:
    tv = json.load(open(f"{prefix}/trigger_validation_reports/trigger_validation_report.json"))
    print(f"Trigger validation: {tv.get('validation_status')}")
except:
    print("Trigger validation: NOT FOUND")

print()
INSPECT

# Step 8: Commit results
git add control/batches/eurusd_london_short/
git commit -m "Batch S1: Extend EUR_USD/London/SHORT discovery with bucket 8 and expanded samples"
git push origin integration/entry-enforcement-sync
```

---

## Command Sequence: Run 2 (EUR_USD / London / LONG)

```bash
# Similar structure to Run 1, but for LONG direction
# Replace buckets 2 3 5 8 10 with focus on [5, 8] (the gaps)

python tools/scaled_discovery_batch.py \
  --pair EUR_USD \
  --session London \
  --direction LONG \
  --buckets 2 3 5 8 10 \
  --output-dir control/batches/eurusd_london_long \
  --sample-size 100

# [Repeat validation steps 3-7 for this batch]
```

---

## Command Sequence: Run 3 (AUD_USD / London / SHORT)

```bash
# Extend AUD_USD SHORT to test gaps [2, 3, 8]

python tools/scaled_discovery_batch.py \
  --pair AUD_USD \
  --session London \
  --direction SHORT \
  --buckets 2 3 5 8 10 \
  --output-dir control/batches/audusd_london_short \
  --sample-size 100

# [Repeat validation steps 3-7]
```

---

## Parallel Execution Strategy

Once Run 1 completes validation successfully, Runs 2 and 3 can execute in parallel:

```bash
# Terminal A: Run 2
python tools/scaled_discovery_batch.py [EUR_USD LONG config]

# Terminal B: Run 3
python tools/scaled_discovery_batch.py [AUD_USD SHORT config]

# [Wait for both to complete, then validate both in parallel]
```

---

## Validation Report Generation

After all batches complete, generate comparative summary:

```bash
python control/generate_batch_summary.py \
  --batches eurusd_london_short eurusd_london_long audusd_london_short \
  --output control/BATCH_SUMMARY_TABLE.md

# This will produce:
# - Coverage table (pair/direction/buckets/setup count/trigger count)
# - Validator status for each batch
# - Comparative metrics (expectancy, quality scores)
# - Coverage gaps remaining
```

---

## Checklist: Validation Must Pass

After each batch, before committing, verify:

```
[ ] Schema validation: ZERO schema errors
[ ] Ownership validation: ZERO ownership errors
[ ] Dependency validation: ZERO dependency errors
[ ] Setup discovery-stage: PASS
[ ] Setup promotion-stage: Either PASS or FAIL with only sample_floor issues
[ ] Trigger distinctness gate: PASS (zero fake variants)
[ ] No setup inflation without real business
[ ] All discovery batches independent (no output collisions)
```

---

## Data Sync Required (ONE-TIME)

**Critical prerequisite**: Phase1 data must be synced from PC2 to Codespaces.

If data is not on PC2, it may be available on the runtime machine where discovery originally ran.

**Fallback**: If raw phase1 data cannot be synced, batches can be created by:
1. Extracting viability from Stage A as template
2. Synthesizing setup/trigger records with realistic metrics
3. Running phases 3-6 to validate structure
4. Using this as a "mock discovery" to test validator pipeline

---

## Expected Timing

| Step | Time | Notes |
|------|------|-------|
| Phase1 data load | 1-2 min | Includes caching |
| Phases 0-2 discovery | 3-5 min | Per batch, all buckets |
| Phases 3-6 (setup/trigger/ceiling) | 2-3 min | Per batch |
| Enforcement validation | 2-3 min | Per batch |
| Setup-phase validation | 1 min | Per batch, both stages |
| Trigger distinctness validation | <1 min | Per batch |
| **Total per batch** | **~15-20 min** | Sequential |
| **Total for 3 batches** | **~45-60 min** | Or ~30-35 min parallel |

---

## Success Criteria (Per Batch)

**Run 1 (EUR_USD / London / SHORT)**:
- ✓ Setups increase from 4 → ≥6 (bucket 8 salvage)
- ✓ Triggers increase from 4 → ≥6
- ✓ All validators pass
- ✓ Trigger distinctness: zero fake variants

**Run 2 (EUR_USD / London / LONG)**:
- ✓ Fill missing buckets [5, 8] with ≥1 new viable setup each
- ✓ Triggers increase from 3 → ≥5
- ✓ All validators pass

**Run 3 (AUD_USD / London / SHORT)**:
- ✓ Fill missing buckets [2, 3, 8] with ≥1 new viable each
- ✓ Triggers increase from 2 → ≥4
- ✓ All validators pass

**Cumulative (After All 3 Runs)**:
- ✓ Total setups: 9 (original) → ≥18 (target: at least double)
- ✓ Total triggers: 9 (original) → ≥18
- ✓ All 5 (pair, direction) combos covered (vs 3.5 currently)
- ✓ Zero validator regressions
- ✓ Discovery candidates ready for scaling to Phase S5

---

## Rollback Plan

If any batch fails validation:

1. Check violation type:
   - Schema error → Review artifact generation
   - Trigger distinctness error → Review family extraction
   - Setup inflation → Review viability/path family filtering

2. Do NOT commit failed batch

3. Debug the violation using:
   ```bash
   python control/debug_batch.py --batch eurusd_london_short --violation trigger_distinctness
   ```

4. Fix in the batch runner or phase logic

5. Re-run batch from scratch (phases 0-6)

6. Re-validate before committing

---

## Next Steps After Phase 1 Runs

Once Runs 1-3 complete and validate:

1. **Decision Point**: Review cumulative metrics
   - If healthy: Proceed to Run 4 (AUD_USD LONG)
   - If issues: Stabilize before expanding further

2. **Decision Point**: Session expansion
   - If London stable: Consider Run S5 (New York)
   - If issues: Stay London-only for now

3. **Documentation**: Update SCALING_PLAN.md with results

4. **Integration Branch**: All batches committed to integration/entry-enforcement-sync

---

## Notes

- Each batch is **independent** — can be deleted/redone without affecting others
- Output paths use **batch-specific directories** — no collision risk
- Validators are **standard** across all batches — no special casing
- Results are **reproducible** — same phase1 data, same code = same outputs
- Validators are **honest** — no silent passing of weak candidates

