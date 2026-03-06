# Promotion Report (DD-Gate)

Patch: `<patch_path>`
Date (UTC): `<iso_utc>`

## Inputs
- Slice S1 cache: `<cache_s1_path>`
- Slice S1 fingerprint: `<cache_s1_sha256>`
- Slice S2 cache: `<cache_s2_path>`
- Slice S2 fingerprint: `<cache_s2_sha256>`
- Replay command flags: `--ceiling-mode first_passage --enforce-family-touch`

## Gate Rule (per slice)
- `ddEE_mean > 0`
- `ddCAP_mean >= 0`
- `ddEph > 0`
- `ddTail_mean_Eph >= 0`
- `ddExits_per_hour >= -0.1 * exits_per_hour_base`
- `touched_targets > 0`
- `touched_targets_neg_ddCAP == 0`

## Results: S1
- `ddEE_mean = <value>`
- `ddCAP_mean = <value>`
- `ddEph = <value>`
- `ddTail_mean_Eph = <value>`
- `ddExits_per_hour = <value>`
- `touched_targets = <value>`
- `touched_targets_neg_ddCAP = <value>`
- Decision: `PASS|HOLD`

## Results: S2
- `ddEE_mean = <value>`
- `ddCAP_mean = <value>`
- `ddEph = <value>`
- `ddTail_mean_Eph = <value>`
- `ddExits_per_hour = <value>`
- `touched_targets = <value>`
- `touched_targets_neg_ddCAP = <value>`
- Decision: `PASS|HOLD`

## Final Promotion Decision
- `PROMOTE|HOLD`
- Notes: `<short rationale>`
