# Strategy Runtime Override Rollout (Phase 1)

- Generated: `2026-03-17T12:59:01Z`
- Sim sample gate: `50`
- Objective: concentrate live sampling on highest-confidence harvester buckets and suppress weak low-distance slices.

## Use This Override File

- `AEE_STRATEGY_OVERRIDES_PATH=strategy_runtime_overrides_focus_phase1.json`

## Enabled Keys (Phase 1 Focus)

- `short:harvester:3.5`
- `short:harvester:5`
- `long:harvester:5`
- `long:harvester:6`
- `short:harvester:2.5`
- `short:harvester:8`

## Hard Suppress Keys

- `long:harvester:1.5`
- `short:harvester:1.5`
- `long:harvester:2.5`

## Continuation Quarantine

- `SUPPRESS_CONTINUATION_ENTRIES=1`
- `PC1_SUPPRESS_CONTINUATION=1`

## Notes

- BREAK|RUNNER|T8_0 is entry-family specific and cannot be disabled via side:mode:distance override table alone.
- Use entry gating (family-aware) plus focus override file to enforce the suppress intent operationally.
- PULLBACK_RECLAIM|HARVESTER|T8_0 remains NEEDS_SAMPLE per live evidence; not included in phase1 focus set.
