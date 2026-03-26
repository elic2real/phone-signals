# AEE Doctrine Quality Baseline - 2026-03-16

## Phase Status

- Control-path hardening is frozen (no new execution plumbing changes in this phase).
- Doctrine quality tuning started using fast synthetic harnesses.

## Harness Run

- Command: `python phase1_aee_tester.py`
- Result artifact: `phase1_aee_test_results.json`
- Final decision from harness: `GO`

## Final Validation (2000 synthetic paths)

- Static avg R: `+0.116`
- AEE avg R: `+0.222`
- Delta R: `+0.106`
- Premature clip rate: `3.3%`
- Loss reduction rate: `15.7%`

## Selected Knobs from Sweep

- `profit_capture_min_atr: 0.25`
- `allowed_giveback_atr_mult: 0.45`

## Observed Doctrine Tradeoffs

- Strong improvements on adverse classes:
  - `immediate_reversal` delta: `+0.517`
  - `slow_bleed` delta: `+0.322`
  - `stall_then_fail` delta: `+0.095`
- Mild underperformance on continuation/extension classes remains:
  - `clean_continuation` delta: `-0.075`
  - `tp_touch_then_extension` delta: `-0.069`

## Hard Doctrine For Phase 2 (No Control-Path Changes)

Tune AEE with the assumption that any live profit is valuable and should be harvested aggressively; holding for more is the exception that must be earned by exceptionally strong continuation, not the default.

## Next Tuning Priorities (Harvest-First)

1. Convert green to realized PnL as early as possible.
2. Prevent green-to-red round trips before anything else.
3. Allow continuation only when continuation quality is unusually strong and still improving.
4. Keep runner behavior as bonus behavior, not core identity.

## Tuning Questions

1. The instant a trade goes green, what is the earliest defensible harvest point that preserves extraction per hour?
2. Which continuation signatures are strong enough to earn hold time, and which should be harvested immediately?
3. How do we keep aggregate delta R positive while reducing dead-profit leakage?

## Live Regression Policy

- Use live windows only for regression confirmation after meaningful doctrine updates.
- Regression gates:
  - `fresh_fills_with_no_first_eval = 0`
  - `close_404_count = 0` (or clearly explained idempotent corner)
  - close attempts/responses parity
  - doctrine exits firing live
