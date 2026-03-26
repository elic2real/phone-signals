# Entry Type Labeling Notes

Important:
Treat the current entry system as a generic negative selector until proven otherwise.
The purpose of this task is to expose the real offensive entry families mechanically, not to add more generic filtering.

## Deterministic precedence
1. RECLAIM_CONTINUATION
2. PULLBACK_CONTINUATION
3. EXPANSION_BREAKOUT
4. RANGE_ESCAPE
5. OTHER

## Data sources used
- Executed trades from aee_state_stream.csv grouped by trade_id
- Existing gate/filter logic from run_aee_band_floor_baseline.py
- Pre-entry proxy features in stream columns (pre_*, compression, release_quality, noise, progress_ratio)
- Existing AEE evaluator function for realized outcome fields
