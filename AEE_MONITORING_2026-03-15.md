# AEE Monitoring Audit - 2026-03-15

## Run Context
- Bounded rerun executed after open-trade adoption patch in `phone_bot.py`.
- Audit window: `2026-03-15 23:44:32.819` to `2026-03-15 23:49:32.819` (latest 5-minute slice from `logs/runtime.log`).

## Summary Metrics
- `DB_OPEN_NOW = 11`
- `ADOPT_OPEN_TRADE_EVENTS = 0`
- `NO_PERIODIC_ADOPTED = 0`
- `FRESH_FILLS = 24`
- `NO_PERIODIC_FRESH = 6`
- `EXIT_ATTEMPT = 11`
- `EXIT_RESPONSE = 11`
- `EXIT_RESPONSE_404 = 6`
- `BROKER_STOP_LOSS_ORDER = 0`

## AEE Decision Counts
- `HOLD = 621`
- `PANIC_EXIT = 4`
- `EXTRACTION_LOSS_EXIT = 3`
- `NEVER_GREEN_FAST_EXIT = 4`

## Fresh Trades Without Periodic Eval in Window
- `5094, 5095, 5101, 5102, 5117, 5118`

Observed fill/eval timestamps:
- `5094`: fill `2026-03-15 23:44:33.431`, eval `-`
- `5095`: fill `2026-03-15 23:44:34.154`, eval `-`
- `5101`: fill `2026-03-15 23:45:12.899`, eval `-`
- `5102`: fill `2026-03-15 23:45:13.604`, eval `-`
- `5117`: fill `2026-03-15 23:49:20.009`, eval `-`
- `5118`: fill `2026-03-15 23:49:20.679`, eval `-`

Current DB states for those IDs:
- All six are `CLOSED` with note `MANUAL_CLOSE_CONFIRMED`.

## Current Interpretation
- Monitoring evidence is mixed:
  - AEE is actively producing live doctrine decisions and close attempts.
  - 404 responses remain non-trivial in this slice (`6/11`).
  - A no-periodic class reappeared for six fresh EUR_CAD shorts, each ending `MANUAL_CLOSE_CONFIRMED` without recorded `AEE_PERIODIC_DECISION`.
- This indicates ownership loss before first periodic eval is still present under at least some live conditions, despite earlier successful slices.
