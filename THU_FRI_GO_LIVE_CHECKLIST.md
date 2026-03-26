# Thu/Fri Go-Live Checklist

Scope:
- Days: `thursday`, `friday`
- Pairs:
  - `AUD_USD`
  - `EUR_JPY`
  - `EUR_USD`
  - `GBP_JPY`
  - `GBP_USD`
  - `NZD_USD`
  - `USD_CAD`
  - `USD_CHF`
  - `USD_JPY`

## 1. Account And Environment

- `OANDA_API_KEY` is set to the intended live or practice key.
- `OANDA_ACCOUNT_ID` is set to the intended account.
- `OANDA_ENV` is set correctly:
  - `practice` for test
  - `live` for real money
- `LIVE_MODE=1` for live trading.
- `DRY_RUN_ONLY=0` for live trading.
- `ALLOW_ENTRIES=1`.
- `PAIR_LIST` is set to exactly:
  - `AUD_USD,EUR_JPY,EUR_USD,GBP_JPY,GBP_USD,NZD_USD,USD_CAD,USD_CHF,USD_JPY`

## 2. Runtime Settings

- `runtime_settings.json` exists for all 36 pair/session combinations under `compiled_session_templates`.
- Each runtime file has:
  - `live_priority_tier`
  - `live_priority_adjustment`
  - `priority`
  - `sizing`
  - `trade_family`
  - `aee`
  - `zones`
- Fallback AEE is donor-based:
  - every fallback zone has a nonempty `fallback_template_source`
  - no selected live pair/session has `fallback_template_source = no_donor`

## 3. Priority Engine

- Candidates only become actionable in:
  - `GET_READY`
  - `ARM_TICK_ENTRY`
- Global ranking is enabled before execution.
- Same-pair crowding penalty is active:
  - `SAME_PAIR_LINEAR_PENALTY=0.12`
- Pair parent cap is active:
  - `PAIR_PARENT_CAP=5`
- Opposite-side same-pair rule is active:
  - no simultaneous long and short on the same pair
  - profitable opposite-side replacement allowed

## 4. Primary / Fallback Pair Weighting

- Primaries:
  - `AUD_USD`
  - `EUR_JPY`
  - `USD_JPY`
  - `EUR_USD`
- Fallback set:
  - `GBP_JPY`
  - `GBP_USD`
  - `NZD_USD`
  - `USD_CAD`
  - `USD_CHF`
- Runtime scoring applies:
  - primary bonus `+0.18`
  - fallback penalty `-0.06`

## 5. Sizing

- Grade ladder matches runtime research:
  - `A = 3.0%`
  - `B = 2.0%`
  - `C = 1.5%`
  - `D = 0.5%`
  - `E = 0.0%`
- Add-on is configured:
  - one-time only
  - `1.0%`
  - `A/B` only
- Harvester/runner split is loaded from runtime settings.

## 6. AEE

- AEE mode is read per zone from runtime settings:
  - `full`
  - `fallback`
- Fallback AEE is template-based, not generic.
- Add-on is disabled automatically in fallback mode.
- Full AEE zones use the richer family profile where available.

## 7. Notifications And Logging

- Notification transport is configured if desired:
  - `NOTIFY_ENABLE_SEND=1` if sending is wanted
- Dry test of alert logging shows:
  - candidate score
  - historical grade
  - skip reason when rejected
- Runtime logs are writable.

## 8. Operational Limits

- `MAX_OPEN_TRADES_PER_PAIR` is not fighting the intended parent-cap behavior.
- `PAIR_PARENT_CAP=5` is the intended controlling cap.
- `MAX_OPEN_TRADES_GLOBAL` is set high enough not to silently override the strategy intent.

## 9. Broker Behavior

- OANDA netting behavior is acknowledged:
  - no simultaneous opposite-side exposure on the same pair
- Margin behavior is accepted as a real gate:
  - insufficient margin can still reject entries

## 10. Safe Rollout

- First pass:
  - `LIVE_MODE=0`
  - `DRY_RUN_ONLY=1`
  - confirm rankings, grades, sizing, skip reasons
- Second pass:
  - `LIVE_MODE=1`
  - `DRY_RUN_ONLY=0`
  - start with the same 9 pairs
- Watch first:
  - ranking order
  - pair crowding behavior
  - opposite-side conflict behavior
  - add-on gating
  - fallback/full AEE mode selection

## 11. Known Caveat

- Not every pocket has full trade-level AEE evidence yet.
- Live use is therefore:
  - full AEE where runtime settings say `full`
  - conservative template-based fallback AEE where runtime settings say `fallback`

## 12. Final Go / No-Go

Go only if all are true:
- account env is correct
- pair list is correct
- runtime settings exist for all 36 pair/session combinations
- fallback donors exist for all fallback zones
- priority, sizing, cap, and AEE settings match the research contract
- dry-run output looks sane

No-Go if any are false:
- wrong account or env
- stale or missing runtime settings
- fallback donors missing
- ranking path not using runtime settings
- sizing path not using grade ladder
- opposite-side rule not enforced
