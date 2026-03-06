# 2% NAV Risk-Based Sizing Implementation

## Overview
Successfully replaced margin-utilization sizing with risk-based sizing that enforces exactly 2% NAV risk per trade.

## Key Components

### 1. New Functions Added

#### `extract_nav_from_account(acct_sum: dict) -> Tuple[float, str]`
- Extracts NAV (Net Asset Value) from Oanda account summary
- Handles both nested and top-level JSON structures
- Falls back to balance if NAV is not available
- Returns the NAV value and source identifier

#### `get_pip_value_usd(pair: str, price_map: Optional[Dict] = None) -> float`
- Calculates pip value in USD for any currency pair
- Handles USD quote pairs (direct), USD base pairs (inverse), and cross pairs
- Uses price_map for currency conversions when available
- Includes fallback logic for missing conversion rates

#### `compute_units_risk_2pct(...) -> dict`
- Core risk-based sizing function
- Calculates position size based on:
  - NAV (Net Asset Value)
  - Risk target (2% of NAV)
  - Stop loss distance in pips
  - Pip value in USD
  - Confidence multiplier (downscale only)
  - Spread multiplier
  - Speed class (for main/runner split)
- Returns detailed sizing information including:
  - units_total, units_main, units_runner
  - risk_usd_target, risk_usd_actual
  - stop_dist_pips, pip_value_usd
  - Block reason if applicable

### 2. Modified Functions

#### `calc_units(...) -> CalcUnitsResult`
- Updated to use risk-based sizing instead of margin utilization
- Now requires `sl_price` parameter
- Optional `nav_usd` parameter (fetches from broker if not provided)
- Optional `price_map` parameter for currency conversions
- Removed `util` and `confidence` parameters (handled internally)
- Preserves existing features:
  - Spread gating (blocks if spread > 5 pips)
  - Late impulse block (blocks if displacement > 3 ATR)
  - Broker minimum enforcement
  - Speed class splits (FAST=85/15, MED=80/20, SLOW=75/25)

#### `compute_units_recycling(...)`
- Disabled by default (flag `_ENABLE_MARGIN_SIZING = False`)
- Returns 0 units with reason "margin_sizing_disabled"
- Kept for reference/testing only

## Implementation Details

### Risk Calculation Formula
```
risk_target = nav_usd * 0.02  # 2% of NAV
units_raw = risk_target / (stop_dist_pips * pip_value_usd)
units_adjusted = units_raw * confidence_mult * spread_mult
units_final = round_to_broker_precision(units_adjusted)
```

### Confidence Multiplier
- Maps confidence [0,1] to multiplier [0.25, 1.0]
- Formula: `0.25 + 0.75 * confidence`
- Downscale only - never increases risk beyond target

### Spread Multiplier
- Tight spread (< 1 pip): 1.0 (no reduction)
- Moderate spread (1-2 pips): 0.8-1.0
- Wide spread (2-5 pips): 0.5-0.8
- Very wide spread (> 5 pips): BLOCK

### Speed Class Splits
- FAST: 85% main / 15% runner
- MED: 80% main / 20% runner
- SLOW: 75% main / 25% runner

## Integration Points

### Entry Loop Integration
- Updated `calc_units` call site to pass `sl1` (structural stop loss price)
- Added `price_map` parameter for currency conversions
- Maintains all existing validation and rejection logic

### Audit Logging
- Emits `SIZE_CALC` event for every sizing decision
- Includes all relevant parameters and results
- Enables tracking of risk-based sizing decisions

### Error Handling
- Fail-closed on missing inputs (SL price required)
- Graceful handling of missing broker metadata
- Optional margin check (skips if OandaClient not initialized)

## Testing

### Unit Tests (`test_risk_sizing.py`)
- Pip value calculations for different pair types
- Basic risk sizing behavior
- Spread impact on sizing
- Speed class split verification
- Confidence multiplier effects

### Integration Tests (`proof_risk_sizing.py`)
- calc_units integration with risk sizing
- Legacy margin sizing disabled
- Spread gating functionality
- Missing SL price handling
- SIZE_CALC event emission
- NAV extraction from various account structures

## Verification Commands

```bash
# Run unit tests
python3 test_risk_sizing.py

# Run integration proof
python3 proof_risk_sizing.py

# Check for any remaining margin-based sizing
grep -n "compute_units_recycling" phone_bot.py
grep -n "util.*eff" phone_bot.py
```

## Benefits

1. **Consistent Risk**: Every trade risks exactly 2% of current NAV (adjusted by confidence)
2. **Clear Logic**: Risk calculation is transparent and auditable
3. **Currency Agnostic**: Works with any currency pair via USD conversion
4. **Preserved Features**: All existing safeguards remain active
5. **Audit Trail**: Complete logging of sizing decisions

## Future Considerations

1. **Dynamic Risk**: Could make risk percentage configurable
2. **Portfolio Risk**: Add total portfolio risk limits
3. **Correlation**: Consider pair correlations in sizing
4. **Volatility**: Incorporate volatility adjustments

## Implementation Status

✅ Complete
- Risk-based sizing implemented and tested
- Legacy margin sizing disabled
- All integration points updated
- Comprehensive test coverage
- Audit logging active
