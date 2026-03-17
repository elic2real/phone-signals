# JPY Unit Collapse - Root Cause Analysis

## Summary
JPY trades are producing 5 units instead of ~12,000 due to an unknown factor in the production environment that's not present in isolated testing.

## Key Findings

### 1. Two Confirmed Issues
1. **Margin Rate Fallback**: Using 3.33% instead of 2% for JPY pairs (reduces leverage from 50:1 to 30:1)
2. **Mysterious Collapse**: Units somehow become 5 in production but 12,006 in testing

### 2. Evidence Trail
```
Production Log: units_raw=5, units_final=5
My Test: units_total=12,006
Same parameters → Different results
```

### 3. The Collapse Point
- SPREAD_ADJUST log shows `units_base: 5`
- This means units are already 5 BEFORE spread adjustment
- The collapse happens in or before calc_units

### 4. Mathematical Clue
```
12,006 / 2400 = 5.0025 ≈ 5
```
Something might be dividing by 2400, but this division is not found in the code.

### 5. Potential Causes (Not Exhaustively Tested)

#### A. Environment/Configuration Differences
- Environment variables not set in test
- Configuration files loaded in production
- Runtime state or caching differences

#### B. Metadata Differences
- Production might have different instrument metadata
- minimumTradeSize could be different
- tradeUnitsPrecision could affect rounding

#### C. Code Path Differences
- Different branch taken based on runtime conditions
- Feature flags enabled in production
- Different initialization sequence

#### D. External Factors
- OANDA API returning different values
- Account-specific restrictions
- Time-based or session-specific logic

## Recommended Investigation Steps

### 1. Enable SIZE_TRACE in Production
Add to environment or code:
```python
SIZE_TRACE_ENABLED = os.getenv('SIZE_TRACE_ENABLED', '0').strip() in ('1', 'true', 'yes')
```

### 2. Compare Production vs Test Environment
Check:
- Environment variables: `env | grep -E "(SIZE|UNIT|LOT|JPY|MARGIN)"`
- Configuration files
- Instrument metadata from OANDA API

### 3. Add Logging to Find the Collapse Point
Add logging after each step in the main loop to see exactly where units become 5.

### 4. Check for Hidden Conversions
Look for:
- Lot size conversions (divide by 1000, 10000, etc.)
- Currency conversions
- Precision rounding that might truncate

### 5. Verify Actual Numbers
The 2400 divisor is suspicious:
- 2400 = 24 * 100 (hours in day * 100?)
- 2400 = 40 * 60 (minutes in 40 hours?)
- Could be related to time-based calculations

## Immediate Actions

1. **Deploy SIZE_TRACE logging** to production
2. **Capture the exact sequence** of unit calculations
3. **Compare production metadata** with test metadata
4. **Check for any runtime divisions** by 1000, 2400, etc.

## Conclusion
The margin rate issue is understood and fixable. The unit collapse to 5 requires production debugging with SIZE_TRACE enabled to identify the exact point of failure. The evidence suggests a hidden division or configuration difference that only exists in the production environment.
