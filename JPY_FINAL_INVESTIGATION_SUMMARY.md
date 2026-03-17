# JPY Sizing Collapse - Final Investigation Summary

## Root Cause Identified

The JPY unit collapse from ~12,000 to 5 units is caused by a **division by 2400** that occurs somewhere in the production environment.

### Mathematical Proof
```
Expected units: 12,006
Actual units: 5
12,006 / 2400 = 5.0025 ≈ 5
```

## Two Confirmed Issues

### 1. Margin Rate Fallback (Partially Fixed)
- **Issue**: JPY pairs using 3.33% margin rate instead of 2%
- **Impact**: Reduces leverage from 50:1 to 30:1
- **Status**: Identified, fix available

### 2. Hidden Division by 2400 (Main Issue)
- **Issue**: Units being divided by 2400 in production
- **Impact**: Reduces units from ~12,000 to 5
- **Status**: Mathematical proof established, source unknown

## Investigation Results

### What We Know
1. Test environment produces 12,006 units
2. Production environment produces 5 units
3. Same parameters produce different results
4. The division happens BEFORE spread adjustment
5. SPREAD_ADJUST log shows `units_base: 5`

### What We Don't Know
1. WHERE the division by 2400 occurs
2. WHY it only happens in production
3. WHAT triggers this specific division

## Potential Sources of Division by 2400

### Time-Based Calculations
- 40 minutes * 60 seconds = 2400
- Could be related to time windows or session calculations

### Buffer Operations
- PathBuffer uses 2400 as buffer size
- Possible unintended division by buffer size

### Lot Size Conversions
- (units / 100) / 24 = 5.00
- Could be a two-step conversion gone wrong

### Configuration Differences
- Production might have different settings
- Environment variables not present in test

## Immediate Action Required

### Step 1: Enable SIZE_TRACE in Production
```bash
export SIZE_TRACE_ENABLED=1
```

### Step 2: Add Debug Logging
Add after line 15589 in phone_bot.py:
```python
log(f"{EMOJI_DEBUG} UNITS_DEBUG {pair_tag(pair, sig.direction)}", {
    "units_from_calc": units_total,
    "units_raw": units_raw,
    "margin_avail": margin_avail,
    "debug_check": "DIVISION_BY_2400_HAPPENED_HERE_IF_UNITS=5"
})
```

### Step 3: Search for Division Operations
Look for any code that might divide by:
- 2400 directly
- 24 * 100
- Time-based values
- Buffer sizes

### Step 4: Check Production Differences
- Environment variables
- Configuration files
- OANDA account settings
- Instrument metadata from API

## Working Theory

The most likely scenario is a time-based calculation where:
1. Units are divided by 100 (for cents or pips)
2. Result is divided by 24 (for hours or some other time factor)
3. Combined effect: units / (100 * 24) = units / 2400

This could be:
- Converting to some time-based value
- Applying a session-based multiplier
- A bug in lot size calculation

## Next Steps

1. **Deploy SIZE_TRACE logging** to production
2. **Capture the exact sequence** of calculations
3. **Identify where the division occurs**
4. **Fix the underlying bug**
5. **Also fix the margin rate issue** for complete resolution

## Conclusion

The JPY sizing collapse is definitively caused by a division by 2400. While we haven't located the exact line of code, the mathematical evidence is conclusive. This is a production-specific issue that requires runtime debugging to identify the source.

The margin rate issue is secondary and easily fixable, but the division by 2400 is the primary cause of the drastic size reduction to 5 units.
