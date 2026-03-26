# JPY Sizing Investigation - Final Report

## Executive Summary
JPY trades are producing single-digit units (5-8) instead of thousands due to **two separate bugs**:

1. **Margin Rate Fallback**: Using 3.33% (30:1 leverage) instead of proper 50:1 leverage for JPY pairs
2. **Mysterious Pre-Sizing Collapse**: Something is reducing units to 5 BEFORE the spread adjustment

## Key Evidence

### From Logs
```
USD_JPY: units_raw=5, units_final=5, risk_actual=$0.14 (should be ~$600)
AUD_JPY: units_raw=8, units_final=8, risk_actual=$0.29 (should be ~$600)
EUR_CAD: units_raw=195,460, units_final=195,460 (works correctly)
```

### From Tests
- **Same parameters in isolation**: 12,006 units
- **Live system**: 5 units
- **Conclusion**: Different code path or configuration in production

## Root Cause Analysis

### Issue 1: Margin Rate Fallback (Confirmed)
- JPY pairs should use 50:1 leverage (2% margin rate)
- System falls back to 3.33% margin rate (30:1 leverage)
- Impact: 67% higher margin requirements than necessary
- Fix: Use dynamic margin rates based on LEVERAGE_50 set

### Issue 2: Pre-Sizing Collapse (Partially Located)
- SPREAD_ADJUST log shows `units_base: 5` 
- This means collapse happens BEFORE spread adjustment
- calc_units() should return 12,000+ units but returns 5
- Likely cause: Different configuration or code path in production

## Implemented Solutions

### 1. Size Tracing (Complete)
Added comprehensive SIZE_TRACE logging to track:
- All intermediate sizing variables
- Margin rate source and warnings
- Precision rounding effects
- Margin downscaling events

### 2. Margin Rate Warnings (Complete)
Added warnings for:
- Fallback margin rate usage
- Leverage mismatches for JPY pairs

## Next Steps for Resolution

### Immediate Actions
1. **Enable SIZE_TRACE** in production to see the full pipeline
2. **Check configuration differences** between test and production
3. **Verify USE_FE_SPREAD_SIZING flag** and other sizing-related env vars
4. **Look for JPY-specific code paths** that might override normal sizing

### Code Areas to Investigate
1. **Environment variables**: Check for sizing-related flags
2. **Configuration files**: Any JPY-specific settings
3. **Runtime state**: Global variables that might affect sizing
4. **Alternative code paths**: Old functions still being called

### Hypotheses to Test
1. **Configuration flag**: Something forces minimal sizing for JPY
2. **Volatility filter**: Low ATR triggers minimum size
3. **Legacy code path**: Old sizing function still active
4. **Runtime state**: Cached or persistent state affecting calculation

## Test Commands
```bash
# Enable size tracing
export SIZE_TRACE_ENABLED=1
python3 phone_bot.py

# Check environment variables
env | grep -E "(SPREAD|SIZE|JPY|MARGIN|LEVERAGE)"
```

## Impact Assessment
- **Current**: JPY trades risk $0.14 instead of $645 (0.02% vs 2% of NAV)
- **Potential missed profit**: ~4,600x smaller positions than intended
- **Risk management**: Not achieving target risk profile

## Conclusion
The margin rate fallback issue is understood and fixable. The pre-sizing collapse requires production tracing to identify the exact cause. The SIZE_TRACE implementation is ready and will provide complete visibility into the sizing pipeline when enabled.
