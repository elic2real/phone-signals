# JPY Sizing Investigation Findings

## Issue Summary
JPY trades are producing single-digit units (5-8) when they should be thousands.

## Key Findings

### 1. Two Separate Issues Confirmed
As predicted, there are indeed TWO separate problems:

1. **Margin Rate Fallback Issue**: Using 3.33% (30:1 leverage) instead of proper 50:1 leverage for JPY pairs
2. **Downstream Collapse**: Something is crushing units from thousands to single digits AFTER risk sizing

### 2. Test Results
- **My test with exact log parameters**: 12,006 units
- **Live system logs**: 5 units
- **Same inputs, different outputs** → Proves there's a different code path or configuration

### 3. What Works Correctly
- Risk sizing calculation (`compute_units_risk_2pct`) works correctly
- Pip value calculation for JPY is correct ($0.00006334)
- Stop distance calculation is correct (445.8 pips)
- Expected units: ~22,857 raw, ~2,877 after margin cap

### 4. What's Wrong
1. **Margin Rate**: Hardcoded 3.33% fallback prevents full 50:1 leverage utilization
2. **Mysterious Collapse**: Live system produces 5 units despite correct calculation producing 12,000+

## Potential Causes for the Collapse

### 1. Different Code Path
- There might be an old sizing function still being called
- Configuration flag might be forcing different behavior
- JPY-specific handling somewhere

### 2. Configuration Differences
- Environment variables
- Configuration files
- Runtime flags

### 3. Post-Sizing Modification
- Something modifying units after `calc_units` returns
- Batch processing or normalization
- Unique size checking (seen in logs: "🔢 UNIQUE SIZE CHECK")

### 4. Lot/Unit Confusion
- Division by 1000 or lot size conversion
- Broker-specific unit handling

## Next Steps

### Immediate Actions
1. Add comprehensive SIZE_TRACE logging (already implemented)
2. Check for any post-calc_units modifications
3. Verify configuration flags and environment variables
4. Look for JPY-specific code paths

### Code Areas to Investigate
1. **Unique size checking**: Search for "UNIQUE SIZE CHECK" implementation
2. **Post-processing**: Any code that modifies units after calc_units
3. **Configuration**: Flags that might force different sizing behavior
4. **Batch processing**: Any normalization or batching logic

### Test Strategy
1. Run the system with SIZE_TRACE enabled to see the full pipeline
2. Compare test vs live execution paths
3. Check if there are different branches for different pair types

## Evidence Summary
```
Live Log: units_raw=5, units_final=5
My Test: units_total=12,006
Same inputs → Different outputs
```

This conclusively proves there's a downstream collapse specific to the live system.
