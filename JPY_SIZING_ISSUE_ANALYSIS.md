# JPY Trade Sizing Issue Analysis

## Problem
JPY trades are extremely small (5-8 units) compared to what they should be for 2% risk sizing.

## Root Cause
The margin calculation in risk sizing uses a fixed 3.33% margin rate (30:1 leverage) but USD_JPY and other JPY pairs could use 50:1 leverage.

## Evidence from Logs
- USD_JPY: Only 5 units placed, risking $0.14 instead of $645.42 (2% of NAV)
- AUD_JPY: Only 8 units placed, risking $0.29 instead of $645.42
- Both have tiny pip values (~$0.000063) and large stop distances (445-571 pips)

## Calculation Breakdown
```
USD_JPY Example:
- NAV: $32,271
- Risk target (2%): $645.42
- Stop distance: 445.8 pips
- Pip value: $0.00006334
- Risk per unit: $0.02824
- Required units for 2% risk: 22,857 units
- Margin per unit (at 3.33%): $5.26
- Total margin needed: $120,169
- Available margin: $15,126
- Max affordable units: 2,877
```

## The Leverage Mismatch
- **Current**: Uses 3.33% margin rate = 30:1 leverage
- **Available**: USD_JPY has 50:1 leverage (2% margin rate)
- **Impact**: 67% higher margin requirements than necessary

## Solution Options

### Option 1: Use Dynamic Margin Rates
Update the risk sizing to use the actual margin rate from broker metadata instead of the 3.33% fallback.

```python
# Instead of:
margin_rate = float(meta.get("marginRate", 0.0333))

# Use:
if pair in LEVERAGE_50:
    margin_rate = 0.02  # 50:1 leverage
else:
    margin_rate = 0.05  # 20:1 leverage
```

### Option 2: Fetch Real Broker Metadata
Ensure the broker metadata is properly fetched and contains accurate margin rates for each pair.

### Option 3: Leverage-Based Margin Calculation
Calculate margin rate based on the pair's leverage tier:
```python
leverage = 50 if pair in LEVERAGE_50 else 20
margin_rate = 1 / leverage
```

## Impact
With proper 50:1 leverage for USD_JPY:
- Max affordable units: 4,790 (vs 2,877)
- Could risk ~$135 per trade (still below 2% target but much better)

## Recommendation
Implement Option 3 as it's the most straightforward and aligns with the existing LEVERAGE_50 set used elsewhere in the code.
