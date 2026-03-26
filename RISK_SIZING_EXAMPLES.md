# Risk-Based Sizing Explanation with Examples

## Overview
The risk-based sizing system calculates position size to risk exactly 2% of NAV per trade. The formula is:

```
Units = (2% of NAV) / (Stop Distance in Pips × Pip Value in USD)
```

## Key Components

### 1. Risk Target
- Fixed at 2% of current NAV (Net Asset Value)
- For $10,000 NAV: $200 risk per trade
- For $50,000 NAV: $1,000 risk per trade

### 2. Stop Distance
- Distance from entry price to structural stop loss
- Converted to pips using the pair's pip size

### 3. Pip Value in USD
- Value of one pip movement per unit
- Varies by currency pair

---

## Example 1: EUR/USD

### Parameters
- NAV: $10,000
- Entry Price: 1.1000
- Stop Loss: 1.0900 (100 pips below entry)
- Direction: LONG

### Step-by-Step Calculation

1. **Risk Target**
   ```
   Risk Target = 2% of $10,000 = $200
   ```

2. **Stop Distance in Price**
   ```
   Stop Distance = |1.1000 - 1.0900| = 0.0100
   ```

3. **Stop Distance in Pips**
   - EUR/USD pip size = 0.0001
   ```
   Stop Distance in Pips = 0.0100 / 0.0001 = 100 pips
   ```

4. **Pip Value in USD**
   - Quote currency is USD, so pip value = pip size
   ```
   Pip Value = $0.0001 per unit
   ```

5. **Risk per Unit**
   ```
   Risk per Unit = 100 pips × $0.0001 = $0.01 per unit
   ```

6. **Calculate Units**
   ```
   Units = $200 / $0.01 = 20,000 units
   ```

7. **Apply Adjustments**
   - Spread multiplier: 0.95 (assuming moderate spread)
   - Confidence: 0.5 → multiplier = 0.25 + 0.75×0.5 = 0.625
   ```
   Final Units = 20,000 × 0.95 × 0.625 = 11,875 units
   ```

8. **Split by Speed Class (MED)**
   - Main leg: 80% = 9,500 units
   - Runner leg: 20% = 2,375 units

### Result
- Total position: 11,875 units
- Actual risk: 11,875 × 100 × $0.0001 = $118.75
- Risk percentage: $118.75 / $10,000 = 1.19% (due to confidence downscaling)

---

## Example 2: AUD/JPY

### Parameters
- NAV: $25,000
- Entry Price: 85.00
- Stop Loss: 83.50 (150 pips below entry)
- Direction: LONG
- Current prices: USD/JPY = 150.00, AUD/USD = 0.6500

### Step-by-Step Calculation

1. **Risk Target**
   ```
   Risk Target = 2% of $25,000 = $500
   ```

2. **Stop Distance in Price**
   ```
   Stop Distance = |85.00 - 83.50| = 1.50
   ```

3. **Stop Distance in Pips**
   - JPY pairs have pip size = 0.01
   ```
   Stop Distance in Pips = 1.50 / 0.01 = 150 pips
   ```

4. **Pip Value in USD**
   - Quote is JPY, need to convert to USD
   - One pip = 0.01 JPY
   - USD/JPY = 150.00 → 1 JPY = $1/150 = $0.006667
   ```
   Pip Value = 0.01 JPY × $0.006667/JPY = $0.00006667 per unit
   ```

5. **Risk per Unit**
   ```
   Risk per Unit = 150 pips × $0.00006667 = $0.01 per unit
   ```

6. **Calculate Units**
   ```
   Units = $500 / $0.01 = 50,000 units
   ```

7. **Apply Adjustments**
   - Spread multiplier: 1.0 (tight spread)
   - Confidence: 0.8 → multiplier = 0.25 + 0.75×0.8 = 0.85
   ```
   Final Units = 50,000 × 1.0 × 0.85 = 42,500 units
   ```

8. **Split by Speed Class (FAST)**
   - Main leg: 85% = 36,125 units
   - Runner leg: 15% = 6,375 units

### Result
- Total position: 42,500 units
- Actual risk: 42,500 × 150 × $0.00006667 = $425
- Risk percentage: $425 / $25,000 = 1.7% (due to confidence downscaling)

---

## Example 3: Short Position on EUR/USD

### Parameters
- NAV: $15,000
- Entry Price: 1.2000
- Stop Loss: 1.2100 (100 pips above entry)
- Direction: SHORT

### Calculation
1. **Risk Target**: 2% of $15,000 = $300
2. **Stop Distance**: |1.2000 - 1.2100| = 0.0100 = 100 pips
3. **Pip Value**: $0.0001 (same as long)
4. **Risk per Unit**: 100 × $0.0001 = $0.01
5. **Units**: $300 / $0.01 = 30,000 units
6. **After adjustments** (confidence 0.6, spread 0.9): 30,000 × 0.9 × 0.7 = 18,900 units

### Result
- Short 18,900 units of EUR/USD
- Risk if stopped: 18,900 × 100 × $0.0001 = $189

---

## Key Features

### Confidence Multiplier
- Maps confidence [0,1] to multiplier [0.25,1.0]
- Formula: `0.25 + 0.75 × confidence`
- Only reduces position size, never increases

### Spread Multiplier
- Tight spread (< 1 pip): 1.0 (no reduction)
- Moderate spread (1-2 pips): 0.8-1.0
- Wide spread (2-5 pips): 0.5-0.8
- Very wide spread (> 5 pips): BLOCK

### Speed Class Splits
- FAST: 85% main / 15% runner
- MED: 80% main / 20% runner
- SLOW: 75% main / 25% runner

### Broker Constraints
- Minimum trade size enforcement
- Precision rounding
- Margin availability check

## Code Implementation

The core calculation happens in `compute_units_risk_2pct()`:

```python
# Calculate risk budget (2% of NAV)
risk_pct = 0.02
risk_usd_target = nav_usd * risk_pct

# Calculate units from risk
risk_per_unit_usd = stop_dist_pips * pip_value_usd
units_total_float = risk_usd_target / risk_per_unit_usd

# Apply spread multiplier
units_total_float *= spread_mult

# Apply confidence downscaling
conf_mult = 0.25 + 0.75 * confidence
units_total = int(units_total_float * conf_mult)
```

This ensures every trade risks exactly 2% of NAV (adjusted by confidence), regardless of the currency pair or market conditions.
