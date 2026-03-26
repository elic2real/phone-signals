# Sizing Audit Directive

## Problem Statement
JPY trades are producing single-digit units (5-8 units) when they should be thousands. The 3.33% margin rate fallback explains undersizing but not the extreme collapse to single digits.

## Required Investigation

### 1. Add Comprehensive Size Tracing
Add a new `SIZE_TRACE` event that logs every intermediate sizing variable:

```python
def emit_size_trace(pair: str, side: str, stage: str, data: dict) -> None:
    """Emit detailed size tracing for audit."""
    emit_trade_kind(
        "SIZE_TRACE",
        {
            **build_event_envelope(kind="SIZE_TRACE", pair=pair),
            "side": side,
            "stage": stage,  # e.g., "raw_risk", "margin_cap", "final"
            "timestamp": now_ts(),
            **data
        }
    )
```

### 2. Trace Points Required
In `compute_units_risk_2pct()`, emit trace at these points:

1. After input validation
2. After risk target calculation
3. After stop distance calculation
4. After pip value calculation
5. After raw risk sizing
6. After margin cap (if applied)
7. After spread multiplier
8. After confidence multiplier
9. After precision rounding
10. After main/runner split
11. Before returning

### 3. Key Variables to Log
```python
trace_data = {
    "nav_usd": nav_usd,
    "risk_usd_target": risk_usd_target,
    "entry_price": entry_price,
    "sl_price": sl_price,
    "stop_dist_price": sl_dist_price,
    "stop_dist_pips": stop_dist_pips,
    "pip_value_usd_per_unit": pip_value_usd,
    "risk_per_unit_usd": risk_per_unit_usd,
    "units_risk_raw": units_total_float,
    "broker_margin_rate_source": "meta" if meta else "fallback",
    "broker_margin_rate_used": margin_rate,
    "units_margin_cap": units_total if margin_downscaled else None,
    "spread_mult": spread_mult,
    "confidence_mult": conf_mult if confidence else None,
    "units_after_modifiers": units_total_float,
    "units_after_precision_round": units_total,
    "units_main": units_main,
    "units_runner": units_runner,
    "units_final": units_total,
    "block_reason": result.get("block_reason"),
    "meta_fallback": result["debug"].get("meta_fallback", False),
    "margin_downscaled": result["debug"].get("margin_downscaled", False)
}
```

### 4. Fix Margin Rate Handling
```python
# At the start of compute_units_risk_2pct()
margin_rate = float(meta.get("marginRate", 0.0333)) if meta else 0.0333

# Warn if using fallback
if not meta or "marginRate" not in meta:
    log_runtime("warning", "BROKER_MARGIN_RATE_FALLBACK", 
                pair=pair, margin_rate=margin_rate, 
                reason="No broker metadata available")

# For JPY pairs, ensure proper leverage
if pair in LEVERAGE_50 and margin_rate > 0.025:  # > 40:1 leverage
    log_runtime("warning", "POTENTIAL_LEVERAGE_MISMATCH",
                pair=pair, margin_rate=margin_rate,
                expected_max="0.02 (50:1)")
```

### 5. Search for Second Sizing Path
Search codebase for:
- Any other calls to compute units after `compute_units_risk_2pct()`
- Any post-processing of units before order submission
- Any division by 1000 or price re-normalization
- Lot vs unit confusion

### 6. Check Precision Handling
JPY pairs may have different precision requirements:
```python
# Add debug for precision
precision = int(meta.get("tradeUnitsPrecision", 0))
if pair.endswith("_JPY") and precision > 0:
    log_runtime("debug", "JPY_PRECISION_CHECK", 
                pair=pair, precision=precision,
                units_before_round=units_total_float,
                units_after_round=units_total)
```

### 7. Verify Order Submission
Ensure final order uses exact units from sizing:
```python
# In order placement, add verification
if units_main != sizing_result["units_main"]:
    log_runtime("error", "UNITS_MISMATCH_BEFORE_ORDER",
                pair=pair, expected=sizing_result["units_main"],
                actual=units_main)
```

## Implementation Steps

1. Add `emit_size_trace()` function
2. Add trace points throughout `compute_units_risk_2pct()`
3. Fix margin rate fallback handling with warnings
4. Add precision debugging for JPY pairs
5. Add order submission verification
6. Run a test JPY trade and analyze the trace
7. Identify where the collapse occurs
8. Fix the root cause

## Expected Trace for Healthy JPY Trade
```
nav_usd: 32271.04
risk_usd_target: 645.42
stop_dist_pips: 445.8
pip_value_usd_per_unit: 0.00006334
risk_per_unit_usd: 0.02824
units_risk_raw: 22857
broker_margin_rate_used: 0.0333
units_margin_cap: 2877  # If margin limited
spread_mult: 0.84
confidence_mult: 0.362
units_after_precision_round: 876
units_main: 701
units_runner: 175
```

## Red Flags to Watch For
- Sudden drop from thousands to single digits
- Unexpected precision rounding (e.g., 876 → 8)
- Multiple sizing calculations
- Division by price or 1000 after sizing
- Lot/unit conversion errors

## Acceptance Criteria
- SIZE_TRACE events show complete sizing pipeline
- Any collapse from thousands to single digits is clearly identified
- Margin rate uses broker metadata when available
- JPY trades achieve appropriate sizing (hundreds+ units)
- No hidden post-sizing modifications
