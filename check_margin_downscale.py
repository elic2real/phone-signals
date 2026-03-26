#!/usr/bin/env python3
"""Check margin downscale calculation."""

# From logs
margin_avail = 15126.764
entry_price = 157.883  # USD_JPY
margin_rate = 0.0333

# Calculate max affordable units
max_affordable_units = int(abs(margin_avail) / max(entry_price * margin_rate, 1e-12))
print(f'Margin available: ${margin_avail:.2f}')
print(f'Entry price: {entry_price}')
print(f'Margin rate: {margin_rate * 100:.2f}%')
print(f'Margin per unit: ${entry_price * margin_rate:.2f}')
print(f'Max affordable units: {max_affordable_units}')

# Check if this matches the log
print(f'\nLog shows 5 units placed')
print(f'Max affordable calculation allows: {max_affordable_units} units')

# Why only 5 then? Let's check confidence and spread
confidence_mult = 0.25 + 0.75 * 0.15  # util=0.15 used as confidence?
spread_mult = 0.84  # from logs
print(f'\nAdjustments:')
print(f'Confidence multiplier (if util=0.15): {confidence_mult:.3f}')
print(f'Spread multiplier: {spread_mult:.2f}')
print(f'Combined multiplier: {confidence_mult * spread_mult:.3f}')

# Final units after multipliers
units_after_mult = max_affordable_units * confidence_mult * spread_mult
print(f'Units after multipliers: {units_after_mult:.0f}')

# Still too high. Let's check if there's something else
print(f'\nMaybe there is a maximum units cap?')
print(f'Or maybe the confidence is different?')

# Try with very low confidence
for conf in [0.1, 0.05, 0.01]:
    conf_mult = 0.25 + 0.75 * conf
    units = max_affordable_units * conf_mult * spread_mult
    print(f'Confidence {conf:.2f} → {units:.0f} units')
