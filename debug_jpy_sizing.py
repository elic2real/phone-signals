#!/usr/bin/env python3
"""Debug JPY sizing issue based on logs."""

# USD_JPY calculation from logs
nav = 32271.0353
risk_target = nav * 0.02  # $645.42
pip_value = 6.334165220365607e-05  # $0.00006334
stop_pips = 445.8
risk_per_unit = stop_pips * pip_value  # $0.02824 per unit
units = risk_target / risk_per_unit

print(f'Risk target: ${risk_target:.2f}')
print(f'Risk per unit: ${risk_per_unit:.6f}')
print(f'Calculated units: {units:.0f}')
print(f'Actual risk with 5 units: ${5 * risk_per_unit:.2f}')

# The issue is clear now:
print(f'\nThe problem: With stop distance of {stop_pips} pips and tiny pip value...')
print(f'Each unit only risks ${risk_per_unit:.6f}')
print(f'To risk ${risk_target:.2f}, we need {units:.0f} units!')
print(f'But the system is only placing 5 units, risking ${5 * risk_per_unit:.2f}')

# Check if there's a cap or minimum issue
print(f'\nChecking for potential issues:')
print(f'1. Broker minimum trade size? (Usually 1 unit)')
print(f'2. Maximum units cap?')
print(f'3. Margin check limiting units?')

# Calculate required margin
entry_price = 157.883
margin_rate = 0.0333  # 3.33%
required_margin_per_unit = entry_price * margin_rate
print(f'\nMargin per unit: ${required_margin_per_unit:.2f}')
print(f'Margin for {units:.0f} units: ${units * required_margin_per_unit:.0f}')
print(f'Available margin: $15,126.764 (from logs)')

# The margin check would pass
print(f'\nMargin check: {units * required_margin_per_unit < 15126.764}')
