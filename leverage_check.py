#!/usr/bin/env python3
"""Check leverage calculations."""

# USD_JPY has 50:1 leverage per LEVERAGE_50 set
pair = "USD_JPY"
leverage = 50  # USD_JPY is in LEVERAGE_50 set
entry_price = 157.883

# Margin calculation with leverage
margin_per_unit_leverage = entry_price / leverage
print(f'With {leverage}:1 leverage:')
print(f'Margin per unit: ${margin_per_unit_leverage:.2f}')

# But the risk sizing uses margin_rate
margin_rate = 0.0333  # 3.33% = ~30:1 leverage
margin_per_unit_rate = entry_price * margin_rate
print(f'\nWith margin_rate {margin_rate*100:.2f}%:')
print(f'Margin per unit: ${margin_per_unit_rate:.2f}')
print(f'Effective leverage: {1/margin_rate:.1f}:1')

# This explains the discrepancy!
print(f'\nThe risk sizing uses 30:1 leverage but USD_JPY could use 50:1')
print(f'This means margin requirements are 67% higher than needed')

# Calculate with correct leverage
margin_avail = 15126.764
max_affordable_50x = int(margin_avail / margin_per_unit_leverage)
max_affordable_30x = int(margin_avail / margin_per_unit_rate)

print(f'\nMax affordable with 50:1 leverage: {max_affordable_50x} units')
print(f'Max affordable with 30:1 leverage: {max_affordable_30x} units')
print(f'\nThe system is under-utilizing available leverage!')
