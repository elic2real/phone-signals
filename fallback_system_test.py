import pandas as pd
from fallback_system import evaluate_candidate, manage_trade

# Minimal deterministic test harness for fallback system


# --- Profitability Sanity Check ---
scenario_pnls = []



def compute_profit_r(entry, stop, last_close, direction):
    if entry is None or stop is None or last_close is None or direction == 0:
        return 0
    stop_dist = abs(entry - stop)
    if stop_dist == 0:
        return 0
    if direction == 1:
        return (last_close - entry) / stop_dist
    else:
        return (entry - last_close) / stop_dist

def print_case(name, df, scenario):
    print(f"\n=== {name} ===")
    # scenario: dict with keys: direction, entry, stop, last_close, exit_type ('HOLD'|'STOP_HIT'|'AEE_CLOSE')
    direction = scenario['direction']
    entry = scenario['entry']
    stop = scenario['stop']
    last_close = scenario['last_close']
    exit_type = scenario['exit_type']
    bars_since_entry = scenario.get('bars_since_entry', 4)
    stall_kill_weight = scenario.get('stall_kill_weight', 1.0)
    # Compute profit_r from prices
    profit_r = compute_profit_r(entry, stop, last_close, direction)
    # Validate stop validity
    if direction == 1:
        if stop >= entry and bars_since_entry == 0:
            print("[WARNING] Long initial stop must be below entry")
        if stop > last_close and exit_type != 'HOLD':
            print("[WARNING] Long stop above market at exit")
    if direction == -1:
        if stop <= entry and bars_since_entry == 0:
            print("[WARNING] Short initial stop must be above entry")
        if stop < last_close and exit_type != 'HOLD':
            print("[WARNING] Short stop below market at exit")
    # Build trade_state for fallback logic
    trade_state = {
        'bars_since_high': scenario.get('bars_since_high', 2),
        'giveback': scenario.get('giveback', 0.1),
        'velocity': scenario.get('velocity', 2),
        'atr': scenario.get('atr', 3),
        'entry': entry,
        'stop': stop,
        'bars_since_entry': bars_since_entry,
        'stall_kill_weight': stall_kill_weight,
        'lock_triggered': scenario.get('lock_triggered', False),
        'aee_action': None,  # will be set by fallback logic
        'profit_r': profit_r
    }
    # Run fallback logic
    from fallback_system import evaluate_candidate, manage_trade
    result = evaluate_candidate(df)
    updated = manage_trade(trade_state)
    # Print state
    print(f"Entry: {entry}, Stop: {stop}, Last Close: {last_close}, Direction: {direction}, profit_r: {profit_r:.3f}, Exit Type: {exit_type}")
    print(f"Trade State: {trade_state}")
    print(f"Updated State: {updated}")
    # Print action traces for key scenarios
    if name in ["Breakout", "Stall", "Giveback"]:
        print("--- Runner Trace ---")
        for msg in updated.get('runner_trace', []):
            print(msg)
        print("--- AEE Trace ---")
        for msg in updated.get('aee_trace', []):
            print(msg)
    # PnL logic
    pnl = 0
    if direction == 0 or entry is None:
        print("PnL = 0 (no valid direction or entry)")
    else:
        if exit_type == 'HOLD':
            pnl = (last_close - entry) * direction
            print(f"PnL (HOLD) = (last_close - entry) * direction = ({last_close} - {entry}) * {direction} = {pnl}")
        elif exit_type == 'STOP_HIT':
            pnl = (stop - entry) * direction
            print(f"PnL (STOP_HIT) = (stop - entry) * direction = ({stop} - {entry}) * {direction} = {pnl}")
        elif exit_type == 'AEE_CLOSE':
            pnl = (last_close - entry) * direction
            print(f"PnL (AEE_CLOSE) = (last_close - entry) * direction = ({last_close} - {entry}) * {direction} = {pnl}")
    scenario_pnls.append((name, pnl))
    print(f"Scenario PnL: {pnl:.3f}")







def make_df(**kwargs):
    # Helper to create a DataFrame with required columns and at least 10 rows
    # If a value is a list, use as-is; if scalar, repeat 10 times
    out = {}
    for k, v in kwargs.items():
        if isinstance(v, list):
            if len(v) < 10:
                # pad with last value
                v = v + [v[-1]] * (10 - len(v))
            out[k] = v[:10]
        else:
            out[k] = [v] * 10
    return pd.DataFrame(out)




# Only run Breakout, Stall, and Giveback for this test


# 1. Clear Trend (LONG, HOLD)
trend_df = make_df(
    open=[100+i for i in range(10)],
    high=[105+i for i in range(10)],
    low=[99+i for i in range(10)],
    close=[101+i for i in range(10)],
    atr=[3]*10,
    ma20=[102+i*0.5 for i in range(10)],
    ma20_shift10=[100]*10,
    rolling_range=[6]*10,
    ma_fast=[103+i*0.5 for i in range(10)],
    ma_slow=[101]*10
)
print_case("Clear Trend", trend_df, {
    'direction': 1,
    'entry': 109,
    'stop': 106,  # below entry
    'last_close': 110,
    'exit_type': 'HOLD',
    'bars_since_high': 2,
    'giveback': 0.1,
    'velocity': 2,
    'atr': 3
})

# 2. Chop (no entry)
chop_df = make_df(
    open=[100]*10,
    high=[101]*10,
    low=[99]*10,
    close=[100.5]*10,
    atr=[1]*10,
    ma20=[100]*10,
    ma20_shift10=[100]*10,
    rolling_range=[2]*10,
    ma_fast=[100]*10,
    ma_slow=[100]*10
)
print_case("Chop", chop_df, {
    'direction': 0,
    'entry': None,
    'stop': None,
    'last_close': 100.5,
    'exit_type': 'HOLD'
})

# 3. Breakout (LONG, HOLD, stop below entry, last_close > entry + 0.5R)
breakout_df = make_df(
    open=[100+i for i in range(10)],
    high=[110+i for i in range(10)],
    low=[99+i for i in range(10)],
    close=[102+i*1.5 for i in range(10)],
    atr=[5]*10,
    ma20=[104+i for i in range(10)],
    ma20_shift10=[100]*10,
    rolling_range=[10]*10,
    ma_fast=[108+i for i in range(10)],
    ma_slow=[102]*10
)
print_case("Breakout", breakout_df, {
    'direction': 1,
    'entry': 115.5,
    'stop': 112,  # below entry
    'last_close': 118.5,  # entry + 3 > 0.5R
    'exit_type': 'HOLD',
    'bars_since_high': 1,
    'giveback': 0.05,
    'velocity': 5,
    'atr': 5
})

# 4. Reversal (SHORT, HOLD, stop above entry)
reversal_df = make_df(
    open=[110-i for i in range(10)],
    high=[111-i for i in range(10)],
    low=[100-i for i in range(10)],
    close=[109-i for i in range(10)],
    atr=[4]*10,
    ma20=[106-i*0.5 for i in range(10)],
    ma20_shift10=[110]*10,
    rolling_range=[8]*10,
    ma_fast=[102-i*0.5 for i in range(10)],
    ma_slow=[108]*10
)
print_case("Reversal", reversal_df, {
    'direction': -1,
    'entry': 100,
    'stop': 104,  # above entry
    'last_close': 99,
    'exit_type': 'HOLD',
    'bars_since_high': 3,
    'giveback': 0.2,
    'velocity': 3,
    'atr': 4
})

# 5. Stall (LONG, early stall kill, small loss)
stall_df = make_df(
    open=[100+i for i in range(10)],
    high=[105+i for i in range(10)],
    low=[99+i for i in range(10)],
    close=[101+i for i in range(10)],
    atr=[3]*10,
    ma20=[102+i*0.5 for i in range(10)],
    ma20_shift10=[100]*10,
    rolling_range=[6]*10,
    ma_fast=[103+i*0.5 for i in range(10)],
    ma_slow=[101]*10
)
print_case("Stall", stall_df, {
    'direction': 1,
    'entry': 109,
    'stop': 107,  # below entry
    'last_close': 108.8,  # small loss
    'exit_type': 'AEE_CLOSE',
    'bars_since_entry': 2,
    'bars_since_high': 7,
    'giveback': 0.1,
    'velocity': 2,
    'atr': 3
})

# 6. Giveback (LONG, AEE_CLOSE, giveback > 0.2, profit_r positive)
giveback_df = make_df(
    open=[100+i for i in range(10)],
    high=[105+i for i in range(10)],
    low=[99+i for i in range(10)],
    close=[101+i for i in range(10)],
    atr=[3]*10,
    ma20=[102+i*0.5 for i in range(10)],
    ma20_shift10=[100]*10,
    rolling_range=[6]*10,
    ma_fast=[103+i*0.5 for i in range(10)],
    ma_slow=[101]*10
)
print_case("Giveback", giveback_df, {
    'direction': 1,
    'entry': 109,
    'stop': 106,  # below entry
    'last_close': 110,  # above entry
    'exit_type': 'AEE_CLOSE',
    'bars_since_high': 2,
    'giveback': 0.7,  # triggers giveback
    'velocity': 2,
    'atr': 3
})

# 7. Stall Kill (no entry)
stall_kill_df = make_df(
    open=[100]*10,
    high=[101]*10,
    low=[99]*10,
    close=[100.1]*10,
    atr=[1]*10,
    ma20=[100]*10,
    ma20_shift10=[100]*10,
    rolling_range=[2]*10,
    ma_fast=[100]*10,
    ma_slow=[100]*10
)
print_case("Stall Kill", stall_kill_df, {
    'direction': 0,
    'entry': None,
    'stop': None,
    'last_close': 100.1,
    'exit_type': 'HOLD'
})
