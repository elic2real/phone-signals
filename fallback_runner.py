def runner_update(trade_state: dict) -> dict:
    profit_r = trade_state.get('profit_r', 0)
    entry = trade_state.get('entry', None)
    stop = trade_state.get('stop', None)
    direction = trade_state.get('direction', 'LONG')
    lock_triggered = trade_state.get('lock_triggered', False)
    last_close = trade_state.get('last_close', None)
    runner_trace = []


    # Fallback lock rule: at +0.25R, move stop to +0.20R (one-time only, never worsen, never above market)
    if not lock_triggered and profit_r >= 0.25:
        runner_trace.append(f"Lock rule checked: profit_r={profit_r} >= 0.25")
        if direction == 'LONG':
            lock_stop = entry + 0.20 * abs(entry - stop)
            if stop < lock_stop and (last_close is None or lock_stop <= last_close):
                trade_state['stop'] = lock_stop
                trade_state['lock_triggered'] = True
                runner_trace.append(f"Lock rule FIRED: stop moved to {lock_stop}")
            else:
                runner_trace.append(f"Lock rule NOT fired: stop={stop}, lock_stop={lock_stop}, last_close={last_close}")
        elif direction == 'SHORT':
            lock_stop = entry - 0.20 * abs(entry - stop)
            if stop > lock_stop and (last_close is None or lock_stop >= last_close):
                trade_state['stop'] = lock_stop
                trade_state['lock_triggered'] = True
                runner_trace.append(f"Lock rule FIRED: stop moved to {lock_stop}")
            else:
                runner_trace.append(f"Lock rule NOT fired: stop={stop}, lock_stop={lock_stop}, last_close={last_close}")

    # Early partial/lock for breakout/fast moves: if profit_r > 0.5, lock stop to 0.4R
    if not lock_triggered and profit_r >= 0.5:
        runner_trace.append(f"Partial/early lock checked: profit_r={profit_r} >= 0.5")
        if direction == 'LONG':
            lock_stop = entry + 0.40 * abs(entry - stop)
            if stop < lock_stop and (last_close is None or lock_stop <= last_close):
                trade_state['stop'] = lock_stop
                trade_state['lock_triggered'] = True
                runner_trace.append(f"Partial/early lock FIRED: stop moved to {lock_stop}")
            else:
                runner_trace.append(f"Partial/early lock NOT fired: stop={stop}, lock_stop={lock_stop}, last_close={last_close}")
        elif direction == 'SHORT':
            lock_stop = entry - 0.40 * abs(entry - stop)
            if stop > lock_stop and (last_close is None or lock_stop >= last_close):
                trade_state['stop'] = lock_stop
                trade_state['lock_triggered'] = True
                runner_trace.append(f"Partial/early lock FIRED: stop moved to {lock_stop}")
            else:
                runner_trace.append(f"Partial/early lock NOT fired: stop={stop}, lock_stop={lock_stop}, last_close={last_close}")
    trade_state['runner_trace'] = runner_trace

    # Remove breakeven logic: fallback does not use it
    return trade_state
