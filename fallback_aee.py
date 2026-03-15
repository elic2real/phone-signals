def detect_stall(trade_state: dict) -> bool:
    # Stall kill: after 2 bars, if profit < 0.05R (more aggressive fallback)
    bars_since_entry = trade_state.get('bars_since_entry', 0)
    profit_r = trade_state.get('profit_r', 0)
    stall_kill_weight = trade_state.get('stall_kill_weight', 1.0)  # tunable
    if bars_since_entry >= 2 and profit_r < 0.05 * stall_kill_weight:
        trade_state.setdefault('aee_trace', []).append(f"Stall kill checked: bars_since_entry={bars_since_entry} profit_r={profit_r} threshold={0.05 * stall_kill_weight}")
        trade_state['aee_trace'].append("Stall kill FIRED")
        return True
    # Fallback: legacy stall logic (bars_since_high > 4)
    if trade_state.get('bars_since_high', 0) > 4:
        trade_state.setdefault('aee_trace', []).append(f"Legacy stall checked: bars_since_high={trade_state.get('bars_since_high', 0)}")
        trade_state['aee_trace'].append("Legacy stall FIRED")
        return True
    return False

def detect_giveback(trade_state: dict) -> bool:
    # Tighter giveback protection: trigger at giveback > 0.2
    giveback = trade_state.get('giveback', 0)
    if giveback > 0.2:
        trade_state.setdefault('aee_trace', []).append(f"Giveback checked: giveback={giveback} threshold=0.2")
        trade_state['aee_trace'].append("Giveback FIRED")
        return True
    return False

def detect_momentum_collapse(trade_state: dict) -> bool:
    velocity = trade_state.get('velocity', 1)
    atr = trade_state.get('atr', 1)
    if velocity < atr * 0.05:
        trade_state.setdefault('aee_trace', []).append(f"Momentum collapse checked: velocity={velocity} atr={atr}")
        trade_state['aee_trace'].append("Momentum collapse FIRED")
        return True
    return False

def aee_decision(trade_state: dict) -> str:
    # Stall kill is a weighted/tunable part of the fallback AEE logic, not the only factor
    trade_state['aee_trace'] = []
    if detect_stall(trade_state):
        trade_state['aee_trace'].append("AEE action: CLOSE (stall)")
        return "CLOSE"
    if detect_giveback(trade_state):
        trade_state['aee_trace'].append("AEE action: CLOSE (giveback)")
        return "CLOSE"
    if detect_momentum_collapse(trade_state):
        trade_state['aee_trace'].append("AEE action: CLOSE (momentum collapse)")
        return "CLOSE"
    trade_state['aee_trace'].append("AEE action: HOLD")
    return "HOLD"
