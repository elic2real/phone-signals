from fallback_regime import detect_regime
from fallback_bias import micro_bias
from fallback_entry import entry_signal
from fallback_priority import priority_score
from fallback_risk import structural_stop, initial_risk
from fallback_aee import aee_decision
from fallback_runner import runner_update

def evaluate_candidate(df) -> dict:
    regime = detect_regime(df)
    bias = micro_bias(df)
    entry = entry_signal(df)
    if entry is None:
        return {'regime': regime, 'bias': bias, 'entry': None}
    stop = structural_stop(df, entry)
    risk = initial_risk(df['close'].iloc[-1], stop)
    score = priority_score(df)
    return {
        'regime': regime,
        'bias': bias,
        'entry': entry,
        'stop': stop,
        'risk': risk,
        'priority_score': score
    }

def manage_trade(trade_state: dict) -> dict:
    # Apply runner_update first and persist all changes
    updated_state = runner_update(trade_state)
    assert 'runner_trace' in updated_state, "runner_trace missing after runner_update"
    # Pass the exact updated state to AEE
    action = aee_decision(updated_state)
    updated_state['aee_action'] = action
    return updated_state
