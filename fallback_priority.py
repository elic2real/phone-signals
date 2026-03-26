import pandas as pd
def priority_score(df) -> float:
    """
    Scores a candidate using momentum, trend alignment, ATR expansion.
    Returns a numeric score.
    """
    momentum = abs(df['close'].iloc[-1] - df['open'].iloc[-1]) / df['atr'].iloc[-1]
    trend_strength = abs(df['ma_fast'].iloc[-1] - df['ma_slow'].iloc[-1])
    # Graceful lookback for ATR expansion
    lookback = 20
    available = len(df) - 1
    use_shift = min(lookback, available) if available > 0 else 1
    atr_past = df['atr'].shift(use_shift).iloc[-1]
    # If atr_past is nan or <=0, degrade gracefully
    if pd.isna(atr_past) or atr_past <= 0:
        atr_expansion = 1.0  # neutral expansion
    else:
        atr_expansion = df['atr'].iloc[-1] / atr_past
    score = momentum * 0.5 + trend_strength * 0.3 + atr_expansion * 0.2
    # Never emit NaN
    if pd.isna(score) or not (score > float('-inf')):
        return 0.01  # deterministic low-but-valid score
    return score

def rank_candidates(candidates) -> list:
    """
    Sorts candidates by priority_score descending.
    Each candidate is a dict with a 'df' key.
    """
    return sorted(candidates, key=lambda c: priority_score(c['df']), reverse=True)
