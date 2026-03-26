def detect_regime(df) -> str:
    """
    Classifies each bar as 'EXPANSION', 'TREND', or 'CHOP'.
    Inputs:
        df: DataFrame with columns ['open', 'high', 'low', 'close', 'atr', 'ma20', 'ma20_shift10', 'rolling_range']
    Returns:
        str: One of 'EXPANSION', 'TREND', 'CHOP'
    Acceptance:
        - Never returns null
        - Deterministic, no future data
    """
    atr_now = df['atr'].iloc[-1]
    atr_past = df['atr'].shift(20).iloc[-1]
    slope = df['ma20'].iloc[-1] - df['ma20_shift10'].iloc[-1]
    if atr_now > atr_past * 1.2 and abs(slope) > 0:
        return "EXPANSION"
    if abs(slope) > df['atr'].iloc[-1] * 0.1:
        return "TREND"
    return "CHOP"
