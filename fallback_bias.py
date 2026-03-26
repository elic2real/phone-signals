def micro_bias(df) -> str:
    """
    Detects short-term directional pressure.
    Inputs:
        df: DataFrame with columns ['high', 'low', 'ma_fast', 'ma_slow']
    Returns:
        str: 'LONG', 'SHORT', or 'NONE'
    Acceptance:
        - Deterministic, no future leakage
    """
    ma_fast = df['ma_fast'].iloc[-1]
    ma_slow = df['ma_slow'].iloc[-1]
    close = df['close'].iloc[-1]
    open_ = df['open'].iloc[-1]
    # Directional bias: MA relationship + recent displacement
    if ma_fast > ma_slow and close > open_:
        return "LONG"
    if ma_fast < ma_slow and close < open_:
        return "SHORT"
    return "NONE"
