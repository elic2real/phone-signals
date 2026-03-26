from fallback_regime import detect_regime
from fallback_bias import micro_bias

def entry_signal(df) -> str | None:
    """
    Entry logic: regime must be EXPANSION or TREND, bias must be LONG/SHORT, momentum confirmation required.
    Returns 'LONG', 'SHORT', or None.
    """
    regime = detect_regime(df)
    bias = micro_bias(df)
    if regime == "CHOP":
        return None
    if bias not in ("LONG", "SHORT"):
        return None
    momentum = abs(df['close'].iloc[-1] - df['open'].iloc[-1]) > df['atr'].iloc[-1] * 0.15
    if momentum:
        return bias
    return None

def entry_features(df) -> dict:
    regime = detect_regime(df)
    bias = micro_bias(df)
    momentum = abs(df['close'].iloc[-1] - df['open'].iloc[-1]) > df['atr'].iloc[-1] * 0.15
    return {
        'regime': regime,
        'bias': bias,
        'momentum': momentum
    }
