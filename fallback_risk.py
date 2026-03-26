def structural_stop(df, direction: str) -> float:
    """
    Returns stop price based on recent swing low/high.
    direction: 'LONG' or 'SHORT'
    """
    if direction == "LONG":
        stop = df['low'].rolling(10).min().iloc[-1]
        if stop >= df['close'].iloc[-1]:
            raise ValueError("Stop not below entry for LONG")
        return stop
    elif direction == "SHORT":
        stop = df['high'].rolling(10).max().iloc[-1]
        if stop <= df['close'].iloc[-1]:
            raise ValueError("Stop not above entry for SHORT")
        return stop
    else:
        raise ValueError("Invalid direction")

def initial_risk(entry_price: float, stop_price: float) -> float:
    risk = abs(entry_price - stop_price)
    if risk <= 0:
        raise ValueError("Risk must be positive")
    return risk
