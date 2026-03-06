#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime, timezone


def oa_event(*, pair: str, reason: str, hold_sec: float, pnl_atr: float, mae_atr: float) -> dict:
    return {
        "kind": "OA_FORCE_CLOSE_TRIGGER",
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "pair": pair,
        "reason": reason,
        "hold_sec": round(float(hold_sec), 2),
        "pnl_atr": round(float(pnl_atr), 4),
        "mae_atr": round(float(mae_atr), 4),
    }


def should_force_close(*, hold_sec: float, min_hold: float, max_hold: float, pnl_atr: float, mae_atr: float) -> tuple[bool, str]:
    if hold_sec >= max_hold:
        return True, "max_hold"
    if hold_sec < min_hold:
        return False, "min_hold"
    if pnl_atr <= -0.6:
        return True, "pnl_atr_floor"
    if mae_atr >= 0.8:
        return True, "mae_atr_ceiling"
    return False, "none"


if __name__ == "__main__":
    print(json.dumps(oa_event(pair="EUR_USD", reason="demo", hold_sec=900, pnl_atr=-0.7, mae_atr=0.9)))
