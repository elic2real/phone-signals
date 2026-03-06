#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional


def _to_dt(ts_utc: Optional[float | str | datetime]) -> datetime:
    if isinstance(ts_utc, datetime):
        return ts_utc.astimezone(timezone.utc)
    if isinstance(ts_utc, (int, float)):
        return datetime.fromtimestamp(float(ts_utc), tz=timezone.utc)
    if isinstance(ts_utc, str) and ts_utc.strip():
        s = ts_utc.strip().replace("Z", "+00:00")
        return datetime.fromisoformat(s).astimezone(timezone.utc)
    return datetime.now(timezone.utc)


def compute_session(ts_utc: Optional[float | str | datetime]) -> str:
    dt = _to_dt(ts_utc)
    h = dt.hour
    if 0 <= h < 8:
        return "ASIA"
    if 8 <= h < 16:
        return "LONDON"
    return "NY"


def compute_quarter(ts_utc: Optional[float | str | datetime], session: Optional[str] = None) -> str:
    dt = _to_dt(ts_utc)
    s = session or compute_session(dt)
    if s == "ASIA":
        i = min(3, max(0, dt.hour // 2))
    elif s == "LONDON":
        i = min(3, max(0, (dt.hour - 8) // 2))
    else:
        i = min(3, max(0, (dt.hour - 16) // 2))
    return f"Q{i+1}"


def compute_dow(ts_utc: Optional[float | str | datetime]) -> str:
    return ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][_to_dt(ts_utc).weekday()]


def compute_regime(*, mode: str = "", speed_class: str = "", spread_pips: float = 0.0) -> str:
    m = str(mode or "").upper()
    sc = str(speed_class or "").upper()
    if "FAST" in sc and spread_pips <= 1.5:
        return "trend"
    if "SLOW" in sc or "SLOW" in m:
        return "range"
    if spread_pips > 2.5:
        return "volatile"
    return "mixed"


@dataclass(frozen=True)
class StateKeyParts:
    pair: str
    mode: str
    entry_type: str
    strategy_id: str
    speed_class: str
    session: str
    quarter: str
    day_of_week: str
    regime: str


def build_state_key_parts(
    *,
    pair: str,
    mode: str,
    entry_type: str,
    strategy_id: str,
    speed_class: str,
    ts_utc: Optional[float | str | datetime] = None,
    regime: Optional[str] = None,
    spread_pips: float = 0.0,
) -> StateKeyParts:
    s = compute_session(ts_utc)
    return StateKeyParts(
        pair=str(pair),
        mode=str(mode),
        entry_type=str(entry_type),
        strategy_id=str(strategy_id),
        speed_class=str(speed_class),
        session=s,
        quarter=compute_quarter(ts_utc, s),
        day_of_week=compute_dow(ts_utc),
        regime=str(regime or compute_regime(mode=mode, speed_class=speed_class, spread_pips=spread_pips)),
    )


def build_state_key_core(**kwargs) -> str:
    p = build_state_key_parts(**kwargs)
    return "|".join([
        f"pair={p.pair}",
        f"mode={p.mode}",
        f"entry_type={p.entry_type}",
        f"strategy_id={p.strategy_id}",
        f"speed_class={p.speed_class}",
        f"session={p.session}",
        f"quarter={p.quarter}",
        f"dow={p.day_of_week}",
        f"regime={p.regime}",
    ])


def build_state_key_full(**kwargs) -> str:
    p = build_state_key_parts(**kwargs)
    return build_state_key_core(**kwargs) + f"|full_regime={p.regime}"
