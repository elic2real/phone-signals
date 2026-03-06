#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Tuple


def _default_cuts() -> Tuple[float, float]:
    return (1.0 / 3.0, 2.0 / 3.0)


def load_vol_bucket_spec(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"VOL_SPEC_MISSING: {path}")
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("VOL_SPEC_INVALID: not a JSON object")
    return obj


def validate_vol_bucket_spec(spec: Dict[str, Any], session: str, k_expected: int = 3) -> None:
    if not isinstance(spec, dict):
        raise ValueError("VOL_SPEC_INVALID: spec not object")
    names = spec.get("names")
    cuts = spec.get("cuts")
    k = int(spec.get("k", len(names) if isinstance(names, list) else 0) or 0)
    if k != int(k_expected):
        raise ValueError(f"VOL_SPEC_INVALID: k={k} expected={k_expected}")
    if not isinstance(names, list) or len(names) != k or any(not isinstance(x, str) for x in names):
        raise ValueError("VOL_SPEC_INVALID: names invalid")
    if not isinstance(cuts, list) or len(cuts) != (k - 1):
        # Compatibility path for selected-form specs.
        sel = spec.get("selected")
        if not isinstance(sel, dict):
            raise ValueError("VOL_SPEC_INVALID: cuts invalid")
    s = str(session or "").upper()
    sp_sess = str(spec.get("session", "")).upper()
    if sp_sess and sp_sess != s:
        raise ValueError(f"VOL_SPEC_INVALID: session mismatch spec={sp_sess} runtime={s}")


def cuts_for_session(spec: Dict[str, Any], session: str) -> Tuple[float, float]:
    s = str(session or "").upper()
    # Optional per-session map support.
    sess_map = spec.get("sessions")
    if isinstance(sess_map, dict):
        row = sess_map.get(s)
        if isinstance(row, dict):
            cuts = row.get("cuts")
            if isinstance(cuts, list) and len(cuts) >= 2:
                lo = float(cuts[0])
                hi = float(cuts[1])
                return (max(0.01, min(0.49, lo)), max(lo + 0.01, min(0.99, hi)))
    # Single-session spec support.
    sp_sess = str(spec.get("session", "")).upper()
    if sp_sess and sp_sess != s:
        return _default_cuts()
    sel = spec.get("selected")
    if isinstance(sel, dict):
        lo = float(sel.get("vol_low_hi", 1.0 / 3.0))
        hi = float(sel.get("vol_mid_hi", 2.0 / 3.0))
        return (max(0.01, min(0.49, lo)), max(lo + 0.01, min(0.99, hi)))
    cuts = spec.get("cuts")
    if isinstance(cuts, list) and len(cuts) >= 2:
        lo = float(cuts[0])
        hi = float(cuts[1])
        return (max(0.01, min(0.49, lo)), max(lo + 0.01, min(0.99, hi)))
    return _default_cuts()


def bucket_from_rank(rank: float, low_hi: float, mid_hi: float) -> str:
    r = float(rank)
    if r <= float(low_hi):
        return "VOL_LOW"
    if r <= float(mid_hi):
        return "VOL_MID"
    return "VOL_HIGH"
