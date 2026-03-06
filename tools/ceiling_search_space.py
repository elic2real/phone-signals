#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_SPACE: dict[str, dict[str, Any]] = {
    "aee.fail_windows": {"type": "int", "min": 2, "max": 6, "step": 1},
    "aee.strictness_mult": {"type": "float", "min": 0.90, "max": 1.25, "step": 0.05},
    "extension_allow_energy_min": {"type": "float", "min": 0.90, "max": 1.30, "step": 0.05},
    "promote_mfe_atr": {"type": "float", "min": 0.10, "max": 0.45, "step": 0.05},
    "entry.tick.base_max_dist_atr": {"type": "float", "min": 0.05, "max": 0.30, "step": 0.05},
    "entry.tick.confirm_disp_atr": {"type": "float", "min": 0.10, "max": 0.35, "step": 0.05},
}


def load_space(path: str = "") -> dict[str, dict[str, Any]]:
    if not path:
        return dict(DEFAULT_SPACE)
    return json.loads(Path(path).read_text(encoding="utf-8"))


def pocket_keys(pair: str, session: str, quarter: str, vols: list[str]) -> list[str]:
    p = str(pair or "").upper()
    sq = f"{session}_{quarter}"
    return [f"{p}|{sq}|{v}" for v in vols]


def validate_pocket_patch(
    patch_obj: dict[str, Any],
    *,
    pair: str,
    session: str,
    quarter: str,
    allowed_levels: tuple[str, ...] = ("SESSION_PAIR",),
) -> tuple[bool, str]:
    allowed_prefix = f"{pair.upper()}|{session}_{quarter}|"
    for p in patch_obj.get("patches", []):
        if not isinstance(p, dict):
            return False, "non-dict patch entry"
        level = str(p.get("level", "") or "")
        key = str(p.get("key", "") or "")
        if level not in allowed_levels:
            return False, f"disallowed level: {level}"
        if not key.startswith(allowed_prefix):
            return False, f"disallowed key outside pocket: {key}"
    return True, ""

