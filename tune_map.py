#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Tuple

import json
from pathlib import Path

LEVELS = ["FULL", "MID", "COARSE", "SESSION_PAIR", "SESSION_GLOBAL", "GLOBAL"]
LADDER = ["FULL", "MID", "COARSE", "SESSION_PAIR", "SESSION_GLOBAL", "GLOBAL"]

def load_knob_schema() -> tuple[set[str], dict[str, tuple[float, float]]]:
    schema_path = Path(__file__).parent / "tunes" / "knob_schema.json"
    with schema_path.open("r", encoding="utf-8") as fh:
        data = json.load(fh)
    allowlist = set(data["allowlist"])
    clamps = {k: tuple(v) for k, v in data["clamps"].items()}
    return allowlist, clamps

ALLOWLIST, CLAMPS = load_knob_schema()


class TuneMap:
    def __init__(self, seed: Dict[str, Any] | None = None):
        self.seed = seed or {"levels": {}, "meta": {}}

    @staticmethod
    def load(path: str | Path) -> "TuneMap":
        p = Path(path)
        with p.open("r", encoding="utf-8") as fh:
            data = json.load(fh)
        if not isinstance(data, dict):
            raise ValueError("seed must be a JSON object")
        if "levels" not in data:
            data = {"levels": {"GLOBAL": data}, "meta": {}}
        return TuneMap(data)

    def _level_map(self, level: str) -> Dict[str, Dict[str, Any]]:
        lv = self.seed.get("levels", {}).get(level, {})
        return lv if isinstance(lv, dict) else {}

    def _hash(self, obj: Any) -> str:
        s = json.dumps(obj, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(s.encode("utf-8")).hexdigest()[:12]

    def _clamp(self, knobs: Dict[str, Any]) -> Tuple[Dict[str, Any], list[dict]]:
        out: Dict[str, Any] = {}
        clamped: list[dict] = []
        for k, v in knobs.items():
            if k not in ALLOWLIST:
                continue
            if k in CLAMPS and isinstance(v, (int, float)):
                lo, hi = CLAMPS[k]
                nv = max(lo, min(hi, v))
                if nv != v:
                    clamped.append({"knob": k, "requested": v, "applied": nv})
                out[k] = int(nv) if isinstance(lo, int) and isinstance(hi, int) else float(nv)
            else:
                out[k] = v
        return out, clamped

    def lookup(self, mode: str, state_keys: Dict[str, str]) -> Dict[str, Any]:
        for level in LADDER:
            key = state_keys.get(level)
            if not key:
                continue
            level_map = self._level_map(level)
            knobs = level_map.get(key)
            if isinstance(knobs, dict):
                effective, clamped = self._clamp(knobs)
                return {
                    "knobs": effective,
                    "source_level": level,
                    "source_key": key,
                    "tune_hash": self._hash(effective),
                    "clamped": clamped,
                    "mode": mode,
                }
        return {
            "knobs": {},
            "source_level": "NONE",
            "source_key": "",
            "tune_hash": self._hash({}),
            "clamped": [],
            "mode": mode,
        }
