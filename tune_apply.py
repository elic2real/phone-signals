#!/usr/bin/env python3
from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import Any, Dict, Optional

from tune_map import TuneMap, ALLOWLIST, CLAMPS


def validate_knob_schema(knobs: dict):
    for k in knobs:
        if k not in ALLOWLIST:
            raise ValueError(f"Unknown knob '{k}' not in allowlist")
        if k not in CLAMPS:
            raise ValueError(f"Knob '{k}' has no clamp specs")


class TuneApply:
    def __init__(
        self,
        seed_path: Optional[str] = None,
        patch_path: str = "calibration/tune_map_patch_active_v4_2_vol_r1.json",
        manual_path: str = "tunes/manual_overrides.json",
    ):
        self.seed_path = seed_path
        self.patch_path = Path(patch_path)
        self.manual_path = Path(manual_path)
        self.seed_version = ""
        self.patch_version = ""
        self.manual_version = ""
        self._seed = TuneMap({"levels": {}, "meta": {}})
        self._patch: Dict[str, Any] = {}
        self._manual: Dict[str, Any] = {}

    def load_seed(self) -> Dict[str, Any]:
        path_chain = []
        if self.seed_path:
            path_chain.append(Path(self.seed_path))
        path_chain.extend([Path("tunes/tune_map_seed_v2.json"), Path("tunes/tune_map_seed.json")])
        for p in path_chain:
            if p.exists():
                self._seed = TuneMap.load(p)
                self.seed_version = str(self._seed.seed.get("meta", {}).get("manifest_hash", ""))
                return {"seed_path": str(p), "seed_hash": self.seed_version}
        self._seed = TuneMap({"levels": {}, "meta": {}})
        self.seed_version = ""
        return {"seed_path": "", "seed_hash": ""}

    def reload_patch_if_newer(self) -> Dict[str, Any]:
        if not self.patch_path.exists():
            return {"loaded": False, "patch_version": self.patch_version}
        data = json.loads(self.patch_path.read_text(encoding="utf-8"))
        for p in data.get("patches", []):
            if isinstance(p, dict):
                knobs = p.get("knobs", {})
                validate_knob_schema(knobs)
        version = str(data.get("version", ""))
        if version and version != self.patch_version:
            self._patch = data
            self.patch_version = version
            return {"loaded": True, "patch_version": version}
        return {"loaded": False, "patch_version": self.patch_version}

    def reload_manual_if_newer(self) -> Dict[str, Any]:
        if not self.manual_path.exists():
            return {"loaded": False, "manual_version": self.manual_version}
        data = json.loads(self.manual_path.read_text(encoding="utf-8"))
        overrides = data.get("overrides", {})
        if isinstance(overrides, dict):
            validate_knob_schema(overrides)
        elif isinstance(overrides, list):
            for rule in overrides:
                if isinstance(rule, dict):
                    set_knobs = rule.get("set", {})
                    validate_knob_schema(set_knobs)
        version = str(data.get("version") or (data.get("meta") or {}).get("version") or "")
        if version and version != self.manual_version:
            self._manual = data
            self.manual_version = version
            return {"loaded": True, "manual_version": version}
        return {"loaded": False, "manual_version": self.manual_version}

    def resolve(self, *, mode: str, state_keys: Dict[str, str]) -> Dict[str, Any]:
        seed_hit = self._seed.lookup(mode=mode, state_keys=state_keys)
        effective = dict(seed_hit.get("knobs", {}))

        # Patch precedence: PAIR -> FAMILY -> existing fallback chain.
        patch_map: Dict[tuple[str, str], Dict[str, Any]] = {}
        for p in (self._patch.get("patches") or []):
            if not isinstance(p, dict):
                continue
            p_mode = str(p.get("mode", "") or "").upper()
            if p_mode and p_mode != str(mode or "").upper():
                continue
            level = str(p.get("level", "") or "")
            key = str(p.get("key", "") or "")
            knobs = p.get("knobs") or {}
            if not level or not key or not isinstance(knobs, dict):
                continue
            patch_map[(level, key)] = knobs

        candidate_chain = [
            ("SESSION_PAIR", str(state_keys.get("PAIR_SESSION_QUARTER_VOL", "") or "")),
            ("SESSION_FAMILY", str(state_keys.get("FAMILY_SESSION_QUARTER_VOL", "") or "")),
            ("SESSION_PAIR", str(state_keys.get("PAIR_SESSION_QUARTER_ATR", "") or "")),
            ("SESSION_FAMILY", str(state_keys.get("FAMILY_SESSION_QUARTER_ATR", "") or "")),
            ("SESSION_PAIR", str(state_keys.get("PAIR_ATR", "") or "")),
            ("SESSION_FAMILY", str(state_keys.get("FAMILY_ATR", "") or "")),
            ("FULL", str(state_keys.get("FULL", "") or "")),
            ("MID", str(state_keys.get("MID", "") or "")),
            ("COARSE", str(state_keys.get("COARSE", "") or "")),
            ("SESSION_PAIR", str(state_keys.get("SESSION_PAIR", "") or "")),
            ("SESSION_GLOBAL", str(state_keys.get("SESSION_GLOBAL", "") or "")),
            ("GLOBAL", str(state_keys.get("GLOBAL", "GLOBAL") or "GLOBAL")),
        ]
        matched_level = str(seed_hit.get("source_level", "NONE") or "NONE")
        matched_key = str(seed_hit.get("source_key", "") or "")
        tier_index = -1
        for i, (lvl, key) in enumerate(candidate_chain):
            if not key:
                continue
            knobs = patch_map.get((lvl, key))
            if knobs is None:
                continue
            effective.update(knobs)
            matched_level = lvl
            matched_key = key
            tier_index = i
            break

        manual_overrides = (self._manual.get("overrides") or {})
        manual_applied_keys = []
        if isinstance(manual_overrides, dict):
            effective.update(manual_overrides)
            manual_applied_keys = sorted(manual_overrides.keys())
        elif isinstance(manual_overrides, list):
            # Support rule-list schema: [{"match": {...}, "set": {...}}]
            for i, row in enumerate(manual_overrides):
                if not isinstance(row, dict):
                    continue
                match = row.get("match") or {}
                set_knobs = row.get("set") or {}
                if not isinstance(match, dict) or not isinstance(set_knobs, dict):
                    continue
                source_level = str(seed_hit.get("source_level", ""))
                session_val = ""
                src_key = str(seed_hit.get("source_key", ""))
                for part in src_key.split("|"):
                    if part.startswith("session="):
                        session_val = part.split("=", 1)[1]
                        break
                if "source_level" in match and str(match.get("source_level")) != source_level:
                    continue
                if "session" in match and str(match.get("session")) != session_val:
                    continue
                effective.update(set_knobs)
                manual_applied_keys.extend(sorted(set_knobs.keys()))
                manual_applied_keys.append(f"rule[{i}]")

        return {
            "knobs": effective,
            "source_level": matched_level,
            "source_key": matched_key,
            "tune_hash": hashlib.sha256(
                json.dumps(effective, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()[:12],
            "patch_version": self.patch_version,
            "manual_version": self.manual_version,
            "clamped": seed_hit.get("clamped", []),
            "manual_applied_keys": manual_applied_keys,
            "tier_index": tier_index,
        }
