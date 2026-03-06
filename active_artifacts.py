#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict


def _sha256(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load_active_artifacts(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"ACTIVE_ARTIFACTS_MISSING: {path}")
    obj = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(obj, dict):
        raise ValueError("ACTIVE_ARTIFACTS_INVALID: root not object")
    if int(obj.get("version", 0) or 0) != 1:
        raise ValueError("ACTIVE_ARTIFACTS_INVALID: version must be 1")
    sessions = obj.get("sessions")
    if not isinstance(sessions, dict):
        raise ValueError("ACTIVE_ARTIFACTS_INVALID: sessions missing")

    out = {"version": 1, "sessions": {}, "active_artifacts_sha256": _sha256(str(p))}
    for s in ("ASIA", "LONDON", "NY"):
        row = sessions.get(s)
        if not isinstance(row, dict):
            raise ValueError(f"ACTIVE_ARTIFACTS_INVALID: session {s} missing")
        patch = str(row.get("patch", "")).strip()
        vol_spec = str(row.get("vol_spec", "")).strip()
        k = int(row.get("k", 0) or 0)
        min_touched = int(row.get("min_touched_targets", 0) or 0)
        min_vol = int(row.get("min_vol_bucket_touched", 0) or 0)
        if not patch or not Path(patch).exists():
            raise FileNotFoundError(f"ACTIVE_PATCH_MISSING: {s} {patch}")
        if not vol_spec or not Path(vol_spec).exists():
            raise FileNotFoundError(f"ACTIVE_VOL_SPEC_MISSING: {s} {vol_spec}")
        if k != 3:
            raise ValueError(f"ACTIVE_ARTIFACTS_INVALID: {s} k must be 3")
        if min_touched <= 0 or min_vol <= 0:
            raise ValueError(f"ACTIVE_ARTIFACTS_INVALID: {s} min constraints invalid")
        out["sessions"][s] = {
            "patch": patch,
            "patch_sha256": _sha256(patch),
            "vol_spec": vol_spec,
            "vol_spec_sha256": _sha256(vol_spec),
            "k": k,
            "min_touched_targets": min_touched,
            "min_vol_bucket_touched": min_vol,
        }
    return out

