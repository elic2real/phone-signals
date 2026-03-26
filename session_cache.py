"""Session-level cache management for deterministic parquet extraction steps."""
from __future__ import annotations

import gzip
import json
import os
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping

from cache_key_utils import (
    build_cache_key,
    directory_signature,
    ensure_dir,
    script_hash,
)

_CACHE_AUDIT_PATH = Path(__file__).resolve().parent / "cache_audit.jsonl"
_CACHE_VERSION = "session_cache_v1"


def _record_cache_event(event: str, cache_type: str, payload: Mapping[str, Any]) -> None:
    entry = {
        "ts": time.time(),
        "event": event,
        "cache_type": cache_type,
        **payload,
    }
    _CACHE_AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _CACHE_AUDIT_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, sort_keys=True) + "\n")


def _serialize_rows(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    serialized: List[Dict[str, Any]] = []
    for row in rows:
        rec = dict(row)
        dt_val = rec.get("dt")
        if isinstance(dt_val, datetime):
            rec["dt"] = dt_val.isoformat()
        serialized.append(rec)
    return serialized


def _deserialize_rows(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    deserialized: List[Dict[str, Any]] = []
    for row in rows:
        rec = dict(row)
        dt_val = rec.get("dt")
        if isinstance(dt_val, str):
            try:
                rec["dt"] = datetime.fromisoformat(dt_val)
            except Exception:
                pass
        deserialized.append(rec)
    return deserialized


@dataclass
class SessionCacheManager:
    data_root: Path
    script_path: Path
    cache_root: Path
    extra_components: MutableMapping[str, Any] | None = None
    disabled: bool = False
    cache_type: str = field(default="session_cache", init=False)

    def __post_init__(self) -> None:
        self.data_root = self.data_root.resolve()
        self.script_path = self.script_path.resolve()
        if self.extra_components is None:
            self.extra_components = {}
        components = [
            _CACHE_VERSION,
            str(self.data_root),
            script_hash(self.script_path),
        ]
        for key in sorted(self.extra_components):
            components.append(f"{key}={self.extra_components[key]}")
        self.cache_key = build_cache_key(*components)
        fanout = self.cache_key[:2]
        self.cache_dir = ensure_dir(self.cache_root / fanout / self.cache_key)
        self.sessions_dir = ensure_dir(self.cache_dir / "sessions")
        self.manifest_path = self.cache_dir / "manifest.json"

    def is_available(self) -> bool:
        return not self.disabled

    def has_complete_cache(self) -> bool:
        if not self.is_available():
            return False
        if not self.manifest_path.exists():
            return False
        try:
            manifest = json.loads(self.manifest_path.read_text())
        except Exception:
            return False
        if not manifest.get("completed"):
            return False
        sessions = manifest.get("sessions") or []
        for session_id in sessions:
            if not (self.sessions_dir / f"{session_id}.json.gz").exists():
                return False
        _record_cache_event(
            "cache_hit_ready",
            self.cache_type,
            {"cache_key": self.cache_key, "session_count": len(sessions)},
        )
        return True

    def load_all_sessions(self) -> Dict[str, List[Dict[str, Any]]]:
        if not self.has_complete_cache():
            raise RuntimeError("Session cache not complete; cannot load")
        manifest = json.loads(self.manifest_path.read_text())
        data: Dict[str, List[Dict[str, Any]]] = {}
        for session_id in manifest.get("sessions", []):
            path = self.sessions_dir / f"{session_id}.json.gz"
            with gzip.open(path, "rt", encoding="utf-8") as fh:
                rows = json.load(fh)
            data[session_id] = _deserialize_rows(rows)
        _record_cache_event(
            "cache_load_success",
            self.cache_type,
            {"cache_key": self.cache_key, "session_count": len(data)},
        )
        return data

    def store_sessions(self, sessions: Mapping[str, Iterable[Mapping[str, Any]]]) -> None:
        if not self.is_available():
            return
        total = 0
        for session_id, rows in sessions.items():
            self._store_session(session_id, rows)
            total += 1
        manifest = {
            "cache_key": self.cache_key,
            "completed": True,
            "sessions": sorted(sessions.keys()),
            "session_count": total,
            "components": {
                "data_root": str(self.data_root),
                "script": str(self.script_path),
                **self.extra_components,
            },
            "directory_signature": directory_signature(self.cache_dir),
            "created_ts": time.time(),
        }
        tmp_path = self.manifest_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
        tmp_path.replace(self.manifest_path)
        _record_cache_event(
            "cache_store_complete",
            self.cache_type,
            {"cache_key": self.cache_key, "session_count": total},
        )

    def _store_session(self, session_id: str, rows: Iterable[Mapping[str, Any]]) -> None:
        serialized = _serialize_rows(rows)
        path = self.sessions_dir / f"{session_id}.json.gz"
        ensure_dir(path.parent)
        tmp_path = path.with_suffix(".tmp")
        with gzip.open(tmp_path, "wt", encoding="utf-8") as fh:
            json.dump(serialized, fh)
        tmp_path.replace(path)

        _record_cache_event(
            "cache_session_store",
            self.cache_type,
            {"cache_key": self.cache_key, "session_id": session_id, "rows": len(serialized)},
        )


def build_session_cache_manager(
    data_root: Path,
    script_path: Path,
    cache_dir: Path | None = None,
    extra: Mapping[str, Any] | None = None,
) -> SessionCacheManager | None:
    env_disable = os.getenv("DISABLE_SESSION_CACHE", "0").strip().lower() in {"1", "true", "yes"}
    if env_disable:
        return None
    if cache_dir is None:
        cache_dir = Path(os.getenv("SESSION_CACHE_DIR", "session_cache"))
    return SessionCacheManager(
        data_root=data_root,
        script_path=script_path,
        cache_root=cache_dir,
        extra_components=dict(extra or {}),
    )
