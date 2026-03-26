"""Shared helpers for building strict cache keys and hashing inputs."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterable, Mapping


def _normalize_component(component: Any) -> str:
    if component is None:
        return "<NONE>"
    if isinstance(component, (str, bytes)):
        return component.decode() if isinstance(component, bytes) else component
    if isinstance(component, Path):
        return str(component.resolve())
    if isinstance(component, Mapping):
        items = ",".join(f"{k}={_normalize_component(v)}" for k, v in sorted(component.items()))
        return f"{{{items}}}"
    if isinstance(component, Iterable) and not isinstance(component, (str, bytes)):
        items = ",".join(_normalize_component(v) for v in component)
        return f"[{items}]"
    return str(component)


def build_cache_key(*components: Any) -> str:
    hasher = hashlib.sha256()
    for component in components:
        normalized = _normalize_component(component)
        hasher.update(normalized.encode("utf-8"))
        hasher.update(b"|")
    return hasher.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_json(data: Any) -> str:
    return sha256_text(json.dumps(data, sort_keys=True, separators=(",", ":")))


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def directory_signature(path: Path) -> str:
    """Hash sorted relative file paths plus stat metadata for determinism."""
    root = path.resolve()
    if not root.exists():
        return sha256_text(f"missing::{root}")
    hasher = hashlib.sha256()
    for file_path in sorted(p for p in root.rglob("*") if p.is_file()):
        rel = file_path.relative_to(root).as_posix()
        stat = file_path.stat()
        hasher.update(rel.encode("utf-8"))
        hasher.update(str(stat.st_mtime_ns).encode("utf-8"))
        hasher.update(str(stat.st_size).encode("utf-8"))
    return hasher.hexdigest()


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def script_hash(script_path: Path) -> str:
    return sha256_file(script_path.resolve())


def env_flag(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}
