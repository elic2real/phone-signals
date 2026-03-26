#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _fail(msg: str) -> int:
    print(f"FAIL: {msg}")
    return 2


def _ok(msg: str) -> None:
    print(f"OK: {msg}")


def _require_file(p: Path, label: str) -> None:
    if not p.exists():
        raise FileNotFoundError(f"Missing {label}: {p}")
    if p.is_dir():
        raise IsADirectoryError(f"Expected file for {label}, got directory: {p}")


def _require_dir(p: Path, label: str) -> None:
    if not p.exists():
        raise FileNotFoundError(f"Missing {label}: {p}")
    if not p.is_dir():
        raise NotADirectoryError(f"Expected directory for {label}, got file: {p}")


def _load_manifest(path: Path) -> dict[str, Any]:
    _require_file(path, "transfer_manifest.json")
    return json.loads(path.read_text())


def _check_hashes(manifest: dict[str, Any], base_dir: Path) -> list[str]:
    errors: list[str] = []
    files = manifest.get("files")
    if not isinstance(files, list):
        return ["manifest.files must be a list"]

    for entry in files:
        if not isinstance(entry, dict):
            errors.append("manifest entry is not an object")
            continue
        rel = entry.get("path")
        expected = entry.get("sha256")
        if not isinstance(rel, str) or not rel:
            errors.append("manifest entry missing path")
            continue
        if not isinstance(expected, str) or len(expected) < 8:
            errors.append(f"manifest entry {rel} missing/invalid sha256")
            continue
        if expected.upper() == "TBD":
            errors.append(f"manifest entry {rel} sha256 is TBD")
            continue

        p = base_dir / rel
        if not p.exists():
            errors.append(f"missing file: {rel}")
            continue
        if p.is_dir():
            errors.append(f"expected file but found directory: {rel}")
            continue

        actual = _sha256_file(p)
        if actual != expected:
            errors.append(f"hash mismatch: {rel} expected={expected} actual={actual}")

    return errors


def _import_check() -> list[str]:
    errors: list[str] = []
    sys.path.insert(0, str(ROOT))
    sys.path.insert(0, str(ROOT / "compiler"))
    sys.path.insert(0, str(ROOT / "production"))

    # Compiler entrypoints should import.
    modules = [
        "phase1_multi_session_compile",
        "phase2_11_sessions_cluster_compile",
        "phase3_11_sessions_entry_windows",
        "phase4_11_sessions_oae",
        "phase5_11_sessions_separability",
        "stage6_11_sessions_odm",
        "phase1_ode_proven",
    ]
    for mod in modules:
        try:
            __import__(mod)
        except Exception as e:
            errors.append(f"import failed: {mod}: {e}")
    return errors


def _config_resolution_check(dataset_lock_path: Path, data_root: Path) -> list[str]:
    errors: list[str] = []
    _require_file(dataset_lock_path, "dataset_lock")
    _require_dir(data_root, "data_root")

    # Minimal resolution checks based on sweep:
    # - dataset_lock must be valid json
    # - data_root must contain at least one parquet matching expected layout for a given pair.
    try:
        lock = json.loads(dataset_lock_path.read_text())
    except Exception as e:
        return [f"dataset_lock is not valid json: {e}"]

    pair = lock.get("pair")
    if not isinstance(pair, str) or not pair:
        # Not required by stage6, but helpful for validation.
        pair = None

    # If pair is present, check for at least one parquet file for that pair.
    if pair:
        globbed = list((data_root).glob(f"pair={pair}/year=*/month=*/part-*.parquet"))
        if not globbed:
            errors.append(f"no parquet found for pair={pair} under data_root using expected glob")

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="PC2 transfer acceptance check (hashes, imports, config resolution)")
    parser.add_argument("--base-dir", default=str(ROOT), help="Root directory containing transferred PC2 bundle")
    parser.add_argument("--manifest", default="transfer_manifest.json")
    parser.add_argument("--dataset-lock", default=None, help="Optional dataset lock path to validate JSON + data glob")
    parser.add_argument("--data-root", default=None, help="Optional data root to validate parquet glob")
    args = parser.parse_args()

    base_dir = Path(args.base_dir).resolve()
    try:
        _require_dir(base_dir, "base_dir")
    except Exception as e:
        return _fail(str(e))

    # Required files (PC2 layout)
    required = [
        "compile_node.py",
        "accept_transfer_check.py",
        "pc2_dependency_sweep.json",
        "transfer_manifest.json",
        "compiler/phase1_multi_session_compile.py",
        "compiler/phase2_11_sessions_cluster_compile.py",
        "compiler/phase3_11_sessions_entry_windows.py",
        "compiler/phase4_11_sessions_oae.py",
        "compiler/phase5_11_sessions_separability.py",
        "compiler/stage6_11_sessions_odm.py",
        "production/phase1_ode_proven.py",
        "datasets/dataset_lock_11_sessions.json",
    ]
    for rel in required:
        try:
            _require_file(base_dir / rel, rel)
        except Exception as e:
            return _fail(str(e))

    _ok("required files present")

    # Hash check
    manifest_path = base_dir / args.manifest
    try:
        manifest = _load_manifest(manifest_path)
    except Exception as e:
        return _fail(str(e))

    hash_errors = _check_hashes(manifest, base_dir)
    if hash_errors:
        print("FAIL: hash verification failed")
        for err in hash_errors:
            print(f"- {err}")
        return 3
    _ok("hashes verified")

    # Import check
    import_errors = _import_check()
    if import_errors:
        print("FAIL: import verification failed")
        for err in import_errors:
            print(f"- {err}")
        return 4
    _ok("imports work")

    # Optional config resolution check
    if args.dataset_lock and args.data_root:
        try:
            errs = _config_resolution_check(Path(args.dataset_lock), Path(args.data_root))
        except Exception as e:
            return _fail(str(e))
        if errs:
            print("FAIL: config resolution check failed")
            for err in errs:
                print(f"- {err}")
            return 5
        _ok("config resolution looks sane")

    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
