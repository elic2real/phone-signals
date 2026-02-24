#!/usr/bin/env python3
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in ("1", "true", "yes", "on")


@dataclass
class RuntimeFlags:
    live_mode: bool
    dry_run: bool
    allow_entries: bool
    disable_trades: bool
    no_trades: bool
    sim_only: bool
    test_mode: bool
    debug_assertions: bool
    legacy_aee_paths: bool


def get_runtime_flags() -> RuntimeFlags:
    live_mode = _env_bool("LIVE_MODE", False)
    # Production-safe defaults: do not block entries/trades unless explicitly enabled.
    dry_run = _env_bool("DRY_RUN_ONLY", False)
    allow_entries = _env_bool("ALLOW_ENTRIES", True)

    disable_trades = _env_bool("DISABLE_TRADES", False)
    no_trades = _env_bool("NO_TRADES", False)
    sim_only = _env_bool("SIM_ONLY", False)
    test_mode = _env_bool("TEST_MODE", False)
    debug_assertions = _env_bool("DEBUG_ASSERTIONS", False)
    legacy_aee_paths = _env_bool("LEGACY_AEE_PATHS", False)

    return RuntimeFlags(
        live_mode=live_mode,
        dry_run=dry_run,
        allow_entries=allow_entries,
        disable_trades=disable_trades,
        no_trades=no_trades,
        sim_only=sim_only,
        test_mode=test_mode,
        debug_assertions=debug_assertions,
        legacy_aee_paths=legacy_aee_paths,
    )


def set_runtime_flag(name: str, value: bool) -> None:
    env_map: Dict[str, str] = {
        "live_mode": "LIVE_MODE",
        "dry_run": "DRY_RUN_ONLY",
        "allow_entries": "ALLOW_ENTRIES",
        "disable_trades": "DISABLE_TRADES",
        "no_trades": "NO_TRADES",
        "sim_only": "SIM_ONLY",
        "test_mode": "TEST_MODE",
        "debug_assertions": "DEBUG_ASSERTIONS",
        "legacy_aee_paths": "LEGACY_AEE_PATHS",
    }
    key = env_map.get(name, name)
    os.environ[key] = "true" if bool(value) else "false"
