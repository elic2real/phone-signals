#!/usr/bin/env python3
"""Small helpers for validating simulation wiring."""

from __future__ import annotations

from typing import Any


def verify_wiring(env: Any) -> bool:
    checks = [
        getattr(env, "oanda", None) is not None,
        getattr(env, "price_feed", None) is not None,
        getattr(env, "logs", None) is not None,
    ]
    return all(checks)
