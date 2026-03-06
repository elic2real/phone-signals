#!/usr/bin/env python3
from __future__ import annotations
from datetime import datetime, timedelta, timezone
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from state_key import build_state_key_core


def main() -> int:
    base = datetime.now(timezone.utc)
    for i in range(50):
        ts = base - timedelta(minutes=i * 30)
        print(build_state_key_core(pair="EUR_USD", mode="ENTRY", entry_type="MR", strategy_id="1", speed_class="MED", ts_utc=ts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
