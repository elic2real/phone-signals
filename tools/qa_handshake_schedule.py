#!/usr/bin/env python3
from __future__ import annotations
from datetime import datetime, timezone


def main() -> int:
    now = datetime.now(timezone.utc)
    hour = now.hour
    minute = now.minute
    at_open = minute < 5
    at_half = 25 <= minute <= 35
    print("HANDSHAKE_SCHEDULE_OK", {"utc_hour": hour, "at_open": at_open, "at_half": at_half})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
