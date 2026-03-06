#!/usr/bin/env python3
from __future__ import annotations
import json
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) < 2:
        print("usage: qa_calibration_patch.py <patch.json>")
        return 2
    p = Path(sys.argv[1])
    d = json.loads(p.read_text(encoding="utf-8"))
    req = ["version", "window", "inputs", "patches"]
    ok = all(k in d for k in req)
    print("CAL_PATCH_SCHEMA_OK" if ok else "CAL_PATCH_SCHEMA_FAIL", {"keys": list(d.keys())})
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
