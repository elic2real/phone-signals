#!/usr/bin/env python3
from __future__ import annotations
import argparse
import json
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--require", required=True)
    args = ap.parse_args()

    req = [x.strip() for x in args.require.split(",") if x.strip()]
    p = Path(args.log)
    for line in p.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            e = json.loads(line)
        except Exception:
            continue
        if all(k in e for k in req):
            print("SCHEMA_PRESENCE_OK")
            return 0
    print("SCHEMA_PRESENCE_FAIL", req)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
