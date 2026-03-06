#!/usr/bin/env python3
from __future__ import annotations
import argparse
import json
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log", required=True)
    ap.add_argument("--min-non-global", type=int, default=1)
    args = ap.parse_args()
    n = 0
    for line in Path(args.log).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            e = json.loads(line)
        except Exception:
            continue
        if e.get("source_level") and e.get("source_level") not in {"", "GLOBAL", "NONE"}:
            n += 1
    ok = n >= args.min_non_global
    print("MAPPING_ROUTING_OK" if ok else "MAPPING_ROUTING_FAIL", {"non_global": n})
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
