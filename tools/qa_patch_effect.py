#!/usr/bin/env python3
from __future__ import annotations
import argparse
import json
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--patch", default="calibration/tune_map_patch.json")
    args = ap.parse_args()
    p = Path(args.patch)
    if not p.exists():
        print("PATCH_EFFECT_NO_PATCH")
        return 1
    d = json.loads(p.read_text(encoding="utf-8"))
    print("PATCH_EFFECT_OK", {"n": len(d.get("patches", [])), "version": d.get("version")})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
