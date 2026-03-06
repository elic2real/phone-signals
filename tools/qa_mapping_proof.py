#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tune_map import TuneMap


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", required=True)
    args = ap.parse_args()
    tm = TuneMap.load(args.seed)
    hit = tm.lookup("ENTRY", {"GLOBAL": "GLOBAL"})
    print("MAPPING_PROOF_OK", hit.get("source_level"), bool(hit.get("tune_hash")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
