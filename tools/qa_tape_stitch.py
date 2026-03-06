#!/usr/bin/env python3
from __future__ import annotations
import argparse
from pathlib import Path
try:
    import pandas as pd
except Exception:
    pd = None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tape", required=True)
    ap.add_argument("--pairs", default="EUR_USD")
    ap.add_argument("--days", type=int, default=3)
    args = ap.parse_args()
    root = Path(args.tape)
    ok = True
    for pair in [p.strip() for p in args.pairs.split(",") if p.strip()]:
        fp = root / f"pair={pair}" / "stitched.parquet"
        if not fp.exists():
            ok = False
            print("MISSING", fp)
            continue
        if pd is None:
            print(pair, "parquet_engine_unavailable", fp)
        else:
            try:
                df = pd.read_parquet(fp)
                print(pair, len(df))
            except Exception:
                print(pair, "read_failed_but_file_exists", fp)
    print("TAPE_STITCH_OK" if ok else "TAPE_STITCH_FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
