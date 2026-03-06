#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def build_manifest(root: Path) -> dict:
    files = sorted(str(p.relative_to(root)) for p in root.rglob("*.parquet"))
    return {"root": str(root), "files": files, "count": len(files)}


def stitch_pair(root_m1: Path, root_stitched: Path, pair: str) -> Path:
    m1_files = sorted((root_m1 / f"pair={pair}").rglob("*.parquet"))
    if not m1_files:
        raise FileNotFoundError(f"no parquet for {pair}")
    df = pd.concat([pd.read_parquet(p) for p in m1_files], ignore_index=True)
    if "timestamp" in df.columns:
        df = df.sort_values("timestamp")
    out_dir = root_stitched / f"pair={pair}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "stitched.parquet"
    df.to_parquet(out_file, index=False)
    return out_file


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--m1-root", default="data_tape")
    ap.add_argument("--stitched-root", default="data_tape_stitched")
    ap.add_argument("--pairs", default="EUR_USD,USD_JPY,AUD_JPY")
    args = ap.parse_args()

    m1_root = Path(args.m1_root)
    st_root = Path(args.stitched_root)
    st_root.mkdir(parents=True, exist_ok=True)

    outputs = []
    for pair in [p.strip() for p in args.pairs.split(",") if p.strip()]:
        try:
            outputs.append(str(stitch_pair(m1_root, st_root, pair)))
        except Exception as e:
            outputs.append(f"ERROR:{pair}:{e}")

    manifest = build_manifest(st_root)
    manifest["outputs"] = outputs
    (st_root / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    # Back-compat manifest location expected by audit scripts.
    m1_manifest = build_manifest(m1_root)
    (m1_root / "_manifest.json").write_text(json.dumps(m1_manifest, indent=2), encoding="utf-8")
    print("STITCH_OK", st_root / "_manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
