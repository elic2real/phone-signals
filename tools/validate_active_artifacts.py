#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from active_artifacts import load_active_artifacts
from vol_bucket_spec import load_vol_bucket_spec, validate_vol_bucket_spec


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--active-artifacts", default="calibration/active/ACTIVE_ARTIFACTS.json")
    args = ap.parse_args()

    aa = load_active_artifacts(args.active_artifacts)
    out = {"active_artifacts_sha256": aa.get("active_artifacts_sha256", ""), "sessions": {}}
    for s in ("ASIA", "LONDON", "NY"):
        row = aa["sessions"][s]
        spec = load_vol_bucket_spec(row["vol_spec"])
        validate_vol_bucket_spec(spec, s, k_expected=int(row["k"]))
        out["sessions"][s] = {
            "patch_sha256": row["patch_sha256"],
            "vol_spec_sha256": row["vol_spec_sha256"],
            "k": row["k"],
            "min_touched_targets": row["min_touched_targets"],
            "min_vol_bucket_touched": row["min_vol_bucket_touched"],
        }
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
