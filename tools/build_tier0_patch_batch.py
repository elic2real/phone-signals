#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top-groups-json", default="/tmp/top10_groups.json")
    ap.add_argument("--max-groups", type=int, default=5)
    ap.add_argument("--out", default="calibration/tune_map_patch.json")
    args = ap.parse_args()

    groups = json.loads(Path(args.top_groups_json).read_text(encoding="utf-8"))
    selected = groups[: max(1, int(args.max_groups))]

    patches = []
    for g in selected:
        session = str(g.get("session", ""))
        quarter = str(g.get("quarter", ""))
        weekday = str(g.get("weekday", ""))
        ee = float(g.get("expected_extraction_atr", 0.0) or 0.0)
        key = f"session={session}|quarter={quarter}|dow={weekday}"

        # Capture-preserving bundle updates.
        # In current replay surrogate, overly loose settings hurt capture; this batch biases negative-adj.
        if ee < -0.05:
            knobs = {
                "entry.tick.base_max_dist_atr": 0.24,
                "entry.tick.dist_vel_k": 0.14,
                "entry.tick.confirm_m1_closes": 2,
                "entry.tick.confirm_sec": 4.0,
                "aee.strictness_mult": 1.08,
                "aee.near_tp_band_atr": 0.28,
            }
        elif ee < -0.02:
            knobs = {
                "entry.tick.base_max_dist_atr": 0.26,
                "entry.tick.dist_vel_k": 0.16,
                "entry.tick.confirm_m1_closes": 2,
                "entry.tick.confirm_sec": 3.5,
                "aee.strictness_mult": 1.05,
                "aee.near_tp_band_atr": 0.27,
            }
        else:
            knobs = {
                "entry.tick.base_max_dist_atr": 0.28,
                "entry.tick.dist_vel_k": 0.18,
                "entry.tick.confirm_m1_closes": 2,
                "entry.tick.confirm_sec": 3.5,
                "aee.strictness_mult": 1.02,
                "aee.near_tp_band_atr": 0.26,
            }

        patches.append(
            {
                "mode": "ENTRY",
                "level": "SESSION_GLOBAL",
                "key": f"session={session}",
                "knobs": {k: v for k, v in knobs.items() if k.startswith("entry.")},
                "meta": {
                    "parent_key_used": "GLOBAL",
                    "state_group": key,
                    "evidence_n": int(g.get("n", 0) or 0),
                    "expected_extraction_atr": ee,
                },
            }
        )
        patches.append(
            {
                "mode": "AEE",
                "level": "SESSION_GLOBAL",
                "key": f"session={session}",
                "knobs": {k: v for k, v in knobs.items() if k.startswith("aee.")},
                "meta": {
                    "parent_key_used": "GLOBAL",
                    "state_group": key,
                    "evidence_n": int(g.get("n", 0) or 0),
                    "expected_extraction_atr": ee,
                },
            }
        )

    out_obj: dict[str, Any] = {
        "version": f"patch-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        "window": {"source": "tier0_batch_from_state_audit"},
        "inputs": {"top_groups_json": args.top_groups_json, "max_groups": int(args.max_groups)},
        "patches": patches,
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out_obj, indent=2), encoding="utf-8")
    print(f"WROTE {out_path}")
    print(f"patch_count={len(patches)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
