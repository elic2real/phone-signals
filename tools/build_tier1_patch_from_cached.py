#!/usr/bin/env python3
import argparse
import json
import os
from datetime import datetime, timezone


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sweep_output_file")
    ap.add_argument("--top-n", type=int, default=3)
    ap.add_argument("--out-patch", default="calibration/tune_map_patch_tier1_narrow.json")
    ap.add_argument("--out-report", default="proof_artifacts/CACHED_SWEEP_REPORT.json")
    args = ap.parse_args()

    with open(args.sweep_output_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    ranked = data.get("ranked_candidates", [])
    top_candidates = ranked[: args.top_n]

    # Merge top candidate patch files into a valid tune_map_patch schema.
    # Priority order is ranking order: earlier candidate wins on duplicate key.
    merged = {}
    selected_sources = []
    for cand in top_candidates:
        patch_path = cand.get("patch", "")
        if not patch_path or not os.path.exists(patch_path):
            continue
        src = json.load(open(patch_path, "r", encoding="utf-8"))
        patches = src.get("patches", []) if isinstance(src, dict) else []
        for p in patches:
            dedupe = (p.get("mode"), p.get("level"), p.get("key"))
            if dedupe in merged:
                continue
            p2 = dict(p)
            meta = dict(p2.get("meta", {}) or {})
            meta.update(
                {
                    "narrow_source_candidate": cand.get("candidate"),
                    "narrow_source_patch": patch_path,
                    "narrow_slice_deltas": {
                        "delta_expected_extraction_atr": cand.get("delta_expected_extraction_atr"),
                        "delta_capture_to_ceiling": cand.get("delta_capture_to_ceiling"),
                        "covered_states": cand.get("covered_states"),
                        "total_states": cand.get("total_states"),
                    },
                }
            )
            p2["meta"] = meta
            merged[dedupe] = p2
        selected_sources.append({"candidate": cand.get("candidate"), "patch": patch_path, "patch_count": len(patches)})

    patch = {
        "version": f"TIER1_NARROW_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        "window": "cached_sweep_narrow",
        "inputs": {
            "sweep_output_file": args.sweep_output_file,
            "selected_top_n": min(args.top_n, len(top_candidates)),
            "selected_sources": selected_sources,
        },
        "patches": list(merged.values()),
    }

    os.makedirs(os.path.dirname(args.out_patch), exist_ok=True)
    with open(args.out_patch, "w", encoding="utf-8") as f:
        json.dump(patch, f, indent=2)

    os.makedirs(os.path.dirname(args.out_report), exist_ok=True)
    report = {
        "summary": f"Processed {len(ranked)} candidates, selected top {min(args.top_n, len(ranked))}",
        "top_candidates": top_candidates,
        "timing": data.get("timing", {}),
        "source": data.get("source", {}),
        "out_patch": args.out_patch,
        "out_patch_count": len(patch["patches"]),
    }
    with open(args.out_report, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"WROTE {args.out_patch}")
    print(f"WROTE {args.out_report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
