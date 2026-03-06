#!/usr/bin/env python3
import argparse
import json
import os
import subprocess
import time
from pathlib import Path


def _cache_ceiling_args(base_cache: str) -> list[str]:
    try:
        obj = json.load(open(base_cache, "r", encoding="utf-8"))
        src = obj.get("source", {}) if isinstance(obj, dict) else {}
        mode = str(src.get("ceiling_mode", "proxy"))
        args = ["--ceiling-mode", mode]
        if mode == "first_passage":
            args += ["--x-atr", str(float(src.get("x_atr", 1.0))), "--y-atr", str(float(src.get("y_atr", 0.5)))]
        return args
    except Exception:
        return []


def _run_state_eval(base_cache: str, patch_path: str, out_path: str) -> dict:
    cmd = [
        "python3",
        "tools/state_replay_metrics.py",
        "--base-cache-in",
        base_cache,
        "--enforce-family-touch",
        "--patch",
        patch_path,
        "--out",
        out_path,
    ]
    cmd.extend(_cache_ceiling_args(base_cache))
    subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    with open(out_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _aggregate_rows(doc: dict) -> dict:
    rows = doc.get("rows", [])
    valid = [r for r in rows if r.get("n", 0) > 0 and r.get("delta_expected_extraction_atr") is not None]
    if not valid:
        return {
            "delta_expected_extraction_atr": 0.0,
            "delta_capture_to_ceiling": 0.0,
            "covered_states": 0,
            "total_states": len(rows),
        }
    n = len(valid)
    d_ee = sum(float(r.get("delta_expected_extraction_atr", 0.0)) for r in valid) / n
    d_cap = sum(float(r.get("delta_capture_to_ceiling", 0.0)) for r in valid) / n
    covered = sum(1 for r in valid if float(r.get("delta_expected_extraction_atr", 0.0)) > 0.0)
    return {
        "delta_expected_extraction_atr": d_ee,
        "delta_capture_to_ceiling": d_cap,
        "covered_states": covered,
        "total_states": n,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("cache_dir", help="Directory with base cache JSON files")
    ap.add_argument("candidates_file", help="JSON list of candidates")
    ap.add_argument("--base-a", default="", help="Override base cache A path")
    ap.add_argument("--base-b", default="", help="Override base cache B path")
    ap.add_argument("--out", default="policy_sweep_output.json")
    args = ap.parse_args()

    t0 = time.time()
    cache_dir = Path(args.cache_dir)
    with open(args.candidates_file, "r", encoding="utf-8") as f:
        candidates = json.load(f)
    if not isinstance(candidates, list):
        raise SystemExit("candidates_file must be a JSON list")

    base_a = args.base_a or str(cache_dir / "base_A.json")
    base_b = args.base_b or str(cache_dir / "base_B.json")
    if not os.path.exists(base_a):
        raise SystemExit(f"Missing base cache A: {base_a}")
    if not os.path.exists(base_b):
        raise SystemExit(f"Missing base cache B: {base_b}")

    load_time = time.time() - t0
    results = []
    candidate_eval_start = time.time()

    os.makedirs("proof_artifacts", exist_ok=True)
    for cand in candidates:
        name = cand.get("name")
        patch_path = cand.get("patch")
        knobs = cand.get("knobs", {})
        if not name or not patch_path:
            continue
        if not os.path.exists(patch_path):
            continue

        out_a = f"proof_artifacts/LANE_A_SWEEP_{name}_A.json"
        out_b = f"proof_artifacts/LANE_A_SWEEP_{name}_B.json"
        doc_a = _run_state_eval(base_a, patch_path, out_a)
        doc_b = _run_state_eval(base_b, patch_path, out_b)
        agg_a = _aggregate_rows(doc_a)
        agg_b = _aggregate_rows(doc_b)

        results.append(
            {
                "candidate": name,
                "patch": patch_path,
                "knobs": knobs,
                "slice_a": agg_a,
                "slice_b": agg_b,
                "delta_expected_extraction_atr": (agg_a["delta_expected_extraction_atr"] + agg_b["delta_expected_extraction_atr"]) / 2.0,
                "delta_capture_to_ceiling": (agg_a["delta_capture_to_ceiling"] + agg_b["delta_capture_to_ceiling"]) / 2.0,
                "covered_states": agg_a["covered_states"] + agg_b["covered_states"],
                "total_states": agg_a["total_states"] + agg_b["total_states"],
            }
        )

    candidate_eval_time = time.time() - candidate_eval_start
    aggregate_time = time.time() - t0

    output = {
        "ranked_candidates": sorted(results, key=lambda x: x["delta_expected_extraction_atr"], reverse=True),
        "timing": {
            "load": load_time,
            "base_eval": 0.0,
            "candidate_eval": candidate_eval_time,
            "aggregate": aggregate_time,
        },
        "source": {
            "base_a": base_a,
            "base_b": base_b,
            "candidates_file": args.candidates_file,
        },
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)
    print(f"WROTE {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
