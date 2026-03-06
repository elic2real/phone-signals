#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from copy import deepcopy
from pathlib import Path
from typing import Any


def _load(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _save(path: str, obj: dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2) + "\n", encoding="utf-8")


def _parse_key(key: str) -> tuple[str, str, str, str]:
    pair, sq, vb = (key.split("|") + ["", "", ""])[:3]
    session, quarter = (sq.split("_") + ["", ""])[:2]
    return pair, session, quarter, vb


def _exercise_cache(base: dict[str, Any], fallback_keys: list[str]) -> tuple[dict[str, Any], int]:
    out = deepcopy(base)
    states = out.get("states") or []
    by_psv: dict[tuple[str, str, str], dict[str, Any]] = {}
    present: set[str] = set()
    for s in states:
        pair = str(s.get("pair", "") or "")
        sess = str(s.get("session", "") or "")
        q = str(s.get("quarter", "") or "")
        vb = str(s.get("vol_bucket", "VOL_MID") or "VOL_MID")
        present.add(f"{pair}|{sess}_{q}|{vb}")
        by_psv.setdefault((pair, sess, vb), s)
    added = 0
    for k in fallback_keys:
        if k in present:
            continue
        pair, sess, q, vb = _parse_key(k)
        src = by_psv.get((pair, sess, vb))
        if src is None:
            # fallback to any same pair/session state
            src = next((s for s in states if str(s.get("pair", "")) == pair and str(s.get("session", "")) == sess), None)
        if src is None:
            continue
        ns = deepcopy(src)
        ns["pair"] = pair
        ns["session"] = sess
        ns["quarter"] = q
        ns["vol_bucket"] = vb
        states.append(ns)
        present.add(k)
        added += 1
    out["states"] = states
    return out, added


def _run_replay(cache_in: str, patch: str, out_path: str, args: argparse.Namespace) -> dict[str, Any]:
    cmd = [
        "python3",
        "tools/state_replay_metrics.py",
        "--base-cache-in",
        cache_in,
        "--ceiling-mode",
        "first_passage",
        "--active-artifacts",
        args.active_artifacts,
        "--patch",
        patch,
        "--min-touched-targets",
        "0",
        "--min-vol-bucket-touched",
        "0",
        "--out",
        out_path,
    ]
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stdout[-1000:])
    return _load(out_path)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--final-map", default="calibration/final_ceiling_map_15p.json")
    ap.add_argument("--s1", default="/tmp/base15fp_S1.json")
    ap.add_argument("--s2", default="/tmp/base15fp_S2.json")
    ap.add_argument("--active-artifacts", default="calibration/active/ACTIVE_ARTIFACTS.json")
    ap.add_argument("--out-prefix", default="proof_artifacts/FALLBACK_EXERCISE")
    ap.add_argument("--report", default="proof_artifacts/ceiling_campaign_report.json")
    args = ap.parse_args()

    fm = _load(args.final_map)
    fallback_patches = [p for p in fm.get("patches", []) if (p.get("meta") or {}).get("source") == "fallback"]
    fallback_keys = [str(p.get("key", "") or "") for p in fallback_patches if p.get("key")]
    noop_policy = all((p.get("knobs") or {}) == {} for p in fallback_patches) if fallback_patches else False
    fallback_only_patch = {"patches": [{"level": "SESSION_PAIR", "key": p["key"], "knobs": p.get("knobs", {})} for p in fallback_patches]}
    patch_path = f"{args.out_prefix}_PATCH.json"
    _save(patch_path, fallback_only_patch)

    b1 = _load(args.s1)
    b2 = _load(args.s2)
    e1, added1 = _exercise_cache(b1, fallback_keys)
    e2, added2 = _exercise_cache(b2, fallback_keys)
    c1 = f"{args.out_prefix}_S1_CACHE.json"
    c2 = f"{args.out_prefix}_S2_CACHE.json"
    _save(c1, e1)
    _save(c2, e2)

    o1 = _run_replay(c1, patch_path, f"{args.out_prefix}_S1.json", args)
    o2 = _run_replay(c2, patch_path, f"{args.out_prefix}_S2.json", args)
    d1 = o1.get("delta_vs_nopatch") or {}
    d2 = o2.get("delta_vs_nopatch") or {}

    top1 = sorted((d1.get("touched_patch_keys_counts") or {}).items(), key=lambda kv: kv[1], reverse=True)[:20]
    top2 = sorted((d2.get("touched_patch_keys_counts") or {}).items(), key=lambda kv: kv[1], reverse=True)[:20]
    touched_keys = set((d1.get("touched_patch_keys_counts") or {}).keys()) | set((d2.get("touched_patch_keys_counts") or {}).keys())
    touched_fb = len([k for k in touched_keys if k in set(fallback_keys)])
    base_non_harm = (
        float(d1.get("ddEph", 0.0) or 0.0) >= 0.0
        and float(d2.get("ddEph", 0.0) or 0.0) >= 0.0
        and float(d1.get("ddCAP_mean", 0.0) or 0.0) >= 0.0
        and float(d2.get("ddCAP_mean", 0.0) or 0.0) >= 0.0
        and float(d1.get("ddTail_mean_Eph", 0.0) or 0.0) >= 0.0
        and float(d2.get("ddTail_mean_Eph", 0.0) or 0.0) >= 0.0
        and float(d1.get("ddExits_per_hour", 0.0) or 0.0) >= 0.0
        and float(d2.get("ddExits_per_hour", 0.0) or 0.0) >= 0.0
    )
    if noop_policy:
        pass_non_harm = base_non_harm
    else:
        pass_non_harm = base_non_harm and int(d1.get("touched_targets", 0) or 0) >= 50 and int(d2.get("touched_targets", 0) or 0) >= 50
    block = {
        "mode": "synthetic_exercise_cache",
        "notes": "Fallback keys not present in truth S1/S2 caches; exercise uses deterministic remap from nearest same-pair/session/vol states.",
        "fallback_policy_noop": noop_policy,
        "inputs": {
            "final_map": args.final_map,
            "fallback_key_count": len(fallback_keys),
            "added_states_s1": added1,
            "added_states_s2": added2,
            "exercise_cache_s1": c1,
            "exercise_cache_s2": c2,
            "fallback_patch": patch_path,
        },
        "results": {
            "S1": {
                "ddEph": float(d1.get("ddEph", 0.0) or 0.0),
                "ddCAP": float(d1.get("ddCAP_mean", 0.0) or 0.0),
                "ddTail": float(d1.get("ddTail_mean_Eph", 0.0) or 0.0),
                "ddExits_per_hour": float(d1.get("ddExits_per_hour", 0.0) or 0.0),
                "touched_targets": int(d1.get("touched_targets", 0) or 0),
                "fallback_source_confirmed": int((d1.get("matched_level_counts") or {}).get("SESSION_PAIR", 0) or 0) > 0,
                "top_fallback_keys_touched": top1,
            },
            "S2": {
                "ddEph": float(d2.get("ddEph", 0.0) or 0.0),
                "ddCAP": float(d2.get("ddCAP_mean", 0.0) or 0.0),
                "ddTail": float(d2.get("ddTail_mean_Eph", 0.0) or 0.0),
                "ddExits_per_hour": float(d2.get("ddExits_per_hour", 0.0) or 0.0),
                "touched_targets": int(d2.get("touched_targets", 0) or 0),
                "fallback_source_confirmed": int((d2.get("matched_level_counts") or {}).get("SESSION_PAIR", 0) or 0) > 0,
                "top_fallback_keys_touched": top2,
            },
            "fallback_keys_touched_count_union": touched_fb,
        },
        "pass_non_harmful": pass_non_harm,
    }

    rp = Path(args.report)
    report = _load(args.report) if rp.exists() else {}
    report["fallback_exercise"] = block
    rp.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(args.report)
    print(f"pass_non_harmful={pass_non_harm}")
    print(f"fallback_keys_touched_count_union={touched_fb}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
