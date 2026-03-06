#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

try:
    import pandas as pd
except Exception:
    pd = None


DEFAULT_KNOBS = [
    "entry.tick.base_max_dist_atr",
    "entry.tick.dist_vel_k",
    "entry.tick.confirm_m1_closes",
    "entry.tick.confirm_sec",
    "aee.strictness_mult",
    "aee.near_tp_band_atr",
]


def _hash(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tape", default="data_tape_stitched")
    ap.add_argument("--out", default="tunes/tune_map_seed_v2.json")
    ap.add_argument("--knobs", default=",".join(DEFAULT_KNOBS))
    args = ap.parse_args()

    tape = Path(args.tape)
    out = Path(args.out)
    knobs_allow = [k.strip() for k in args.knobs.split(",") if k.strip()]

    levels = {"GLOBAL": {}, "SESSION_GLOBAL": {}, "SESSION_PAIR": {}, "COARSE": {}}
    rows = 0
    session_pair_counts: dict[str, int] = {}
    session_global_counts: dict[str, int] = {}
    coarse_counts: dict[str, int] = {}
    pairs_seen: set[str] = set()

    def session_from_hour(h: int) -> str:
        if 8 <= h < 16:
            return "LONDON"
        if 14 <= h < 21:
            return "NY"
        return "ASIA"

    for p in tape.rglob("stitched.parquet"):
        pair = p.parent.name.replace("pair=", "")
        pairs_seen.add(pair)
        if pd is None:
            rows += 1
            continue
        try:
            df = pd.read_parquet(p)
            rows += len(df)
            if "timestamp" not in df.columns or len(df) == 0:
                continue
            ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
            ts = ts.dropna()
            if len(ts) == 0:
                continue
            sess = ts.dt.hour.map(session_from_hour)
            for s, n in sess.value_counts().items():
                k_sp = f"session={s}|pair={pair}"
                session_pair_counts[k_sp] = session_pair_counts.get(k_sp, 0) + int(n)
                k_sg = f"session={s}"
                session_global_counts[k_sg] = session_global_counts.get(k_sg, 0) + int(n)
                k_c = f"session={s}|pair={pair}|speed=MED"
                coarse_counts[k_c] = coarse_counts.get(k_c, 0) + int(n)
        except Exception:
            rows += 1

    # Conservative default seed values.
    base = {
        "entry.tick.base_max_dist_atr": 0.30,
        "entry.tick.dist_vel_k": 0.20,
        "entry.tick.confirm_m1_closes": 1,
        "entry.tick.confirm_sec": 3.0,
        "aee.strictness_mult": 1.0,
        "aee.near_tp_band_atr": 0.25,
    }
    base = {k: v for k, v in base.items() if k in knobs_allow}
    levels["GLOBAL"]["GLOBAL"] = base
    # Slightly session-shaped defaults with bounded ranges.
    for k in sorted(session_global_counts.keys()):
        sess = k.split("=", 1)[1]
        v = dict(base)
        if sess == "LONDON":
            v["entry.tick.base_max_dist_atr"] = min(0.40, float(v.get("entry.tick.base_max_dist_atr", 0.30) + 0.05))
            v["entry.tick.confirm_sec"] = max(1.0, float(v.get("entry.tick.confirm_sec", 3.0) - 1.0))
        elif sess == "ASIA":
            v["entry.tick.confirm_m1_closes"] = int(v.get("entry.tick.confirm_m1_closes", 1) + 1)
            v["aee.strictness_mult"] = min(1.2, float(v.get("aee.strictness_mult", 1.0) + 0.05))
        levels["SESSION_GLOBAL"][k] = {kk: vv for kk, vv in v.items() if kk in knobs_allow}

    for k in sorted(session_pair_counts.keys()):
        v = dict(base)
        if "pair=AUD_" in k:
            v["entry.tick.dist_vel_k"] = min(0.35, float(v.get("entry.tick.dist_vel_k", 0.20) + 0.05))
        if "session=NY" in k:
            v["aee.near_tp_band_atr"] = max(0.20, float(v.get("aee.near_tp_band_atr", 0.25) - 0.03))
        levels["SESSION_PAIR"][k] = {kk: vv for kk, vv in v.items() if kk in knobs_allow}

    for k in sorted(coarse_counts.keys()):
        v = dict(base)
        levels["COARSE"][k] = {kk: vv for kk, vv in v.items() if kk in knobs_allow}

    tape_manifest = tape / "_manifest.json"
    tape_manifest_hash = ""
    if tape_manifest.exists():
        tape_manifest_hash = _hash(tape_manifest.read_text(encoding="utf-8"))
    manifest_hash = _hash(json.dumps({"tape": str(tape), "rows": rows, "tape_manifest_hash": tape_manifest_hash}, sort_keys=True))
    doc = {
        "levels": levels,
        "meta": {
            "generated_by": "tune_map_generate.py",
            "rows": rows,
            "pairs_seen": sorted(pairs_seen),
            "bucket_counts": {
                "COARSE": len(levels["COARSE"]),
                "SESSION_PAIR": len(levels["SESSION_PAIR"]),
                "SESSION_GLOBAL": len(levels["SESSION_GLOBAL"]),
                "GLOBAL": len(levels["GLOBAL"]),
            },
            "manifest_hash": manifest_hash,
            "tape_manifest_hash": tape_manifest_hash,
            "knobs_allowlist": knobs_allow,
        },
    }
    # Back-compat top-level mirrors expected by existing audit snippets.
    doc["COARSE"] = levels["COARSE"]
    doc["SESSION_PAIR"] = levels["SESSION_PAIR"]
    doc["SESSION_GLOBAL"] = levels["SESSION_GLOBAL"]
    doc["GLOBAL"] = levels["GLOBAL"]

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2, sort_keys=True), encoding="utf-8")
    # Also keep compatibility path.
    compat = out.parent / "tune_map_seed.json"
    compat.write_text(json.dumps(doc, indent=2, sort_keys=True), encoding="utf-8")
    print("SEED_GEN_OK", out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
