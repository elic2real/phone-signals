#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any


DEFAULT_KNOBS = {
    "entry.tick.base_max_dist_atr": [0.02],
    "entry.tick.confirm_disp_atr": [0.02],
    "aee.strictness_mult": [0.03],
    "aee.fail_windows": [1.0],
    "promote_mfe_atr": [0.03],
}


def _load(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _save(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def _run_verify(base_cache: str, tape_root: str, pairs: str, start: str, end: str, active: str, patch: str, out: str) -> dict[str, Any]:
    cmd = [
        "python3",
        "tools/state_replay_metrics.py",
        "--tape-root",
        tape_root,
        "--pairs",
        pairs,
        "--start-utc",
        start,
        "--end-utc",
        end,
        "--base-cache-in",
        base_cache,
        "--active-artifacts",
        active,
        "--patch",
        patch,
        "--out",
        out,
    ]
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stdout[-500:])
    return _load(out)


def _score(obj: dict[str, Any]) -> dict[str, float]:
    d = obj.get("delta_vs_nopatch") or {}
    return {
        "ddEph": float(d.get("ddEph", 0.0) or 0.0),
        "ddTail": float(d.get("ddTail_mean_Eph", 0.0) or 0.0),
        "ddCAP": float(d.get("ddCAP_mean", 0.0) or 0.0),
        "ddExitsH": float(d.get("ddExits_per_hour", 0.0) or 0.0),
    }


def _perturb(patch_obj: dict[str, Any], knob: str, delta: float) -> dict[str, Any]:
    out = json.loads(json.dumps(patch_obj))
    for p in out.get("patches", []):
        if not isinstance(p, dict):
            continue
        if str(p.get("level", "")) != "SESSION_FAMILY":
            continue
        knobs = p.setdefault("knobs", {})
        cur = float(knobs.get(knob, 0.0) or 0.0)
        if knob == "aee.fail_windows":
            knobs[knob] = max(1, int(round(cur + delta)))
        else:
            knobs[knob] = cur + delta
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--session", required=True, choices=["ASIA", "LONDON", "NY"])
    ap.add_argument("--patch", required=True)
    ap.add_argument("--base-cache", required=True)
    ap.add_argument("--tape-root", required=True)
    ap.add_argument("--pairs", required=True)
    ap.add_argument("--start-utc", required=True)
    ap.add_argument("--end-utc", required=True)
    ap.add_argument("--active-artifacts", default="calibration/active/ACTIVE_ARTIFACTS.json")
    ap.add_argument("--out-md", default="")
    args = ap.parse_args()

    base_out = f"proof_artifacts/SENS_BASE_{args.session}.json"
    base = _run_verify(
        args.base_cache,
        args.tape_root,
        args.pairs,
        args.start_utc,
        args.end_utc,
        args.active_artifacts,
        args.patch,
        base_out,
    )
    base_s = _score(base)
    patch_obj = _load(args.patch)
    rows = []
    for knob, deltas in DEFAULT_KNOBS.items():
        for d in deltas:
            for sign in (-1.0, 1.0):
                step = sign * float(d)
                c = _perturb(patch_obj, knob, step)
                cp = Path(f"calibration/candidates/sensitivity_{args.session}_{knob.replace('.', '_')}_{'p' if step>=0 else 'm'}{str(abs(step)).replace('.','p')}.json")
                _save(cp, c)
                out = f"proof_artifacts/SENS_{args.session}_{knob}_{step:+.3f}.json".replace("/", "_")
                obj = _run_verify(
                    args.base_cache,
                    args.tape_root,
                    args.pairs,
                    args.start_utc,
                    args.end_utc,
                    args.active_artifacts,
                    str(cp),
                    out,
                )
                s = _score(obj)
                rows.append(
                    {
                        "knob": knob,
                        "delta": step,
                        "d_ddEph": s["ddEph"] - base_s["ddEph"],
                        "d_ddTail": s["ddTail"] - base_s["ddTail"],
                        "d_ddCAP": s["ddCAP"] - base_s["ddCAP"],
                        "d_ddExitsH": s["ddExitsH"] - base_s["ddExitsH"],
                    }
                )
    rows.sort(key=lambda r: (abs(r["d_ddEph"]), -r["d_ddTail"]), reverse=True)
    out_md = args.out_md or f"proof_artifacts/SENSITIVITY_{args.session}.md"
    lines = [f"# Sensitivity {args.session}", "", f"- base patch: `{args.patch}`", ""]
    for r in rows:
        lines.append(
            f"- `{r['knob']}` delta={r['delta']:+.3f} d_ddEph={r['d_ddEph']:+.6f} d_ddTail={r['d_ddTail']:+.6f} d_ddCAP={r['d_ddCAP']:+.6f} d_ddExitsH={r['d_ddExitsH']:+.6f}"
        )
    Path(out_md).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(out_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
