#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def run_eval(cache: str, patch: str, out: str, low: float, high: float, min_touched: int) -> dict:
    cmd = [
        "python3",
        "tools/state_replay_metrics.py",
        "--base-cache-in",
        cache,
        "--ceiling-mode",
        "first_passage",
        "--patch",
        patch,
        "--enforce-family-touch",
        "--min-touched-targets",
        str(min_touched),
        "--vol-cut-low-pct",
        str(low),
        "--vol-cut-high-pct",
        str(high),
        "--out",
        out,
    ]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0:
        return {"ok": False, "err": (p.stderr or p.stdout).strip()}
    d = json.loads(Path(out).read_text(encoding="utf-8"))
    dd = d.get("delta_vs_nopatch", {})
    sm = d.get("summary", {})
    return {
        "ok": True,
        "dd": dd,
        "summary": sm,
        "fingerprint": d.get("cache_fingerprint", ""),
        "source": d.get("source", {}),
    }


def gate_ok(dd: dict, exits_base: float) -> bool:
    return (
        float(dd.get("ddEE_mean", -1.0)) > 0.0
        and float(dd.get("ddCAP_mean", -1.0)) >= 0.0
        and float(dd.get("ddEph", -1.0)) > 0.0
        and float(dd.get("ddTail_mean_Eph", -1.0)) >= 0.0
        and float(dd.get("ddExits_per_hour", -1.0)) >= (-0.1 * float(exits_base or 0.0))
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="Derive vol bucket boundaries from constant-cache replay")
    ap.add_argument("--cache-s1", default="/tmp/base15fp_S1.json")
    ap.add_argument("--cache-s2", default="/tmp/base15fp_S2.json")
    ap.add_argument("--patch", required=True)
    ap.add_argument("--session", default="ASIA")
    ap.add_argument("--step-pct", type=int, default=5)
    ap.add_argument("--min-touched-targets", type=int, default=18)
    ap.add_argument("--min-vol-bucket-touched", type=int, default=1)
    ap.add_argument("--out-spec", default="calibration/vol_bucketing_spec_asia.json")
    ap.add_argument("--out-report", default="proof_artifacts/BUCKET_DERIVE_REPORT.md")
    args = ap.parse_args()

    vals = [x / 100.0 for x in range(20, 81, int(args.step_pct))]
    rows = []
    with tempfile.TemporaryDirectory(prefix="derive_vol_") as td:
        for low in vals:
            for high in vals:
                if high <= low + 0.10:
                    continue
                o1 = str(Path(td) / "s1.json")
                o2 = str(Path(td) / "s2.json")
                r1 = run_eval(args.cache_s1, args.patch, o1, low, high, args.min_touched_targets)
                r2 = run_eval(args.cache_s2, args.patch, o2, low, high, args.min_touched_targets)
                if not r1.get("ok") or not r2.get("ok"):
                    rows.append({"low": low, "high": high, "ok": False, "reason": r1.get("err") or r2.get("err")})
                    continue
                dd1 = r1["dd"]
                dd2 = r2["dd"]
                g1 = gate_ok(dd1, float(r1["summary"].get("exits_per_hour_base", 0.0)))
                g2 = gate_ok(dd2, float(r2["summary"].get("exits_per_hour_base", 0.0)))
                vb1 = dd1.get("vol_bucket_distribution", {}) or {}
                vb2 = dd2.get("vol_bucket_distribution", {}) or {}
                min_v = int(args.min_vol_bucket_touched)
                b1 = all(int(vb1.get(k, 0) or 0) >= min_v for k in ("VOL_LOW", "VOL_MID", "VOL_HIGH"))
                b2 = all(int(vb2.get(k, 0) or 0) >= min_v for k in ("VOL_LOW", "VOL_MID", "VOL_HIGH"))
                score = float(dd1.get("ddEph", 0.0)) + float(dd2.get("ddEph", 0.0))
                rows.append(
                    {
                        "low": low,
                        "high": high,
                        "ok": bool(g1 and g2 and b1 and b2),
                        "score": score,
                        "S1": dd1,
                        "S2": dd2,
                        "bucket_ok": {"S1": b1, "S2": b2},
                        "cache_fingerprints": {"S1": r1.get("fingerprint", ""), "S2": r2.get("fingerprint", "")},
                    }
                )

    good = [r for r in rows if r.get("ok")]
    good.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)
    best = good[0] if good else None

    spec = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "session": args.session,
        "basis": "session_local_percentile_rank",
        "search": {
            "step_pct": int(args.step_pct),
            "min_touched_targets": int(args.min_touched_targets),
            "min_vol_bucket_touched": int(args.min_vol_bucket_touched),
        },
        "selected": {
            "vol_low_hi": float(best["low"]) if best else 0.33,
            "vol_mid_hi": float(best["high"]) if best else 0.66,
            "vol_high_lo": float(best["high"]) if best else 0.66,
        },
        "proof": {
            "S1": (best or {}).get("S1", {}),
            "S2": (best or {}).get("S2", {}),
        },
        "cache_fingerprints": (best or {}).get("cache_fingerprints", {}),
        "top_candidates": good[:10],
    }
    Path(args.out_spec).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_spec).write_text(json.dumps(spec, indent=2), encoding="utf-8")

    lines = [
        "# Vol Bucket Derive Report",
        "",
        f"- patch: `{args.patch}`",
        f"- session: `{args.session}`",
        f"- min_touched_targets: `{args.min_touched_targets}`",
        f"- min_vol_bucket_touched: `{args.min_vol_bucket_touched}`",
        "",
    ]
    if best:
        lines += [
            "## Selected",
            f"- low/high percentiles: `{best['low']:.2f}` / `{best['high']:.2f}`",
            f"- score (ddEph S1+S2): `{best['score']}`",
            "",
            "## Proof",
            f"- S1: `{best['S1']}`",
            f"- S2: `{best['S2']}`",
            "",
        ]
    else:
        lines += ["## Selected", "- No candidate passed gates.", ""]
    lines += ["## Top 10", ""]
    for r in good[:10]:
        lines.append(f"- low={r['low']:.2f} high={r['high']:.2f} score={r['score']}")
    Path(args.out_report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_report).write_text("\n".join(lines), encoding="utf-8")

    print(json.dumps({"out_spec": args.out_spec, "out_report": args.out_report, "pass_count": len(good)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
