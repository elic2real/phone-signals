#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path
from typing import Any


PAIR_SET_15 = "EUR_USD,GBP_USD,USD_JPY,USD_CHF,AUD_USD,USD_CAD,NZD_USD,EUR_GBP,EUR_JPY,GBP_JPY,AUD_JPY,CHF_JPY,EUR_CHF,AUD_CAD,NZD_JPY"


def _load(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _save(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def _active_session_patch_path(active_artifacts_path: str, session: str) -> str:
    try:
        obj = json.loads(Path(active_artifacts_path).read_text(encoding="utf-8"))
        return str((((obj.get("sessions") or {}).get(session) or {}).get("patch") or "")).strip()
    except Exception:
        return ""


def _run_verify(base_cache: str, patch: str, out: str, args: argparse.Namespace, enforce_touches: bool = True) -> dict[str, Any]:
    cmd = [
        "python3",
        "tools/state_replay_metrics.py",
        "--tape-root",
        args.tape_root,
        "--pairs",
        args.pairs,
        "--start-utc",
        args.start_utc,
        "--end-utc",
        args.end_utc,
        "--ceiling-mode",
        args.ceiling_mode,
        "--base-cache-in",
        base_cache,
        "--active-artifacts",
        args.active_artifacts,
        "--vol-spec",
        args.vol_spec,
        "--patch",
        patch,
        "--out",
        out,
    ]
    if enforce_touches:
        cmd.extend(
            [
                "--enforce-tier-touches",
                "--enforce-quarter-no-shadow",
                "--min-touched-targets",
                str(args.min_touched_targets),
                "--min-vol-bucket-touched",
                str(args.min_vol_bucket_touched),
            ]
        )
    r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stdout[-1000:])
    return _load(out)


def _gate_single(o: dict[str, Any], args: argparse.Namespace) -> tuple[bool, dict[str, float | str]]:
    d = o.get("delta_vs_nopatch") or {}
    touched = int(d.get("touched_targets", 0) or 0)
    neg_ddcap = int(d.get("touched_targets_neg_ddCAP", 0) or 0)
    worst_ddcap_raw = d.get("worst_touched_ddCAP")
    worst_ddcap = float(worst_ddcap_raw) if worst_ddcap_raw is not None else 0.0
    allowed_neg = int(args.max_neg_ddcap_count)
    if allowed_neg < 0:
        allowed_neg = max(1, int(float(args.max_neg_ddcap_frac) * max(0, touched)))
    ok = (
        float(d.get("ddEE_mean", 0.0) or 0.0) > 0.0
        and float(d.get("ddCAP_mean", 0.0) or 0.0) >= 0.0
        and float(d.get("ddEph", 0.0) or 0.0) > 0.0
        and float(d.get("ddTail_mean_Eph", 0.0) or 0.0) >= 0.0
        and float(d.get("ddExits_per_hour", 0.0) or 0.0) >= 0.0
        and touched >= int(args.min_touched_targets)
        and neg_ddcap <= allowed_neg
        and worst_ddcap >= float(args.min_worst_ddcap)
        and min(
            (d.get("vol_bucket_distribution") or {"VOL_LOW": 0, "VOL_MID": 0, "VOL_HIGH": 0}).values()
        )
        >= int(args.min_vol_bucket_touched)
    )
    return ok, {
        "ddEph": float(d.get("ddEph", 0.0) or 0.0),
        "ddTail": float(d.get("ddTail_mean_Eph", 0.0) or 0.0),
        "ddCAP": float(d.get("ddCAP_mean", 0.0) or 0.0),
        "ddEE": float(d.get("ddEE_mean", 0.0) or 0.0),
        "ddExits_per_hour": float(d.get("ddExits_per_hour", 0.0) or 0.0),
        "touched": float(touched),
        "neg_ddcap": float(neg_ddcap),
        "worst_ddcap": float(worst_ddcap),
        "cache_fingerprint": str(o.get("cache_fingerprint", "") or ""),
        "active_artifacts_sha256": str((o.get("source") or {}).get("active_artifacts_sha256", "") or ""),
    }


def _gate_pair(o_s1: dict[str, Any], o_s2: dict[str, Any], args: argparse.Namespace) -> tuple[bool, dict[str, Any]]:
    ok1, m1 = _gate_single(o_s1, args)
    ok2, m2 = _gate_single(o_s2, args)
    return bool(ok1 and ok2), {
        "s1": m1,
        "s2": m2,
        "score": {
            "ddEph": (float(m1["ddEph"]) + float(m2["ddEph"])) / 2.0,
            "ddTail": (float(m1["ddTail"]) + float(m2["ddTail"])) / 2.0,
            "ddCAP": (float(m1["ddCAP"]) + float(m2["ddCAP"])) / 2.0,
            "neg_ddcap_total": float(m1["neg_ddcap"]) + float(m2["neg_ddcap"]),
        },
    }


def _score(metrics: dict[str, Any]) -> tuple[float, float, float, float]:
    s = metrics["score"]
    return (float(s["ddEph"]), float(s["ddTail"]), float(s["ddCAP"]), -float(s["neg_ddcap_total"]))


def _session_key_matches(key: str, session: str) -> bool:
    return f"|{session}|" in key or f"|{session}_Q" in key


def _apply_knob(obj: dict[str, Any], knob: str, val: float, session: str) -> dict[str, Any]:
    out = json.loads(json.dumps(obj))
    for p in out.get("patches", []):
        if not isinstance(p, dict):
            continue
        if str(p.get("level", "")) != "SESSION_FAMILY":
            continue
        if not _session_key_matches(str(p.get("key", "") or ""), session):
            continue
        k = p.setdefault("knobs", {})
        if knob == "aee.fail_windows":
            k[knob] = int(val)
        else:
            k[knob] = float(val)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--session", required=True, choices=["ASIA", "LONDON", "NY"])
    ap.add_argument("--start-patch", required=True)
    ap.add_argument("--vol-spec", default="calibration/active/vol_bucket_spec_active_asia.json")
    ap.add_argument("--base-cache-s1", default="/tmp/base15fp_S1.json")
    ap.add_argument("--base-cache-s2", default="/tmp/base15fp_S2.json")
    ap.add_argument("--tape-root", default="data_tape_oanda_m5_15_stitched")
    ap.add_argument("--pairs", default=PAIR_SET_15)
    ap.add_argument("--start-utc", default="2025-12-01T00:00:00Z")
    ap.add_argument("--end-utc", default="2026-03-01T00:00:00Z")
    ap.add_argument("--ceiling-mode", default="first_passage", choices=["proxy", "first_passage"])
    ap.add_argument("--active-artifacts", default="calibration/active/ACTIVE_ARTIFACTS.json")
    ap.add_argument("--min-touched-targets", type=int, default=24)
    ap.add_argument("--min-vol-bucket-touched", type=int, default=4)
    ap.add_argument("--max-neg-ddcap-count", type=int, default=-1)
    ap.add_argument("--max-neg-ddcap-frac", type=float, default=0.02)
    ap.add_argument("--min-worst-ddcap", type=float, default=-0.002)
    ap.add_argument("--max-iters", type=int, default=3)
    ap.add_argument("--out-patch", default="")
    ap.add_argument("--out-json", default="")
    ap.add_argument("--out-md", default="")
    args = ap.parse_args()

    grid = {
        "entry.tick.base_max_dist_atr": [0.20, 0.25, 0.30],
        "entry.tick.confirm_disp_atr": [0.10, 0.12, 0.14],
        "aee.strictness_mult": [0.97, 1.00, 1.03],
        "aee.fail_windows": [2, 3, 4],
        "promote_mfe_atr": [0.20, 0.25, 0.30],
    }

    cur = _load(args.start_patch)
    active_session_patch = _active_session_patch_path(args.active_artifacts, args.session)
    if active_session_patch and Path(args.start_patch).resolve() == Path(active_session_patch).resolve():
        # The active patch is already applied by --active-artifacts in replay.
        # Use a no-op delta patch as the CA start point to avoid double-applying.
        cur = {"patches": []}
    work = Path("calibration/candidates/coordinate_ascent")
    work.mkdir(parents=True, exist_ok=True)
    cur_path = work / f"{args.session}_ca_iter0.json"
    _save(cur_path, cur)
    base_s1 = _run_verify(
        args.base_cache_s1,
        str(cur_path),
        f"proof_artifacts/CA_{args.session}_iter0_S1.json",
        args,
        enforce_touches=False,
    )
    base_s2 = _run_verify(
        args.base_cache_s2,
        str(cur_path),
        f"proof_artifacts/CA_{args.session}_iter0_S2.json",
        args,
        enforce_touches=False,
    )
    ok, cur_m = _gate_pair(base_s1, base_s2, args)
    start_ok = ok
    if not start_ok:
        print(
            json.dumps(
                {
                    "session": args.session,
                    "start_patch": args.start_patch,
                    "vol_spec": args.vol_spec,
                    "active_artifacts_sha256": cur_m["s1"]["active_artifacts_sha256"],
                    "cache_fingerprint_s1": cur_m["s1"]["cache_fingerprint"],
                    "cache_fingerprint_s2": cur_m["s2"]["cache_fingerprint"],
                    "gate_metrics_s1": cur_m["s1"],
                    "gate_metrics_s2": cur_m["s2"],
                },
                indent=2,
            )
        )
        print(f"START_PATCH_FAILS_GATE session={args.session} (continuing search from start candidate)")

    history: list[dict[str, Any]] = [{"iter": 0, "metrics": cur_m, "patch": str(cur_path)}]
    for it in range(1, int(args.max_iters) + 1):
        improved = False
        best: tuple[tuple[float, float, float, float], dict[str, Any], Path, dict[str, Any]] | None = (
            (_score(cur_m), cur_m, cur_path, cur) if start_ok else None
        )
        for knob, vals in grid.items():
            for v in vals:
                cand = _apply_knob(cur, knob, v, args.session)
                cp = work / f"{args.session}_ca_iter{it}_{knob.replace('.', '_')}_{str(v).replace('.', 'p')}.json"
                _save(cp, cand)
                try:
                    o1 = _run_verify(
                        args.base_cache_s1,
                        str(cp),
                        f"proof_artifacts/CA_{args.session}_iter{it}_{knob.replace('.', '_')}_{str(v).replace('.', 'p')}_S1.json",
                        args,
                    )
                    o2 = _run_verify(
                        args.base_cache_s2,
                        str(cp),
                        f"proof_artifacts/CA_{args.session}_iter{it}_{knob.replace('.', '_')}_{str(v).replace('.', 'p')}_S2.json",
                        args,
                    )
                    gok, gm = _gate_pair(o1, o2, args)
                except Exception:
                    gok, gm = False, {"score": {"ddEph": -1e9, "ddTail": -1e9, "ddCAP": -1e9, "neg_ddcap_total": 1e9}}
                if gok:
                    if best is None or _score(gm) > best[0]:
                        best = (_score(gm), gm, cp, cand)
                        improved = True
        if not improved:
            break
        if best is None:
            break
        cur_m = best[1]
        cur_path = best[2]
        cur = best[3]
        start_ok = True
        history.append({"iter": it, "metrics": cur_m, "patch": str(cur_path)})

    if not start_ok:
        raise SystemExit(f"NO_GATED_CANDIDATE_FOUND session={args.session}")

    out_patch = args.out_patch or f"calibration/candidates/{args.session.lower()}_CA_FINAL.json"
    _save(Path(out_patch), cur)
    out_json = args.out_json or f"proof_artifacts/{args.session}_CA_FINAL.json"
    Path(out_json).write_text(
        json.dumps(
            {
                "session": args.session,
                "start_patch": args.start_patch,
                "final_patch": out_patch,
                "vol_spec": args.vol_spec,
                "history": history,
                "final_metrics": cur_m,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    out_md = args.out_md or f"proof_artifacts/COORDINATE_ASCENT_{args.session}.md"
    lines = [
        f"# Coordinate Ascent {args.session}",
        "",
        f"- start patch: `{args.start_patch}`",
        f"- final patch: `{out_patch}`",
        f"- vol spec: `{args.vol_spec}`",
        f"- cache S1: `{args.base_cache_s1}`",
        f"- cache S2: `{args.base_cache_s2}`",
        "",
    ]
    for h in history:
        s = h["metrics"]["score"]
        s1 = h["metrics"]["s1"]
        s2 = h["metrics"]["s2"]
        lines.append(
            f"- iter {h['iter']}: score(ddEph={s['ddEph']:.6f},ddTail={s['ddTail']:.6f},ddCAP={s['ddCAP']:.6f},neg_ddcap_total={s['neg_ddcap_total']:.0f}) "
            f"S1(ddEph={s1['ddEph']:.6f},ddTail={s1['ddTail']:.6f},ddCAP={s1['ddCAP']:.6f}) "
            f"S2(ddEph={s2['ddEph']:.6f},ddTail={s2['ddTail']:.6f},ddCAP={s2['ddCAP']:.6f}) patch=`{h['patch']}`"
        )
    Path(out_md).write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(out_patch)
    print(out_json)
    print(out_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
