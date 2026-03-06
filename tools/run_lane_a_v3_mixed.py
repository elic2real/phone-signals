#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path
from statistics import mean


def norm_key(s: str) -> str:
    return str(s or "").strip().upper().replace(" ", "").replace("-", "_")


def make_v3_key(pair: str, session: str, atr_bucket: str) -> str:
    return f"{norm_key(pair)}|{norm_key(session)}|{norm_key(atr_bucket)}"


def load_json(path: str):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def write_json(path: str, obj) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(obj, indent=2), encoding="utf-8")


def build_default_grid() -> list[dict]:
    rows = []
    idx = 0
    for confirm_disp in (0.10, 0.12, 0.15):
        for max_dist in (0.20, 0.25, 0.30):
            for promote in (0.15, 0.25, 0.35):
                for ext_min in (0.85, 0.95, 1.05):
                    for fail_w in (1, 2):
                        idx += 1
                        rows.append(
                            {
                                "name": f"c{idx:03d}",
                                "knobs": {
                                    "entry.tick.confirm_disp_atr": confirm_disp,
                                    "entry.tick.base_max_dist_atr": max_dist,
                                    "promote_mfe_atr": promote,
                                    "extension_allow_energy_min": ext_min,
                                    "aee.fail_windows": fail_w,
                                },
                            }
                        )
    return rows


def extract_targets(targets_doc: dict, limit: int) -> list[dict]:
    out = []
    rows = targets_doc.get("targets", []) if isinstance(targets_doc, dict) else []
    for t in rows[:limit]:
        key = t.get("key")
        if key:
            parts = [norm_key(x) for x in str(key).split("|")]
            if len(parts) >= 3:
                key = "|".join(parts[:3])
            else:
                key = norm_key(key)
        else:
            key = make_v3_key(t.get("pair", ""), t.get("session", ""), t.get("atr_bucket", ""))
        out.append({"key": key, **t})
    return out


def run_one_sweep(trace_path: str, target_key: str, candidates_file: str, out_file: str) -> dict:
    cmd = [
        "python3",
        "tools/sweep_from_trace.py",
        "--trace",
        trace_path,
        "--target-key",
        target_key,
        "--candidates",
        candidates_file,
        "--out",
        out_file,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(
            f"sweep_from_trace failed target={target_key} trace={trace_path}\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    doc = load_json(out_file)
    rows = doc.get("ranked_candidates") or []
    if not rows:
        raise RuntimeError(f"EMPTY_SWEEP_RESULTS target={target_key} trace={trace_path}")
    required = [
        "delta_expected_extraction_atr",
        "delta_capture_to_ceiling",
        "delta_extraction_per_hour",
        "delta_pnl_atr_p10",
        "delta_exits_per_hour",
    ]
    missing = [k for k in required if k not in rows[0]]
    if missing:
        raise RuntimeError(f"MISSING_DELTA_FIELDS {missing} target={target_key} trace={trace_path}")
    return doc


def shard_pass(best: dict, baseline: dict, eps_ee: float, eps_cap: float, eps_eph: float) -> tuple[bool, list[str]]:
    reasons = []
    d_ee = float(best.get("delta_expected_extraction_atr", 0.0) or 0.0)
    d_cap = float(best.get("delta_capture_to_ceiling", 0.0) or 0.0)
    d_eph = float(best.get("delta_extraction_per_hour", 0.0) or 0.0)
    d_tail = float(best.get("delta_pnl_atr_p10", 0.0) or 0.0)
    d_exh = float(best.get("delta_exits_per_hour", 0.0) or 0.0)
    base_exh = float((baseline or {}).get("exits_per_hour", 0.0) or 0.0)

    if d_ee < eps_ee:
        reasons.append("D_EE_FAIL")
    if d_cap < eps_cap:
        reasons.append("D_CAP_FAIL")
    if d_eph < eps_eph:
        reasons.append("D_EPH_FAIL")
    if d_tail < 0.0:
        reasons.append("TAIL_WORSE")
    if base_exh > 0 and d_exh < (-0.10 * base_exh):
        reasons.append("EXIT_RATE_DROP_GT_10PCT")

    return (len(reasons) == 0), reasons


def main() -> int:
    ap = argparse.ArgumentParser(description="Deterministic Lane A V3 mixed runner")
    ap.add_argument("--traceset", default="proof_artifacts/TRACESET_4x24H_MANIFEST_v2.json")
    ap.add_argument("--targets", default="proof_artifacts/SWEEP_TARGETS_V3_TOP10.json")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--eps-ee", type=float, default=1e-5)
    ap.add_argument("--eps-cap", type=float, default=0.005)
    ap.add_argument("--eps-eph", type=float, default=0.01)
    ap.add_argument("--min-pass-shards", type=int, default=3)
    ap.add_argument("--sweep-out", default="proof_artifacts/LANE_A_V3_MIXED_SWEEP.json")
    ap.add_argument("--gate-out", default="proof_artifacts/LANE_A_V3_MIXED_GATE.json")
    ap.add_argument("--patch-out", default="calibration/tune_map_patch_v3_candidates.json")
    ap.add_argument("--candidate-grid", default="", help="Optional JSON list of candidates")
    args = ap.parse_args()

    traceset = load_json(args.traceset)
    targets_doc = load_json(args.targets)

    shard_rows = traceset.get("shards") or []
    if not shard_rows:
        raise SystemExit("No shards in traceset")
    shards = []
    for s in shard_rows:
        trace = s.get("trace_path")
        if not trace:
            raise SystemExit("Missing trace_path in shard")
        sid = s.get("shard_id") or Path(trace).stem
        shards.append({"shard_id": sid, "trace_path": trace})

    targets = extract_targets(targets_doc, args.top)
    if not targets:
        raise SystemExit("No targets found")

    grid = load_json(args.candidate_grid) if args.candidate_grid else build_default_grid()
    if not isinstance(grid, list) or not grid:
        raise SystemExit("Candidate grid is empty")

    with tempfile.TemporaryDirectory(prefix="lane_a_v3_") as td:
        cfile = Path(td) / "candidates.json"
        cfile.write_text(json.dumps(grid), encoding="utf-8")

        sweep_results = []
        gate_results = []
        patches = []

        for t in targets:
            tkey = t["key"]
            per_shard = []
            for sh in shards:
                out_file = Path(td) / f"sweep_{norm_key(tkey).replace('|','_')}_{norm_key(sh['shard_id'])}.json"
                doc = run_one_sweep(sh["trace_path"], tkey, str(cfile), str(out_file))
                ranked = doc.get("ranked_candidates") or []
                if not ranked:
                    raise RuntimeError(f"Unexpected empty ranked_candidates target={tkey} shard={sh['shard_id']}")
                best = ranked[0]
                ok, reasons = shard_pass(best, doc.get("baseline", {}), args.eps_ee, args.eps_cap, args.eps_eph)
                per_shard.append(
                    {
                        "shard_id": sh["shard_id"],
                        "trace_path": sh["trace_path"],
                        "baseline": doc.get("baseline", {}),
                        "best": best,
                        "pass": ok,
                        "reasons": reasons,
                    }
                )

            pass_shards = sum(1 for r in per_shard if r["pass"])
            decision = "PASS" if pass_shards >= args.min_pass_shards else "HOLD"
            gate_results.append(
                {
                    "target_key": tkey,
                    "pass_shards": pass_shards,
                    "total_shards": len(per_shard),
                    "decision": decision,
                    "shards": [
                        {
                            "shard_id": r["shard_id"],
                            "pass": r["pass"],
                            "reasons": r["reasons"],
                            "candidate": r["best"].get("candidate"),
                            "delta_expected_extraction_atr": r["best"].get("delta_expected_extraction_atr"),
                            "delta_capture_to_ceiling": r["best"].get("delta_capture_to_ceiling"),
                            "delta_extraction_per_hour": r["best"].get("delta_extraction_per_hour"),
                        }
                        for r in per_shard
                    ],
                }
            )

            avg_d_ee = mean(float(r["best"].get("delta_expected_extraction_atr", 0.0) or 0.0) for r in per_shard)
            avg_d_cap = mean(float(r["best"].get("delta_capture_to_ceiling", 0.0) or 0.0) for r in per_shard)
            avg_d_eph = mean(float(r["best"].get("delta_extraction_per_hour", 0.0) or 0.0) for r in per_shard)
            sweep_results.append(
                {
                    "target_key": tkey,
                    "shards_evaluated": len(per_shard),
                    "avg_delta_expected_extraction_atr": avg_d_ee,
                    "avg_delta_capture_to_ceiling": avg_d_cap,
                    "avg_delta_extraction_per_hour": avg_d_eph,
                    "best_candidates_by_shard": [
                        {
                            "shard_id": r["shard_id"],
                            "candidate": r["best"].get("candidate"),
                            "knobs": r["best"].get("knobs", {}),
                        }
                        for r in per_shard
                    ],
                }
            )

            if decision == "PASS":
                passing = [r for r in per_shard if r["pass"]]
                # choose strongest passing shard candidate by delta_extraction_per_hour
                chosen = sorted(
                    passing,
                    key=lambda x: float(x["best"].get("delta_extraction_per_hour", 0.0) or 0.0),
                    reverse=True,
                )[0]["best"]
                patches.append(
                    {
                        "key": tkey,
                        "knobs": chosen.get("knobs", {}),
                        "meta": {
                            "source": "lane_a_v3_mixed_deterministic_runner",
                            "candidate": chosen.get("candidate"),
                            "avg_delta_expected_extraction_atr": avg_d_ee,
                            "avg_delta_capture_to_ceiling": avg_d_cap,
                            "avg_delta_extraction_per_hour": avg_d_eph,
                            "pass_shards": pass_shards,
                            "total_shards": len(per_shard),
                        },
                    }
                )

    sweep_doc = {
        "generated_utc": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "state_model": "pair|session|atr_bucket",
        "targets_requested": len(targets),
        "candidate_grid": len(grid),
        "results": sweep_results,
    }
    gate_doc = {
        "generated_utc": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "gate_rule": {
            "dEE": f">={args.eps_ee}",
            "dCAP": f">={args.eps_cap}",
            "dExtractionPerHour": f">={args.eps_eph}",
            "tail_not_worse": True,
            "throughput_not_minus_10pct": True,
            "pass_requires": f">={args.min_pass_shards}/{len(shards)} shards",
        },
        "results": gate_results,
        "pass_count": sum(1 for r in gate_results if r["decision"] == "PASS"),
        "state_model": "pair|session|atr_bucket",
    }
    patch_doc = {
        "version": "V3_MIXED_CANDIDATES",
        "state_model": "pair|session|atr_bucket",
        "patches": patches,
    }

    write_json(args.sweep_out, sweep_doc)
    write_json(args.gate_out, gate_doc)
    write_json(args.patch_out, patch_doc)

    summary = {
        "evaluated_targets": len(targets),
        "pass_count": gate_doc["pass_count"],
        "candidate_patch_keys": [p["key"] for p in patches],
    }
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
