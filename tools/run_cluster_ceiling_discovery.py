#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any


def _load(path: str) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _resolve_base_cache_for_session(session: str) -> str:
    return {
        "ASIA": "/tmp/base1d_ASIA.json",
        "LONDON": "/tmp/base1d_LONDON.json",
        "NY": "/tmp/base1d_NY.json",
    }[session]


def _emit_final_map(
    *,
    pocket_universe: dict[str, Any],
    clusters: dict[str, Any],
    cluster_ceilings: dict[str, Any],
    out_path: str,
) -> None:
    p2c = clusters.get("pocket_to_cluster", {})
    ceil = cluster_ceilings.get("cluster_ceilings", {})
    touch_map = cluster_ceilings.get("touches_by_pocket", {})
    fallback_knobs = cluster_ceilings.get("fallback_knobs", {})
    patches: list[dict[str, Any]] = []
    unresolved: list[str] = []
    by_source = {"cluster": 0, "fallback": 0}
    for key in pocket_universe.get("full_pockets", []):
        cid = p2c.get(key)
        source = "cluster"
        knobs: dict[str, Any] = {}
        if cid is not None:
            c = ceil.get(str(cid), {})
            knobs = c.get("knobs") or {}
        if not knobs:
            source = "fallback"
            pair, sq, _vb = (key.split("|") + ["", "", ""])[:3]
            session = sq.split("_")[0] if "_" in sq else ""
            knobs = (fallback_knobs.get(session, {}) or fallback_knobs.get("GLOBAL", {}) or {}).copy()
        if not knobs:
            unresolved.append(key)
            continue
        by_source[source] = by_source.get(source, 0) + 1
        patches.append(
            {
                "level": "SESSION_PAIR",
                "key": key,
                "knobs": knobs,
                "meta": {
                    "source": source,
                    "cluster_id": cid,
                    "touch_count": touch_map.get(key),
                },
            }
        )
    out = {
        "version": 1,
        "source": {
            "pocket_universe": "calibration/pocket_universe_15p.json",
            "cluster_map": "calibration/pocket_clusters_v1.json",
            "cluster_ceilings": "calibration/cluster_ceilings_v1.json",
        },
        "patches": patches,
        "summary": {
            "full_pockets": len(pocket_universe.get("full_pockets", [])),
            "mapped_pockets": len(patches),
            "unresolved_pockets": len(unresolved),
            "mapped_by_source": by_source,
        },
        "unresolved_pockets": unresolved[:200],
    }
    op = Path(out_path)
    op.parent.mkdir(parents=True, exist_ok=True)
    op.write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clusters", default="calibration/pocket_clusters_v1.json")
    ap.add_argument("--behavior-csv", default="artifacts/pocket_behavior_table_all.csv")
    ap.add_argument("--pocket-universe", default="calibration/pocket_universe_15p.json")
    ap.add_argument("--base-patch", default="calibration/candidates/q4_search_dsp0p00_dfp1_dpp0p00.json")
    ap.add_argument("--active-artifacts", default="calibration/active/ACTIVE_ARTIFACTS.json")
    ap.add_argument("--bootstrap", type=int, default=4)
    ap.add_argument("--batch-size", type=int, default=2)
    ap.add_argument("--budget-trials", type=int, default=8)
    ap.add_argument("--cand-pool", type=int, default=200)
    ap.add_argument("--max-clusters", type=int, default=40)
    ap.add_argument("--resume-ceilings", default="")
    ap.add_argument("--meta-only", action="store_true")
    ap.add_argument("--out-ceilings", default="calibration/cluster_ceilings_v1.json")
    ap.add_argument("--out-final-map", default="calibration/final_ceiling_map_15p.json")
    ap.add_argument("--out-report", default="proof_artifacts/ceiling_campaign_report.json")
    args = ap.parse_args()

    cl = _load(args.clusters)
    p2c: dict[str, int] = {str(k): int(v) for k, v in (cl.get("pocket_to_cluster") or {}).items()}
    rows = list(csv.DictReader(Path(args.behavior_csv).open(encoding="utf-8")))
    by_cluster: dict[int, list[dict[str, Any]]] = {}
    touches_by_pocket: dict[str, float | None] = {}
    for r in rows:
        key = str(r.get("target_key", "") or "")
        if key not in p2c:
            continue
        by_cluster.setdefault(p2c[key], []).append(r)
        try:
            touches_by_pocket[key] = float(r.get("touches_min", 0.0) or 0.0)
        except Exception:
            touches_by_pocket[key] = None

    run_dir = Path("proof_artifacts/cluster_runs")
    run_dir.mkdir(parents=True, exist_ok=True)
    ceil: dict[str, Any] = {}
    if args.resume_ceilings and Path(args.resume_ceilings).exists():
        prev = _load(args.resume_ceilings)
        ceil.update((prev.get("cluster_ceilings") or {}))
    ordered_clusters = sorted(by_cluster.keys(), key=lambda c: len(by_cluster[c]), reverse=True)[: args.max_clusters]
    processed_before = len(ceil)
    # small meta-like space to reduce compute
    tmp_space_path = ""
    if args.meta_only:
        meta_space = {
            "aee.fail_windows": {"type": "int", "min": 2, "max": 6, "step": 1},
            "aee.strictness_mult": {"type": "float", "min": 0.95, "max": 1.15, "step": 0.05},
            "extension_allow_energy_min": {"type": "float", "min": 0.95, "max": 1.15, "step": 0.05},
        }
        fd = tempfile.NamedTemporaryFile(prefix="cluster_meta_space_", suffix=".json", delete=False)
        Path(fd.name).write_text(json.dumps(meta_space, indent=2) + "\n", encoding="utf-8")
        tmp_space_path = fd.name
    for cid in ordered_clusters:
        if str(cid) in ceil and (ceil.get(str(cid), {}) or {}).get("status") == "ok":
            continue
        members = by_cluster[cid]
        rep = max(members, key=lambda r: float(r.get("touches_min", 0.0) or 0.0))
        key = str(rep.get("target_key", "") or "")
        try:
            pair, sq, _vol = key.split("|")
            session, quarter = sq.split("_")
        except Exception:
            continue
        out_prefix = run_dir / f"CLUSTER_{cid}_{pair}_{session}_{quarter}"
        cache = _resolve_base_cache_for_session(session)
        cmd = [
            "python3",
            "tools/run_ceiling_discovery.py",
            "--pair",
            pair,
            "--session",
            session,
            "--quarter",
            quarter,
            "--base-patch",
            args.base_patch,
            "--bootstrap",
            str(args.bootstrap),
            "--batch-size",
            str(args.batch_size),
            "--budget-trials",
            str(args.budget_trials),
            "--cand-pool",
            str(args.cand_pool),
            "--base-cache-s1",
            cache,
            "--base-cache-s2",
            cache,
            "--active-artifacts",
            args.active_artifacts,
            "--out-prefix",
            str(out_prefix),
        ]
        if tmp_space_path:
            cmd.extend(["--space-json", tmp_space_path])
        r = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if r.returncode != 0:
            ceil[str(cid)] = {"status": "failed", "error": r.stdout[-1000:], "representative": key}
            continue
        summary_path = f"{out_prefix}_SUMMARY.json"
        summary = _load(summary_path)
        best = summary.get("best") or {}
        ceil[str(cid)] = {
            "status": "ok",
            "representative": key,
            "pair": pair,
            "session": session,
            "quarter": quarter,
            "knobs": (best.get("knobs") or {}),
            "score": float(best.get("score", -1e9) or -1e9),
            "members": len(members),
            "summary_path": summary_path,
        }

    # session fallback knobs from active session patches (pair->session key)
    fallback_knobs: dict[str, dict[str, Any]] = {}
    aa = _load(args.active_artifacts)
    for sess in ("ASIA", "LONDON", "NY"):
        ppath = str((((aa.get("sessions") or {}).get(sess) or {}).get("patch") or ""))
        knobs = {}
        if ppath and Path(ppath).exists():
            pobj = _load(ppath)
            cand = None
            for p in pobj.get("patches", []):
                k = str(p.get("key", "") or "")
                if f"|{sess}_" in k and p.get("knobs"):
                    cand = p.get("knobs") or {}
                    break
            if cand:
                knobs = cand
        fallback_knobs[sess] = knobs or {
            "aee.fail_windows": 3,
            "aee.strictness_mult": 1.0,
            "extension_allow_energy_min": 1.0,
            "promote_mfe_atr": 0.25,
            "entry.tick.base_max_dist_atr": 0.2,
            "entry.tick.confirm_disp_atr": 0.12,
        }
    fallback_knobs["GLOBAL"] = fallback_knobs.get("ASIA", {}).copy()

    out_obj = {
        "version": 1,
        "cluster_count_requested": int(args.max_clusters),
        "cluster_count_processed": len([c for c in ordered_clusters if str(c) in ceil]),
        "cluster_count_preexisting": processed_before,
        "cluster_ceilings": ceil,
        "touches_by_pocket": touches_by_pocket,
        "fallback_knobs": fallback_knobs,
    }
    Path(args.out_ceilings).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_ceilings).write_text(json.dumps(out_obj, indent=2) + "\n", encoding="utf-8")

    pu = _load(args.pocket_universe)
    _emit_final_map(
        pocket_universe=pu,
        clusters=cl,
        cluster_ceilings=out_obj,
        out_path=args.out_final_map,
    )

    final_map = _load(args.out_final_map)
    low_conf = [k for k, v in touches_by_pocket.items() if v is None or float(v or 0.0) <= 0.0]
    weak_clusters = []
    for cid, c in ceil.items():
        if (c or {}).get("status") != "ok":
            weak_clusters.append({"cluster_id": cid, "status": c.get("status")})
            continue
        if float(c.get("score", 0.0) or 0.0) <= 0.0:
            weak_clusters.append({"cluster_id": cid, "status": "weak_score", "score": c.get("score")})
    report = {
        "version": 1,
        "cluster_ceilings_path": args.out_ceilings,
        "final_map_path": args.out_final_map,
        "clusters_total_target": int(args.max_clusters),
        "clusters_ok": sum(1 for c in ceil.values() if (c or {}).get("status") == "ok"),
        "clusters_failed": sum(1 for c in ceil.values() if (c or {}).get("status") != "ok"),
        "mapped_summary": final_map.get("summary", {}),
        "low_confidence_pockets_count": len(low_conf),
        "low_confidence_pockets_sample": low_conf[:200],
        "weak_clusters": weak_clusters,
    }
    Path(args.out_report).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out_report).write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(args.out_ceilings)
    print(args.out_final_map)
    print(args.out_report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
