#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import itertools
import json
import os
import random
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from artificial_data import generate_session


MANUAL_OVERRIDE_PATH = Path("tunes/manual_overrides.json")


@dataclass
class CandidateResult:
    idx: int
    knobs: Dict[str, Any]
    synthetic_runs: int
    synthetic_pph_mean: float
    synthetic_pips_mean: float
    synthetic_tail_loss_rate: float
    synthetic_throughput: float
    historical_runs: int
    historical_pph_mean: float
    historical_pips_mean: float
    score: float


def _now_ver(prefix: str = "train-entry") -> str:
    return datetime.now(timezone.utc).strftime(f"{prefix}-%Y%m%dT%H%M%SZ")


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")


def _backup_manual() -> dict:
    existed = MANUAL_OVERRIDE_PATH.exists()
    original = MANUAL_OVERRIDE_PATH.read_text(encoding="utf-8") if existed else ""
    return {"existed": existed, "content": original}


def _restore_manual(backup: dict) -> None:
    if backup.get("existed"):
        MANUAL_OVERRIDE_PATH.parent.mkdir(parents=True, exist_ok=True)
        MANUAL_OVERRIDE_PATH.write_text(str(backup.get("content", "")), encoding="utf-8")
    elif MANUAL_OVERRIDE_PATH.exists():
        MANUAL_OVERRIDE_PATH.unlink()


def _apply_manual_overrides(knobs: Dict[str, Any]) -> None:
    payload = {
        "version": _now_ver("manual"),
        "overrides": dict(knobs),
        "history": [],
    }
    _save_json(MANUAL_OVERRIDE_PATH, payload)


def _write_ticks_csv(rows: List[dict], path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["instrument", "ts", "bid", "ask", "mid", "spread_pips"], extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)


def _run_harness(ticks_path: Path, pair: str, direction: str, speed_class: str, atr_pips: float, friction_mult: float, out_path: Path) -> dict:
    env = os.environ.copy()
    env["FRICTION_SEVERITY_MULT"] = f"{float(friction_mult):.6f}"
    cmd = [
        "python3",
        "sim_harness.py",
        "--ticks",
        str(ticks_path),
        "--pair",
        pair,
        "--direction",
        direction,
        "--atr-pips",
        str(float(atr_pips)),
        "--speed-class",
        speed_class,
        "--out",
        str(out_path),
    ]
    proc = subprocess.run(cmd, check=False, env=env, capture_output=True, text=True)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"sim_harness_failed pair={pair} dir={direction} rc={proc.returncode}: {err[:600]}")
    return _load_json(out_path, {})


def _pair_start_price(pair: str, fallback: float) -> float:
    if "JPY" in pair:
        return 110.0
    if pair == "USD_CAD":
        return 1.35
    return float(fallback)


def _extract_metrics(run_obj: dict) -> dict:
    pph = float(((run_obj.get("pips_per_hour") or {}).get("weighted", 0.0)) if isinstance(run_obj, dict) else 0.0)
    pips = float(run_obj.get("weighted_pips", 0.0) or 0.0)
    return {"pph": pph, "pips": pips}


def _score(run_metrics: List[dict]) -> dict:
    if not run_metrics:
        return {"pph_mean": 0.0, "pips_mean": 0.0, "tail_loss_rate": 1.0, "throughput": 0.0}
    pph_vals = [float(m["pph"]) for m in run_metrics]
    pips_vals = [float(m["pips"]) for m in run_metrics]
    losses = sum(1 for x in pips_vals if x < 0.0)
    pph_mean = sum(pph_vals) / len(pph_vals)
    pips_mean = sum(pips_vals) / len(pips_vals)
    tail_loss_rate = losses / float(len(pips_vals))
    throughput = float(len(pips_vals))
    return {
        "pph_mean": pph_mean,
        "pips_mean": pips_mean,
        "tail_loss_rate": tail_loss_rate,
        "throughput": throughput,
    }


def _final_score(synth: dict, hist: dict | None = None) -> float:
    hist = hist or {"pph_mean": 0.0, "pips_mean": 0.0, "tail_loss_rate": 0.0, "throughput": 0.0}
    return (
        (1.00 * float(synth["pph_mean"]))
        + (0.20 * float(synth["pips_mean"]))
        + (0.05 * float(synth["throughput"]))
        - (30.0 * float(synth["tail_loss_rate"]))
        + (0.50 * float(hist["pph_mean"]))
        + (0.10 * float(hist["pips_mean"]))
    )


def _candidate_space(max_candidates: int, rng: random.Random) -> List[dict]:
    grids = {
        "entry.tick.confirm_disp_atr": [0.12, 0.16, 0.20, 0.24, 0.28],
        "entry.tick.base_max_dist_atr": [0.22, 0.30, 0.38, 0.46, 0.54],
        "entry.tick.confirm_m1_closes": [0, 1, 2],
        "entry.tick.confirm_sec": [0.0, 3.0, 6.0],
        "entry.tick.require_pullback": [True, False],
        "entry.tick.pullback_atr_min": [0.20, 0.30, 0.40],
        "entry.tick.require_reclaim": [True, False],
        "entry.tick.reclaim_tolerance_atr": [0.03, 0.05, 0.08],
        "friction_severity_mult": [0.70, 0.80, 0.90, 1.00],
    }
    keys = list(grids.keys())
    pool = []
    for vals in itertools.product(*(grids[k] for k in keys)):
        d = dict(zip(keys, vals))
        # basic sanity so we do not generate impossible combos
        if (not d["entry.tick.require_pullback"]) and d["entry.tick.pullback_atr_min"] > 0.35:
            continue
        pool.append(d)
    rng.shuffle(pool)
    return pool[: max(1, int(max_candidates))]


def run_training(args: argparse.Namespace) -> dict:
    rng = random.Random(int(args.seed))
    stats_path = Path(args.stats)
    stats = _load_json(stats_path, {})
    candidates = _candidate_space(args.max_candidates, rng)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    backup = _backup_manual()
    leaderboard: List[CandidateResult] = []
    try:
        for i, cand in enumerate(candidates, start=1):
            entry_knobs = {k: v for k, v in cand.items() if k.startswith("entry.tick.")}
            friction_mult = float(cand.get("friction_severity_mult", 0.8))
            _apply_manual_overrides(entry_knobs)

            synth_metrics = []
            for rix in range(int(args.synthetic_runs)):
                pair = args.pairs[rix % len(args.pairs)]
                direction = "LONG" if (rix % 2 == 0) else "SHORT"
                rows = generate_session(
                    stats,
                    pair=pair,
                    start_price=_pair_start_price(pair, float(args.start_price)),
                    n_ticks=int(args.synthetic_ticks),
                    seed=rng.randint(0, 2**32 - 1),
                )
                with tempfile.TemporaryDirectory(prefix="entry-train-") as td:
                    tdp = Path(td)
                    ticks_path = tdp / "ticks.csv"
                    out_path = tdp / "sim_out.json"
                    _write_ticks_csv(rows, ticks_path)
                    try:
                        obj = _run_harness(
                            ticks_path=ticks_path,
                            pair=pair,
                            direction=direction,
                            speed_class=args.speed_class,
                            atr_pips=float(args.atr_pips),
                            friction_mult=friction_mult,
                            out_path=out_path,
                        )
                        synth_metrics.append(_extract_metrics(obj))
                    except Exception:
                        synth_metrics.append({"pph": 0.0, "pips": 0.0})

            synth_score = _score(synth_metrics)

            leaderboard.append(
                CandidateResult(
                    idx=i,
                    knobs=cand,
                    synthetic_runs=int(args.synthetic_runs),
                    synthetic_pph_mean=float(synth_score["pph_mean"]),
                    synthetic_pips_mean=float(synth_score["pips_mean"]),
                    synthetic_tail_loss_rate=float(synth_score["tail_loss_rate"]),
                    synthetic_throughput=float(synth_score["throughput"]),
                    historical_runs=0,
                    historical_pph_mean=0.0,
                    historical_pips_mean=0.0,
                    score=_final_score(synth_score),
                )
            )

        leaderboard.sort(key=lambda x: x.score, reverse=True)
        top = leaderboard[: max(1, int(args.top_k))]

        # Optional historical confirmation if CSVs are provided
        hist_paths = [Path(p) for p in (args.historical_ticks or []) if Path(p).exists()]
        if hist_paths:
            for item in top:
                entry_knobs = {k: v for k, v in item.knobs.items() if k.startswith("entry.tick.")}
                friction_mult = float(item.knobs.get("friction_severity_mult", 0.8))
                _apply_manual_overrides(entry_knobs)
                hist_metrics = []
                for i, hp in enumerate(hist_paths[: int(args.historical_runs)]):
                    pair = args.pairs[i % len(args.pairs)]
                    direction = "LONG" if (i % 2 == 0) else "SHORT"
                    with tempfile.TemporaryDirectory(prefix="entry-hist-") as td:
                        out_path = Path(td) / "hist_out.json"
                        try:
                            obj = _run_harness(
                                ticks_path=hp,
                                pair=pair,
                                direction=direction,
                                speed_class=args.speed_class,
                                atr_pips=float(args.atr_pips),
                                friction_mult=friction_mult,
                                out_path=out_path,
                            )
                            hist_metrics.append(_extract_metrics(obj))
                        except Exception:
                            hist_metrics.append({"pph": 0.0, "pips": 0.0})
                hs = _score(hist_metrics)
                item.historical_runs = len(hist_metrics)
                item.historical_pph_mean = float(hs["pph_mean"])
                item.historical_pips_mean = float(hs["pips_mean"])
                item.score = _final_score(
                    {
                        "pph_mean": item.synthetic_pph_mean,
                        "pips_mean": item.synthetic_pips_mean,
                        "tail_loss_rate": item.synthetic_tail_loss_rate,
                        "throughput": item.synthetic_throughput,
                    },
                    hs,
                )
            top.sort(key=lambda x: x.score, reverse=True)

        best = top[0]
        baseline = {
            "version": _now_ver("entry-global"),
            "session": args.session,
            "pairs": args.pairs,
            "knobs": {k: v for k, v in best.knobs.items() if k.startswith("entry.tick.")},
            "friction_severity_mult": best.knobs.get("friction_severity_mult", 0.8),
            "score": best.score,
            "synthetic_pph_mean": best.synthetic_pph_mean,
            "synthetic_pips_mean": best.synthetic_pips_mean,
            "synthetic_tail_loss_rate": best.synthetic_tail_loss_rate,
            "historical_pph_mean": best.historical_pph_mean,
            "historical_pips_mean": best.historical_pips_mean,
        }
        _save_json(Path("tunes/entry_global_baseline.json"), baseline)

        leaderboard_json = {
            "version": baseline["version"],
            "session": args.session,
            "pairs": args.pairs,
            "stats_path": str(stats_path),
            "candidates_evaluated": len(leaderboard),
            "top_k": [
                {
                    "rank": i + 1,
                    "score": r.score,
                    "knobs": r.knobs,
                    "synthetic_pph_mean": r.synthetic_pph_mean,
                    "synthetic_pips_mean": r.synthetic_pips_mean,
                    "synthetic_tail_loss_rate": r.synthetic_tail_loss_rate,
                    "historical_pph_mean": r.historical_pph_mean,
                    "historical_pips_mean": r.historical_pips_mean,
                }
                for i, r in enumerate(top)
            ],
        }
        _save_json(outdir / "entry_global_leaderboard.json", leaderboard_json)

        summary_md = outdir / "entry_global_summary.md"
        lines = [
            "# Entry Global Training Summary",
            f"- version: `{baseline['version']}`",
            f"- session: `{args.session}`",
            f"- pairs: `{', '.join(args.pairs)}`",
            f"- candidates evaluated: `{len(leaderboard)}`",
            f"- best score: `{best.score:.4f}`",
            "",
            "## Best Candidate",
            f"- synthetic pph mean: `{best.synthetic_pph_mean:.4f}`",
            f"- synthetic pips mean: `{best.synthetic_pips_mean:.4f}`",
            f"- synthetic tail loss rate: `{best.synthetic_tail_loss_rate:.4f}`",
            f"- historical pph mean: `{best.historical_pph_mean:.4f}`",
            f"- historical pips mean: `{best.historical_pips_mean:.4f}`",
            "- knobs:",
        ]
        for k, v in sorted(best.knobs.items()):
            lines.append(f"  - `{k}`: `{v}`")
        summary_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

        return {"baseline": baseline, "leaderboard_path": str(outdir / "entry_global_leaderboard.json")}
    finally:
        _restore_manual(backup)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Synthetic-first global entry trainer using existing sim_harness.")
    ap.add_argument("--session", default="LONDON", choices=["ASIA", "LONDON", "NY"])
    ap.add_argument("--pairs", nargs="+", default=["EUR_USD", "USD_JPY", "AUD_JPY"])
    ap.add_argument("--stats", default="stats/session_LONDON.json")
    ap.add_argument("--synthetic-runs", type=int, default=12)
    ap.add_argument("--historical-runs", type=int, default=0)
    ap.add_argument("--historical-ticks", nargs="*", default=[])
    ap.add_argument("--max-candidates", type=int, default=24)
    ap.add_argument("--top-k", type=int, default=5)
    ap.add_argument("--synthetic-ticks", type=int, default=650)
    ap.add_argument("--start-price", type=float, default=1.1000)
    ap.add_argument("--speed-class", default="SLOW", choices=["FAST", "MED", "SLOW"])
    ap.add_argument("--atr-pips", type=float, default=15.0)
    ap.add_argument("--seed", type=int, default=123)
    ap.add_argument("--outdir", default="reports")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    result = run_training(args)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
