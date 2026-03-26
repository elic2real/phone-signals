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

import tick_generator
from artificial_data import generate_session


MANUAL_OVERRIDE_PATH = Path("tunes/manual_overrides.json")


@dataclass
class AEECandidateResult:
    idx: int
    knobs: Dict[str, Any]
    synthetic_runs: int
    pph_mean: float
    pips_mean: float
    capture_mean: float
    giveback_mean: float
    dead_hold_rate: float
    tail_loss_rate: float
    score: float


def _now_ver(prefix: str = "train-aee") -> str:
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
    content = MANUAL_OVERRIDE_PATH.read_text(encoding="utf-8") if existed else ""
    return {"existed": existed, "content": content}


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


def _run_harness(ticks_path: Path, pair: str, direction: str, speed_class: str, atr_pips: float, out_path: Path) -> dict:
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
    proc = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if proc.returncode != 0:
        err = (proc.stderr or proc.stdout or "").strip()
        raise RuntimeError(f"sim_harness_failed pair={pair} dir={direction} rc={proc.returncode}: {err[:600]}")
    return _load_json(out_path, {})


def _extract_metrics(run_obj: dict) -> dict:
    pph = float((run_obj.get("pips_per_hour") or {}).get("weighted", 0.0))
    pips = float(run_obj.get("weighted_pips", 0.0) or 0.0)
    legs = run_obj.get("legs") or {}
    captures = []
    givebacks = []
    hold_vals = []
    for leg_name in ("core", "runner"):
        lg = legs.get(leg_name) or {}
        c = lg.get("capture")
        if isinstance(c, (int, float)):
            captures.append(float(c))
        lot = lg.get("left_on_table_pips")
        if isinstance(lot, (int, float)):
            givebacks.append(max(0.0, float(lot)))
        hs = lg.get("hold_sec")
        if isinstance(hs, (int, float)):
            hold_vals.append(float(hs))
    capture_mean = sum(captures) / len(captures) if captures else 0.0
    giveback_mean = sum(givebacks) / len(givebacks) if givebacks else 0.0
    hold_mean = sum(hold_vals) / len(hold_vals) if hold_vals else 0.0
    dead_hold = 1.0 if hold_mean > 1200.0 else 0.0
    tail_loss = 1.0 if pips < 0.0 else 0.0
    return {
        "pph": pph,
        "pips": pips,
        "capture": capture_mean,
        "giveback": giveback_mean,
        "dead_hold": dead_hold,
        "tail_loss": tail_loss,
    }


def _score(metrics: List[dict]) -> dict:
    if not metrics:
        return {"pph_mean": 0.0, "pips_mean": 0.0, "capture_mean": 0.0, "giveback_mean": 0.0, "dead_hold_rate": 1.0, "tail_loss_rate": 1.0}
    n = len(metrics)
    pph_mean = sum(m["pph"] for m in metrics) / n
    pips_mean = sum(m["pips"] for m in metrics) / n
    capture_mean = sum(m["capture"] for m in metrics) / n
    giveback_mean = sum(m["giveback"] for m in metrics) / n
    dead_hold_rate = sum(m["dead_hold"] for m in metrics) / n
    tail_loss_rate = sum(m["tail_loss"] for m in metrics) / n
    return {
        "pph_mean": pph_mean,
        "pips_mean": pips_mean,
        "capture_mean": capture_mean,
        "giveback_mean": giveback_mean,
        "dead_hold_rate": dead_hold_rate,
        "tail_loss_rate": tail_loss_rate,
    }


def _final_score(s: dict) -> float:
    # Profit + capture - giveback/dead hold/tail.
    return (
        (1.00 * float(s["pph_mean"]))
        + (20.0 * float(s["capture_mean"]))
        + (0.20 * float(s["pips_mean"]))
        - (2.0 * float(s["giveback_mean"]))
        - (40.0 * float(s["dead_hold_rate"]))
        - (30.0 * float(s["tail_loss_rate"]))
    )


def _candidate_space(max_candidates: int, rng: random.Random) -> List[dict]:
    grids = {
        "aee.strictness_mult": [0.90, 1.00, 1.10, 1.20],
        "aee.fail_windows": [2, 3, 4, 5],
        "aee.near_tp_band_atr": [0.20, 0.25, 0.30, 0.35],
    }
    keys = list(grids.keys())
    pool = [dict(zip(keys, vals)) for vals in itertools.product(*(grids[k] for k in keys))]
    rng.shuffle(pool)
    return pool[: max(1, int(max_candidates))]


def _build_synthetic_rows(stats: dict, pair: str, seed: int, n_ticks: int) -> List[dict]:
    start_price = 110.0 if "JPY" in pair else 1.1000
    return generate_session(stats, pair=pair, start_price=start_price, n_ticks=n_ticks, seed=seed)


def run_training(args: argparse.Namespace) -> dict:
    rng = random.Random(int(args.seed))
    stats = _load_json(Path(args.stats), {})
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    backup = _backup_manual()
    leaderboard: List[AEECandidateResult] = []
    try:
        candidates = _candidate_space(args.max_candidates, rng)
        for i, cand in enumerate(candidates, start=1):
            _apply_manual_overrides(cand)
            run_metrics = []
            for ridx in range(int(args.synthetic_runs)):
                pair = args.pairs[ridx % len(args.pairs)]
                direction = "LONG" if (ridx % 2 == 0) else "SHORT"
                if ridx % 3 == 0:
                    rows = _build_synthetic_rows(stats, pair, rng.randint(0, 2**32 - 1), int(args.synthetic_ticks))
                else:
                    sname, rows = tick_generator.sample_scenario_mix(rng=rng)
                    # normalize pair if scenario doesn't match desired pair
                    if rows and rows[0].get("instrument") != pair:
                        rows = _build_synthetic_rows(stats, pair, rng.randint(0, 2**32 - 1), int(args.synthetic_ticks))
                with tempfile.TemporaryDirectory(prefix="aee-train-") as td:
                    td = Path(td)
                    ticks_path = td / "ticks.csv"
                    out_path = td / "sim_out.json"
                    _write_ticks_csv(rows, ticks_path)
                    try:
                        obj = _run_harness(
                            ticks_path=ticks_path,
                            pair=pair,
                            direction=direction,
                            speed_class=args.speed_class,
                            atr_pips=float(args.atr_pips),
                            out_path=out_path,
                        )
                        run_metrics.append(_extract_metrics(obj))
                    except Exception:
                        run_metrics.append(
                            {
                                "pph": 0.0,
                                "pips": 0.0,
                                "capture": 0.0,
                                "giveback": 0.0,
                                "dead_hold": 1.0,
                                "tail_loss": 1.0,
                            }
                        )

            s = _score(run_metrics)
            leaderboard.append(
                AEECandidateResult(
                    idx=i,
                    knobs=cand,
                    synthetic_runs=int(args.synthetic_runs),
                    pph_mean=float(s["pph_mean"]),
                    pips_mean=float(s["pips_mean"]),
                    capture_mean=float(s["capture_mean"]),
                    giveback_mean=float(s["giveback_mean"]),
                    dead_hold_rate=float(s["dead_hold_rate"]),
                    tail_loss_rate=float(s["tail_loss_rate"]),
                    score=_final_score(s),
                )
            )

        leaderboard.sort(key=lambda x: x.score, reverse=True)
        top = leaderboard[: max(1, int(args.top_k))]
        best = top[0]

        baseline = {
            "version": _now_ver("aee-global"),
            "session": args.session,
            "pairs": args.pairs,
            "knobs": best.knobs,
            "score": best.score,
            "synthetic_runs": best.synthetic_runs,
            "pph_mean": best.pph_mean,
            "pips_mean": best.pips_mean,
            "capture_mean": best.capture_mean,
            "giveback_mean": best.giveback_mean,
            "dead_hold_rate": best.dead_hold_rate,
            "tail_loss_rate": best.tail_loss_rate,
        }
        _save_json(Path("tunes/aee_global_baseline.json"), baseline)

        leaderboard_json = {
            "version": baseline["version"],
            "session": args.session,
            "pairs": args.pairs,
            "stats_path": args.stats,
            "candidates_evaluated": len(leaderboard),
            "top_k": [
                {
                    "rank": i + 1,
                    "score": r.score,
                    "knobs": r.knobs,
                    "pph_mean": r.pph_mean,
                    "pips_mean": r.pips_mean,
                    "capture_mean": r.capture_mean,
                    "giveback_mean": r.giveback_mean,
                    "dead_hold_rate": r.dead_hold_rate,
                    "tail_loss_rate": r.tail_loss_rate,
                }
                for i, r in enumerate(top)
            ],
        }
        _save_json(outdir / "aee_global_leaderboard.json", leaderboard_json)

        summary = [
            "# AEE Global Training Summary",
            f"- version: `{baseline['version']}`",
            f"- session: `{args.session}`",
            f"- pairs: `{', '.join(args.pairs)}`",
            f"- candidates evaluated: `{len(leaderboard)}`",
            f"- best score: `{best.score:.4f}`",
            f"- pph mean: `{best.pph_mean:.4f}`",
            f"- capture mean: `{best.capture_mean:.4f}`",
            f"- giveback mean: `{best.giveback_mean:.4f}`",
            f"- dead hold rate: `{best.dead_hold_rate:.4f}`",
            f"- tail loss rate: `{best.tail_loss_rate:.4f}`",
            "",
            "## Best Knobs",
        ]
        for k, v in sorted(best.knobs.items()):
            summary.append(f"- `{k}`: `{v}`")
        (outdir / "aee_global_summary.md").write_text("\n".join(summary) + "\n", encoding="utf-8")

        return {"baseline": baseline, "leaderboard_path": str(outdir / "aee_global_leaderboard.json")}
    finally:
        _restore_manual(backup)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Global AEE trainer using sim_harness and synthetic path replay.")
    ap.add_argument("--session", default="LONDON", choices=["ASIA", "LONDON", "NY"])
    ap.add_argument("--pairs", nargs="+", default=["EUR_USD", "USD_JPY", "AUD_JPY"])
    ap.add_argument("--stats", default="stats/session_LONDON.json")
    ap.add_argument("--synthetic-runs", type=int, default=20)
    ap.add_argument("--max-candidates", type=int, default=20)
    ap.add_argument("--top-k", type=int, default=5)
    ap.add_argument("--synthetic-ticks", type=int, default=650)
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
