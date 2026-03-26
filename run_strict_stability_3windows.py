#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class WindowResult:
    name: str
    stream_count: int
    run_json: str
    verdict: str
    trade_count: int
    avg_pips_per_trade: float
    gross_realized_pips_per_hour: float
    net_realized_pips_per_hour: float
    net_delta_realized_pips_per_hour: float


def _discover_streams(root: Path, globs: list[str]) -> list[Path]:
    streams: list[Path] = []
    for g in globs:
        streams.extend([p.resolve() for p in root.glob(g) if p.is_file()])
    return sorted(set(streams))


def _split_three_windows(streams: list[Path]) -> list[list[Path]]:
    buckets = [[], [], []]
    for i, sp in enumerate(streams):
        buckets[i % 3].append(sp)
    return buckets


def _sample_three_windows(streams: list[Path], streams_per_window: int, seed: int) -> list[list[Path]]:
    if not streams:
        return [[], [], []]
    k = max(1, min(int(streams_per_window), len(streams)))
    windows: list[list[Path]] = []
    for i in range(3):
        rnd = random.Random(seed + i)
        windows.append(sorted(rnd.sample(streams, k)))
    return windows


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _run_window(
    root: Path,
    python_bin: str,
    cfg: str,
    window_name: str,
    window_dir: Path,
    spread_pips: float,
    slippage_pips_per_side: float,
    commission_pips_roundtrip: float,
    latency_penalty_pips: float,
) -> WindowResult:
    run_out = root / f"stability_{window_name}_run.json"
    dist_out = root / f"stability_{window_name}_dist.json"
    runbook_out = root / f"stability_{window_name}_runbook.json"
    candidate_out = root / f"stability_{window_name}_candidate_table.json"
    ci_out = root / f"stability_{window_name}_ci_report.json"
    decision_out = root / f"stability_{window_name}_final_decision.json"

    cmd = [
        python_bin,
        "run_aee_band_floor_baseline.py",
        "--config",
        cfg,
        "--stream-glob",
        f"{window_dir.relative_to(root)}/*.csv",
        "--max-streams",
        "999",
        "--spread-pips",
        str(spread_pips),
        "--slippage-pips-per-side",
        str(slippage_pips_per_side),
        "--commission-pips-roundtrip",
        str(commission_pips_roundtrip),
        "--latency-penalty-pips",
        str(latency_penalty_pips),
        "--run-out",
        str(run_out.relative_to(root)),
        "--dist-out",
        str(dist_out.relative_to(root)),
        "--runbook-out",
        str(runbook_out.relative_to(root)),
        "--candidate-table-out",
        str(candidate_out.relative_to(root)),
        "--ci-report-out",
        str(ci_out.relative_to(root)),
        "--final-decision-out",
        str(decision_out.relative_to(root)),
    ]

    subprocess.run(cmd, cwd=str(root), check=True)

    payload = json.loads(run_out.read_text(encoding="utf-8"))
    return WindowResult(
        name=window_name,
        stream_count=len(list(window_dir.glob("*.csv"))),
        run_json=str(run_out),
        verdict=str(payload.get("verdict", "UNKNOWN")),
        trade_count=_safe_int(payload.get("trade_count", 0), 0),
        avg_pips_per_trade=_safe_float(payload.get("avg_pips_per_trade", 0.0), 0.0),
        gross_realized_pips_per_hour=_safe_float(payload.get("gross_realized_pips_per_hour", 0.0), 0.0),
        net_realized_pips_per_hour=_safe_float(payload.get("net_realized_pips_per_hour", 0.0), 0.0),
        net_delta_realized_pips_per_hour=_safe_float(payload.get("net_delta_realized_pips_per_hour", 0.0), 0.0),
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Run strict 3-window stability proof with frozen config/cost model.")
    ap.add_argument("--config", default="entry_v23_policy_strict_active_only.json")
    ap.add_argument("--python-bin", default="python3")
    ap.add_argument("--spread-pips", type=float, default=0.8)
    ap.add_argument("--slippage-pips-per-side", type=float, default=0.15)
    ap.add_argument("--commission-pips-roundtrip", type=float, default=0.0)
    ap.add_argument("--latency-penalty-pips", type=float, default=0.0)
    ap.add_argument("--out", default="stability_3window_report.json")
    ap.add_argument("--window-mode", choices=["disjoint", "sampled"], default="sampled")
    ap.add_argument("--streams-per-window", type=int, default=24)
    ap.add_argument("--seed", type=int, default=20260324)
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    windows_root = root / ".stability_windows"

    globs = [
        "compiled_market_nodes/EUR_USD__*/aee_stage/aee_state_stream/aee_state_stream.csv",
        "compiled_market_nodes/EUR_CHF__*/aee_stage/aee_state_stream/aee_state_stream.csv",
        "compiled_market_nodes/USD_CAD__*/aee_stage/aee_state_stream/aee_state_stream.csv",
        "compiled_market_nodes/EUR_GBP__*/aee_stage/aee_state_stream/aee_state_stream.csv",
    ]

    streams = _discover_streams(root, globs)
    if len(streams) < 3:
        raise SystemExit("need at least 3 streams for 3-window stability proof")

    if str(args.window_mode) == "sampled":
        windows = _sample_three_windows(streams, int(args.streams_per_window), int(args.seed))
    else:
        windows = _split_three_windows(streams)

    if windows_root.exists():
        shutil.rmtree(windows_root)
    windows_root.mkdir(parents=True, exist_ok=True)

    names = ["window_1", "window_2", "window_3"]
    for i, bucket in enumerate(windows):
        wdir = windows_root / names[i]
        wdir.mkdir(parents=True, exist_ok=True)
        for j, sp in enumerate(bucket, 1):
            link_name = wdir / f"s_{j:04d}.csv"
            if link_name.exists() or link_name.is_symlink():
                link_name.unlink()
            os.symlink(sp, link_name)

    results: list[WindowResult] = []
    for name in names:
        wdir = windows_root / name
        results.append(
            _run_window(
                root=root,
                python_bin=args.python_bin,
                cfg=args.config,
                window_name=name,
                window_dir=wdir,
                spread_pips=float(args.spread_pips),
                slippage_pips_per_side=float(args.slippage_pips_per_side),
                commission_pips_roundtrip=float(args.commission_pips_roundtrip),
                latency_penalty_pips=float(args.latency_penalty_pips),
            )
        )

    all_positive = all(r.net_delta_realized_pips_per_hour > 0.0 for r in results)
    all_accepted = all(r.verdict == "BASELINE_ACCEPTED" for r in results)

    out_payload = {
        "generated_at": _iso_now(),
        "objective": "stability_proof_no_tuning",
        "lock": {
            "config": args.config,
            "aee_and_routing": "frozen",
            "cost_model": {
                "spread_pips": float(args.spread_pips),
                "slippage_pips_per_side": float(args.slippage_pips_per_side),
                "commission_pips_roundtrip": float(args.commission_pips_roundtrip),
                "latency_penalty_pips": float(args.latency_penalty_pips),
            },
            "tuning_changes": "none",
        },
        "windows": [
            {
                "name": r.name,
                "stream_count": r.stream_count,
                "run_json": r.run_json,
                "verdict": r.verdict,
                "trade_count": r.trade_count,
                "avg_pips_per_trade": r.avg_pips_per_trade,
                "gross_realized_pips_per_hour": r.gross_realized_pips_per_hour,
                "net_realized_pips_per_hour": r.net_realized_pips_per_hour,
                "net_delta_realized_pips_per_hour": r.net_delta_realized_pips_per_hour,
            }
            for r in results
        ],
        "window_generation": {
            "mode": str(args.window_mode),
            "streams_per_window": int(args.streams_per_window),
            "seed": int(args.seed),
            "total_streams_available": len(streams),
        },
        "pass_criteria": {
            "all_three_positive_net_delta": all_positive,
            "all_three_baseline_accepted": all_accepted,
        },
        "stability_verdict": "STABLE_POSITIVE" if (all_positive and all_accepted) else "NOT_YET_PROVEN",
    }

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = (root / out_path).resolve()
    out_path.write_text(json.dumps(out_payload, indent=2) + "\n", encoding="utf-8")

    print(json.dumps({"out": str(out_path), "stability_verdict": out_payload["stability_verdict"]}, indent=2))


if __name__ == "__main__":
    main()
