#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any

import pandas as pd


PIP = 0.0001
TARGET = 2.5


def parse_ts(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    if len(vals) == 1:
        return vals[0]
    idx = q * (len(vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(vals) - 1)
    frac = idx - lo
    return vals[lo] * (1 - frac) + vals[hi] * frac


def load_prices(data_root: Path) -> list[dict[str, Any]]:
    parts = sorted(data_root.rglob("part-000.parquet"))
    rows: list[dict[str, Any]] = []
    for p in parts:
        df = pd.read_parquet(p)
        for rec in df.to_dict("records"):
            dt = parse_ts(str(rec["timestamp"]))
            rows.append(
                {
                    "timestamp": str(rec["timestamp"]),
                    "dt": dt,
                    "price": float(rec["close"]),
                    "session": "london" if 8 <= dt.hour < 16 else "other",
                    "weekday": dt.strftime("%A").lower(),
                    "session_date": dt.date().isoformat(),
                }
            )
    rows.sort(key=lambda r: r["dt"])
    return rows


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def build_candidates(
    prices: list[dict[str, Any]],
    phase1_rows: list[dict[str, Any]],
    clusters: list[dict[str, Any]],
    labels: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    price_idx = {r["timestamp"]: i for i, r in enumerate(prices)}
    phase1_idx = {r["timestamp"]: r for r in phase1_rows}
    label_idx = {(r["timestamp_start"], r["direction"]): r["zone_label"] for r in labels}
    session_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in prices:
        session_groups[r["session_date"]].append(r)

    out: list[dict[str, Any]] = []
    for cluster in clusters:
        start_dt = parse_ts(cluster["cluster_start"])
        end_dt = parse_ts(cluster["cluster_end"])
        session_rows = session_groups[cluster["session_date"]]
        cluster_rows = [r for r in session_rows if start_dt <= r["dt"] <= end_dt]
        if not cluster_rows:
            continue
        denom = max(1, len(cluster_rows) - 1)
        for pos, row in enumerate(cluster_rows):
            idx = price_idx[row["timestamp"]]
            prev = prices[max(0, idx - 10):idx + 1]
            prev_prices = [r["price"] for r in prev]
            if len(prev_prices) < 2:
                continue
            diffs = [abs(prev_prices[i] - prev_prices[i - 1]) / PIP for i in range(1, len(prev_prices))]
            pre_speed_3 = mean(diffs[-3:]) if diffs else 0.0
            pre_volatility_10 = mean(diffs) if diffs else 0.0
            pre_range_10 = (max(prev_prices) - min(prev_prices)) / PIP
            net_5 = (prev_prices[-1] - prev_prices[max(0, len(prev_prices) - 6)]) / PIP
            aligned_trend = net_5 if cluster["direction"] == "LONG" else -net_5

            outcome = phase1_idx[row["timestamp"]]
            if cluster["direction"] == "LONG":
                exists = outcome["up_exists"] == "1"
                tau_this = int(float(outcome["tau_up_min"])) if exists else 10**9
                tau_opp = int(float(outcome["tau_down_min"])) if outcome["down_exists"] == "1" else 10**9
                mae = float(outcome["mae_up_pips"]) if exists else TARGET
                mfe = float(outcome["mfe_up_pips"])
            else:
                exists = outcome["down_exists"] == "1"
                tau_this = int(float(outcome["tau_down_min"])) if exists else 10**9
                tau_opp = int(float(outcome["tau_up_min"])) if outcome["up_exists"] == "1" else 10**9
                mae = float(outcome["mae_down_pips"]) if exists else TARGET
                mfe = float(outcome["mfe_down_pips"])

            if exists and mae <= TARGET and tau_this <= tau_opp:
                static_pips = TARGET
                static_reason = "TP_HIT"
            elif tau_opp < tau_this or (exists and mae > TARGET):
                static_pips = -TARGET
                static_reason = "SL_HIT"
            else:
                static_pips = 0.0
                static_reason = "TIMEOUT"

            out.append(
                {
                    "cluster_id": cluster["cluster_id"],
                    "timestamp_start": row["timestamp"],
                    "direction": cluster["direction"],
                    "session_date": cluster["session_date"],
                    "cluster_progress": pos / denom,
                    "minutes_to_cluster_end": len(cluster_rows) - 1 - pos,
                    "pre_speed": pre_speed_3,
                    "pre_volatility": pre_volatility_10,
                    "pre_range": pre_range_10,
                    "aligned_trend": aligned_trend,
                    "label": label_idx.get((row["timestamp"], cluster["direction"]), "NOISE"),
                    "static_pips": static_pips,
                    "static_R": static_pips / TARGET,
                    "static_reason": static_reason,
                    "future_mfe_pips": mfe,
                }
            )
    return out


def select_rows(rows: list[dict[str, Any]], cfg: dict[str, float]) -> list[dict[str, Any]]:
    chosen = []
    seen_cluster = set()
    for row in rows:
        if row["cluster_id"] in seen_cluster:
            continue
        if row["pre_speed"] < cfg["pre_speed_min"]:
            continue
        if row["pre_volatility"] < cfg["pre_volatility_min"]:
            continue
        if row["pre_range"] < cfg["pre_range_min"]:
            continue
        if row["aligned_trend"] < cfg["aligned_trend_min"]:
            continue
        if row["cluster_progress"] > cfg["cluster_progress_max"]:
            continue
        chosen.append(row)
        seen_cluster.add(row["cluster_id"])
    return chosen


def metrics(chosen: list[dict[str, Any]], population: list[dict[str, Any]], cfg: dict[str, float]) -> dict[str, Any]:
    good = [r for r in population if r["label"] == "GOOD"]
    bad = [r for r in population if r["label"] == "BAD"]
    noise = [r for r in population if r["label"] == "NOISE"]
    chosen_ids = {(r["cluster_id"], r["timestamp_start"]) for r in chosen}
    good_chosen = [r for r in good if (r["cluster_id"], r["timestamp_start"]) in chosen_ids]
    bad_chosen = [r for r in bad if (r["cluster_id"], r["timestamp_start"]) in chosen_ids]
    noise_chosen = [r for r in noise if (r["cluster_id"], r["timestamp_start"]) in chosen_ids]
    wins = [r for r in chosen if r["static_pips"] > 0]
    losses = [r for r in chosen if r["static_pips"] < 0]
    timeouts = [r for r in chosen if r["static_pips"] == 0]
    total_pips = sum(r["static_pips"] for r in chosen)
    avg_win = mean(r["static_pips"] for r in wins) if wins else 0.0
    avg_loss = mean(r["static_pips"] for r in losses) if losses else 0.0
    return {
        "best_config": cfg,
        "good_capture": len(good_chosen) / len(good) if good else 0.0,
        "bad_trigger": len(bad_chosen) / len(bad) if bad else 0.0,
        "noise_trigger": len(noise_chosen) / len(noise) if noise else 0.0,
        "trade_count": len(chosen),
        "wins": len(wins),
        "losses": len(losses),
        "timeouts": len(timeouts),
        "win_rate": len(wins) / len(chosen) if chosen else 0.0,
        "avg_win_pips": avg_win,
        "avg_loss_pips": avg_loss,
        "avg_pips": mean(r["static_pips"] for r in chosen) if chosen else 0.0,
        "total_pips": total_pips,
        "avg_R": mean(r["static_R"] for r in chosen) if chosen else 0.0,
        "expectancy_R": mean(r["static_R"] for r in chosen) if chosen else 0.0,
        "pips_per_hour": total_pips / (11 * 8.0) if chosen else 0.0,
        "estimated_equity_per_hour_at_2pct_risk": (sum(r["static_R"] for r in chosen) * 0.02) / (11 * 8.0) if chosen else 0.0,
    }


def fit(rows: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    good = [r for r in rows if r["label"] == "GOOD"]
    if not good:
        return metrics([], rows, {"logic": "no_good_rows"}), []
    grid = []
    for sq in (0.25, 0.4, 0.55):
        for vq in (0.2, 0.35, 0.5):
            for rq in (0.2, 0.35, 0.5):
                for tq in (0.2, 0.35, 0.5):
                    for prog in (0.45, 0.6, 0.75):
                        grid.append(
                            {
                                "logic": "live_safe_pretrade_multifeature",
                                "pre_speed_min": percentile([r["pre_speed"] for r in good], sq),
                                "pre_volatility_min": percentile([r["pre_volatility"] for r in good], vq),
                                "pre_range_min": percentile([r["pre_range"] for r in good], rq),
                                "aligned_trend_min": percentile([r["aligned_trend"] for r in good], tq),
                                "cluster_progress_max": prog,
                            }
                        )
    best_summary = None
    best_rows: list[dict[str, Any]] = []
    best_score = None
    for cfg in grid:
        chosen = select_rows(rows, cfg)
        summary = metrics(chosen, rows, cfg)
        score = (
            summary["good_capture"] * 4.0
            - summary["bad_trigger"] * 2.5
            - summary["noise_trigger"] * 1.5
            + summary["expectancy_R"] * 1.5
            + summary["win_rate"] * 0.5
        )
        if best_score is None or score > best_score:
            best_score = score
            best_summary = summary
            best_rows = chosen
    assert best_summary is not None
    best_summary["top_configs"] = [
        {
            "pre_speed_min": best_summary["best_config"]["pre_speed_min"],
            "pre_volatility_min": best_summary["best_config"]["pre_volatility_min"],
            "pre_range_min": best_summary["best_config"]["pre_range_min"],
            "aligned_trend_min": best_summary["best_config"]["aligned_trend_min"],
            "cluster_progress_max": best_summary["best_config"]["cluster_progress_max"],
            "good_capture": best_summary["good_capture"],
            "bad_trigger": best_summary["bad_trigger"],
            "noise_trigger": best_summary["noise_trigger"],
            "pips_mean": best_summary["avg_pips"],
            "trade_count": best_summary["trade_count"],
            "win_rate": best_summary["win_rate"],
            "expectancy_R": best_summary["expectancy_R"],
            "pips_per_hour": best_summary["pips_per_hour"],
        }
    ]
    return best_summary, best_rows


def blockers(rows: list[dict[str, Any]], cfg: dict[str, float]) -> dict[str, Any]:
    counts = Counter()
    for row in rows:
        if row["label"] != "GOOD":
            continue
        if row["pre_speed"] < cfg["pre_speed_min"]:
            counts["pre_speed_below_threshold"] += 1
        elif row["pre_volatility"] < cfg["pre_volatility_min"]:
            counts["pre_volatility_below_threshold"] += 1
        elif row["pre_range"] < cfg["pre_range_min"]:
            counts["pre_range_below_threshold"] += 1
        elif row["aligned_trend"] < cfg["aligned_trend_min"]:
            counts["aligned_trend_below_threshold"] += 1
        elif row["cluster_progress"] > cfg["cluster_progress_max"]:
            counts["late_cluster_entry"] += 1
    return {
        "first_blocker_reason_counts": dict(counts),
        "config_used": cfg,
        "candidate_entry_states": len(rows),
    }


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Live-safe stage 7 fitter for the 11-session pipeline")
    parser.add_argument("--data-root", default="london_session_data_11")
    parser.add_argument("--phase1-csv", default="compiled_stage1_6_11_sessions/phase1/opportunity_map_raw.csv")
    parser.add_argument("--clusters-csv", default="compiled_stage1_6_11_sessions/phase2/opportunity_clusters.csv")
    parser.add_argument("--labeled-csv", default="compiled_stage1_6_11_sessions/phase4/opportunity_zones_labeled.csv")
    parser.add_argument("--output-dir", default="compiled_stage1_7_live_safe_11_sessions/phase7")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    prices = load_prices(Path(args.data_root))
    phase1_rows = load_csv(Path(args.phase1_csv))
    clusters = load_csv(Path(args.clusters_csv))
    labels = load_csv(Path(args.labeled_csv))
    candidates = build_candidates(prices, phase1_rows, clusters, labels)

    both_summary, _ = fit(candidates)
    long_summary, _ = fit([r for r in candidates if r["direction"] == "LONG"])
    short_summary, _ = fit([r for r in candidates if r["direction"] == "SHORT"])
    blocker_report = blockers(candidates, both_summary["best_config"])

    write_json(output_dir / "entry_fit_both.json", both_summary)
    write_json(output_dir / "entry_fit_long.json", long_summary)
    write_json(output_dir / "entry_fit_short.json", short_summary)
    write_json(output_dir / "entry_blockers.json", blocker_report)

    print(json.dumps({"both": both_summary, "long": long_summary, "short": short_summary, "blockers": blocker_report}, indent=2))


if __name__ == "__main__":
    main()
