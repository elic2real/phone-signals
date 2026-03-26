#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any

import pandas as pd


PIP = 0.0001
TARGET = 2.5
HOURS_TOTAL = 11 * 8.0


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
    rows: list[dict[str, Any]] = []
    for p in sorted(data_root.rglob("part-000.parquet")):
        df = pd.read_parquet(p)
        for rec in df.to_dict("records"):
            dt = parse_ts(str(rec["timestamp"]))
            rows.append(
                {
                    "timestamp": str(rec["timestamp"]),
                    "dt": dt,
                    "price": float(rec["close"]),
                    "session_date": dt.date().isoformat(),
                }
            )
    rows.sort(key=lambda r: r["dt"])
    return rows


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def signed_pips(direction: str, start: float, end: float) -> float:
    raw = (end - start) / PIP
    return raw if direction == "LONG" else -raw


def slope_sign_consistency(vals: list[float], direction: str) -> float:
    if len(vals) < 2:
        return 0.0
    diffs = [(vals[i] - vals[i - 1]) / PIP for i in range(1, len(vals))]
    signed = [d if direction == "LONG" else -d for d in diffs]
    positives = sum(1 for d in signed if d > 0)
    return positives / len(signed)


def compute_energy_features(direction: str, prev_prices: list[float]) -> dict[str, float]:
    diffs = [(prev_prices[i] - prev_prices[i - 1]) / PIP for i in range(1, len(prev_prices))]
    signed = [d if direction == "LONG" else -d for d in diffs]
    adiffs = [abs(d) for d in diffs]
    speed_3 = mean(adiffs[-3:]) if len(adiffs) >= 3 else mean(adiffs)
    speed_5 = mean(adiffs[-5:]) if len(adiffs) >= 5 else mean(adiffs)
    speed_10 = mean(adiffs[-10:]) if len(adiffs) >= 10 else mean(adiffs)
    vol_10 = speed_10
    vol_20 = mean(adiffs[-20:]) if len(adiffs) >= 20 else mean(adiffs)
    range_5 = (max(prev_prices[-5:]) - min(prev_prices[-5:])) / PIP if len(prev_prices) >= 5 else (max(prev_prices) - min(prev_prices)) / PIP
    range_10 = (max(prev_prices[-10:]) - min(prev_prices[-10:])) / PIP if len(prev_prices) >= 10 else (max(prev_prices) - min(prev_prices)) / PIP
    range_20 = (max(prev_prices[-20:]) - min(prev_prices[-20:])) / PIP if len(prev_prices) >= 20 else (max(prev_prices) - min(prev_prices)) / PIP
    trend_3 = signed_pips(direction, prev_prices[max(0, len(prev_prices) - 4)], prev_prices[-1])
    trend_5 = signed_pips(direction, prev_prices[max(0, len(prev_prices) - 6)], prev_prices[-1])
    trend_10 = signed_pips(direction, prev_prices[max(0, len(prev_prices) - 11)], prev_prices[-1])
    trend_20 = signed_pips(direction, prev_prices[0], prev_prices[-1])
    bias_5 = sum(signed[-5:]) / max(1e-9, sum(abs(x) for x in signed[-5:])) if len(signed) >= 5 else 0.0
    bias_10 = sum(signed[-10:]) / max(1e-9, sum(abs(x) for x in signed[-10:])) if len(signed) >= 10 else 0.0
    bias_20 = sum(signed) / max(1e-9, sum(abs(x) for x in signed)) if signed else 0.0
    acceleration = speed_3 - speed_10
    compression = range_5 / max(range_20, 1e-9)
    if direction == "LONG":
        dist_from_extreme_10 = (prev_prices[-1] - min(prev_prices[-10:])) / PIP if len(prev_prices) >= 10 else 0.0
        pullback_depth_10 = (max(prev_prices[-10:]) - prev_prices[-1]) / PIP if len(prev_prices) >= 10 else 0.0
        breakout_distance_20 = (prev_prices[-1] - max(prev_prices[:-1])) / PIP if len(prev_prices) > 1 else 0.0
    else:
        dist_from_extreme_10 = (max(prev_prices[-10:]) - prev_prices[-1]) / PIP if len(prev_prices) >= 10 else 0.0
        pullback_depth_10 = (prev_prices[-1] - min(prev_prices[-10:])) / PIP if len(prev_prices) >= 10 else 0.0
        breakout_distance_20 = (min(prev_prices[:-1]) - prev_prices[-1]) / PIP if len(prev_prices) > 1 else 0.0
    breakout_distance_20 = max(0.0, breakout_distance_20)
    return {
        "speed_3": speed_3,
        "speed_5": speed_5,
        "speed_10": speed_10,
        "vol_10": vol_10,
        "vol_20": vol_20,
        "range_5": range_5,
        "range_10": range_10,
        "range_20": range_20,
        "trend_3": trend_3,
        "trend_5": trend_5,
        "trend_10": trend_10,
        "trend_20": trend_20,
        "bias_5": bias_5,
        "bias_10": bias_10,
        "bias_20": bias_20,
        "acceleration": acceleration,
        "compression": compression,
        "dist_from_extreme_10": dist_from_extreme_10,
        "pullback_depth_10": pullback_depth_10,
        "breakout_distance_20": breakout_distance_20,
        "slope_consistency_10": slope_sign_consistency(prev_prices[-10:], direction),
        "slope_consistency_20": slope_sign_consistency(prev_prices, direction),
    }


def build_candidates(prices: list[dict[str, Any]], phase1_rows: list[dict[str, Any]], labels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    phase1_idx = {r["timestamp"]: r for r in phase1_rows}
    label_idx = {(r["timestamp_start"], r["direction"]): r["zone_label"] for r in labels}
    session_groups: dict[str, list[dict[str, Any]]] = {}
    for row in prices:
        session_groups.setdefault(row["session_date"], []).append(row)

    out: list[dict[str, Any]] = []
    for session_date, rows in session_groups.items():
        for idx, row in enumerate(rows):
            prev = rows[max(0, idx - 30):idx + 1]
            prev_prices = [r["price"] for r in prev]
            if len(prev_prices) < 21:
                continue
            outcome = phase1_idx[row["timestamp"]]
            for direction, exists_key, tau_key, mae_key, mfe_key, opp_tau_key in (
                ("LONG", "up_exists", "tau_up_min", "mae_up_pips", "mfe_up_pips", "tau_down_min"),
                ("SHORT", "down_exists", "tau_down_min", "mae_down_pips", "mfe_down_pips", "tau_up_min"),
            ):
                feats = compute_energy_features(direction, prev_prices)
                exists = outcome[exists_key] == "1"
                tau_this = int(float(outcome[tau_key])) if exists else 10**9
                tau_opp = int(float(outcome[opp_tau_key])) if outcome["up_exists" if direction == "SHORT" else "down_exists"] == "1" else 10**9
                mae = float(outcome[mae_key]) if exists else TARGET
                mfe = float(outcome[mfe_key])
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
                        "timestamp_start": row["timestamp"],
                        "direction": direction,
                        "label": label_idx.get((row["timestamp"], direction), "NOISE"),
                        "static_pips": static_pips,
                        "static_R": static_pips / TARGET,
                        "static_reason": static_reason,
                        "future_mfe_pips": mfe,
                        **feats,
                    }
                )
    return out


def select_rows(rows: list[dict[str, Any]], cfg: dict[str, float]) -> list[dict[str, Any]]:
    chosen = []
    last_dt_by_side: dict[str, datetime] = {}
    for row in rows:
        ts = parse_ts(row["timestamp_start"])
        if row["speed_3"] < cfg["speed_3_min"]:
            continue
        if row["speed_10"] < cfg["speed_10_min"]:
            continue
        if row["trend_5"] < cfg["trend_5_min"]:
            continue
        if row["trend_10"] < cfg["trend_10_min"]:
            continue
        if row["bias_10"] < cfg["bias_10_min"]:
            continue
        if row["bias_20"] < cfg["bias_20_min"]:
            continue
        if row["slope_consistency_10"] < cfg["slope_consistency_10_min"]:
            continue
        if row["compression"] > cfg["compression_max"]:
            continue
        if row["pullback_depth_10"] > cfg["pullback_depth_10_max"]:
            continue
        if row["dist_from_extreme_10"] < cfg["dist_from_extreme_10_min"]:
            continue
        if row["breakout_distance_20"] < cfg["breakout_distance_20_min"]:
            continue
        key = f"{ts.date().isoformat()}|{row['direction']}"
        prev = last_dt_by_side.get(key)
        if prev is not None and ts < prev + timedelta(minutes=3):
            continue
        chosen.append(row)
        last_dt_by_side[key] = ts
    return chosen


def summarize(chosen: list[dict[str, Any]], population: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    good = [r for r in population if r["label"] == "GOOD"]
    bad = [r for r in population if r["label"] == "BAD"]
    noise = [r for r in population if r["label"] == "NOISE"]
    keys = {(r["timestamp_start"], r["direction"]) for r in chosen}
    good_chosen = [r for r in good if (r["timestamp_start"], r["direction"]) in keys]
    bad_chosen = [r for r in bad if (r["timestamp_start"], r["direction"]) in keys]
    noise_chosen = [r for r in noise if (r["timestamp_start"], r["direction"]) in keys]
    wins = [r for r in chosen if r["static_pips"] > 0]
    losses = [r for r in chosen if r["static_pips"] < 0]
    timeouts = [r for r in chosen if r["static_pips"] == 0]
    total_pips = sum(r["static_pips"] for r in chosen)
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
        "avg_win_pips": mean(r["static_pips"] for r in wins) if wins else 0.0,
        "avg_loss_pips": mean(r["static_pips"] for r in losses) if losses else 0.0,
        "avg_pips": mean(r["static_pips"] for r in chosen) if chosen else 0.0,
        "total_pips": total_pips,
        "avg_R": mean(r["static_R"] for r in chosen) if chosen else 0.0,
        "expectancy_R": mean(r["static_R"] for r in chosen) if chosen else 0.0,
        "pips_per_hour": total_pips / HOURS_TOTAL if chosen else 0.0,
        "estimated_equity_per_hour_at_2pct_risk": (sum(r["static_R"] for r in chosen) * 0.02) / HOURS_TOTAL if chosen else 0.0,
    }


def fit(rows: list[dict[str, Any]]) -> dict[str, Any]:
    good = [r for r in rows if r["label"] == "GOOD"]
    if not good:
        out = summarize([], rows, {"logic": "no_good_rows"})
        out["top_configs"] = []
        return out
    grid = []
    for sq in (0.15, 0.30, 0.45):
        for tq in (0.15, 0.30, 0.45):
            for bq in (0.10, 0.25, 0.40):
                for sc in (0.55, 0.65, 0.75):
                    for comp in (0.75, 0.9, 1.05):
                        for pb in (0.35, 0.5, 0.65):
                            cfg = {
                                "logic": "energy_descriptor_v3",
                                "speed_3_min": percentile([r["speed_3"] for r in good], sq),
                                "speed_10_min": percentile([r["speed_10"] for r in good], sq),
                                "trend_5_min": percentile([r["trend_5"] for r in good], tq),
                                "trend_10_min": percentile([r["trend_10"] for r in good], tq),
                                "bias_10_min": percentile([r["bias_10"] for r in good], bq),
                                "bias_20_min": percentile([r["bias_20"] for r in good], bq),
                                "slope_consistency_10_min": sc,
                                "compression_max": comp,
                                "pullback_depth_10_max": percentile([r["pullback_depth_10"] for r in good], pb),
                                "dist_from_extreme_10_min": percentile([r["dist_from_extreme_10"] for r in good], tq),
                                "breakout_distance_20_min": percentile([r["breakout_distance_20"] for r in good], tq),
                            }
                            grid.append(cfg)
    best = None
    for cfg in grid:
        chosen = select_rows(rows, cfg)
        summary = summarize(chosen, rows, cfg)
        score = (
            summary["expectancy_R"] * 4.0
            + summary["good_capture"] * 3.0
            - summary["bad_trigger"] * 2.5
            - summary["noise_trigger"] * 1.5
            + summary["win_rate"] * 0.5
            + min(summary["trade_count"], 400) / 3000.0
        )
        if best is None or score > best[0]:
            best = (score, summary)
    assert best is not None
    best_summary = best[1]
    best_summary["top_configs"] = [{
        **best_summary["best_config"],
        "good_capture": best_summary["good_capture"],
        "bad_trigger": best_summary["bad_trigger"],
        "noise_trigger": best_summary["noise_trigger"],
        "trade_count": best_summary["trade_count"],
        "win_rate": best_summary["win_rate"],
        "avg_pips": best_summary["avg_pips"],
        "expectancy_R": best_summary["expectancy_R"],
        "pips_per_hour": best_summary["pips_per_hour"],
    }]
    return best_summary


def blockers(rows: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    counts = Counter()
    for row in rows:
        if row["label"] != "GOOD":
            continue
        if row["speed_3"] < cfg["speed_3_min"]:
            counts["speed_3_below_threshold"] += 1
        elif row["speed_10"] < cfg["speed_10_min"]:
            counts["speed_10_below_threshold"] += 1
        elif row["trend_5"] < cfg["trend_5_min"]:
            counts["trend_5_below_threshold"] += 1
        elif row["trend_10"] < cfg["trend_10_min"]:
            counts["trend_10_below_threshold"] += 1
        elif row["bias_10"] < cfg["bias_10_min"]:
            counts["bias_10_below_threshold"] += 1
        elif row["bias_20"] < cfg["bias_20_min"]:
            counts["bias_20_below_threshold"] += 1
        elif row["slope_consistency_10"] < cfg["slope_consistency_10_min"]:
            counts["slope_consistency_10_below_threshold"] += 1
        elif row["compression"] > cfg["compression_max"]:
            counts["compression_above_threshold"] += 1
        elif row["pullback_depth_10"] > cfg["pullback_depth_10_max"]:
            counts["pullback_depth_10_above_threshold"] += 1
        elif row["dist_from_extreme_10"] < cfg["dist_from_extreme_10_min"]:
            counts["dist_from_extreme_10_below_threshold"] += 1
        elif row["breakout_distance_20"] < cfg["breakout_distance_20_min"]:
            counts["breakout_distance_20_below_threshold"] += 1
    return {
        "first_blocker_reason_counts": dict(counts),
        "config_used": cfg,
        "candidate_entry_states": len(rows),
    }


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Energy-descriptor stage 7 fitter for the 11-session pipeline")
    parser.add_argument("--data-root", default="london_session_data_11")
    parser.add_argument("--phase1-csv", default="compiled_stage1_6_11_sessions/phase1/opportunity_map_raw.csv")
    parser.add_argument("--labeled-csv", default="compiled_stage1_6_11_sessions/phase4/opportunity_zones_labeled.csv")
    parser.add_argument("--output-dir", default="compiled_stage1_7_energy_v3_11_sessions/phase7")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    prices = load_prices(Path(args.data_root))
    phase1_rows = load_csv(Path(args.phase1_csv))
    labels = load_csv(Path(args.labeled_csv))
    candidates = build_candidates(prices, phase1_rows, labels)

    both = fit(candidates)
    long = fit([r for r in candidates if r["direction"] == "LONG"])
    short = fit([r for r in candidates if r["direction"] == "SHORT"])
    blocker_report = blockers(candidates, both["best_config"])

    write_json(output_dir / "entry_fit_both.json", both)
    write_json(output_dir / "entry_fit_long.json", long)
    write_json(output_dir / "entry_fit_short.json", short)
    write_json(output_dir / "entry_blockers.json", blocker_report)

    print(json.dumps({"both": both, "long": long, "short": short, "blockers": blocker_report}, indent=2))


if __name__ == "__main__":
    main()
