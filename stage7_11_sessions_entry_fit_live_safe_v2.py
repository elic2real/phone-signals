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
                    "session_date": dt.date().isoformat(),
                    "session": "london" if 8 <= dt.hour < 16 else "other",
                    "weekday": dt.strftime("%A").lower(),
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


def build_candidates(prices: list[dict[str, Any]], phase1_rows: list[dict[str, Any]], labels: list[dict[str, Any]]) -> list[dict[str, Any]]:
    phase1_idx = {r["timestamp"]: r for r in phase1_rows}
    label_idx = {(r["timestamp_start"], r["direction"]): r["zone_label"] for r in labels}
    session_groups: dict[str, list[dict[str, Any]]] = {}
    for row in prices:
        session_groups.setdefault(row["session_date"], []).append(row)

    out: list[dict[str, Any]] = []
    for session_date, rows in session_groups.items():
        for idx, row in enumerate(rows):
            outcome = phase1_idx[row["timestamp"]]
            for direction, exists_key, tau_key, mae_key, mfe_key, opp_tau_key in (
                ("LONG", "up_exists", "tau_up_min", "mae_up_pips", "mfe_up_pips", "tau_down_min"),
                ("SHORT", "down_exists", "tau_down_min", "mae_down_pips", "mfe_down_pips", "tau_up_min"),
            ):
                prev = rows[max(0, idx - 30):idx + 1]
                prev_prices = [r["price"] for r in prev]
                if len(prev_prices) < 6:
                    continue
                diffs = [(prev_prices[i] - prev_prices[i - 1]) / PIP for i in range(1, len(prev_prices))]
                adiffs = [abs(x) for x in diffs]
                speed_3 = mean(adiffs[-3:])
                speed_5 = mean(adiffs[-5:])
                vol_10 = mean(adiffs[-10:]) if len(adiffs) >= 10 else mean(adiffs)
                vol_20 = mean(adiffs[-20:]) if len(adiffs) >= 20 else mean(adiffs)
                range_10 = (max(prev_prices[-10:]) - min(prev_prices[-10:])) / PIP if len(prev_prices) >= 10 else (max(prev_prices) - min(prev_prices)) / PIP
                trend_5 = signed_pips(direction, prev_prices[max(0, len(prev_prices) - 6)], prev_prices[-1])
                trend_15 = signed_pips(direction, prev_prices[max(0, len(prev_prices) - 16)], prev_prices[-1])
                signed_recent = [x if direction == "LONG" else -x for x in diffs[-15:]]
                bias_15 = sum(signed_recent) / max(1e-9, sum(abs(x) for x in signed_recent))
                acceleration = speed_3 - vol_10
                compression = vol_10 / max(vol_20, 1e-9)
                if direction == "LONG":
                    dist_from_extreme = (prev_prices[-1] - min(prev_prices[-10:])) / PIP
                else:
                    dist_from_extreme = (max(prev_prices[-10:]) - prev_prices[-1]) / PIP

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
                        "dt": row["dt"],
                        "session_date": session_date,
                        "direction": direction,
                        "speed_3": speed_3,
                        "speed_5": speed_5,
                        "vol_10": vol_10,
                        "vol_20": vol_20,
                        "range_10": range_10,
                        "trend_5": trend_5,
                        "trend_15": trend_15,
                        "bias_15": bias_15,
                        "acceleration": acceleration,
                        "compression": compression,
                        "dist_from_extreme": dist_from_extreme,
                        "label": label_idx.get((row["timestamp"], direction), "NOISE"),
                        "static_pips": static_pips,
                        "static_R": static_pips / TARGET,
                        "static_reason": static_reason,
                        "future_mfe_pips": mfe,
                    }
                )
    return out


def select_rows(rows: list[dict[str, Any]], cfg: dict[str, float]) -> list[dict[str, Any]]:
    chosen = []
    last_dt_by_day: dict[str, datetime] = {}
    for row in rows:
        if row["speed_3"] < cfg["speed_3_min"]:
            continue
        if row["speed_5"] < cfg["speed_5_min"]:
            continue
        if row["vol_10"] < cfg["vol_10_min"]:
            continue
        if row["range_10"] < cfg["range_10_min"]:
            continue
        if row["trend_5"] < cfg["trend_5_min"]:
            continue
        if row["bias_15"] < cfg["bias_15_min"]:
            continue
        if row["acceleration"] < cfg["acceleration_min"]:
            continue
        if row["compression"] > cfg["compression_max"]:
            continue
        if row["dist_from_extreme"] < cfg["dist_from_extreme_min"]:
            continue
        key = f"{row['session_date']}|{row['direction']}"
        prev = last_dt_by_day.get(key)
        if prev is not None and row["dt"] < prev + timedelta(minutes=3):
            continue
        chosen.append(row)
        last_dt_by_day[key] = row["dt"]
    return chosen


def summarize(chosen: list[dict[str, Any]], population: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    good = [r for r in population if r["label"] == "GOOD"]
    bad = [r for r in population if r["label"] == "BAD"]
    noise = [r for r in population if r["label"] == "NOISE"]
    chosen_keys = {(r["timestamp_start"], r["direction"]) for r in chosen}
    good_chosen = [r for r in good if (r["timestamp_start"], r["direction"]) in chosen_keys]
    bad_chosen = [r for r in bad if (r["timestamp_start"], r["direction"]) in chosen_keys]
    noise_chosen = [r for r in noise if (r["timestamp_start"], r["direction"]) in chosen_keys]
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


def fit(rows: list[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    good = [r for r in rows if r["label"] == "GOOD"]
    if not good:
        empty = summarize([], rows, {"logic": "no_good_rows"})
        empty["top_configs"] = []
        return empty, []
    grid = []
    for sq in (0.2, 0.35, 0.5):
        for tq in (0.15, 0.3, 0.45):
            for bq in (0.1, 0.25, 0.4):
                for aq in (0.2, 0.35, 0.5):
                    for cx in (0.9, 1.05, 1.2):
                        cfg = {
                            "logic": "live_safe_multifeature_v2",
                            "speed_3_min": percentile([r["speed_3"] for r in good], sq),
                            "speed_5_min": percentile([r["speed_5"] for r in good], sq),
                            "vol_10_min": percentile([r["vol_10"] for r in good], tq),
                            "range_10_min": percentile([r["range_10"] for r in good], tq),
                            "trend_5_min": percentile([r["trend_5"] for r in good], tq),
                            "bias_15_min": percentile([r["bias_15"] for r in good], bq),
                            "acceleration_min": percentile([r["acceleration"] for r in good], aq),
                            "compression_max": cx,
                            "dist_from_extreme_min": percentile([r["dist_from_extreme"] for r in good], tq),
                        }
                        grid.append(cfg)
    best_summary = None
    best_rows = []
    best_score = None
    for cfg in grid:
        chosen = select_rows(rows, cfg)
        summary = summarize(chosen, rows, cfg)
        score = (
            summary["expectancy_R"] * 4.0
            + summary["good_capture"] * 2.0
            - summary["bad_trigger"] * 2.5
            - summary["noise_trigger"] * 1.25
            + summary["win_rate"] * 0.75
            + min(summary["trade_count"], 400) / 4000.0
        )
        if best_score is None or score > best_score:
            best_score = score
            best_summary = summary
            best_rows = chosen
    assert best_summary is not None
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
    return best_summary, best_rows


def blockers(rows: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    counts = Counter()
    for row in rows:
        if row["label"] != "GOOD":
            continue
        if row["speed_3"] < cfg["speed_3_min"]:
            counts["speed_3_below_threshold"] += 1
        elif row["speed_5"] < cfg["speed_5_min"]:
            counts["speed_5_below_threshold"] += 1
        elif row["vol_10"] < cfg["vol_10_min"]:
            counts["vol_10_below_threshold"] += 1
        elif row["range_10"] < cfg["range_10_min"]:
            counts["range_10_below_threshold"] += 1
        elif row["trend_5"] < cfg["trend_5_min"]:
            counts["trend_5_below_threshold"] += 1
        elif row["bias_15"] < cfg["bias_15_min"]:
            counts["bias_15_below_threshold"] += 1
        elif row["acceleration"] < cfg["acceleration_min"]:
            counts["acceleration_below_threshold"] += 1
        elif row["compression"] > cfg["compression_max"]:
            counts["compression_above_threshold"] += 1
        elif row["dist_from_extreme"] < cfg["dist_from_extreme_min"]:
            counts["dist_from_extreme_below_threshold"] += 1
    return {
        "first_blocker_reason_counts": dict(counts),
        "config_used": cfg,
        "candidate_entry_states": len(rows),
    }


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Improved live-safe stage 7 fitter for the 11-session pipeline")
    parser.add_argument("--data-root", default="london_session_data_11")
    parser.add_argument("--phase1-csv", default="compiled_stage1_6_11_sessions/phase1/opportunity_map_raw.csv")
    parser.add_argument("--labeled-csv", default="compiled_stage1_6_11_sessions/phase4/opportunity_zones_labeled.csv")
    parser.add_argument("--output-dir", default="compiled_stage1_7_live_safe_v2_11_sessions/phase7")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    prices = load_prices(Path(args.data_root))
    phase1_rows = load_csv(Path(args.phase1_csv))
    labels = load_csv(Path(args.labeled_csv))
    candidates = build_candidates(prices, phase1_rows, labels)

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
