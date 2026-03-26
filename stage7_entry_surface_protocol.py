#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any

import matplotlib.pyplot as plt
import numpy as np
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


def load_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


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


def ks_statistic(a: list[float], b: list[float]) -> float:
    if not a or not b:
        return 0.0
    a_sorted = sorted(a)
    b_sorted = sorted(b)
    i = j = 0
    d = 0.0
    while i < len(a_sorted) or j < len(b_sorted):
        if j >= len(b_sorted) or (i < len(a_sorted) and a_sorted[i] <= b_sorted[j]):
            x = a_sorted[i]
        else:
            x = b_sorted[j]
        while i < len(a_sorted) and a_sorted[i] <= x:
            i += 1
        while j < len(b_sorted) and b_sorted[j] <= x:
            j += 1
        d = max(d, abs(i / len(a_sorted) - j / len(b_sorted)))
    return d


def auroc(good: list[float], other: list[float]) -> float:
    if not good or not other:
        return 0.5
    wins = 0.0
    ties = 0.0
    for g in good:
        for o in other:
            if g > o:
                wins += 1
            elif g == o:
                ties += 1
    total = len(good) * len(other)
    return (wins + 0.5 * ties) / total if total else 0.5


def mutual_information_binary(feature: list[float], labels: list[int], bins: int = 10) -> float:
    if not feature or len(set(feature)) <= 1:
        return 0.0
    f = np.asarray(feature, dtype=float)
    y = np.asarray(labels, dtype=int)
    edges = np.quantile(f, np.linspace(0.0, 1.0, bins + 1))
    edges[0] -= 1e-9
    edges[-1] += 1e-9
    digitized = np.digitize(f, edges[1:-1], right=True)
    total = len(f)
    mi = 0.0
    for b in range(bins):
        mask_b = digitized == b
        p_b = mask_b.sum() / total
        if p_b == 0:
            continue
        for cls in (0, 1):
            mask_bc = mask_b & (y == cls)
            p_bc = mask_bc.sum() / total
            if p_bc == 0:
                continue
            p_c = (y == cls).sum() / total
            mi += p_bc * math.log(p_bc / (p_b * p_c + 1e-12) + 1e-12)
    return float(mi)


def correlation(a: list[float], b: list[float]) -> float:
    if len(a) != len(b) or len(a) < 2:
        return 0.0
    return float(np.corrcoef(np.asarray(a, dtype=float), np.asarray(b, dtype=float))[0, 1])


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
    speed_3 = mean(adiffs[-3:])
    speed_5 = mean(adiffs[-5:])
    speed_10 = mean(adiffs[-10:])
    vol_10 = speed_10
    vol_20 = mean(adiffs[-20:]) if len(adiffs) >= 20 else mean(adiffs)
    range_5 = (max(prev_prices[-5:]) - min(prev_prices[-5:])) / PIP
    range_10 = (max(prev_prices[-10:]) - min(prev_prices[-10:])) / PIP
    range_20 = (max(prev_prices[-20:]) - min(prev_prices[-20:])) / PIP
    trend_3 = signed_pips(direction, prev_prices[-4], prev_prices[-1])
    trend_5 = signed_pips(direction, prev_prices[-6], prev_prices[-1])
    trend_10 = signed_pips(direction, prev_prices[-11], prev_prices[-1])
    trend_20 = signed_pips(direction, prev_prices[0], prev_prices[-1])
    bias_5 = sum(signed[-5:]) / max(1e-9, sum(abs(x) for x in signed[-5:]))
    bias_10 = sum(signed[-10:]) / max(1e-9, sum(abs(x) for x in signed[-10:]))
    bias_20 = sum(signed) / max(1e-9, sum(abs(x) for x in signed))
    acceleration = speed_3 - speed_10
    compression = range_5 / max(range_20, 1e-9)
    if direction == "LONG":
        dist_from_extreme_10 = (prev_prices[-1] - min(prev_prices[-10:])) / PIP
        pullback_depth_10 = (max(prev_prices[-10:]) - prev_prices[-1]) / PIP
        breakout_distance_20 = (prev_prices[-1] - max(prev_prices[:-1])) / PIP
    else:
        dist_from_extreme_10 = (max(prev_prices[-10:]) - prev_prices[-1]) / PIP
        pullback_depth_10 = (prev_prices[-1] - min(prev_prices[-10:])) / PIP
        breakout_distance_20 = (min(prev_prices[:-1]) - prev_prices[-1]) / PIP
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


def load_prices(data_root: Path) -> dict[str, list[dict[str, Any]]]:
    by_session: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for p in sorted(data_root.rglob("part-000.parquet")):
        df = pd.read_parquet(p)
        for rec in df.to_dict("records"):
            dt = parse_ts(str(rec["timestamp"]))
            session_id = str(rec.get("session_id") or dt.date().isoformat())
            by_session[session_id].append(
                {
                    "timestamp": str(rec["timestamp"]),
                    "dt": dt,
                    "price": float(rec["close"]),
                    "session_id": session_id,
                }
            )
    for rows in by_session.values():
        rows.sort(key=lambda r: r["dt"])
    return by_session


def build_truth_rows(
    data_root: Path,
    labeled_csv: Path,
    entry_windows_csv: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    prices_by_session = load_prices(data_root)
    labeled_rows = load_csv(labeled_csv)
    entry_rows = load_csv(entry_windows_csv)
    valid_entry_keys = {(r["timestamp"], r["direction"]) for r in entry_rows}
    truth_rows: list[dict[str, Any]] = []
    by_session_index: dict[str, dict[str, int]] = {}
    for session_date, rows in prices_by_session.items():
        by_session_index[session_date] = {r["timestamp"]: idx for idx, r in enumerate(rows)}
    for row in labeled_rows:
        session_date = row["session_date"]
        ts = row["timestamp_start"]
        direction = row["direction"]
        idx = by_session_index[session_date].get(ts)
        if idx is None:
            continue
        session_rows = prices_by_session[session_date]
        if idx < 20:
            continue
        prev_prices = [r["price"] for r in session_rows[idx - 20:idx + 1]]
        feats = compute_energy_features(direction, prev_prices)
        static_pips = TARGET if float(row["max_mfe_pips"]) >= TARGET and float(row["max_mae_pips"]) <= TARGET else (-TARGET if float(row["max_mae_pips"]) > TARGET else 0.0)
        truth_rows.append(
            {
                "timestamp": ts,
                "session_id": session_date,
                "cluster_id": "",
                "entry_candidate_state": 1,
                "in_stage3_valid_window": int((ts, direction) in valid_entry_keys),
                "outcome_label": row["zone_label"],
                "direction": direction,
                "target_distance": float(row["target_distance"]),
                "time_to_target": float(row["time_to_target"]),
                "future_mfe_pips": float(row["max_mfe_pips"]),
                "future_mae_pips": float(row["max_mae_pips"]),
                "static_pips": static_pips,
                "static_R": static_pips / TARGET,
                **feats,
            }
        )
    truth_rows.sort(key=lambda r: (r["session_id"], parse_ts(r["timestamp"]), r["direction"]))
    report = {
        "truth_row_count": len(truth_rows),
        "stage4_labeled_row_count": len(labeled_rows),
        "stage3_valid_entry_row_count": len(entry_rows),
        "stage3_overlap_count": sum(r["in_stage3_valid_window"] for r in truth_rows),
        "timestamps_strictly_sorted": all(
            (truth_rows[i]["session_id"], truth_rows[i]["timestamp"], truth_rows[i]["direction"])
            <= (truth_rows[i + 1]["session_id"], truth_rows[i + 1]["timestamp"], truth_rows[i + 1]["direction"])
            for i in range(len(truth_rows) - 1)
        ),
        "label_counts": Counter(r["outcome_label"] for r in truth_rows),
        "note": "Stage 3 contains valid-entry windows only; stage 7 truth surface uses the full stage-4 timestamp/direction labeled surface and reports exact overlap with stage 3.",
    }
    return truth_rows, report


def feature_metrics(rows: list[dict[str, Any]], feature_names: list[str]) -> tuple[list[dict[str, Any]], dict[str, list[float]]]:
    good_rows = [r for r in rows if r["outcome_label"] == "GOOD"]
    bad_rows = [r for r in rows if r["outcome_label"] == "BAD"]
    noise_rows = [r for r in rows if r["outcome_label"] == "NOISE"]
    others = [r for r in rows if r["outcome_label"] != "GOOD"]
    feature_values: dict[str, list[float]] = {f: [float(r[f]) for r in rows] for f in feature_names}
    table: list[dict[str, Any]] = []
    for feature in feature_names:
        good_vals = [float(r[feature]) for r in good_rows]
        bad_vals = [float(r[feature]) for r in bad_rows]
        noise_vals = [float(r[feature]) for r in noise_rows]
        other_vals = [float(r[feature]) for r in others]
        mi = mutual_information_binary(feature_values[feature], [1 if r["outcome_label"] == "GOOD" else 0 for r in rows])
        auc = auroc(good_vals, other_vals)
        ks = ks_statistic(good_vals, other_vals)
        strength = (abs(auc - 0.5) * 2.0) + ks + mi
        if strength >= 0.6:
            klass = "strong_signal"
        elif strength >= 0.3:
            klass = "weak_signal"
        elif auc < 0.45:
            klass = "anti_signal"
        else:
            klass = "no_signal"
        table.append(
            {
                "feature": feature,
                "ks_statistic": ks,
                "mutual_information": mi,
                "auroc_good_vs_rest": auc,
                "good_mean": mean(good_vals) if good_vals else 0.0,
                "bad_mean": mean(bad_vals) if bad_vals else 0.0,
                "noise_mean": mean(noise_vals) if noise_vals else 0.0,
                "good_median": median(good_vals) if good_vals else 0.0,
                "bad_median": median(bad_vals) if bad_vals else 0.0,
                "noise_median": median(noise_vals) if noise_vals else 0.0,
                "signal_strength": strength,
                "classification": klass,
            }
        )
    table.sort(key=lambda r: r["signal_strength"], reverse=True)
    for idx, row in enumerate(table, start=1):
        row["signal_strength_rank"] = idx
    return table, feature_values


def add_interactions(rows: list[dict[str, Any]]) -> list[str]:
    interactions = {
        "speed_accel": lambda r: float(r["speed_3"]) - float(r["speed_10"]),
        "trend_accel": lambda r: float(r["trend_5"]) - float(r["trend_10"]),
        "bias_alignment": lambda r: float(r["bias_10"]) - float(r["bias_20"]),
        "pullback_breakout_ratio": lambda r: float(r["pullback_depth_10"]) / max(float(r["breakout_distance_20"]), 0.25),
        "compression_speed": lambda r: float(r["compression"]) * float(r["speed_3"]),
        "bias_slope": lambda r: float(r["bias_10"]) * float(r["slope_consistency_10"]),
        "range_speed_ratio": lambda r: float(r["range_10"]) / max(float(r["speed_10"]), 0.25),
        "trend_compression": lambda r: float(r["trend_5"]) * float(r["compression"]),
    }
    for row in rows:
        for name, fn in interactions.items():
            row[name] = fn(row)
    return list(interactions.keys())


def reduce_feature_set(
    feature_table: list[dict[str, Any]],
    interaction_table: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    ranked = [r for r in feature_table + interaction_table if r["classification"] in {"strong_signal", "weak_signal"}]
    ranked.sort(key=lambda r: r["signal_strength"], reverse=True)
    selected: list[str] = []
    correlations: dict[str, dict[str, float]] = {}
    for row in ranked:
        feat = row["feature"]
        if len(selected) >= 8:
            break
        vals = [float(r[feat]) for r in rows]
        too_correlated = False
        for chosen in selected:
            corr = abs(correlation(vals, [float(r[chosen]) for r in rows]))
            correlations.setdefault(feat, {})[chosen] = corr
            if corr > 0.85:
                too_correlated = True
                break
        if not too_correlated:
            selected.append(feat)
    return {
        "selected_features": selected,
        "selected_count": len(selected),
        "candidate_count": len(ranked),
        "correlation_screen_max": 0.85,
        "correlations_checked": correlations,
    }


def derive_surface_rules(rows: list[dict[str, Any]], features: list[str]) -> dict[str, Any]:
    good = [r for r in rows if r["outcome_label"] == "GOOD"]
    cfg: dict[str, Any] = {"logic": "derived_entry_surface_v1", "rules": []}
    for feat in features[:5]:
        good_vals = [float(r[feat]) for r in good]
        all_vals = [float(r[feat]) for r in rows]
        if not good_vals:
            continue
        if mean(good_vals) >= mean(all_vals):
            threshold = percentile(good_vals, 0.35)
            cfg["rules"].append({"feature": feat, "op": ">=", "threshold": threshold})
        else:
            threshold = percentile(good_vals, 0.65)
            cfg["rules"].append({"feature": feat, "op": "<=", "threshold": threshold})
    return cfg


def select_by_rules(rows: list[dict[str, Any]], cfg: dict[str, Any]) -> list[dict[str, Any]]:
    chosen = []
    last_dt_by_side: dict[str, datetime] = {}
    for row in rows:
        passed = True
        for rule in cfg["rules"]:
            val = float(row[rule["feature"]])
            thr = float(rule["threshold"])
            if rule["op"] == ">=" and val < thr:
                passed = False
                break
            if rule["op"] == "<=" and val > thr:
                passed = False
                break
        if not passed:
            continue
        ts = parse_ts(row["timestamp"])
        key = f"{row['session_id']}|{row['direction']}"
        prev = last_dt_by_side.get(key)
        if prev is not None and ts < prev + timedelta(minutes=3):
            continue
        chosen.append(row)
        last_dt_by_side[key] = ts
    return chosen


def replay_summary(chosen: list[dict[str, Any]], population: list[dict[str, Any]]) -> dict[str, Any]:
    good = [r for r in population if r["outcome_label"] == "GOOD"]
    bad = [r for r in population if r["outcome_label"] == "BAD"]
    noise = [r for r in population if r["outcome_label"] == "NOISE"]
    keys = {(r["timestamp"], r["direction"]) for r in chosen}
    good_chosen = [r for r in good if (r["timestamp"], r["direction"]) in keys]
    bad_chosen = [r for r in bad if (r["timestamp"], r["direction"]) in keys]
    noise_chosen = [r for r in noise if (r["timestamp"], r["direction"]) in keys]
    wins = [r for r in chosen if float(r["static_pips"]) > 0]
    losses = [r for r in chosen if float(r["static_pips"]) < 0]
    total_pips = sum(float(r["static_pips"]) for r in chosen)
    return {
        "trade_count": len(chosen),
        "win_rate": len(wins) / len(chosen) if chosen else 0.0,
        "avg_win": mean(float(r["static_pips"]) for r in wins) if wins else 0.0,
        "avg_loss": mean(float(r["static_pips"]) for r in losses) if losses else 0.0,
        "expectancy": mean(float(r["static_pips"]) for r in chosen) if chosen else 0.0,
        "avg_R": mean(float(r["static_R"]) for r in chosen) if chosen else 0.0,
        "pips_per_hour": total_pips / HOURS_TOTAL if chosen else 0.0,
        "estimated_equity_per_hour_at_2pct_risk": (sum(float(r["static_R"]) for r in chosen) * 0.02) / HOURS_TOTAL if chosen else 0.0,
        "good_capture": len(good_chosen) / len(good) if good else 0.0,
        "bad_trigger": len(bad_chosen) / len(bad) if bad else 0.0,
        "noise_trigger": len(noise_chosen) / len(noise) if noise else 0.0,
        "wins": len(wins),
        "losses": len(losses),
    }


def build_stability(rows: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    sessions = sorted({r["session_id"] for r in rows})
    split = set(sessions[:5])
    first = [r for r in rows if r["session_id"] in split]
    second = [r for r in rows if r["session_id"] not in split]
    first_sel = select_by_rules(first, cfg)
    second_sel = select_by_rules(second, cfg)
    first_sum = replay_summary(first_sel, first)
    second_sum = replay_summary(second_sel, second)
    return {
        "train_sessions": sorted(split),
        "test_sessions": sorted(set(sessions) - split),
        "first_half": first_sum,
        "second_half": second_sum,
        "capture_deviation": abs(first_sum["good_capture"] - second_sum["good_capture"]),
        "win_rate_deviation": abs(first_sum["win_rate"] - second_sum["win_rate"]),
        "stability_pass": abs(first_sum["good_capture"] - second_sum["good_capture"]) < 0.10
        and abs(first_sum["win_rate"] - second_sum["win_rate"]) < 0.05,
    }


def make_edge_maps(rows: list[dict[str, Any]], out_dir: Path) -> dict[str, Any]:
    xs = np.asarray([float(r["speed_accel"]) for r in rows])
    ys = np.asarray([float(r["pullback_breakout_ratio"]) for r in rows])
    evs = np.asarray([float(r["static_R"]) for r in rows])
    labels = [r["outcome_label"] for r in rows]
    xbins = np.quantile(xs, np.linspace(0.0, 1.0, 21))
    ybins = np.quantile(ys, np.linspace(0.0, 1.0, 21))
    xbins[0] -= 1e-9
    xbins[-1] += 1e-9
    ybins[0] -= 1e-9
    ybins[-1] += 1e-9
    label_map = np.full((20, 20), np.nan)
    ev_map = np.full((20, 20), np.nan)
    summary_rows = []
    for xi in range(20):
        for yi in range(20):
            mask = (
                (xs >= xbins[xi]) & (xs < xbins[xi + 1]) &
                (ys >= ybins[yi]) & (ys < ybins[yi + 1])
            )
            count = int(mask.sum())
            if not count:
                continue
            bin_labels = [labels[i] for i, m in enumerate(mask) if m]
            good_rate = bin_labels.count("GOOD") / count
            ev = float(evs[mask].mean())
            label_map[yi, xi] = good_rate
            ev_map[yi, xi] = ev
            summary_rows.append(
                {
                    "x_bin": xi,
                    "y_bin": yi,
                    "x_range": [float(xbins[xi]), float(xbins[xi + 1])],
                    "y_range": [float(ybins[yi]), float(ybins[yi + 1])],
                    "occupancy": count,
                    "good_rate": good_rate,
                    "ev_R": ev,
                }
            )
    plt.figure(figsize=(8, 6))
    plt.imshow(label_map, origin="lower", aspect="auto", cmap="RdYlGn", interpolation="nearest")
    plt.colorbar(label="GOOD rate")
    plt.xlabel("speed_3 - speed_10")
    plt.ylabel("pullback_depth_10 / breakout_distance_20")
    plt.title("Entry Edge Map by Label Composition")
    plt.tight_layout()
    labels_path = out_dir / "entry_edge_map_labels.png"
    plt.savefig(labels_path, dpi=150)
    plt.close()

    plt.figure(figsize=(8, 6))
    plt.imshow(ev_map, origin="lower", aspect="auto", cmap="RdYlGn", interpolation="nearest")
    plt.colorbar(label="Mean R")
    plt.xlabel("speed_3 - speed_10")
    plt.ylabel("pullback_depth_10 / breakout_distance_20")
    plt.title("Entry Edge Map by Expected Value")
    plt.tight_layout()
    ev_path = out_dir / "entry_edge_map_ev.png"
    plt.savefig(ev_path, dpi=150)
    plt.close()

    summary_rows.sort(key=lambda r: r["good_rate"], reverse=True)
    top_good = summary_rows[:10]
    summary_rows_by_ev = sorted(summary_rows, key=lambda r: r["ev_R"], reverse=True)
    top_ev = summary_rows_by_ev[:10]
    bottom_ev = sorted(summary_rows, key=lambda r: r["ev_R"])[:10]
    return {
        "labels_plot": str(labels_path),
        "ev_plot": str(ev_path),
        "top_good_bins": top_good,
        "top_ev_bins": top_ev,
        "bottom_ev_bins": bottom_ev,
        "bin_count": len(summary_rows),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--labeled-csv", required=True, type=Path)
    parser.add_argument("--entry-windows-csv", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    truth_rows, truth_report = build_truth_rows(args.data_root, args.labeled_csv, args.entry_windows_csv)
    base_features = [
        "speed_3", "speed_5", "speed_10",
        "vol_10", "vol_20",
        "range_5", "range_10", "range_20",
        "trend_3", "trend_5", "trend_10", "trend_20",
        "bias_5", "bias_10", "bias_20",
        "acceleration", "compression",
        "dist_from_extreme_10", "pullback_depth_10",
        "breakout_distance_20", "slope_consistency_10", "slope_consistency_20",
    ]
    interaction_features = add_interactions(truth_rows)

    truth_fields = [
        "timestamp", "session_id", "cluster_id", "entry_candidate_state",
        "in_stage3_valid_window", "outcome_label", "direction", "target_distance",
        "time_to_target", "future_mfe_pips", "future_mae_pips", "static_pips", "static_R",
        *base_features, *interaction_features,
    ]
    write_csv(out_dir / "entry_state_truth_table.csv", truth_rows, truth_fields)
    (out_dir / "entry_truth_dataset_report.json").write_text(json.dumps(truth_report, indent=2))

    feature_table, _ = feature_metrics(truth_rows, base_features)
    interaction_table, _ = feature_metrics(truth_rows, interaction_features)
    write_csv(out_dir / "feature_separability_table.csv", feature_table, list(feature_table[0].keys()))
    write_csv(out_dir / "interaction_separability_table.csv", interaction_table, list(interaction_table[0].keys()))
    feature_report = {
        "row_count": len(truth_rows),
        "feature_count": len(base_features),
        "strong_signal_count": sum(1 for r in feature_table if r["classification"] == "strong_signal"),
        "weak_signal_count": sum(1 for r in feature_table if r["classification"] == "weak_signal"),
        "no_signal_count": sum(1 for r in feature_table if r["classification"] == "no_signal"),
        "anti_signal_count": sum(1 for r in feature_table if r["classification"] == "anti_signal"),
        "top_features": feature_table[:10],
    }
    interaction_report = {
        "interaction_count": len(interaction_features),
        "strong_signal_count": sum(1 for r in interaction_table if r["classification"] == "strong_signal"),
        "top_interactions": interaction_table[:10],
    }
    (out_dir / "feature_separability_report.json").write_text(json.dumps(feature_report, indent=2))
    (out_dir / "interaction_separability_report.json").write_text(json.dumps(interaction_report, indent=2))

    reduced = reduce_feature_set(feature_table, interaction_table, truth_rows)
    (out_dir / "reduced_feature_set.json").write_text(json.dumps(reduced, indent=2))

    surface_model = derive_surface_rules(truth_rows, reduced["selected_features"])
    (out_dir / "entry_surface_model.json").write_text(json.dumps(surface_model, indent=2))
    (out_dir / "entry_surface_rules.json").write_text(json.dumps(surface_model, indent=2))

    selected = select_by_rules(truth_rows, surface_model)
    static_replay = replay_summary(selected, truth_rows)
    static_replay["config"] = surface_model
    (out_dir / "entry_static_replay_report.json").write_text(json.dumps(static_replay, indent=2))

    stability = build_stability(truth_rows, surface_model)
    (out_dir / "entry_surface_stability_report.json").write_text(json.dumps(stability, indent=2))

    verification = {
        "good_capture_threshold": 0.60,
        "bad_trigger_threshold": 0.06,
        "noise_trigger_threshold": 0.30,
        "expectancy_positive": static_replay["expectancy"] > 0,
        "trade_count_threshold": 1000,
        "actual": static_replay,
        "ceiling_reached": (
            static_replay["good_capture"] >= 0.60
            and static_replay["bad_trigger"] <= 0.06
            and static_replay["noise_trigger"] <= 0.30
            and static_replay["expectancy"] > 0
            and static_replay["trade_count"] >= 1000
        ),
    }
    (out_dir / "entry_ceiling_verification.json").write_text(json.dumps(verification, indent=2))

    stage7_population_fields = ["timestamp", "direction", "session_id", "outcome_label", "static_pips", "static_R", *reduced["selected_features"]]
    write_csv(out_dir / "stage7_entry_population.csv", selected, stage7_population_fields)

    edge_map_summary = make_edge_maps(truth_rows, out_dir)
    (out_dir / "entry_edge_map_summary.json").write_text(json.dumps(edge_map_summary, indent=2))

    compiler_report = {
        "truth_report": truth_report,
        "static_replay": static_replay,
        "stability": stability,
        "verification": verification,
        "edge_maps": edge_map_summary,
    }
    (out_dir / "stage7_protocol_report.json").write_text(json.dumps(compiler_report, indent=2))

    print(json.dumps({
        "truth_rows": len(truth_rows),
        "selected_trades": static_replay["trade_count"],
        "good_capture": static_replay["good_capture"],
        "bad_trigger": static_replay["bad_trigger"],
        "noise_trigger": static_replay["noise_trigger"],
        "pips_per_hour": static_replay["pips_per_hour"],
        "ceiling_reached": verification["ceiling_reached"],
        "top_reduced_features": reduced["selected_features"],
    }, indent=2))


if __name__ == "__main__":
    main()
