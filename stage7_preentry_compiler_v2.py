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


def directional_pressure(prices: list[float], direction: str, window: int) -> float:
    if len(prices) < window + 1:
        window = len(prices) - 1
    diffs = [(prices[i] - prices[i - 1]) / PIP for i in range(len(prices) - window, len(prices))]
    signed = [d if direction == "LONG" else -d for d in diffs]
    pos = sum(max(0.0, d) for d in signed)
    neg = sum(abs(min(0.0, d)) for d in signed)
    return (pos - neg) / max(pos + neg, 1e-9)


def rolling_signed_close_position(prices: list[float], direction: str, window: int) -> float:
    segment = prices[-window:]
    hi = max(segment)
    lo = min(segment)
    pos = (segment[-1] - lo) / max(hi - lo, 1e-9)
    return pos if direction == "LONG" else (1.0 - pos)


def find_last_impulse(prices: list[float], direction: str) -> dict[str, float]:
    signed = [((prices[i] - prices[i - 1]) / PIP) if direction == "LONG" else ((prices[i - 1] - prices[i]) / PIP) for i in range(1, len(prices))]
    best_sum = 0.0
    best_end = len(signed)
    run = 0.0
    run_start = 0
    best_start = 0
    for i, val in enumerate(signed):
        if run <= 0:
            run = val
            run_start = i
        else:
            run += val
        if run > best_sum:
            best_sum = run
            best_start = run_start
            best_end = i + 1
    impulse_start_price = prices[best_start]
    impulse_end_price = prices[best_end]
    last_price = prices[-1]
    impulse_size = signed_pips(direction, impulse_start_price, impulse_end_price)
    if direction == "LONG":
        pullback = (impulse_end_price - last_price) / PIP
        reclaim = (last_price - min(prices[best_end:])) / PIP if best_end < len(prices) - 1 else 0.0
    else:
        pullback = (last_price - impulse_end_price) / PIP
        reclaim = (max(prices[best_end:]) - last_price) / PIP if best_end < len(prices) - 1 else 0.0
    return {
        "impulse_pullback_last_impulse_size": max(0.0, impulse_size),
        "impulse_pullback_pullback_depth": max(0.0, pullback),
        "impulse_pullback_pullback_pct": max(0.0, pullback) / max(abs(impulse_size), 0.25),
        "impulse_pullback_bars_since_impulse_peak": float(max(0, len(prices) - 1 - best_end)),
        "impulse_pullback_reclaim_strength": max(0.0, reclaim),
        "impulse_pullback_entry_vs_impulse_origin": signed_pips(direction, impulse_start_price, last_price),
    }


def breakout_reclaim_features(prices: list[float], direction: str, window: int = 20) -> dict[str, float]:
    seg = prices[-window:]
    prior = seg[:-1]
    last = seg[-1]
    if direction == "LONG":
        break_level = max(prior)
        breakout_distance = (last - break_level) / PIP
        above = [p > break_level for p in seg]
    else:
        break_level = min(prior)
        breakout_distance = (break_level - last) / PIP
        above = [p < break_level for p in seg]
    bars_beyond = 0
    for val in reversed(above):
        if val:
            bars_beyond += 1
        else:
            break
    if direction == "LONG":
        failure_return = (break_level - min(seg[-5:])) / PIP if bars_beyond else 0.0
    else:
        failure_return = (max(seg[-5:]) - break_level) / PIP if bars_beyond else 0.0
    return {
        "breakout_reclaim_breakout_distance": max(0.0, breakout_distance),
        "breakout_reclaim_bars_beyond_level": float(bars_beyond),
        "breakout_reclaim_break_hold_time": float(bars_beyond),
        "breakout_reclaim_failure_return_distance": max(0.0, failure_return),
        "breakout_reclaim_reclaim_strength": max(0.0, breakout_distance - failure_return),
    }


def swing_structure_features(prices: list[float], direction: str, window: int = 20) -> dict[str, float]:
    seg = prices[-window:]
    last = seg[-1]
    highs = []
    lows = []
    for i in range(1, len(seg) - 1):
        if seg[i] > seg[i - 1] and seg[i] > seg[i + 1]:
            highs.append(seg[i])
        if seg[i] < seg[i - 1] and seg[i] < seg[i + 1]:
            lows.append(seg[i])
    last_high = highs[-1] if highs else max(seg)
    last_low = lows[-1] if lows else min(seg)
    if direction == "LONG":
        dist_high = (last_high - last) / PIP
        dist_low = (last - last_low) / PIP
        broke_swing = 1.0 if last > last_high else 0.0
        range_edge = (last - min(seg)) / max(max(seg) - min(seg), 1e-9)
    else:
        dist_high = (last_high - last) / PIP
        dist_low = (last - last_low) / PIP
        broke_swing = 1.0 if last < last_low else 0.0
        range_edge = (max(seg) - last) / max(max(seg) - min(seg), 1e-9)
    failures = 0
    for i in range(2, len(seg)):
        if direction == "LONG" and seg[i - 2] < seg[i - 1] > seg[i] and seg[i] < seg[i - 2]:
            failures += 1
        if direction == "SHORT" and seg[i - 2] > seg[i - 1] < seg[i] and seg[i] > seg[i - 2]:
            failures += 1
    return {
        "swing_structure_distance_to_last_swing_high": max(0.0, dist_high),
        "swing_structure_distance_to_last_swing_low": max(0.0, dist_low),
        "swing_structure_break_of_last_swing": broke_swing,
        "swing_structure_failure_count": float(failures),
        "swing_structure_position_in_local_box": float(range_edge),
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


def quarter_from_dt(dt: datetime, session_start_hour: int = 7) -> str:
    minute_of_session = ((dt.hour - session_start_hour) % 24) * 60 + dt.minute
    if minute_of_session < 120:
        return "Q1"
    if minute_of_session < 240:
        return "Q2"
    if minute_of_session < 360:
        return "Q3"
    return "Q4"


def session_relative_context(
    session_rows: list[dict[str, Any]],
    idx: int,
    direction: str,
) -> dict[str, float]:
    prices = [r["price"] for r in session_rows[:idx + 1]]
    dt = session_rows[idx]["dt"]
    quarter = quarter_from_dt(dt)
    quarter_start_map = {"Q1": 0, "Q2": 120, "Q3": 240, "Q4": 360}
    minute_of_session = (dt.hour - 8) * 60 + dt.minute
    quarter_start = quarter_start_map[quarter]
    qtd_rows = session_rows[:idx + 1]
    qtd_prices = [r["price"] for r in qtd_rows[max(0, quarter_start):]]
    pressure_5 = directional_pressure(prices, direction, 5)
    pressure_15 = directional_pressure(prices, direction, 15)
    pressure_30 = directional_pressure(prices, direction, min(30, len(prices) - 1))
    qtd_pressure = directional_pressure(qtd_prices, direction, min(len(qtd_prices) - 1, max(1, len(qtd_prices) - 1))) if len(qtd_prices) > 1 else 0.0
    running_pressure = directional_pressure(prices, direction, min(len(prices) - 1, max(1, len(prices) - 1))) if len(prices) > 1 else 0.0
    return {
        "pressure_5": pressure_5,
        "pressure_15": pressure_15,
        "pressure_30": pressure_30,
        "pressure_ratio_5_15": pressure_5 - pressure_15,
        "pressure_ratio_15_30": pressure_15 - pressure_30,
        "session_relative_bias_vs_session_mean": pressure_15 - running_pressure,
        "session_relative_directional_dominance_qtd": qtd_pressure,
        "session_relative_quarter_relative_push": pressure_5 - qtd_pressure,
        "session_relative_signed_close_position_5": rolling_signed_close_position(prices, direction, min(6, len(prices))),
    }


def build_truth_rows(
    data_root: Path,
    labeled_csv: Path,
    entry_windows_csv: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any], list[str]]:
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
        if idx < 30:
            continue
        prev_prices = [r["price"] for r in session_rows[:idx + 1]]
        pressure = session_relative_context(session_rows, idx, direction)
        impulse = find_last_impulse(prev_prices, direction)
        breakout = breakout_reclaim_features(prev_prices, direction, window=min(20, len(prev_prices)))
        swing = swing_structure_features(prev_prices, direction, window=min(20, len(prev_prices)))
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
                "quarter": quarter_from_dt(parse_ts(ts)),
                "target_distance": float(row["target_distance"]),
                "time_to_target": float(row["time_to_target"]),
                "future_mfe_pips": float(row["max_mfe_pips"]),
                "future_mae_pips": float(row["max_mae_pips"]),
                "static_pips": static_pips,
                "static_R": static_pips / TARGET,
                **pressure,
                **impulse,
                **breakout,
                **swing,
            }
        )
    truth_rows.sort(key=lambda r: (r["session_id"], parse_ts(r["timestamp"]), r["direction"]))
    feature_names = [
        "pressure_5", "pressure_15", "pressure_30", "pressure_ratio_5_15", "pressure_ratio_15_30",
        "session_relative_bias_vs_session_mean", "session_relative_directional_dominance_qtd",
        "session_relative_quarter_relative_push", "session_relative_signed_close_position_5",
        "impulse_pullback_last_impulse_size", "impulse_pullback_pullback_depth",
        "impulse_pullback_pullback_pct", "impulse_pullback_bars_since_impulse_peak",
        "impulse_pullback_reclaim_strength", "impulse_pullback_entry_vs_impulse_origin",
        "breakout_reclaim_breakout_distance", "breakout_reclaim_bars_beyond_level",
        "breakout_reclaim_break_hold_time", "breakout_reclaim_failure_return_distance",
        "breakout_reclaim_reclaim_strength",
        "swing_structure_distance_to_last_swing_high", "swing_structure_distance_to_last_swing_low",
        "swing_structure_break_of_last_swing", "swing_structure_failure_count",
        "swing_structure_position_in_local_box",
    ]
    report = {
        "truth_row_count": len(truth_rows),
        "stage4_labeled_row_count": len(labeled_rows),
        "stage3_valid_entry_row_count": len(entry_rows),
        "stage3_overlap_count": sum(r["in_stage3_valid_window"] for r in truth_rows),
        "timestamps_strictly_sorted": True,
        "label_counts": Counter(r["outcome_label"] for r in truth_rows),
        "feature_family_count": 5,
        "feature_count": len(feature_names),
    }
    return truth_rows, report, feature_names


def feature_metrics(rows: list[dict[str, Any]], feature_names: list[str]) -> list[dict[str, Any]]:
    good_rows = [r for r in rows if r["outcome_label"] == "GOOD"]
    bad_rows = [r for r in rows if r["outcome_label"] == "BAD"]
    noise_rows = [r for r in rows if r["outcome_label"] == "NOISE"]
    others = [r for r in rows if r["outcome_label"] != "GOOD"]
    table: list[dict[str, Any]] = []
    labels = [1 if r["outcome_label"] == "GOOD" else 0 for r in rows]
    for feature in feature_names:
        all_vals = [float(r[feature]) for r in rows]
        good_vals = [float(r[feature]) for r in good_rows]
        bad_vals = [float(r[feature]) for r in bad_rows]
        noise_vals = [float(r[feature]) for r in noise_rows]
        other_vals = [float(r[feature]) for r in others]
        mi = mutual_information_binary(all_vals, labels)
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
                "signal_strength": strength,
                "classification": klass,
            }
        )
    table.sort(key=lambda r: r["signal_strength"], reverse=True)
    for idx, row in enumerate(table, 1):
        row["signal_strength_rank"] = idx
    return table


def add_interactions(rows: list[dict[str, Any]]) -> list[str]:
    interactions = {
        "pressure_accel": lambda r: float(r["pressure_ratio_5_15"]),
        "pressure_stack": lambda r: float(r["pressure_5"]) * float(r["pressure_15"]),
        "pullback_reclaim_ratio": lambda r: float(r["impulse_pullback_pullback_depth"]) / max(float(r["impulse_pullback_reclaim_strength"]), 0.25),
        "breakout_acceptance": lambda r: float(r["breakout_reclaim_breakout_distance"]) * (1.0 + float(r["breakout_reclaim_bars_beyond_level"])),
        "swing_pressure_alignment": lambda r: float(r["session_relative_quarter_relative_push"]) * float(r["swing_structure_position_in_local_box"]),
        "impulse_context": lambda r: float(r["impulse_pullback_entry_vs_impulse_origin"]) - float(r["impulse_pullback_pullback_depth"]),
        "failure_pressure_gap": lambda r: float(r["session_relative_directional_dominance_qtd"]) - float(r["swing_structure_failure_count"]),
        "reclaim_structure": lambda r: float(r["breakout_reclaim_reclaim_strength"]) * (1.0 + float(r["swing_structure_break_of_last_swing"])),
    }
    for row in rows:
        for name, fn in interactions.items():
            row[name] = fn(row)
    return list(interactions.keys())


def reduce_feature_set(feature_table: list[dict[str, Any]], interaction_table: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, Any]:
    ranked = [r for r in feature_table + interaction_table if r["classification"] in {"strong_signal", "weak_signal"}]
    ranked.sort(key=lambda r: r["signal_strength"], reverse=True)
    selected: list[str] = []
    for row in ranked:
        feat = row["feature"]
        vals = [float(r[feat]) for r in rows]
        if any(abs(correlation(vals, [float(r[s]) for r in rows])) > 0.85 for s in selected):
            continue
        selected.append(feat)
        if len(selected) >= 8:
            break
    return {"selected_features": selected, "selected_count": len(selected), "candidate_count": len(ranked)}


def derive_surface_rules(rows: list[dict[str, Any]], features: list[str]) -> dict[str, Any]:
    good = [r for r in rows if r["outcome_label"] == "GOOD"]
    cfg: dict[str, Any] = {"logic": "derived_entry_surface_v2", "rules": []}
    for feat in features:
        good_vals = [float(r[feat]) for r in good]
        all_vals = [float(r[feat]) for r in rows]
        if mean(good_vals) >= mean(all_vals):
            cfg["rules"].append({"feature": feat, "op": ">=", "threshold": percentile(good_vals, 0.35)})
        else:
            cfg["rules"].append({"feature": feat, "op": "<=", "threshold": percentile(good_vals, 0.65)})
    return cfg


def select_by_rules(rows: list[dict[str, Any]], cfg: dict[str, Any]) -> list[dict[str, Any]]:
    chosen = []
    last_dt_by_side: dict[str, datetime] = {}
    for row in rows:
        ok = True
        for rule in cfg["rules"]:
            val = float(row[rule["feature"]])
            thr = float(rule["threshold"])
            if rule["op"] == ">=" and val < thr:
                ok = False
                break
            if rule["op"] == "<=" and val > thr:
                ok = False
                break
        if not ok:
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


def stability_report(rows: list[dict[str, Any]], cfg: dict[str, Any]) -> dict[str, Any]:
    sessions = sorted({r["session_id"] for r in rows})
    split = set(sessions[:5])
    first = [r for r in rows if r["session_id"] in split]
    second = [r for r in rows if r["session_id"] not in split]
    first_sum = replay_summary(select_by_rules(first, cfg), first)
    second_sum = replay_summary(select_by_rules(second, cfg), second)
    return {
        "capture_deviation": abs(first_sum["good_capture"] - second_sum["good_capture"]),
        "win_rate_deviation": abs(first_sum["win_rate"] - second_sum["win_rate"]),
        "stability_pass": abs(first_sum["good_capture"] - second_sum["good_capture"]) < 0.10 and abs(first_sum["win_rate"] - second_sum["win_rate"]) < 0.05,
        "first_half": first_sum,
        "second_half": second_sum,
    }


def make_edge_maps(rows: list[dict[str, Any]], out_dir: Path) -> dict[str, Any]:
    xs = np.asarray([float(r["pressure_ratio_5_15"]) for r in rows])
    ys = np.asarray([float(r["impulse_pullback_pullback_depth"]) / max(float(r["breakout_reclaim_breakout_distance"]), 0.25) for r in rows])
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
    bins = []
    for xi in range(20):
        for yi in range(20):
            mask = (xs >= xbins[xi]) & (xs < xbins[xi + 1]) & (ys >= ybins[yi]) & (ys < ybins[yi + 1])
            count = int(mask.sum())
            if not count:
                continue
            lbls = [labels[i] for i, m in enumerate(mask) if m]
            good_rate = lbls.count("GOOD") / count
            ev = float(evs[mask].mean())
            label_map[yi, xi] = good_rate
            ev_map[yi, xi] = ev
            bins.append({"x_bin": xi, "y_bin": yi, "occupancy": count, "good_rate": good_rate, "ev_R": ev})
    plt.figure(figsize=(8, 6))
    plt.imshow(label_map, origin="lower", aspect="auto", cmap="RdYlGn", interpolation="nearest")
    plt.colorbar(label="GOOD rate")
    plt.tight_layout()
    labels_path = out_dir / "entry_edge_map_labels.png"
    plt.savefig(labels_path, dpi=150)
    plt.close()
    plt.figure(figsize=(8, 6))
    plt.imshow(ev_map, origin="lower", aspect="auto", cmap="RdYlGn", interpolation="nearest")
    plt.colorbar(label="Mean R")
    plt.tight_layout()
    ev_path = out_dir / "entry_edge_map_ev.png"
    plt.savefig(ev_path, dpi=150)
    plt.close()
    return {
        "labels_plot": str(labels_path),
        "ev_plot": str(ev_path),
        "top_good_bins": sorted(bins, key=lambda r: r["good_rate"], reverse=True)[:10],
        "top_ev_bins": sorted(bins, key=lambda r: r["ev_R"], reverse=True)[:10],
        "bottom_ev_bins": sorted(bins, key=lambda r: r["ev_R"])[:10],
        "bin_count": len(bins),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", required=True, type=Path)
    parser.add_argument("--labeled-csv", required=True, type=Path)
    parser.add_argument("--entry-windows-csv", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--baseline-report", required=True, type=Path)
    args = parser.parse_args()

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    truth_rows, truth_report, feature_names = build_truth_rows(args.data_root, args.labeled_csv, args.entry_windows_csv)
    interaction_names = add_interactions(truth_rows)
    truth_fields = [
        "timestamp", "session_id", "cluster_id", "entry_candidate_state",
        "in_stage3_valid_window", "outcome_label", "direction", "quarter",
        "target_distance", "time_to_target", "future_mfe_pips", "future_mae_pips", "static_pips", "static_R",
        *feature_names, *interaction_names,
    ]
    write_csv(out_dir / "entry_state_truth_table.csv", truth_rows, truth_fields)
    (out_dir / "entry_truth_dataset_report.json").write_text(json.dumps(truth_report, indent=2))

    feature_table = feature_metrics(truth_rows, feature_names)
    interaction_table = feature_metrics(truth_rows, interaction_names)
    write_csv(out_dir / "feature_separability_table.csv", feature_table, list(feature_table[0].keys()))
    write_csv(out_dir / "interaction_separability_table.csv", interaction_table, list(interaction_table[0].keys()))
    (out_dir / "feature_separability_report.json").write_text(json.dumps({
        "row_count": len(truth_rows),
        "feature_count": len(feature_names),
        "strong_signal_count": sum(1 for r in feature_table if r["classification"] == "strong_signal"),
        "weak_signal_count": sum(1 for r in feature_table if r["classification"] == "weak_signal"),
        "top_features": feature_table[:10],
    }, indent=2))
    (out_dir / "interaction_separability_report.json").write_text(json.dumps({
        "interaction_count": len(interaction_names),
        "strong_signal_count": sum(1 for r in interaction_table if r["classification"] == "strong_signal"),
        "top_interactions": interaction_table[:10],
    }, indent=2))

    reduced = reduce_feature_set(feature_table, interaction_table, truth_rows)
    (out_dir / "reduced_feature_set.json").write_text(json.dumps(reduced, indent=2))
    model = derive_surface_rules(truth_rows, reduced["selected_features"])
    (out_dir / "entry_surface_model.json").write_text(json.dumps(model, indent=2))
    (out_dir / "entry_surface_rules.json").write_text(json.dumps(model, indent=2))

    selected = select_by_rules(truth_rows, model)
    static_replay = replay_summary(selected, truth_rows)
    static_replay["config"] = model
    (out_dir / "entry_static_replay_report.json").write_text(json.dumps(static_replay, indent=2))

    stability = stability_report(truth_rows, model)
    (out_dir / "entry_surface_stability_report.json").write_text(json.dumps(stability, indent=2))

    verification = {
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
    write_csv(out_dir / "stage7_entry_population.csv", selected, ["timestamp", "direction", "session_id", "outcome_label", "static_pips", "static_R", *reduced["selected_features"]])
    edge_summary = make_edge_maps(truth_rows, out_dir)
    (out_dir / "entry_edge_map_summary.json").write_text(json.dumps(edge_summary, indent=2))

    baseline = json.loads(args.baseline_report.read_text())
    v1_feature_report = baseline.get("feature_separability_report", baseline.get("feature_report"))
    v1_interaction_report = baseline.get("interaction_separability_report", baseline.get("interaction_report"))
    comparison = {
        "v1": {
            "strong_signal_feature_count": v1_feature_report["strong_signal_count"] if v1_feature_report else None,
            "strong_signal_interaction_count": v1_interaction_report["strong_signal_count"] if v1_interaction_report else None,
            "selected_feature_count": len(baseline["reduced_feature_set"]["selected_features"]) if "reduced_feature_set" in baseline else None,
            "static_replay": baseline["entry_static_replay_report"] if "entry_static_replay_report" in baseline else None,
        },
        "v2": {
            "strong_signal_feature_count": sum(1 for r in feature_table if r["classification"] == "strong_signal"),
            "strong_signal_interaction_count": sum(1 for r in interaction_table if r["classification"] == "strong_signal"),
            "selected_feature_count": len(reduced["selected_features"]),
            "static_replay": static_replay,
        },
    }
    if comparison["v1"]["static_replay"]:
        v1 = comparison["v1"]["static_replay"]
        v2 = comparison["v2"]["static_replay"]
        comparison["delta"] = {
            "good_capture": v2["good_capture"] - v1["good_capture"],
            "bad_trigger": v2["bad_trigger"] - v1["bad_trigger"],
            "noise_trigger": v2["noise_trigger"] - v1["noise_trigger"],
            "pips_per_hour": v2["pips_per_hour"] - v1["pips_per_hour"],
            "expectancy": v2["expectancy"] - v1["expectancy"],
            "trade_count": v2["trade_count"] - v1["trade_count"],
        }
    (out_dir / "stage7_protocol_v1_vs_v2_comparison.json").write_text(json.dumps(comparison, indent=2))
    (out_dir / "stage7_protocol_report.json").write_text(json.dumps({
        "truth_report": truth_report,
        "feature_report": json.loads((out_dir / "feature_separability_report.json").read_text()),
        "interaction_report": json.loads((out_dir / "interaction_separability_report.json").read_text()),
        "reduced_feature_set": reduced,
        "static_replay": static_replay,
        "stability": stability,
        "verification": verification,
    }, indent=2))

    print(json.dumps({
        "truth_rows": len(truth_rows),
        "selected_trades": static_replay["trade_count"],
        "good_capture": static_replay["good_capture"],
        "bad_trigger": static_replay["bad_trigger"],
        "noise_trigger": static_replay["noise_trigger"],
        "pips_per_hour": static_replay["pips_per_hour"],
        "selected_features": reduced["selected_features"],
        "ceiling_reached": verification["ceiling_reached"],
    }, indent=2))


if __name__ == "__main__":
    main()
