#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Tuple

import pandas as pd


ROOT = Path(".")
PIP = 0.0001
DISTANCES = [1.5, 2.5, 3.5, 5.0, 6.0, 7.0, 8.0]
HORIZON = 100
CLUSTER_GAP_MIN = 5
RUNNER_MIN_DISTANCE = 2.5
RUNNER_PARTIAL_TP = 1.5
RUNNER_PARTIAL_FRACTION = 0.9


def parse_ts(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def percentile(vals: List[float], q: float) -> float:
    if not vals:
        return 0.0
    vals = sorted(vals)
    if len(vals) == 1:
        return vals[0]
    idx = q * (len(vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(vals) - 1)
    frac = idx - lo
    return vals[lo] * (1 - frac) + vals[hi] * frac


def pnl(direction: str, start: float, px: float) -> float:
    return ((px - start) / PIP) if direction == "LONG" else ((start - px) / PIP)


def path_efficiency(prices: List[float]) -> float:
    if len(prices) < 2:
        return 0.0
    path_len = sum(abs(prices[i] - prices[i - 1]) / PIP for i in range(1, len(prices)))
    if path_len <= 0:
        return 0.0
    net = abs(prices[-1] - prices[0]) / PIP
    return net / path_len


def write_json(path: str, data: Dict[str, Any]) -> None:
    (ROOT / path).write_text(json.dumps(data, indent=2))


def write_csv(path: str, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with (ROOT / path).open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def load_prices() -> List[Dict[str, Any]]:
    p = ROOT / "london_session_data/pair=EUR_USD/year=2024/month=01/part-000.parquet"
    df = pd.read_parquet(p)
    rows = []
    for rec in df.to_dict("records"):
        ts = parse_ts(str(rec["timestamp"]))
        rows.append(
            {
                "pair": rec["pair"],
                "timestamp": str(rec["timestamp"]),
                "dt": ts,
                "price": float(rec["close"]),
                "session": "london" if 8 <= ts.hour < 16 else "other",
                "weekday": ts.strftime("%A").lower(),
            }
        )
    return rows


def discover_for_distance(prices: List[Dict[str, Any]], distance: float) -> List[Dict[str, Any]]:
    out = []
    for i, point in enumerate(prices):
        start = point["price"]
        pre = prices[max(0, i - 10):i]
        pre_prices = [r["price"] for r in pre]
        pre_range = ((max(pre_prices) - min(pre_prices)) / PIP) if pre_prices else 0.0
        pre_trend = ((pre_prices[-1] - pre_prices[0]) / PIP) if len(pre_prices) > 1 else 0.0
        pre_vol = mean(abs(pre_prices[j] - pre_prices[j - 1]) / PIP for j in range(1, len(pre_prices))) if len(pre_prices) > 1 else 0.0
        for direction in ("LONG", "SHORT"):
            best = 0.0
            worst = 0.0
            tau = None
            path = [start]
            for k in range(1, min(HORIZON + 1, len(prices) - i)):
                px = prices[i + k]["price"]
                path.append(px)
                favorable = pnl(direction, start, px)
                adverse = pnl("SHORT" if direction == "LONG" else "LONG", start, px)
                best = max(best, favorable)
                worst = max(worst, adverse)
                if tau is None and favorable >= distance:
                    tau = k
            if tau is None:
                continue
            out.append(
                {
                    "pair": point["pair"],
                    "timestamp_start": point["timestamp"],
                    "dt": point["dt"],
                    "direction": direction,
                    "distance": distance,
                    "session": point["session"],
                    "weekday": point["weekday"],
                    "time_to_target": tau,
                    "future_mfe": best,
                    "future_mae": worst,
                    "speed": distance / tau if tau else 0.0,
                    "efficiency": min(1.0, distance / max(best + worst, distance)) if best > 0 else 0.0,
                    "pre_range_pips": pre_range,
                    "pre_trend_pips": pre_trend,
                    "pre_volatility": pre_vol,
                    "price_start": start,
                    "price_path": path,
                }
            )
    return out


def build_cluster_state_rows(
    prices: List[Dict[str, Any]],
    clusters: List[Dict[str, Any]],
    distance: float,
    direction: str,
) -> List[Dict[str, Any]]:
    ts_index = {row["timestamp"]: idx for idx, row in enumerate(prices)}
    state_rows = []
    for cluster in clusters:
        if cluster["distance"] != distance or cluster["direction"] != direction:
            continue
        start_dt = parse_ts(cluster["cluster_start"])
        end_dt = parse_ts(cluster["cluster_end"])
        cluster_indices = [i for i, row in enumerate(prices) if start_dt <= row["dt"] <= end_dt]
        if not cluster_indices:
            continue
        cluster_start_idx = cluster_indices[0]
        cluster_end_idx = cluster_indices[-1]
        cluster_length = max(1, cluster_end_idx - cluster_start_idx)
        for idx in cluster_indices:
            row = prices[idx]
            pre_window = prices[max(0, idx - 10):idx + 1]
            pre_prices = [r["price"] for r in pre_window]
            pre_range = ((max(pre_prices) - min(pre_prices)) / PIP) if pre_prices else 0.0
            pre_trend = ((pre_prices[-1] - pre_prices[0]) / PIP) if len(pre_prices) > 1 else 0.0
            pre_vol = mean(abs(pre_prices[j] - pre_prices[j - 1]) / PIP for j in range(1, len(pre_prices))) if len(pre_prices) > 1 else 0.0
            pre_speed = mean(abs(pre_prices[j] - pre_prices[j - 1]) / PIP for j in range(max(1, len(pre_prices) - 4), len(pre_prices))) if len(pre_prices) > 1 else 0.0
            pre_eff = path_efficiency(pre_prices)
            start_price = row["price"]
            best = 0.0
            worst = 0.0
            tau = None
            path = [start_price]
            for k in range(1, min(HORIZON + 1, len(prices) - idx)):
                px = prices[idx + k]["price"]
                path.append(px)
                favorable = pnl(direction, start_price, px)
                adverse = pnl("SHORT" if direction == "LONG" else "LONG", start_price, px)
                best = max(best, favorable)
                worst = max(worst, adverse)
                if tau is None and favorable >= distance:
                    tau = k
            static_row = {
                "price_start": start_price,
                "direction": direction,
                "price_path": path,
            }
            harvester_sim = simulate_harvester_trade(static_row, distance)
            state_rows.append(
                {
                    "cluster_id": cluster["cluster_id"],
                    "distance": distance,
                    "direction": direction,
                    "timestamp_start": row["timestamp"],
                    "dt": row["dt"],
                    "session": row["session"],
                    "weekday": row["weekday"],
                    "price_start": start_price,
                    "price_path": path,
                    "pre_range_pips": pre_range,
                    "pre_trend_pips": pre_trend,
                    "pre_volatility": pre_vol,
                    "pre_speed": pre_speed,
                    "pre_efficiency": pre_eff,
                    "minutes_from_cluster_start": idx - cluster_start_idx,
                    "minutes_to_cluster_end": cluster_end_idx - idx,
                    "cluster_progress": (idx - cluster_start_idx) / cluster_length,
                    "future_mfe": best,
                    "future_mae": worst,
                    "time_to_target": tau if tau is not None else HORIZON + 1,
                    "harvester_profit": harvester_sim["pips"],
                    "harvester_reason": harvester_sim["reason"],
                    "runner_extension": max(0.0, best - distance),
                    "stop_hit": harvester_sim["reason"] == "SL_HIT",
                }
            )
    return state_rows


def cluster_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[(row["distance"], row["direction"])].append(row)
    clusters = []
    for (distance, direction), items in grouped.items():
        items = sorted(items, key=lambda r: r["dt"])
        current = None
        idx = 0
        for row in items:
            end = row["dt"] + timedelta(minutes=int(row["time_to_target"]))
            if current is None or row["dt"] > current["end"] + timedelta(minutes=CLUSTER_GAP_MIN):
                idx += 1
                current = {
                    "cluster_id": f"{direction}_{distance:g}_{idx:03d}",
                    "distance": distance,
                    "direction": direction,
                    "start": row["dt"],
                    "end": end,
                    "members": [],
                }
                clusters.append(current)
            current["members"].append(row)
            if end > current["end"]:
                current["end"] = end
    cluster_rows = []
    for c in clusters:
        members = c["members"]
        best_member = max(members, key=lambda r: (r["future_mfe"] - r["future_mae"], r["speed"]))
        cluster_rows.append(
            {
                "cluster_id": c["cluster_id"],
                "distance": c["distance"],
                "direction": c["direction"],
                "cluster_start": c["start"].isoformat(),
                "cluster_end": c["end"].isoformat(),
                "member_count": len(members),
                "teacher_label": label_from_member(best_member),
                "best_member_ts": best_member["timestamp_start"],
            }
        )
    return cluster_rows


def label_from_member(row: Dict[str, Any]) -> str:
    quality = row["future_mfe"] - row["future_mae"]
    if row["future_mae"] >= row["distance"]:
        return "BAD"
    if quality >= row["distance"] * 0.75 and row["speed"] >= row["distance"] / 10.0:
        return "GOOD"
    return "NOISE"


def select_trades(
    rows: List[Dict[str, Any]],
    clusters: List[Dict[str, Any]],
    mode: str,
    distance: float,
    direction: str,
    speed_q: float,
    eff_q: float,
) -> List[Dict[str, Any]]:
    candidates = [r for r in rows if r["distance"] == distance and r["direction"] == direction]
    if not candidates:
        return []
    speeds = [r["pre_speed"] for r in candidates]
    effs = [r["pre_efficiency"] for r in candidates]
    if mode == "harvester":
        speed_cut = percentile(speeds, speed_q)
        eff_cut = percentile(effs, eff_q)
        chosen = []
        last_by_cluster: Dict[str, datetime] = {}
        cluster_map = {c["cluster_id"]: c for c in clusters if c["distance"] == distance and c["direction"] == direction}
        for cluster in cluster_map.values():
            member_rows = sorted([r for r in candidates if cluster["cluster_start"] <= r["dt"].isoformat() <= cluster["cluster_end"]], key=lambda r: r["dt"])
            for row in member_rows:
                if row["pre_speed"] < speed_cut or row["pre_efficiency"] < eff_cut:
                    continue
                prev = last_by_cluster.get(cluster["cluster_id"])
                if prev is None or row["dt"] >= prev + timedelta(minutes=3):
                    row = dict(row)
                    chosen.append(row)
                    last_by_cluster[cluster["cluster_id"]] = row["dt"]
        return chosen
    if distance < RUNNER_MIN_DISTANCE:
        return []
    speed_cut = percentile(speeds, speed_q)
    eff_cut = percentile(effs, eff_q)
    chosen = []
    cluster_map = {c["cluster_id"]: c for c in clusters if c["distance"] == distance and c["direction"] == direction}
    for cluster in cluster_map.values():
        member_rows = sorted([r for r in candidates if cluster["cluster_start"] <= r["dt"].isoformat() <= cluster["cluster_end"]], key=lambda r: r["dt"])
        for row in member_rows:
            if row["pre_speed"] >= speed_cut and row["pre_efficiency"] >= eff_cut:
                row = dict(row)
                chosen.append(row)
                break
    return chosen


def simulate_harvester_trade(row: Dict[str, Any], distance: float) -> Dict[str, Any]:
    start = row["price_start"]
    direction = row["direction"]
    path = row["price_path"]
    exit_pips = pnl(direction, start, path[-1])
    reason = "TIMEOUT"
    for px in path[1:]:
        cur = pnl(direction, start, px)
        if cur >= distance:
            exit_pips = distance
            reason = "TP_HIT"
            break
        if cur <= -distance:
            exit_pips = -distance
            reason = "SL_HIT"
            break
    return {"pips": exit_pips, "r": exit_pips / distance, "reason": reason}


def simulate_runner_trade(
    row: Dict[str, Any],
    distance: float,
    partial_tp: float | None = None,
    partial_fraction: float = RUNNER_PARTIAL_FRACTION,
) -> Dict[str, Any]:
    start = row["price_start"]
    direction = row["direction"]
    path = row["price_path"]
    partial_tp = RUNNER_PARTIAL_TP if partial_tp is None else partial_tp
    partial_bank = partial_fraction * partial_tp
    remainder_fraction = max(0.0, 1.0 - partial_fraction)
    runner_pips = 0.0
    partial_hit = False
    for px in path[1:]:
        cur = pnl(direction, start, px)
        if not partial_hit and cur >= partial_tp:
            partial_hit = True
        if cur <= -distance:
            if partial_hit:
                # Preserve banked profit and stop only the remaining size.
                runner_pips = -remainder_fraction * distance
                total = partial_bank + runner_pips
                return {"pips": total, "r": total / distance, "reason": "PARTIAL_THEN_SL", "partial_bank_pips": partial_bank, "runner_pips": runner_pips}
            return {"pips": -distance, "r": -1.0, "reason": "SL_HIT", "partial_bank_pips": 0.0, "runner_pips": 0.0}
    if partial_hit:
        for px in path[1:]:
            cur = pnl(direction, start, px)
            if cur >= distance:
                runner_pips = remainder_fraction * max(distance - partial_tp, 0.0)
                return {
                    "pips": partial_bank + runner_pips,
                    "r": (partial_bank + runner_pips) / distance,
                    "reason": "PARTIAL_PLUS_TP",
                    "partial_bank_pips": partial_bank,
                    "runner_pips": runner_pips,
                }
        total = partial_bank
        return {"pips": total, "r": total / distance, "reason": "PARTIAL_ONLY", "partial_bank_pips": partial_bank, "runner_pips": 0.0}
    final = pnl(direction, start, path[-1])
    return {"pips": final, "r": final / distance, "reason": "TIMEOUT", "partial_bank_pips": 0.0, "runner_pips": 0.0}


def infer_runner_configs_from_selected() -> Dict[tuple[str, float], Dict[str, float]]:
    path = ROOT / "entry_metric_ceiling_report_unified.json"
    if not path.exists():
        return {}
    report = json.loads(path.read_text())
    configs: Dict[tuple[str, float], Dict[str, float]] = {}
    for side in ("long", "short"):
        side_name = side.upper()
        for dist_text, payload in report["results"][side]["runner"].items():
            if not payload:
                continue
            rows = payload.get("profit_ceiling", {}).get("rows", [])
            distance = float(dist_text)
            inferred = None
            for row in rows:
                partial_bank = float(row.get("partial_bank_pips", 0.0) or 0.0)
                runner_pips = float(row.get("runner_pips", 0.0) or 0.0)
                reason = str(row.get("reason", ""))
                if partial_bank <= 0.0:
                    continue
                if reason == "PARTIAL_PLUS_TP":
                    denom = distance - runner_pips
                    if denom <= 0:
                        continue
                    partial_tp = distance - (runner_pips * distance / denom)
                    partial_fraction = partial_bank / partial_tp if partial_tp > 0 else 0.0
                    inferred = {"partial_tp": partial_tp, "partial_fraction": partial_fraction}
                    break
                if reason == "PARTIAL_THEN_SL":
                    remainder_frac = runner_pips / (-distance) if distance > 0 and runner_pips < 0 else None
                    if remainder_frac is None:
                        continue
                    partial_fraction = 1.0 - remainder_frac
                    partial_tp = partial_bank / partial_fraction if partial_fraction > 0 else 0.0
                    inferred = {"partial_tp": partial_tp, "partial_fraction": partial_fraction}
                    break
            if inferred is None:
                inferred = {"partial_tp": RUNNER_PARTIAL_TP, "partial_fraction": RUNNER_PARTIAL_FRACTION}
            configs[(side_name, distance)] = inferred
    return configs


def load_selected_runner_rows() -> Dict[tuple[str, float, str, str], Dict[str, Any]]:
    path = ROOT / "entry_metric_ceiling_report_unified.json"
    if not path.exists():
        return {}
    report = json.loads(path.read_text())
    out: Dict[tuple[str, float, str, str], Dict[str, Any]] = {}
    for side in ("long", "short"):
        side_name = side.upper()
        for dist_text, payload in report["results"][side]["runner"].items():
            if not payload:
                continue
            distance = float(dist_text)
            for row in payload.get("profit_ceiling", {}).get("rows", []):
                out[(side_name, distance, row["cluster_id"], row["timestamp_start"])] = row
    return out


def summarize(mode: str, direction: str, distance: float, chosen: List[Dict[str, Any]], clusters: List[Dict[str, Any]], speed_q: float, eff_q: float) -> Dict[str, Any]:
    rel_clusters = [c for c in clusters if c["distance"] == distance and c["direction"] == direction]
    cluster_labels = {c["cluster_id"]: c["teacher_label"] for c in rel_clusters}
    chosen_cluster_ids = {r["cluster_id"] for r in chosen if r["cluster_id"] in cluster_labels}
    taken_labels = Counter(cluster_labels[cluster_id] for cluster_id in chosen_cluster_ids)
    cluster_counts = Counter(cluster_labels.values())
    sim_rows = []
    for row in chosen:
        sim = simulate_harvester_trade(row, distance) if mode == "harvester" else simulate_runner_trade(row, distance)
        sim_rows.append({**row, **sim})
    wins = sum(1 for r in sim_rows if r["pips"] > 0)
    losses = sum(1 for r in sim_rows if r["pips"] < 0)
    breakeven = len(sim_rows) - wins - losses
    serializable_rows = []
    for row in sim_rows:
        clean = dict(row)
        if "dt" in clean:
            clean["dt"] = clean["dt"].isoformat()
        serializable_rows.append(clean)
    return {
        "mode": mode,
        "direction": direction,
        "tp_pips": distance,
        "sl_pips": distance,
        "speed_quantile": speed_q,
        "efficiency_quantile": eff_q,
        "cluster_population": len(rel_clusters),
        "good_clusters": cluster_counts.get("GOOD", 0),
        "bad_clusters": cluster_counts.get("BAD", 0),
        "noise_clusters": cluster_counts.get("NOISE", 0),
        "trade_count": len(sim_rows),
        "unique_clusters_traded": len(chosen_cluster_ids),
        "traded_good_clusters": taken_labels.get("GOOD", 0),
        "traded_bad_clusters": taken_labels.get("BAD", 0),
        "traded_noise_clusters": taken_labels.get("NOISE", 0),
        "capture_rate": taken_labels.get("GOOD", 0) / max(1, cluster_counts.get("GOOD", 0)),
        "cluster_participation_rate": len(chosen_cluster_ids) / max(1, len(rel_clusters)),
        "bad_trigger": taken_labels.get("BAD", 0) / max(1, cluster_counts.get("BAD", 0)) if cluster_counts.get("BAD", 0) else 0.0,
        "noise_trigger": taken_labels.get("NOISE", 0) / max(1, cluster_counts.get("NOISE", 0)) if cluster_counts.get("NOISE", 0) else 0.0,
        "wins": wins,
        "losses": losses,
        "breakeven": breakeven,
        "avg_R": mean([r["r"] for r in sim_rows]) if sim_rows else 0.0,
        "win_rate": wins / len(sim_rows) if sim_rows else 0.0,
        "avg_pips": mean([r["pips"] for r in sim_rows]) if sim_rows else 0.0,
        "total_pips": sum(r["pips"] for r in sim_rows) if sim_rows else 0.0,
        "pips_per_hour": sum(r["pips"] for r in sim_rows) / 9.0 if sim_rows else 0.0,
        "estimated_equity_per_hour_at_2pct_risk": ((sum(r["pips"] for r in sim_rows) / distance) * 0.02) / 9.0 if sim_rows else 0.0,
        "partial_bank_avg_pips": mean([r.get("partial_bank_pips", 0.0) for r in sim_rows]) if sim_rows else 0.0,
        "runner_avg_pips": mean([r.get("runner_pips", 0.0) for r in sim_rows]) if sim_rows else 0.0,
        "verdict": "PASS" if sim_rows else "FAIL",
        "rows": serializable_rows,
    }


def optimize_and_summarize(rows: List[Dict[str, Any]], clusters: List[Dict[str, Any]], mode: str, direction: str, distance: float) -> Dict[str, Any]:
    grid = [(0.15, 0.15), (0.25, 0.25), (0.35, 0.30), (0.45, 0.35), (0.55, 0.45), (0.65, 0.55), (0.75, 0.65)]
    best = None
    for speed_q, eff_q in grid:
        chosen = select_trades(rows, clusters, mode, distance, direction, speed_q, eff_q)
        summary = summarize(mode, direction, distance, chosen, clusters, speed_q, eff_q)
        score = (
            summary["total_pips"],
            summary["wins"],
            -summary["losses"],
            summary["capture_rate"],
            -summary["bad_trigger"],
            -summary["noise_trigger"],
        )
        if best is None or score > best[0]:
            best = (score, summary)
    return best[1] if best else summarize(mode, direction, distance, [], clusters, 0.0, 0.0)


def main() -> None:
    prices = load_prices()
    runner_configs = infer_runner_configs_from_selected()
    selected_runner_rows = load_selected_runner_rows()
    pretrade_rows = []
    outcome_rows = []
    summaries = []
    for distance in DISTANCES:
        discovered = discover_for_distance(prices, distance)
        distance_clusters = cluster_rows(discovered)
        state_rows = []
        for direction in ("LONG", "SHORT"):
            state_rows.extend(build_cluster_state_rows(prices, distance_clusters, distance, direction))
        for row in state_rows:
            harvester_sim = simulate_harvester_trade(row, distance)
            runner_cfg = runner_configs.get((row["direction"], float(distance)), {"partial_tp": RUNNER_PARTIAL_TP, "partial_fraction": RUNNER_PARTIAL_FRACTION})
            selected_runner = selected_runner_rows.get((row["direction"], float(distance), row["cluster_id"], row["timestamp_start"]))
            if selected_runner is not None:
                runner_sim = {
                    "pips": selected_runner["pips"],
                    "r": selected_runner["r"],
                    "reason": selected_runner["reason"],
                    "partial_bank_pips": selected_runner.get("partial_bank_pips", 0.0),
                    "runner_pips": selected_runner.get("runner_pips", 0.0),
                }
                if selected_runner.get("partial_bank_pips", 0.0):
                    partial_tp = selected_runner["partial_bank_pips"] / max(runner_cfg["partial_fraction"], 1e-9)
                    runner_cfg = {
                        "partial_tp": partial_tp,
                        "partial_fraction": runner_cfg["partial_fraction"],
                    }
            else:
                runner_sim = simulate_runner_trade(
                    row,
                    distance,
                    partial_tp=runner_cfg["partial_tp"],
                    partial_fraction=runner_cfg["partial_fraction"],
                )
            pretrade_rows.append(
                {
                    "cluster_id": row["cluster_id"],
                    "distance": distance,
                    "direction": row["direction"],
                    "timestamp": row["timestamp_start"],
                    "session": row["session"],
                    "weekday": row["weekday"],
                    "pre_range_pips": row["pre_range_pips"],
                    "pre_trend_pips": row["pre_trend_pips"],
                    "pre_volatility": row["pre_volatility"],
                    "pre_speed": row["pre_speed"],
                    "pre_efficiency": row["pre_efficiency"],
                    "minutes_from_cluster_start": row["minutes_from_cluster_start"],
                    "minutes_to_cluster_end": row["minutes_to_cluster_end"],
                    "cluster_progress": row["cluster_progress"],
                }
            )
            outcome_rows.append(
                {
                    "cluster_id": row["cluster_id"],
                    "distance": distance,
                    "direction": row["direction"],
                    "timestamp": row["timestamp_start"],
                    "future_mfe": row["future_mfe"],
                    "future_mae": row["future_mae"],
                    "time_to_target": row["time_to_target"],
                    "harvester_profit": harvester_sim["pips"],
                    "harvester_reason": harvester_sim["reason"],
                    "runner_extension": row["runner_extension"],
                    "runner_static_profit": runner_sim["pips"],
                    "runner_static_reason": runner_sim["reason"],
                    "runner_partial_tp": runner_cfg["partial_tp"],
                    "runner_partial_fraction": runner_cfg["partial_fraction"],
                    "stop_hit": harvester_sim["reason"] == "SL_HIT",
                }
            )
        for direction in ("LONG", "SHORT"):
            direction_rows = [r for r in state_rows if r["direction"] == direction]
            harvester = optimize_and_summarize(direction_rows, distance_clusters, "harvester", direction, distance)
            runner_static = optimize_and_summarize(direction_rows, distance_clusters, "runner_static", direction, distance)
            summaries.extend([harvester, runner_static])
            write_json(f"{direction.lower()}_{distance:g}_harvester.json", harvester)
            write_json(f"{direction.lower()}_{distance:g}_runner_static.json", runner_static)
    write_csv(
        "entry_pretrade_states.csv",
        pretrade_rows,
        ["cluster_id", "distance", "direction", "timestamp", "session", "weekday", "pre_range_pips", "pre_trend_pips", "pre_volatility", "pre_speed", "pre_efficiency", "minutes_from_cluster_start", "minutes_to_cluster_end", "cluster_progress"],
    )
    write_csv(
        "entry_outcomes.csv",
        outcome_rows,
        [
            "cluster_id",
            "distance",
            "direction",
            "timestamp",
            "future_mfe",
            "future_mae",
            "time_to_target",
            "harvester_profit",
            "harvester_reason",
            "runner_extension",
            "runner_static_profit",
            "runner_static_reason",
            "runner_partial_tp",
            "runner_partial_fraction",
            "stop_hit",
        ],
    )
    write_json("entry_mode_distance_sweep_summary.json", {"results": summaries})


if __name__ == "__main__":
    main()
