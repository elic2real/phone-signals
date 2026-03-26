#!/usr/bin/env python3
from __future__ import annotations

import ast
import csv
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, List


ROOT = Path(".")
PIP = 0.0001
TARGET_PIPS = 2.5
STOP_PIPS = 2.5


def load_csv(path: Path) -> List[Dict[str, Any]]:
    with path.open() as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2))


def parse_ts(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


def pnl(direction: str, start: float, px: float) -> float:
    return ((px - start) / PIP) if direction == "LONG" else ((start - px) / PIP)


def dataset_lock() -> Dict[str, Any]:
    p = ROOT / "london_session_data/pair=EUR_USD/year=2024/month=01/part-000.parquet"
    digest = hashlib.sha256(p.read_bytes()).hexdigest()
    data_audit = json.loads((ROOT / "data_audit_outputs/data_source_audit.json").read_text())
    return {
        "pair": "EUR_USD",
        "session": "london",
        "weekday": "monday",
        "start_date": data_audit["first_timestamp"][:10],
        "end_date": data_audit["last_timestamp"][:10],
        "row_count": data_audit["row_count"],
        "hash": digest,
    }


def emit_clusters_alias() -> None:
    src = load_csv(ROOT / "opportunity_clusters.csv")
    rows = []
    for row in src:
        rows.append(
            {
                "cluster_id": row["cluster_id"],
                "cluster_start": row["cluster_start"],
                "cluster_end": row["cluster_end"],
                "cluster_direction": row["direction"],
                "cluster_MFE": row["cluster_mfe_pips"],
                "cluster_MAE": row["cluster_mae_pips"],
                "cluster_duration": int((parse_ts(row["cluster_end"]) - parse_ts(row["cluster_start"])).total_seconds() / 60),
                "pair": row["pair"],
                "session": row["session"],
                "weekday": row["weekday"],
            }
        )
    write_csv(
        ROOT / "clusters.csv",
        rows,
        ["cluster_id", "cluster_start", "cluster_end", "cluster_direction", "cluster_MFE", "cluster_MAE", "cluster_duration", "pair", "session", "weekday"],
    )


def build_entry_states_and_labels() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    opps = load_csv(ROOT / "phase1_correct_outputs/opportunities_dataset.csv")
    labels = {
        (r["timestamp_start"], r["direction"]): r["zone_label"]
        for r in load_csv(ROOT / "opportunity_zones_labeled.csv")
    }
    clusters = load_csv(ROOT / "opportunity_clusters.csv")
    state_rows: List[Dict[str, Any]] = []
    label_rows: List[Dict[str, Any]] = []
    opp_by_key = {(r["timestamp_start"], r["direction"]): r for r in opps}
    triggered_rows: List[Dict[str, Any]] = []

    for cluster in clusters:
        direction = cluster["direction"]
        for ts in cluster["member_timestamps"].split("|"):
            opp = opp_by_key[(ts, direction)]
            future_mfe = float(opp["max_mfe_pips"])
            future_mae = float(opp["max_mae_pips"])
            valid = future_mfe >= TARGET_PIPS and future_mae <= STOP_PIPS
            row = {
                "cluster_id": cluster["cluster_id"],
                "timestamp": ts,
                "direction": direction,
                "future_mfe": future_mfe,
                "future_mae": future_mae,
                "valid_entry": int(valid),
                "speed": float(opp["speed"]),
                "efficiency": float(opp["efficiency"]),
                "extension": float(opp["extension"]),
            }
            state_rows.append(row)

            label = labels[(ts, direction)]
            # Use the locked stage-4 label as the teacher so the entry problem
            # is not trivialized by "all discovered opportunity states are GOOD".
            entry_label = label
            label_rows.append(
                {
                    "cluster_id": cluster["cluster_id"],
                    "timestamp": ts,
                    "direction": direction,
                    "future_mfe": future_mfe,
                    "future_mae": future_mae,
                    "entry_label": entry_label,
                    "zone_label": label,
                }
            )
    write_csv(
        ROOT / "entry_states.csv",
        state_rows,
        ["cluster_id", "timestamp", "direction", "future_mfe", "future_mae", "valid_entry", "speed", "efficiency", "extension"],
    )
    write_csv(
        ROOT / "entry_labels.csv",
        label_rows,
        ["cluster_id", "timestamp", "direction", "future_mfe", "future_mae", "entry_label", "zone_label"],
    )
    return state_rows, label_rows, triggered_rows


def optimize_entry(state_rows: List[Dict[str, Any]], label_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    labels = {(r["timestamp"], r["direction"]): r["entry_label"] for r in label_rows}
    candidates = sorted({round(float(r["speed"]), 6) for r in state_rows})
    best = None
    best_any = None
    best_triggered: List[Dict[str, Any]] = []
    best_any_triggered: List[Dict[str, Any]] = []
    cluster_best_label = {}
    cluster_state_counts = Counter()
    for row in sorted(label_rows, key=lambda r: (r["cluster_id"], r["timestamp"])):
        cluster_state_counts[row["cluster_id"]] += 1
        cluster_best_label.setdefault(row["cluster_id"], row["entry_label"])
    good_total = sum(1 for v in cluster_best_label.values() if v == "GOOD")
    bad_total = sum(1 for v in cluster_best_label.values() if v == "BAD")
    noise_total = sum(1 for v in cluster_best_label.values() if v == "NOISE")
    for threshold in candidates:
        raw_triggered = [r for r in state_rows if float(r["speed"]) >= threshold and int(r["valid_entry"]) == 1]
        # Enforce executable cluster-level entries: first triggered timestamp wins.
        by_cluster: Dict[str, Dict[str, Any]] = {}
        for row in sorted(raw_triggered, key=lambda r: (r["cluster_id"], r["timestamp"])):
            by_cluster.setdefault(row["cluster_id"], row)
        triggered = list(by_cluster.values())
        if not triggered:
            continue
        good_hit = sum(1 for r in triggered if cluster_best_label.get(r["cluster_id"]) == "GOOD")
        bad_hit = sum(1 for r in triggered if cluster_best_label.get(r["cluster_id"]) == "BAD")
        noise_hit = sum(1 for r in triggered if cluster_best_label.get(r["cluster_id"]) == "NOISE")
        good_capture = good_hit / good_total if good_total else 0.0
        bad_trigger = bad_hit / bad_total if bad_total else 0.0
        score = good_capture - 1.5 * bad_trigger
        candidate_summary = {
            "threshold": threshold,
            "good_capture": good_capture,
            "bad_trigger": bad_trigger,
            "noise_trigger": noise_hit / max(1, noise_total),
            "trade_count": len(triggered),
            "score": score,
        }
        if best_any is None or score > best_any["score"]:
            best_any = candidate_summary
            best_any_triggered = triggered
        if bad_trigger <= 0.15 and (best is None or score > best["score"]):
            best = candidate_summary
            best_triggered = triggered
    constrained = best is not None
    if best is None:
        best = best_any
        best_triggered = best_any_triggered
    assert best is not None
    out = {
        "pair": "EUR_USD",
        "session": "london",
        "weekday": "monday",
        "config": {
            "confirm_disp_atr": best["threshold"],
            "confirm_m1_closes": 1,
            "confirm_sec": 60,
            "base_max_dist_atr": 1.0,
            "dist_vel_k": 1.0,
            "logic": "speed_threshold_on_valid_entry_states",
        },
        "objective": {
            "good_capture": best["good_capture"],
            "bad_trigger": best["bad_trigger"],
            "noise_trigger": best["noise_trigger"],
            "trade_count": best["trade_count"],
            "score": best["score"],
            "constraint_satisfied": constrained,
            "bad_trigger_constraint": 0.15,
        },
    }
    write_json(ROOT / "entry_optimal_config.json", out)
    return {"summary": out, "triggered": best_triggered}


def derive_aee_rules(triggered: List[Dict[str, Any]]) -> Dict[str, Any]:
    opps = {(r["timestamp_start"], r["direction"]): r for r in load_csv(ROOT / "phase1_correct_outputs/opportunities_dataset.csv")}
    givebacks: List[float] = []
    profitable_peaks: List[float] = []
    bars_since_peak: List[int] = []
    extraction = []
    for row in triggered:
        opp = opps[(row["timestamp"], row["direction"])]
        path = list(ast.literal_eval(opp["price_path"]))
        start = float(opp["price_start"])
        profits = [pnl(opp["direction"], start, px) for px in path]
        peak = profits[0]
        peak_idx = 0
        exit_idx = len(profits) - 1
        for i, val in enumerate(profits):
            if val > peak:
                peak = val
                peak_idx = i
            giveback = peak - val
            if peak >= TARGET_PIPS and giveback >= 0.6:
                exit_idx = i
                givebacks.append(giveback)
                profitable_peaks.append(peak)
                bars_since_peak.append(i - peak_idx)
                break
        extraction.append(profits[exit_idx] / max(peak, TARGET_PIPS))
    rules = {
        "pair": "EUR_USD",
        "session": "london",
        "weekday": "monday",
        "rules": {
            "harvest": {
                "if_profit_ge_pips": round(median(profitable_peaks), 4) if profitable_peaks else TARGET_PIPS,
                "if_giveback_ge_pips": round(median(givebacks), 4) if givebacks else 0.6,
                "action": "HARVEST",
            },
            "decay": {
                "if_bars_since_peak_ge": int(round(median(bars_since_peak))) if bars_since_peak else 2,
                "action": "DECAY",
            },
            "panic": {
                "if_profit_le_pips": -STOP_PIPS,
                "action": "PANIC",
            },
            "extend": {
                "if_profit_ge_pips": TARGET_PIPS,
                "if_giveback_lt_pips": 0.6,
                "action": "EXTEND",
            },
        },
        "derived_metrics": {
            "avg_extraction_efficiency": mean(extraction) if extraction else 0.0,
            "triggered_trade_count": len(triggered),
        },
    }
    write_json(ROOT / "aee_optimal_rules.json", rules)
    return rules


def replay_ceiling(triggered: List[Dict[str, Any]], aee_rules: Dict[str, Any]) -> Dict[str, Any]:
    opps = {(r["timestamp_start"], r["direction"]): r for r in load_csv(ROOT / "phase1_correct_outputs/opportunities_dataset.csv")}
    total_pips = []
    available_mfe = []
    for row in triggered:
        opp = opps[(row["timestamp"], row["direction"])]
        path = list(ast.literal_eval(opp["price_path"]))
        start = float(opp["price_start"])
        profits = [pnl(opp["direction"], start, px) for px in path]
        peak = profits[0]
        exit_profit = profits[-1]
        for val in profits:
            peak = max(peak, val)
            if peak >= aee_rules["rules"]["harvest"]["if_profit_ge_pips"] and (peak - val) >= aee_rules["rules"]["harvest"]["if_giveback_ge_pips"]:
                exit_profit = val
                break
            if val <= aee_rules["rules"]["panic"]["if_profit_le_pips"]:
                exit_profit = val
                break
        total_pips.append(exit_profit)
        available_mfe.append(max(profits))
    total_hours = 540 / 60.0
    label_rows = load_csv(ROOT / "entry_labels.csv")
    cluster_best_label = {}
    for row in sorted(label_rows, key=lambda r: (r["cluster_id"], r["timestamp"])):
        cluster_best_label.setdefault(row["cluster_id"], row["entry_label"])
    good_clusters = sum(1 for v in cluster_best_label.values() if v == "GOOD")
    report = {
        "pair": "EUR_USD",
        "session": "london",
        "weekday": "monday",
        "total_trades": len(triggered),
        "capture_rate": sum(1 for r in triggered if cluster_best_label.get(r["cluster_id"]) == "GOOD") / max(1, good_clusters),
        "extraction_rate": (mean(total_pips) / mean(available_mfe)) if total_pips and available_mfe else 0.0,
        "pip_per_hour": sum(total_pips) / total_hours if total_hours else 0.0,
        "drawdown_proxy_pips": min(total_pips) if total_pips else 0.0,
        "runner_harvester_enabled": True,
        "risk_percent": 2.0,
    }
    write_json(ROOT / "ceiling_replay_report.json", report)
    return report


def csv_count(path: Path, label: str | None = None) -> int:
    rows = load_csv(path)
    if label is None:
        return len(rows)
    return sum(1 for r in rows if r.get("entry_label") == label)


def main() -> None:
    write_json(ROOT / "dataset_lock.json", dataset_lock())
    emit_clusters_alias()
    state_rows, label_rows, _ = build_entry_states_and_labels()
    entry = optimize_entry(state_rows, label_rows)
    aee = derive_aee_rules(entry["triggered"])
    replay_ceiling(entry["triggered"], aee)


if __name__ == "__main__":
    main()
