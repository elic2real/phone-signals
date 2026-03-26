#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parent
IN_CSV = ROOT / "compiled_trade_type_truth_11_sessions" / "harvester_truth_table.csv"
OUT_DIR = ROOT / "compiled_harvester_entry_11_sessions"


def load_csv(path: Path) -> list[dict[str, str]]:
    with path.open() as f:
        return list(csv.DictReader(f))


def mean0(vals: list[float]) -> float:
    return sum(vals) / len(vals) if vals else 0.0


def pct(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    arr = sorted(values)
    idx = min(len(arr) - 1, max(0, int(round(q * (len(arr) - 1)))))
    return arr[idx]


def evaluate(rows: list[dict[str, str]], min_score: float, min_bias: float, max_cbr: float, require_quick: int) -> dict[str, float]:
    selected = []
    for r in rows:
        if float(r["speed_objective_score"]) < min_score:
            continue
        if float(r["speed_bias"]) < min_bias:
            continue
        if float(r["compression_breakout_ratio"]) > max_cbr:
            continue
        if require_quick and int(r["quick_tp"]) != 1:
            continue
        selected.append(r)
    tp = sum(1 for r in selected if float(r["static_pips"]) > 0)
    sl = sum(1 for r in selected if float(r["static_pips"]) < 0)
    to = sum(1 for r in selected if float(r["static_pips"]) == 0)
    total_pips = sum(float(r["static_pips"]) for r in selected)
    return {
        "trade_count": len(selected),
        "tp_hits": tp,
        "sl_hits": sl,
        "timeouts": to,
        "tp_hit_rate": tp / len(selected) if selected else 0.0,
        "avg_pips": total_pips / len(selected) if selected else 0.0,
        "pips_per_hour": total_pips / 88.0 if selected else 0.0,
        "good_capture": sum(1 for r in selected if r["outcome_label"] == "GOOD") / max(1, sum(1 for r in rows if r["outcome_label"] == "GOOD")),
        "bad_trigger": sum(1 for r in selected if r["outcome_label"] == "BAD") / max(1, sum(1 for r in rows if r["outcome_label"] == "BAD")),
        "noise_trigger": sum(1 for r in selected if r["outcome_label"] == "NOISE") / max(1, sum(1 for r in rows if r["outcome_label"] == "NOISE")),
    }


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    rows = load_csv(IN_CSV)
    by_class: dict[tuple[str, float], list[dict[str, str]]] = defaultdict(list)
    for r in rows:
        by_class[(r["direction_assumed"], float(r["target_distance"]))].append(r)

    report = {"summary": []}
    classes = {}
    for (direction, target), subset in sorted(by_class.items()):
        scores = [float(r["speed_objective_score"]) for r in subset]
        biases = [float(r["speed_bias"]) for r in subset]
        cbrs = [float(r["compression_breakout_ratio"]) for r in subset]
        best = None
        best_cfg = None
        for sq in (0.3, 0.4, 0.5, 0.6, 0.7):
            for bq in (0.2, 0.3, 0.4, 0.5):
                for cq in (0.5, 0.6, 0.7, 0.8):
                    for require_quick in (0, 1):
                        cfg = {
                            "min_speed_objective_score": round(pct(scores, sq), 6),
                            "min_speed_bias": round(pct(biases, bq), 6),
                            "max_compression_breakout_ratio": round(pct(cbrs, cq), 6),
                            "require_quick_tp": require_quick,
                        }
                        metrics = evaluate(
                            subset,
                            cfg["min_speed_objective_score"],
                            cfg["min_speed_bias"],
                            cfg["max_compression_breakout_ratio"],
                            cfg["require_quick_tp"],
                        )
                        score = (
                            metrics["pips_per_hour"],
                            metrics["avg_pips"],
                            metrics["tp_hit_rate"],
                            metrics["trade_count"],
                        )
                        if best is None or score > best:
                            best = score
                            best_cfg = {"direction": direction, "target_distance": target, "metrics": metrics, "config": cfg}
        key = f"{direction}_{target:g}"
        classes[key] = best_cfg
        report["summary"].append({"direction": direction, "target_distance": target, **best_cfg["metrics"], **best_cfg["config"]})

    (OUT_DIR / "harvester_entry_classes.json").write_text(json.dumps(classes, indent=2))
    (OUT_DIR / "harvester_entry_report.json").write_text(json.dumps(report, indent=2))
    with (OUT_DIR / "harvester_entry_summary.csv").open("w", newline="") as f:
        if report["summary"]:
            writer = csv.DictWriter(f, fieldnames=list(report["summary"][0].keys()))
            writer.writeheader()
            writer.writerows(report["summary"])
    print(json.dumps({"status": "PASS", "classes": len(classes)}, indent=2))


if __name__ == "__main__":
    main()
