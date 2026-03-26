from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

from state_key import compute_quarter


ROOT = Path(__file__).resolve().parent
OUT_JSON = ROOT / "compiled_ceiling_quarters.json"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def aggregate_trades(label: str, trades: list[dict]) -> dict:
    out: dict[str, dict] = {}
    by_quarter: dict[str, list[dict]] = defaultdict(list)
    for trade in trades:
        ts = trade.get("timestamp_start")
        quarter = compute_quarter(ts, "LONDON")
        by_quarter[quarter].append(trade)

    for quarter in ["Q1", "Q2", "Q3", "Q4"]:
        qtrades = by_quarter.get(quarter, [])
        counts = Counter(trade.get("exit_reason", "UNKNOWN") for trade in qtrades)
        static_pips = sum(float(trade.get("static_pips", 0.0)) for trade in qtrades)
        aee_pips = sum(float(trade.get("aee_pips", 0.0)) for trade in qtrades)
        static_r = sum(float(trade.get("static_R", 0.0)) for trade in qtrades)
        aee_r = sum(float(trade.get("aee_R", 0.0)) for trade in qtrades)
        n = len(qtrades)
        out[quarter] = {
            "label": label,
            "quarter": quarter,
            "total_trades": n,
            "HOLD_count": counts.get("HOLD", 0),
            "HARVEST_count": counts.get("HARVEST", 0),
            "PANIC_count": counts.get("PANIC", 0),
            "DECAY_EXIT_count": counts.get("DECAY_EXIT", 0),
            "DO_NOT_ENTER_count": counts.get("DO_NOT_ENTER", 0),
            "avg_static_pips": (static_pips / n) if n else 0.0,
            "avg_aee_pips": (aee_pips / n) if n else 0.0,
            "avg_static_R": (static_r / n) if n else 0.0,
            "avg_aee_R": (aee_r / n) if n else 0.0,
            "static_pips_per_hour": static_pips / 2.0,
            "aee_pips_per_hour": aee_pips / 2.0,
            "delta_pips_per_hour": (aee_pips - static_pips) / 2.0,
            "delta_avg_R": ((aee_r - static_r) / n) if n else 0.0,
        }
    return out


def main() -> None:
    long_obj = load_json(ROOT / "aee_state_machine_replay_long.json")
    short_obj = load_json(ROOT / "aee_state_machine_replay_short.json")
    combined_obj = load_json(ROOT / "aee_state_machine_replay_combined.json")
    dataset_lock = load_json(ROOT / "dataset_lock.json")

    report = {
        "node": {
            "pair": dataset_lock.get("pair"),
            "weekday": dataset_lock.get("weekday"),
            "session": str(dataset_lock.get("session", "")).upper(),
        },
        "long": aggregate_trades("LONG", long_obj.get("trades", [])),
        "short": aggregate_trades("SHORT", short_obj.get("trades", [])),
        "combined": aggregate_trades("COMBINED", combined_obj.get("trades", [])),
    }
    OUT_JSON.write_text(json.dumps(report, indent=2))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
