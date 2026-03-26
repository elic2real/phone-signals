from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List

from state_key import compute_quarter


ROOT = Path(__file__).resolve().parent


def load_json(name: str) -> Dict[str, Any]:
    return json.loads((ROOT / name).read_text())


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def load_session_trades() -> List[Dict[str, Any]]:
    combined = load_json("aee_state_machine_replay_combined.json")
    trades = []
    for idx, trade in enumerate(combined.get("trades", []), start=1):
        trades.append(
            {
                "trade_id": f"T{idx:04d}",
                "cluster_id": trade["cluster_id"],
                "entry_time": trade["timestamp_start"],
                "exit_time": trade["timestamp_start"],  # exit timestamp not stored in replay artifact
                "direction": trade["direction"],
                "distance": trade["distance"],
                "entry_mode": trade["entry_mode"],
                "static_pips": float(trade["static_pips"]),
                "aee_pips": float(trade["aee_pips"]),
                "static_R": float(trade["static_R"]),
                "aee_R": float(trade["aee_R"]),
                "static_reason": trade["static_reason"],
                "aee_reason": trade["exit_reason"],
            }
        )
    return trades


def main() -> None:
    session_trades = load_session_trades()
    quarter_rows = []
    for row in session_trades:
        quarter_rows.append({**row, "quarter": compute_quarter(row["entry_time"], "LONDON")})

    write_csv(
        ROOT / "session_trades.csv",
        session_trades,
        [
            "trade_id",
            "cluster_id",
            "entry_time",
            "exit_time",
            "direction",
            "distance",
            "entry_mode",
            "static_pips",
            "aee_pips",
            "static_R",
            "aee_R",
            "static_reason",
            "aee_reason",
        ],
    )
    write_csv(
        ROOT / "quarter_bucketed_trades.csv",
        quarter_rows,
        [
            "trade_id",
            "cluster_id",
            "entry_time",
            "exit_time",
            "quarter",
            "direction",
            "distance",
            "entry_mode",
            "static_pips",
            "aee_pips",
            "static_R",
            "aee_R",
            "static_reason",
            "aee_reason",
        ],
    )

    quarter_totals: Dict[str, Dict[str, float]] = {}
    for quarter in ("Q1", "Q2", "Q3", "Q4"):
        qrows = [r for r in quarter_rows if r["quarter"] == quarter]
        quarter_totals[quarter] = {
            "trade_count": len(qrows),
            "static_pips": sum(r["static_pips"] for r in qrows),
            "aee_pips": sum(r["aee_pips"] for r in qrows),
            "static_R": sum(r["static_R"] for r in qrows),
            "aee_R": sum(r["aee_R"] for r in qrows),
        }

    session_totals = {
        "trade_count": len(session_trades),
        "static_pips": sum(r["static_pips"] for r in session_trades),
        "aee_pips": sum(r["aee_pips"] for r in session_trades),
        "static_R": sum(r["static_R"] for r in session_trades),
        "aee_R": sum(r["aee_R"] for r in session_trades),
    }
    summed_quarters = {
        "trade_count": sum(v["trade_count"] for v in quarter_totals.values()),
        "static_pips": sum(v["static_pips"] for v in quarter_totals.values()),
        "aee_pips": sum(v["aee_pips"] for v in quarter_totals.values()),
        "static_R": sum(v["static_R"] for v in quarter_totals.values()),
        "aee_R": sum(v["aee_R"] for v in quarter_totals.values()),
    }
    reconciliation = {
        "session_totals": session_totals,
        "quarter_totals": quarter_totals,
        "summed_quarters": summed_quarters,
        "differences": {
            k: session_totals[k] - summed_quarters[k] for k in session_totals.keys()
        },
        "verdict": "PASS" if all(abs(session_totals[k] - summed_quarters[k]) < 1e-9 for k in session_totals.keys()) else "RECONCILIATION_MISMATCH",
    }
    (ROOT / "quarter_reconciliation_report.json").write_text(json.dumps(reconciliation, indent=2))
    print(json.dumps(reconciliation, indent=2))


if __name__ == "__main__":
    main()
