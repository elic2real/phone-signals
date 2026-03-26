from __future__ import annotations

import glob
import json
from collections import Counter
from pathlib import Path
from typing import Any

from aee_live_doctrine import LiveDoctrineEngine

KIND_CANDIDATES = {"MANUAL_TEACHER", "TEACH_HEARTBEAT", "MANUAL_CLOSE"}
TARGET_CLASSES = ["HOLD", "PARTIAL", "TIGHTEN", "CLOSE"]


def _iter_log_files() -> list[str]:
    files = sorted(glob.glob("logs/trades.jsonl*"))
    files += sorted(glob.glob("logs/archive/trades*.jsonl"))
    return files


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _build_mode_hints(files: list[str]) -> dict[str, str]:
    hints: dict[str, str] = {}
    for file_path in files:
        with open(file_path, "r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    record = json.loads(line)
                except Exception:
                    continue
                if str(record.get("kind", "")) not in KIND_CANDIDATES:
                    continue
                snapshot = record.get("trade_state_snapshot") or {}
                trade_id = snapshot.get("trade_id")
                if trade_id is None:
                    continue
                leg_type = str(snapshot.get("leg_type") or record.get("leg_type") or "").upper()
                if leg_type in {"HARVESTER", "RUNNER"}:
                    hints[str(trade_id)] = leg_type
    return hints


def run_live_wiring_proof() -> dict[str, Any]:
    files = _iter_log_files()
    mode_hints = _build_mode_hints(files)
    engine = LiveDoctrineEngine()

    by_class: Counter[str] = Counter()
    by_kind: Counter[str] = Counter()
    by_mode: Counter[str] = Counter()
    examples: dict[str, dict[str, Any]] = {}

    rows_seen = 0
    rows_used = 0
    max_abs_pnl_atr = 0.0
    max_abs_mfe_atr = 0.0

    for file_path in files:
        with open(file_path, "r", encoding="utf-8") as handle:
            for line in handle:
                rows_seen += 1
                try:
                    record = json.loads(line)
                except Exception:
                    continue

                kind = str(record.get("kind", ""))
                if kind not in KIND_CANDIDATES:
                    continue

                snapshot = record.get("trade_state_snapshot") or {}
                trade_id = snapshot.get("trade_id")
                if trade_id is None:
                    continue

                rows_used += 1
                trade_key = str(trade_id)
                mode = mode_hints.get(trade_key) or (
                    "RUNNER" if bool(snapshot.get("runner_mode")) else "HARVESTER"
                )

                current_r = _safe_float(snapshot.get("pnl_atr"), 0.0)
                mfe_r = _safe_float(snapshot.get("mfe_atr"), 0.0)
                mae_r = _safe_float(snapshot.get("mae_atr"), 0.0)
                energy = _safe_float(snapshot.get("energy_ratio"), 0.0)
                force_close = kind == "MANUAL_CLOSE" or str(record.get("exit_reason", "")).upper() == "PANIC_EXIT"

                result = engine.update(
                    trade_key=trade_key,
                    mode=mode,
                    now_s=_safe_float(snapshot.get("time_in_trade_sec"), 0.0),
                    current_r=current_r,
                    mfe_r=mfe_r,
                    mae_r=mae_r,
                    energy=energy,
                    force_close=force_close,
                )

                action = str(result.get("action", "HOLD"))
                by_class[action] += 1
                by_kind[kind] += 1
                by_mode[mode] += 1

                max_abs_pnl_atr = max(max_abs_pnl_atr, abs(current_r))
                max_abs_mfe_atr = max(max_abs_mfe_atr, abs(mfe_r))

                if action not in examples:
                    examples[action] = {
                        "file": file_path,
                        "kind": kind,
                        "trade_id": trade_id,
                        "ts": record.get("ts"),
                        "mode": mode,
                        "snapshot": {
                            "pnl_atr": current_r,
                            "mfe_atr": mfe_r,
                            "mae_atr": mae_r,
                            "energy_ratio": energy,
                            "time_in_trade_sec": _safe_float(snapshot.get("time_in_trade_sec"), 0.0),
                        },
                        "state": result.get("state", {}),
                    }

    missing_classes = [name for name in TARGET_CLASSES if by_class.get(name, 0) == 0]

    return {
        "proof_name": "aee_live_wiring_proof",
        "source": {
            "files_scanned": files,
            "rows_seen": rows_seen,
            "rows_used": rows_used,
            "event_kinds": sorted(KIND_CANDIDATES),
        },
        "coverage": {
            "action_counts": {name: int(by_class.get(name, 0)) for name in TARGET_CLASSES},
            "observed_action_set": sorted([name for name, count in by_class.items() if count > 0]),
            "missing_action_set": missing_classes,
            "kind_counts": dict(by_kind),
            "mode_counts": dict(by_mode),
        },
        "data_range": {
            "max_abs_pnl_atr": max_abs_pnl_atr,
            "max_abs_mfe_atr": max_abs_mfe_atr,
        },
        "examples": examples,
        "notes": [
            "This proof replays real open-trade snapshots through the same LiveDoctrineEngine used by phone_bot.py.",
            "TIGHTEN requires qualifying runner-path drawdown conditions; if absent here, no qualifying runner path was observed in scanned logs.",
        ],
    }


def main() -> None:
    report = run_live_wiring_proof()
    out_path = Path("aee_live_wiring_proof.json")
    out_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"wrote {out_path}")
    print(json.dumps(report["coverage"], indent=2))


if __name__ == "__main__":
    main()
