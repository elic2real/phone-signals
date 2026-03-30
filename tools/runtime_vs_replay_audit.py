import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_RUNTIME = Path(r"c:\Users\mawil\phone_signals\phone-signals-publish-clean\logs\trades.jsonl")
DEFAULT_REPLAY = Path(r"c:\Users\mawil\phone_signals\phone-signals-publish-clean\control\aee_widened_validation_report.json")


def _to_float(value: Any) -> Optional[float]:
    try:
        v = float(value)
    except Exception:
        return None
    return v if math.isfinite(v) else None


def _extract_ts(obj: Dict[str, Any]) -> Optional[str]:
    for key in ("ts_utc", "ts", "timestamp", "time"):
        value = obj.get(key)
        if value is not None and str(value).strip() != "":
            return str(value)
    return None


def _extract_first(obj: Dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in obj and obj.get(key) is not None:
            return obj.get(key)
    return None


@dataclass
class RuntimeTrade:
    trade_id: str
    pair: Optional[str] = None
    entry_time: Optional[str] = None
    entry_price: Optional[float] = None
    direction: Optional[str] = None
    entry_reason: Optional[str] = None
    entry_id: Optional[str] = None
    units: Optional[float] = None
    broker_trade_id: Optional[str] = None
    exit_time: Optional[str] = None
    exit_reason: Optional[str] = None
    pnl: Optional[float] = None


@dataclass
class ReplayTrade:
    trade_id: str
    expected_pnl: Optional[float]
    baseline_pnl: Optional[float]
    expected_exit: Optional[str]


def load_runtime_trades(runtime_path: Path) -> Dict[str, RuntimeTrade]:
    if not runtime_path.exists():
        raise FileNotFoundError(f"Runtime file not found: {runtime_path}")

    trades: Dict[str, RuntimeTrade] = {}
    broker_to_db: Dict[str, str] = {}

    with runtime_path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue

            kind = str(obj.get("kind", "") or "").upper()

            if kind == "ENTRY_RESULT" and str(obj.get("result", "") or "").upper() == "FILLED":
                db_id = str(obj.get("trade_id", "") or "").strip()
                broker_id = str(obj.get("broker_trade_id", "") or "").strip()
                if db_id and broker_id:
                    broker_to_db[broker_id] = db_id
                continue

            if kind == "ORDER_FILLED":
                broker_tid = str(obj.get("trade_id", "") or "").strip()
                trade_id = broker_to_db.get(broker_tid, broker_tid)
                if not trade_id:
                    continue
                trade = trades.get(trade_id)
                if trade is None:
                    trade = RuntimeTrade(trade_id=trade_id)
                    trades[trade_id] = trade

                trade.pair = trade.pair or obj.get("pair")
                trade.entry_time = trade.entry_time or _extract_ts(obj)
                trade.entry_price = trade.entry_price or _to_float(obj.get("price"))
                trade.direction = trade.direction or obj.get("dir") or obj.get("side")
                trade.entry_reason = trade.entry_reason or obj.get("reason")
                trade.entry_id = trade.entry_id or obj.get("entry_id")
                trade.units = trade.units or _to_float(obj.get("units"))
                trade.broker_trade_id = trade.broker_trade_id or broker_tid
                continue

            if kind in {"TRADE_CLOSE", "EXIT_RESULT", "EXIT_PNL_AUDIT", "NONLOCAL_CLOSE_DB_WRITE", "NONLOCAL_CLOSE_CONFIRM_APPLY"}:
                raw_tid = _extract_first(obj, ["trade_id", "db_trade_id", "id", "broker_trade_id"])
                if raw_tid is None:
                    continue
                raw_tid_s = str(raw_tid).strip()
                trade_id = broker_to_db.get(raw_tid_s, raw_tid_s)
                if not trade_id:
                    continue
                trade = trades.get(trade_id)
                if trade is None:
                    trade = RuntimeTrade(trade_id=trade_id)
                    trades[trade_id] = trade

                trade.exit_time = trade.exit_time or _extract_ts(obj)
                trade.exit_reason = trade.exit_reason or _extract_first(obj, ["reason", "reason_code", "exit_reason", "result"])
                if trade.pnl is None:
                    for key in ("pnl_pips", "pips", "pnl", "realized_pips", "realized_pnl_pips"):
                        value = _to_float(obj.get(key))
                        if value is not None:
                            trade.pnl = value
                            break

    return trades


def _iter_replay_rows(data: Any) -> Iterable[Dict[str, Any]]:
    if isinstance(data, dict):
        if isinstance(data.get("per_trade"), list):
            for row in data["per_trade"]:
                if isinstance(row, dict):
                    yield row
            return
        for key in ("trades", "results", "rows"):
            if isinstance(data.get(key), list):
                for row in data[key]:
                    if isinstance(row, dict):
                        yield row
                return
    if isinstance(data, list):
        for row in data:
            if isinstance(row, dict):
                yield row


def load_replay_trades(replay_path: Path) -> Dict[str, ReplayTrade]:
    if not replay_path.exists():
        raise FileNotFoundError(f"Replay file not found: {replay_path}")

    with replay_path.open("r", encoding="utf-8", errors="replace") as fh:
        data = json.load(fh)

    replay: Dict[str, ReplayTrade] = {}
    for row in _iter_replay_rows(data):
        trade_id_raw = _extract_first(row, ["trade_id", "id", "entry_id", "runtime_trade_id", "db_trade_id"])
        if trade_id_raw is None:
            continue
        trade_id = str(trade_id_raw).strip()
        if not trade_id:
            continue

        expected = _to_float(_extract_first(row, ["winner_result_pips", "expected_pnl", "pnl_pips", "realized_pips", "result_pips"]))
        baseline = _to_float(_extract_first(row, ["baseline_1to1_pips", "baseline_pnl", "baseline_pips"]))
        expected_exit = _extract_first(row, ["exit_reason", "expected_exit", "reason", "expected_reason"])
        expected_exit = str(expected_exit) if expected_exit is not None else None

        replay[trade_id] = ReplayTrade(
            trade_id=trade_id,
            expected_pnl=expected,
            baseline_pnl=baseline,
            expected_exit=expected_exit,
        )

    return replay


def compare(runtime: Dict[str, RuntimeTrade], replay: Dict[str, ReplayTrade]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for trade_id, rt in runtime.items():
        rp = replay.get(trade_id)
        if rp is None:
            continue
        if rt.pnl is None:
            continue

        expected = rp.expected_pnl if rp.expected_pnl is not None else 0.0
        delta = rt.pnl - expected
        rows.append(
            {
                "trade_id": trade_id,
                "pair": rt.pair,
                "runtime_pnl": rt.pnl,
                "expected_pnl": rp.expected_pnl,
                "baseline_pnl": rp.baseline_pnl,
                "delta_pips": delta,
                "runtime_exit": rt.exit_reason,
                "expected_exit": rp.expected_exit,
                "entry_time": rt.entry_time,
                "exit_time": rt.exit_time,
                "entry_reason": rt.entry_reason,
                "entry_id": rt.entry_id,
            }
        )

    rows.sort(key=lambda x: abs(float(x["delta_pips"])), reverse=True)
    return rows


def summarize(rows: List[Dict[str, Any]], mismatch_threshold: float) -> Dict[str, Any]:
    if not rows:
        return {
            "trades_compared": 0,
            "total_delta_pips": 0.0,
            "avg_delta_pips": 0.0,
            "mismatch_threshold": mismatch_threshold,
            "large_mismatches": 0,
            "sample_mismatches": [],
        }

    total = sum(float(r["delta_pips"]) for r in rows)
    avg = total / len(rows)
    mismatches = [r for r in rows if abs(float(r["delta_pips"])) > mismatch_threshold]

    return {
        "trades_compared": len(rows),
        "total_delta_pips": round(total, 6),
        "avg_delta_pips": round(avg, 6),
        "mismatch_threshold": mismatch_threshold,
        "large_mismatches": len(mismatches),
        "sample_mismatches": mismatches[:10],
    }


def discover_json_candidates(base: Path) -> List[str]:
    if not base.exists():
        return []
    out: List[str] = []
    for p in base.rglob("*.json"):
        name = p.name.lower()
        if any(token in name for token in ("replay", "validation", "report", "aee")):
            out.append(str(p))
            if len(out) >= 20:
                break
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit runtime trade outcomes vs replay outcomes")
    parser.add_argument("--runtime", type=Path, default=DEFAULT_RUNTIME)
    parser.add_argument("--replay", type=Path, default=DEFAULT_REPLAY)
    parser.add_argument("--threshold", type=float, default=2.0)
    parser.add_argument("--out", type=Path, default=None, help="Optional path to write full comparison rows as JSON")
    args = parser.parse_args()

    try:
        runtime = load_runtime_trades(args.runtime)
    except Exception as exc:
        print(f"Runtime load error: {exc}")
        return 1

    try:
        replay = load_replay_trades(args.replay)
    except Exception as exc:
        print(f"Replay load error: {exc}")
        base = args.replay.parent if args.replay.parent.exists() else Path(r"c:\Users\mawil\phone_signals\phone-signals-publish-clean")
        candidates = discover_json_candidates(base)
        if candidates:
            print("Replay candidates:")
            for c in candidates:
                print(f"- {c}")
        else:
            print("No obvious replay candidates found.")
        return 2

    rows = compare(runtime, replay)
    summary = summarize(rows, args.threshold)

    print("----- SUMMARY -----")
    print(json.dumps(summary, indent=2))

    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps(rows, indent=2), encoding="utf-8")
        print(f"Wrote rows to: {args.out}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
