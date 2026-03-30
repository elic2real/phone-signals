import argparse
import ast
import csv
import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


DEFAULT_TRADES = Path(r"c:\Users\mawil\phone_signals\phone-signals-publish-clean\logs\trades.jsonl")
DEFAULT_RUNTIME_LOG = Path(r"c:\Users\mawil\phone_signals\phone-signals-publish-clean\logs\runtime.log")
DEFAULT_OUT_DIR = Path(r"c:\Users\mawil\phone_signals\phone-signals\control\runtime_replay_bridge")

LOG_LINE_RE = re.compile(r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})\s+-\s+\w+\s+-\s+(?P<msg>.*)$")


def parse_iso_ts(value: Any) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None


def parse_log_ts(value: str) -> Optional[float]:
    try:
        dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S,%f")
        return dt.replace(tzinfo=timezone.utc).timestamp()
    except Exception:
        return None


def ffloat(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


@dataclass
class RuntimeTrade:
    trade_id: str
    pair: str
    direction: str
    entry_time: float
    entry_ts_raw: str
    entry_reason: str
    entry_group_id: Optional[str]
    setup: Optional[str]
    leg_type: Optional[str]
    broker_trade_id: Optional[str]
    units: Optional[float]
    exit_time: Optional[float]
    exit_ts_raw: Optional[str]
    exit_reason: Optional[str]
    pnl_pips: Optional[float]


@dataclass
class TickRow:
    instrument: str
    ts: float
    bid: float
    ask: float


def _event_kind(obj: Dict[str, Any]) -> str:
    return str(obj.get("kind", "") or "").upper()


def _derive_direction(obj: Dict[str, Any]) -> str:
    direction = str(obj.get("dir", "") or obj.get("direction", "")).upper().strip()
    if direction in {"LONG", "SHORT"}:
        return direction
    units = ffloat(obj.get("units"))
    if units is not None:
        return "LONG" if units > 0 else "SHORT"
    return "LONG"


def load_runtime_trades(trades_path: Path, include_reasons: Optional[set[str]] = None, limit: Optional[int] = None) -> List[RuntimeTrade]:
    if not trades_path.exists():
        raise FileNotFoundError(f"Trades file not found: {trades_path}")

    # broker trade id -> db trade id
    broker_to_db: Dict[str, str] = {}
    db_meta: Dict[str, Dict[str, Any]] = {}
    entries: Dict[str, Dict[str, Any]] = {}

    with trades_path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue

            kind = _event_kind(obj)

            if kind == "ENTRY_RESULT" and str(obj.get("result", "") or "").upper() == "FILLED":
                db_id = str(obj.get("trade_id", "") or "").strip()
                br_id = str(obj.get("broker_trade_id", "") or "").strip()
                if db_id:
                    db_meta.setdefault(db_id, {})
                    db_meta[db_id].update({
                        "setup": obj.get("setup"),
                        "leg_type": obj.get("leg_type"),
                        "entry_group_id": obj.get("entry_group_id"),
                    })
                if db_id and br_id:
                    broker_to_db[br_id] = db_id
                continue

            if kind == "ORDER_FILLED":
                reason = str(obj.get("reason", "") or "")
                if include_reasons and reason not in include_reasons:
                    continue

                br_id = str(obj.get("trade_id", "") or "").strip()
                db_id = broker_to_db.get(br_id, br_id)
                if not db_id:
                    continue

                ts_raw = str(obj.get("ts_utc", "") or obj.get("ts", ""))
                ts = parse_iso_ts(ts_raw)
                if ts is None:
                    continue

                entries.setdefault(db_id, {})
                entries[db_id].update({
                    "trade_id": db_id,
                    "pair": str(obj.get("pair", "") or "").upper(),
                    "direction": _derive_direction(obj),
                    "entry_time": ts,
                    "entry_ts_raw": ts_raw,
                    "entry_reason": reason,
                    "entry_group_id": obj.get("entry_group_id"),
                    "broker_trade_id": br_id or None,
                    "units": ffloat(obj.get("units")),
                })
                continue

            if kind in {"EXIT_RESULT", "EXIT_PNL_AUDIT", "TRADE_CLOSE", "NONLOCAL_CLOSE_DB_WRITE", "NONLOCAL_CLOSE_CONFIRM_APPLY"}:
                raw_id = obj.get("trade_id") or obj.get("db_trade_id") or obj.get("broker_trade_id")
                if raw_id is None:
                    continue
                rid = str(raw_id).strip()
                db_id = broker_to_db.get(rid, rid)
                if not db_id:
                    continue

                ts_raw = str(obj.get("ts_utc", "") or obj.get("ts", "") or "")
                ts = parse_iso_ts(ts_raw) if ts_raw else None
                if db_id not in entries:
                    entries[db_id] = {"trade_id": db_id}

                # keep first seen close reason/time to avoid overwriting with post-confirm events
                if ts is not None and entries[db_id].get("exit_time") is None:
                    entries[db_id]["exit_time"] = ts
                    entries[db_id]["exit_ts_raw"] = ts_raw
                if entries[db_id].get("exit_reason") is None:
                    entries[db_id]["exit_reason"] = obj.get("reason") or obj.get("reason_code") or obj.get("result")
                if entries[db_id].get("pnl_pips") is None:
                    for k in ("pnl_pips", "pips", "realized_pips", "pnl"):
                        v = ffloat(obj.get(k))
                        if v is not None:
                            entries[db_id]["pnl_pips"] = v
                            break

    # merge db metadata
    out: List[RuntimeTrade] = []
    for trade_id, rec in entries.items():
        if not rec.get("pair") or rec.get("entry_time") is None:
            continue
        meta = db_meta.get(trade_id, {})
        rt = RuntimeTrade(
            trade_id=str(trade_id),
            pair=str(rec.get("pair", "")).upper(),
            direction=str(rec.get("direction", "LONG") or "LONG").upper(),
            entry_time=float(rec["entry_time"]),
            entry_ts_raw=str(rec.get("entry_ts_raw", "")),
            entry_reason=str(rec.get("entry_reason", "") or ""),
            entry_group_id=(str(rec.get("entry_group_id") or meta.get("entry_group_id") or "") or None),
            setup=(str(meta.get("setup") or "") or None),
            leg_type=(str(meta.get("leg_type") or "") or None),
            broker_trade_id=(str(rec.get("broker_trade_id") or "") or None),
            units=rec.get("units"),
            exit_time=rec.get("exit_time"),
            exit_ts_raw=(str(rec.get("exit_ts_raw") or "") or None),
            exit_reason=(str(rec.get("exit_reason") or "") or None),
            pnl_pips=rec.get("pnl_pips"),
        )
        out.append(rt)

    out.sort(key=lambda t: t.entry_time)
    if limit is not None and limit > 0:
        out = out[-limit:]
    return out


def iter_runtime_ticks(runtime_log_path: Path, allowed_pairs: Optional[set[str]] = None) -> Iterable[TickRow]:
    if not runtime_log_path.exists():
        raise FileNotFoundError(f"Runtime log not found: {runtime_log_path}")

    with runtime_log_path.open("r", encoding="utf-8", errors="replace") as fh:
        for line in fh:
            m = LOG_LINE_RE.match(line.rstrip("\n"))
            if not m:
                continue
            msg = m.group("msg")
            if "OANDA_PRICING_RESPONSE" not in msg and "PRICING_OANDA_DEBUG" not in msg:
                continue
            parts = msg.split("|", 1)
            if len(parts) != 2:
                continue
            payload_s = parts[1].strip()
            try:
                payload = ast.literal_eval(payload_s)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            pair = str(payload.get("pair", "") or payload.get("instrument", "") or "").upper().replace("/", "_")
            if not pair:
                continue
            if allowed_pairs and pair not in allowed_pairs:
                continue
            bid = ffloat(payload.get("bid"))
            ask = ffloat(payload.get("ask"))
            if bid is None or ask is None:
                continue
            ts = parse_iso_ts(payload.get("raw_time"))
            if ts is None:
                ts = ffloat(payload.get("timestamp"))
            if ts is None:
                ts = parse_log_ts(m.group("ts"))
            if ts is None:
                continue
            yield TickRow(instrument=pair, ts=ts, bid=bid, ask=ask)


def write_ticks_csv(path: Path, ticks: List[TickRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(["instrument", "ts", "bid", "ask"])
        for t in ticks:
            w.writerow([t.instrument, f"{t.ts:.6f}", f"{t.bid:.10f}", f"{t.ask:.10f}"])


def infer_speed_class(trade: RuntimeTrade) -> str:
    s = (trade.setup or "").upper()
    if "RUN" in s or "VOL_REIGNITE" in s or "INTENTIONAL_RUNNER" in s:
        return "SLOW"
    if "LIQUIDITY" in s or "FAILED_BREAKOUT" in s:
        return "FAST"
    return "MED"


def build_bridge(
    trades: List[RuntimeTrade],
    ticks: List[TickRow],
    out_dir: Path,
    before_sec: float,
    after_sec: float,
) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "replay_results").mkdir(parents=True, exist_ok=True)
    ticks_by_pair: Dict[str, List[TickRow]] = {}
    for t in ticks:
        ticks_by_pair.setdefault(t.instrument, []).append(t)

    for pair in ticks_by_pair:
        ticks_by_pair[pair].sort(key=lambda x: x.ts)

    manifest_rows: List[Dict[str, Any]] = []
    run_lines: List[str] = []

    for tr in trades:
        pair_ticks = ticks_by_pair.get(tr.pair, [])
        if not pair_ticks:
            continue

        start = tr.entry_time - max(0.0, before_sec)
        end = (tr.exit_time if tr.exit_time is not None else tr.entry_time + max(0.0, after_sec)) + max(0.0, after_sec)

        window = [tk for tk in pair_ticks if start <= tk.ts <= end]
        if len(window) < 2:
            continue

        base = f"{tr.pair}_{tr.trade_id}"
        tick_path = out_dir / "ticks" / f"{base}.csv"
        result_path = out_dir / "replay_results" / f"{base}.json"
        write_ticks_csv(tick_path, window)

        cmd = (
            f"python sim_harness.py --ticks \"{tick_path}\" --pair {tr.pair} "
            f"--direction {tr.direction} --speed-class {infer_speed_class(tr)} --out \"{result_path}\""
        )
        run_lines.append(cmd)

        manifest_rows.append(
            {
                "trade_id": tr.trade_id,
                "broker_trade_id": tr.broker_trade_id,
                "pair": tr.pair,
                "direction": tr.direction,
                "setup": tr.setup,
                "leg_type": tr.leg_type,
                "entry_time": tr.entry_ts_raw,
                "entry_reason": tr.entry_reason,
                "entry_group_id": tr.entry_group_id,
                "exit_time": tr.exit_ts_raw,
                "exit_reason": tr.exit_reason,
                "runtime_pnl_pips": tr.pnl_pips,
                "ticks_csv": str(tick_path),
                "tick_rows": len(window),
                "replay_out": str(result_path),
                "sim_command": cmd,
            }
        )

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "trade_count": len(manifest_rows),
        "rows": manifest_rows,
    }

    (out_dir / "manifest_runtime_replay_bridge.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    (out_dir / "run_replay_from_runtime.ps1").write_text("\n".join(run_lines) + ("\n" if run_lines else ""), encoding="utf-8")
    return manifest


def main() -> int:
    p = argparse.ArgumentParser(description="Build runtime->replay bridge artifacts from runtime logs")
    p.add_argument("--trades", type=Path, default=DEFAULT_TRADES, help="Path to trades.jsonl")
    p.add_argument("--runtime-log", type=Path, default=DEFAULT_RUNTIME_LOG, help="Path to runtime.log")
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, help="Output directory for bridge artifacts")
    p.add_argument("--entry-reasons", default="run_entry,main_entry", help="Comma-separated ORDER_FILLED reasons to include")
    p.add_argument("--before-sec", type=float, default=300.0, help="Tick window before entry")
    p.add_argument("--after-sec", type=float, default=120.0, help="Extra tick window after exit/entry")
    p.add_argument("--limit", type=int, default=30, help="Max newest trades to bridge")
    args = p.parse_args()

    include_reasons = {x.strip() for x in str(args.entry_reasons).split(",") if x.strip()}

    trades = load_runtime_trades(args.trades, include_reasons=include_reasons, limit=args.limit)
    if not trades:
        print("No runtime trades matched filters.")
        return 1

    pairs = {t.pair for t in trades}
    ticks = list(iter_runtime_ticks(args.runtime_log, allowed_pairs=pairs))
    if not ticks:
        print("No pricing ticks found in runtime log for selected pairs.")
        return 2

    manifest = build_bridge(
        trades=trades,
        ticks=ticks,
        out_dir=args.out_dir,
        before_sec=args.before_sec,
        after_sec=args.after_sec,
    )

    print(
        json.dumps(
            {
                "out_dir": str(args.out_dir),
                "trades_selected": len(trades),
                "ticks_loaded": len(ticks),
                "bridged_trades": manifest.get("trade_count", 0),
                "manifest": str(args.out_dir / "manifest_runtime_replay_bridge.json"),
                "runner_script": str(args.out_dir / "run_replay_from_runtime.ps1"),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
