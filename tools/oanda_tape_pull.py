#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _month_start(dt: datetime) -> datetime:
    return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month(dt: datetime) -> datetime:
    if dt.month == 12:
        return dt.replace(year=dt.year + 1, month=1, day=1)
    return dt.replace(month=dt.month + 1, day=1)


@dataclass
class OandaClientLite:
    api_key: str
    account_id: str
    env: str = "practice"
    timeout: float = 20.0

    def __post_init__(self) -> None:
        host = "https://api-fxpractice.oanda.com" if self.env != "live" else "https://api-fxtrade.oanda.com"
        self.base_url = host.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept-Datetime-Format": "RFC3339",
            }
        )

    def candles(self, instrument: str, granularity: str, dt_from: datetime, dt_to: datetime, price: str = "M") -> list[dict]:
        # Critical OANDA quirk: do NOT send count when from/to are present.
        params = {
            "granularity": str(granularity).upper(),
            "price": price,
            "from": _iso_z(dt_from),
            "to": _iso_z(dt_to),
        }
        url = f"{self.base_url}/v3/instruments/{instrument}/candles"
        r = self.session.get(url, params=params, timeout=self.timeout)
        if r.status_code >= 400:
            raise RuntimeError(f"candles_http_{r.status_code}: {r.text[:300]}")
        obj = r.json()
        candles = obj.get("candles") if isinstance(obj, dict) else None
        return candles if isinstance(candles, list) else []


def _to_rows(candles: list[dict], pair: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for c in candles:
        if not isinstance(c, dict):
            continue
        mid = c.get("mid") or {}
        if not isinstance(mid, dict):
            continue
        try:
            o = float(mid.get("o"))
            h = float(mid.get("h"))
            l = float(mid.get("l"))
            cl = float(mid.get("c"))
        except Exception:
            continue
        ts = str(c.get("time", ""))
        vol = int(c.get("volume", 0) or 0)
        complete = bool(c.get("complete", False))
        rows.append(
            {
                "pair": pair,
                "timestamp": ts,
                "open": o,
                "high": h,
                "low": l,
                "close": cl,
                "volume": vol,
                "complete": complete,
            }
        )
    return rows


def _write_partition(root: Path, pair: str, year: int, month: int, rows: list[dict[str, Any]]) -> Path:
    out_dir = root / f"pair={pair}" / f"year={year}" / f"month={month:02d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "part-000.parquet"
    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp")
    df.to_parquet(out_file, index=False)
    return out_file


def _manifest(root: Path, meta: dict[str, Any]) -> dict[str, Any]:
    files = []
    rows_total = 0
    by_pair: dict[str, int] = {}
    for p in sorted(root.rglob("*.parquet")):
        rel = str(p.relative_to(root))
        files.append(rel)
        try:
            n = len(pd.read_parquet(p))
        except Exception:
            n = 0
        rows_total += n
        pair = rel.split("/")[0].replace("pair=", "")
        by_pair[pair] = by_pair.get(pair, 0) + n
    payload = {
        "root": str(root),
        "generated_utc": _iso_z(datetime.now(timezone.utc)),
        "rows_total": rows_total,
        "rows_by_pair": by_pair,
        "files": files,
        "count": len(files),
        **meta,
    }
    payload_str = json.dumps(payload, sort_keys=True)
    payload["manifest_hash"] = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()
    return payload


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs", default="EUR_USD,USD_CAD,AUD_USD,USD_JPY,AUD_JPY")
    ap.add_argument("--start-utc", required=True, help="ISO-8601 UTC, e.g. 2025-01-01T00:00:00Z")
    ap.add_argument("--end-utc", default="", help="ISO-8601 UTC; default now")
    ap.add_argument("--chunk-days", type=int, default=7)
    ap.add_argument("--out-root", default="data_tape")
    ap.add_argument("--granularity", default="M1")
    ap.add_argument("--price", default="M")
    ap.add_argument("--env", default=os.getenv("OANDA_ENV", "practice"))
    args = ap.parse_args()

    api_key = str(os.getenv("OANDA_API_KEY", "")).strip()
    account_id = str(os.getenv("OANDA_ACCOUNT_ID", "")).strip()
    if not api_key or not account_id:
        raise SystemExit("missing OANDA_API_KEY/OANDA_ACCOUNT_ID in env")

    def parse_iso(s: str) -> datetime:
        x = s.strip()
        if x.endswith("Z"):
            x = x[:-1] + "+00:00"
        dt = datetime.fromisoformat(x)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    start = parse_iso(args.start_utc)
    end = parse_iso(args.end_utc) if args.end_utc else datetime.now(timezone.utc)
    if end <= start:
        raise SystemExit("end must be after start")

    root = Path(args.out_root)
    root.mkdir(parents=True, exist_ok=True)
    client = OandaClientLite(api_key=api_key, account_id=account_id, env=args.env)
    pairs = [p.strip().upper() for p in args.pairs.split(",") if p.strip()]

    step = timedelta(days=max(1, int(args.chunk_days)))
    pulled_files: list[str] = []
    for pair in pairs:
        month_rows: dict[tuple[int, int], list[dict[str, Any]]] = {}
        cur = start
        while cur < end:
            nxt = min(end, cur + step)
            candles = client.candles(pair, args.granularity, cur, nxt, price=args.price)
            rows = _to_rows(candles, pair)
            for r in rows:
                dt = parse_iso(str(r["timestamp"]))
                key = (dt.year, dt.month)
                month_rows.setdefault(key, []).append(r)
            cur = nxt
            time.sleep(0.06)
        for (yy, mm), rows in sorted(month_rows.items()):
            out_file = _write_partition(root, pair, yy, mm, rows)
            pulled_files.append(str(out_file))
            print(f"WROTE {out_file} rows={len(rows)}")

    meta = {
        "source": "oanda",
        "granularity": args.granularity,
        "price": args.price,
        "start_utc": _iso_z(start),
        "end_utc": _iso_z(end),
        "chunk_days": int(args.chunk_days),
        "pairs": pairs,
        "outputs": pulled_files,
    }
    m = _manifest(root, meta)
    (root / "_manifest.json").write_text(json.dumps(m, indent=2), encoding="utf-8")
    print("MANIFEST_OK", root / "_manifest.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

