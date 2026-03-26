#!/usr/bin/env python3
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd


ROOT = Path(__file__).resolve().parent
BASE_ROOT = ROOT / "london_session_data"
FULL_ROOT = ROOT / "data_tape_15_full"
OUT_ROOT = ROOT / "london_session_data_11"
PAIR = "EUR_USD"
TARGET_SESSION_COUNT = 11


def parse_ts(ts: str) -> datetime:
    ts = str(ts)
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def london_monday(df: pd.DataFrame) -> pd.DataFrame:
    dts = df["timestamp"].map(parse_ts)
    mask = dts.map(lambda dt: 8 <= dt.hour < 16 and dt.strftime("%A").lower() == "monday")
    out = df.loc[mask].copy()
    out["_date"] = dts[mask].map(lambda dt: dt.date().isoformat())
    return out


def collect_target_dates() -> List[str]:
    base_file = BASE_ROOT / "pair=EUR_USD/year=2024/month=01/part-000.parquet"
    base_df = pd.read_parquet(base_file)
    base_dates = sorted(london_monday(base_df)["_date"].unique().tolist())

    all_dates: List[str] = []
    for path in sorted((FULL_ROOT / f"pair={PAIR}").glob("year=*/month=*/part-000.parquet")):
        df = pd.read_parquet(path, columns=["timestamp"])
        filt = london_monday(df)
        all_dates.extend(sorted(filt["_date"].unique().tolist()))

    merged = []
    seen = set()
    for date in base_dates + sorted(set(all_dates)):
        if date not in seen:
            merged.append(date)
            seen.add(date)
        if len(merged) >= TARGET_SESSION_COUNT:
            break
    return merged


def collect_rows_for_dates(target_dates: List[str]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    target_set = set(target_dates)

    source_files = [BASE_ROOT / "pair=EUR_USD/year=2024/month=01/part-000.parquet"]
    source_files += sorted((FULL_ROOT / f"pair={PAIR}").glob("year=*/month=*/part-000.parquet"))

    seen_ts = set()
    for path in source_files:
        df = pd.read_parquet(path)
        filt = london_monday(df)
        filt = filt[filt["_date"].isin(target_set)].copy()
        if filt.empty:
            continue
        filt["timestamp"] = filt["timestamp"].astype(str)
        filt = filt[~filt["timestamp"].isin(seen_ts)].copy()
        seen_ts.update(filt["timestamp"].tolist())
        frames.append(filt.drop(columns=["_date"]))

    if not frames:
        return pd.DataFrame(columns=["pair", "timestamp", "open", "high", "low", "close", "volume", "complete"])

    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values("timestamp").reset_index(drop=True)
    return out


def write_partitioned(df: pd.DataFrame) -> None:
    for path in OUT_ROOT.glob("pair=*"):
        if path.is_dir():
            for child in sorted(path.rglob("*"), reverse=True):
                if child.is_file():
                    child.unlink()
                elif child.is_dir():
                    child.rmdir()
            path.rmdir()

    dts = df["timestamp"].map(parse_ts)
    df = df.copy()
    df["_year"] = dts.map(lambda dt: dt.year)
    df["_month"] = dts.map(lambda dt: dt.month)

    for (year, month), part in df.groupby(["_year", "_month"], sort=True):
        out_dir = OUT_ROOT / f"pair={PAIR}" / f"year={year}" / f"month={month:02d}"
        out_dir.mkdir(parents=True, exist_ok=True)
        part.drop(columns=["_year", "_month"]).to_parquet(out_dir / "part-000.parquet", index=False)


def sha256_dates(dates: List[str]) -> str:
    h = hashlib.sha256()
    for date in dates:
        h.update(date.encode())
    return h.hexdigest()


def main() -> int:
    target_dates = collect_target_dates()
    df = collect_rows_for_dates(target_dates)
    write_partitioned(df)

    audit: Dict[str, object] = {
        "pair": PAIR,
        "session": "london",
        "weekday": "monday",
        "session_count": len(target_dates),
        "dates": target_dates,
        "row_count": int(len(df)),
        "first_timestamp": None if df.empty else str(df.iloc[0]["timestamp"]),
        "last_timestamp": None if df.empty else str(df.iloc[-1]["timestamp"]),
        "data_root": str(OUT_ROOT.relative_to(ROOT)),
        "hash": sha256_dates(target_dates),
    }
    (ROOT / "dataset_lock_11_sessions.json").write_text(json.dumps(audit, indent=2))
    print(json.dumps(audit, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
