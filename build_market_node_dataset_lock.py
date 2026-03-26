#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

import pandas as pd


ROOT = Path(__file__).resolve().parent
FULL_ROOT = ROOT / "data_tape_15_full"
STITCHED_ROOT = ROOT / "data_tape_oanda_m5_15_stitched"
DEFAULT_OUT_BASE = ROOT / "market_node_data"

SESSION_CONFIG = {
    "asia": {"tz": "Asia/Tokyo", "start_hour": 7, "end_hour": 17},
    "london": {"tz": "Europe/London", "start_hour": 7, "end_hour": 17},
    "new_york": {"tz": "America/New_York", "start_hour": 7, "end_hour": 17},
    "sydney": {"tz": "Australia/Sydney", "start_hour": 7, "end_hour": 17},
}


def parse_ts(ts: str) -> datetime:
    s = str(ts)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def filter_session_weekday(df: pd.DataFrame, weekday: str, session: str) -> pd.DataFrame:
    cfg = SESSION_CONFIG[session]
    session_tz = ZoneInfo(cfg["tz"])
    start_hour = int(cfg["start_hour"])
    end_hour = int(cfg["end_hour"])
    local_dts = df["timestamp"].map(parse_ts).map(lambda dt: dt.astimezone(session_tz))
    mask = local_dts.map(
        lambda dt: start_hour <= dt.hour < end_hour and dt.strftime("%A").lower() == weekday
    )
    out = df.loc[mask].copy()
    out["_date"] = local_dts[mask].map(lambda dt: dt.date().isoformat())
    return out


def iter_pair_files(pair: str) -> Iterable[Path]:
    return sorted((FULL_ROOT / f"pair={pair}").glob("year=*/month=*/part-000.parquet"))


def stitched_pair_file(pair: str) -> Path:
    return STITCHED_ROOT / f"pair={pair}" / "stitched.parquet"


def stitched_coverage(pair: str) -> dict[str, Any] | None:
    path = stitched_pair_file(pair)
    if not path.exists():
        return None
    df = pd.read_parquet(path, columns=["timestamp"])
    if df.empty:
        return {"path": str(path), "row_count": 0, "first_timestamp": None, "last_timestamp": None}
    return {
        "path": str(path),
        "row_count": int(len(df)),
        "first_timestamp": str(df.iloc[0]["timestamp"]),
        "last_timestamp": str(df.iloc[-1]["timestamp"]),
    }


def select_target_dates(all_dates: list[str], session_count: int, selection_mode: str) -> list[str]:
    deduped = sorted(set(all_dates))
    if selection_mode == "oldest":
        return deduped[:session_count]
    if selection_mode == "newest":
        return deduped[-session_count:]
    raise ValueError(f"unsupported selection_mode={selection_mode}")


def collect_target_dates(pair: str, weekday: str, session: str, session_count: int, selection_mode: str) -> list[str]:
    all_dates: list[str] = []
    for path in iter_pair_files(pair):
        df = pd.read_parquet(path, columns=["timestamp"])
        filt = filter_session_weekday(df, weekday, session)
        if filt.empty:
            continue
        all_dates.extend(sorted(filt["_date"].unique().tolist()))
    return select_target_dates(all_dates, session_count, selection_mode)


def collect_rows_for_dates(pair: str, weekday: str, session: str, target_dates: list[str]) -> pd.DataFrame:
    required_columns = ["timestamp", "close"]
    optional_columns = ["pair", "session_id", "session", "weekday"]
    frames: list[pd.DataFrame] = []
    target_set = set(target_dates)
    seen_ts: set[str] = set()
    for path in iter_pair_files(pair):
        df = pd.read_parquet(path, columns=required_columns)
        filt = filter_session_weekday(df, weekday, session)
        filt = filt[filt["_date"].isin(target_set)].copy()
        if filt.empty:
            continue
        for col in optional_columns:
            if col not in filt.columns:
                if col == "pair":
                    filt[col] = pair
        filt["session_id"] = filt["_date"]
        filt["session"] = session
        filt["weekday"] = weekday
        filt["timestamp"] = filt["timestamp"].astype(str)
        filt = filt[["pair", "timestamp", "close", "session_id", "session", "weekday"]]
        filt = filt[~filt["timestamp"].isin(seen_ts)].copy()
        seen_ts.update(filt["timestamp"].tolist())
        frames.append(filt)
    if not frames:
        return pd.DataFrame(columns=["pair", "timestamp", "close", "session_id", "session", "weekday"])
    out = pd.concat(frames, ignore_index=True)
    return out.sort_values("timestamp").reset_index(drop=True)


def collect_dates_and_rows(pair: str, weekday: str, session: str, session_count: int, selection_mode: str) -> tuple[list[str], pd.DataFrame]:
    frames: list[pd.DataFrame] = []
    all_dates: list[str] = []
    for path in iter_pair_files(pair):
        df = pd.read_parquet(path, columns=["timestamp", "close"])
        filt = filter_session_weekday(df, weekday, session)
        if filt.empty:
            continue
        filt["pair"] = pair
        filt["session_id"] = filt["_date"]
        filt["session"] = session
        filt["weekday"] = weekday
        filt["timestamp"] = filt["timestamp"].astype(str)
        filt = filt[["pair", "timestamp", "close", "session_id", "session", "weekday"]]
        all_dates.extend(sorted(filt["session_id"].unique().tolist()))
        frames.append(filt)
    target_dates = select_target_dates(all_dates, session_count, selection_mode)
    if not frames:
        return target_dates, pd.DataFrame(columns=["pair", "timestamp", "close", "session_id", "session", "weekday"])
    target_set = set(target_dates)
    filtered = [frame[frame["session_id"].isin(target_set)].copy() for frame in frames]
    filtered = [frame for frame in filtered if not frame.empty]
    if not filtered:
        return target_dates, pd.DataFrame(columns=["pair", "timestamp", "close", "session_id", "session", "weekday"])
    out = pd.concat(filtered, ignore_index=True)
    out = out.drop_duplicates(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return target_dates, out


def write_partitioned(df: pd.DataFrame, pair: str, out_root: Path) -> None:
    if out_root.exists():
        for child in sorted(out_root.rglob("*"), reverse=True):
            if child.is_file() or child.is_symlink():
                child.unlink()
            elif child.is_dir():
                child.rmdir()
    out_root.mkdir(parents=True, exist_ok=True)

    dts = df["timestamp"].map(parse_ts)
    part_df = df.copy()
    part_df["_year"] = dts.map(lambda dt: dt.year)
    part_df["_month"] = dts.map(lambda dt: dt.month)
    for (year, month), part in part_df.groupby(["_year", "_month"], sort=True):
        out_dir = out_root / f"pair={pair}" / f"year={year}" / f"month={month:02d}"
        out_dir.mkdir(parents=True, exist_ok=True)
        part.drop(columns=["_year", "_month"]).to_parquet(out_dir / "part-000.parquet", index=False)


def build_data_fingerprint(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty:
        return {
            "row_count": 0,
            "first_timestamp": None,
            "last_timestamp": None,
            "content_hash": hashlib.sha256(b"").hexdigest(),
            "schema_columns": ["pair", "timestamp", "close", "session_id", "session", "weekday"],
        }
    h = hashlib.sha256()
    for row in df.itertuples(index=False):
        h.update(
            json.dumps(
                {
                    "pair": row.pair,
                    "timestamp": row.timestamp,
                    "close": row.close,
                    "session_id": row.session_id,
                    "session": row.session,
                    "weekday": row.weekday,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )
    return {
        "row_count": int(len(df)),
        "first_timestamp": str(df.iloc[0]["timestamp"]),
        "last_timestamp": str(df.iloc[-1]["timestamp"]),
        "content_hash": h.hexdigest(),
        "schema_columns": ["pair", "timestamp", "close", "session_id", "session", "weekday"],
    }


def build_lock_hash(pair: str, weekday: str, session: str, session_count: int, dates: list[str]) -> str:
    h = hashlib.sha256()
    payload = {
        "pair": pair,
        "weekday": weekday,
        "session": session,
        "session_count": session_count,
        "dates": dates,
        "schema_mode": "price_only",
    }
    h.update(json.dumps(payload, sort_keys=True).encode())
    return h.hexdigest()


def lock_matches_existing(lock_path: Path, expected: dict[str, Any]) -> bool:
    if not lock_path.exists():
        return False
    try:
        current = json.loads(lock_path.read_text())
    except Exception:
        return False
    for key, value in expected.items():
        if current.get(key) != value:
            return False
    data_root = Path(str(current.get("data_root", "")))
    resolved_root = data_root if data_root.is_absolute() else ROOT / data_root
    return resolved_root.exists()


def main() -> int:
    ap = argparse.ArgumentParser(description="Build a reusable dataset lock for any pair/weekday/session node.")
    ap.add_argument("--pair", required=True)
    ap.add_argument("--weekday", required=True, choices=["monday", "tuesday", "wednesday", "thursday", "friday"])
    ap.add_argument("--session", required=True, choices=sorted(SESSION_CONFIG))
    ap.add_argument("--session-count", type=int, default=11)
    ap.add_argument("--date-selection", choices=["oldest", "newest"], default="newest")
    ap.add_argument("--out-base", type=Path, default=DEFAULT_OUT_BASE)
    ap.add_argument("--lock-path", type=Path, default=None)
    args = ap.parse_args()

    pair = args.pair.upper()
    weekday = args.weekday.lower()
    session = args.session.lower()
    node_slug = f"{pair.lower()}__{weekday}__{session}__{args.session_count}"
    data_root = args.out_base / node_slug
    lock_path = args.lock_path or (ROOT / f"dataset_lock__{node_slug}.json")

    pair_files = list(iter_pair_files(pair))
    if not pair_files:
        alt = stitched_coverage(pair)
        detail = {
            "pair": pair,
            "source_root": str(FULL_ROOT),
            "missing_pair_dir": str(FULL_ROOT / f"pair={pair}"),
            "stitched_coverage": alt,
        }
        raise FileNotFoundError(
            f"No source files for {pair} under {FULL_ROOT}. Details: {json.dumps(detail, indent=2)}"
        )

    target_dates, df = collect_dates_and_rows(pair, weekday, session, args.session_count, args.date_selection)
    if not target_dates:
        alt = stitched_coverage(pair)
        detail = {
            "pair": pair,
            "weekday": weekday,
            "session": session,
            "requested_session_count": args.session_count,
            "source_root": str(FULL_ROOT),
            "pair_file_count": len(pair_files),
            "stitched_coverage": alt,
            "date_selection": args.date_selection,
        }
        raise RuntimeError(
            f"No target dates found for {pair} {weekday} {session}. Details: {json.dumps(detail, indent=2)}"
        )

    audit_preview = {
        "pair": pair,
        "session": session,
        "weekday": weekday,
        "session_count": len(target_dates),
        "dates": target_dates,
        "data_root": str(data_root.relative_to(ROOT)),
        "hash": build_lock_hash(pair, weekday, session, args.session_count, target_dates),
        "schema_mode": "price_only",
        "date_selection": args.date_selection,
    }
    if lock_matches_existing(lock_path, audit_preview):
        print(json.dumps({"status": "SKIP", "lock_path": str(lock_path), "data_root": str(data_root), "reason": "dataset_lock_current"}, indent=2))
        return 0

    fingerprint = build_data_fingerprint(df)
    write_partitioned(df, pair, data_root)

    audit = {
        **audit_preview,
        **fingerprint,
    }
    lock_path.write_text(json.dumps(audit, indent=2))
    print(json.dumps({"status": "PASS", "lock_path": str(lock_path), "data_root": str(data_root), "row_count": len(df)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
