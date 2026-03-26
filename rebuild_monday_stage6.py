#!/usr/bin/env python3
"""
rebuild_monday_stage6.py

Stage-6 rebuild for all Monday nodes.
Reads raw parquet data, computes canonical energy features, and writes:
  - session_energy_state_stream.csv  (with all required fields)
  - node_identity.json
  - session_state_build_report.json
"""
from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parent
NODES_ROOT = ROOT / "compiled_market_nodes"
DATA_ROOT = ROOT / "market_node_data"

SESSION_TZ = {
    "sydney":   "Australia/Sydney",
    "asia":     "Asia/Tokyo",
    "london":   "Europe/London",
    "new_york": "America/New_York",
}
SESSION_START_HOUR = 7  # each session starts at local 07:00

REQUIRED_COLS = [
    "speed_3", "speed_10", "bias_20", "compression",
    "pullback_depth_10", "distance_from_extreme_10",
    "reclaim_state", "swing_break_state", "quarter_phase",
]

STREAM_FIELDNAMES = [
    "timestamp", "session_id", "direction", "pair", "session", "weekday",
    "speed_3", "speed_10", "bias_20", "compression",
    "pullback_depth_10", "distance_from_extreme_10",
    "reclaim_state", "swing_break_state", "quarter_phase",
    # bonus columns kept for downstream context
    "speed_5", "vol_10", "vol_20", "range_5", "range_10", "range_20",
    "bias_5", "bias_10", "acceleration", "breakout_distance_20",
    "slope_consistency_10", "slope_consistency_20",
    "price",
]

MIN_LOOKBACK = 21  # bars required before emitting a feature row


def parse_ts(ts: str) -> datetime:
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def pip_size(pair: str) -> float:
    return 0.01 if pair.upper().endswith("_JPY") else 0.0001


def quarter_from_dt(dt: datetime, session: str) -> str:
    tz_name = SESSION_TZ.get(session, "UTC")
    local_dt = dt.astimezone(ZoneInfo(tz_name))
    minute_offset = (local_dt.hour - SESSION_START_HOUR) * 60 + local_dt.minute
    if minute_offset < 120:
        return "Q1"
    if minute_offset < 240:
        return "Q2"
    if minute_offset < 360:
        return "Q3"
    return "Q4"


def slope_sign_consistency(vals: list[float], direction: str, pip: float) -> float:
    if len(vals) < 2:
        return 0.0
    diffs = [(vals[i] - vals[i - 1]) / pip for i in range(1, len(vals))]
    signed = [d if direction == "LONG" else -d for d in diffs]
    positives = sum(1 for d in signed if d > 0)
    return positives / len(signed)


def compute_features(
    direction: str,
    prev_prices: list[float],
    pair: str,
) -> dict[str, Any]:
    """
    Canonical Stage-6 energy feature computation.
    Authoritative logic mirrors stage7_11_sessions_entry_fit_energy_v3.py::compute_energy_features.
    """
    pip = pip_size(pair)
    p = prev_prices
    n = len(p)

    diffs = [(p[i] - p[i - 1]) / pip for i in range(1, n)]
    signed = [d if direction == "LONG" else -d for d in diffs]
    adiffs = [abs(d) for d in diffs]

    speed_3  = mean(adiffs[-3:])  if len(adiffs) >= 3  else (mean(adiffs) if adiffs else 0.0)
    speed_5  = mean(adiffs[-5:])  if len(adiffs) >= 5  else (mean(adiffs) if adiffs else 0.0)
    speed_10 = mean(adiffs[-10:]) if len(adiffs) >= 10 else (mean(adiffs) if adiffs else 0.0)
    vol_10   = speed_10
    vol_20   = mean(adiffs[-20:]) if len(adiffs) >= 20 else (mean(adiffs) if adiffs else 0.0)

    def price_range(window: int) -> float:
        seg = p[-window:] if len(p) >= window else p
        return (max(seg) - min(seg)) / pip

    range_5  = price_range(5)
    range_10 = price_range(10)
    range_20 = price_range(20)

    def trend_pips(start_idx: int) -> float:
        s = p[max(0, n - start_idx - 1)]
        raw = (p[-1] - s) / pip
        return raw if direction == "LONG" else -raw

    def bias(window: int) -> float:
        seg = signed[-window:] if len(signed) >= window else signed
        return sum(seg) / max(1e-9, sum(abs(x) for x in seg)) if seg else 0.0

    bias_5  = bias(5)
    bias_10 = bias(10)
    bias_20 = bias(20) if len(signed) >= 20 else (sum(signed) / max(1e-9, sum(abs(x) for x in signed)) if signed else 0.0)

    compression = range_5 / max(range_20, 1e-9)
    acceleration = speed_3 - speed_10

    # Directional extreme distances
    last = p[-1]
    if direction == "LONG":
        lookback10 = p[-10:] if len(p) >= 10 else p
        pullback_depth_10      = (max(lookback10) - last) / pip
        distance_from_extreme_10 = (last - min(lookback10)) / pip
        breakout_distance_20   = max(0.0, (last - max(p[:-1])) / pip) if n > 1 else 0.0
    else:
        lookback10 = p[-10:] if len(p) >= 10 else p
        pullback_depth_10      = (last - min(lookback10)) / pip
        distance_from_extreme_10 = (max(lookback10) - last) / pip
        breakout_distance_20   = max(0.0, (min(p[:-1]) - last) / pip) if n > 1 else 0.0

    # swing_break_state: 1 if current bar broke through the prior 10-bar extreme
    prior10 = p[-(11):-1] if len(p) >= 11 else (p[:-1] if n > 1 else p)
    if direction == "LONG":
        swing_break_state = 1 if (prior10 and last > max(prior10)) else 0
    else:
        swing_break_state = 1 if (prior10 and last < min(prior10)) else 0

    # reclaim_state: 1 if price has "reclaimed" – returned to / above the 20-bar
    # midpoint after a prior visit below it (LONG) or vice versa (SHORT).
    # Proxy: price is on the favourable side of the 20-bar midpoint AND crossed that
    # midpoint from the wrong side within the last 5 bars.
    lookback20 = p[-20:] if len(p) >= 20 else p
    midpoint   = (max(lookback20) + min(lookback20)) / 2.0
    prev5      = p[-5:-1] if len(p) >= 5 else (p[:-1] if n > 1 else p)
    if direction == "LONG":
        on_right_side   = last > midpoint
        was_below_mid   = any(x <= midpoint for x in prev5) if prev5 else False
        reclaim_state   = 1 if (on_right_side and was_below_mid) else 0
    else:
        on_right_side   = last < midpoint
        was_above_mid   = any(x >= midpoint for x in prev5) if prev5 else False
        reclaim_state   = 1 if (on_right_side and was_above_mid) else 0

    scons10 = slope_sign_consistency(p[-10:] if len(p) >= 10 else p, direction, pip)
    scons20 = slope_sign_consistency(p[-20:] if len(p) >= 20 else p, direction, pip)

    return {
        "speed_3":  round(speed_3,  6),
        "speed_5":  round(speed_5,  6),
        "speed_10": round(speed_10, 6),
        "vol_10":   round(vol_10,   6),
        "vol_20":   round(vol_20,   6),
        "range_5":  round(range_5,  6),
        "range_10": round(range_10, 6),
        "range_20": round(range_20, 6),
        "bias_5":   round(bias_5,   6),
        "bias_10":  round(bias_10,  6),
        "bias_20":  round(bias_20,  6),
        "acceleration":        round(acceleration,         6),
        "compression":         round(compression,          6),
        "pullback_depth_10":   round(pullback_depth_10,    6),
        "distance_from_extreme_10": round(distance_from_extreme_10, 6),
        "breakout_distance_20":     round(breakout_distance_20,     6),
        "slope_consistency_10":     round(scons10, 6),
        "slope_consistency_20":     round(scons20, 6),
        "swing_break_state": swing_break_state,
        "reclaim_state":     reclaim_state,
    }


def load_parquet_for_node(data_root: Path, pair: str) -> list[dict[str, Any]]:
    """Load all parquet files for a node, sorted by timestamp."""
    rows: list[dict[str, Any]] = []
    for pf in sorted(data_root.rglob("part-000.parquet")):
        df = pd.read_parquet(pf)
        for rec in df.to_dict("records"):
            dt = parse_ts(str(rec["timestamp"]))
            rows.append({
                "timestamp": str(rec["timestamp"]),
                "dt":         dt,
                "price":      float(rec["close"]),
                "pair":       str(rec.get("pair", pair)),
                "session":    str(rec.get("session", "")),
                "weekday":    str(rec.get("weekday", "")),
                "session_id": str(rec.get("session_id", dt.date().isoformat())),
            })
    rows.sort(key=lambda r: r["dt"])
    return rows


def build_stream(rows: list[dict[str, Any]], session: str) -> list[dict[str, Any]]:
    """Group rows by session_id, then emit per-bar LONG/SHORT feature rows."""
    by_session: dict[str, list[dict[str, Any]]] = {}
    for r in rows:
        sid = r.get("session_id") or r["dt"].date().isoformat()
        by_session.setdefault(sid, []).append(r)

    stream: list[dict[str, Any]] = []
    for sid, sess_rows in sorted(by_session.items()):
        prices = [r["price"] for r in sess_rows]
        for idx, row in enumerate(sess_rows):
            if idx < MIN_LOOKBACK:
                continue
            prev_prices = [r["price"] for r in sess_rows[: idx + 1]]
            quarter = quarter_from_dt(row["dt"], session)
            for direction in ("LONG", "SHORT"):
                feats = compute_features(direction, prev_prices, row["pair"])
                stream.append({
                    "timestamp":  row["timestamp"],
                    "session_id": sid,
                    "direction":  direction,
                    "pair":       row["pair"],
                    "session":    row.get("session", session),
                    "weekday":    row.get("weekday", "monday"),
                    "quarter_phase": quarter,
                    "price":      round(row["price"], 6),
                    **feats,
                })
    return stream


def build_node_identity(node_dir: Path) -> dict[str, Any]:
    manifest_path = node_dir / "node_manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text())
        node = manifest.get("node", {})
        return {
            "pair":      node.get("pair"),
            "weekday":   node.get("weekday"),
            "session":   node.get("session"),
            "node_path": str(node_dir),
        }
    except Exception:
        # Derive from directory name: PAIR__monday__SESSION
        name = node_dir.name
        parts = name.split("__")
        return {
            "pair":    parts[0] if len(parts) > 0 else "unknown",
            "weekday": parts[1] if len(parts) > 1 else "monday",
            "session": parts[2] if len(parts) > 2 else "unknown",
            "node_path": str(node_dir),
        }


def build_session_state_report(stream: list[dict[str, Any]]) -> dict[str, Any]:
    session_ids = sorted({r["session_id"] for r in stream})
    directions  = sorted({r["direction"] for r in stream})
    quarters    = sorted({r["quarter_phase"] for r in stream})
    return {
        "status": "PASS",
        "stream_rows": len(stream),
        "session_count": len(session_ids),
        "direction_count": len(directions),
        "quarters": quarters,
        "rebuild_source": "rebuild_monday_stage6.py",
        "rebuild_timestamp": datetime.now(timezone.utc).isoformat(),
        "required_columns_present": REQUIRED_COLS,
    }


def process_node(node_dir: Path) -> dict[str, Any]:
    identity = build_node_identity(node_dir)
    pair     = identity["pair"]
    weekday  = identity["weekday"]
    session  = identity["session"]

    # Locate data root
    node_key = f"{pair.lower()}__{weekday}__{session}__11"
    data_root = DATA_ROOT / node_key
    if not data_root.exists():
        return {"node": str(node_dir), "status": "SKIP", "reason": f"data_root not found: {data_root}"}

    # Load parquet data and build feature stream
    rows = load_parquet_for_node(data_root, pair)
    if not rows:
        return {"node": str(node_dir), "status": "SKIP", "reason": "no parquet rows found"}

    stream = build_stream(rows, session)
    if not stream:
        return {"node": str(node_dir), "status": "SKIP", "reason": "stream is empty after feature computation"}

    # Write node_identity.json
    (node_dir / "node_identity.json").write_text(json.dumps(identity, indent=2))

    # Write session_state_build_report.json
    report = build_session_state_report(stream)
    (node_dir / "session_state_build_report.json").write_text(json.dumps(report, indent=2))

    # Write session_energy_state_stream.csv
    stream_path = node_dir / "session_energy_state_stream.csv"
    # Determine fieldnames: specified order first, then any extra keys
    extra_keys = [k for k in stream[0].keys() if k not in STREAM_FIELDNAMES]
    fieldnames = STREAM_FIELDNAMES + extra_keys
    with stream_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(stream)

    return {
        "node":        str(node_dir),
        "status":      "OK",
        "stream_rows": len(stream),
        "sessions":    report["session_count"],
    }


def main() -> None:
    monday_nodes = sorted(
        d for d in NODES_ROOT.iterdir()
        if d.is_dir() and "__monday__" in d.name
        and not d.name.endswith("_stale_1773285329")   # skip stale clones
    )

    if not monday_nodes:
        print("No Monday nodes found under compiled_market_nodes/", file=sys.stderr)
        sys.exit(1)

    print(f"Rebuilding Stage-6 for {len(monday_nodes)} Monday nodes...")
    results = []
    for node_dir in monday_nodes:
        print(f"  [{monday_nodes.index(node_dir)+1}/{len(monday_nodes)}] {node_dir.name} ...", end=" ", flush=True)
        result = process_node(node_dir)
        status = result["status"]
        if status == "OK":
            print(f"OK ({result['stream_rows']} rows, {result['sessions']} sessions)")
        else:
            print(f"{status}: {result.get('reason', '')}")
        results.append(result)

    # Summary
    ok    = [r for r in results if r["status"] == "OK"]
    skip  = [r for r in results if r["status"] == "SKIP"]
    total_rows = sum(r.get("stream_rows", 0) for r in ok)

    print(f"\n{'='*60}")
    print(f"Stage-6 rebuild complete.")
    print(f"  OK:      {len(ok)}")
    print(f"  Skipped: {len(skip)}")
    print(f"  Total stream rows written: {total_rows}")
    if skip:
        print(f"\nSkipped nodes:")
        for r in skip:
            print(f"  {r['node']}: {r.get('reason', '')}")

    # Write rebuild manifest
    manifest = {
        "rebuild_timestamp": datetime.now(timezone.utc).isoformat(),
        "nodes_ok":   len(ok),
        "nodes_skip": len(skip),
        "total_stream_rows": total_rows,
        "results": results,
    }
    (ROOT / "monday_stage6_rebuild_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(f"\nManifest written → monday_stage6_rebuild_manifest.json")


if __name__ == "__main__":
    main()
