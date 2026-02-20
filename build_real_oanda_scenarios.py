#!/usr/bin/env python3
from __future__ import annotations

import csv
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import phone_bot


OUT_DIR = Path("/workspaces/phone-signals/scenarios/golden/v1.0")
PAIR = "EUR_USD"


@dataclass
class CandleTick:
    instrument: str
    ts: float
    bid: float
    ask: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0


def _to_epoch(ts_s: str) -> float:
    dt = datetime.fromisoformat(str(ts_s).replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).timestamp()


def _to_rfc3339(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _load_fallback_creds_from_phone_bot() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    content = Path("/workspaces/phone-signals/phone_bot.py").read_text(encoding="utf-8")
    key_match = re.search(r'os\.environ\["OANDA_API_KEY"\]\s*=\s*"([^"]+)"', content)
    acct_match = re.search(r'os\.environ\["OANDA_ACCOUNT_ID"\]\s*=\s*"([^"]+)"', content)
    env_match = re.search(r'os\.environ\["OANDA_ENV"\]\s*=\s*"([^"]+)"', content)
    return (
        key_match.group(1) if key_match else None,
        acct_match.group(1) if acct_match else None,
        env_match.group(1) if env_match else "practice",
    )


def build_oanda_client() -> phone_bot.OandaClient:
    key = str(os.getenv("OANDA_API_KEY", "") or "").strip()
    account = str(os.getenv("OANDA_ACCOUNT_ID", "") or "").strip()
    env = str(os.getenv("OANDA_ENV", "practice") or "practice").strip()

    if not key or not account or key == "stub" or account == "stub":
        fb_key, fb_account, fb_env = _load_fallback_creds_from_phone_bot()
        if fb_key and fb_account:
            key = fb_key
            account = fb_account
            env = str(fb_env or "practice")

    if not key or not account or key == "stub" or account == "stub":
        raise RuntimeError("Missing usable OANDA credentials in env and fallback source")

    return phone_bot.OandaClient(api_key=key, account_id=account, env=env)


def fetch_candles(
    client: phone_bot.OandaClient,
    instrument: str,
    start_dt: datetime,
    end_dt: datetime,
    granularity: str,
) -> List[CandleTick]:
    if end_dt <= start_dt:
        return []

    out: List[CandleTick] = []
    cursor = start_dt
    gran = str(granularity).upper()
    if gran == "M1":
        step = timedelta(minutes=1)
        max_chunk = timedelta(minutes=4500)
    elif gran == "S5":
        step = timedelta(seconds=5)
        max_chunk = timedelta(hours=6)
    else:
        raise ValueError(f"Unsupported granularity: {granularity}")

    while cursor < end_dt:
        chunk_end = min(end_dt, cursor + max_chunk)
        params: Dict[str, Any] = {
            "granularity": gran,
            "price": "BA",
            "from": _to_rfc3339(cursor),
            "to": _to_rfc3339(chunk_end),
        }
        resp = client._get(f"/v3/instruments/{instrument}/candles", params=params)
        if not isinstance(resp, dict) or resp.get("_http_error") or resp.get("_exception"):
            raise RuntimeError(f"OANDA candles request failed: {resp}")

        candles = resp.get("candles", [])
        for c in candles:
            if not c.get("complete", False):
                continue
            bid_c = c.get("bid", {})
            ask_c = c.get("ask", {})
            bid = float(bid_c.get("c"))
            ask = float(ask_c.get("c"))
            out.append(CandleTick(instrument=instrument, ts=_to_epoch(c.get("time")), bid=bid, ask=ask))

        cursor = chunk_end + step

    out.sort(key=lambda x: x.ts)
    return out


def _window_net_pips(w: List[CandleTick]) -> float:
    if len(w) < 2:
        return 0.0
    return (w[-1].mid - w[0].mid) / 0.0001


def _window_range_pips(w: List[CandleTick]) -> float:
    if not w:
        return 0.0
    mids = [x.mid for x in w]
    return (max(mids) - min(mids)) / 0.0001


def _window_pullback_pips(w: List[CandleTick]) -> float:
    if len(w) < 3:
        return 0.0
    peak = w[0].mid
    max_pullback = 0.0
    for x in w:
        if x.mid > peak:
            peak = x.mid
        max_pullback = max(max_pullback, (peak - x.mid) / 0.0001)
    return max_pullback


def _is_contiguous_window(w: List[CandleTick], max_gap_sec: float = 90.0) -> bool:
    if len(w) < 2:
        return False
    for i in range(1, len(w)):
        if (w[i].ts - w[i - 1].ts) > max_gap_sec:
            return False
    return True


def _fill_s5_gaps(w: List[CandleTick], max_fill_gap_sec: float = 60.0) -> List[CandleTick]:
    if len(w) < 2:
        return w
    out: List[CandleTick] = [w[0]]
    for i in range(1, len(w)):
        prev = out[-1]
        cur = w[i]
        gap = cur.ts - prev.ts
        if gap <= 5.0:
            out.append(cur)
            continue
        if gap > max_fill_gap_sec:
            raise RuntimeError(f"Large S5 gap detected: {gap:.1f}s")
        t = prev.ts + 5.0
        while t < cur.ts:
            out.append(CandleTick(instrument=prev.instrument, ts=t, bid=prev.bid, ask=prev.ask))
            t += 5.0
        out.append(cur)
    return out


def _best_window(
    ticks: List[CandleTick],
    length: int,
    scorer,
) -> List[CandleTick]:
    best_score = float("-inf")
    best: List[CandleTick] = []
    for i in range(0, max(0, len(ticks) - length + 1), 5):
        w = ticks[i : i + length]
        if not _is_contiguous_window(w):
            continue
        score = scorer(w)
        if score > best_score:
            best_score = score
            best = w
    return best


def find_scenarios(ticks: List[CandleTick]) -> Dict[str, List[CandleTick]]:
    trend = _best_window(
        ticks,
        length=240,
        scorer=lambda w: (_window_net_pips(w) if 25.0 <= _window_net_pips(w) <= 120.0 else -9999.0),
    )

    def panic_score(w: List[CandleTick]) -> float:
        if len(w) < 180:
            return -9999.0
        first = w[:90]
        second = w[90:]
        up = _window_net_pips(first)
        down = _window_net_pips(second)
        total = _window_net_pips(w)
        rev_strength = up - down
        return rev_strength if (up >= 12.0 and down <= -12.0 and total <= 8.0) else -9999.0

    panic = _best_window(ticks, length=180, scorer=panic_score)

    whipsaw = _best_window(
        ticks,
        length=120,
        scorer=lambda w: (_window_range_pips(w) - abs(_window_net_pips(w)) * 2.0)
        if (_window_range_pips(w) >= 18.0 and abs(_window_net_pips(w)) <= 8.0)
        else -9999.0,
    )

    energy_depletion = _best_window(
        ticks,
        length=300,
        scorer=lambda w: (
            (_window_net_pips(w[:150]) - abs(_window_net_pips(w[150:])))
            if (_window_net_pips(w[:150]) >= 15.0 and abs(_window_net_pips(w[150:])) <= 8.0)
            else -9999.0
        ),
    )

    high_energy = _best_window(
        ticks,
        length=180,
        scorer=lambda w: (
            (_window_net_pips(w) + _window_range_pips(w) - _window_pullback_pips(w))
            if (_window_net_pips(w) >= 20.0 and _window_range_pips(w) >= 28.0)
            else -9999.0
        ),
    )

    return {
        "trend_continuation.csv": trend,
        "panic_reversal.csv": panic,
        "whipsaw_spike_fade.csv": whipsaw,
        "energy_depletion.csv": energy_depletion,
        "high_energy_trend.csv": high_energy,
    }


def write_ticks_csv(path: Path, ticks: List[CandleTick]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["instrument", "ts", "bid", "ask", "mid"])
        for t in ticks:
            writer.writerow([t.instrument, f"{t.ts:.1f}", f"{t.bid:.5f}", f"{t.ask:.5f}", f"{t.mid:.5f}"])


def _window_bounds(window: List[CandleTick]) -> Tuple[datetime, datetime]:
    if not window:
        raise ValueError("empty window")
    start_dt = datetime.fromtimestamp(window[0].ts, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(window[-1].ts, tz=timezone.utc)
    return start_dt, end_dt


def main() -> int:
    client = build_oanda_client()
    end_dt = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_dt = end_dt - timedelta(days=60)

    m1_ticks = fetch_candles(client, PAIR, start_dt, end_dt, granularity="M1")
    if len(m1_ticks) < 2000:
        raise RuntimeError(f"Not enough OANDA M1 candles fetched: {len(m1_ticks)}")

    scenarios_m1 = find_scenarios(m1_ticks)
    for fname, m1_window in scenarios_m1.items():
        if not m1_window:
            raise RuntimeError(f"Failed to find scenario window for {fname}")
        w_start, w_end = _window_bounds(m1_window)
        s5_start = w_start
        s5_end = w_end + timedelta(minutes=1)
        s5_ticks = fetch_candles(client, PAIR, s5_start, s5_end, granularity="S5")
        if len(s5_ticks) < 30:
            raise RuntimeError(f"Insufficient S5 candles for {fname}: {len(s5_ticks)}")
        s5_ticks = _fill_s5_gaps(s5_ticks, max_fill_gap_sec=60.0)
        if not _is_contiguous_window(s5_ticks, max_gap_sec=5.1):
            raise RuntimeError(f"Non-contiguous S5 window for {fname}")
        write_ticks_csv(OUT_DIR / fname, s5_ticks)
        print(
            f"WROTE {fname} rows={len(s5_ticks)} "
            f"m1_net_pips={_window_net_pips(m1_window):.1f} "
            f"s5_net_pips={_window_net_pips(s5_ticks):.1f}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())