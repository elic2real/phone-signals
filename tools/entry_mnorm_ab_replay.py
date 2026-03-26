#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import phone_bot


@dataclass
class Tick:
    ts: float
    bid: float
    ask: float

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0


def parse_ts(raw: str) -> float:
    s = str(raw).strip()
    try:
        return float(s)
    except Exception:
        return 0.0


def load_ticks(path: str) -> tuple[str, list[Tick]]:
    pair = "EUR_USD"
    out: list[Tick] = []
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            pair = str(row.get("instrument") or pair)
            out.append(
                Tick(
                    ts=parse_ts(str(row.get("ts") or "0")),
                    bid=float(row.get("bid") or 0.0),
                    ask=float(row.get("ask") or 0.0),
                )
            )
    out.sort(key=lambda t: t.ts)
    return pair, out


def build_candles(ticks: list[Tick], bucket_sec: int = 300) -> list[dict[str, Any]]:
    if not ticks:
        return []
    candles: list[dict[str, Any]] = []
    b0 = int(ticks[0].ts // bucket_sec) * bucket_sec
    o = h = l = c = ticks[0].mid
    for t in ticks:
        b = int(t.ts // bucket_sec) * bucket_sec
        if b != b0:
            candles.append({"time": float(b0), "o": o, "h": h, "l": l, "c": c, "complete": True})
            b0 = b
            o = h = l = c = t.mid
        else:
            px = t.mid
            h = max(h, px)
            l = min(l, px)
            c = px
    candles.append({"time": float(b0), "o": o, "h": h, "l": l, "c": c, "complete": True})
    return candles


def eval_contract_direction(candles: list[dict[str, Any]], mode: str) -> Counter:
    """
    mode:
      - before_abs: m_norm = abs(mom)/atr
      - after_signed: m_norm = mom/atr
    """
    out = Counter()
    st = phone_bot.PairState()
    n = max(30, int(getattr(phone_bot, "MOM_N", 5) + 5))
    for i in range(n, len(candles) + 1):
        w = candles[:i]
        atr = float(phone_bot.compute_atr_price(w, int(getattr(phone_bot, "ATR_N", 14))) or 0.0)
        mom = float(phone_bot.momentum(w, int(getattr(phone_bot, "MOM_N", 5))) or 0.0)
        wr = float(phone_bot.williams_r(w, int(getattr(phone_bot, "WR_N", 14))) or float("nan"))
        if not (math.isfinite(atr) and atr > 0):
            continue
        if mode == "before_abs":
            m_norm = abs(mom) / atr if math.isfinite(mom) else float("nan")
        else:
            m_norm = mom / atr if math.isfinite(mom) else float("nan")
        st.m_norm = m_norm
        st.wr = wr if math.isfinite(wr) else -50.0
        d, src = phone_bot._watch_primitive_contract_direction(st)
        if d in ("LONG", "SHORT"):
            out[f"armed_{d.lower()}"] += 1
            out["armed_total"] += 1
        else:
            out["blocked_total"] += 1
            out[f"blocked_{src or 'unknown'}"] += 1
    return out


def summarize(results: dict[str, Counter]) -> dict[str, Any]:
    agg = Counter()
    for c in results.values():
        agg.update(c)
    n_files = max(1, len(results))
    return {
        "files": len(results),
        "armed_total": int(agg.get("armed_total", 0)),
        "armed_long": int(agg.get("armed_long", 0)),
        "armed_short": int(agg.get("armed_short", 0)),
        "blocked_total": int(agg.get("blocked_total", 0)),
        "blocked_primitive_disagreement": int(agg.get("blocked_primitive_disagreement", 0)),
        "blocked_no_directional_primitive": int(agg.get("blocked_no_directional_primitive", 0)),
        "armed_per_file_mean": float(agg.get("armed_total", 0)) / n_files,
        "short_share": (float(agg.get("armed_short", 0)) / float(max(1, agg.get("armed_total", 0)))),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Deterministic entry-gate A/B replay for m_norm abs vs signed.")
    ap.add_argument("--glob", default="scenarios/golden/v1.0/*.csv")
    ap.add_argument("--bucket-sec", type=int, default=300)
    ap.add_argument("--out", default="reports/entry_mnorm_ab_replay.json")
    args = ap.parse_args()

    paths = sorted(glob.glob(args.glob))
    before_by_file: dict[str, Counter] = {}
    after_by_file: dict[str, Counter] = {}
    by_file_delta: dict[str, dict[str, Any]] = {}

    for p in paths:
        pair, ticks = load_ticks(p)
        candles = build_candles(ticks, bucket_sec=max(60, int(args.bucket_sec)))
        b = eval_contract_direction(candles, mode="before_abs")
        a = eval_contract_direction(candles, mode="after_signed")
        k = Path(p).name
        before_by_file[k] = b
        after_by_file[k] = a
        by_file_delta[k] = {
            "pair": pair,
            "before_armed_long": int(b.get("armed_long", 0)),
            "before_armed_short": int(b.get("armed_short", 0)),
            "after_armed_long": int(a.get("armed_long", 0)),
            "after_armed_short": int(a.get("armed_short", 0)),
            "delta_armed_short": int(a.get("armed_short", 0) - b.get("armed_short", 0)),
            "delta_blocked_primitive_disagreement": int(
                a.get("blocked_primitive_disagreement", 0) - b.get("blocked_primitive_disagreement", 0)
            ),
        }

    before = summarize(before_by_file)
    after = summarize(after_by_file)
    delta = {
        "armed_total": after["armed_total"] - before["armed_total"],
        "armed_long": after["armed_long"] - before["armed_long"],
        "armed_short": after["armed_short"] - before["armed_short"],
        "blocked_total": after["blocked_total"] - before["blocked_total"],
        "blocked_primitive_disagreement": after["blocked_primitive_disagreement"] - before["blocked_primitive_disagreement"],
        "short_share": after["short_share"] - before["short_share"],
    }

    out = {
        "glob": args.glob,
        "bucket_sec": int(args.bucket_sec),
        "before_abs_mnorm": before,
        "after_signed_mnorm": after,
        "delta_after_minus_before": delta,
        "by_file_delta": by_file_delta,
        "note": "Deterministic promotion-gate replay only (WATCH primitive contract), not full order placement simulation.",
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
