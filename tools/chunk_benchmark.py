#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import json
from collections import Counter
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any


TRACK_KINDS = {
    "ENTRY_GATE_EVAL",
    "ENTRY_ATTEMPT",
    "ENTRY_RESULT",
    "EXIT_RESULT",
    "OA_FORCE_CLOSE_TRIGGER",
    "AEE_TIMEBOX_EXIT",
    "AEE_STALL_EXIT",
    "AEE_DECAY_EXIT",
    "AEE_PANIC_EXIT",
}


def _parse_ts(v: Any) -> datetime | None:
    if not v:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _iter_events(log_glob: str):
    for fp in sorted(glob.glob(log_glob)):
        with open(fp, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                try:
                    o = json.loads(line)
                except Exception:
                    continue
                k = str(o.get("kind") or "").upper()
                if k not in TRACK_KINDS:
                    continue
                dt = _parse_ts(o.get("ts_utc"))
                if not dt:
                    continue
                yield dt, o


def _parse_state_key_core(s: str) -> dict[str, str]:
    s = str(s or "").strip()
    out: dict[str, str] = {"pair": "", "session": "", "weekday": "", "quarter": "", "regime": ""}
    if not s:
        return out
    if "pair=" in s:
        # key=value format
        for part in s.split("|"):
            if "=" not in part:
                continue
            k, v = part.split("=", 1)
            k = k.strip()
            v = v.strip()
            if k == "pair":
                out["pair"] = v
            elif k == "session":
                out["session"] = v
            elif k == "dow":
                out["weekday"] = v
            elif k == "quarter":
                out["quarter"] = v
            elif k == "regime":
                out["regime"] = v
        return out
    # compact fallback: PAIR|ENTRY|TYPE|SPEED|SESSION|Qx|Dow
    p = s.split("|")
    if len(p) >= 7:
        out["pair"] = p[0]
        out["session"] = p[4]
        out["quarter"] = p[5]
        out["weekday"] = p[6]
    return out


def _bucket_rows(rows: list[dict], hours: float) -> list[dict]:
    by_key: dict[tuple[str, str, str, str, str], list[dict]] = {}
    for r in rows:
        if (r.get("kind") or "").upper() != "EXIT_RESULT":
            continue
        core = _parse_state_key_core(r.get("state_key_core_str", ""))
        key = (
            core.get("pair", "") or str(r.get("pair") or ""),
            core.get("session", ""),
            core.get("weekday", ""),
            core.get("quarter", ""),
            core.get("regime", "") or "unknown",
        )
        by_key.setdefault(key, []).append(r)

    out = []
    for key, exits in by_key.items():
        pair, session, weekday, quarter, regime = key
        n = len(exits)
        pnl_atr = []
        pnl_pips = []
        mfe = []
        mae = []
        capture = []
        regions = Counter()
        extension_eligible = 0
        extension_killed = 0
        wins = 0
        for r in exits:
            try:
                pa = float(r.get("pnl_atr"))
                pnl_atr.append(pa)
                if pa > 0:
                    wins += 1
                if pa >= 1.0:
                    regions["EXTENSION"] += 1
                elif pa > 0.0:
                    regions["HARVEST"] += 1
                else:
                    regions["LOSS_CTRL"] += 1
            except Exception:
                pass
            try:
                pp = float(r.get("pnl_pips"))
                pnl_pips.append(pp)
            except Exception:
                pass
            try:
                mf = float(r.get("MFE_atr"))
                mfe.append(mf)
            except Exception:
                mf = 0.0
            try:
                ma = float(r.get("MAE_atr"))
                mae.append(ma)
            except Exception:
                pass
            try:
                pa = float(r.get("pnl_atr"))
                if mf > 0:
                    capture.append(pa / mf)
                if mf >= 1.0:
                    extension_eligible += 1
                    if pa < 1.0:
                        extension_killed += 1
            except Exception:
                pass
        avg_win = (sum(x for x in pnl_atr if x > 0) / max(1, wins)) if pnl_atr else 0.0
        losses = [x for x in pnl_atr if x <= 0]
        avg_loss = (sum(losses) / max(1, len(losses))) if pnl_atr else 0.0
        winrate = (wins / max(1, len(pnl_atr))) if pnl_atr else 0.0
        expected_extraction_atr = (winrate * avg_win) + ((1.0 - winrate) * avg_loss)
        exit_per_h = n / max(hours, 1e-9)
        # proxy ceiling while forward path is not available
        ceiling_x_atr_mean = (sum(mfe) / len(mfe)) if mfe else 0.0
        ceiling_capture_mean = (sum(capture) / len(capture)) if capture else 0.0
        # Proxy-first ceiling schema with stable horizon/x keys.
        def _proxy_ceiling_block(x_key: str) -> dict:
            return {
                "p_hit_plus_x_before_minus_y": None,
                "p_hit_minus_y_before_plus_x": None,
                "t_hit_plus_x_median_sec": None,
                "t_hit_minus_y_median_sec": None,
                "ceiling_x_atr_mean": round(ceiling_x_atr_mean, 6),
                "capture": {
                    "ceiling_capture_mean": round(ceiling_capture_mean, 6),
                    "ceiling_capture_median": round(median(capture), 6) if capture else 0.0,
                },
                "mode": "proxy_mfe_atr",
                "x_key": x_key,
            }

        out.append(
            {
                "key": {
                    "pair": pair,
                    "session": session,
                    "weekday": weekday,
                    "quarter": quarter,
                    "regime": regime,
                },
                "n": n,
                "entry_result_per_h": None,
                "exit_result_per_h": round(exit_per_h, 4),
                "expected_extraction_atr": round(expected_extraction_atr, 6),
                "pnl_atr_mean": round((sum(pnl_atr) / len(pnl_atr)) if pnl_atr else 0.0, 6),
                "pnl_pips_mean": round((sum(pnl_pips) / len(pnl_pips)) if pnl_pips else 0.0, 6),
                "avg_MFE_atr": round((sum(mfe) / len(mfe)) if mfe else 0.0, 6),
                "avg_MAE_atr": round((sum(mae) / len(mae)) if mae else 0.0, 6),
                "ceiling_capture_mean": round(ceiling_capture_mean, 6),
                "ratio_extension": round(regions["EXTENSION"] / max(1, n), 6),
                "ratio_harvest": round(regions["HARVEST"] / max(1, n), 6),
                "ratio_loss_ctrl": round(regions["LOSS_CTRL"] / max(1, n), 6),
                "avg_progress_close_in_harvest": round(
                    (sum(x for x in pnl_atr if 0.0 < x < 1.0) / max(1, sum(1 for x in pnl_atr if 0.0 < x < 1.0)))
                    if pnl_atr
                    else 0.0,
                    6,
                ),
                "extension_kill_rate": round(extension_killed / max(1, extension_eligible), 6),
                "ceiling": {
                    "h1800": {
                        "x_0p5": _proxy_ceiling_block("x_0p5"),
                        "x_1p0": _proxy_ceiling_block("x_1p0"),
                    },
                    "h3600": {
                        "x_0p5": _proxy_ceiling_block("x_0p5"),
                        "x_1p0": _proxy_ceiling_block("x_1p0"),
                    },
                },
                "guardrails": {
                    "throughput_ok": bool(exit_per_h >= 1.0),
                    "tail_ok": bool((sorted(pnl_atr)[max(0, int(0.05 * max(1, len(pnl_atr))) - 1)] if pnl_atr else 0.0) >= -0.15),
                },
            }
        )
    out.sort(key=lambda r: (r["expected_extraction_atr"], -r["n"]))
    return out


def _bucket_deltas(prev_rows: list[dict], curr_rows: list[dict], top_n: int = 25) -> tuple[list[dict], list[dict]]:
    def _cap(row: dict, horizon_key: str, x_key: str) -> float:
        try:
            c0 = row.get("ceiling", {}) or {}
            c1 = c0.get(horizon_key, {}) or {}
            c2 = c1.get(x_key, {}) or {}
            c3 = c2.get("capture", {}) or {}
            return float(c3.get("ceiling_capture_mean", 0.0) or 0.0)
        except Exception:
            return 0.0

    def idx(rows: list[dict]) -> dict[tuple[str, str, str, str, str], dict]:
        out = {}
        for r in rows:
            k = r["key"]
            key = (k["pair"], k["session"], k["weekday"], k["quarter"], k["regime"])
            out[key] = r
        return out

    p = idx(prev_rows)
    c = idx(curr_rows)
    keys = set(p) | set(c)
    deltas = []
    for k in keys:
        pr = p.get(k, {})
        cr = c.get(k, {})
        prev_cap_h1800_x1 = _cap(pr, "h1800", "x_1p0")
        curr_cap_h1800_x1 = _cap(cr, "h1800", "x_1p0")
        prev_cap_h3600_x1 = _cap(pr, "h3600", "x_1p0")
        curr_cap_h3600_x1 = _cap(cr, "h3600", "x_1p0")
        deltas.append(
            {
                "key": {
                    "pair": k[0],
                    "session": k[1],
                    "weekday": k[2],
                    "quarter": k[3],
                    "regime": k[4],
                },
                "delta_expected_extraction_atr": round(float(cr.get("expected_extraction_atr", 0.0)) - float(pr.get("expected_extraction_atr", 0.0)), 6),
                "delta_ceiling_capture_mean": round(float(cr.get("ceiling_capture_mean", 0.0)) - float(pr.get("ceiling_capture_mean", 0.0)), 6),
                "delta_ceiling_capture_mean_h1800_x_1p0": round(curr_cap_h1800_x1 - prev_cap_h1800_x1, 6),
                "delta_ceiling_capture_mean_h3600_x_1p0": round(curr_cap_h3600_x1 - prev_cap_h3600_x1, 6),
                "delta_exit_result_per_h": round(float(cr.get("exit_result_per_h", 0.0)) - float(pr.get("exit_result_per_h", 0.0)), 6),
                "prev_n": int(pr.get("n", 0)),
                "curr_n": int(cr.get("n", 0)),
            }
        )
    top = sorted(deltas, key=lambda x: (x["delta_expected_extraction_atr"], x["delta_ceiling_capture_mean"]), reverse=True)[:top_n]
    worst = sorted(deltas, key=lambda x: (x["delta_expected_extraction_atr"], x["delta_ceiling_capture_mean"]))[:top_n]
    return top, worst


def _summarize(rows: list[dict], hours: float) -> dict:
    c = Counter((r.get("kind") or "").upper() for r in rows)
    exits = [r for r in rows if (r.get("kind") or "").upper() == "EXIT_RESULT"]
    pnl_atr = []
    pnl_pips = []
    mfe_atr = []
    mae_atr = []
    capture_ratio_atr = []
    for r in exits:
        try:
            pnl_atr.append(float(r.get("pnl_atr")))
        except Exception:
            pass
        try:
            pnl_pips.append(float(r.get("pnl_pips")))
        except Exception:
            pass
        try:
            x = float(r.get("MFE_atr"))
            mfe_atr.append(x)
        except Exception:
            pass
        try:
            mae_atr.append(float(r.get("MAE_atr")))
        except Exception:
            pass
        try:
            pa = float(r.get("pnl_atr"))
            mf = float(r.get("MFE_atr"))
            if mf > 0:
                capture_ratio_atr.append(pa / mf)
        except Exception:
            pass
    wins = sum(1 for x in pnl_atr if x > 0)
    avg_win = (sum(x for x in pnl_atr if x > 0) / max(1, wins)) if pnl_atr else 0.0
    losses = [x for x in pnl_atr if x <= 0]
    avg_loss = (sum(losses) / max(1, len(losses))) if pnl_atr else 0.0
    winrate = (wins / max(1, len(pnl_atr))) if pnl_atr else 0.0
    expected_extraction_atr = (winrate * avg_win) + ((1.0 - winrate) * avg_loss)
    entry_gate = c.get("ENTRY_GATE_EVAL", 0)
    entry_result = c.get("ENTRY_RESULT", 0)
    exit_result = c.get("EXIT_RESULT", 0)
    entry_from_gate = (entry_result / entry_gate) if entry_gate > 0 else None
    exit_from_entry = (exit_result / entry_result) if entry_result > 0 else None
    return {
        "counts": dict(c),
        "rates_per_hour": {
            "entry_result_per_h": round(c.get("ENTRY_RESULT", 0) / max(hours, 1e-9), 3),
            "exit_result_per_h": round(c.get("EXIT_RESULT", 0) / max(hours, 1e-9), 3),
            "oa_trigger_per_h": round(c.get("OA_FORCE_CLOSE_TRIGGER", 0) / max(hours, 1e-9), 3),
        },
        "conversion": {
            "entry_from_gate": round(entry_from_gate, 4) if entry_from_gate is not None else None,
            "exit_from_entry": round(exit_from_entry, 4) if exit_from_entry is not None else None,
        },
        "quality": {
            "exit_n": len(exits),
            "winrate_pnl_atr_gt_0": round(winrate, 4),
            "pnl_atr_mean": round((sum(pnl_atr) / len(pnl_atr)) if pnl_atr else 0.0, 6),
            "pnl_atr_median": round(median(pnl_atr), 6) if pnl_atr else 0.0,
            "expected_extraction_atr": round(expected_extraction_atr, 6),
        },
        "extraction": {
            "pnl_pips_mean": round((sum(pnl_pips) / len(pnl_pips)) if pnl_pips else 0.0, 6),
            "pnl_pips_median": round(median(pnl_pips), 6) if pnl_pips else 0.0,
            "avg_MFE_atr": round((sum(mfe_atr) / len(mfe_atr)) if mfe_atr else 0.0, 6),
            "avg_MAE_atr": round((sum(mae_atr) / len(mae_atr)) if mae_atr else 0.0, 6),
            "capture_ratio_atr_mean": round((sum(capture_ratio_atr) / len(capture_ratio_atr)) if capture_ratio_atr else 0.0, 6),
        },
        # Capital layer requires units/pip value in EXIT_RESULT or join against trade store.
        "capital": {
            "weighted_metrics_available": False,
            "reason": "EXIT_RESULT currently has no units field",
        }
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--log-glob", default="logs/trades.jsonl*")
    ap.add_argument("--hours", type=float, default=1.0, help="window size ending now")
    ap.add_argument("--prev-hours", type=float, default=1.0, help="previous comparison window size")
    ap.add_argument("--end-utc", default="", help="window end timestamp (ISO-8601). Default now.")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    if args.end_utc:
        parsed = _parse_ts(args.end_utc)
        if parsed is None:
            raise SystemExit(f"invalid --end-utc: {args.end_utc}")
        now = parsed
    else:
        now = datetime.now(timezone.utc)
    curr_start = now - timedelta(hours=max(args.hours, 0.01))
    prev_end = curr_start
    prev_start = prev_end - timedelta(hours=max(args.prev_hours, 0.01))

    prev_rows: list[dict] = []
    curr_rows: list[dict] = []
    for dt, o in _iter_events(args.log_glob):
        if prev_start <= dt < prev_end:
            prev_rows.append(o)
        elif curr_start <= dt <= now:
            curr_rows.append(o)

    prev = _summarize(prev_rows, args.prev_hours)
    curr = _summarize(curr_rows, args.hours)
    prev_bucket_rows = _bucket_rows(prev_rows, args.prev_hours)
    curr_bucket_rows = _bucket_rows(curr_rows, args.hours)
    top_improved, worst_regressions = _bucket_deltas(prev_bucket_rows, curr_bucket_rows, top_n=25)

    def _avg_bucket_ceiling(rows: list[dict], horizon_key: str, x_key: str) -> float:
        if not rows:
            return 0.0
        vals = []
        for r in rows:
            try:
                c0 = r.get("ceiling", {}) or {}
                c1 = c0.get(horizon_key, {}) or {}
                c2 = c1.get(x_key, {}) or {}
                c3 = c2.get("capture", {}) or {}
                v = c3.get("ceiling_capture_mean", 0.0)
                vals.append(float(v))
            except Exception:
                vals.append(0.0)
        return sum(vals) / max(1, len(vals))

    out = {
        "windows_utc": {
            "prev_start": prev_start.isoformat(),
            "prev_end": prev_end.isoformat(),
            "curr_start": curr_start.isoformat(),
            "curr_end": now.isoformat(),
        },
        "prev": prev,
        "curr": curr,
        "delta": {
            "entry_result_per_h": round(curr["rates_per_hour"]["entry_result_per_h"] - prev["rates_per_hour"]["entry_result_per_h"], 3),
            "exit_result_per_h": round(curr["rates_per_hour"]["exit_result_per_h"] - prev["rates_per_hour"]["exit_result_per_h"], 3),
            "exit_from_entry": (
                round(curr["conversion"]["exit_from_entry"] - prev["conversion"]["exit_from_entry"], 4)
                if (curr["conversion"]["exit_from_entry"] is not None and prev["conversion"]["exit_from_entry"] is not None)
                else None
            ),
            "pnl_atr_mean": round(curr["quality"]["pnl_atr_mean"] - prev["quality"]["pnl_atr_mean"], 6),
            "winrate": round(curr["quality"]["winrate_pnl_atr_gt_0"] - prev["quality"]["winrate_pnl_atr_gt_0"], 4),
        },
        "bucket_stats": {
            "prev": prev_bucket_rows,
            "curr": curr_bucket_rows,
            "delta_top_improved": top_improved,
            "delta_worst_regressions": worst_regressions,
        },
        "ceiling": {
            "spec": {
                "mode": "proxy_mfe_atr_until_price_path_connected",
                "granularity": "tick_or_1s",
                "x_atr_levels": [0.5, 1.0],
                "y_atr_stop": 0.5,
                "horizons_sec": [1800, 3600],
                "anchor": "entry_ts",
                "price_series": "mid",
                "atr_source": "atr_exec",
            },
            "prev": {
                "ceiling_capture_mean": round((sum(r.get("ceiling_capture_mean", 0.0) for r in prev_bucket_rows) / len(prev_bucket_rows)) if prev_bucket_rows else 0.0, 6),
                "ceiling_capture_mean_h1800_x_1p0": round(_avg_bucket_ceiling(prev_bucket_rows, "h1800", "x_1p0"), 6),
                "ceiling_capture_mean_h3600_x_1p0": round(_avg_bucket_ceiling(prev_bucket_rows, "h3600", "x_1p0"), 6),
                "bucket_count": len(prev_bucket_rows),
            },
            "curr": {
                "ceiling_capture_mean": round((sum(r.get("ceiling_capture_mean", 0.0) for r in curr_bucket_rows) / len(curr_bucket_rows)) if curr_bucket_rows else 0.0, 6),
                "ceiling_capture_mean_h1800_x_1p0": round(_avg_bucket_ceiling(curr_bucket_rows, "h1800", "x_1p0"), 6),
                "ceiling_capture_mean_h3600_x_1p0": round(_avg_bucket_ceiling(curr_bucket_rows, "h3600", "x_1p0"), 6),
                "bucket_count": len(curr_bucket_rows),
            },
        },
    }

    text = json.dumps(out, indent=2)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text)
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
