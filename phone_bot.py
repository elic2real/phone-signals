# === Whipsaw capture constants (module scope) ===
WHIP_PEAK_MIN_ATR = 0.45
WHIP_GIVEBACK_ATR = 0.18
WHIP_CONFIRM_HITS = 2
WHIP_CONFIRM_WINDOW_SEC = 1.0
WHIP_MIN_AGE_SEC = 3.0
# --- STUBS FOR LOGGING AND MISSING CONFIG (for batch proof only) ---
def log_runtime(*a, **k):
    pass
def log_metrics(*a, **k):
    pass
def log_trade_event(*a, **k):
    pass
DEFAULT_OANDA_API_KEY = "stub"
DEFAULT_OANDA_ACCOUNT_ID = "stub"
DEFAULT_OANDA_ENV = "practice"
from dataclasses import dataclass, field

# === MODE PROOF INSTRUMENTATION ===
AEE_MODE_COUNTS = {"WHIPSAW": 0, "NORMAL": 0}
AEE_EXTENSION_COUNT = 0
AEE_BLOCKED_RUNNER_EXIT_COUNT = 0
AEE_SCENARIO_SEEN = {}
from typing import Any, Optional, Dict, List, Tuple
from pathlib import Path
def ffloat(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default
#!/usr/bin/env python3
import os
import sys
import json
import time
import math
import gzip
import shutil
import signal
import random
import sqlite3
import threading
import statistics
import subprocess

from collections import deque
from dataclasses import dataclass, field
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP
from enum import Enum
from dataclasses import dataclass, field

@dataclass
class AEEState:
        meta = _fallback_instrument_meta(pair)
        log_throttled(
            f"sizing_meta_cache_miss:{normalize_pair(pair)}:displayPrecision",
            "SIZING_META_CACHE_MISS_FALLBACK",
            {"pair": pair, "field": "displayPrecision"},
            min_interval=300.0,
        )
    return Decimal(10) ** Decimal(-int(meta["displayPrecision"]))


def pip_size(pair: str) -> Decimal:
    meta = get_instrument_meta_cached(pair)
    if meta is None:
        meta = _fallback_instrument_meta(pair)
        log_throttled(
            f"sizing_meta_cache_miss:{normalize_pair(pair)}:pipLocation",
            "SIZING_META_CACHE_MISS_FALLBACK",
            {"pair": pair, "field": "pipLocation"},
            min_interval=300.0,
        )
    return Decimal(10) ** Decimal(int(meta["pipLocation"]))


def to_pips(pair: str, value: float) -> float:
    pip = pip_size(pair)
    return float(Decimal(str(value)) / pip)


def round_tick_down(price: float, pair: str) -> float:
    tick = tick_size(pair)
    d = Decimal(str(price))
    return float((d / tick).to_integral_value(rounding=ROUND_FLOOR) * tick)


def round_tick_up(price: float, pair: str) -> float:
    tick = tick_size(pair)
    d = Decimal(str(price))
    return float((d / tick).to_integral_value(rounding=ROUND_CEILING) * tick)


def round_tick(price: float, pair: str) -> float:
    tick = tick_size(pair)
    d = Decimal(str(price))
    return float((d / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick)


def _run_test_sizing_and_exit() -> None:
    import json
    def _meta_test_fixture(pair: str) -> dict:
        return {
            "tradeUnitsPrecision": 0,
            "minimumTradeSize": 1000,
            "marginRate": 0.0333,
        }
    globals()["get_instrument_meta"] = _meta_test_fixture
    for confidence in (0.2, 0.6, 0.95):
        for spread_mult in (1.0, 0.5):
            units_main, units_runner, dbg = compute_units_recycling(
                pair="EUR_USD",
                direction="LONG",
                price=1.1000,
                margin_available=10000.0,
                margin_rate=float(_meta_test_fixture("EUR_USD")["marginRate"]),
                confidence=confidence,
                spread_mult=spread_mult,
                base_deploy_frac=0.10,
            )
            line = json.dumps(dbg, sort_keys=True)
            log("DEBUG_LINE", {"line": line})
            log("DEBUG_LINE_STDERR", {"line": line})
    raise SystemExit(0)


_RUN_TEST_SIZING = "--test-sizing" in sys.argv

_LOG_THROTTLE_TS: Dict[str, float] = {}


def log_throttled(key, event, meta=None, min_interval: float = 30.0):
    now = now_ts()
    k = str(key or "")
    last = float(_LOG_THROTTLE_TS.get(k, 0.0))
    if (now - last) < float(min_interval):
        return
    _LOG_THROTTLE_TS[k] = now
    log_runtime("info", f"THROTTLED: {event}", **(meta or {}))

MTF_COORDINATION_ENABLED = False

# ATR alias for legacy code
# (moved below compute_atr_pips definition)

# --- Bucket B Helpers ---
def clamp(val, lo, hi):
    return max(lo, min(hi, val))

def pair_tag(pair, direction=None):
    tag = str(pair or "")
    if direction:
        tag += f"_{direction}"
    return tag

# ===== LEGACY CONSTANTS - DO NOT USE FOR NEW GATES =====

# --- AdaptiveCadence default adapter (Bucket A) ---
class AdaptiveCadence:
    def get_interval(self, key):
        return 15.0

# Logging function alias
def log(event, meta=None):
    meta = dict(meta or {})
    log_runtime("info", event, **meta)

# Notification adapter
def notify(event, message):
    event_s = str(event or "").strip()
    message_s = str(message or "").strip()
    full_msg = f"{event_s} - {message_s}".strip(" -")
    log_runtime("info", f"NOTIFY: {full_msg}")

    if not ALERT_SYSTEM_ENABLED:
        return

    # Best-effort local OS notification first.
    try:
        title = "Phone Bot Alert"
        if shutil.which("termux-notification"):
            subprocess.run(
                ["termux-notification", "--title", title, "--content", full_msg, "--priority", "high"],
                check=False,
                capture_output=True,
                text=True,
            )
            if shutil.which("termux-vibrate"):
                subprocess.run(["termux-vibrate", "-f", "-d", "250"], check=False, capture_output=True, text=True)
        elif shutil.which("notify-send"):
            subprocess.run(
                ["notify-send", "-u", "critical", title, full_msg],
                check=False,
                capture_output=True,
                text=True,
            )
        elif sys.platform == "darwin":
            escaped_msg = full_msg.replace('"', '\\"')
            script = f'display notification "{escaped_msg}" with title "{title}"'
            subprocess.run(["osascript", "-e", script], check=False, capture_output=True, text=True)
        elif sys.platform.startswith("win"):
            ps = (
                "[reflection.assembly]::loadwithpartialname('System.Windows.Forms') | Out-Null; "
                f"[System.Windows.Forms.MessageBox]::Show('{full_msg}','{title}')"
            )
            subprocess.run(["powershell", "-Command", ps], check=False, capture_output=True, text=True)
    except Exception as e:
        log_runtime("warning", "NOTIFY_LOCAL_DISPATCH_FAILED", error=str(e), event=event_s)

    # Forward to webhook/push channels when configured (best-effort).
    try:
        if "send_webhook_notification" in globals():
            send_webhook_notification(
                "notify",
                {"event": event_s, "message": message_s},
                priority="high",
            )
    except Exception as e:
        log_runtime("warning", "NOTIFY_WEBHOOK_DISPATCH_FAILED", error=str(e), event=event_s)
    try:
        if "send_push_notification" in globals():
            send_push_notification(
                f"Phone Bot: {event_s}"[:128],
                message_s[:1000],
                priority="high",
            )
    except Exception as e:
        log_runtime("warning", "NOTIFY_PUSH_DISPATCH_FAILED", error=str(e), event=event_s)

# Price validation helper
def is_valid_price(p):
    return isinstance(p, (int, float)) and math.isfinite(p) and p != 0

# Safe float helper
def _safe_float(val):
    try:
        return float(val)
    except Exception:
        return 0.0



try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

# Startup initialization function
def initialize_bot():
    """Initialize runtime singletons from environment and fail fast on invalid setup."""
    global _RUNTIME_OANDA, _RUNTIME_DB, _RUNTIME_HUB
    global db, o, enhanced_market_hub, market_hub, INSTR_META, INSTR_META_TS

    if load_dotenv is not None:
        load_dotenv()

    api_key = str(os.getenv("OANDA_API_KEY", DEFAULT_OANDA_API_KEY)).strip()
    account_id = str(os.getenv("OANDA_ACCOUNT_ID", DEFAULT_OANDA_ACCOUNT_ID)).strip()
    env_raw = str(os.getenv("OANDA_ENV", DEFAULT_OANDA_ENV)).strip()
    env = normalize_oanda_env(env_raw) or "practice"
    if not api_key or not account_id:
        raise RuntimeError("initialize_bot: missing OANDA_API_KEY or OANDA_ACCOUNT_ID")

    _RUNTIME_OANDA = OandaClient(api_key=api_key, account_id=account_id, env=env)
    o = _RUNTIME_OANDA

    db_path = str(globals().get("DB_PATH") or (Path.cwd() / "phone_bot.db"))
    _RUNTIME_DB = DB(db_path)
    db = _RUNTIME_DB

    if "EnhancedMarketDataHub" not in globals():
        raise RuntimeError("initialize_bot: EnhancedMarketDataHub class not defined")
    pairs = globals().get("PAIRS") or ["EUR_USD"]
    if not isinstance(INSTR_META, dict):
        INSTR_META = {}
    for pair in pairs:
        p = normalize_pair(pair)
        if p not in INSTR_META:
            INSTR_META[p] = _fallback_instrument_meta(p)
    INSTR_META_TS = now_ts()
    _RUNTIME_HUB = EnhancedMarketDataHub()
    enhanced_market_hub = _RUNTIME_HUB
    market_hub = _RUNTIME_HUB

    return {"ok": True, "env": env, "account_id": account_id, "pairs": len(pairs), "db_path": db_path}

_RUNTIME_OANDA: Optional["OandaClient"] = None
_RUNTIME_DB: Any = None
_RUNTIME_HUB: Any = None


def get_oanda() -> "OandaClient":
    """Get the runtime OANDA client, raising a clear error if not initialized."""
    global _RUNTIME_OANDA
    if _RUNTIME_OANDA is None:
        raise RuntimeError("OANDA runtime not initialized (_RUNTIME_OANDA is None).")
    return _RUNTIME_OANDA


def _require_runtime_oanda() -> "OandaClient":
    if _RUNTIME_OANDA is None:
        raise RuntimeError("RUNTIME_NOT_INITIALIZED: OandaClient is not initialized. Call main() startup init before trading.")
    return _RUNTIME_OANDA


def _require_runtime_db():
    if _RUNTIME_DB is None:
        raise RuntimeError("RUNTIME_NOT_INITIALIZED: DB is not initialized. Call main() startup init before trading.")
    return _RUNTIME_DB


def _require_runtime_hub():
    if _RUNTIME_HUB is None:
        raise RuntimeError("RUNTIME_NOT_INITIALIZED: enhanced_market_hub is not initialized. Call main() startup init before trading.")
    return _RUNTIME_HUB


def db_call(label: str, fn, *args, **kwargs):
    _require_runtime_db()
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        log(f"{EMOJI_DB} DB_ERROR {label}", {"err": str(e)})
        return None


def oanda_call(label: str, fn, *args, allow_error_dict: bool = False, max_retries: int = 3, budget_aware: bool = False, **kwargs):
    """Canonical OANDA boundary wrapper with retry/backoff; requires initialized runtime.
    
    Args:
        label: Call label for logging
        fn: OANDA client function to call
        *args: Arguments to pass to fn
        allow_error_dict: If True, return error dicts instead of raising
        max_retries: Maximum retry attempts
        budget_aware: If True, check API budget and track calls
        **kwargs: Keyword arguments to pass to fn
    """
    _require_runtime_oanda()

    # Budget gate BEFORE calling OANDA (if enabled)
    if budget_aware:
        if not _check_api_budget():
            return {"ok": False, "blocked": True, "status": None, "err_head": "api_budget_exhausted", "value": None}
        # Track the call (we are about to spend budget)
        _track_api_call()

    def _parse_http_status(msg: str) -> Optional[int]:
        if "http_" not in msg:
            return None
        try:
            tail = msg.split("http_")[1]
            digits = ""
            for ch in tail:
                if ch.isdigit():
                    digits += ch
                else:
                    break
            return int(digits) if digits else None
        except Exception:
            return None

    def _is_transient_exc(e: Exception) -> bool:
        msg = str(e).lower()
        if "rate_limited" in msg or "429" in msg:
            return True
        if "timeout" in msg or "timed out" in msg or "connection" in msg or "tempor" in msg or "network" in msg:
            return True
        if "dns" in msg or "chunked" in msg or "proxy" in msg or "ssl" in msg:
            return True
        st = _parse_http_status(msg)
        if st and st in (408, 425, 429, 500, 502, 503, 504):
            return True
        # ...existing code...
        return False

    def _is_transient_resp(res: dict) -> bool:
        if res.get("_rate_limited"):
            return True
        if res.get("_json_error"):
            return True
        if res.get("_http_error"):
            st = res.get("_status")
            if isinstance(st, int) and st in (408, 425, 429, 500, 502, 503, 504):
                return True
        return False

    for attempt in range(int(max_retries) + 1):
        try:
            res = fn(*args, **kwargs)
        except Exception as e:
            transient = _is_transient_exc(e)
            if transient and attempt < max_retries:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                log(f"{EMOJI_WARN} RETRY {label}", {"wait": wait_time, "attempt": attempt + 1, "err": str(e)})
                time.sleep(wait_time)
                continue
            if allow_error_dict:
                return {"_exception": True, "_err": str(e)}
            log_throttled(
                f"http_err:{label}",
                f"{EMOJI_ERR} HTTP_ERROR {label}",
                {"err": str(e), "label": label},
            )
            return None

        if isinstance(res, dict) and _is_transient_resp(res) and attempt < max_retries:
            wait_time = res.get("_retry_after") if isinstance(res.get("_retry_after"), (int, float)) else (2 ** attempt) + random.uniform(0, 1)
            log(f"{EMOJI_WARN} RETRY_RESP {label}", {"wait": wait_time, "attempt": attempt + 1, "resp": res})
            time.sleep(float(wait_time) if wait_time else 0.0)
            continue

        if budget_aware:
            if res is None:
                return {"ok": False, "blocked": False, "status": None, "err_head": "none_return", "value": None}
            if isinstance(res, dict):
                if res.get("_exception"):
                    err = str(res.get("_err") or "exception")
                    return {"ok": False, "blocked": False, "status": None, "err_head": err[:200], "value": res}
                if res.get("_http_error") or res.get("_json_error") or res.get("_rate_limited"):
                    st = res.get("_status") if isinstance(res.get("_status"), int) else None
                    return {"ok": False, "blocked": False, "status": st, "err_head": str(res)[:200], "value": res}
            return {"ok": True, "blocked": False, "status": None, "err_head": None, "value": res}

        return res

    return None


# ===============================
# UNIFIED FEATURE PIPELINE (FeatureSet)
# ===============================

@dataclass
class FeatureSet:
    pair: str
    ts: float
    bid: float
    ask: float
    mid: float
    spread_pips: float
    atr_m1_pips: float
    atr_m5_pips: float
    atr_m15_pips: float
    atr_h1_pips: float
    atr_h4_pips: float
    v_scalar: float
    vol_slope_m1: float
    wr_m5: float
    wr_m15: float
    mnorm_m1: float
    mnorm_m5: float
    volz_m1: float
    volz_m5: float
    path_10s: dict = field(default_factory=dict)
    path_30s: dict = field(default_factory=dict)
    path_60s: dict = field(default_factory=dict)
    book: dict = field(default_factory=dict)
    ok: bool = True
    reason: str = ""

def compute_features(pair, pricing_stream, candle_cache, book_cache):
    ts = time.time()
    tick = pricing_stream.get_latest_tick(pair)
    bid = tick.bid if tick else 0.0
    ask = tick.ask if tick else 0.0
    mid = tick.mid if tick else 0.0
    spread_pips = tick.spread_pips if tick else 0.0
    # ATRs
    m1 = candle_cache.get((pair, "M1"), [])
    m5 = candle_cache.get((pair, "M5"), [])
    m15 = candle_cache.get((pair, "M15"), [])
    h1 = candle_cache.get((pair, "H1"), [])
    h4 = candle_cache.get((pair, "H4"), [])
    atr_m1_price = compute_atr_price(m1, 14)
    atr_m5_price = compute_atr_price(m5, 14)
    _ = compute_atr_price(m15, 14)
    _ = compute_atr_price(h1, 14)
    _ = compute_atr_price(h4, 14)

    atr_m1_pips = compute_atr_pips(pair, m1, 14)
    atr_m5_pips = compute_atr_pips(pair, m5, 14)
    atr_m15_pips = compute_atr_pips(pair, m15, 14)
    atr_h1_pips = compute_atr_pips(pair, h1, 14)
    atr_h4_pips = compute_atr_pips(pair, h4, 14)
    v_scalar = atr_m1_pips / (compute_atr_pips(pair, m1, 100) or 1.0)
    vol_slope_m1 = compute_vol_slope(m1)
    wr_m5 = compute_wr(m5, 14)
    wr_m15 = compute_wr(m15, 14)
    mnorm_m1 = compute_mnorm(m1, 14, atr_m1_price)
    mnorm_m5 = compute_mnorm(m5, 14, atr_m5_price)
    volz_m1 = compute_volz(m1)
    volz_m5 = compute_volz(m5)
    # Path metrics
    path_10s = compute_path_metrics(pair, pricing_stream, 10)
    path_30s = compute_path_metrics(pair, pricing_stream, 30)
    path_60s = compute_path_metrics(pair, pricing_stream, 60)
    
    # Fail-closed gate for path metrics
    if not path_10s or not path_30s or not path_60s:
        log_runtime("warn", "FEATURES_INVALID_NO_PATH", pair=pair)
        return FeatureSet(
            pair=pair, ts=ts, bid=bid, ask=ask, mid=mid, spread_pips=spread_pips,
            atr_m1_pips=atr_m1_pips, atr_m5_pips=atr_m5_pips, atr_m15_pips=atr_m15_pips,
            atr_h1_pips=atr_h1_pips, atr_h4_pips=atr_h4_pips, v_scalar=v_scalar,
            vol_slope_m1=vol_slope_m1, wr_m5=wr_m5, wr_m15=wr_m15,
            mnorm_m1=mnorm_m1, mnorm_m5=mnorm_m5, volz_m1=volz_m1, volz_m5=volz_m5,
            path_10s={}, path_30s={}, path_60s={}, book=book_cache.get(pair, {}),
            ok=False, reason="no_path_metrics"
        )
    
    # Book features
    book = book_cache.get(pair, {})
    return FeatureSet(
        pair=pair,
        ts=ts,
        bid=bid,
        ask=ask,
        mid=mid,
        spread_pips=spread_pips,
        atr_m1_pips=atr_m1_pips,
        atr_m5_pips=atr_m5_pips,
        atr_m15_pips=atr_m15_pips,
        atr_h1_pips=atr_h1_pips,
        atr_h4_pips=atr_h4_pips,
        v_scalar=v_scalar,
        vol_slope_m1=vol_slope_m1,
        wr_m5=wr_m5,
        wr_m15=wr_m15,
        mnorm_m1=mnorm_m1,
        mnorm_m5=mnorm_m5,
        volz_m1=volz_m1,
        volz_m5=volz_m5,
        path_10s=path_10s,
        path_30s=path_30s,
        path_60s=path_60s,
        book=book,
        ok=True,
        reason=""
    )
    
# Indicator computation helpers (single source of truth)
def compute_atr_price(candles, n):
    if len(candles) < n:
        return 0.0
    def _px(candle: dict, primary: str, alt: str, fallback: float = 0.0) -> float:
        try:
            return float(candle.get(primary, candle.get(alt, fallback)))
        except Exception:
            return float(fallback)
    tr = []
    for i in range(1, n + 1):
        cur = candles[-i] if isinstance(candles[-i], dict) else {}
        h = _px(cur, "high", "h", 0.0)
        low = _px(cur, "low", "l", 0.0)
        if i < len(candles) and isinstance(candles[-i - 1], dict):
            prev = candles[-i - 1]
            c_prev = _px(prev, "close", "c", h)
        else:
            c_prev = h
        tr.append(max(h - low, abs(h - c_prev), abs(low - c_prev)))
    return sum(tr) / n


def compute_atr_pips(*args, **kwargs):
    """Compute ATR in pips.

    New form: compute_atr_pips(pair, candles, n)
    Legacy form: compute_atr_pips(candles, n) (assumes EUR_USD)
    """
    pair = "EUR_USD"
    candles = None
    n = None
    if len(args) == 3:
        pair, candles, n = args
    elif len(args) == 2:
        candles, n = args
    else:
        pair = kwargs.get("pair", pair)
        candles = kwargs.get("candles")
        n = kwargs.get("n")

    if not candles or not n:
        return 0.0
    atr_price = float(compute_atr_price(candles, int(n)))
    return atr_pips(normalize_pair(pair), atr_price)

# ATR alias for legacy code (must be after definition)
atr = compute_atr_pips

def compute_wr(candles, n):
    if len(candles) < n:
        return 0.0
    highs = [c["high"] for c in candles[-n:]]
    lows = [c["low"] for c in candles[-n:]]
    close = candles[-1]["close"]
    hh = max(highs)
    ll = min(lows)
    return -100 * (hh - close) / (hh - ll + 1e-8) if hh != ll else 0.0

def compute_mnorm(candles, n, atr):
    if len(candles) < n:
        return 0.0
    mom = candles[-1]["close"] - candles[-n]["close"]
    return mom / (atr or 1.0)

def compute_volz(candles):
    if len(candles) < 10:
        return 0.0
    vols = [c["volume"] for c in candles[-10:]]
    med = statistics.median(vols)
    mad = statistics.median([abs(v-med) for v in vols]) + 1e-8
    return (vols[-1] - med) / mad

def compute_vol_slope(candles):
    if len(candles) < 15:
        return 0.0
    atrs = [compute_atr_price(candles[-i:], 14) for i in range(1, 6)]
    return atrs[-1] - atrs[0] if len(atrs) == 5 else 0.0

def compute_path_metrics(pair, pricing_stream, window_s):
    ticks = pricing_stream.get_recent_ticks(pair, 100)
    now = time.time()
    cutoff = now - window_s
    window_ticks = [t for t in ticks if t.ts >= cutoff]
    if not window_ticks:
        return {}
    prices = [t.mid for t in window_ticks]
    signed_disp = (prices[-1] - prices[0]) if len(prices) > 1 else 0.0
    disp = abs(signed_disp)
    path_len = sum(abs(prices[i] - prices[i-1]) for i in range(1, len(prices)))
    eff = disp / (path_len + 1e-8)
    overlap = path_len / (disp + 1e-8) if disp > 0 else 0.0
    speed = disp / (window_ticks[-1].spread_pips or 1.0)
    velocity = speed - (disp / (window_ticks[0].spread_pips or 1.0)) if len(window_ticks) > 1 else 0.0
    local_hi = max(prices)
    local_lo = min(prices)
    if prices[-1] >= prices[0]:
        pullback = max(0.0, local_hi - prices[-1])
    else:
        pullback = max(0.0, prices[-1] - local_lo)
    acceptance_time = sum(1 for p in prices if p > local_hi*0.98 or p < local_lo*1.02)
    return {
        "disp": disp,
        "signed_disp": signed_disp,
        "path_len": path_len,
        "eff": eff,
        "overlap": overlap,
        "speed": speed,
        "velocity": velocity,
        "pullback": pullback,
        "local_hi": local_hi,
        "local_lo": local_lo,
        "acceptance_time": acceptance_time
    }
try:
    import requests
    _HAS_REQUESTS = True
except Exception:
    requests = None
    _HAS_REQUESTS = False


class OandaClient:
    """Canonical OANDA API client (production self-contained).

    NOTE: Single source of truth for OANDA I/O methods used by this file.
    """

    def __init__(self, api_key: str, account_id: str, env: str = "practice"):
        self.api_key = str(api_key or "").strip()
        self.account_id = str(account_id or "").strip()
        self.env = str(env or "practice").strip()
        self.base = "https://api-fxpractice.oanda.com" if self.env == "practice" else "https://api-fxtrade.oanda.com"
        self._sess = None
        self._rate_limit_until = 0.0
        if _HAS_REQUESTS and requests is not None:
            self._sess = requests.Session()
            self._sess.headers.update(
                {
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "Accept-Datetime-Format": "RFC3339",
                }
            )

    def _err(self, **kw) -> dict:
        out = dict(kw)
        out.setdefault("_exception", False)
        out.setdefault("_http_error", False)
        out.setdefault("_json_error", False)
        out.setdefault("_rate_limited", False)
        return out

    def _request(self, method: str, path: str, *, params: Optional[dict] = None, body: Optional[dict] = None) -> dict:
        if not (_HAS_REQUESTS and self._sess is not None):
            return self._err(_exception=True, _error="requests_not_available")
        url = f"{self.base}{path}"
        try:
            resp = self._sess.request(method, url, params=params, json=body, timeout=15)
        except Exception as e:
            return self._err(_exception=True, _error=str(e))

        out = None
        try:
            out = resp.json()
        except Exception:
            out = {"_text": getattr(resp, "text", "")}
            out.update(self._err(_json_error=True))

        # Normalize error fields for oanda_call wrapper
        if isinstance(resp.status_code, int) and resp.status_code >= 400:
            out = out if isinstance(out, dict) else {"_text": str(out)}
            out.update(self._err(_http_error=True, _status=int(resp.status_code)))
            if resp.status_code == 429:
                out["_rate_limited"] = "1"
                retry_after = 2.0
                try:
                    raw_ra = resp.headers.get("Retry-After")
                    if raw_ra is not None:
                        retry_after = float(raw_ra)
                except Exception:
                    retry_after = 2.0
                self._rate_limit_until = max(self._rate_limit_until, now_ts() + max(0.5, retry_after))
        return out if isinstance(out, dict) else {"_text": str(out)}

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        return self._request("GET", path, params=params)

    def _post(self, path: str, body: dict) -> dict:
        return self._request("POST", path, body=body)

    def _put(self, path: str, body: dict) -> dict:
        return self._request("PUT", path, body=body)

    # --- Core endpoints used by this file ---
    def pricing(self, pair: str) -> Tuple[float, float]:
        resp = self._get(
            f"/v3/accounts/{self.account_id}/pricing",
            params={"instruments": normalize_pair(pair)},
        )
        prices = resp.get("prices") if isinstance(resp, dict) else None
        if isinstance(prices, list) and prices:
            p0 = prices[0]
            bid = p0.get("bid")
            ask = p0.get("ask")
            if bid is None or ask is None:
                bids = p0.get("bids")
                asks = p0.get("asks")
                if isinstance(bids, list) and bids:
                    bid = bids[0].get("price")
                if isinstance(asks, list) and asks:
                    ask = asks[0].get("price")
            if bid is None or ask is None:
                raise RuntimeError(f"pricing_missing_bid_ask:{pair}:{p0}")
            return float(bid), float(ask)
        raise RuntimeError(f"No pricing for {pair}")

    def pricing_multi(self, pairs: List[str]) -> Dict[str, Tuple[float, float, float]]:
        instruments = ",".join([normalize_pair(p) for p in pairs])
        resp = self._get(
            f"/v3/accounts/{self.account_id}/pricing",
            params={"instruments": instruments},
        )
        if isinstance(resp, dict) and (
            resp.get("_http_error") or resp.get("_json_error") or resp.get("_exception")
        ):
            raise RuntimeError(
                f"pricing_multi_error status={resp.get('_status')} err={resp.get('_error') or resp.get('errorMessage') or resp.get('message') or 'unknown'}"
            )
        out: Dict[str, Tuple[float, float, float]] = {}
        for p in resp.get("prices", []) if isinstance(resp, dict) else []:
            try:
                bid = p.get("bid")
                ask = p.get("ask")
                if bid is None or ask is None:
                    bids = p.get("bids")
                    asks = p.get("asks")
                    if isinstance(bids, list) and bids:
                        bid = bids[0].get("price")
                    if isinstance(asks, list) and asks:
                        ask = asks[0].get("price")
                if bid is None or ask is None:
                    continue
                # Extract and normalize broker timestamp
                raw_time = p.get("time")
                broker_ts = parse_time_oanda(raw_time)
                recv_ts = time.time()
                ts = broker_ts if (math.isfinite(broker_ts) and broker_ts > 0.0) else recv_ts
                out[normalize_pair(p.get("instrument"))] = (float(bid), float(ask), float(ts))
            except Exception:
                continue
        return out

    def candles(self, instrument: str, granularity: str, count: int = 500, *, price: str = "M") -> List[dict]:
        params = {"granularity": granularity, "count": int(count), "price": price}
        resp = self._get(f"/v3/instruments/{normalize_pair(instrument)}/candles", params=params)
        if isinstance(resp, dict) and (
            resp.get("_http_error") or resp.get("_json_error") or resp.get("_exception")
        ):
            raise RuntimeError(
                f"candles_error instrument={normalize_pair(instrument)} gran={granularity} status={resp.get('_status')} err={resp.get('_error') or resp.get('errorMessage') or resp.get('message') or 'unknown'}"
            )
        return resp.get("candles", []) if isinstance(resp, dict) else []

    def account_summary(self) -> dict:
        return self._get(f"/v3/accounts/{self.account_id}/summary")

    def open_positions(self) -> dict:
        return self._get(f"/v3/accounts/{self.account_id}/openPositions")

    def pending_orders(self, params: Optional[dict] = None) -> dict:
        p = dict(params or {})
        p.setdefault("state", "PENDING")
        return self._get(f"/v3/accounts/{self.account_id}/orders", params=p)

    def close_position(self, instrument: str, side: str = "long", units: str = "ALL", **body) -> dict:
        # Accept both legacy (side, units) and caller passing longUnits/shortUnits.
        if body:
            payload = body
        else:
            payload = {"longUnits": "0", "shortUnits": "0"}
            if str(side).lower().startswith("l"):
                payload["longUnits"] = units
            else:
                payload["shortUnits"] = units
        return self._put(f"/v3/accounts/{self.account_id}/positions/{normalize_pair(instrument)}/close", payload)

    def set_trade_stop_loss(self, trade_id: str, price: float) -> dict:
        body = {"stopLoss": {"price": str(price)}}
        return self._put(f"/v3/accounts/{self.account_id}/trades/{trade_id}/orders", body)

    def place_market(self, instrument: str, units: int, sl_price: float, tp_price: float, *, client_id: str = "") -> dict:
        # stopLossOnFill + takeProfitOnFill are required by SOP.
        order: dict = {
            "type": "MARKET",
            "instrument": normalize_pair(instrument),
            "units": str(int(units)),
            "timeInForce": "FOK",
            "positionFill": "DEFAULT",
            "takeProfitOnFill": {"price": str(tp_price)},
            "stopLossOnFill": {"price": str(sl_price)},
        }
        if client_id:
            order["clientExtensions"] = {"id": str(client_id)[:32]}
        return self._post(f"/v3/accounts/{self.account_id}/orders", {"order": order})

    def order_book(self, instrument: str, *, bucket_width: Optional[float] = None, time: Optional[str] = None) -> dict:
        params: dict = {}
        if bucket_width is not None:
            params["bucketWidth"] = str(bucket_width)
        if time is not None:
            params["time"] = str(time)
        return self._get(f"/v3/instruments/{normalize_pair(instrument)}/orderBook", params=params or None)

    def position_book(self, instrument: str, *, bucket_width: Optional[float] = None, time: Optional[str] = None) -> dict:
        params: dict = {}
        if bucket_width is not None:
            params["bucketWidth"] = str(bucket_width)
        if time is not None:
            params["time"] = str(time)
        return self._get(f"/v3/instruments/{normalize_pair(instrument)}/positionBook", params=params or None)

# ============================================================================
# DATA QUALITY STATES - Must be defined before use
# ============================================================================
class DataQuality:
    OK = "OK"
    DEGRADED = "DEGRADED"
    BAD = "BAD"

# ============================================================================
# PATH-SPACE ENGINE - V12 LOCKED - Single Source of Truth
# ============================================================================

@dataclass
class PathSpaceState:
    """Canonical path-space state for a single instrument"""
    # Core price tracking
    current_price: float = 0.0
    entry_price: Optional[float] = None
    direction: Optional[str] = None  # LONG/SHORT
    
    # Path-space primitives (canonical calculations only)
    path_len: float = 0.0  # Σ |ΔPᵢ| over window
    displacement: float = 0.0  # |P - Entry| or |P - P_start|
    efficiency: float = 0.0  # |ΔP| / PathLen
    overlap: float = 0.0  # PathLen / |ΔP| (churn proxy)
    progress: float = 0.0  # |P - Entry| / ATR
    
    # Energy and momentum
    energy: float = 0.0  # |P - P_k| / ATR (k = reference point)
    speed: float = 0.0  # |ΔP_recent| / ATR over window
    velocity: float = 0.0  # Speed_now - Speed_prev
    
    # Extrema tracking
    rolling_high: float = float('-inf')
    rolling_low: float = float('inf')
    pullback: float = 0.0  # Retrace from rolling extrema / ATR
    
    # Time and persistence
    last_update: float = 0.0
    time_at_level: float = 0.0  # TimeSpent(level)
    
    # Volatility
    atr: float = 0.0  # Noise scale from ATR_M1(14)
    vol_slope: float = 0.0  # ΔATR_M1 (volatility slope)
    
    # History windows
    price_history: deque = field(default_factory=lambda: deque(maxlen=100))
    displacement_history: deque = field(default_factory=lambda: deque(maxlen=20))
    speed_history: deque = field(default_factory=lambda: deque(maxlen=10))
    atr_history: deque = field(default_factory=lambda: deque(maxlen=15))

class PathSpaceEngine:
    """Canonical engine for path-space primitive calculations"""
    
    def __init__(self, window_seconds: int = 300):
        self.window_seconds = window_seconds
        self.states: Dict[str, PathSpaceState] = {}
    
    def update_price(self, pair: str, price: float, atr: float, timestamp: Optional[float] = None) -> PathSpaceState:
        """Update price and recalculate all primitives"""
        if timestamp is None:
            timestamp = time.time()
            
        state = self.states.get(pair)
        if state is None:
            state = PathSpaceState()
            state.current_price = price
            state.atr = atr
            state.last_update = timestamp
            self.states[pair] = state
            return state
        
        # Calculate displacement
        prev_price = state.current_price
        delta_p = abs(price - prev_price)
        
        # Update histories
        state.price_history.append(price)
        state.atr_history.append(atr)
        
        # Update path length (sum of absolute displacements)
        state.path_len += delta_p
        
        # Calculate displacement from entry or start
        if state.entry_price is not None:
            state.displacement = abs(price - state.entry_price)
        else:
            # Use first price as reference
            if len(state.price_history) > 1:
                start_price = state.price_history[0]
                state.displacement = abs(price - start_price)
        
        # Calculate efficiency
        if state.path_len > 0:
            state.efficiency = state.displacement / state.path_len
            state.overlap = state.path_len / max(state.displacement, 0.0001)
        
        # Update rolling extrema
        if price > state.rolling_high:
            state.rolling_high = price
        if price < state.rolling_low:
            state.rolling_low = price
        
        # Calculate pullback from extrema
        if state.direction == "LONG" and state.rolling_high > float('-inf'):
            state.pullback = (state.rolling_high - price) / atr
        elif state.direction == "SHORT" and state.rolling_low < float('inf'):
            state.pullback = (price - state.rolling_low) / atr
        
        # Calculate progress
        if state.entry_price is not None and atr > 0:
            state.progress = abs(price - state.entry_price) / atr
        
        # Calculate energy (displacement from reference point)
        if len(state.price_history) >= 5:  # Reduced from 20 for testing
            ref_price = state.price_history[0]  # Use first price as reference
            state.energy = abs(price - ref_price) / atr
        
        # Calculate speed (recent displacement rate)
        if len(state.price_history) >= 10 and atr > 0:
            recent_window = min(10, len(state.price_history))
            recent_disp = abs(price - state.price_history[-recent_window])
            state.speed = recent_disp / atr
            
            # Store speed history for velocity calculation
            state.speed_history.append(state.speed)
            
            # Calculate velocity (change in speed)
            if len(state.speed_history) >= 2:
                state.velocity = state.speed_history[-1] - state.speed_history[-2]
        
        # Calculate volatility slope
        if len(state.atr_history) >= 3:
            state.vol_slope = state.atr_history[-1] - state.atr_history[-3]
        
        # Update time tracking
        if abs(price - state.current_price) < 0.00001:  # Same price level
            state.time_at_level += (timestamp - state.last_update)
        else:
            state.time_at_level = 0.0
        
        # Update state
        state.current_price = price
        state.atr = atr
        state.last_update = timestamp
        
        # LOG METRICS - SINGLE SOURCE OF TRUTH
        metrics_obj = {
            "pair": pair,
            "side": state.direction.lower() if state.direction else "none",
            "price_exec": price,
            "entry_price": state.entry_price or 0.0,
            "atr_price": atr,
            "atr_pips": atr_pips(pair, atr),
            "spread_pips": 0.0,  # Will be updated by caller
            
            # Path-space primitives (minimum required)
            "dp": delta_p,
            "path_len": state.path_len,
            "efficiency": state.efficiency,
            "overlap": state.overlap,
            "progress": state.progress,
            "speed": state.speed,
            "velocity": state.velocity,
            "pullback": state.pullback,
            "local_high": state.rolling_high,
            "local_low": state.rolling_low,
            
            # Additional fields
            "time_at_level": state.time_at_level,
            "vol_slope": state.vol_slope
        }
        
        # Log metrics to metrics.jsonl
        log_metrics(metrics_obj)
        
        return state
    
    def set_entry(self, pair: str, entry_price: float, direction: str) -> None:
        """Set entry parameters for trade"""
        state = self.states.get(pair)
        if state is None:
            state = PathSpaceState()
            self.states[pair] = state
        
        state.entry_price = entry_price
        state.direction = direction.upper()
        state.rolling_high = entry_price if direction == "LONG" else float('-inf')
        state.rolling_low = entry_price if direction == "SHORT" else float('inf')
    
    def get_primitives(self, pair: str) -> Dict[str, float]:
        """Get all primitives for a pair"""
        state = self.states.get(pair)
        if state is None:
            return {}
        
        return {
            "current_price": state.current_price,
            "entry_price": state.entry_price or 0.0,
            "path_len": state.path_len,
            "displacement": state.displacement,
            "efficiency": state.efficiency,
            "overlap": state.overlap,
            "progress": state.progress,
            "energy": state.energy,
            "speed": state.speed,
            "velocity": state.velocity,
            "rolling_high": state.rolling_high,
            "rolling_low": state.rolling_low,
            "pullback": state.pullback,
            "time_at_level": state.time_at_level,
            "atr": state.atr,
            "vol_slope": state.vol_slope,
        }
    
    def reset(self, pair: str) -> None:
        """Reset state for a pair"""
        if pair in self.states:
            del self.states[pair]

# Global canonical instance
_path_engine = PathSpaceEngine()

def get_path_engine() -> PathSpaceEngine:
    """Get the canonical path-space engine instance"""
    return _path_engine

# ============================================================================
# END PATH-SPACE ENGINE
# ============================================================================

# ============================================================================
# PRICING STREAM - Continuous Tick Data
# ============================================================================

@dataclass
class TickData:
    """Single tick data point"""
    ts: float
    bid: float
    ask: float
    mid: float
    spread: float
    spread_pips: float

class PricingStream:
    """Continuous pricing stream for tick-grade data"""
    
    def __init__(self, pairs: List[str], max_buffer: int = 1000):
        self.pairs = pairs
        self.max_buffer = max_buffer
        self.tick_buffers: Dict[str, deque] = {pair: deque(maxlen=max_buffer) for pair in pairs}
        self.latest_ticks: Dict[str, Optional[TickData]] = {pair: None for pair in pairs}
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.oanda_client = None
        
    def start(self, oanda_client):
        """Start the pricing stream"""
        if self.running:
            return
            
        self.oanda_client = oanda_client
        self.running = True
        self.thread = threading.Thread(target=self._stream_loop, daemon=True)
        self.thread.start()
        log(f"{EMOJI_INFO} PRICING_STREAM_STARTED", {"pairs": self.pairs})
        
    def stop(self):
        """Stop the pricing stream"""
        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        log(f"{EMOJI_INFO} PRICING_STREAM_STOPPED", {"pairs": self.pairs})
        
    def _stream_loop(self):
        """Main streaming loop - polls pricing endpoint continuously"""
        poll_interval = 0.5  # 500ms for high-frequency data
        
        while self.running and self.oanda_client:
            try:
                for pair in self.pairs:
                    bid, ask = self.oanda_client.pricing(pair)
                    if is_valid_price(bid) and is_valid_price(ask):
                        self._add_tick(pair, float(bid), float(ask))
                time.sleep(poll_interval)
            except Exception as e:
                log(f"{EMOJI_WARN} PRICING_STREAM_ERROR", {"error": str(e)})
                time.sleep(1)

    def _add_tick(self, pair: str, bid: float, ask: float):
        """Add a new tick to the buffer"""
        if not (is_valid_price(bid) and is_valid_price(ask)):
            return

        mid = (bid + ask) / 2
        spread = ask - bid
        spread_pips = float(Decimal(str(spread)) / pip_size(pair))
        
        tick = TickData(
            ts=now_ts(),
            bid=bid,
            ask=ask,
            mid=mid,
            spread=spread,
            spread_pips=spread_pips
        )
        
        # Update buffer and latest
        self.tick_buffers[pair].append(tick)
        self.latest_ticks[pair] = tick
        
    def get_latest_tick(self, pair: str) -> Optional[TickData]:
        """Get the most recent tick for a pair"""
        return self.latest_ticks.get(pair)
        
    def get_recent_ticks(self, pair: str, count: int = 100) -> List[TickData]:
        """Get the most recent N ticks for a pair"""
        buffer = self.tick_buffers.get(pair)
        if not buffer:
            return []
        return list(buffer)[-count:]
        
    def get_tick_statistics(self, pair: str, window_seconds: float = 60.0) -> dict:
        """Calculate tick statistics over a time window"""
        ticks = self.get_recent_ticks(pair)
        if not ticks:
            return {}
            
        # Filter by time window
        cutoff = now_ts() - window_seconds
        recent = [t for t in ticks if t.ts >= cutoff]
        
        if not recent:
            return {}
            
        # Calculate statistics
        spreads = [t.spread_pips for t in recent]
        mids = [t.mid for t in recent]
        
        return {
            "tick_count": len(recent),
            "avg_spread_pips": sum(spreads) / len(spreads),
            "min_spread_pips": min(spreads),
            "max_spread_pips": max(spreads),
            "price_change": mids[-1] - mids[0] if len(mids) > 1 else 0,
            "volatility": sum(abs(mids[i] - mids[i-1]) for i in range(1, len(mids))) / len(mids)
        }

# Global pricing stream instance
_pricing_stream: Optional[PricingStream] = None

def get_pricing_stream() -> Optional[PricingStream]:
    """Get the global pricing stream instance"""
    return _pricing_stream

# ============================================================================
# END PRICING STREAM
# ============================================================================

# ============================================================================
# STATE MACHINE - Formal State Transitions with Alerts
# ============================================================================

@dataclass
class StateTransition:
    """Record of a state transition"""
    ts: float
    pair: str
    from_state: str
    to_state: str
    strategy: Optional[str] = None
    direction: Optional[str] = None
    reason: Optional[str] = None
    metadata: Optional[dict] = None

class StateMachine:
    """Formal state machine with alert integration and logging"""
    
    # Valid states
    STATES = {
        "PASS", "SKIP", "WAIT", "WATCH", 
        "GET_READY", "ARM_TICK_ENTRY", "ENTER", "MANAGING"
    }
    
    # State transition matrix (allowed transitions)
    # ...existing code...
    ALLOWED_TRANSITIONS = {
        "PASS": {"WAIT", "WATCH", "SKIP", "GET_READY"},
        "SKIP": {"WAIT", "WATCH"},
        "WAIT": {"WATCH", "PASS", "SKIP"},
        "WATCH": {"GET_READY", "WAIT", "PASS", "SKIP"},
        "GET_READY": {"ARM_TICK_ENTRY", "ENTER", "WATCH", "WAIT", "SKIP"},
        "ARM_TICK_ENTRY": {"GET_READY", "ENTER", "SKIP", "WATCH"},
        # ENTER can cool down back to WATCH/SKIP via _apply_state_machine()
        "ENTER": {"MANAGING", "WATCH", "SKIP"},
        "MANAGING": {"WAIT", "WATCH", "PASS", "SKIP"}
    }
    
    def __init__(self):
        self.current_states: Dict[str, str] = {}  # pair -> state
        self.state_history: Dict[str, List[StateTransition]] = {}  # pair -> history
        self.state_entry_time: Dict[str, float] = {}  # pair -> entry timestamp
        self._lock = threading.Lock()
        
    def get_state(self, pair: str) -> str:
        """Get current state for a pair"""
        with self._lock:
            return self.current_states.get(pair, "PASS")
            
    def set_state(self, pair: str, state: str, strategy: Optional[str] = None, 
                  direction: Optional[str] = None, reason: Optional[str] = None,
                  metadata: Optional[dict] = None):
        """Set state for a pair (only for initialization)"""
        with self._lock:
            if state not in self.STATES:
                log(f"{EMOJI_WARN} INVALID_STATE", {"pair": pair, "state": state})
                return
                
            self.current_states[pair] = state
            self.state_entry_time[pair] = now_ts()
            if pair not in self.state_history:
                self.state_history[pair] = []
                
    def transition(self, pair: str, to_state: str, strategy: Optional[str] = None,
                   direction: Optional[str] = None, reason: Optional[str] = None,
                   metadata: Optional[dict] = None) -> bool:
        """Transition to a new state with alerts and logging"""
        with self._lock:
            from_state = self.current_states.get(pair, "PASS")
            
            # Validate transition
            if to_state not in self.STATES:
                log(f"{EMOJI_WARN} INVALID_STATE", {"pair": pair, "state": to_state})
                return False
                
            if from_state not in self.ALLOWED_TRANSITIONS:
                log(f"{EMOJI_WARN} INVALID_FROM_STATE", {"pair": pair, "state": from_state})
                return False
                
            if to_state not in self.ALLOWED_TRANSITIONS[from_state]:
                log(f"{EMOJI_WARN} INVALID_TRANSITION", 
                    {"pair": pair, "from": from_state, "to": to_state})
                return False
                
            # Skip if already in target state
            if from_state == to_state:
                return False
                
            # Record transition
            transition = StateTransition(
                ts=now_ts(),
                pair=pair,
                from_state=from_state,
                to_state=to_state,
                strategy=strategy,
                direction=direction,
                reason=reason,
                metadata=metadata
            )
            
            # Update state
            self.current_states[pair] = to_state
            self.state_entry_time[pair] = transition.ts
            
            if pair not in self.state_history:
                self.state_history[pair] = []
            self.state_history[pair].append(transition)
            
            # Emit alerts based on transition
            self._emit_alert(transition)
            
            # Log transition
            self._log_transition(transition)
            
            return True
            
    def get_state_duration(self, pair: str) -> float:
        """Get duration in current state"""
        with self._lock:
            entry_time = self.state_entry_time.get(pair, now_ts())
            return now_ts() - entry_time
            
    def get_last_transition(self, pair: str) -> Optional[StateTransition]:
        """Get the most recent transition for a pair"""
        with self._lock:
            history = self.state_history.get(pair, [])
            return history[-1] if history else None
            
    def _emit_alert(self, transition: StateTransition):
        """Emit appropriate alert based on state transition"""
        try:
            # Call actual alert functions defined later in the file
            if transition.to_state == "WATCH":
                # Import here to avoid circular dependency
                from sys import modules
                if 'phone_bot' in modules:
                    alert_watch_triggered(
                        transition.pair, 
                        transition.strategy or "UNKNOWN",
                        transition.metadata.get('signal_strength', 0.0) if transition.metadata else 0.0
                    )
                    
            elif transition.to_state == "GET_READY":
                entry_conditions = transition.metadata or {}
                alert_get_ready(
                    transition.pair,
                    transition.strategy or "UNKNOWN",
                    entry_conditions
                )
                    
            elif transition.to_state == "ENTER":
                alert_enter_placed(
                    transition.pair,
                    transition.strategy or "UNKNOWN",
                    transition.direction or "UNKNOWN",
                    transition.metadata.get('order_id', 'UNKNOWN') if transition.metadata else 'UNKNOWN'
                )
                    
            elif transition.from_state == "ENTER" and transition.to_state == "MANAGING":
                alert_trade_entered(
                    transition.pair,
                    transition.strategy or "UNKNOWN",
                    transition.direction or "UNKNOWN",
                    transition.metadata.get('trade_id', 'UNKNOWN') if transition.metadata else 'UNKNOWN'
                )
                    
        except Exception as e:
            log(f"{EMOJI_WARN} ALERT_EMIT_FAILED", 
                {"pair": transition.pair, "error": str(e)})
                
    def _log_transition(self, transition: StateTransition):
        """Log state transition using unified logging system"""
        try:
            log_trade_event({
                "ts": transition.ts,
                "event": "STATE_TRANSITION",
                "pair": transition.pair,
                "from": transition.from_state,
                "to": transition.to_state,
                "strategy": transition.strategy,
                "direction": transition.direction,
                "reason": transition.reason,
                "metadata": transition.metadata
            })
            # Also log to runtime
            log_runtime("info", "STATE_TRANSITION", pair=transition.pair, from_state=transition.from_state, to_state=transition.to_state)
        except Exception as e:
            log_runtime("warning", "STATE_LOG_FAILED", pair=transition.pair, error=str(e))

# Global state machine instance
_state_machine: Optional[StateMachine] = None

def get_state_machine() -> StateMachine:
    """Get the global state machine instance"""
    global _state_machine
    if _state_machine is None:
        _state_machine = StateMachine()
    return _state_machine

# ============================================================================
# END STATE MACHINE
# ============================================================================

def normalize_pair(pair: str) -> str:
    p = str(pair or "").strip().upper()
    if not p:
        return ""
    p = p.replace("-", "_").replace("/", "_")
    if "_" not in p and len(p) == 6 and p.isalpha():
        p = f"{p[:3]}_{p[3:]}"
    return p

def normalize_granularity(gran: str) -> str:
    return str(gran or "").strip().upper()

def normalize_oanda_env(env: str) -> str:
    e = str(env or "").strip().lower()
    if e in ("practice", "fxpractice", "demo", "paper", "test"):
        return "practice"
    if e in ("live", "fxtrade", "prod", "production"):
        return "live"
    return ""


def extract_margin_available(acct_sum: dict) -> Tuple[float, str]:
    """Parse marginAvailable from account summary response across known shapes."""
    if not isinstance(acct_sum, dict):
        return float("nan"), "invalid"
    account_obj = acct_sum.get("account")
    if isinstance(account_obj, dict) and "marginAvailable" in account_obj:
        val = account_obj.get("marginAvailable")
        try:
            return float(val if val is not None else 0.0), "nested_account"
        except Exception:
            return float("nan"), "nested_account_invalid"
    if "marginAvailable" in acct_sum:
        val = acct_sum.get("marginAvailable")
        try:
            return float(val if val is not None else 0.0), "top_level"
        except Exception:
            return float("nan"), "top_level_invalid"
    return float("nan"), "missing"

PAIRS = [
    normalize_pair(p)
    for p in [
        "EUR_USD",
        "EUR_CAD",
        "USD_CAD",
        "AUD_USD",
        "AUD_JPY",
        "USD_JPY",
        "USD_DKK",
    ]
]
_pair_env = (os.getenv("PAIR_LIST", "") or "").strip()
if _pair_env:
    parts = []
    for chunk in _pair_env.replace(";", ",").split(","):
        parts.extend(chunk.split())
    _pair_override = [normalize_pair(p) for p in parts if normalize_pair(p)]
    if _pair_override:
        PAIRS = _pair_override

# Broker rejection logging
BROKER_REJECT_LOG = "broker_rejections.log"

def log_broker_reject(event_type: str, pair: str, details: Dict[str, Any]) -> None:
    """Log broker rejections/cancellations to a dedicated file for debugging."""
    pair = normalize_pair(pair)
    timestamp = datetime.utcnow().isoformat() + " UTC"
    entry = {
        "ts": timestamp,
        "event": event_type,
        "pair": pair,
        **details
    }
    try:
        with open(BROKER_REJECT_LOG, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
    except Exception as e:
        # Fallback to main log if file write fails
        log(f"{EMOJI_ERR} BROKER_REJECT_LOG_FAIL", {"error": str(e), "event": event_type, "pair": pair})

LEVERAGE_50 = {normalize_pair(p) for p in {"EUR_USD", "USD_JPY", "USD_CAD"}}
LEV_DEFAULT = 20  # Changed from 33 to prevent insufficient margin rejections

# Scan cadence (override via env)
SKIP_SCAN_SEC = float(os.getenv("SKIP_SCAN_SEC", "300") or "300")  # 5 minutes for SKIP state
WATCH_SCAN_SEC = float(os.getenv("WATCH_SCAN_SEC", "60") or "60")  # 1 minute for WATCH state
FOCUS_SCAN_SEC = float(os.getenv("FOCUS_SCAN_SEC", "5") or "5")    # 5 seconds for GET_READY/ENTER
EXIT_SCAN_SEC = float(os.getenv("EXIT_SCAN_SEC", "2") or "2")      # Every 2 seconds for open trades
EXIT_REFRESH_SEC = float(os.getenv("EXIT_REFRESH_SEC", "20") or "20")

# Main loop sleep (override via env)
LOOP_SLEEP_SEC = float(os.getenv("LOOP_SLEEP_SEC", "0.2") or "0.2")

# Legacy compatibility constant (rate limiting removed)
MAX_HTTP_PER_MIN = 120

# Call frequency tuning (cache TTLs)
ACCOUNT_REFRESH_SEC = float(os.getenv("ACCOUNT_REFRESH_SEC", "15") or "15")
OPEN_POS_REFRESH_SEC = float(os.getenv("OPEN_POS_REFRESH_SEC", "10") or "10")
PENDING_REFRESH_SEC = float(os.getenv("PENDING_REFRESH_SEC", "10") or "10")
PRICE_REFRESH_SEC = float(os.getenv("PRICE_REFRESH_SEC", "5") or "5")
EXIT_PRICE_REFRESH_SEC = float(os.getenv("EXIT_PRICE_REFRESH_SEC", "2") or "2")
CANDLE_REFRESH_SEC = float(os.getenv("CANDLE_REFRESH_SEC", "5") or "5")

# Candle refresh by scan type (seconds)
CANDLE_REFRESH_FOCUS_SEC = float(os.getenv("CANDLE_REFRESH_FOCUS_SEC", "5") or "5")
CANDLE_REFRESH_WATCH_SEC = float(os.getenv("CANDLE_REFRESH_WATCH_SEC", "15") or "15")
CANDLE_REFRESH_SKIP_SEC = float(os.getenv("CANDLE_REFRESH_SKIP_SEC", "30") or "30")
EXIT_CANDLE_REFRESH_SEC = float(os.getenv("EXIT_CANDLE_REFRESH_SEC", "5") or "5")
BOOKS_REFRESH_SEC = float(os.getenv("BOOKS_REFRESH_SEC", "10") or "10")
ALLOW_PARTIAL_CANDLES = os.getenv("ALLOW_PARTIAL_CANDLES", "1").strip().lower() in ("1", "true", "yes")
PARTIAL_CANDLE_REFRESH_SEC = float(os.getenv("PARTIAL_CANDLE_REFRESH_SEC", "1.0") or "1.0")

# ===== LEGACY CONSTANTS - DO NOT USE FOR NEW GATES =====
SPREAD_SPIKE_WINDOW = int(os.getenv("SPREAD_SPIKE_WINDOW", "60") or "60")
SPREAD_SPIKE_MIN_SAMPLES = int(os.getenv("SPREAD_SPIKE_MIN_SAMPLES", "20") or "20")
SPREAD_SPIKE_THRESHOLD = float(os.getenv("SPREAD_SPIKE_THRESHOLD", "2.5") or "2.5")
SPREAD_SPIKE_MULT = float(os.getenv("SPREAD_SPIKE_MULT", "0.75") or "0.75")
SPREAD_SIZE_MIN = float(os.getenv("SPREAD_SIZE_MIN", "0.15") or "0.15")
SPREAD_SIZE_ALPHA = float(os.getenv("SPREAD_SIZE_ALPHA", "1.5") or "1.5")
SPREAD_SIZE_EPS = float(os.getenv("SPREAD_SIZE_EPS", "0.05") or "0.05")
SPREAD_F_MAX = float(os.getenv("SPREAD_F_MAX", "3.0") or "3.0")
USE_FE_SPREAD_SIZING = os.getenv("USE_FE_SPREAD_SIZING", "0").strip().lower() in ("1", "true", "yes")

MAX_POS_PER_PAIR = 2
# No global limit - we have currency exposure limits instead

MAX_FAST_TRADES = 999  # Effectively unlimited - controlled by currency exposure
MAX_MED_TRADES = 999   # Effectively unlimited - controlled by currency exposure
MAX_SLOW_TRADES = 999  # Effectively unlimited - controlled by currency exposure
MAX_CURRENCY_EXPOSURE_FAST = 8
MAX_CURRENCY_EXPOSURE_MED = 6
MAX_CURRENCY_EXPOSURE_SLOW = 4

MIN_ATR_PIPS_EXEC_NONJPY = 0.05  # Increased to filter noise (was 0.01)
MIN_ATR_PIPS_EXEC_JPY = 0.05     # Increased to filter noise (was 0.01)

# No MAX_UNITS_PER_TRADE - size controlled by margin utilization percentage

TF_EXEC = normalize_granularity(os.getenv("TF_EXEC", "S5") or "S5")        # Entry/exit timing
TF_TREND = normalize_granularity(os.getenv("TF_TREND", "M15") or "M15")      # Trend analysis  
TF_GLOBAL = normalize_granularity(os.getenv("TF_GLOBAL", "H1") or "H1")       # Market context
TF_POSITION = normalize_granularity(os.getenv("TF_POSITION", "H4") or "H4")   # Position management
# Session detection for strategy timing (no trading restrictions)
SESSION_DETECTION = os.getenv("SESSION_DETECTION", "1").strip().lower() in ("1", "true", "yes")

# Session definitions (UTC times)
TRADING_SESSIONS = {
    "SYDNEY": {"open": "21:00", "close": "06:00", "days": range(5)},  # Sun-Thu
    "TOKYO": {"open": "23:00", "close": "08:00", "days": range(5)},   # Sun-Thu  
    "LONDON": {"open": "07:00", "close": "16:00", "days": range(5)},  # Mon-Fri
    "NEW_YORK": {"open": "12:00", "close": "21:00", "days": range(5)}, # Mon-Fri
}

# Tick data for high-frequency analysis
TICK_DATA_ENABLED = os.getenv("TICK_DATA_ENABLED", "1").strip().lower() in ("1", "true", "yes")

# =========================
# V12 LOCKED CONSTANTS (DO NOT DRIFT)
# =========================
LIVE_MODE = os.getenv("LIVE_MODE", "0").strip().lower() in ("1", "true", "yes")

# Back-compat: derive from LIVE_MODE unless explicitly overridden by env.
# Default OFF unless explicitly enabled.
DRY_RUN_ONLY = False  # Force disabled - no dry run mode
ALLOW_ENTRIES = os.getenv("ALLOW_ENTRIES", "1").strip().lower() in ("1", "true", "yes")

SIGNAL_STALE_TTL_SEC = 12 * 60  # 720s
ARM_ENTRY_DIST_ATR = 0.30
ENTRY_BREAK_BUFFER_ATR = 0.10
ENTRY_RECLAIM_BUFFER_ATR = 0.05
ENTRY_CONFIRM_DISP_ATR = 0.20

REASON_DRY_RUN = "DRY_RUN"
REASON_ENTRIES_DISABLED = "ALLOW_ENTRIES_FALSE"
REASON_STALE_SIGNAL = "SIGNAL_STALE"
REASON_PAIR_EXIT_BLOCKED = "PAIR_EXIT_BLOCKED"
REASON_PAIR_HARD_BLOCK = "PAIR_ENTRY_HARD_BLOCK"
REASON_SCHEMA_FAIL = "GET_READY_SCHEMA_FAIL"
REASON_TOO_FAR_FROM_ZONE = "ARM_DIST_ATR_GT_0_30"

ATR_N = 14
ATR_LONG_N = 50
MOM_N = 14
WR_N = 14

# ATR fallback chain (primary to last-resort)
ATR_FALLBACK_GRANS = ["M1", "M5", "H1", "H4", "D"]

BOX_BARS = 180
SWEEP_L = 8

# Optimized Setup Triggers (Looser for more entries)
SWEEP_ATR_THRESHOLD = 0.40         # Was 0.75 - catch smaller wicks
FAIL_BREAK_EXTENSION_MAX = 1.50    # Was 1.25 - allow wilder wicks
COMPRESSION_WIDTH_MAX = 0.95       # Was 0.70 - accept messier consolidations
MIN_BREAKOUT_PIPS = 5.0            # New - prevent tiny breakouts
EXHAUSTION_MA_DISTANCE = 2.5       # New - use MA distance for true exhaustion
MOMENTUM_THRESHOLD = 0.60          # New - lower than 0.80 for earlier entry

# MODE_RULES thresholds aligned with detect_mode defaults (no behavior change).
MODE_RULES = {
    "SLOW": {"ratio_min": 0.0, "ratio_max": 0.60, "atr_pips_min": 0.4},
    "MED": {"ratio_min": 0.60, "ratio_max": 1.50, "atr_pips_min": 0.5},
    "FAST": {"ratio_min": 1.50, "ratio_max": 3.00, "atr_pips_min": 0.6},
    "VOLATILE": {"ratio_min": 3.00, "atr_pips_min": 0.8},
}

MODE_SPEED_CLASS_PARAMS = {
    "SLOW": {"util": 0.15, "ttl": 720, "pg_t": 360, "pg_atr": 0.25},   # Conservative
    "MED": {"util": 0.20, "ttl": 480, "pg_t": 240, "pg_atr": 0.30},    # Balanced
    "FAST": {"util": 0.25, "ttl": 240, "pg_t": 120, "pg_atr": 0.30},   # Slightly reduced for speed
    "VOLATILE": {"util": 0.12, "ttl": 120, "pg_t": 60, "pg_atr": 0.40}, # Survival: Small size
}

# Back-compat alias used by legacy tests
MODE_EXEC = MODE_SPEED_CLASS_PARAMS

SPLIT_FAST = (0.70, 0.30)  # 30% runner for quick scalps
SPLIT_MED = (0.50, 0.50)  # 50% runner to maximize trend profit
SPLIT_SLOW = (0.80, 0.20)  # Bank most in slow markets

SPEED_WEIGHT_FAST = 1.30  # Increased for aggressive scalps
SPEED_WEIGHT_MED = 1.00   # Standard
SPEED_WEIGHT_SLOW = 0.80  # Increased for better size in slow markets

WR_OVERSOLD = -80
WR_OVERBOUGHT = -20
WR_TURN_PTS = 10

WR_NEUTRAL_MIN = -70
WR_NEUTRAL_MAX = -30
WR_NEUTRAL_BARS_RESET = 8
STATE_MIN_HOLD_SEC = 55.0  # +10s to increase median hold
ENTER_HOLD_SEC = 6.0
ORDER_DEDUPE_SEC = 15.0
ALERT_REPEAT_SEC = float(os.getenv("ALERT_REPEAT_SEC", "60") or "60")

# Tick entry/exit behavior
TICK_ENTRY_ENABLED = os.getenv("TICK_ENTRY_ENABLED", "1").strip().lower() in ("1", "true", "yes")
# Legacy knob retained for compatibility; V12 authority is SIGNAL_STALE_TTL_SEC.
ENTRY_ARM_TTL_SEC = SIGNAL_STALE_TTL_SEC
ENTRY_PULLBACK_ATR = float(os.getenv("ENTRY_PULLBACK_ATR", "0.30") or "0.30")
ENTRY_RESUME_ATR = float(os.getenv("ENTRY_RESUME_ATR", "0.10") or "0.10")
TICK_EXIT_ENABLED = os.getenv("TICK_EXIT_ENABLED", "1").strip().lower() in ("1", "true", "yes")
EXIT_SCAN_TICK_SEC = float(os.getenv("EXIT_SCAN_TICK_SEC", "0.5") or "0.5")
EXIT_PRICE_REFRESH_TICK_SEC = float(os.getenv("EXIT_PRICE_REFRESH_TICK_SEC", "0.5") or "0.5")
TICK_MODE_TTL_SEC = float(os.getenv("TICK_MODE_TTL_SEC", "30") or "30")

# Alert System Configuration
ALERT_SYSTEM_ENABLED = os.getenv("ALERT_SYSTEM_ENABLED", "1").strip().lower() in ("1", "true", "yes")


SETUP_SPEED_CLASS = {
    4: "FAST",  # FAILED_BREAKOUT_FADE
    5: "FAST",  # SWEEP_POP
    1: "MED",   # COMPRESSION_RELEASE
    2: "MED",   # CONTINUATION_PUSH
    3: "SLOW",  # EXHAUSTION_SNAP
    6: "SLOW",  # VOL_REIGNITE
    7: "SLOW",  # INTENTIONAL_RUNNER
}

SPEED_CLASS_PARAMS = {
    "FAST": {
        "tp1_atr": 0.40, "tp2_atr": 1.60, "sl_atr": 0.85,
        "ttl_main": 240, "ttl_run": 360,
        "pg_t_frac": 0.5, "pg_atr": 0.30,
    },
    "MED": {
        "tp1_atr": 0.55, "tp2_atr": 2.10, "sl_atr": 1.10,
        "ttl_main": 500, "ttl_run": 800,
        "pg_t_frac": 0.375, "pg_atr": 0.25,  # 375% = 180/480
    },
    "SLOW": {
        "tp1_atr": 0.35, "tp2_atr": 1.20, "sl_atr": 1.25,
        "ttl_main": 1200, "ttl_run": 1800,
        "pg_t_frac": 0.3, "pg_atr": 0.20,  # 30% = 270/900
    },
}

# Mechanical ATR fallback exits (used only when structure is unclear / partial bars).
ATR_FALLBACK_PARAMS = {
    "FAST": {"sl_atr": 0.6, "tp1_atr": 0.6, "tp2_atr": 1.2},
    "MED": {"sl_atr": 0.7, "tp1_atr": 0.7, "tp2_atr": 1.4},
    "SLOW": {"sl_atr": 0.8, "tp1_atr": 0.8, "tp2_atr": 1.4},
}

# Percent-based fallback for SL/TP when ATR is invalid (applied to entry price).
FALLBACK_SL_PCT = float(os.getenv("FALLBACK_SL_PCT", "0.002") or "0.002")   # 0.20%
FALLBACK_TP1_PCT = float(os.getenv("FALLBACK_TP1_PCT", "0.002") or "0.002") # 0.20%
FALLBACK_TP2_PCT = float(os.getenv("FALLBACK_TP2_PCT", "0.004") or "0.004") # 0.40%

LATE_IMPULSE_BLOCK_ATR = 1.20
LATE_IMPULSE_SCALE_START = 0.90

ADVERSE_KILL_ATR = 0.35
ADVERSE_KILL_TTL_FRAC = 0.30

ENABLE_MAD_EXIT = False
MIN_RUNNER_PROFIT_ATR = 0.20
MIN_AGE_SEC_FAST = 90
MIN_AGE_SEC_MED = 180
MIN_AGE_SEC_SLOW = 300

MAD_DECAY_THRESHOLD_ATR = 0.06
MAD_DECAY_TIME_SEC = 45.0
MAD_DECAY_MIN_PROFIT_ATR = 0.15
MAD_SAMPLE_COUNT = 20

RUNNER_BFE_MIN_ATR = 0.60
RUNNER_GIVEBACK_ATR = 0.35
RUNNER_VEL_MAX = 0.0

SWEET_SPOT_STATES = {"GET_READY", "ENTER"}

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
DB_PATH = os.path.join(PROJECT_DIR, "phone_bot.db")
STOP_FLAG = os.path.join(PROJECT_DIR, "STOP_TRADING.flag")
LOG_PATH = os.path.join(PROJECT_DIR, "phone_bot.log")
MAX_LOG_SIZE = 2 * 1024 * 1024

BOT_ID = os.getenv("PHONE_BOT_ID", "phone-bot").strip() or "phone-bot"
MAX_RISK_USD_PER_TRADE = float(os.getenv("MAX_RISK_USD_PER_TRADE", "0") or "0")
MIN_FREE_DISK_MB = float(os.getenv("MIN_FREE_DISK_MB", "0") or "0")

LEGACY_MIN_TRADE_SIZE_DO_NOT_USE = 1  # Deprecated legacy constant. Do not use.

# Economic Viability Gate constants
ENTRY_BUFFER_PIPS = 0.5  # Buffer for entry slippage
EXIT_BUFFER_PIPS = 0.5   # Buffer for exit slippage
COST_MULT = 1.10          # TP must be 1.10x the round-trip cost

def check_economic_viability(pair: str, spread_pips: float, payoff_pips_min: float) -> Tuple[bool, str, dict]:
    """
    Universal Economic Viability Gate: Block trades that can't beat round-trip friction.
    
    Args:
        pair: Currency pair
        spread_pips: Current spread in pips
        payoff_pips_min: Strategy's earliest required "must-achieve" payoff proxy in pips
        
    Returns:
        (ok: bool, reason: str, debug: dict)
        reason is one of:
            - "FRICTION_OK"
            - "FRICTION_NOT_COVERED"
            - "FRICTION_INVALID_INPUT"
    """
    # Validate inputs
    if not (math.isfinite(spread_pips) and math.isfinite(payoff_pips_min)):
        debug = {
            "spread_pips": spread_pips,
            "payoff_pips_min": payoff_pips_min,
            "error": "invalid_inputs"
        }
        return False, "FRICTION_INVALID_INPUT", debug
    
    if spread_pips < 0 or payoff_pips_min < 0:
        debug = {
            "spread_pips": spread_pips,
            "payoff_pips_min": payoff_pips_min,
            "error": "negative_values"
        }
        return False, "FRICTION_INVALID_INPUT", debug
    
    # Compute friction
    cost_pips = spread_pips + ENTRY_BUFFER_PIPS + EXIT_BUFFER_PIPS
    min_required = cost_pips * COST_MULT
    
    debug = {
        "spread_pips": spread_pips,
        "entry_buffer_pips": ENTRY_BUFFER_PIPS,
        "exit_buffer_pips": EXIT_BUFFER_PIPS,
        "cost_pips": cost_pips,
        "cost_mult": COST_MULT,
        "payoff_pips_min": payoff_pips_min,
        "min_required": min_required
    }
    
    # Check if payoff covers friction
    if payoff_pips_min >= min_required:
        log_runtime("info", "FRICTION_OK", pair=pair, **debug)
        return True, "FRICTION_OK", debug
    else:
        reason = f"FRICTION_NOT_COVERED: payoff={payoff_pips_min:.1f} < required={min_required:.1f} pips"
        log_runtime("warn", "FRICTION_NOT_COVERED", pair=pair, **debug)
        return False, "FRICTION_NOT_COVERED", debug

def check_broker_min_units(pair: str, desired_units: int) -> Tuple[int, str, dict]:
    """
    Broker Min Units Gate: Ensure units meet broker minimum.
    
    Args:
        pair: Currency pair
        desired_units: Computed desired units
        
    Returns:
        (final_units, reason, debug_info)
    """
    meta = get_instrument_meta_cached(pair)
    if meta is None:
        # Fail-closed: cannot determine broker requirements
        debug = {
            "desired_units": desired_units,
            "error": "instrument_meta_not_cached",
            "pair": pair,
        }
        return desired_units, "BROKER_MIN_UNITS_UNKNOWN", debug
    broker_min_units = int(float(meta.get("minimumTradeSize", 1)))
    
    debug: Dict[str, object] = {
        "desired_units": desired_units,
        "broker_min_units": broker_min_units,
    }
    
    if desired_units < broker_min_units:
        # Try to bump to min units
        final_units = broker_min_units
        debug["final_units"] = final_units
        debug["action"] = "bumped_to_min"
        log_runtime("info", "BROKER_MIN_UNITS_BUMP", pair=pair, **debug)
        return final_units, "bumped_to_min_units", debug
    
    debug["final_units"] = desired_units
    debug["action"] = "no_change"
    return desired_units, "units_ok", debug

_SHUTDOWN = False
_BROKER_TIME_OFFSET = 0.0
_BROKER_TIME_LAST_SYNC = 0.0
EXIT_BLOCKED_PAIRS: Dict[str, dict] = {}
# Order rejection cooldown tracking (per pair)
ORDER_REJECT_BLOCK: Dict[str, dict] = {}
TRUTH_CACHE: Dict[Any, Any] = {}

# Exit retry backoff (seconds)
EXIT_RETRY_BASE_SEC = float(os.getenv("EXIT_RETRY_BASE_SEC", "5") or "5")
EXIT_RETRY_MAX_SEC = float(os.getenv("EXIT_RETRY_MAX_SEC", "60") or "60")

# Order reject cooldown (seconds)
ORDER_REJECT_COOLDOWN_SEC = float(os.getenv("ORDER_REJECT_COOLDOWN_SEC", "30") or "30")
# Stop loss placement retry (seconds/attempts)
SL_RETRY_MAX = int(os.getenv("SL_RETRY_MAX", "3") or "3")
SL_RETRY_BASE_SEC = float(os.getenv("SL_RETRY_BASE_SEC", "1.5") or "1.5")

# Thread locks for shared state
_trade_track_lock = threading.Lock()
_price_cache_lock = threading.Lock()
_db_lock = threading.Lock()

# Alert deduplication tracking
_last_state_alert: Dict[str, Dict[str, float]] = {}  # pair -> {state: timestamp}
ALERT_DEDUP_COOLDOWN_SEC = 60.0  # Minimum seconds between identical alerts

# Alert repeat tuning (override via env). Set GET_READY repeat to 0 to stop spam.
ALERT_REPEAT_GET_READY_SEC = float(os.getenv("ALERT_REPEAT_GET_READY_SEC", str(ALERT_REPEAT_SEC)) or str(ALERT_REPEAT_SEC))
ALERT_REPEAT_ENTER_SEC = float(os.getenv("ALERT_REPEAT_ENTER_SEC", str(ALERT_REPEAT_SEC)) or str(ALERT_REPEAT_SEC))
ALERT_REPEAT_OTHER_SEC = float(os.getenv("ALERT_REPEAT_OTHER_SEC", str(ALERT_REPEAT_SEC)) or str(ALERT_REPEAT_SEC))

# Tick activity tracking
TICK_WINDOW_SEC = float(os.getenv("TICK_WINDOW_SEC", "60") or "60")

# Hourly API budget tracking (override via env). Set to 0 to disable.
_api_calls_this_hour: int = 0
_hour_start_time: float = 0.0
_last_api_budget_log: float = 0.0
HOURLY_API_LIMIT: int = int(os.getenv("HOURLY_API_LIMIT", "500") or "500")
API_BUDGET_RESERVE_CORE: int = int(os.getenv("API_BUDGET_RESERVE_CORE", "3") or "3")

# Optional low-call hourly scan mode
HOURLY_SCAN_MODE = os.getenv("HOURLY_SCAN_MODE", "0").strip().lower() in ("1", "true", "yes")
HOURLY_SCAN_INTERVAL_SEC = float(os.getenv("HOURLY_SCAN_INTERVAL_SEC", "3600") or "3600")
if HOURLY_SCAN_MODE and HOURLY_API_LIMIT <= 0:
    HOURLY_API_LIMIT = 10


def _check_api_budget() -> bool:
    """Check if we have API budget remaining for this hour."""
    global _api_calls_this_hour, _hour_start_time, _last_api_budget_log
    
    now = now_ts()

    if HOURLY_API_LIMIT <= 0:
        return True
    
    # Reset counter at the start of each hour
    if (now - _hour_start_time) >= 3600:
        _api_calls_this_hour = 0
        _hour_start_time = now
        log("API_BUDGET_RESET", {"limit": HOURLY_API_LIMIT})
    
    if _api_calls_this_hour >= HOURLY_API_LIMIT:
        if (now - _last_api_budget_log) >= 60.0:
            log("API_BUDGET_LIMIT_REACHED", {"calls": _api_calls_this_hour, "limit": HOURLY_API_LIMIT})
            _last_api_budget_log = now
        return False
    
    return True

def _api_budget_remaining() -> Optional[int]:
    """Return remaining hourly API calls, or None when budget is disabled."""
    global _api_calls_this_hour, _hour_start_time
    now = now_ts()
    if HOURLY_API_LIMIT <= 0:
        return None
    if (now - _hour_start_time) >= 3600:
        _api_calls_this_hour = 0
        _hour_start_time = now
    return max(0, HOURLY_API_LIMIT - _api_calls_this_hour)


def _api_budget_low_for_optional(reserve_core: Optional[int] = None) -> bool:
    """True when optional endpoints should be skipped to preserve core calls."""
    remaining = _api_budget_remaining()
    if remaining is None:
        return False
    reserve = API_BUDGET_RESERVE_CORE if reserve_core is None else int(reserve_core)
    reserve = max(0, reserve)
    return remaining <= reserve


def _track_api_call() -> None:
    """Track an API call against the hourly budget."""
    global _api_calls_this_hour
    if HOURLY_API_LIMIT <= 0:
        return
    _api_calls_this_hour += 1
    remaining = HOURLY_API_LIMIT - _api_calls_this_hour
    if remaining <= 2:
        log("API_BUDGET_WARNING", {"remaining": remaining})

def block_pair_exits(pair: str, reason: str, duration_sec: float = 60.0) -> None:
    """Backoff exit attempts for a pair after failed closes."""
    pair = normalize_pair(pair)
    now = now_ts()
    prev = EXIT_BLOCKED_PAIRS.get(pair, {})
    fail_count = int(prev.get("fail_count", 0)) + 1
    delay = min(EXIT_RETRY_MAX_SEC, max(duration_sec, EXIT_RETRY_BASE_SEC * (2 ** (fail_count - 1))))
    next_retry = now + delay
    EXIT_BLOCKED_PAIRS[pair] = {
        "ts": now,
        "reason": reason,
        "duration": duration_sec,
        "fail_count": fail_count,
        "next_retry_ts": next_retry,
    }
    log(
        f"{EMOJI_WARN} EXIT_BLOCK_ADD {pair}",
        {"pair": pair, "reason": reason, "duration": duration_sec, "fail_count": fail_count, "next_retry_in_sec": round(delay, 2)},
    )


def _signal_handler(signum, frame):
    global _SHUTDOWN
    _SHUTDOWN = True


def parse_time_oanda(t) -> float:
    """Parse OANDA time format (UNIX timestamp or RFC3339) to float seconds."""
    if t is None:
        return 0.0

    # Already numeric
    if isinstance(t, (int, float)):
        if not math.isfinite(t):
            return 0.0
        # Heuristic: if milliseconds, scale down
        return float(t) / 1000.0 if t > 1e12 else float(t)

    # Try numeric string (epoch seconds or ms, with possible extra precision)
    if isinstance(t, str):
        s = t.strip()
        try:
            val = float(s)
            if not math.isfinite(val):
                return 0.0
            # If value looks like milliseconds (>= 1e11), scale to seconds
            return val / 1000.0 if val >= 1e11 else val
        except (ValueError, TypeError):
            pass

        # Try ISO-8601 (OANDA style with nanoseconds)
        ts = s
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        if "+" not in ts and "-" not in ts[-6:]:
            ts = ts + "+00:00"
        try:
            dt = datetime.fromisoformat(ts)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except Exception:
            return 0.0

    return 0.0


def to_epoch_seconds(ts):
    """Convert any timestamp format to UTC epoch seconds."""
    if ts is None:
        return 0.0
    # string timestamp (RFC3339 / ISO)
    if isinstance(ts, str):
        try:
            # handles: 2026-02-13T12:34:56.123456Z
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.timestamp()
        except Exception:
            return 0.0
    # numeric
    if isinstance(ts, (int, float)):
        t = float(ts)
        # ms epoch
        if t > 1e12:
            return t / 1000.0
        # seconds epoch
        if t > 1e9:
            return t
        return 0.0
    return 0.0


def _normalize_price_obj(p):
    """Accept dicts, tuples, lists; output dict with timestamp if possible."""
    if isinstance(p, dict):
        # ensure timestamp key exists if alternative naming is used
        if "timestamp" not in p:
            for k in ("ts", "time", "t"):
                if k in p:
                    p["timestamp"] = p[k]
                    break
        p["timestamp"] = parse_time_oanda(p.get("timestamp", 0.0))
        return p

    if isinstance(p, (tuple, list)):
        # common shapes:
        # (bid, ask, ts) or (mid, ts) or (bid, ask) etc
        out = {}
        if len(p) >= 1:
            out["bid"] = p[0]
        if len(p) >= 2:
            out["ask"] = p[1]
        # timestamp often last
        if len(p) >= 3:
            out["timestamp"] = parse_time_oanda(p[2])
        elif len(p) == 2 and isinstance(p[-1], (int, float)) and p[-1] > 1e9:
            out["timestamp"] = parse_time_oanda(p[-1])
        else:
            out["timestamp"] = 0
        return out

    # fallback - treat as opaque with zero timestamp
    return {"timestamp": 0}


def now_ts() -> float:
    # Use broker time offset when available to avoid local clock drift issues.
    if math.isfinite(_BROKER_TIME_OFFSET):
        return time.time() + _BROKER_TIME_OFFSET
    return time.time()


def broker_now_ts() -> float:
    return now_ts()


def ts_str(ts: Optional[float] = None) -> str:
    dt = datetime.fromtimestamp(ts or now_ts(), tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")


class RateLimiter:
    def __init__(self, max_per_min: int):
        self.max_per_min = int(max_per_min)
        self.calls: List[float] = []

    def _prune(self) -> None:
        cut = now_ts() - 60.0
        while self.calls and self.calls[0] < cut:
            self.calls.pop(0)

    def can_take(self, n: int = 1) -> bool:
        self._prune()
        return (len(self.calls) + int(n)) <= self.max_per_min

    def take(self, n: int = 1) -> None:
        self._prune()
        for _ in range(int(n)):
            self.calls.append(now_ts())

    def count(self) -> int:
        self._prune()
        return len(self.calls)


# Adaptive Exit Engine (AEE) - Core functions (Updated for Path-Space)
def calculate_aee_metrics(trade: dict, current_price: float, atr: float, candles: List[dict]) -> dict:
    """Calculate all AEE metrics for trade using path-space primitives"""
    
    pair = trade.get("pair", "")
    entry = _safe_float(trade.get("entry"))
    direction = str(trade.get("dir", "") or "")
    current_price = _safe_float(current_price)
    atr = _safe_float(atr)
    
    # Use enhanced market data hub for path-space primitives
    try:
        # Get path primitives from enhanced hub
        primitives = _require_runtime_hub().get_path_primitives(pair, entry, direction, atr)
        
        if primitives:
            # Extract path-space metrics
            progress = primitives.get("progress", 0.0)
            speed = primitives.get("speed", 0.0)
            velocity = primitives.get("velocity", 0.0)
            pullback = primitives.get("pullback", 0.0)
            local_high = primitives.get("local_high", current_price)
            local_low = primitives.get("local_low", current_price)
            efficiency = primitives.get("efficiency", 0.0)
            overlap = primitives.get("overlap", 0.0)
            data_quality = primitives.get("data_quality", DataQuality.DEGRADED)
            
            # Calculate additional AEE fields
            # Get AEE state if available
            aee_state = None
            hub = _require_runtime_hub()
            if hasattr(hub, 'get_aee_state'):
                aee_state = hub.get_aee_state(pair)
            
            # Calculate distance to TP if TP anchor exists
            dist_to_tp_atr = 0.0
            near_tp_band_atr = 0.0
            if aee_state and aee_state.tp_anchor and aee_state.tp_anchor > 0:
                dist_to_tp_atr = abs(aee_state.tp_anchor - current_price) / atr
                near_tp_band_atr = 1.0 if dist_to_tp_atr < 0.3 else 0.0
            
            # Get tick management state
            tick_mode = False
            armed_by = "none"
            if aee_state:
                tick_mode = aee_state.tick_mode
                armed_by = aee_state.armed_by
            
            # Get phase
            phase = "NONE"
            if aee_state:
                phase = aee_state.phase.value if hasattr(aee_state.phase, 'value') else str(aee_state.phase)
            
            # Get exit line if pulse logic uses it
            exit_line = None
            if aee_state and aee_state.pulse_exit_line:
                exit_line = aee_state.pulse_exit_line
            
            result = {
                "progress": progress,
                "speed": speed,
                "velocity": velocity,
                "pullback": pullback,
                "local_high": local_high,
                "local_low": local_low,
                "efficiency": efficiency,
                "overlap": overlap,
                "data_quality": data_quality,
                "source": "path_space",
                
                # AEE / tick management fields
                "phase": phase,
                "tick_mode": tick_mode,
                "armed_by": armed_by,
                "dist_to_tp_atr": dist_to_tp_atr,
                "near_tp_band_atr": near_tp_band_atr,
                "exit_line": exit_line
            }
            
            log_metrics(result)
            
            return result
    except Exception as e:
        # Fallback to candle-based calculation if path-space fails
        log_runtime("warning", f"AEE_PATH_SPACE_FALLBACK {pair}", error=str(e))
    
    # Fallback: Original candle-based calculation
    progress = abs(current_price - entry) / atr if atr > 0 else 0.0
    
    # Speed: |Δprice(last 3-5 bars)| / ATR
    speed_bars = min(5, max(0, len(candles) - 1))
    if speed_bars >= 3:
        c_last = _safe_float(candles[-1].get("c"))
        c_prev = _safe_float(candles[-speed_bars].get("c"))
        price_change = abs(c_last - c_prev) if (math.isfinite(c_last) and math.isfinite(c_prev)) else 0.0
        speed = price_change / atr if atr > 0 else 0
    else:
        speed = 0
    
    # Velocity: Speed_now - Speed_prev
    if len(candles) >= 10:
        c5 = _safe_float(candles[-5].get("c"))
        c10 = _safe_float(candles[-10].get("c"))
        prev_change = abs(c5 - c10) if (math.isfinite(c5) and math.isfinite(c10)) else 0.0
        speed_prev = prev_change / atr if atr > 0 else 0
        velocity = speed - speed_prev
    else:
        velocity = 0
    
    # Pullback: |Price - LocalHigh/Low| / ATR
    if len(candles) >= 20:
        highs = [_safe_float(c.get("h")) for c in candles[-20:]]
        lows = [_safe_float(c.get("l")) for c in candles[-20:]]
        highs = [h for h in highs if math.isfinite(h)]
        lows = [low for low in lows if math.isfinite(low)]
        local_high = max(highs) if highs else current_price
        local_low = min(lows) if lows else current_price
    else:
        local_high = current_price
        local_low = current_price
    
    if direction == "LONG":
        pullback = (local_high - current_price) / atr if atr > 0 else 0
    else:
        pullback = (current_price - local_low) / atr if atr > 0 else 0
    
    return {
        "progress": progress,
        "speed": speed,
        "velocity": velocity,
        "pullback": pullback,
        "local_high": local_high,
        "local_low": local_low,
        "efficiency": 0.0,  # Not available in candle mode
        "overlap": 0.0,     # Not available in candle mode
        "data_quality": DataQuality.DEGRADED,
        "source": "candle_fallback"
    }


def simulate_price_stream_update(pair: str, bid: float, ask: float, tick_cache: Optional[dict] = None):
    """Simulate price stream update for enhanced market data hub."""
    
    # Validate prices
    if not (is_valid_price(bid) and is_valid_price(ask)):
        return
    
    if bid > ask:
        return
    
    # Calculate spread
    spread_pips = to_pips(pair, ask - bid) if math.isfinite(ask) and math.isfinite(bid) else 0
    
    # Create price event
    event = PriceEvent(
        t_exchange=broker_now_ts(),
        t_local=now_ts(),
        bid=bid,
        ask=ask,
        mid=(bid + ask) / 2.0,
        spread_pips=spread_pips,
        tradeable=True,
        source="stream",
        quality=DataQuality.OK
    )
    
    # Add to enhanced market hub
    _require_runtime_hub().add_price_event(pair, event)

    # Update tick cache if provided (for tick-mode lookups)
    if tick_cache is not None:
        mid = (bid + ask) / 2.0
        tick_cache[pair] = {
            "ts": now_ts(),
            "data": {
                "bid": bid,
                "ask": ask,
                "mid": mid,
                "spread_pips": spread_pips,
                "t_exchange": event.t_exchange,
                "t_local": event.t_local,
                "tradeable": True,
            },
        }

def atr_pips(pair: str, atr_price: float) -> float:
    """Convert ATR price to pips."""
    ps = pip_size(pair)
    return float(Decimal(str(atr_price)) / ps) if ps else float("nan")


def spread_atr(pair: str, spread_pips: float, atr_price: float) -> float:
    """Calculate spread as fraction of ATR (universal metric)."""
    ap = atr_pips(pair, atr_price)
    if not math.isfinite(ap) or ap <= 0:
        return float("nan")
    return spread_pips / ap


def williams_r(candles: List[dict], n: int) -> float:
    """Compute Williams %R."""
    if not candles or n <= 0 or len(candles) < n:
        return float("nan")
    
    # Extract values with NaN checks
    highs = []
    lows = []
    for c in candles[-n:]:
        try:
            h = float(c["h"])
            low = float(c["l"])
            if math.isfinite(h) and math.isfinite(low):
                highs.append(h)
                lows.append(low)
        except (KeyError, ValueError, TypeError):
            return float("nan")
    
    # Need valid data
    if not highs or not lows:
        return float("nan")
    
    try:
        cl = float(candles[-1]["c"])
        if not math.isfinite(cl):
            return float("nan")
    except (KeyError, ValueError, TypeError):
        return float("nan")
    
    hh = max(highs)
    ll = min(lows)
    
    if hh == ll:
        return -50.0
    
    wr = -100.0 * ((hh - cl) / (hh - ll))
    return wr if math.isfinite(wr) else float("nan")


def compute_volume_z(candles: List[dict], win: int = 20) -> float:
    """Robust z-score of latest volume vs rolling window using median/MAD."""
    if not candles:
        return float("nan")
    vols = [float(c.get("volume", 0.0) or 0.0) for c in candles if isinstance(c, dict)]
    if len(vols) < max(5, win):
        return float("nan")
    recent = vols[-win:]
    latest = recent[-1]
    med = statistics.median(recent)
    deviations = [abs(v - med) for v in recent]
    mad = statistics.median(deviations) if deviations else 0.0
    denom = mad if mad > 0 else (statistics.pstdev(recent) or 0.0)
    if denom <= 0:
        return float("nan")
    return (latest - med) / denom


def _book_poll_interval(state: str, cadence: Optional["AdaptiveCadence"] = None) -> float:
    """Dynamic polling interval for order/position books based on state."""
    if cadence:
        return cadence.get_interval("books_sec")
    
    s = str(state or "").upper()
    if s in ("WATCH", "GET_READY", "ENTER", "MANAGING", "ARM_TICK_ENTRY"):
        return 15.0
    if s in ("WAIT", "PASS", "SKIP"):
        return 45.0
    return 30.0


def compute_book_metrics(pair: str, mid: float, order_book: Optional[dict], position_book: Optional[dict]) -> Dict[str, float]:
    """Compute order/position book metrics."""
    metrics: Dict[str, float] = {}
    if not (math.isfinite(mid) and mid > 0):
        return metrics
    pip = float(pip_size(pair))
    window_pips = 10.0
    window_price = window_pips * pip

    def _bucket_stats(book: Optional[dict]) -> Dict[str, float]:
        buckets = book.get("buckets", []) if isinstance(book, dict) else []
        cluster = 0.0
        wall_dist = float("inf")
        imb_sum = 0.0
        imb_count = 0
        for b in buckets:
            try:
                price = float(b.get("price"))
                lc = float(b.get("longCountPercent", 0.0))
                sc = float(b.get("shortCountPercent", 0.0))
            except Exception:
                continue
            if not math.isfinite(price):
                continue
            dist = abs(price - mid)
            if dist <= window_price:
                weight = max(lc + sc, 0.0)
                cluster += weight
                imb_sum += (lc - sc)
                imb_count += 1
                if weight >= 2.0:  # treat 2%+ as a wall bucket
                    wall_dist = min(wall_dist, dist)
        imbalance = (imb_sum / imb_count) if imb_count > 0 else 0.0
        if wall_dist == float("inf"):
            wall_dist = float("nan")
        return {"cluster": cluster, "imbalance": imbalance, "wall_dist": wall_dist}

    ob_stats = _bucket_stats(order_book)
    pb_stats = _bucket_stats(position_book)

    # Combine: sum clusters, avg imbalance, min wall distance
    metrics["cluster_density"] = ob_stats["cluster"] + pb_stats["cluster"]
    metrics["imbalance"] = (ob_stats["imbalance"] + pb_stats["imbalance"]) / 2.0
    metrics["wall_distance_pips"] = min(ob_stats["wall_dist"], pb_stats["wall_dist"]) / float(pip) if (
        math.isfinite(ob_stats["wall_dist"]) or math.isfinite(pb_stats["wall_dist"])
    ) else float("nan")

    # Trap score heuristic: high cluster near price with opposing tilt
    opposing_tilt = -metrics["imbalance"]
    metrics["trap_score"] = max(0.0, metrics["cluster_density"] * opposing_tilt)

    return metrics


def _poll_books(
    o: "OandaClient",
    states: Dict[str, "PairState"],
    price_map: Dict[str, Tuple[float, float]],
    book_cache: Dict[str, Dict[str, Any]],
    now: float,
    cadence: Optional["AdaptiveCadence"] = None,
) -> None:
    """Poll order/position books with state-driven cadence and update metrics."""
    if _api_budget_low_for_optional():
        remaining = _api_budget_remaining()
        log_throttled(
            "books_budget_skip",
            "BOOKS_FETCH_SKIPPED_BUDGET",
            {"remaining_calls": remaining, "reserve_core": API_BUDGET_RESERVE_CORE},
            min_interval=30.0,
        )
        return

    for pair, st in states.items():
        interval = _book_poll_interval(st.state, cadence)
        cache = book_cache.get(pair)
        if cache and (now - ffloat(cache.get("ts", 0.0), 0.0)) < interval:
            continue

        # Resolve midprice
        mid = float("nan")
        if pair in price_map:
            try:
                px = price_map[pair]
                if isinstance(px, (tuple, list)) and len(px) >= 2:
                    bid, ask = px[0], px[1]
                else:
                    bid, ask = float("nan"), float("nan")
                mid = (ffloat(bid, float("nan")) + ffloat(ask, float("nan"))) * 0.5
            except Exception:
                mid = float("nan")
        if not math.isfinite(mid):
            resp_price = oanda_call("pricing_book", o.pricing, pair)
            try:
                if resp_price:
                    bid, ask = resp_price
                    mid = (ffloat(bid, float("nan")) + ffloat(ask, float("nan"))) * 0.5
                    mid = (float(bid) + float(ask)) * 0.5
            except Exception:
                mid = float("nan")
        if not math.isfinite(mid):
            continue

        ob = oanda_call("order_book", o.order_book, pair)
        pb = oanda_call("position_book", o.position_book, pair)
        metrics = compute_book_metrics(pair, mid, ob, pb)
        st.book_metrics = metrics
        book_cache[pair] = {"ts": now, "order_book": ob, "position_book": pb, "mid": mid, "metrics": metrics}


def granularity_sec(gran: str) -> int:
    g = gran.upper()
    if g.startswith("S") and g[1:].isdigit():
        return int(g[1:])
    if g.startswith("M") and g[1:].isdigit():
        return int(g[1:]) * 60
    if g.startswith("H") and g[1:].isdigit():
        return int(g[1:]) * 3600
    if g == "H":
        return 3600
    if g.startswith("D") and g[1:].isdigit():
        return int(g[1:]) * 86400
    if g == "D":
        return 86400
    if g.startswith("W") and g[1:].isdigit():
        return int(g[1:]) * 604800
    if g == "W":
        return 604800
    return 0


def candles_valid(candles: List[dict], tf_sec: int = 0) -> bool:
    if len(candles) < 2:
        return False
    prev = None
    for c in candles:
        ts = c.get("time")
        if not isinstance(ts, (int, float)) or not math.isfinite(ts):
            return False
        try:
            o = ffloat(c.get("o"), float("nan"))
            h = ffloat(c.get("h"), float("nan"))
            low = ffloat(c.get("l"), float("nan"))
            cl = ffloat(c.get("c"), float("nan"))
        except Exception:
            return False
        if not all(math.isfinite(x) for x in (o, h, low, cl)):
            return False
        if h < low:
            return False
        if prev is not None:
            if ts <= prev:
                return False
            if tf_sec > 0 and (ts - prev) > (tf_sec * 3):
                return False
        prev = ts
    return True


def validate_candles(pair: str, candles: List[dict], tf_sec: int, allow_partial: bool = True) -> Tuple[bool, str]:
    if not candles:
        return False, "empty"
    if not candles_valid(candles, tf_sec=tf_sec):
        return False, "invalid_structure"
    if not allow_partial and not bool(candles[-1].get("complete", True)):
        return False, "partial_not_allowed"
    return True, "ok"


def validate_price(pair: str, bid: float, ask: float, source: str = "") -> bool:
    if not (is_valid_price(bid) and is_valid_price(ask)):
        return False
    if float(ask) <= float(bid):
        return False
    spread = to_pips(pair, float(ask) - float(bid))
    return math.isfinite(spread) and spread >= 0.0


def momentum(candles: List[dict], n: int) -> float:
    if not candles or n <= 0 or len(candles) <= n:
        return float("nan")
    try:
        return float(candles[-1]["c"]) - float(candles[-1 - n]["c"])
    except Exception:
        return float("nan")


def spread_size_mult(speed_class: str, spread_atr: float) -> float:
    # Economic engine is authoritative for spread gating; keep sizing neutral.
    return 1.0


def spread_size_mult_fe(spread_atr: float, speed_norm: float) -> float:
    # Economic engine is authoritative for spread gating; keep sizing neutral.
    return 1.0


def log_trade_attempt(
    *,
    pair: str,
    sig: Any,
    st: Any,
    speed_class: str,
    decision: str,
    reason: str,
    leg: str = "MAIN",
    state_from: Optional[str] = None,
    state_to: Optional[str] = None,
    extra: Optional[dict] = None,
    bar_complete: bool = True,
    bar_age_ms: float = 0.0,
) -> None:
    payload = {
        "event": "TRADE_ATTEMPT",
        "pair": pair,
        "setup": getattr(sig, "setup_name", ""),
        "direction": getattr(sig, "direction", ""),
        "state": getattr(st, "state", ""),
        "speed_class": speed_class,
        "decision": decision,
        "reason": reason,
        "leg": leg,
        "state_from": state_from,
        "state_to": state_to,
        "bar_complete": bar_complete,
        "bar_age_ms": bar_age_ms,
    }
    if isinstance(extra, dict):
        payload.update(extra)
    log_trade_event(payload)


def box_range(candles: List[dict], bars: int, use_prev: bool = True) -> Tuple[float, float, float]:
    """Compute box high/low over `bars` candles. use_prev=True excludes current candle."""
    need = bars + 1 if use_prev else bars
    if len(candles) < need:
        return float("nan"), float("nan"), float("nan")
    if use_prev:
        subset = candles[-(bars + 1) : -1]
    else:
        subset = candles[-bars:]
    hh = max(float(c["h"]) for c in subset)
    ll = min(float(c["l"]) for c in subset)
    return hh, ll, (hh - ll)


def ema(data: List[float], period: int) -> float:
    """Calculate EMA for given period."""
    if not data or period <= 0:
        return float('nan')
    
    multiplier = 2 / (period + 1)
    ema_val = data[0]
    
    for val in data[1:]:
        ema_val = (val * multiplier) + (ema_val * (1 - multiplier))
    
    return ema_val


def _median(vals: List[float]) -> float:
    if not vals:
        return float("nan")
    s = sorted(vals)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return float(s[mid])
    return 0.5 * (float(s[mid - 1]) + float(s[mid]))


def _mad(vals: List[float]) -> float:
    med = _median(vals)
    if not math.isfinite(med):
        return float("nan")
    devs = [abs(v - med) for v in vals]
    return _median(devs)


def _velocity_atr_per_sec(samples: List[Tuple[float, float]] | float, p1: Optional[float] = None, dt_sec: Optional[float] = None, atr: Optional[float] = None) -> float:
    # Legacy signature: _velocity_atr_per_sec(p0, p1, dt_sec, atr)
    if not isinstance(samples, list):
        p0 = float(samples)
        if p1 is None or dt_sec is None or atr is None:
            return 0.0
        if dt_sec <= 0 or atr <= 0:
            return 0.0
        return ((p1 - p0) / atr) / dt_sec

    if len(samples) < 2:
        return 0.0
    t1, v1 = samples[-2]
    t2, v2 = samples[-1]
    dt = t2 - t1
    if dt <= 0:
        return 0.0
    return (v2 - v1) / dt


def _microtrend_alive(samples: List[Tuple[float, float]] | List[dict], k: int = 3, direction: Optional[str] = None, atr: Optional[float] = None) -> bool:
    # Legacy signature: _microtrend_alive(candles, "LONG", atr)
    if isinstance(k, str):
        direction = k
        k = 3
    direction = str(direction or "LONG").upper()

    if not samples:
        return False

    # Candle list support
    if isinstance(samples[0], dict):
        closes: List[float] = []
        for c in samples[-int(k):]:
            try:
                    if isinstance(c, dict):
                        close_val = c.get("c")
                        if close_val is not None:
                            closes.append(float(close_val))
                    elif isinstance(c, (tuple, list)) and len(c) > 0:
                        closes.append(float(c[0]))
            except Exception:
                return False
        if len(closes) < int(k):
            return False
        if direction.startswith("S"):
            return all(closes[i] <= closes[i - 1] for i in range(1, len(closes)))
        return all(closes[i] >= closes[i - 1] for i in range(1, len(closes)))

    # Default sample list support
    if len(samples) < k:
        return False
    vals = [s[1] for s in samples[-k:]]
    return all(vals[i] >= vals[i - 1] for i in range(1, k))


def _new_runner_stats() -> dict:
    return {
        "count_total": 0,
        "sum_exit_atr_total": 0.0,
        "count_giveback": 0,
        "sum_exit_atr_giveback": 0.0,
        "count_other": 0,
        "sum_exit_atr_other": 0.0,
    }


def _update_runner_stats(stats: dict, reason: str, exit_atr: float) -> None:
    stats["count_total"] += 1
    stats["sum_exit_atr_total"] += exit_atr
    if reason == "RUNNER_GIVEBACK":
        stats["count_giveback"] += 1
        stats["sum_exit_atr_giveback"] += exit_atr
    else:
        stats["count_other"] += 1
        stats["sum_exit_atr_other"] += exit_atr


def _exit_log(tr: dict, reason: str, exit_atr: float, track: Optional[dict]) -> None:
    setup_name = str(tr.get("setup", ""))
    leg = "RUN" if "_RUN" in setup_name else "MAIN"
    setup_id = setup_id_from_name(setup_name)
    speed_class = speed_class_from_setup_name(setup_name)
    entry_ts = float(tr.get("ts", now_ts()))
    peak = float(track.get("peak", 0.0)) if track else 0.0
    max_dd = float(track.get("max_dd", 0.0)) if track else 0.0
    peak_ts = float(track.get("peak_ts", entry_ts)) if track else entry_ts
    log(
        f"{EMOJI_EXIT} EXIT {reason} {pair_tag(tr.get('pair', ''), tr.get('dir', ''))}",
        {
            "trade_id": int(tr.get("id", 0)),
            "entry_ts": entry_ts,
            "exit_ts": now_ts(),
            "duration_sec": round(now_ts() - entry_ts, 2),
            "exit_reason": reason,
            "setup_id": setup_id,
            "speed_class": speed_class,
            "leg": leg,
            "exit_atr": exit_atr,
            "bfe_atr": peak,
            "max_dd_atr": max_dd,
            "time_to_bfe_sec": (peak_ts - entry_ts),
        },
    )


def directional_closes(candles: List[dict], k: int, direction: str) -> bool:
    """Check for k consecutive directional bars. Works with partial candles."""
    if len(candles) < k + 1:
        return False
    if direction == "LONG":
        return all(float(candles[-i]["c"]) > float(candles[-i - 1]["c"]) for i in range(1, k + 1))
    if direction == "SHORT":
        return all(float(candles[-i]["c"]) < float(candles[-i - 1]["c"]) for i in range(1, k + 1))
    return False


def wick_sweep(candles: List[dict], L: int, atr_val: float) -> Tuple[bool, bool]:
    if len(candles) < L + 2 or not (atr_val > 0):
        return False, False
    swing_high = max(float(c["h"]) for c in candles[-L - 1 : -1])
    swing_low = min(float(c["l"]) for c in candles[-L - 1 : -1])
    cur = candles[-1]
    o = float(cur["o"])
    cl = float(cur["c"])
    h = float(cur["h"])
    low = float(cur["l"])
    upper_wick = h - max(o, cl)
    lower_wick = min(o, cl) - low
    sweep_up = (h > swing_high) and (cl < swing_high) and ((upper_wick / atr_val) >= SWEEP_ATR_THRESHOLD)
    sweep_dn = (low < swing_low) and (cl > swing_low) and ((lower_wick / atr_val) >= SWEEP_ATR_THRESHOLD)
    
    # Volume filter disabled - OANDA doesn't provide reliable volume data
    # When using a broker with volume data, uncomment and implement:
    # volume_ok = float(cur.get("volume", 0)) > avg_volume * 1.2
    
    return sweep_up, sweep_dn



@dataclass
class PairState:
    degraded: bool = False
    state: str = "SKIP"
    mode: str = "SLOW"
    last_alert: float = 0.0
    last_alert_key: str = ""
    last_trade: float = 0.0
    cooldown_until: float = 0.0
    state_since: float = 0.0
    neutral_bars: int = 0

    breakout_ts: float = 0.0
    breakout_dir: str = ""

    box_hi: float = float("nan")
    box_lo: float = float("nan")
    box_atr: float = float("nan")
    wr: float = float("nan")
    wr_prev: float = float("nan")
    m_norm: float = float("nan")
    atr_exec: float = float("nan")
    atr_long: float = float("nan")
    spread_pips: float = float("nan")
    spread_history: List[float] = field(default_factory=list)

    # Hysteresis tracking for GET_READY persistence
    get_ready_weak_scans: int = 0  # Count of consecutive weak scans
    last_close_history: List[float] = field(default_factory=list)  # Track last K closes for momentum check
    last_data_stale_log_ts: float = 0.0  # Track last DATA_STALE log timestamp

    # Debounce tracking for signal stability
    signal_debounce: Dict[str, float] = field(default_factory=dict)  # Track when each signal condition first met
    last_candle_time: float = 0.0  # Track candle time for stale detection
    stale_poll_count: int = 0  # Count polls with no OHLC change
    last_tick_mid: float = float("nan")  # Midprice from previous tick tracking
    tick_times: List[float] = field(default_factory=list)  # Timestamps of recent ticks for activity calc
    ticks_per_min: float = 0.0  # Rolling ticks per minute estimate
    entry_arm: dict = field(default_factory=dict)  # Tick-entry arming state
    vol_z: float = float("nan")  # Volume regime z-score
    book_metrics: Dict[str, float] = field(default_factory=dict)  # Order/position book metrics
    # Bucket A runtime params
    params: dict = field(default_factory=dict)
    aee_metrics_obj: dict = field(default_factory=dict)

    # State transition attributes for GATE-16
    entry_arms: bool = False  # Entry conditions met
    ready_at: float = 0.0  # Timestamp when ready
    entry_triggered: bool = False  # Entry trigger activated


PAIR_ENTRY_HARD_BLOCK: Dict[str, Dict[str, Any]] = {}


def _signal_time_fields(signal: Any) -> Tuple[float, float]:
    if isinstance(signal, dict):
        created_ts = float(signal.get("created_ts", signal.get("created_at", 0.0)) or 0.0)
        expires_ts = float(signal.get("expires_ts", signal.get("expires_at", 0.0)) or 0.0)
        return created_ts, expires_ts
    created_ts = float(getattr(signal, "created_at", 0.0) or 0.0)
    expires_ts = float(getattr(signal, "expires_at", 0.0) or 0.0)
    return created_ts, expires_ts


def can_enter(pair: str, spread: float, now: float, signal: Optional[Any] = None) -> Tuple[bool, str]:
    pair = normalize_pair(pair)
    if DRY_RUN_ONLY:
        return False, REASON_DRY_RUN
    if not ALLOW_ENTRIES:
        return False, REASON_ENTRIES_DISABLED
    if pair in EXIT_BLOCKED_PAIRS:
        return False, REASON_PAIR_EXIT_BLOCKED
    if pair in PAIR_ENTRY_HARD_BLOCK:
        return False, REASON_PAIR_HARD_BLOCK
    # Spread-based entry gating is handled exclusively by check_economic_viability()
    if signal is not None:
        created_ts, expires_ts = _signal_time_fields(signal)
        if expires_ts > 0.0 and now > expires_ts:
            return False, REASON_STALE_SIGNAL
        if created_ts > 0.0 and (now - created_ts) > SIGNAL_STALE_TTL_SEC:
            return False, REASON_STALE_SIGNAL
    return True, ""


def get_latest_spread_pips(pair: str) -> Optional[float]:
    pair = normalize_pair(pair)
    try:
        stream = get_pricing_stream()
        if stream is not None:
            tick = stream.get_latest_tick(pair)
            if tick and math.isfinite(float(tick.spread_pips)):
                return float(tick.spread_pips)
    except Exception:
        pass
    return None


def validate_get_ready_payload(signal: Any) -> Tuple[bool, str]:
    required = ("trigger_mode", "entry_zone_price", "invalid_level", "tp_anchor_price", "created_at", "expires_at")
    for field_name in required:
        if getattr(signal, field_name, None) is None:
            return False, f"{REASON_SCHEMA_FAIL}:{field_name}_MISSING"
    if str(getattr(signal, "trigger_mode", "") or "").upper() not in ("BREAK", "RECLAIM", "RESUME"):
        return False, f"{REASON_SCHEMA_FAIL}:trigger_mode_INVALID"
    for field_name in ("entry_zone_price", "invalid_level", "created_at", "expires_at"):
        value = getattr(signal, field_name, None)
        if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            return False, f"{REASON_SCHEMA_FAIL}:{field_name}_NOT_FLOAT"
    return True, ""


def update_tick_stats(st: PairState, pair: str, bid: float, ask: float, now: Optional[float] = None) -> None:
    """Update rolling tick activity (ticks per minute) using mid-price changes."""
    pair = normalize_pair(pair)
    if now is None:
        now = now_ts()
    if not (is_valid_price(bid) and is_valid_price(ask)):
        return
    try:
        mid = (float(bid) + float(ask)) * 0.5
    except Exception:
        return
    if not math.isfinite(mid):
        return
    tick = tick_size(pair)
    last_mid = st.last_tick_mid
    # Count a tick when mid moves at least one tick size, or on first sample
    if (not math.isfinite(last_mid)) or abs(mid - last_mid) >= tick:
        st.tick_times.append(float(now))
        st.last_tick_mid = mid
    window = max(1.0, float(TICK_WINDOW_SEC))
    cutoff = now - window
    # Prune old ticks
    while st.tick_times and st.tick_times[0] < cutoff:
        st.tick_times.pop(0)
    st.ticks_per_min = (len(st.tick_times) * 60.0) / window


def _entry_trigger_for_setup(setup_id: int) -> str:
    # BREAK: momentum breakout entries
    # RECLAIM: failed breakout/sweep type entries
    # RESUME: continuation/exhaustion entries after pullback
    if setup_id in (1, 6):
        return "BREAK"
    if setup_id in (4, 5):
        return "RECLAIM"
    return "RESUME"


def _arm_tick_entry(st: PairState, sig: "SignalDef", entry_px: float, box_hi: float, box_lo: float, atr: float, now: float) -> None:
    trigger = _entry_trigger_for_setup(sig.setup_id)
    st.entry_arm = {
        "ts": now,
        "expires_at": float(sig.expires_at or (now + SIGNAL_STALE_TTL_SEC)),
        "trigger": trigger,
        "dir": sig.direction,
        "entry_px": float(entry_px),
        "entry_zone_price": float(sig.entry_zone_price) if sig.entry_zone_price is not None else float(entry_px),
        "invalid_level": float(sig.invalid_level) if sig.invalid_level is not None else float("nan"),
        "tp_anchor_price": float(sig.tp_anchor_price) if sig.tp_anchor_price is not None else float("nan"),
        "box_hi": float(box_hi) if math.isfinite(box_hi) else float("nan"),
        "box_lo": float(box_lo) if math.isfinite(box_lo) else float("nan"),
        "atr": float(atr) if math.isfinite(atr) else float("nan"),
        "pullback_seen": False,
        "reclaim_seen": False,
        "setup_id": sig.setup_id,
        "setup_name": sig.setup_name,
        "sig": {
            "pair": sig.pair,
            "setup_id": sig.setup_id,
            "setup_name": sig.setup_name,
            "direction": sig.direction,
            "mode": sig.mode,
            "ttl_sec": sig.ttl_sec,
            "pg_t": sig.pg_t,
            "pg_atr": sig.pg_atr,
            "tp1_atr": sig.tp1_atr,
            "tp2_atr": sig.tp2_atr,
            "sl_atr": sig.sl_atr,
            "reason": sig.reason,
            "created_at": sig.created_at,
            "expires_at": sig.expires_at,
            "trigger_mode": sig.trigger_mode,
            "entry_zone_price": sig.entry_zone_price,
            "invalid_level": sig.invalid_level,
            "tp_anchor_price": sig.tp_anchor_price,
        }
    }
    
    # T1-17 Tick Mode Integrity Gate Validation - ARMED
    try:
        from pathlib import Path
        import json
        proof_dirs = sorted(Path(__file__).parent.glob("proof_artifacts/*"))
        if proof_dirs:
            latest_proof = proof_dirs[-1]
            
            # Check if arming is allowed in current state
            allowed_states = ["GET_READY", "ARM_TICK_ENTRY"]
            is_allowed = st.state in allowed_states
            
            # Load or initialize tick mode events
            tick_file = latest_proof / "tick_mode_events.jsonl"
            if tick_file.exists():
                events = []
                for line in tick_file.read_text().strip().splitlines():
                    events.append(json.loads(line))
            else:
                events = []
            
            # Add ARMED event
            events.append({
                "pair": sig.pair,
                "action": "ARMED",
                "state": st.state,
                "timestamp": now,
                "setup_id": sig.setup_id
            })
            
            # Keep last 100 events
            events = events[-100:]
            tick_file.write_text("\n".join(json.dumps(e) for e in events) + "\n")
            
            # Write report
            report = {
                "status": "PASS" if is_allowed else "FAIL",
                "pair": sig.pair,
                "events": events[-10:],  # Last 10 events
                "latest": {"action": "ARMED", "state": st.state}
            }
            
            report_file = latest_proof / f"tick_mode_report_{sig.pair}.json"
            report_file.write_text(json.dumps(report, indent=2))
            
            if not is_allowed:
                log_runtime("error", "T1-17_TICK_MODE_FAIL",
                          pair=sig.pair, action="ARMED", state=st.state)
    except Exception as e:
        log_runtime("warning", "T1-17_ARTIFACT_ERROR", pair=sig.pair, error=str(e))


def _tick_entry_triggered(st: PairState, bid: float, ask: float, now: float) -> Tuple[bool, str]:
    arm = st.entry_arm or {}
    if not arm:
        return False, "not_armed"
    expires_at = float(arm.get("expires_at", 0.0) or 0.0)
    if (expires_at > 0.0 and now > expires_at) or ((now - float(arm.get("ts", now))) > SIGNAL_STALE_TTL_SEC):
        st.entry_arm = {}
        return False, "arm_expired"
    direction = str(arm.get("dir", ""))
    trigger = str(arm.get("trigger", "BREAK"))
    atr = float(arm.get("atr", float("nan")))
    entry_px = float(arm.get("entry_zone_price", arm.get("entry_px", float("nan"))))
    box_hi = float(arm.get("box_hi", float("nan")))
    box_lo = float(arm.get("box_lo", float("nan")))
    px = ask if direction == "LONG" else bid
    pair = normalize_pair(str(arm.get("sig", {}).get("pair", "EUR_USD") or "EUR_USD"))
    spread_pips = abs(ask - bid) / max(float(pip_size(pair)), 1e-9)
    # Spread-based entry gating is handled exclusively by check_economic_viability()

    if math.isfinite(atr) and atr > 0.0 and math.isfinite(entry_px):
        dist_atr = abs(px - entry_px) / atr
        if dist_atr > ARM_ENTRY_DIST_ATR:
            return False, "dist_too_far"

    # BREAK trigger: cross box boundary or entry price
    if trigger == "BREAK":
        buf = ENTRY_BREAK_BUFFER_ATR * atr if math.isfinite(atr) and atr > 0 else 0.0
        lvl = box_hi if direction == "LONG" and math.isfinite(box_hi) else (box_lo if math.isfinite(box_lo) else entry_px)
        if direction == "LONG":
            crossed = px >= (lvl + buf)
            if not crossed:
                return False, "break_not_crossed"
            confirm_disp = (ENTRY_CONFIRM_DISP_ATR * atr if atr > 0 else 0.0)
            if (px - lvl) < confirm_disp:
                return False, "break_confirm_disp_fail"
            return True, "break"
        crossed = px <= (lvl - buf)
        if not crossed:
            return False, "break_not_crossed"
        confirm_disp = (ENTRY_CONFIRM_DISP_ATR * atr if atr > 0 else 0.0)
        if (lvl - px) < confirm_disp:
            return False, "break_confirm_disp_fail"
        return True, "break"

    # RECLAIM trigger: require an adverse move, then reclaim the boundary
    if trigger == "RECLAIM":
        rb = ENTRY_RECLAIM_BUFFER_ATR * atr if math.isfinite(atr) and atr > 0 else 0.0
        if direction == "LONG":
            if px < (box_hi if math.isfinite(box_hi) else entry_px):
                arm["reclaim_seen"] = True
            if not arm.get("reclaim_seen"):
                return False, "reclaim_wait_adverse"
            if px >= ((box_hi if math.isfinite(box_hi) else entry_px) + rb):
                return True, "reclaim"
        else:
            if px > (box_lo if math.isfinite(box_lo) else entry_px):
                arm["reclaim_seen"] = True
            if not arm.get("reclaim_seen"):
                return False, "reclaim_wait_adverse"
            if px <= ((box_lo if math.isfinite(box_lo) else entry_px) - rb):
                return True, "reclaim"
        return False, "reclaim_wait_reclaim"

    # RESUME trigger: see pullback then resume in direction
    if math.isfinite(atr) and atr > 0:
        pullback_lvl = entry_px - (ENTRY_PULLBACK_ATR * atr) if direction == "LONG" else entry_px + (ENTRY_PULLBACK_ATR * atr)
        resume_lvl = entry_px + (ENTRY_CONFIRM_DISP_ATR * atr) if direction == "LONG" else entry_px - (ENTRY_CONFIRM_DISP_ATR * atr)
    else:
        pullback_lvl = entry_px
        resume_lvl = entry_px
    if direction == "LONG":
        if px <= pullback_lvl:
            arm["pullback_seen"] = True
        if not arm.get("pullback_seen"):
            return False, "resume_wait_pullback"
        if px >= resume_lvl:
            return True, "resume"
    else:
        if px >= pullback_lvl:
            arm["pullback_seen"] = True
        if not arm.get("pullback_seen"):
            return False, "resume_wait_pullback"
        if px <= resume_lvl:
            return True, "resume"
    return False, "resume_wait_confirm"


def _sig_from_entry_arm(arm: dict) -> Optional["SignalDef"]:
    sig = arm.get("sig") if isinstance(arm, dict) else None
    if not isinstance(sig, dict):
        return None
    try:
        return SignalDef(
            pair=normalize_pair(sig.get("pair", "")),
            setup_id=int(sig.get("setup_id", 0)),
            setup_name=str(sig.get("setup_name", "")),
            direction=str(sig.get("direction", "")),
            mode=str(sig.get("mode", "")),
            ttl_sec=int(sig.get("ttl_sec", 0)),
            pg_t=int(sig.get("pg_t", 0)),
            pg_atr=float(sig.get("pg_atr", 0.0)),
            tp1_atr=float(sig.get("tp1_atr", 0.0)),
            tp2_atr=float(sig.get("tp2_atr", 0.0)),
            sl_atr=float(sig.get("sl_atr", 0.0)),
            reason=str(sig.get("reason", "")),
            size_mult=float(sig.get("size_mult", 1.0)),
            created_at=float(sig.get("created_at", 0.0) or 0.0),
            expires_at=float(sig.get("expires_at", 0.0) or 0.0),
            trigger_mode=str(sig.get("trigger_mode", "")) if sig.get("trigger_mode") is not None else None,
            entry_zone_price=ffloat(sig.get("entry_zone_price"), float("nan")) if sig.get("entry_zone_price") is not None else None,
            invalid_level=ffloat(sig.get("invalid_level"), float("nan")) if sig.get("invalid_level") is not None else None,
            tp_anchor_price=ffloat(sig.get("tp_anchor_price"), float("nan")) if sig.get("tp_anchor_price") is not None else None,
        )
    except Exception:
        return None

@dataclass
class SignalDef:
    pair: str
    setup_id: int
    setup_name: str
    direction: str
    mode: str
    ttl_sec: int
    pg_t: int
    pg_atr: float
    tp1_atr: float
    tp2_atr: float
    sl_atr: float
    reason: str
    size_mult: float = 1.0
    created_at: float = 0.0
    expires_at: float = 0.0
    trigger_mode: Optional[str] = None
    entry_zone_price: Optional[float] = None
    invalid_level: Optional[float] = None
    tp_anchor_price: Optional[float] = None
    
    def __post_init__(self):
        if self.created_at == 0.0:
            self.created_at = time.time()
        if self.expires_at == 0.0:
            self.expires_at = self.created_at + SIGNAL_STALE_TTL_SEC
    
    def is_expired(self) -> bool:
        """Check if signal has expired based on TTL."""
        return (time.time() - self.created_at) > self.ttl_sec


class DB:
    def __init__(self, path: str):
        self.path = path
        self._init()

    def _con(self) -> sqlite3.Connection:
        con = sqlite3.connect(self.path)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        return con

    def _init(self) -> None:
        con = self._con()
        cur = con.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pair_state (
              pair TEXT PRIMARY KEY,
              ts REAL,
              state TEXT,
              mode TEXT,
              last_alert REAL,
              last_trade REAL,
              cooldown_until REAL,
              state_since REAL,
              neutral_bars INTEGER,
              breakout_ts REAL,
              breakout_dir TEXT
            )
            """
        )
        # Add atr_long column if it doesn't exist
        try:
            cur.execute("ALTER TABLE pair_state ADD COLUMN atr_long REAL")
        except sqlite3.OperationalError:
            # Column already exists
            pass
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
              client_id TEXT PRIMARY KEY,
              ts REAL,
              pair TEXT,
              setup TEXT,
              leg TEXT,
              units INTEGER,
              sl REAL,
              tp REAL,
              oanda_order_id TEXT,
              oanda_transaction_id TEXT,
              status TEXT,
              raw_json TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              ts REAL,
              pair TEXT,
              setup TEXT,
              dir TEXT,
              mode TEXT,
              units INTEGER,
              entry REAL,
              atr_entry REAL,
              ttl_sec INTEGER,
              pg_t INTEGER,
              pg_atr REAL,
              state TEXT,
              note TEXT
            )
            """
        )
        con.commit()
        con.close()
        self._ensure_columns()

    def _ensure_columns(self) -> None:
        con = self._con()
        cur = con.cursor()
        cur.execute("PRAGMA table_info(orders)")
        cols = {row[1] for row in cur.fetchall()}
        if "setup" not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN setup TEXT")
        if "leg" not in cols:
            cur.execute("ALTER TABLE orders ADD COLUMN leg TEXT")
        
        # Add oanda_trade_id to trades table
        cur.execute("PRAGMA table_info(trades)")
        trade_cols = {row[1] for row in cur.fetchall()}
        if "oanda_trade_id" not in trade_cols:
            cur.execute("ALTER TABLE trades ADD COLUMN oanda_trade_id TEXT")
        
        # Add Adaptive Exit Engine columns
        if "aee_phase" not in trade_cols:
            cur.execute("ALTER TABLE trades ADD COLUMN aee_phase TEXT")
        if "aee_entry_protected" not in trade_cols:
            cur.execute("ALTER TABLE trades ADD COLUMN aee_entry_protected INTEGER DEFAULT 0")
        if "aee_local_high" not in trade_cols:
            cur.execute("ALTER TABLE trades ADD COLUMN aee_local_high REAL")
        if "aee_local_low" not in trade_cols:
            cur.execute("ALTER TABLE trades ADD COLUMN aee_local_low REAL")
        
        con.commit()
        con.close()

    def load_states(self, pairs: List[str]) -> Dict[str, PairState]:
        states = {normalize_pair(p): PairState(state_since=now_ts()) for p in pairs}
        con = self._con()
        cur = con.cursor()
        cur.execute(
            "SELECT pair,state,mode,last_alert,last_trade,cooldown_until,state_since,neutral_bars,breakout_ts,breakout_dir,atr_long FROM pair_state"
        )
        for row in cur.fetchall():
            pair = normalize_pair(str(row[0]))
            if pair not in states:
                continue
            st = states[pair]
            st.state = str(row[1])
            st.mode = str(row[2])
            st.last_alert = float(row[3] or 0.0)
            st.last_trade = float(row[4] or 0.0)
            st.cooldown_until = float(row[5] or 0.0)
            st.state_since = float(row[6] or st.state_since)
            st.neutral_bars = int(row[7] or 0)
            st.breakout_ts = float(row[8] or 0.0)
            st.breakout_dir = str(row[9] or "")
            st.atr_long = float(row[10] or float("nan"))
        con.close()
        return states

    def save_state(self, pair: str, st: PairState) -> None:
        pair = normalize_pair(pair)
        con = self._con()
        cur = con.cursor()
        cur.execute(
            """
            INSERT INTO pair_state(pair,ts,state,mode,last_alert,last_trade,cooldown_until,state_since,neutral_bars,breakout_ts,breakout_dir,atr_long)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(pair) DO UPDATE SET
              ts=excluded.ts,
              state=excluded.state,
              mode=excluded.mode,
              last_alert=excluded.last_alert,
              last_trade=excluded.last_trade,
              cooldown_until=excluded.cooldown_until,
              state_since=excluded.state_since,
              neutral_bars=excluded.neutral_bars,
              breakout_ts=excluded.breakout_ts,
              breakout_dir=excluded.breakout_dir,
              atr_long=excluded.atr_long
            """,
            (
                pair,
                now_ts(),
                st.state,
                st.mode,
                st.last_alert,
                st.last_trade,
                st.cooldown_until,
                st.state_since,
                st.neutral_bars,
                st.breakout_ts,
                st.breakout_dir,
                st.atr_long,
            ),
        )
        con.commit()
        con.close()

    def record_order(
        self,
        *,
        client_id: str,
        pair: str,
        setup: str,
        leg: str,
        units: int,
        sl: float,
        tp: float,
        oanda_order_id: str,
        oanda_transaction_id: str,
        status: str,
        raw: dict,
    ) -> None:
        pair = normalize_pair(pair)
        con = self._con()
        cur = con.cursor()
        cur.execute(
            """
            INSERT OR REPLACE INTO orders(client_id,ts,pair,setup,leg,units,sl,tp,oanda_order_id,oanda_transaction_id,status,raw_json)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                client_id,
                now_ts(),
                pair,
                setup,
                leg,
                int(units),
                float(sl),
                float(tp),
                str(oanda_order_id),
                str(oanda_transaction_id),
                str(status),
                json.dumps(raw, default=str),
            ),
        )
        con.commit()
        con.close()

    def recent_order_attempt(self, *, pair: str, setup: str, leg: str, since_ts: float) -> bool:
        pair = normalize_pair(pair)
        con = self._con()
        cur = con.cursor()
        cur.execute(
            "SELECT 1 FROM orders WHERE pair=? AND setup=? AND leg=? AND ts>=? LIMIT 1",
            (pair, setup