#!/usr/bin/env python3
import argparse
import json
import os
import sys
import time
from typing import Any, Dict, Optional, Tuple

import requests


def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    return "" if v is None else str(v).strip()


def _base_url(env: str) -> str:
    e = (env or "practice").strip().lower()
    if e in ("practice", "fxpractice", "demo", "paper", "test"):
        return "https://api-fxpractice.oanda.com"
    if e in ("live", "fxtrade", "prod", "production"):
        return "https://api-fxtrade.oanda.com"
    raise ValueError(f"unsupported OANDA_ENV={env}")


def _request(
    method: str,
    base: str,
    account_id: str,
    api_key: str,
    path: str,
    body: Optional[Dict[str, Any]] = None,
) -> Tuple[int, Dict[str, Any]]:
    url = f"{base}{path}"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    r = requests.request(method, url, headers=headers, data=json.dumps(body) if body is not None else None, timeout=30)
    try:
        payload = r.json()
    except Exception:
        payload = {"raw": r.text}
    return r.status_code, payload


def _pricing_mid(base: str, account_id: str, api_key: str, pair: str) -> float:
    status, payload = _request(
        "GET",
        base,
        account_id,
        api_key,
        f"/v3/accounts/{account_id}/pricing?instruments={pair}",
    )
    if status >= 300:
        raise RuntimeError(f"pricing_error status={status} payload={payload}")
    prices = payload.get("prices") or []
    if not prices:
        raise RuntimeError(f"pricing_empty payload={payload}")
    p = prices[0]
    bids = p.get("bids") or []
    asks = p.get("asks") or []
    bid = float((bids[0] or {}).get("price")) if bids else 0.0
    ask = float((asks[0] or {}).get("price")) if asks else 0.0
    if bid <= 0 or ask <= 0:
        raise RuntimeError(f"pricing_bad_bid_ask payload={payload}")
    return (bid + ask) / 2.0


def _pip_size(pair: str) -> float:
    return 0.01 if pair.endswith("_JPY") else 0.0001


def _round_px(px: float, pair: str) -> str:
    digits = 3 if pair.endswith("_JPY") else 5
    return f"{px:.{digits}f}"


def main() -> int:
    ap = argparse.ArgumentParser(description="Place and immediately close a canary order on OANDA practice.")
    ap.add_argument("--pair", default="EUR_USD")
    ap.add_argument("--units", type=int, default=1000)
    ap.add_argument("--side", choices=["LONG", "SHORT"], default="LONG")
    ap.add_argument("--tp-pips", type=float, default=6.0)
    ap.add_argument("--sl-pips", type=float, default=6.0)
    ap.add_argument("--execute", action="store_true", help="Actually place and close the canary order.")
    args = ap.parse_args()

    api_key = _env("OANDA_API_KEY")
    account_id = _env("OANDA_ACCOUNT_ID")
    env = _env("OANDA_ENV", "practice")
    if not api_key or not account_id:
        print("missing OANDA_API_KEY or OANDA_ACCOUNT_ID", file=sys.stderr)
        return 2

    env_norm = env.lower()
    if env_norm not in ("practice", "fxpractice", "demo", "paper", "test"):
        print(f"refusing non-practice env OANDA_ENV={env}", file=sys.stderr)
        return 2

    base = _base_url(env)
    pair = args.pair.strip().upper()
    units = abs(int(args.units))
    if args.side == "SHORT":
        units = -units

    mid = _pricing_mid(base, account_id, api_key, pair)
    pip = _pip_size(pair)
    if units > 0:
        tp = mid + args.tp_pips * pip
        sl = mid - args.sl_pips * pip
    else:
        tp = mid - args.tp_pips * pip
        sl = mid + args.sl_pips * pip

    order_body = {
        "order": {
            "type": "MARKET",
            "instrument": pair,
            "units": str(units),
            "timeInForce": "FOK",
            "positionFill": "DEFAULT",
            "takeProfitOnFill": {"price": _round_px(tp, pair)},
            "stopLossOnFill": {"price": _round_px(sl, pair)},
            "clientExtensions": {"id": f"canary-{int(time.time())}"},
        }
    }

    print(json.dumps({
        "env": env,
        "account_id": account_id,
        "pair": pair,
        "units": units,
        "mid": mid,
        "tp": _round_px(tp, pair),
        "sl": _round_px(sl, pair),
        "execute": bool(args.execute),
    }, indent=2))

    if not args.execute:
        print("dry_run_only=true")
        return 0

    status, placed = _request("POST", base, account_id, api_key, f"/v3/accounts/{account_id}/orders", order_body)
    print(json.dumps({"order_status": status, "order_response": placed}, indent=2))
    if status >= 300:
        return 1

    trade_id = (
        (((placed.get("orderFillTransaction") or {}).get("tradeOpened") or {}).get("tradeID"))
        or (((placed.get("orderFillTransaction") or {}).get("tradesOpened") or [{}])[0].get("tradeID"))
    )
    if not trade_id:
        print("no_trade_id_opened=true")
        return 0

    close_body = {"units": "ALL"}
    c_status, closed = _request("PUT", base, account_id, api_key, f"/v3/accounts/{account_id}/trades/{trade_id}/close", close_body)
    print(json.dumps({"close_status": c_status, "close_response": closed, "trade_id": trade_id}, indent=2))
    return 0 if c_status < 300 else 1


if __name__ == "__main__":
    raise SystemExit(main())

