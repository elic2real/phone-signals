#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tune_map import ALLOWLIST, CLAMPS

MAX_KNOBS_PER_WRITE = 12
FILE_PATH = Path("tunes/manual_overrides.json")


def _load() -> dict:
    if not FILE_PATH.exists():
        return {"version": "", "overrides": {}, "history": []}
    return json.loads(FILE_PATH.read_text(encoding="utf-8"))


def _save(d: dict) -> None:
    FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    FILE_PATH.write_text(json.dumps(d, indent=2, sort_keys=True), encoding="utf-8")


def _ver() -> str:
    return datetime.now(timezone.utc).strftime("manual-%Y%m%dT%H%M%SZ")


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_set = sub.add_parser("set")
    p_set.add_argument("key")
    p_set.add_argument("value")
    p_set.add_argument("--expiry-hours", type=float, default=0.0)

    sub.add_parser("show")

    p_clear = sub.add_parser("clear")
    p_clear.add_argument("key")

    p_rb = sub.add_parser("rollback")
    p_rb.add_argument("--steps", type=int, default=1)

    args = ap.parse_args()
    d = _load()
    d.setdefault("overrides", {})
    d.setdefault("history", [])

    if args.cmd == "show":
        print(json.dumps(d, indent=2, sort_keys=True))
        return 0

    if args.cmd == "set":
        if args.key not in ALLOWLIST:
            raise SystemExit(f"key not allowed: {args.key}")
        if len(d["overrides"]) >= MAX_KNOBS_PER_WRITE and args.key not in d["overrides"]:
            raise SystemExit("max knobs reached")
        try:
            v = float(args.value)
            if v.is_integer():
                v = int(v)
        except Exception:
            v = args.value
        if args.key in CLAMPS and isinstance(v, (int, float)):
            lo, hi = CLAMPS[args.key]
            v = max(lo, min(hi, v))
        d["history"].append({"ts": _ver(), "action": "set", "key": args.key, "prev": d["overrides"].get(args.key)})
        d["overrides"][args.key] = v
        if args.expiry_hours > 0:
            d["expiry_utc"] = (datetime.now(timezone.utc) + timedelta(hours=args.expiry_hours)).isoformat()

    elif args.cmd == "clear":
        d["history"].append({"ts": _ver(), "action": "clear", "key": args.key, "prev": d["overrides"].get(args.key)})
        d["overrides"].pop(args.key, None)

    elif args.cmd == "rollback":
        steps = max(1, int(args.steps))
        for _ in range(steps):
            if not d["history"]:
                break
            h = d["history"].pop()
            if h.get("action") in {"set", "clear"}:
                prev = h.get("prev")
                key = h.get("key")
                if prev is None:
                    d["overrides"].pop(key, None)
                else:
                    d["overrides"][key] = prev

    d["version"] = _ver()
    _save(d)
    print("MANUAL_OVERRIDE_SAVED", d["version"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
