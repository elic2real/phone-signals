from __future__ import annotations

import json
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional


_LOCK = threading.Lock()
_COLLECTOR_ROOT: Optional[Path] = None
_ACTIVE_PROOF_DIR: Optional[Path] = None


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dump(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _latest_proof_dir(root: Path) -> Optional[Path]:
    dirs = [p for p in root.glob("*") if p.is_dir()]
    if not dirs:
        return None
    return sorted(dirs, key=lambda p: p.stat().st_mtime)[-1]


def init_collector(base_dir: Path, proof_dir: Optional[Path] = None) -> Path:
    root = Path(base_dir) / "proof_artifacts"
    root.mkdir(parents=True, exist_ok=True)
    global _COLLECTOR_ROOT, _ACTIVE_PROOF_DIR
    _COLLECTOR_ROOT = root
    if proof_dir is not None:
        _ACTIVE_PROOF_DIR = Path(proof_dir)
        _ACTIVE_PROOF_DIR.mkdir(parents=True, exist_ok=True)
    else:
        _ACTIVE_PROOF_DIR = _latest_proof_dir(root)
        if _ACTIVE_PROOF_DIR is None:
            _ACTIVE_PROOF_DIR = root / f"collector_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
            _ACTIVE_PROOF_DIR.mkdir(parents=True, exist_ok=True)
    return _ACTIVE_PROOF_DIR


def get_collector_root() -> Optional[Path]:
    return _COLLECTOR_ROOT


def get_active_proof_dir() -> Optional[Path]:
    return _ACTIVE_PROOF_DIR


def _target_file_for_path(path: str) -> Optional[str]:
    p = str(path or "")
    if "/candles" in p:
        return "oanda_candles_raw.json"
    if "/pricing" in p:
        return "oanda_pricing_raw.json"
    if "/summary" in p:
        return "oanda_accounts_raw.json"
    if "/instruments" in p and "/candles" not in p and "/orderBook" not in p and "/positionBook" not in p:
        return "oanda_instruments_raw.json"
    return None


def _normalize_payload_for_target(target: str, payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload
    if target == "oanda_candles_raw.json":
        candles = payload.get("candles")
        return candles if isinstance(candles, list) else payload
    if target == "oanda_pricing_raw.json":
        return payload
    if target == "oanda_accounts_raw.json":
        acct = payload.get("account")
        return acct if isinstance(acct, dict) else payload
    if target == "oanda_instruments_raw.json":
        instruments = payload.get("instruments")
        return instruments if isinstance(instruments, list) else payload
    return payload


def record_oanda_payload(
    *,
    method: str,
    path: str,
    url: str,
    status: Optional[int],
    params: Optional[dict],
    request_body: Optional[dict],
    response_payload: Any,
    error: Optional[str] = None,
) -> None:
    proof_dir = _ACTIVE_PROOF_DIR
    if proof_dir is None:
        return

    with _LOCK:
        event = {
            "ts": _utc_iso(),
            "method": method,
            "path": path,
            "url": url,
            "status": status,
            "params": params or {},
            "request_body": request_body or {},
            "error": error,
            "response": response_payload,
        }

        http_file = proof_dir / "oanda_http_raw.json"
        events = _load_json(http_file, [])
        if not isinstance(events, list):
            events = []
        events.append(event)
        events = events[-500:]
        _json_dump(http_file, events)

        target_name = _target_file_for_path(path)
        if target_name:
            target_file = proof_dir / target_name
            normalized = _normalize_payload_for_target(target_name, response_payload)
            _json_dump(target_file, normalized)
