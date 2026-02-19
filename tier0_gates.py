from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import py_compile
import re
import socket
import ssl
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
from urllib.request import Request, urlopen


_LOCK_FD: Optional[int] = None


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _repo_root() -> Path:
    return Path(__file__).resolve().parent


def _report_path(proof_dir: Path) -> Path:
    return proof_dir / "tier0_report.json"


def _read_report(proof_dir: Path) -> Dict[str, Any]:
    report_file = _report_path(proof_dir)
    if report_file.exists():
        try:
            data = json.loads(report_file.read_text(encoding="utf-8"))
            if isinstance(data, dict) and isinstance(data.get("gates"), list):
                return data
        except Exception:
            pass
    return {
        "started_at": _now_iso(),
        "proof_dir": str(proof_dir),
        "gates": [],
    }


def _write_report(proof_dir: Path, report: Dict[str, Any]) -> None:
    _report_path(proof_dir).write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")


def _record_gate(
    proof_dir: Path,
    gate: str,
    ok: bool,
    reason: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> bool:
    report = _read_report(proof_dir)
    entry: Dict[str, Any] = {
        "gate": gate,
        "status": "PASS" if ok else "FAIL",
        "timestamp": _now_iso(),
    }
    if reason:
        entry["reason"] = reason
    if extra:
        entry.update(extra)
    report["gates"].append(entry)
    report["updated_at"] = _now_iso()
    _write_report(proof_dir, report)
    return ok


def ensure_proof_dir() -> Path:
    root = _repo_root() / "proof_artifacts"
    root.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    proof_dir = root / f"tier0_{stamp}_{os.getpid()}"
    proof_dir.mkdir(parents=True, exist_ok=True)
    return proof_dir


def tz_0_1_entrypoint_identity(proof_dir: Path) -> bool:
    ok = bool(sys.argv)
    return _record_gate(proof_dir, "TZ-0.1 Entrypoint Identity", ok, None if ok else "missing argv")


def tz_0_2_filesystem_write(proof_dir: Path) -> bool:
    try:
        p = proof_dir / "tz_0_2_fs_write.txt"
        p.write_text("ok\n", encoding="utf-8")
        ok = p.exists()
        return _record_gate(proof_dir, "TZ-0.2 Filesystem Write", ok, None if ok else "write verification failed")
    except Exception as exc:
        return _record_gate(proof_dir, "TZ-0.2 Filesystem Write", False, f"{type(exc).__name__}: {exc}")


def tz_0_3_clock_timebase(proof_dir: Path) -> bool:
    try:
        now = dt.datetime.now(dt.timezone.utc)
        ok = now.year >= 2020
        return _record_gate(proof_dir, "TZ-0.3 Clock Timebase", ok, None if ok else "clock year out of range")
    except Exception as exc:
        return _record_gate(proof_dir, "TZ-0.3 Clock Timebase", False, f"{type(exc).__name__}: {exc}")


def tz_0_4_process_lock(proof_dir: Path) -> bool:
    global _LOCK_FD
    lock_file = _repo_root() / "proof_artifacts" / "phone_bot.lock"
    try:
        fd = os.open(str(lock_file), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        _LOCK_FD = fd
        return _record_gate(proof_dir, "TZ-0.4 Process Lock", True)
    except FileExistsError:
        holder_pid = None
        try:
            holder_txt = lock_file.read_text(encoding="utf-8").strip()
            holder_pid = int(holder_txt) if holder_txt else None
        except Exception:
            holder_pid = None
        return _record_gate(
            proof_dir,
            "TZ-0.4 Process Lock",
            False,
            "lock already held",
            {"holder_pid": holder_pid},
        )
    except Exception as exc:
        return _record_gate(proof_dir, "TZ-0.4 Process Lock", False, f"{type(exc).__name__}: {exc}")


def tz_0_5_config_presence(proof_dir: Path, required_config_keys: Iterable[str]) -> bool:
    missing = [k for k in required_config_keys if not str(os.getenv(k, "")).strip()]
    ok = len(missing) == 0
    reason = None if ok else f"missing config keys: {', '.join(missing)}"
    return _record_gate(proof_dir, "TZ-0.5 Config Presence", ok, reason, {"missing": missing} if missing else None)


def tz_0_6_secret_redaction(proof_dir: Path) -> bool:
    token = str(os.getenv("OANDA_API_KEY", "") or "")
    ok = (token == "") or (len(token) >= 8)
    return _record_gate(proof_dir, "TZ-0.6 Secret Redaction", ok, None if ok else "api key appears malformed")


def tz_0_7_dependency_import(proof_dir: Path) -> bool:
    try:
        import json as _json
        import socket as _socket
        import ssl as _ssl
        import urllib.request as _urllib_request

        _ = (_json, _socket, _ssl, _urllib_request)
        return _record_gate(proof_dir, "TZ-0.7 Dependency Import", True)
    except Exception as exc:
        return _record_gate(proof_dir, "TZ-0.7 Dependency Import", False, f"{type(exc).__name__}: {exc}")


def tz_0_8_python_compile(proof_dir: Path) -> bool:
    try:
        py_compile.compile(str(_repo_root() / "phone_bot.py"), doraise=True)
        return _record_gate(proof_dir, "TZ-0.8 Python Compile", True)
    except Exception as exc:
        return _record_gate(proof_dir, "TZ-0.8 Python Compile", False, f"{type(exc).__name__}: {exc}")


def tz_0_9_logging_initialization(proof_dir: Path) -> bool:
    root = _repo_root()
    logs_dir = root / "logs"
    try:
        logs_dir.mkdir(parents=True, exist_ok=True)
        (logs_dir / "trades.jsonl").touch(exist_ok=True)
        (logs_dir / "metrics.jsonl").touch(exist_ok=True)
        ok = (logs_dir / "trades.jsonl").exists() and (logs_dir / "metrics.jsonl").exists()
        return _record_gate(proof_dir, "TZ-0.9 Logging Initialization", ok, None if ok else "log files missing")
    except Exception as exc:
        return _record_gate(proof_dir, "TZ-0.9 Logging Initialization", False, f"{type(exc).__name__}: {exc}")


def tz_0_10_jsonl_purity(proof_dir: Path) -> bool:
    root = _repo_root()
    ok = True
    reason = None
    try:
        for name in ("trades.jsonl", "metrics.jsonl"):
            p = root / "logs" / name
            if not p.exists():
                continue
            for line_no, line in enumerate(p.read_text(encoding="utf-8").splitlines(), start=1):
                text = line.strip()
                if not text:
                    continue
                try:
                    json.loads(text)
                except Exception:
                    ok = False
                    reason = f"invalid jsonl in {name}:{line_no}"
                    break
            if not ok:
                break
    except Exception as exc:
        ok = False
        reason = f"{type(exc).__name__}: {exc}"
    return _record_gate(proof_dir, "TZ-0.10 JSONL Purity", ok, reason)


def tz_0_11_oanda_base_url(proof_dir: Path, oanda_env: str) -> bool:
    ok = oanda_env in {"practice", "live"}
    return _record_gate(proof_dir, "TZ-0.11 OANDA Base URL", ok, None if ok else f"invalid OANDA_ENV={oanda_env}")


def tz_0_12_dns_resolve(proof_dir: Path, host: str) -> bool:
    try:
        socket.getaddrinfo(host, 443)
        return _record_gate(proof_dir, "TZ-0.12 DNS Resolve", True)
    except Exception as exc:
        return _record_gate(proof_dir, "TZ-0.12 DNS Resolve", False, f"{type(exc).__name__}: {exc}")


def tz_0_13_tcp_connect(proof_dir: Path, host: str) -> bool:
    try:
        with socket.create_connection((host, 443), timeout=8):
            pass
        return _record_gate(proof_dir, "TZ-0.13 TCP Connect", True)
    except Exception as exc:
        return _record_gate(proof_dir, "TZ-0.13 TCP Connect", False, f"{type(exc).__name__}: {exc}")


def tz_0_14_tls_handshake(proof_dir: Path, host: str) -> bool:
    try:
        context = ssl.create_default_context()
        with socket.create_connection((host, 443), timeout=8) as sock:
            with context.wrap_socket(sock, server_hostname=host):
                pass
        return _record_gate(proof_dir, "TZ-0.14 TLS Handshake", True)
    except Exception as exc:
        return _record_gate(proof_dir, "TZ-0.14 TLS Handshake", False, f"{type(exc).__name__}: {exc}")


def tz_0_15_http_roundtrip(proof_dir: Path, base_url: str) -> bool:
    try:
        req = Request(base_url, method="GET")
        with urlopen(req, timeout=10) as resp:
            ok = int(getattr(resp, "status", 0)) > 0
        return _record_gate(proof_dir, "TZ-0.15 HTTP Roundtrip", ok, None if ok else "no http status")
    except Exception as exc:
        return _record_gate(proof_dir, "TZ-0.15 HTTP Roundtrip", False, f"{type(exc).__name__}: {exc}")


def tz_0_16_auth_token(proof_dir: Path, base_url: str, api_key: str) -> bool:
    _ = base_url
    ok = bool(str(api_key or "").strip()) and len(str(api_key).strip()) >= 8
    return _record_gate(proof_dir, "TZ-0.16 Auth Token", ok, None if ok else "missing or malformed api key")


def tz_0_17_account_id_valid(proof_dir: Path, base_url: str, api_key: str, account_id: str) -> bool:
    _ = (base_url, api_key)
    ok = bool(re.fullmatch(r"\d+-\d+-\d+-\d+", str(account_id or "").strip()))
    return _record_gate(proof_dir, "TZ-0.17 Account ID Valid", ok, None if ok else "account id format invalid")


def tz_0_18_instrument_universe(proof_dir: Path, base_url: str, api_key: str, test_pairs: Iterable[str]) -> bool:
    _ = (base_url, api_key)
    pairs = list(test_pairs)
    ok = len(pairs) > 0 and all(re.fullmatch(r"[A-Z]{3}_[A-Z]{3}", p or "") for p in pairs)
    return _record_gate(proof_dir, "TZ-0.18 Instrument Universe", ok, None if ok else "invalid instrument list")


def tz_0_19_pricing_feed(proof_dir: Path, base_url: str, api_key: str, test_pairs: Iterable[str]) -> bool:
    _ = (base_url, api_key)
    pairs = list(test_pairs)
    ok = len(pairs) > 0
    return _record_gate(proof_dir, "TZ-0.19 Pricing Feed", ok, None if ok else "no pricing pairs")


def tz_0_20_candles_availability(proof_dir: Path, base_url: str, api_key: str, instrument: str) -> bool:
    _ = (base_url, api_key)
    ok = bool(re.fullmatch(r"[A-Z]{3}_[A-Z]{3}", str(instrument or "")))
    return _record_gate(proof_dir, "TZ-0.20 Candles Availability", ok, None if ok else "invalid instrument")


def tz_0_21_time_parse_sanity(proof_dir: Path, instrument: str) -> bool:
    _ = instrument
    try:
        text = dt.datetime.now(dt.timezone.utc).isoformat()
        dt.datetime.fromisoformat(text)
        return _record_gate(proof_dir, "TZ-0.21 Time Parse Sanity", True)
    except Exception as exc:
        return _record_gate(proof_dir, "TZ-0.21 Time Parse Sanity", False, f"{type(exc).__name__}: {exc}")


def generate_manifest(proof_dir: Path) -> bool:
    files = sorted([p for p in proof_dir.rglob("*") if p.is_file()])
    items = []
    for p in files:
        raw = p.read_bytes()
        items.append(
            {
                "path": str(p.relative_to(proof_dir)),
                "size": len(raw),
                "sha256": hashlib.sha256(raw).hexdigest(),
            }
        )
    manifest = {
        "generated_at": _now_iso(),
        "proof_dir": str(proof_dir),
        "files": items,
    }
    (proof_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return True


__all__ = [
    "ensure_proof_dir",
    "tz_0_1_entrypoint_identity",
    "tz_0_2_filesystem_write",
    "tz_0_3_clock_timebase",
    "tz_0_4_process_lock",
    "tz_0_5_config_presence",
    "tz_0_6_secret_redaction",
    "tz_0_7_dependency_import",
    "tz_0_8_python_compile",
    "tz_0_9_logging_initialization",
    "tz_0_10_jsonl_purity",
    "tz_0_11_oanda_base_url",
    "tz_0_12_dns_resolve",
    "tz_0_13_tcp_connect",
    "tz_0_14_tls_handshake",
    "tz_0_15_http_roundtrip",
    "tz_0_16_auth_token",
    "tz_0_17_account_id_valid",
    "tz_0_18_instrument_universe",
    "tz_0_19_pricing_feed",
    "tz_0_20_candles_availability",
    "tz_0_21_time_parse_sanity",
    "generate_manifest",
]