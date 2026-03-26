from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import phone_bot

ROOT = Path(__file__).resolve().parent

PROFIT_BRANCHES: dict[str, dict[str, str]] = {
    "AEE_BAND_EARLY_PROFIT_LOCK": {
        "fn": "check_early_profit_lock",
        "trigger_kind": "AEE_EARLY_PROFIT_LOCK_TRIGGER",
        "proof_file": "aee_proof_early_profit_lock.json",
    },
    "AEE_BAND_PROFIT_STALL_EXIT": {
        "fn": "check_profit_stall_exit",
        "trigger_kind": "AEE_PROFIT_STALL_EXIT_TRIGGER",
        "proof_file": "aee_proof_profit_stall_exit.json",
    },
    "AEE_BAND_GIVEBACK_EXIT": {
        "fn": "check_giveback_exit",
        "trigger_kind": "AEE_GIVEBACK_EXIT_TRIGGER",
        "proof_file": "aee_proof_giveback_exit.json",
    },
    "AEE_BAND_EXTENSION_HOLD": {
        "fn": "check_extension_hold",
        "trigger_kind": "AEE_PROFIT_BRANCH_DEBUG",
        "proof_file": "aee_proof_extension_hold.json",
    },
    "AEE_BAND_EXTENSION_DECAY_EXIT": {
        "fn": "check_extension_decay_exit",
        "trigger_kind": "AEE_EXTENSION_DECAY_EXIT_TRIGGER",
        "proof_file": "aee_proof_extension_decay_exit.json",
    },
}


def _iso_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _base_metrics() -> dict[str, Any]:
    return {
        "hold_sec": 12.0,
        "green_age_sec": 6.0,
        "time_since_best_favorable_sec": 2.0,
        "favorable_stall_s": 2.0,
        "net_r": 0.03,
        "mae_r": -0.10,
        "best_favorable_r": 0.10,
        "favorable_giveback_r": 0.02,
        "progress_vel_r_per_sec": 0.002,
        "rolling_adverse_move_r_short": 0.01,
        "rolling_favorable_move_r_short": 0.02,
    }


def _scenarios() -> dict[str, list[dict[str, Any]]]:
    b = _base_metrics()
    return {
        "AEE_BAND_EARLY_PROFIT_LOCK": [
            {"name": "obvious_trigger", "expected": True, "metrics": {**b, "net_r": 0.06, "best_favorable_r": 0.14, "favorable_giveback_r": 0.05, "progress_vel_r_per_sec": 0.001}},
            {"name": "borderline_trigger", "expected": True, "metrics": {**b, "net_r": 0.031, "best_favorable_r": 0.081, "favorable_giveback_r": 0.021, "progress_vel_r_per_sec": 0.0038}},
            {"name": "control_no_trigger", "expected": False, "metrics": {**b, "net_r": 0.02, "best_favorable_r": 0.09, "favorable_giveback_r": 0.01, "progress_vel_r_per_sec": 0.006}},
        ],
        "AEE_BAND_PROFIT_STALL_EXIT": [
            {"name": "obvious_trigger", "expected": True, "metrics": {**b, "net_r": 0.05, "best_favorable_r": 0.16, "favorable_giveback_r": 0.06, "favorable_stall_s": 8.0, "progress_vel_r_per_sec": 0.0012}},
            {"name": "borderline_trigger", "expected": True, "metrics": {**b, "net_r": 0.031, "best_favorable_r": 0.11, "favorable_giveback_r": 0.031, "favorable_stall_s": 4.1, "progress_vel_r_per_sec": 0.0019}},
            {"name": "control_no_trigger", "expected": False, "metrics": {**b, "net_r": 0.06, "best_favorable_r": 0.20, "favorable_giveback_r": 0.01, "favorable_stall_s": 0.8, "progress_vel_r_per_sec": 0.008}},
        ],
        "AEE_BAND_GIVEBACK_EXIT": [
            {"name": "obvious_trigger", "expected": True, "metrics": {**b, "net_r": 0.04, "best_favorable_r": 0.22, "favorable_giveback_r": 0.11, "time_since_best_favorable_sec": 2.2}},
            {"name": "borderline_trigger", "expected": True, "metrics": {**b, "net_r": 0.03, "best_favorable_r": 0.13, "favorable_giveback_r": 0.05, "time_since_best_favorable_sec": 1.6}},
            {"name": "control_no_trigger", "expected": False, "metrics": {**b, "net_r": 0.07, "best_favorable_r": 0.19, "favorable_giveback_r": 0.02, "time_since_best_favorable_sec": 1.0}},
        ],
        "AEE_BAND_EXTENSION_HOLD": [
            {"name": "obvious_trigger", "expected": True, "metrics": {**b, "net_r": 0.10, "best_favorable_r": 0.18, "favorable_giveback_r": 0.03, "progress_vel_r_per_sec": 0.005}},
            {"name": "borderline_trigger", "expected": True, "metrics": {**b, "net_r": 0.061, "best_favorable_r": 0.11, "favorable_giveback_r": 0.02, "progress_vel_r_per_sec": 0.0031}},
            {"name": "control_no_trigger", "expected": False, "metrics": {**b, "net_r": 0.04, "best_favorable_r": 0.10, "favorable_giveback_r": 0.05, "progress_vel_r_per_sec": 0.0005}},
        ],
        "AEE_BAND_EXTENSION_DECAY_EXIT": [
            {"name": "obvious_trigger", "expected": True, "metrics": {**b, "best_favorable_r": 0.22, "favorable_giveback_r": 0.12, "progress_vel_r_per_sec": 0.0007, "time_since_best_favorable_sec": 3.0}},
            {"name": "borderline_trigger", "expected": True, "metrics": {**b, "best_favorable_r": 0.15, "favorable_giveback_r": 0.062, "progress_vel_r_per_sec": 0.001, "time_since_best_favorable_sec": 2.1}},
            {"name": "control_no_trigger", "expected": False, "metrics": {**b, "best_favorable_r": 0.18, "favorable_giveback_r": 0.03, "progress_vel_r_per_sec": 0.004, "time_since_best_favorable_sec": 1.0}},
        ],
    }


def _threshold_snapshot(branch: str) -> dict[str, float]:
    if branch == "AEE_BAND_EARLY_PROFIT_LOCK":
        return {
            "min_net_r": float(phone_bot.AEE_P_EARLY_LOCK_MIN_NET_R),
            "min_best_r": float(phone_bot.AEE_P_EARLY_LOCK_MIN_BEST_R),
            "min_giveback_r": float(phone_bot.AEE_P_EARLY_LOCK_MIN_GIVEBACK_R),
        }
    if branch == "AEE_BAND_PROFIT_STALL_EXIT":
        return {
            "min_best_r": float(phone_bot.AEE_P_STALL_MIN_BEST_R),
            "min_green_age_sec": float(phone_bot.AEE_P_STALL_MIN_GREEN_AGE_SEC),
            "min_time_since_peak_sec": float(phone_bot.AEE_P_STALL_MIN_TIME_SINCE_PEAK_SEC),
        }
    if branch == "AEE_BAND_GIVEBACK_EXIT":
        return {
            "min_best_r": float(phone_bot.AEE_P_GIVEBACK_MIN_BEST_R),
            "min_giveback_r": float(phone_bot.AEE_P_GIVEBACK_MIN_R),
            "min_giveback_frac": float(phone_bot.AEE_P_GIVEBACK_MIN_FRAC),
        }
    if branch == "AEE_BAND_EXTENSION_HOLD":
        return {
            "min_net_r": float(phone_bot.AEE_P_EXTENSION_HOLD_MIN_NET_R),
            "min_vel_r_per_sec": float(phone_bot.AEE_P_EXTENSION_HOLD_MIN_VEL_R_PER_SEC),
            "max_giveback_frac": float(phone_bot.AEE_P_EXTENSION_HOLD_MAX_GIVEBACK_FRAC),
        }
    return {
        "min_best_r": float(phone_bot.AEE_P_EXTENSION_DECAY_MIN_BEST_R),
        "max_vel_r_per_sec": float(phone_bot.AEE_P_EXTENSION_DECAY_MAX_VEL_R_PER_SEC),
        "min_giveback_frac": float(phone_bot.AEE_P_EXTENSION_DECAY_MIN_GIVEBACK_FRAC),
    }


def run_replay_proofs() -> dict[str, Any]:
    matrix = _scenarios()
    result: dict[str, Any] = {}
    for branch, meta in PROFIT_BRANCHES.items():
        fn = getattr(phone_bot, meta["fn"], None)
        rows = []
        pass_count = 0
        fail_count = 0
        false_pos = 0
        false_neg = 0
        for sc in matrix[branch]:
            expected = bool(sc["expected"])
            if fn is None:
                got = False
                reason = ""
                detail = {}
            else:
                got, reason, detail = fn(dict(sc["metrics"]))
                got = bool(got)
            ok = (got == expected) and (reason == branch or (not got and not expected))
            pass_count += int(ok)
            fail_count += int(not ok)
            false_pos += int(got and not expected)
            false_neg += int((not got) and expected)
            rows.append({"scenario": sc["name"], "expected": expected, "got": got, "reason": reason, "ok": ok, "detail": detail})

        payload = {
            "branch": branch,
            "generated_at": _iso_now(),
            "scenarios_tested": len(rows),
            "pass_count": pass_count,
            "fail_count": fail_count,
            "false_positives": false_pos,
            "false_negatives": false_neg,
            "threshold_snapshot": _threshold_snapshot(branch),
            "representative_outputs": rows[:3],
            "all_results": rows,
        }
        (ROOT / meta["proof_file"]).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        result[branch] = payload
    return result


def _parse_ts(rec: dict[str, Any]) -> float | None:
    value = rec.get("ts")
    if value is None:
        value = rec.get("ts_utc")
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        pass
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s).timestamp()
    except Exception:
        return None


def _select_window() -> tuple[float, float] | None:
    candidates = [
        ROOT / "logs/mvp_aee_branch_runtime_10m_proof_tuned_window.txt",
        ROOT / "logs/mvp_aee_branch_runtime_10m_window.txt",
        ROOT / "logs/mvp_aee_branch_runtime_5m_window.txt",
    ]
    for p in candidates:
        if not p.exists():
            continue
        lines = [x.strip() for x in p.read_text(encoding="utf-8", errors="ignore").splitlines() if x.strip()]
        if len(lines) < 2:
            continue
        try:
            return float(lines[0]), float(lines[1])
        except Exception:
            continue
    return None


def _is_branch_hit(branch: str, trigger_kind: str, rec: dict[str, Any]) -> bool:
    kind = str(rec.get("kind") or rec.get("event") or "")
    reason = str(rec.get("exit_reason") or rec.get("reason") or "")
    if reason == branch:
        return True
    if kind == trigger_kind:
        return True
    if branch == "AEE_BAND_EXTENSION_HOLD" and kind == "AEE_PROFIT_BRANCH_DEBUG":
        return bool(rec.get("extension_hold_trigger", False))
    return False


def build_runtime_validation() -> dict[str, Any]:
    window = _select_window()
    rows: list[dict[str, Any]] = []
    path = ROOT / "logs/trades.jsonl"
    if path.exists():
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            t = _parse_ts(rec)
            if window and t is not None and not (window[0] <= t <= window[1]):
                continue
            rows.append(rec)

    all_exits = [r for r in rows if str(r.get("kind") or r.get("event") or "") in {"AEE_EXIT_SNAPSHOT_PRE", "AEE_EXIT_SNAPSHOT_POST", "AEE_DECAY_EXIT", "AEE_STALL_EXIT", "AEE_PRE_SL_EXIT", "AEE_PANIC_EXIT"}]
    duration_sec = (window[1] - window[0]) if window else 0.0
    duration_hr = duration_sec / 3600.0 if duration_sec > 0 else 0.0
    realized_pips = [float(r.get("realized_pips", r.get("pnl_pips", 0.0)) or 0.0) for r in all_exits]
    realized_usd = [float(r.get("realized_usd", 0.0) or 0.0) for r in all_exits]
    green_roundtrip_loss = 0
    green_rows = 0

    out: dict[str, Any] = {
        "generated_at": _iso_now(),
        "window": list(window) if window else None,
        "window_duration_sec": duration_sec,
        "realized_pips_per_hour": (sum(realized_pips) / duration_hr) if duration_hr > 0 else 0.0,
        "realized_usd_per_hour": (sum(realized_usd) / duration_hr) if duration_hr > 0 else 0.0,
        "close_cycle_capture_rate": (len(all_exits) / duration_hr) if duration_hr > 0 else 0.0,
        "green_roundtrip_loss_rate": 0.0,
        "avg_giveback_at_exit": 0.0,
        "branches": {},
    }

    givebacks = []
    for branch, meta in PROFIT_BRANCHES.items():
        hits = [r for r in rows if _is_branch_hit(branch, meta["trigger_kind"], r)]
        branch_givebacks = [float(r.get("favorable_giveback_r", 0.0) or 0.0) for r in hits]
        givebacks.extend(branch_givebacks)
        trade_type_split: dict[str, int] = {}
        path_bucket_split: dict[str, int] = {}
        realized_rs = []
        for r in hits:
            tt = str(r.get("trade_type") or r.get("leg_type") or "UNKNOWN").upper()
            trade_type_split[tt] = int(trade_type_split.get(tt, 0) + 1)
            pb = str(r.get("path_bucket") or "UNKNOWN")
            path_bucket_split[pb] = int(path_bucket_split.get(pb, 0) + 1)
            try:
                realized_rs.append(float(r.get("net_r", 0.0) or 0.0))
            except Exception:
                pass
            try:
                if float(r.get("best_favorable_r", 0.0) or 0.0) > 0 and float(r.get("net_r", 0.0) or 0.0) <= 0:
                    green_roundtrip_loss += 1
                if float(r.get("best_favorable_r", 0.0) or 0.0) > 0:
                    green_rows += 1
            except Exception:
                pass

        def _avg(key: str) -> float:
            vals = []
            for r in hits:
                try:
                    vals.append(float(r.get(key, 0.0) or 0.0))
                except Exception:
                    pass
            return (sum(vals) / len(vals)) if vals else 0.0

        out["branches"][branch] = {
            "trigger_count": len(hits),
            "average_hold_time_sec": _avg("hold_sec"),
            "average_best_favorable_r": _avg("best_favorable_r"),
            "average_realized_r": (sum(realized_rs) / len(realized_rs)) if realized_rs else 0.0,
            "average_giveback_at_exit": (sum(branch_givebacks) / len(branch_givebacks)) if branch_givebacks else 0.0,
            "trade_type_split": trade_type_split,
            "path_bucket_distribution": path_bucket_split,
            "sample_rows": hits[:3],
        }

    out["green_roundtrip_loss_rate"] = (green_roundtrip_loss / green_rows) if green_rows > 0 else 0.0
    out["avg_giveback_at_exit"] = (sum(givebacks) / len(givebacks)) if givebacks else 0.0

    (ROOT / "aee_profit_runtime_validation.json").write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    return out


def build_status_dashboard(replay: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
    dashboard = {"generated_at": _iso_now(), "branches": {}}
    for branch, meta in PROFIT_BRANCHES.items():
        implemented = hasattr(phone_bot, meta["fn"])
        replay_pass = bool(replay.get(branch, {}).get("fail_count", 1) == 0)
        runtime_count = int(runtime.get("branches", {}).get(branch, {}).get("trigger_count", 0) or 0)
        runtime_pass = runtime_count > 0
        if not implemented:
            status = "UNBUILT"
        elif not replay_pass:
            status = "BUILT"
        elif replay_pass and not runtime_pass:
            status = "REPLAY_PROVEN"
        else:
            status = "RUNTIME_PROVEN"
        dashboard["branches"][branch] = {
            "implemented": bool(implemented),
            "replay_proven": "pass" if replay_pass else "fail",
            "runtime_proven": "pass" if runtime_pass else "fail",
            "current_thresholds": replay.get(branch, {}).get("threshold_snapshot", {}),
            "trigger_count": runtime_count,
            "average_realized_r": float(runtime.get("branches", {}).get(branch, {}).get("average_realized_r", 0.0) or 0.0),
            "average_hold_time_sec": float(runtime.get("branches", {}).get(branch, {}).get("average_hold_time_sec", 0.0) or 0.0),
            "current_status": status,
        }

    (ROOT / "aee_profit_side_status_dashboard.json").write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")
    return dashboard


def main() -> None:
    replay = run_replay_proofs()
    runtime = build_runtime_validation()
    dashboard = build_status_dashboard(replay, runtime)
    print(
        json.dumps(
            {
                "proof_files": [v["proof_file"] for v in PROFIT_BRANCHES.values()],
                "runtime_validation": "aee_profit_runtime_validation.json",
                "dashboard": "aee_profit_side_status_dashboard.json",
                "status": {k: v["current_status"] for k, v in dashboard["branches"].items()},
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
