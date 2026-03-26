from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import phone_bot

ROOT = Path(__file__).resolve().parent

BRANCHES: dict[str, dict[str, str]] = {
    "AEE_FAST_ADVERSE_EXIT": {
        "fn": "check_fast_adverse_exit",
        "trigger_kind": "AEE_FAST_ADVERSE_EXIT_TRIGGER",
        "proof_file": "aee_proof_fast_adverse_exit.json",
    },
    "AEE_NEVER_GREEN_TIMEOUT": {
        "fn": "check_never_green_timeout",
        "trigger_kind": "AEE_NEVER_GREEN_TIMEOUT_TRIGGER",
        "proof_file": "aee_proof_never_green_timeout.json",
    },
    "AEE_PROFIT_STALL_EXIT": {
        "fn": "check_profit_stall_exit",
        "trigger_kind": "AEE_PROFIT_STALL_EXIT_TRIGGER",
        "proof_file": "aee_proof_profit_stall_exit.json",
    },
    "AEE_PRE_SL_EXIT": {
        "fn": "check_pre_sl_exit",
        "trigger_kind": "AEE_PRE_SL_PROTECTION_TRIGGER",
        "proof_file": "aee_proof_pre_sl_exit.json",
    },
    "AEE_PANIC_EXIT": {
        "fn": "check_panic_exit",
        "trigger_kind": "AEE_PANIC_EXIT_TRIGGER",
        "proof_file": "aee_proof_panic_exit.json",
    },
}


def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _base_metrics() -> dict[str, Any]:
    return {
        "hold_sec": 15.0,
        "net_r": -0.01,
        "mae_r": -0.20,
        "best_favorable_r": 0.12,
        "favorable_giveback_r": 0.02,
        "progress_vel_r_per_sec": 0.0,
        "favorable_stall_s": 2.0,
        "rolling_window_sec_short": 2.0,
        "rolling_adverse_move_r_short": 0.01,
        "rolling_favorable_move_r_short": 0.01,
        "rolling_adverse_vel_r_per_sec_short": -0.005,
    }


def _scenario_matrix() -> dict[str, list[dict[str, Any]]]:
    return {
        "AEE_FAST_ADVERSE_EXIT": [
            {
                "name": "obvious_pass",
                "expected": True,
                "metrics": {
                    **_base_metrics(),
                    "hold_sec": 1.2,
                    "rolling_window_sec_short": 1.5,
                    "rolling_adverse_move_r_short": 0.18,
                    "rolling_adverse_vel_r_per_sec_short": -0.09,
                },
            },
            {
                "name": "borderline_pass",
                "expected": True,
                "metrics": {
                    **_base_metrics(),
                    "hold_sec": 0.8,
                    "rolling_window_sec_short": 2.8,
                    "rolling_adverse_move_r_short": 0.105,
                    "rolling_adverse_vel_r_per_sec_short": -0.036,
                },
            },
            {
                "name": "control_no_trigger",
                "expected": False,
                "metrics": {
                    **_base_metrics(),
                    "rolling_window_sec_short": 4.2,
                    "rolling_adverse_move_r_short": 0.20,
                    "rolling_adverse_vel_r_per_sec_short": -0.08,
                },
            },
        ],
        "AEE_NEVER_GREEN_TIMEOUT": [
            {
                "name": "obvious_pass",
                "expected": True,
                "metrics": {
                    **_base_metrics(),
                    "hold_sec": 36.0,
                    "best_favorable_r": 0.03,
                    "net_r": -0.02,
                    "progress_vel_r_per_sec": 0.0005,
                },
            },
            {
                "name": "borderline_pass",
                "expected": True,
                "metrics": {
                    **_base_metrics(),
                    "hold_sec": 24.1,
                    "best_favorable_r": 0.079,
                    "net_r": 0.019,
                    "progress_vel_r_per_sec": 0.0014,
                },
            },
            {
                "name": "control_no_trigger",
                "expected": False,
                "metrics": {
                    **_base_metrics(),
                    "hold_sec": 18.0,
                    "best_favorable_r": 0.02,
                    "net_r": -0.01,
                },
            },
        ],
        "AEE_PROFIT_STALL_EXIT": [
            {
                "name": "obvious_pass",
                "expected": True,
                "metrics": {
                    **_base_metrics(),
                    "net_r": 0.07,
                    "best_favorable_r": 0.22,
                    "favorable_giveback_r": 0.07,
                    "favorable_stall_s": 8.0,
                    "progress_vel_r_per_sec": 0.0008,
                },
            },
            {
                "name": "borderline_pass",
                "expected": True,
                "metrics": {
                    **_base_metrics(),
                    "net_r": 0.031,
                    "best_favorable_r": 0.11,
                    "favorable_giveback_r": 0.031,
                    "favorable_stall_s": 4.1,
                    "progress_vel_r_per_sec": 0.0019,
                },
            },
            {
                "name": "control_no_trigger",
                "expected": False,
                "metrics": {
                    **_base_metrics(),
                    "net_r": 0.08,
                    "best_favorable_r": 0.24,
                    "favorable_giveback_r": 0.01,
                    "favorable_stall_s": 1.0,
                    "progress_vel_r_per_sec": 0.009,
                },
            },
        ],
        "AEE_PRE_SL_EXIT": [
            {
                "name": "obvious_pass",
                "expected": True,
                "metrics": {**_base_metrics(), "mae_r": -0.70, "net_r": -0.58},
            },
            {
                "name": "borderline_pass",
                "expected": True,
                "metrics": {**_base_metrics(), "mae_r": -0.46, "net_r": -0.56},
            },
            {
                "name": "control_no_trigger",
                "expected": False,
                "metrics": {**_base_metrics(), "mae_r": -0.30, "net_r": -0.20},
            },
        ],
        "AEE_PANIC_EXIT": [
            {
                "name": "obvious_pass",
                "expected": True,
                "metrics": {**_base_metrics(), "net_r": -0.95},
            },
            {
                "name": "borderline_pass",
                "expected": True,
                "metrics": {**_base_metrics(), "net_r": -0.76},
            },
            {
                "name": "control_no_trigger",
                "expected": False,
                "metrics": {**_base_metrics(), "net_r": -0.40},
            },
        ],
    }


def _threshold_snapshot(reason: str) -> dict[str, float]:
    if reason == "AEE_FAST_ADVERSE_EXIT":
        return {
            "window_sec": float(phone_bot.AEE_H_FAST_ADVERSE_WINDOW_SEC),
            "min_move_r": float(phone_bot.AEE_H_FAST_ADVERSE_MIN_MOVE_R),
            "min_vel_r_per_sec": float(phone_bot.AEE_H_FAST_ADVERSE_MIN_VEL_R_PER_SEC),
        }
    if reason == "AEE_NEVER_GREEN_TIMEOUT":
        return {
            "timeout_sec": float(phone_bot.AEE_H_NEVER_GREEN_TIMEOUT_SEC),
            "max_best_r": float(phone_bot.AEE_H_NEVER_GREEN_MAX_BEST_R),
            "max_net_r": float(phone_bot.AEE_H_NEVER_GREEN_MAX_NET_R),
        }
    if reason == "AEE_PROFIT_STALL_EXIT":
        return {
            "min_net_r": float(phone_bot.AEE_H_PROFIT_STALL_MIN_NET_R),
            "min_best_r": float(phone_bot.AEE_H_PROFIT_STALL_MIN_BEST_R),
            "min_stall_sec": float(phone_bot.AEE_H_PROFIT_STALL_MIN_STALL_SEC),
            "min_giveback_r": float(phone_bot.AEE_H_PROFIT_STALL_MIN_GIVEBACK_R),
        }
    if reason == "AEE_PRE_SL_EXIT":
        return {
            "arm_mae_r": float(phone_bot.AEE_H_PRE_SL_ARM_MAE_R),
            "hard_exit_r": float(phone_bot.AEE_H_PRE_SL_HARD_EXIT_R),
        }
    return {"panic_exit_r": float(phone_bot.AEE_H_PANIC_EXIT_R)}


def run_replay_proofs() -> dict[str, Any]:
    matrix = _scenario_matrix()
    summary: dict[str, Any] = {}

    for reason, meta in BRANCHES.items():
        fn = getattr(phone_bot, meta["fn"], None)
        rows = []
        pass_count = 0
        fail_count = 0
        false_pos = 0
        false_neg = 0

        for sc in matrix[reason]:
            expected = bool(sc["expected"])
            if fn is None:
                got = False
                ret_reason = ""
                detail: dict[str, Any] = {}
            else:
                got, ret_reason, detail = fn(dict(sc["metrics"]))
                got = bool(got)
            ok = got == expected and (ret_reason == reason or (not got and not expected))
            pass_count += int(ok)
            fail_count += int(not ok)
            if got and not expected:
                false_pos += 1
            if (not got) and expected:
                false_neg += 1
            rows.append(
                {
                    "scenario": sc["name"],
                    "expected": expected,
                    "got": got,
                    "reason": ret_reason,
                    "ok": ok,
                    "detail": detail,
                }
            )

        payload = {
            "branch": reason,
            "generated_at": _now_iso(),
            "scenarios_tested": len(rows),
            "pass_count": pass_count,
            "fail_count": fail_count,
            "false_positives": false_pos,
            "false_negatives": false_neg,
            "threshold_snapshot": _threshold_snapshot(reason),
            "representative_samples": rows[:3],
            "all_results": rows,
        }
        (ROOT / meta["proof_file"]).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        summary[reason] = payload

    return summary


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


def _load_window() -> tuple[float, float] | None:
    candidates = [
        ROOT / "logs/mvp_aee_minextract_4m_window.txt",
        ROOT / "logs/mvp_proceed_window_10m_clean.txt",
        ROOT / "logs/mvp_proceed_window_long.txt",
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


def build_runtime_branch_validation() -> dict[str, Any]:
    window = _load_window()
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

    out: dict[str, Any] = {
        "generated_at": _now_iso(),
        "window": list(window) if window else None,
        "window_duration_sec": (window[1] - window[0]) if window else None,
        "branches": {},
    }

    for reason, meta in BRANCHES.items():
        trigger_kind = meta["trigger_kind"]
        reason_rows = [
            r
            for r in rows
            if str(r.get("exit_reason") or r.get("reason") or "") == reason
            or str(r.get("kind") or r.get("event") or "") == trigger_kind
        ]
        if not reason_rows:
            out["branches"][reason] = {
                "trigger_count": 0,
                "affected_trade_type": {},
                "average_hold_sec": 0.0,
                "average_net_r_at_exit": 0.0,
                "average_best_favorable_r": 0.0,
                "average_adverse_move_metric": 0.0,
                "sample_rows": [],
            }
            continue

        def avg(key: str) -> float:
            vals = []
            for r in reason_rows:
                try:
                    vals.append(float(r.get(key, 0.0) or 0.0))
                except Exception:
                    pass
            return sum(vals) / len(vals) if vals else 0.0

        types: dict[str, int] = {}
        for r in reason_rows:
            tt = str(r.get("trade_type") or r.get("leg_type") or "UNKNOWN").upper()
            types[tt] = int(types.get(tt, 0)) + 1

        out["branches"][reason] = {
            "trigger_count": len(reason_rows),
            "affected_trade_type": types,
            "average_hold_sec": avg("hold_sec"),
            "average_net_r_at_exit": avg("net_r"),
            "average_best_favorable_r": avg("best_favorable_r"),
            "average_adverse_move_metric": avg("rolling_adverse_move_r_short"),
            "sample_rows": reason_rows[:3],
        }

    (ROOT / "aee_runtime_branch_validation.json").write_text(json.dumps(out, indent=2) + "\n", encoding="utf-8")
    return out


def build_status_dashboard(replay: dict[str, Any], runtime: dict[str, Any]) -> dict[str, Any]:
    dashboard: dict[str, Any] = {"generated_at": _now_iso(), "branches": {}}
    for reason, meta in BRANCHES.items():
        fn_name = meta["fn"]
        implemented = hasattr(phone_bot, fn_name)
        replay_ok = bool(replay.get(reason, {}).get("fail_count", 1) == 0)
        rt_count = int(runtime.get("branches", {}).get(reason, {}).get("trigger_count", 0) or 0)
        runtime_ok = rt_count > 0
        if not implemented:
            status = "UNBUILT"
        elif not replay_ok:
            status = "BUILT"
        elif replay_ok and not runtime_ok:
            status = "REPLAY_PROVEN"
        else:
            status = "RUNTIME_PROVEN"

        dashboard["branches"][reason] = {
            "implemented": bool(implemented),
            "isolated_function_exists": bool(implemented),
            "replay_proof": "pass" if replay_ok else "fail",
            "runtime_proof": "pass" if runtime_ok else "fail",
            "latest_trigger_count": rt_count,
            "latest_thresholds": replay.get(reason, {}).get("threshold_snapshot", {}),
            "current_status": status,
        }

    (ROOT / "aee_branch_status_dashboard.json").write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")
    return dashboard


def main() -> None:
    replay = run_replay_proofs()
    runtime = build_runtime_branch_validation()
    dashboard = build_status_dashboard(replay, runtime)
    print(json.dumps({
        "proof_files": [meta["proof_file"] for meta in BRANCHES.values()],
        "runtime_file": "aee_runtime_branch_validation.json",
        "dashboard_file": "aee_branch_status_dashboard.json",
        "status": {k: v["current_status"] for k, v in dashboard["branches"].items()},
    }, indent=2))


if __name__ == "__main__":
    main()
