#!/usr/bin/env python3
import glob
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iter_events(paths):
    for path in paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line_no, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    yield path, line_no, obj
        except OSError:
            continue


def _find_line(paths, predicate):
    for path, line_no, obj in _iter_events(paths):
        try:
            if predicate(obj):
                return {"path": os.path.relpath(path), "line": line_no, "event": obj}
        except Exception:
            continue
    return None


def main():
    workspace = os.path.dirname(os.path.abspath(__file__))
    log_glob = os.path.join(workspace, "logs", "trades.jsonl*")
    paths = sorted(glob.glob(log_glob))
    if not paths:
        raise SystemExit("No trades logs found")

    counts = Counter()
    size_stages = Counter()
    aee_action_kinds = Counter()
    source_level_counter = Counter()

    main_by_group = set()
    runner_by_group = set()
    entry_result_main_ids = set()
    exit_result_ids = set()
    managed_transition_ids = set()

    signal_has_priority = 0
    signal_total = 0

    for _, _, e in _iter_events(paths):
        event = str(e.get("event", ""))
        kind = str(e.get("kind", ""))

        if event:
            counts[event] += 1
        if kind:
            counts[kind] += 1

        if event == "SIGNAL_GENERATED":
            signal_total += 1
            if "priority_score" in e and "priority_reason" in e:
                signal_has_priority += 1

        if kind == "SIZE_TRACE":
            stage = str(e.get("stage", ""))
            if stage:
                size_stages[stage] += 1

        if kind == "ENTRY_GATE_EVAL":
            source_level = str(e.get("source_level", ""))
            source_level_counter[source_level] += 1

        if kind.startswith("AEE_") and kind.endswith("_EXIT"):
            aee_action_kinds[kind] += 1

        if kind == "ENTRY_RESULT" and str(e.get("result", "")).upper() == "FILLED":
            trade_id = e.get("trade_id")
            if isinstance(trade_id, int):
                entry_result_main_ids.add(trade_id)
            entry_group_id = str(e.get("entry_group_id", ""))
            leg_type = str(e.get("leg_type", "")).upper()
            setup = str(e.get("setup", ""))
            if entry_group_id:
                if leg_type == "MAIN":
                    main_by_group.add(entry_group_id)
                if leg_type == "RUNNER" or setup.endswith("_RUN"):
                    runner_by_group.add(entry_group_id)

        if event == "STATE_TRANSITION":
            if str(e.get("from", "")) == "ENTER" and str(e.get("to", "")) == "MANAGING":
                md = e.get("metadata") or {}
                tid = md.get("trade_id") if isinstance(md, dict) else None
                if isinstance(tid, int):
                    managed_transition_ids.add(tid)

        if kind == "EXIT_RESULT":
            trade_id = e.get("trade_id")
            if isinstance(trade_id, int):
                exit_result_ids.add(trade_id)

    runner_without_main = sorted(runner_by_group - main_by_group)

    required_size_stages = {
        "risk_target",
        "stop_distance",
        "pip_value",
        "raw_risk_sizing",
        "margin_check",
        "deploy_cap",
        "final",
    }
    present_size_stages = {k for k, v in size_stages.items() if v > 0}

    checks = {
        "entries_detected": counts["SIGNAL_GENERATED"] > 0,
        "priority_ranking_present": signal_total > 0 and signal_has_priority == signal_total,
        "trades_opened": counts["ENTRY_RESULT"] > 0,
        "enter_to_managing_seen": len(managed_transition_ids) > 0,
        "aee_managing_actions_seen": sum(aee_action_kinds.values()) > 0,
        "trades_closed": counts["EXIT_RESULT"] > 0,
        "close_side_effect_seen": counts["pair_close_complete"] > 0,
        "harvester_banked_seen": counts["HARVESTER_BANKED"] > 0,
        "account_physics_traces_present": required_size_stages.issubset(present_size_stages),
        "baseline_compiled_source_present": source_level_counter["COMPILED"] > 0,
        "runner_has_main_pairing": len(runner_without_main) == 0,
    }

    # pair_close_complete appears as reason on STATE_TRANSITION events, not as event/kind key.
    pair_close_sample = _find_line(
        paths,
        lambda e: str(e.get("event", "")) == "STATE_TRANSITION" and str(e.get("reason", "")) == "pair_close_complete",
    )
    if pair_close_sample:
        checks["close_side_effect_seen"] = True

    signal_sample = _find_line(paths, lambda e: str(e.get("event", "")) == "SIGNAL_GENERATED")
    gate_sample = _find_line(paths, lambda e: str(e.get("kind", "")) == "ENTRY_GATE_EVAL" and str(e.get("decision", "")) == "ALLOW")
    attempt_sample = _find_line(paths, lambda e: str(e.get("event", "")) == "TRADE_ATTEMPT")
    entry_result_sample = _find_line(paths, lambda e: str(e.get("kind", "")) == "ENTRY_RESULT" and str(e.get("result", "")).upper() == "FILLED")
    aee_sample = _find_line(paths, lambda e: str(e.get("kind", "")).startswith("AEE_") and str(e.get("kind", "")).endswith("_EXIT"))
    exit_sample = _find_line(paths, lambda e: str(e.get("kind", "")) == "EXIT_RESULT")
    banked_sample = _find_line(paths, lambda e: str(e.get("kind", "")) == "HARVESTER_BANKED")

    proof = {
        "generated_at_utc": _iso_now(),
        "log_files_scanned": [os.path.relpath(p) for p in paths],
        "summary": {
            "signal_generated": counts["SIGNAL_GENERATED"],
            "entry_gate_eval": counts["ENTRY_GATE_EVAL"],
            "trade_attempt": counts["TRADE_ATTEMPT"],
            "entry_result": counts["ENTRY_RESULT"],
            "exit_result": counts["EXIT_RESULT"],
            "aee_exit_actions": dict(aee_action_kinds),
            "harvester_banked": counts["HARVESTER_BANKED"],
            "signal_with_priority_fields": signal_has_priority,
            "signal_total": signal_total,
            "source_level_counts": dict(source_level_counter),
            "size_trace_stages": dict(size_stages),
            "runner_entry_groups_without_main": runner_without_main,
        },
        "checks": checks,
        "overall_pass": all(checks.values()),
        "samples": {
            "signal_generated": signal_sample,
            "entry_gate_eval_allow": gate_sample,
            "trade_attempt": attempt_sample,
            "entry_result_filled": entry_result_sample,
            "aee_exit_action": aee_sample,
            "exit_result": exit_sample,
            "pair_close_complete": pair_close_sample,
            "harvester_banked": banked_sample,
        },
        "interpretation": {
            "working_system_operational": bool(
                checks["entries_detected"]
                and checks["priority_ranking_present"]
                and checks["trades_opened"]
                and checks["aee_managing_actions_seen"]
                and checks["trades_closed"]
                and checks["close_side_effect_seen"]
                and checks["account_physics_traces_present"]
            ),
            "mapping_separate_from_runtime": bool(checks["baseline_compiled_source_present"]),
            "note": "This verifies observed runtime behavior from production logs. It is operational proof, not a ceiling-optimization proof.",
        },
    }

    out_json = os.path.join(workspace, "operational_split_runtime_proof.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(proof, f, indent=2)
        f.write("\n")

    lines = []
    lines.append("# Operational Split Runtime Proof")
    lines.append("")
    lines.append(f"Generated UTC: `{proof['generated_at_utc']}`")
    lines.append(f"Overall pass: `{proof['overall_pass']}`")
    lines.append("")
    lines.append("## Working System Checks")
    for key in [
        "entries_detected",
        "priority_ranking_present",
        "trades_opened",
        "enter_to_managing_seen",
        "aee_managing_actions_seen",
        "trades_closed",
        "close_side_effect_seen",
        "harvester_banked_seen",
        "account_physics_traces_present",
        "runner_has_main_pairing",
    ]:
        lines.append(f"- `{key}`: `{checks[key]}`")
    lines.append("")
    lines.append("## Mapping Split Evidence")
    lines.append(f"- `baseline_compiled_source_present`: `{checks['baseline_compiled_source_present']}`")
    lines.append("")
    lines.append("## Counts")
    for k, v in proof["summary"].items():
        if isinstance(v, (int, float, str)):
            lines.append(f"- `{k}`: `{v}`")
    lines.append("")
    lines.append("## Sample Evidence")
    for k, v in proof["samples"].items():
        if isinstance(v, dict) and v.get("path"):
            lines.append(f"- `{k}`: `{v['path']}:{v['line']}`")
    lines.append("")
    lines.append("## Interpretation")
    lines.append(f"- `working_system_operational`: `{proof['interpretation']['working_system_operational']}`")
    lines.append(f"- `mapping_separate_from_runtime`: `{proof['interpretation']['mapping_separate_from_runtime']}`")
    lines.append(f"- Note: {proof['interpretation']['note']}")

    out_md = os.path.join(workspace, "operational_split_runtime_proof.md")
    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    print(out_json)
    print(out_md)


if __name__ == "__main__":
    main()
