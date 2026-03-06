#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _iter_jsonl(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or not line.startswith("{"):
                continue
            try:
                yield json.loads(line)
            except Exception:
                continue


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades-log", default="logs/trades.jsonl")
    ap.add_argument("--runtime-log", default="logs/runtime.log")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    trades = Path(args.trades_log)
    runtime = Path(args.runtime_log)
    if not trades.exists():
        raise SystemExit(f"TRADES_LOG_MISSING: {trades}")
    if not runtime.exists():
        raise SystemExit(f"RUNTIME_LOG_MISSING: {runtime}")

    tune_levels: Counter[str] = Counter()
    tune_keys: Counter[str] = Counter()
    entry_gate_eval = 0
    aee_exits = 0
    fallback_events = 0
    for row in _iter_jsonl(trades):
        kind = str(row.get("kind", "") or row.get("event_type", "") or "")
        if kind == "TUNE_MATCH":
            lvl = str(row.get("matched_level", "") or "UNKNOWN")
            key = str(row.get("matched_key", "") or "UNKNOWN")
            tune_levels[lvl] += 1
            tune_keys[key] += 1
        if kind == "ENTRY_GATE_EVAL":
            entry_gate_eval += 1
        if kind == "AEE_EXIT_SNAPSHOT_POST":
            aee_exits += 1
        if "FALLBACK" in kind:
            fallback_events += 1

    text = runtime.read_text(encoding="utf-8", errors="ignore")
    for pat in (r"\bFALLBACK\b", r"SIZING_META_FALLBACK_USED", r"EMPTY_SCAN_FALLBACK"):
        fallback_events += len(re.findall(pat, text))
    aee_decisions = len(re.findall(r"\bAEE_DECISION\b", text))
    enters = len(re.findall(r"\bENTER\b", text))
    active_rows = len(re.findall(r"\bACTIVE_ARTIFACT\b", text))

    total_signal = max(1, entry_gate_eval + enters + aee_decisions)
    fallback_rate = fallback_events / float(total_signal)
    out_path = Path(args.out) if args.out else Path("proof_artifacts") / f"RUNTIME_PROOF_{_now_tag()}.md"
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lines = []
    lines.append(f"# Runtime Proof {datetime.now(timezone.utc).isoformat()}")
    lines.append("")
    lines.append("## Inputs")
    lines.append(f"- trades_log: `{trades}`")
    lines.append(f"- runtime_log: `{runtime}`")
    lines.append("")
    lines.append("## Summary")
    lines.append(f"- ACTIVE_ARTIFACT lines: `{active_rows}`")
    lines.append(f"- ENTRY_GATE_EVAL count: `{entry_gate_eval}`")
    lines.append(f"- ENTER count (runtime.log): `{enters}`")
    lines.append(f"- AEE_DECISION count (runtime.log): `{aee_decisions}`")
    lines.append(f"- AEE exit snapshot count (trades): `{aee_exits}`")
    lines.append(f"- fallback_events: `{fallback_events}`")
    lines.append(f"- fallback_rate: `{fallback_rate:.6f}`")
    lines.append("")
    lines.append("## TUNE_MATCH Levels")
    if tune_levels:
        for k, v in tune_levels.most_common():
            lines.append(f"- {k}: `{v}`")
    else:
        lines.append("- none")
    lines.append("")
    lines.append("## Top Matched Keys")
    if tune_keys:
        for k, v in tune_keys.most_common(20):
            lines.append(f"- `{k}`: `{v}`")
    else:
        lines.append("- none")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(str(out_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
