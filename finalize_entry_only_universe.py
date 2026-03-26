#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parent
OUTPUT_ROOT = ROOT / "compiled_market_nodes"
ARTIFACTS = ROOT / "artifacts"
FINAL_POLICY = "entry_only_final_v1_2026_03_19"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text())


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def issue_names(report: dict) -> list[str]:
    return sorted(
        {
            str(item.get("issue"))
            for item in (report.get("issues") or [])
            if isinstance(item, dict) and item.get("issue")
        }
    )


def main() -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    allow_nodes: list[dict] = []
    reject_nodes: list[dict] = []

    for node_dir in sorted(path for path in OUTPUT_ROOT.iterdir() if path.is_dir()):
        manifest_path = node_dir / "node_manifest.json"
        perf_path = node_dir / "session_performance_check" / "session_performance_check_report.json"
        if not manifest_path.exists() or not perf_path.exists():
            continue

        manifest = load_json(manifest_path)
        performance = load_json(perf_path)
        if manifest.get("pipeline_mode") != "entry-only":
            continue

        node = node_dir.name
        perf_status = str(performance.get("status"))
        issues = issue_names(performance)
        is_clean = perf_status == "PASS" and manifest.get("node_class") in {"accept", "light_delta"}

        manifest["final_entry_policy"] = FINAL_POLICY
        manifest["final_entry_reviewed_at"] = timestamp

        if is_clean:
            manifest["final_entry_verdict"] = "allow"
            allow_nodes.append(
                {
                    "node": node,
                    "pair": manifest.get("node", {}).get("pair"),
                    "weekday": manifest.get("node", {}).get("weekday"),
                    "session": manifest.get("node", {}).get("session"),
                    "node_class": manifest.get("node_class"),
                    "reason": manifest.get("reason") or "performance_pass",
                }
            )
        else:
            manifest["final_entry_verdict"] = "reject"
            if not manifest.get("reason"):
                manifest["reason"] = "terminal_entry_reject"
            reject_nodes.append(
                {
                    "node": node,
                    "pair": manifest.get("node", {}).get("pair"),
                    "weekday": manifest.get("node", {}).get("weekday"),
                    "session": manifest.get("node", {}).get("session"),
                    "node_class": manifest.get("node_class"),
                    "failure_route": manifest.get("failure_route"),
                    "reason": manifest.get("reason"),
                    "perf_status": perf_status,
                    "issue_names": "|".join(issues),
                }
            )

        write_json(manifest_path, manifest)

    allow_payload = {
        "policy": FINAL_POLICY,
        "generated_at": timestamp,
        "entry_allow_count": len(allow_nodes),
        "allow_nodes": [row["node"] for row in allow_nodes],
        "allow_records": allow_nodes,
    }
    reject_payload = {
        "policy": FINAL_POLICY,
        "generated_at": timestamp,
        "entry_reject_count": len(reject_nodes),
        "reject_nodes": [row["node"] for row in reject_nodes],
        "reject_records": reject_nodes,
    }
    summary_payload = {
        "policy": FINAL_POLICY,
        "generated_at": timestamp,
        "entry_only_total": len(allow_nodes) + len(reject_nodes),
        "entry_allow_count": len(allow_nodes),
        "entry_reject_count": len(reject_nodes),
        "entry_clean_pct": round(
            100.0 * len(allow_nodes) / max(1, len(allow_nodes) + len(reject_nodes)),
            2,
        ),
        "allowlist_json": str(ARTIFACTS / "final_entry_allowlist_v1.json"),
        "rejects_json": str(ARTIFACTS / "final_entry_rejects_v1.json"),
    }

    write_json(ARTIFACTS / "final_entry_allowlist_v1.json", allow_payload)
    write_json(ARTIFACTS / "final_entry_rejects_v1.json", reject_payload)
    write_json(ARTIFACTS / "final_entry_summary_v1.json", summary_payload)

    write_csv(
        ARTIFACTS / "final_entry_allowlist_v1.csv",
        allow_nodes,
        ["node", "pair", "weekday", "session", "node_class", "reason"],
    )
    write_csv(
        ARTIFACTS / "final_entry_rejects_v1.csv",
        reject_nodes,
        ["node", "pair", "weekday", "session", "node_class", "failure_route", "reason", "perf_status", "issue_names"],
    )

    print(json.dumps(summary_payload, indent=2))


if __name__ == "__main__":
    main()
