from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
CONTROL = ROOT / "control"

RESULTS_PATH = CONTROL / "direction_attribution_results.json"
ENTRY_REPORT = CONTROL / "entry_mapping_report.json"
AEE_REPORT = CONTROL / "aee_mapping_report.json"

STATUS_REPLAY = "REQUIRES_REPLAY"
STATUS_SOURCE = "REQUIRES_SOURCE_DATA"
STATUS_BLOCKED = "BLOCKED_UNRESOLVED_UPSTREAM"
STATUS_DERIVABLE = "DERIVABLE_CANDIDATE"

BUCKETS = {"11_A", "11_B", "11_C", "11_D", "11_E", "11_F", "22_A", "22_B", "22_C", "44_A"}


def load_json(path: Path) -> Dict:
	with path.open("r", encoding="utf-8") as f:
		return json.load(f)


def norm_session(s: str) -> str:
	if not isinstance(s, str):
		return ""
	t = s.strip().lower().replace("_", "")
	if t in {"newyork", "new york"}:
		return "newyork"
	return t


def is_unresolved(v: str) -> bool:
	return v in {None, "", "UNRESOLVED_UPSTREAM", "INVALID_DEPENDENCY"}


def session_in_path(path: str, session: str) -> bool:
	p = path.lower()
	s = norm_session(session)
	if s == "newyork":
		return "newyork" in p
	return s in p


def bucket_in_path(path: str, bucket: str) -> bool:
	if bucket not in BUCKETS:
		return False
	return f"__{bucket.lower()}" in path.lower() or f"/{bucket.lower()}" in path.lower()


def collect_assets(report: Dict) -> Dict[Tuple[str, str], List[str]]:
	out = defaultdict(list)
	for group in report.get("segmented_groups", []):
		key = group.get("key", {})
		pair = key.get("pair")
		session = key.get("session")
		if is_unresolved(pair) or is_unresolved(session):
			continue
		k = (pair, norm_session(session))
		for asset in group.get("sample_assets", []):
			if isinstance(asset, str) and asset not in out[k]:
				out[k].append(asset)
	return out


def structural_counts(paths: List[str]) -> Dict[str, int]:
	counts = Counter()
	for p in paths:
		pl = p.lower()
		if "phase2/cluster_summary.json" in pl:
			counts["cluster_summary"] += 1
		if "phase2/cluster_audit.json" in pl:
			counts["cluster_audit"] += 1
		if "phase3/entry_window" in pl or "phase1/opportunity_map" in pl:
			counts["entry_struct"] += 1
		if "phase6/odm_ceiling_report.json" in pl or "phase6/odm_audit.json" in pl:
			counts["aee_struct"] += 1
		if re.search(r"(^|[^a-z])(long|short)([^a-z]|$)", pl):
			counts["directional"] += 1
		else:
			counts["agnostic"] += 1
	return counts


def classify_strict(
	row: Dict,
	entry_assets: Dict[Tuple[str, str], List[str]],
	aee_assets: Dict[Tuple[str, str], List[str]],
) -> Tuple[str, Dict]:
	pair = row.get("pair")
	session = row.get("session")
	bucket = row.get("target_bucket")

	unresolved = {
		"direction": False,
		"target_bucket": is_unresolved(bucket),
		"pair": is_unresolved(pair),
		"session": is_unresolved(session),
	}
	if any(unresolved.values()):
		return STATUS_BLOCKED, {
			"same_bucket_asset_count": 0,
			"entry_structural_count": 0,
			"aee_structural_count": 0,
			"directional_asset_count": 0,
			"agnostic_asset_count": 0,
			"reason": "Unresolved composite key fields",
			"source_candidates": [],
			"unresolved": unresolved,
		}

	key = (pair, norm_session(session))
	all_paths = []
	for src in (entry_assets.get(key, []), aee_assets.get(key, [])):
		all_paths.extend(src)

	same_bucket = [
		p for p in all_paths if session_in_path(p, session) and bucket_in_path(p, bucket)
	]
	counts = structural_counts(same_bucket)

	same_bucket_asset_count = len(same_bucket)
	entry_structural_count = counts["cluster_summary"] + counts["cluster_audit"] + counts["entry_struct"]
	aee_structural_count = counts["aee_struct"]
	directional_asset_count = counts["directional"]
	agnostic_asset_count = counts["agnostic"]

	# Strict thresholds:
	# - same pair/session/target_bucket evidence only
	# - enough LONG-side density in structural artifacts
	# - structural mirror available (entry + aee)
	# - direction is still agnostic => replay needed to attribute SHORT
	replay_ready = (
		same_bucket_asset_count >= 5
		and entry_structural_count >= 3
		and aee_structural_count >= 1
		and directional_asset_count == 0
	)

	if replay_ready:
		status = STATUS_REPLAY
		reason = "Sufficient same-combo structural evidence exists, but direction remains agnostic; replay required."
	else:
		status = STATUS_SOURCE
		reason = "Same-combo mirrored structural evidence is thin or missing; source-history regeneration required."

	return status, {
		"same_bucket_asset_count": same_bucket_asset_count,
		"entry_structural_count": entry_structural_count,
		"aee_structural_count": aee_structural_count,
		"directional_asset_count": directional_asset_count,
		"agnostic_asset_count": agnostic_asset_count,
		"reason": reason,
		"source_candidates": same_bucket[:12],
		"unresolved": unresolved,
	}


def main() -> None:
	results = load_json(RESULTS_PATH)
	entry = load_json(ENTRY_REPORT)
	aee = load_json(AEE_REPORT)

	entry_assets = collect_assets(entry)
	aee_assets = collect_assets(aee)

	original_records = results.get("records", [])
	original_derivable = sum(1 for r in original_records if r.get("status") == STATUS_DERIVABLE)

	updated = []
	downgraded = 0

	for row in original_records:
		prev_status = row.get("status")
		new_status, metrics = classify_strict(row, entry_assets, aee_assets)

		if prev_status == STATUS_DERIVABLE and new_status in {STATUS_REPLAY, STATUS_SOURCE}:
			downgraded += 1

		row = dict(row)
		row["status"] = new_status
		row["evidence_available"] = metrics["same_bucket_asset_count"] > 0
		row["derivable_from_long"] = False
		row["requires_regeneration"] = new_status in {STATUS_REPLAY, STATUS_SOURCE}
		row["source_candidates"] = metrics["source_candidates"]
		row["unresolved_upstream"] = metrics["unresolved"]
		row["notes"] = metrics["reason"]
		row["strict_evidence"] = {
			"same_bucket_asset_count": metrics["same_bucket_asset_count"],
			"entry_structural_count": metrics["entry_structural_count"],
			"aee_structural_count": metrics["aee_structural_count"],
			"directional_asset_count": metrics["directional_asset_count"],
			"agnostic_asset_count": metrics["agnostic_asset_count"],
		}
		updated.append(row)

	updated.sort(key=lambda r: (-int(r.get("priority_score", 0) or 0), r.get("target_bucket", ""), r.get("pair", ""), r.get("session", "")))

	counts = Counter(r.get("status") for r in updated)

	results["status"] = "STRICTLY_PROMOTED"
	results["run_metadata"] = {
		"generated_at_utc": datetime.now(timezone.utc).isoformat(),
		"generator": "tools/strict_promote_direction_results.py",
		"mode": "strict_promotion",
	}
	results["result_summary"] = {
		"combos_evaluated": len(updated),
		"short_present": 0,
		"short_missing": len(updated),
		"short_derivable_from_existing": 0,
		"short_requires_source_replay": counts.get(STATUS_REPLAY, 0),
		"short_unresolved_upstream": counts.get(STATUS_BLOCKED, 0),
		"count_by_status": dict(counts),
		"former_derivable_rows": original_derivable,
		"former_derivable_downgraded": downgraded,
		"pass": False,
	}
	results["records"] = updated

	RESULTS_PATH.write_text(json.dumps(results, indent=2), encoding="utf-8")

	print(f"Wrote {RESULTS_PATH}")
	print(f"Former derivable rows: {original_derivable}")
	print(f"Downgraded derivable rows: {downgraded}")
	print("Counts by status:")
	for k in sorted(counts.keys()):
		print(f"  {k}: {counts[k]}")


if __name__ == "__main__":
	main()
