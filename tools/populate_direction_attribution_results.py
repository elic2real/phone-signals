from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple


ROOT = Path(__file__).resolve().parents[1]
CONTROL = ROOT / "control"

GAP_REPORT = CONTROL / "pc2_segmentation_gap_report.json"
ENTRY_REPORT = CONTROL / "entry_mapping_report.json"
AEE_REPORT = CONTROL / "aee_mapping_report.json"
RESULTS_PATH = CONTROL / "direction_attribution_results.json"

ALLOWED_STATUS = {
	"MISSING_SHORT",
	"DERIVABLE_CANDIDATE",
	"REQUIRES_REPLAY",
	"REQUIRES_SOURCE_DATA",
	"BLOCKED_UNRESOLVED_UPSTREAM",
}


def load_json(path: Path) -> Dict:
	with path.open("r", encoding="utf-8") as f:
		return json.load(f)


def is_resolved(value: str) -> bool:
	return value not in {None, "", "UNRESOLVED_UPSTREAM", "INVALID_DEPENDENCY"}


def collect_pair_session_assets(report: Dict) -> Dict[Tuple[str, str], Dict]:
	merged: Dict[Tuple[str, str], Dict] = defaultdict(lambda: {"asset_count": 0, "sample_assets": []})
	for group in report.get("segmented_groups", []):
		key = group.get("key", {})
		pair = key.get("pair")
		session = key.get("session")
		if not is_resolved(pair) or not is_resolved(session):
			continue

		bucket = (pair, session)
		merged[bucket]["asset_count"] += int(group.get("asset_count", 0) or 0)
		for p in group.get("sample_assets", []):
			if p not in merged[bucket]["sample_assets"]:
				merged[bucket]["sample_assets"].append(p)
	return merged


def pick_source_candidates(entry_assets: Dict, aee_assets: Dict, pair: str, session: str) -> List[str]:
	key = (pair, session)
	candidates = []
	for source in (entry_assets.get(key), aee_assets.get(key)):
		if not source:
			continue
		for path in source.get("sample_assets", []):
			if path not in candidates:
				candidates.append(path)
			if len(candidates) >= 8:
				return candidates
	return candidates


def compute_priority(entry_assets: Dict, aee_assets: Dict, pair: str, session: str) -> int:
	key = (pair, session)
	entry_count = int(entry_assets.get(key, {}).get("asset_count", 0) or 0)
	aee_count = int(aee_assets.get(key, {}).get("asset_count", 0) or 0)
	return entry_count + aee_count


def derive_status(unresolved: Dict, evidence_available: bool, derivable_from_long: bool) -> str:
	if any(unresolved.values()):
		return "BLOCKED_UNRESOLVED_UPSTREAM"
	if evidence_available and derivable_from_long:
		return "DERIVABLE_CANDIDATE"
	if evidence_available:
		return "REQUIRES_REPLAY"
	return "REQUIRES_SOURCE_DATA"


def main() -> None:
	gap = load_json(GAP_REPORT)
	entry = load_json(ENTRY_REPORT)
	aee = load_json(AEE_REPORT)
	existing = load_json(RESULTS_PATH)

	short_gap_rows = gap.get("segmentation_gaps", {}).get("short_direction_gaps", {}).get("examples", [])
	entry_assets = collect_pair_session_assets(entry)
	aee_assets = collect_pair_session_assets(aee)

	records = []
	for idx, combo in enumerate(short_gap_rows, start=1):
		target_bucket = combo.get("target_bucket")
		pair = combo.get("pair")
		session = combo.get("session")

		unresolved = {
			"direction": False,
			"target_bucket": not is_resolved(target_bucket),
			"pair": not is_resolved(pair),
			"session": not is_resolved(session),
		}

		source_candidates = pick_source_candidates(entry_assets, aee_assets, pair, session)
		evidence_available = len(source_candidates) > 0

		# Gap report already states LONG missing = 0 for these resolved combos.
		long_exists = True
		derivable_from_long = long_exists and evidence_available and not any(unresolved.values())
		status = derive_status(unresolved, evidence_available, derivable_from_long)
		if status not in ALLOWED_STATUS:
			raise ValueError(f"Unexpected status: {status}")

		requires_regeneration = status in {"REQUIRES_REPLAY", "REQUIRES_SOURCE_DATA"}

		if status == "DERIVABLE_CANDIDATE":
			notes = (
				"SHORT missing in current mapping. LONG side exists; candidate for mirrored/derived logic "
				"from existing assets. Not solved."
			)
		elif status == "BLOCKED_UNRESOLVED_UPSTREAM":
			notes = "Blocked by unresolved upstream key fields."
		elif status == "REQUIRES_REPLAY":
			notes = "SHORT missing; evidence exists but replay regeneration is required."
		elif status == "REQUIRES_SOURCE_DATA":
			notes = "SHORT missing; no sufficient evidence candidates found, source data regeneration required."
		else:
			notes = "SHORT missing in current mapping."

		records.append(
			{
				"row_id": f"SHORT_GAP_{idx:03d}",
				"direction": "SHORT",
				"target_bucket": target_bucket,
				"pair": pair,
				"session": session,
				"status": status,
				"evidence_available": evidence_available,
				"derivable_from_long": derivable_from_long,
				"requires_regeneration": requires_regeneration,
				"source_candidates": source_candidates,
				"unresolved_upstream": unresolved,
				"notes": notes,
				"priority_score": compute_priority(entry_assets, aee_assets, pair, session),
			}
		)

	records.sort(key=lambda r: (-int(r.get("priority_score", 0)), r["target_bucket"], r["pair"], r["session"]))

	status_counts = Counter(r["status"] for r in records)

	out = dict(existing)
	out["status"] = "SEEDED"
	out["run_metadata"] = {
		"generated_at_utc": datetime.now(timezone.utc).isoformat(),
		"generator": "tools/populate_direction_attribution_results.py",
		"source_gap_report": "control/pc2_segmentation_gap_report.json",
	}
	out["result_summary"] = {
		"combos_evaluated": len(records),
		"short_present": 0,
		"short_missing": len(records),
		"short_derivable_from_existing": status_counts.get("DERIVABLE_CANDIDATE", 0),
		"short_requires_source_replay": status_counts.get("REQUIRES_REPLAY", 0),
		"short_unresolved_upstream": status_counts.get("BLOCKED_UNRESOLVED_UPSTREAM", 0),
		"count_by_status": dict(status_counts),
		"pass": False,
	}
	out["status_enums"] = {
		"status": [
			"MISSING_SHORT",
			"DERIVABLE_CANDIDATE",
			"REQUIRES_REPLAY",
			"REQUIRES_SOURCE_DATA",
			"BLOCKED_UNRESOLVED_UPSTREAM",
		]
	}
	out["record_schema"] = {
		"required_fields": [
			"direction",
			"target_bucket",
			"pair",
			"session",
			"status",
			"evidence_available",
			"derivable_from_long",
			"requires_regeneration",
			"source_candidates",
			"unresolved_upstream",
			"notes",
		]
	}
	out["records"] = records

	RESULTS_PATH.write_text(json.dumps(out, indent=2), encoding="utf-8")

	print(f"Wrote {RESULTS_PATH}")
	print(f"Seeded rows: {len(records)}")
	print("Status counts:")
	for key in sorted(status_counts.keys()):
		print(f"  {key}: {status_counts[key]}")


if __name__ == "__main__":
	main()
