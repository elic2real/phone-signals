#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple


@dataclass
class Candidate:
    node_id: str
    pair: str
    session: str
    day: str
    quarter: str
    delta_viable: int
    delta_borderline: int
    status: str
    process_status: str
    quality_reason: str
    quality_node_class: str
    sample_size: int
    strategy_baseline: str
    combined_wr: float
    total_selected: int
    long_wr: float
    long_n: int
    short_wr: float
    short_n: int


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _fingerprint(path: Path) -> str:
    st = path.stat()
    raw = f"{path}:{st.st_mtime_ns}:{st.st_size}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def _load_node_win_rates(path: Path) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    with path.open(encoding="utf-8", newline="") as handle:
        for row in csv.DictReader(handle):
            node_id = str(row.get("node_id") or "").strip()
            if not node_id:
                continue
            out[node_id] = {
                "combined_wr": _safe_float(row.get("combined_entry_win_rate_weighted"), float("nan")),
                "total_selected": int(float(row.get("total_selected_count") or 0)),
                "long_wr": _safe_float(row.get("long_entry_win_rate"), float("nan")),
                "long_n": int(float(row.get("long_selected_count") or 0)),
                "short_wr": _safe_float(row.get("short_entry_win_rate"), float("nan")),
                "short_n": int(float(row.get("short_selected_count") or 0)),
            }
    return out


def _compute_structure_score(c: Candidate, structure_freq: Dict[str, Dict[str, int]]) -> float:
    # Prefer under-represented structure buckets to avoid concentration risk.
    pair_score = 1.0 / float(1 + structure_freq["pair"].get(c.pair, 0))
    session_score = 1.0 / float(1 + structure_freq["session"].get(c.session, 0))
    day_score = 1.0 / float(1 + structure_freq["day"].get(c.day, 0))
    quarter_score = 1.0 / float(1 + structure_freq["quarter"].get(c.quarter, 0))
    return (pair_score + session_score + day_score + quarter_score) / 4.0


def _risk_penalty(c: Candidate) -> float:
    p = 0.0
    if c.quality_node_class == "heavy_delta":
        p += 25.0
    if c.quality_reason == "too_many_repair_zones":
        p += 8.0
    if c.quality_reason == "below_break_even":
        p += 5.0
    if c.sample_size > 0 and c.sample_size < 20:
        p += 8.0
    if c.status == "process_error" or c.process_status == "error":
        p += 40.0
    return p


def _is_locked_candidate(
    c: Candidate,
    *,
    lock_combined_wr: float,
    lock_total_selected: int,
    lock_min_side_selected: int,
) -> bool:
    if not math.isfinite(float(c.combined_wr)):
        return False
    return (
        c.combined_wr >= float(lock_combined_wr)
        and c.total_selected >= int(lock_total_selected)
        and min(c.long_n, c.short_n) >= int(lock_min_side_selected)
        and c.status != "process_error"
        and c.process_status != "error"
    )


def _preflight_rejection_reason(
    c: Candidate,
    *,
    borderline_inflation_cap: int,
    viable_loss_cap: int,
    locked_borderline_cap: int,
    lock_combined_wr: float,
    lock_total_selected: int,
    lock_min_side_selected: int,
) -> str:
    is_locked = _is_locked_candidate(
        c,
        lock_combined_wr=lock_combined_wr,
        lock_total_selected=lock_total_selected,
        lock_min_side_selected=lock_min_side_selected,
    )
    if c.status == "process_error" or c.process_status == "error":
        return "process_error"
    if c.quality_node_class == "heavy_delta":
        return "heavy_delta_rejected"
    if is_locked and c.delta_borderline > locked_borderline_cap:
        return "locked_node_borderline_cap_exceeded"
    if is_locked and c.delta_viable < 0:
        return "locked_node_viable_loss"
    if c.delta_borderline > borderline_inflation_cap:
        return "borderline_inflation_cap_exceeded"
    if c.delta_viable < -viable_loss_cap:
        return "viable_loss_cap_exceeded"
    return ""


def _damage_aware_rank_score(c: Candidate, *, is_locked: bool) -> float:
    combined = c.combined_wr if math.isfinite(float(c.combined_wr)) else 0.0
    total_selected = max(1, int(c.total_selected or 0))
    score = (
        100.0 * combined
        + 8.0 * math.log10(total_selected)
        + 2.0 * max(int(c.delta_viable), 0)
        - 1.5 * max(int(c.delta_borderline), 0)
        - _risk_penalty(c)
    )
    if is_locked:
        score += 6.0
    return round(score, 4)


def _rank_tuple(row: Dict[str, Any]) -> Tuple[Any, ...]:
    # Higher damage-aware score first, then prefer lower damage and stronger structure.
    return (
        -float(row["rank_score"]),
        int(row["delta_borderline"]),
        -int(row["delta_viable"]),
        float(row["risk_penalty"]),
        -float(row["structure_score"]),
        str(row["node_id"]),
    )


def _build_candidates(
    damage_payload: Dict[str, Any],
    allowlist_payload: Dict[str, Any],
    node_win_rates: Dict[str, Dict[str, Any]],
) -> List[Candidate]:
    allow_details = {
        str(d.get("node") or "").strip(): d
        for d in (allowlist_payload.get("allow_details") or [])
        if str(d.get("node") or "").strip()
    }

    out: List[Candidate] = []
    for row in (damage_payload.get("per_node_rows") or []):
        node = str(row.get("node_id") or "").strip()
        if not node:
            continue
        ad = allow_details.get(node, {})
        wr = node_win_rates.get(node, {})
        out.append(
            Candidate(
                node_id=node,
                pair=str(row.get("pair") or ""),
                session=str(row.get("session") or ""),
                day=str(row.get("day") or ""),
                quarter=str(row.get("quarter") or ad.get("quarter") or ""),
                delta_viable=int(row.get("delta_viable") or 0),
                delta_borderline=int(row.get("delta_borderline") or 0),
                status=str(row.get("status") or ""),
                process_status=str(row.get("process_status") or ""),
                quality_reason=str(row.get("quality_reason") or ""),
                quality_node_class=str(row.get("quality_node_class") or ""),
                sample_size=int(ad.get("sample_size") or 0),
                strategy_baseline=str(ad.get("strategy_baseline") or ""),
                combined_wr=_safe_float(wr.get("combined_wr"), float("nan")),
                total_selected=int(wr.get("total_selected") or 0),
                long_wr=_safe_float(wr.get("long_wr"), float("nan")),
                long_n=int(wr.get("long_n") or 0),
                short_wr=_safe_float(wr.get("short_wr"), float("nan")),
                short_n=int(wr.get("short_n") or 0),
            )
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Build entry-only micro allowlist from second-pass damage artifact with preflight, cache, and queue outputs.")
    ap.add_argument("--damage-report", type=Path, default=Path("artifacts/entry_only_per_node_damage_report_secondpass_allowlist.json"))
    ap.add_argument("--allowlist", type=Path, default=Path("artifacts/entry_only_second_pass_allowlist.json"))
    ap.add_argument("--out-allowlist", type=Path, default=Path("artifacts/entry_only_micro_allowlist_v1.json"))
    ap.add_argument("--out-queue", type=Path, default=Path("artifacts/entry_only_micro_queue_v1.json"))
    ap.add_argument("--out-verdict", type=Path, default=Path("artifacts/entry_only_micro_verdict_v1.json"))
    ap.add_argument("--out-ranking-json", type=Path, default=Path("artifacts/entry_only_micro_candidate_ranking_secondpass.json"))
    ap.add_argument("--out-ranking-csv", type=Path, default=Path("artifacts/entry_only_micro_candidate_ranking_secondpass.csv"))
    ap.add_argument("--cache-file", type=Path, default=Path("artifacts/entry_only_micro_analysis_cache_v1.json"))
    ap.add_argument("--micro-size", type=int, default=5)
    ap.add_argument("--min-micro-size", type=int, default=4)
    ap.add_argument("--max-micro-size", type=int, default=6)
    ap.add_argument("--borderline-inflation-cap", type=int, default=15)
    ap.add_argument("--viable-loss-cap", type=int, default=2)
    ap.add_argument("--locked-borderline-cap", type=int, default=10)
    ap.add_argument("--node-win-rates", type=Path, default=Path("artifacts/node_win_rates_rerun_nodes.csv"))
    ap.add_argument("--lock-combined-wr", type=float, default=0.58)
    ap.add_argument("--lock-total-selected", type=int, default=300)
    ap.add_argument("--lock-min-side-selected", type=int, default=50)
    ap.add_argument("--promote-combined-wr", type=float, default=0.55)
    ap.add_argument("--promote-total-selected", type=int, default=500)
    ap.add_argument("--promote-borderline-cap", type=int, default=10)
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()

    project_root = Path(__file__).resolve().parent.parent
    damage_path = args.damage_report if args.damage_report.is_absolute() else project_root / args.damage_report
    allowlist_path = args.allowlist if args.allowlist.is_absolute() else project_root / args.allowlist
    out_allowlist = args.out_allowlist if args.out_allowlist.is_absolute() else project_root / args.out_allowlist
    out_queue = args.out_queue if args.out_queue.is_absolute() else project_root / args.out_queue
    out_verdict = args.out_verdict if args.out_verdict.is_absolute() else project_root / args.out_verdict
    out_ranking_json = args.out_ranking_json if args.out_ranking_json.is_absolute() else project_root / args.out_ranking_json
    out_ranking_csv = args.out_ranking_csv if args.out_ranking_csv.is_absolute() else project_root / args.out_ranking_csv
    cache_file = args.cache_file if args.cache_file.is_absolute() else project_root / args.cache_file
    node_win_rates_path = args.node_win_rates if args.node_win_rates.is_absolute() else project_root / args.node_win_rates

    if not damage_path.exists():
        raise FileNotFoundError(f"Missing damage report: {damage_path}")
    if not allowlist_path.exists():
        raise FileNotFoundError(f"Missing allowlist: {allowlist_path}")
    if not node_win_rates_path.exists():
        raise FileNotFoundError(f"Missing node win rates: {node_win_rates_path}")

    desired_size = max(int(args.min_micro_size), min(int(args.max_micro_size), int(args.micro_size)))

    damage_payload = _load_json(damage_path)
    allowlist_payload = _load_json(allowlist_path)
    node_win_rates = _load_node_win_rates(node_win_rates_path)
    candidates = _build_candidates(damage_payload, allowlist_payload, node_win_rates)

    remove_immediate = set(damage_payload.get("split", {}).get("remove_immediately") or [])
    uncertain = [c for c in candidates if c.node_id in set(damage_payload.get("split", {}).get("uncertain_review_manually") or [])]

    fingerprints = {
        "damage_report": _fingerprint(damage_path),
        "allowlist": _fingerprint(allowlist_path),
        "node_win_rates": _fingerprint(node_win_rates_path),
    }

    cache_payload: Dict[str, Any] = {}
    if cache_file.exists():
        try:
            cache_payload = _load_json(cache_file)
        except Exception:
            cache_payload = {}

    use_cache = (
        cache_payload.get("fingerprints") == fingerprints
        and isinstance(cache_payload.get("analysis_rows"), list)
    )

    analysis_rows: List[Dict[str, Any]] = []
    preflight_rejections: List[Dict[str, Any]] = []

    if use_cache:
        analysis_rows = list(cache_payload.get("analysis_rows") or [])
        preflight_rejections = list(cache_payload.get("preflight_rejections") or [])
    else:
        structure_freq = {
            "pair": {},
            "session": {},
            "day": {},
            "quarter": {},
        }
        for c in uncertain:
            structure_freq["pair"][c.pair] = structure_freq["pair"].get(c.pair, 0) + 1
            structure_freq["session"][c.session] = structure_freq["session"].get(c.session, 0) + 1
            structure_freq["day"][c.day] = structure_freq["day"].get(c.day, 0) + 1
            structure_freq["quarter"][c.quarter] = structure_freq["quarter"].get(c.quarter, 0) + 1

        def _analyze_one(c: Candidate) -> Dict[str, Any]:
            is_locked = _is_locked_candidate(
                c,
                lock_combined_wr=float(args.lock_combined_wr),
                lock_total_selected=int(args.lock_total_selected),
                lock_min_side_selected=int(args.lock_min_side_selected),
            )
            reject_reason = _preflight_rejection_reason(
                c,
                borderline_inflation_cap=int(args.borderline_inflation_cap),
                viable_loss_cap=int(args.viable_loss_cap),
                locked_borderline_cap=int(args.locked_borderline_cap),
                lock_combined_wr=float(args.lock_combined_wr),
                lock_total_selected=int(args.lock_total_selected),
                lock_min_side_selected=int(args.lock_min_side_selected),
            )
            enough_sample = c.total_selected >= int(args.promote_total_selected)
            low_borderline_inflation = c.delta_borderline <= int(args.promote_borderline_cap)
            non_negative_viable = c.delta_viable >= 0
            no_process_error = c.status != "process_error" and c.process_status != "error"
            balanced_sides = min(c.long_n, c.short_n) >= int(args.lock_min_side_selected)
            hard_pass = (
                no_process_error
                and c.quality_node_class != "heavy_delta"
                and non_negative_viable
                and low_borderline_inflation
                and math.isfinite(float(c.combined_wr))
                and c.combined_wr >= float(args.promote_combined_wr)
                and enough_sample
                and balanced_sides
            )
            row = {
                "node_id": c.node_id,
                "pair": c.pair,
                "session": c.session,
                "day": c.day,
                "quarter": c.quarter,
                "delta_viable": c.delta_viable,
                "delta_borderline": c.delta_borderline,
                "status": c.status,
                "process_status": c.process_status,
                "quality_reason": c.quality_reason,
                "quality_node_class": c.quality_node_class,
                "sample_size": c.sample_size,
                "strategy_baseline": c.strategy_baseline,
                "combined_wr": c.combined_wr if math.isfinite(float(c.combined_wr)) else None,
                "total_selected": c.total_selected,
                "long_wr": c.long_wr if math.isfinite(float(c.long_wr)) else None,
                "long_n": c.long_n,
                "short_wr": c.short_wr if math.isfinite(float(c.short_wr)) else None,
                "short_n": c.short_n,
                "locked_node": is_locked,
                "no_process_error": no_process_error,
                "low_borderline_inflation": low_borderline_inflation,
                "non_negative_viable": non_negative_viable,
                "enough_sample": enough_sample,
                "balanced_sides": balanced_sides,
                "hard_pass": hard_pass,
                "structure_score": _compute_structure_score(c, structure_freq),
                "risk_penalty": _risk_penalty(c),
                "rank_score": _damage_aware_rank_score(c, is_locked=is_locked),
                "preflight_rejection_reason": reject_reason,
            }
            return row

        with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as ex:
            analysis_rows = list(ex.map(_analyze_one, uncertain))

        for r in analysis_rows:
            if r.get("preflight_rejection_reason"):
                preflight_rejections.append(r)

        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(
            json.dumps(
                {
                    "created_at_utc": datetime.now(timezone.utc).isoformat(),
                    "fingerprints": fingerprints,
                    "analysis_rows": analysis_rows,
                    "preflight_rejections": preflight_rejections,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    ranked_all = list(analysis_rows)
    ranked_all.sort(key=_rank_tuple)
    ranked_pool = [r for r in ranked_all if not r.get("preflight_rejection_reason")]

    # Related-node seeding: first pass takes best candidate per strategy baseline.
    selected: List[Dict[str, Any]] = []
    seen_strategy: set[str] = set()
    for r in ranked_pool:
        sb = str(r.get("strategy_baseline") or "")
        if sb and sb not in seen_strategy and len(selected) < desired_size:
            selected.append(r)
            seen_strategy.add(sb)

    # Fill remaining slots by global ranking.
    selected_ids = {r["node_id"] for r in selected}
    for r in ranked_pool:
        if len(selected) >= desired_size:
            break
        if r["node_id"] in selected_ids:
            continue
        selected.append(r)
        selected_ids.add(r["node_id"])

    # Keep list from previous immediate removes + preflight rejects.
    remove_nodes = set(remove_immediate)
    remove_nodes.update(r["node_id"] for r in preflight_rejections)

    uncertain_review = [r for r in ranked_pool if r["node_id"] not in selected_ids]
    blocked = [r for r in analysis_rows if r["node_id"] in remove_nodes]
    locked_nodes = [r["node_id"] for r in analysis_rows if bool(r.get("locked_node"))]

    micro_allowlist_nodes = [r["node_id"] for r in selected]

    allowlist_payload_out = {
        "policy": "entry_only_micro_allowlist_v2_preservation",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_damage_report": str(damage_path),
        "source_allowlist": str(allowlist_path),
        "source_node_win_rates": str(node_win_rates_path),
        "micro_size": len(micro_allowlist_nodes),
        "requested_micro_size": desired_size,
        "selection_rules": {
            "rank_priority": [
                "highest_damage_aware_rank_score",
                "lowest_delta_borderline",
                "least_viable_damage",
                "structure_preference_pair_session_day_quarter",
            ],
            "borderline_inflation_cap": int(args.borderline_inflation_cap),
            "viable_loss_cap": int(args.viable_loss_cap),
            "locked_borderline_cap": int(args.locked_borderline_cap),
            "lock_combined_wr": float(args.lock_combined_wr),
            "lock_total_selected": int(args.lock_total_selected),
            "lock_min_side_selected": int(args.lock_min_side_selected),
            "promote_combined_wr": float(args.promote_combined_wr),
            "promote_total_selected": int(args.promote_total_selected),
            "promote_borderline_cap": int(args.promote_borderline_cap),
        },
        "allow_nodes": micro_allowlist_nodes,
        "allow_details": selected,
        "locked_nodes": locked_nodes,
        "excluded_immediate": sorted(list(remove_immediate)),
        "excluded_preflight": [
            {
                "node": r["node_id"],
                "reason": r["preflight_rejection_reason"],
                "delta_viable": r["delta_viable"],
                "delta_borderline": r["delta_borderline"],
            }
            for r in preflight_rejections
        ],
    }

    queue_payload = {
        "policy": "entry_only_micro_queue_v2_preservation",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "safe_nodes_ready_to_run": micro_allowlist_nodes,
        "unsafe_nodes_blocked": [r["node_id"] for r in blocked],
        "uncertain_nodes_review": [r["node_id"] for r in uncertain_review],
        "preserved_nodes_locked": locked_nodes,
        "counts": {
            "safe": len(micro_allowlist_nodes),
            "unsafe": len(blocked),
            "uncertain": len(uncertain_review),
            "locked": len(locked_nodes),
        },
    }

    verdict_payload = {
        "policy": "entry_only_micro_verdict_v2_preservation",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "selected_node_only_delta_from_source": damage_payload.get("summary", {}),
        "keep_remove_uncertain": {
            "keep_for_micro_pass": micro_allowlist_nodes,
            "remove_immediately": sorted(list(remove_nodes)),
            "uncertain_review_manually": [r["node_id"] for r in uncertain_review],
            "preserve_locked": locked_nodes,
        },
        "analysis_rows": analysis_rows,
        "ranked_pool": ranked_pool,
    }

    ranking_payload = {
        "policy": "entry_only_micro_candidate_ranking_secondpass_v2_preservation",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "score": "100*combined_wr + 8*log10(total_selected) + 2*max(delta_viable,0) - 1.5*max(delta_borderline,0) - risk_penalties",
        "selection_rules": allowlist_payload_out["selection_rules"],
        "rows": ranked_all,
        "blocked_rows": blocked,
        "locked_nodes": locked_nodes,
    }

    for p in [out_allowlist, out_queue, out_verdict, out_ranking_json, out_ranking_csv]:
        p.parent.mkdir(parents=True, exist_ok=True)

    out_allowlist.write_text(json.dumps(allowlist_payload_out, indent=2) + "\n", encoding="utf-8")
    out_queue.write_text(json.dumps(queue_payload, indent=2) + "\n", encoding="utf-8")
    out_verdict.write_text(json.dumps(verdict_payload, indent=2) + "\n", encoding="utf-8")
    out_ranking_json.write_text(json.dumps(ranking_payload, indent=2) + "\n", encoding="utf-8")
    fieldnames = [
        "rank_score",
        "hard_pass",
        "locked_node",
        "node_id",
        "combined_wr",
        "total_selected",
        "long_wr",
        "long_n",
        "short_wr",
        "short_n",
        "delta_viable",
        "delta_borderline",
        "status",
        "quality_node_class",
        "quality_reason",
        "no_process_error",
        "low_borderline_inflation",
        "non_negative_viable",
        "enough_sample",
        "balanced_sides",
        "preflight_rejection_reason",
    ]
    with out_ranking_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in ranked_all:
            writer.writerow({k: row.get(k) for k in fieldnames})

    print(
        json.dumps(
            {
                "status": "PASS",
                "out_allowlist": str(out_allowlist),
                "out_queue": str(out_queue),
                "out_verdict": str(out_verdict),
                "out_ranking_json": str(out_ranking_json),
                "out_ranking_csv": str(out_ranking_csv),
                "out_cache": str(cache_file),
                "safe": len(micro_allowlist_nodes),
                "unsafe": len(blocked),
                "uncertain": len(uncertain_review),
                "locked": len(locked_nodes),
                "allow_nodes": micro_allowlist_nodes,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
