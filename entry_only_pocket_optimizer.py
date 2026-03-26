#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        out = float(v)
        if math.isfinite(out):
            return out
    except Exception:
        pass
    return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _load_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _pair_tokens(pair: str) -> Tuple[str, str]:
    parts = str(pair or "").upper().split("_")
    if len(parts) == 2:
        return parts[0], parts[1]
    return str(pair or "").upper(), ""


def _pair_similarity(a: str, b: str) -> float:
    if a == b:
        return 1.0
    ab, aq = _pair_tokens(a)
    bb, bq = _pair_tokens(b)
    if not (ab and aq and bb and bq):
        return 0.0
    overlap = len({ab, aq}.intersection({bb, bq}))
    if overlap == 2:
        return 0.75
    if overlap == 1:
        return 0.4
    return 0.0


def _tier_for_rate(rate: float) -> str:
    if rate >= 0.60:
        return "A"
    if rate >= 0.50:
        return "B"
    if rate >= 0.40:
        return "C"
    return "D"


def _confidence_note(sample: int, min_sample: int) -> str:
    if sample >= max(2 * min_sample, 100):
        return "high"
    if sample >= min_sample:
        return "medium"
    return "low"


def _feature_center(bounds: Dict[str, Any]) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for name, span in (bounds or {}).items():
        if not isinstance(span, dict):
            continue
        lo = _safe_float(span.get("min"))
        hi = _safe_float(span.get("max"))
        if hi < lo:
            lo, hi = hi, lo
        out[str(name)] = (lo + hi) * 0.5
    return out


def _node_paths(compiled_root: Path) -> Iterable[Path]:
    for node_dir in sorted(compiled_root.iterdir()):
        if node_dir.is_dir():
            yield node_dir


def _extract_pocket_records(compiled_root: Path) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for node_dir in _node_paths(compiled_root):
        parts = node_dir.name.split("__")
        if len(parts) != 3:
            continue
        pair, weekday, session = parts
        report_path = node_dir / "target_entry_no_timeouts" / "target_entry_class_report.json"
        if not report_path.exists():
            continue
        try:
            report = _load_json(report_path)
        except Exception:
            continue

        for row in report.get("summary", []) or []:
            direction = str(row.get("direction", "")).upper()
            target_distance = _safe_float(row.get("target_distance"))
            rules = row.get("rules", []) if isinstance(row.get("rules"), list) else []
            if not direction or target_distance <= 0.0:
                continue
            for rule in rules:
                replay = dict(rule.get("candidate_replay") or {})
                quarter = str(rule.get("quarter", "")).upper() or "Q?"
                path_class_name = str(rule.get("path_class_name", "")).strip().lower() or "unknown"
                feature_center = _feature_center(dict(rule.get("feature_bounds") or {}))
                trade_count = _safe_int(replay.get("trade_count"))
                if trade_count <= 0:
                    continue

                strategy_identity = {
                    "type": direction,
                    "entry_model": path_class_name,
                    "target_profile": f"T{target_distance:.1f}".replace(".", "_"),
                    "target_distance": target_distance,
                }
                pocket_context = {
                    "pair": pair,
                    "session": session,
                    "weekday": weekday,
                    "session_quarter": f"{session}_{quarter.lower()}",
                    "quarter": quarter,
                }
                entry_hit_rate = _safe_float(replay.get("win_rate"))
                # target_touch proxy is equivalent to hit rate in this entry-only replay.
                target_touch_rate = _safe_float(replay.get("win_rate"))
                records.append(
                    {
                        "node": node_dir.name,
                        "strategy_identity": strategy_identity,
                        "pocket_context": pocket_context,
                        "path_class_id": str(rule.get("path_class_id", "")),
                        "rule_name": path_class_name,
                        "trade_count": trade_count,
                        "entry_hit_rate": entry_hit_rate,
                        "target_touch_rate": target_touch_rate,
                        "expectancy": _safe_float(replay.get("expectancy")),
                        "avg_R": _safe_float(replay.get("avg_R")),
                        "pips_per_hour": _safe_float(replay.get("pips_per_hour")),
                        "good_capture": _safe_float(replay.get("good_capture")),
                        "bad_trigger": _safe_float(replay.get("bad_trigger")),
                        "noise_trigger": _safe_float(replay.get("noise_trigger")),
                        "feature_center": feature_center,
                    }
                )
    return records


def _group_by_strategy(records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for rec in records:
        sid = rec["strategy_identity"]
        key = f"{sid['type']}|{sid['entry_model']}|{sid['target_profile']}"
        grouped[key].append(rec)
    return grouped


def _related_seed_score(source: Dict[str, Any], target: Dict[str, Any]) -> float:
    sctx = source["pocket_context"]
    tctx = target["pocket_context"]
    score = 0.0
    if sctx["session"] == tctx["session"]:
        score += 2.0
    if sctx["weekday"] == tctx["weekday"]:
        score += 2.0
    if sctx["quarter"] == tctx["quarter"]:
        score += 2.0
    if sctx["pair"] == tctx["pair"]:
        score += 3.0
    score += _pair_similarity(sctx["pair"], tctx["pair"]) * 2.0
    score += min(2.0, _safe_float(source.get("entry_hit_rate")) * 2.0)
    score += min(1.0, _safe_int(source.get("trade_count")) / 250.0)
    return score


def _stable_features(winners: List[Dict[str, Any]], min_occurrence: int = 3) -> Dict[str, Dict[str, float]]:
    buckets: Dict[str, List[float]] = defaultdict(list)
    for rec in winners:
        for name, value in rec.get("feature_center", {}).items():
            buckets[name].append(_safe_float(value))
    out: Dict[str, Dict[str, float]] = {}
    for name, vals in buckets.items():
        if len(vals) < min_occurrence:
            continue
        mean = sum(vals) / len(vals)
        var = sum((v - mean) ** 2 for v in vals) / len(vals)
        std = math.sqrt(var)
        cv = (std / abs(mean)) if abs(mean) > 1e-6 else 999.0
        out[name] = {
            "mean": mean,
            "std": std,
            "cv": cv,
            "count": float(len(vals)),
            "stable": 1.0 if cv <= 0.35 else 0.0,
        }
    return out


def _top_feature_deltas(seed: Dict[str, Any], target: Dict[str, Any], top_n: int = 5) -> List[Dict[str, Any]]:
    a = seed.get("feature_center", {})
    b = target.get("feature_center", {})
    deltas: List[Tuple[str, float, float, float]] = []
    for k in sorted(set(a.keys()).intersection(b.keys())):
        av = _safe_float(a[k])
        bv = _safe_float(b[k])
        deltas.append((k, abs(av - bv), av, bv))
    deltas.sort(key=lambda x: x[1], reverse=True)
    return [
        {
            "feature": name,
            "abs_delta": delta,
            "seed_center": seed_v,
            "target_center": target_v,
        }
        for name, delta, seed_v, target_v in deltas[:top_n]
    ]


def _analyze_strategy(
    strategy_key: str,
    records: List[Dict[str, Any]],
    min_sample: int,
) -> Dict[str, Any]:
    pockets: List[Dict[str, Any]] = []
    for rec in records:
        sample = _safe_int(rec.get("trade_count"))
        hit = _safe_float(rec.get("entry_hit_rate"))
        tier = _tier_for_rate(hit)
        conf = _confidence_note(sample, min_sample)
        pocket = {
            **rec,
            "tier": tier,
            "sample_ok": sample >= min_sample,
            "confidence": conf,
            "status": (
                "viable" if tier in {"A", "B"} and sample >= min_sample else
                "borderline" if tier == "C" or sample < min_sample else
                "dead"
            ),
            "entry_only_metrics": {
                "sample_count": sample,
                "entry_hit_rate": hit,
                "target_touch_rate": _safe_float(rec.get("target_touch_rate")),
                "mfe_before_adverse_invalidation": None,
                "time_to_first_green": None,
                "spread_adjusted_edge_quality": _safe_float(rec.get("expectancy")),
                "notes": [
                    "mfe/time-to-first-green not available in current no-timeout summary schema",
                    "target_touch_rate is mapped to candidate_replay win_rate in this phase",
                ],
            },
        }
        pockets.append(pocket)

    winners = [p for p in pockets if p["status"] == "viable"]
    stable = _stable_features(winners)

    # Suggest related seeds for non-viable pockets.
    for pocket in pockets:
        if pocket["status"] == "viable" or not winners:
            pocket["seed_suggestion"] = None
            pocket["transfer_analysis"] = None
            continue
        best_seed = max(winners, key=lambda w: _related_seed_score(w, pocket))
        seed_score = _related_seed_score(best_seed, pocket)
        source_mode = "same_pocket" if best_seed["node"] == pocket["node"] else (
            "different_pair" if best_seed["pocket_context"]["pair"] != pocket["pocket_context"]["pair"] else "neighboring_pocket"
        )
        pocket["seed_suggestion"] = {
            "seed_node": best_seed["node"],
            "seed_pair": best_seed["pocket_context"]["pair"],
            "seed_session": best_seed["pocket_context"]["session"],
            "seed_weekday": best_seed["pocket_context"]["weekday"],
            "seed_quarter": best_seed["pocket_context"]["quarter"],
            "seed_hit_rate": best_seed["entry_hit_rate"],
            "seed_trade_count": best_seed["trade_count"],
            "seed_origin": source_mode,
            "related_score": seed_score,
        }
        pocket["transfer_analysis"] = {
            "transfer_success_likelihood": "high" if seed_score >= 8.0 else "medium" if seed_score >= 5.0 else "low",
            "top_parameter_deltas": _top_feature_deltas(best_seed, pocket),
            "stable_winner_traits": [
                {
                    "feature": name,
                    "mean": vals["mean"],
                    "std": vals["std"],
                    "cv": vals["cv"],
                }
                for name, vals in stable.items()
                if vals.get("stable", 0.0) >= 1.0
            ][:12],
        }

    pockets_sorted = sorted(
        pockets,
        key=lambda p: (
            0 if p["status"] == "viable" else 1 if p["status"] == "borderline" else 2,
            -_safe_float(p.get("entry_hit_rate")),
            -_safe_int(p.get("trade_count")),
        ),
    )

    strategy_summary = {
        "strategy_key": strategy_key,
        "total_pockets_tested": len(pockets),
        "pockets_with_enough_sample": sum(1 for p in pockets if p["sample_ok"]),
        "reached_50_plus": sum(1 for p in pockets if p["sample_ok"] and _safe_float(p.get("entry_hit_rate")) >= 0.50),
        "reached_60_plus": sum(1 for p in pockets if p["sample_ok"] and _safe_float(p.get("entry_hit_rate")) >= 0.60),
        "tier_counts": {
            "A": sum(1 for p in pockets if p["tier"] == "A"),
            "B": sum(1 for p in pockets if p["tier"] == "B"),
            "C": sum(1 for p in pockets if p["tier"] == "C"),
            "D": sum(1 for p in pockets if p["tier"] == "D"),
        },
        "viable_count": sum(1 for p in pockets if p["status"] == "viable"),
        "borderline_count": sum(1 for p in pockets if p["status"] == "borderline"),
        "dead_count": sum(1 for p in pockets if p["status"] == "dead"),
        "top_pockets": [
            {
                "node": p["node"],
                "pair": p["pocket_context"]["pair"],
                "session": p["pocket_context"]["session"],
                "weekday": p["pocket_context"]["weekday"],
                "quarter": p["pocket_context"]["quarter"],
                "tier": p["tier"],
                "entry_hit_rate": p["entry_hit_rate"],
                "trade_count": p["trade_count"],
            }
            for p in pockets_sorted[:15]
        ],
        "weak_pockets": [
            {
                "node": p["node"],
                "pair": p["pocket_context"]["pair"],
                "session": p["pocket_context"]["session"],
                "weekday": p["pocket_context"]["weekday"],
                "quarter": p["pocket_context"]["quarter"],
                "tier": p["tier"],
                "entry_hit_rate": p["entry_hit_rate"],
                "trade_count": p["trade_count"],
                "seed_suggestion": p["seed_suggestion"],
            }
            for p in pockets_sorted
            if p["status"] != "viable"
        ][:20],
        "unsalvageable_pockets": [
            {
                "node": p["node"],
                "pair": p["pocket_context"]["pair"],
                "session": p["pocket_context"]["session"],
                "weekday": p["pocket_context"]["weekday"],
                "quarter": p["pocket_context"]["quarter"],
                "entry_hit_rate": p["entry_hit_rate"],
                "trade_count": p["trade_count"],
                "confidence": p["confidence"],
            }
            for p in pockets_sorted
            if p["status"] == "dead" and p["sample_ok"]
        ],
    }

    return {
        "summary": strategy_summary,
        "pockets": pockets_sorted,
    }


def _rank_strategies(analyses: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for skey, data in analyses.items():
        s = data["summary"]
        rows.append(
            {
                "strategy_key": skey,
                "viable_pockets": s["viable_count"],
                "reached_60_plus": s["reached_60_plus"],
                "reached_50_plus": s["reached_50_plus"],
                "total_pockets": s["total_pockets_tested"],
                "sampled_pockets": s["pockets_with_enough_sample"],
            }
        )
    rows.sort(
        key=lambda r: (
            -_safe_int(r.get("viable_pockets")),
            -_safe_int(r.get("reached_60_plus")),
            -_safe_int(r.get("reached_50_plus")),
            -_safe_int(r.get("sampled_pockets")),
        )
    )
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description="Entry-only pocket optimizer report with related-node seed transfer guidance.")
    ap.add_argument("--compiled-root", type=Path, default=Path("compiled_market_nodes"))
    ap.add_argument("--out-json", type=Path, default=Path("artifacts/entry_only_pocket_optimization_report.json"))
    ap.add_argument("--out-survivors", type=Path, default=Path("artifacts/entry_only_pocket_survivors.json"))
    ap.add_argument("--min-sample", type=int, default=50)
    args = ap.parse_args()

    compiled_root = args.compiled_root
    if not compiled_root.is_absolute():
        compiled_root = Path(__file__).resolve().parent / compiled_root
    if not compiled_root.exists():
        raise FileNotFoundError(f"Missing compiled root: {compiled_root}")

    records = _extract_pocket_records(compiled_root)
    if not records:
        payload = {
            "status": "NO_DATA",
            "compiled_root": str(compiled_root),
            "message": "No target_entry_no_timeouts class reports found with candidate_replay rows.",
        }
        args.out_json.parent.mkdir(parents=True, exist_ok=True)
        args.out_json.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(payload, indent=2))
        return 0

    grouped = _group_by_strategy(records)
    analyses: Dict[str, Dict[str, Any]] = {}
    for skey, recs in sorted(grouped.items()):
        analyses[skey] = _analyze_strategy(skey, recs, args.min_sample)

    strategy_rank = _rank_strategies(analyses)

    survivors: List[Dict[str, Any]] = []
    for skey, data in analyses.items():
        for pocket in data["pockets"]:
            if pocket["status"] != "viable":
                continue
            survivors.append(
                {
                    "strategy_key": skey,
                    "strategy_identity": pocket["strategy_identity"],
                    "node": pocket["node"],
                    "pocket_context": pocket["pocket_context"],
                    "entry_hit_rate": pocket["entry_hit_rate"],
                    "trade_count": pocket["trade_count"],
                    "tier": pocket["tier"],
                    "confidence": pocket["confidence"],
                }
            )

    report = {
        "status": "PASS",
        "directive": {
            "phase_order": [
                "phase_1_entry_only_optimization",
                "phase_2_borderline_dead_split",
                "phase_3_strategy_and_pocket_reporting",
                "phase_4_aee_extraction_on_survivors_only",
            ],
            "search_doctrine": [
                "seed_from_related_winners_first",
                "cross_pair_inheritance",
                "coarse_to_fine_hierarchical_search",
                "report_what_works_and_why",
            ],
        },
        "min_sample": args.min_sample,
        "total_records": len(records),
        "total_strategies": len(analyses),
        "strategy_rank": strategy_rank,
        "strategies": {k: v["summary"] for k, v in analyses.items()},
        "strategy_details": analyses,
        "overall": {
            "viable_pockets": sum(v["summary"]["viable_count"] for v in analyses.values()),
            "borderline_pockets": sum(v["summary"]["borderline_count"] for v in analyses.values()),
            "dead_pockets": sum(v["summary"]["dead_count"] for v in analyses.values()),
            "strategies_with_viable_pockets": sum(1 for v in analyses.values() if v["summary"]["viable_count"] > 0),
            "strategies_with_almost_none": sum(1 for v in analyses.values() if v["summary"]["viable_count"] <= 1),
        },
    }

    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    args.out_survivors.parent.mkdir(parents=True, exist_ok=True)
    args.out_survivors.write_text(
        json.dumps(
            {
                "status": "PASS",
                "count": len(survivors),
                "min_sample": args.min_sample,
                "survivors": sorted(
                    survivors,
                    key=lambda r: (-_safe_float(r.get("entry_hit_rate")), -_safe_int(r.get("trade_count"))),
                ),
            },
            indent=2,
        ) + "\n",
        encoding="utf-8",
    )

    print(
        json.dumps(
            {
                "status": "PASS",
                "out_json": str(args.out_json),
                "out_survivors": str(args.out_survivors),
                "total_strategies": len(analyses),
                "viable_pockets": report["overall"]["viable_pockets"],
                "borderline_pockets": report["overall"]["borderline_pockets"],
                "dead_pockets": report["overall"]["dead_pockets"],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
