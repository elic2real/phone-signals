from __future__ import annotations

import math
from collections import Counter
from typing import Any, Dict, List, Tuple

from tools.v2_tier1_truth_kernel import build_truth_kernel


def _direction_group(profile: Dict[str, Any]) -> str | None:
    direct = str(profile.get("direction_group", "") or "").upper()
    if direct in {"LONG", "SHORT"}:
        return direct
    bias = str(profile.get("vector_bias", "") or "").upper()
    if bias == "UP":
        return "LONG"
    if bias == "DOWN":
        return "SHORT"
    return None


def _episode_key(profile: Dict[str, Any]) -> str:
    key = str(profile.get("opportunity_episode_id") or "").strip()
    if key and key != "NO_OPPORTUNITY_EPISODE":
        return key
    return str(profile.get("profile_id"))


def _representation_key(profile: Dict[str, Any]) -> str:
    episode = _episode_key(profile)
    distance_family = str(profile.get("distance_family_id", "") or "")
    energy_family = str(profile.get("energy_family_id", "") or "")
    if distance_family or energy_family:
        return f"{episode}|{distance_family}|{energy_family}"
    return episode


def _confidence_rank(profile: Dict[str, Any]) -> int:
    tier = str(profile.get("opportunity_confidence_tier", "") or "").upper()
    rank = {
        "QUALIFIED_CONSERVATIVE": 5,
        "QUALIFIED_PATH": 4,
        "CONSERVATIVE": 3,
        "AGGRESSIVE_PATH": 2,
        "NON_OPPORTUNITY": 1,
    }
    return rank.get(tier, 0)


def _float(profile: Dict[str, Any], key: str) -> float:
    try:
        out = float(profile.get(key, 0.0) or 0.0)
    except Exception:
        return 0.0
    if math.isnan(out) or math.isinf(out):
        return 0.0
    return out


def _pick_episode_representatives(profiles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best: Dict[str, Dict[str, Any]] = {}
    for profile in profiles:
        key = _representation_key(profile)
        current = best.get(key)
        if current is None:
            best[key] = profile
            continue
        current_sort = (
            _confidence_rank(current),
            int(bool(current.get("pattern_qualified_opportunity"))),
            _float(current, "discovered_distance_pips"),
            _float(current, "impulse_ratio"),
        )
        candidate_sort = (
            _confidence_rank(profile),
            int(bool(profile.get("pattern_qualified_opportunity"))),
            _float(profile, "discovered_distance_pips"),
            _float(profile, "impulse_ratio"),
        )
        if candidate_sort > current_sort:
            best[key] = profile
    return list(best.values())


def _is_raw_opportunity(profile: Dict[str, Any]) -> bool:
    return bool(
        profile.get("pattern_qualified_opportunity")
        or profile.get("conservative_opportunity")
        or profile.get("aggressive_path_opportunity")
    )


def _payload_ready(profile: Dict[str, Any]) -> bool:
    return str(profile.get("payload_status", "") or "") == "READY"


def _doctrine_state(profile: Dict[str, Any]) -> Tuple[str, str]:
    doctrine_family_id = str(profile.get("doctrine_family_id", "") or "").upper()
    if _payload_ready(profile) and doctrine_family_id and doctrine_family_id not in {"NO_DOCTRINE_MATCH", "DEFERRED_TOXIC_BOOK"}:
        return doctrine_family_id, "PAYLOAD_MATCHED"
    return "NO_DOCTRINE_MATCH", "REJECTED"


def _doctrine_name(pattern_match_state: str, direction: str) -> str:
    del direction
    return str(pattern_match_state or "NO_DOCTRINE_MATCH").upper()


def _sorted_counter_keys(counter: Counter[str], limit: int | None = None) -> List[str]:
    rows = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    if limit is not None:
        rows = rows[:limit]
    return [key for key, _ in rows]


def _share(count: int, total: int) -> float:
    return count / max(total, 1)


def _dominant_keys(
    counter: Counter[str],
    *,
    total: int,
    min_share: float,
    min_count: int,
    limit: int,
) -> List[str]:
    keys = [
        key
        for key, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
        if key and count >= min_count and _share(count, total) >= min_share
    ]
    if not keys and counter:
        keys = [max(counter.items(), key=lambda item: (item[1], item[0]))[0]]
    return keys[:limit]


def _signature_id(profile: Dict[str, Any]) -> str:
    return "|".join(
        [
            str(profile.get("topology_family_id", "") or ""),
            str(profile.get("location_relation_id", "") or ""),
            str(profile.get("precursor_state", "") or ""),
            str(profile.get("energy_state", "") or ""),
        ]
    )


def _truth_kernel(profile: Dict[str, Any]) -> Dict[str, Any]:
    kernel = dict(profile.get("truth_kernel", {}) or {})
    if kernel:
        return kernel
    return build_truth_kernel(profile)


def _kernel_value(profile: Dict[str, Any], group: str, field: str, default: str = "UNKNOWN") -> str:
    kernel = _truth_kernel(profile)
    return str(((kernel.get(group, {}) or {}).get(field, default) or default))


def _active_level_type(profile: Dict[str, Any]) -> str:
    direction = _direction_group(profile)
    if direction == "LONG":
        return _kernel_value(profile, "structure_kernel", "level_type_long")
    if direction == "SHORT":
        return _kernel_value(profile, "structure_kernel", "level_type_short")
    return "UNKNOWN"


def _kernel_signature(profile: Dict[str, Any]) -> str:
    kernel = _truth_kernel(profile)
    return str(kernel.get("kernel_signature", "") or "")


def _build_distance_expressions(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        distance_family_id = str(row.get("distance_family_id", "") or "")
        if not distance_family_id:
            continue
        grouped.setdefault(distance_family_id, []).append(row)

    expressions: List[Dict[str, Any]] = []
    total = len(rows)
    for distance_family_id, family_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), item[0])):
        bucket_counts = Counter(str(row.get("target_distance_bucket", "") or "") for row in family_rows)
        topology_counts = Counter(str(row.get("topology_family_id", "") or "") for row in family_rows)
        location_counts = Counter(str(row.get("location_relation_id", "") or "") for row in family_rows)
        precursor_counts = Counter(str(row.get("precursor_state", "") or "") for row in family_rows)
        energy_counts = Counter(str(row.get("energy_state", "") or "") for row in family_rows)
        confidence_counts = Counter(str(row.get("opportunity_confidence_tier", "") or "") for row in family_rows)
        qualified_count = sum(1 for row in family_rows if bool(row.get("pattern_qualified_opportunity")))
        expressions.append(
            {
                "distance_family_id": distance_family_id,
                "target_distance_bucket": max(bucket_counts, key=bucket_counts.get),
                "support_count": len(family_rows),
                "support_share": round(_share(len(family_rows), total), 6),
                "qualified_share": round(_share(qualified_count, len(family_rows)), 6),
                "dominant_topology_family_id": max(topology_counts, key=topology_counts.get),
                "dominant_location_relation_id": max(location_counts, key=location_counts.get),
                "dominant_precursor_state": max(precursor_counts, key=precursor_counts.get),
                "dominant_energy_state": max(energy_counts, key=energy_counts.get),
                "allowed_confidence_tiers": _sorted_counter_keys(confidence_counts),
            }
        )
    return expressions


def fit_phase2_clusters(profiles: List[Dict[str, Any]], seed: int) -> Dict[str, Any]:
    del seed
    eligible = [
        profile
        for profile in profiles
        if _is_raw_opportunity(profile) and _direction_group(profile) in {"LONG", "SHORT"} and _payload_ready(profile)
    ]
    representatives = _pick_episode_representatives(eligible)

    doctrine_groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    doctrine_sources: Dict[Tuple[str, str], str] = {}
    for profile in representatives:
        direction = _direction_group(profile)
        doctrine_state, doctrine_source = _doctrine_state(profile)
        if doctrine_source == "REJECTED":
            continue
        doctrine_groups.setdefault((direction, doctrine_state), []).append(profile)
        doctrine_sources[(direction, doctrine_state)] = doctrine_source

    doctrines: List[Dict[str, Any]] = []
    assignments: Dict[str, str] = {}
    doctrine_source_counts = Counter()
    for (direction, doctrine_state), rows in sorted(doctrine_groups.items(), key=lambda item: item[0]):
        doctrine_id = _doctrine_name(doctrine_state, direction)
        doctrine_source = doctrine_sources[(direction, doctrine_state)]
        doctrine_source_counts[doctrine_source] += 1
        signature_counts = Counter(_signature_id(row) for row in rows)
        topology_counts = Counter(str(row.get("topology_family_id", "") or "") for row in rows)
        location_counts = Counter(str(row.get("location_relation_id", "") or "") for row in rows)
        precursor_state_counts = Counter(str(row.get("precursor_state", "") or "") for row in rows)
        precursor_family_counts = Counter(str(row.get("precursor_family_id", "") or "") for row in rows)
        energy_state_counts = Counter(str(row.get("energy_state", "") or "") for row in rows)
        energy_family_counts = Counter(str(row.get("energy_family_id", "") or "") for row in rows)
        order_flow_band_counts = Counter(_kernel_value(row, "direction_kernel", "order_flow_band") for row in rows)
        direction_alignment_counts = Counter(_kernel_value(row, "direction_kernel", "direction_alignment_band") for row in rows)
        book_quality_counts = Counter(_kernel_value(row, "quality_kernel", "book_quality_band") for row in rows)
        active_level_type_counts = Counter(_active_level_type(row) for row in rows)
        truth_kernel_signature_counts = Counter(_kernel_signature(row) for row in rows)
        distance_bucket_counts = Counter(str(row.get("target_distance_bucket", "") or "") for row in rows)
        distance_family_counts = Counter(str(row.get("distance_family_id", "") or "") for row in rows)
        confidence_counts = Counter(str(row.get("opportunity_confidence_tier", "") or "") for row in rows)
        zone_counts = Counter(str(row.get("zone_state", "") or "") for row in rows)
        qualified_count = sum(1 for row in rows if bool(row.get("pattern_qualified_opportunity")))
        conservative_count = sum(1 for row in rows if bool(row.get("conservative_opportunity")))
        aggressive_count = sum(1 for row in rows if bool(row.get("aggressive_path_opportunity")))

        avg_velocity = sum(abs(_float(row, "velocity_pips_per_sec")) for row in rows) / max(len(rows), 1)
        avg_acceleration = sum(abs(_float(row, "acceleration_pips_per_sec2")) for row in rows) / max(len(rows), 1)
        avg_compression = sum(_float(row, "compression_ratio") for row in rows) / max(len(rows), 1)
        avg_distance = sum(_float(row, "discovered_distance_pips") for row in rows) / max(len(rows), 1)
        avg_impulse = sum(_float(row, "impulse_ratio") for row in rows) / max(len(rows), 1)

        member_episode_ids = sorted({_episode_key(row) for row in rows})
        distance_expressions = _build_distance_expressions(rows)
        support_core_topologies = _dominant_keys(topology_counts, total=len(rows), min_share=0.12, min_count=2, limit=4)
        support_core_locations = _dominant_keys(location_counts, total=len(rows), min_share=0.12, min_count=2, limit=4)
        support_core_precursors = _dominant_keys(
            precursor_state_counts,
            total=len(rows),
            min_share=0.18,
            min_count=2,
            limit=3,
        )
        support_core_energy_states = _dominant_keys(
            energy_state_counts,
            total=len(rows),
            min_share=0.18,
            min_count=2,
            limit=3,
        )
        support_core_order_flow_bands = _dominant_keys(
            order_flow_band_counts,
            total=len(rows),
            min_share=0.18,
            min_count=2,
            limit=3,
        )
        support_core_direction_alignment_bands = _dominant_keys(
            direction_alignment_counts,
            total=len(rows),
            min_share=0.18,
            min_count=2,
            limit=3,
        )
        support_core_book_quality_bands = _dominant_keys(
            book_quality_counts,
            total=len(rows),
            min_share=0.18,
            min_count=2,
            limit=3,
        )
        support_core_level_types = _dominant_keys(
            active_level_type_counts,
            total=len(rows),
            min_share=0.18,
            min_count=2,
            limit=3,
        )
        support_signatures = _dominant_keys(signature_counts, total=len(rows), min_share=0.08, min_count=2, limit=8)
        support_kernel_signatures = _dominant_keys(
            truth_kernel_signature_counts,
            total=len(rows),
            min_share=0.08,
            min_count=2,
            limit=8,
        )
        for row in rows:
            assignments[str(row["profile_id"])] = doctrine_id

        doctrines.append(
            {
                "cluster_id": doctrine_id,
                "doctrine_id": doctrine_id,
                "direction_group": direction,
                "pattern_match_state": doctrine_state,
                "doctrine_source": doctrine_source,
                "cluster_size": len(rows),
                "episode_count": len(member_episode_ids),
                "match_mode": "PATTERN_MATCH_STATE_PRIMARY_WITH_SUPPORT_CONTRACT",
                "average_abs_velocity": round(avg_velocity, 6),
                "average_abs_acceleration": round(avg_acceleration, 6),
                "average_compression_ratio": round(avg_compression, 6),
                "average_discovered_distance_pips": round(avg_distance, 6),
                "average_impulse_ratio": round(avg_impulse, 6),
                "pattern_qualified_share": round(_share(qualified_count, len(rows)), 6),
                "conservative_share": round(_share(conservative_count, len(rows)), 6),
                "aggressive_path_share": round(_share(aggressive_count, len(rows)), 6),
                "dominant_zone_state": max(zone_counts, key=zone_counts.get),
                "support_contract": {
                    "direction_group": direction,
                    "pattern_match_state": doctrine_state,
                    "minimum_score_to_assign": 5,
                    "minimum_support_hits": 2,
                    "assignment_policy": "pattern_state_then_support_signature",
                },
                "support_core_topology_family_ids": support_core_topologies,
                "support_core_location_relation_ids": support_core_locations,
                "support_core_precursor_states": support_core_precursors,
                "support_core_energy_states": support_core_energy_states,
                "support_core_order_flow_bands": support_core_order_flow_bands,
                "support_core_direction_alignment_bands": support_core_direction_alignment_bands,
                "support_core_book_quality_bands": support_core_book_quality_bands,
                "support_core_level_types": support_core_level_types,
                "support_signatures": support_signatures,
                "support_kernel_signatures": support_kernel_signatures,
                "allowed_target_distance_buckets": _sorted_counter_keys(distance_bucket_counts),
                "allowed_distance_families": _sorted_counter_keys(distance_family_counts, limit=24),
                "allowed_topology_family_ids": _sorted_counter_keys(topology_counts, limit=12),
                "allowed_location_relation_ids": _sorted_counter_keys(location_counts, limit=8),
                "allowed_precursor_states": _sorted_counter_keys(precursor_state_counts),
                "allowed_precursor_family_ids": _sorted_counter_keys(precursor_family_counts, limit=16),
                "allowed_energy_states": _sorted_counter_keys(energy_state_counts),
                "allowed_energy_family_ids": _sorted_counter_keys(energy_family_counts, limit=16),
                "allowed_order_flow_bands": _sorted_counter_keys(order_flow_band_counts),
                "allowed_direction_alignment_bands": _sorted_counter_keys(direction_alignment_counts),
                "allowed_book_quality_bands": _sorted_counter_keys(book_quality_counts),
                "allowed_level_types": _sorted_counter_keys(active_level_type_counts),
                "allowed_truth_kernel_signatures": _sorted_counter_keys(truth_kernel_signature_counts, limit=24),
                "allowed_confidence_tiers": _sorted_counter_keys(confidence_counts),
                "distance_expression_count": len(distance_expressions),
                "distance_expressions": distance_expressions,
                "member_episode_ids": member_episode_ids,
                "profile_ids": [str(row["profile_id"]) for row in rows],
            }
        )

    doctrines.sort(key=lambda row: (-int(row["episode_count"]), row["doctrine_id"]))
    return {
        "clusters": doctrines,
        "assignments": assignments,
        "coverage": {
            "raw_opportunity_input_count": len(eligible),
            "representative_count": len(representatives),
            "doctrine_count": len(doctrines),
            "doctrine_source_counts": dict(doctrine_source_counts),
        },
    }


def assign_profile_to_cluster(profile: Dict[str, Any], clusters: List[Dict[str, Any]]) -> str | None:
    direction = _direction_group(profile)
    if direction is None:
        return None

    doctrine_state, _ = _doctrine_state(profile)
    eligible = [
        doctrine
        for doctrine in clusters
        if doctrine.get("direction_group") == direction and doctrine.get("pattern_match_state") == doctrine_state
    ]
    if not eligible:
        return None

    best_id = None
    best_score = -1
    best_support_hits = -1
    signature_id = _signature_id(profile)
    kernel_signature = _kernel_signature(profile)
    order_flow_band = _kernel_value(profile, "direction_kernel", "order_flow_band")
    direction_alignment_band = _kernel_value(profile, "direction_kernel", "direction_alignment_band")
    book_quality_band = _kernel_value(profile, "quality_kernel", "book_quality_band")
    active_level_type = _active_level_type(profile)
    for doctrine in eligible:
        score = 0
        support_hits = 0
        if str(profile.get("distance_family_id", "")) in set(doctrine.get("allowed_distance_families", [])):
            score += 3
            support_hits += 1
        if str(profile.get("target_distance_bucket", "")) in set(doctrine.get("allowed_target_distance_buckets", [])):
            score += 1
        if str(profile.get("topology_family_id", "")) in set(doctrine.get("allowed_topology_family_ids", [])):
            score += 2
        if str(profile.get("location_relation_id", "")) in set(doctrine.get("allowed_location_relation_ids", [])):
            score += 2
        if str(profile.get("precursor_state", "")) in set(doctrine.get("allowed_precursor_states", [])):
            score += 1
        if str(profile.get("precursor_family_id", "")) in set(doctrine.get("allowed_precursor_family_ids", [])):
            score += 1
        if str(profile.get("energy_state", "")) in set(doctrine.get("allowed_energy_states", [])):
            score += 1
        if str(profile.get("energy_family_id", "")) in set(doctrine.get("allowed_energy_family_ids", [])):
            score += 1
        if str(profile.get("opportunity_confidence_tier", "")) in set(doctrine.get("allowed_confidence_tiers", [])):
            score += 1
        if order_flow_band in set(doctrine.get("allowed_order_flow_bands", [])):
            score += 1
        if direction_alignment_band in set(doctrine.get("allowed_direction_alignment_bands", [])):
            score += 1
        if book_quality_band in set(doctrine.get("allowed_book_quality_bands", [])):
            score += 1
        if active_level_type in set(doctrine.get("allowed_level_types", [])):
            score += 1
        if kernel_signature in set(doctrine.get("allowed_truth_kernel_signatures", [])):
            score += 2
        if signature_id in set(doctrine.get("support_signatures", [])):
            score += 3
            support_hits += 1
        if kernel_signature in set(doctrine.get("support_kernel_signatures", [])):
            score += 2
            support_hits += 1
        if str(profile.get("topology_family_id", "")) in set(doctrine.get("support_core_topology_family_ids", [])):
            support_hits += 1
        if str(profile.get("location_relation_id", "")) in set(doctrine.get("support_core_location_relation_ids", [])):
            support_hits += 1
        if str(profile.get("precursor_state", "")) in set(doctrine.get("support_core_precursor_states", [])):
            support_hits += 1
        if str(profile.get("energy_state", "")) in set(doctrine.get("support_core_energy_states", [])):
            support_hits += 1
        if order_flow_band in set(doctrine.get("support_core_order_flow_bands", [])):
            support_hits += 1
        if direction_alignment_band in set(doctrine.get("support_core_direction_alignment_bands", [])):
            support_hits += 1
        if book_quality_band in set(doctrine.get("support_core_book_quality_bands", [])):
            support_hits += 1
        if active_level_type in set(doctrine.get("support_core_level_types", [])):
            support_hits += 1

        if (score, support_hits) > (best_score, best_support_hits):
            best_score = score
            best_support_hits = support_hits
            best_id = str(doctrine["doctrine_id"])

    doctrine = next((row for row in eligible if str(row["doctrine_id"]) == best_id), None)
    if doctrine is None:
        return None
    contract = doctrine.get("support_contract", {})
    if best_score < int(contract.get("minimum_score_to_assign", 0)):
        return None
    if best_support_hits < int(contract.get("minimum_support_hits", 0)):
        return None
    return best_id
