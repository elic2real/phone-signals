from __future__ import annotations

from typing import Any, Dict, List, Sequence, Tuple


def evaluate_candidate(
    *,
    candidate: Dict[str, Any],
    ticks: List[Dict[str, Any]],
    commission_pips: float,
    slippage_pips: float,
) -> Dict[str, Any]:
    if candidate.get("status") != "READY":
        return {
            "strategy_id": candidate.get("strategy_id"),
            "doctrine_id": candidate.get("doctrine_id"),
            "distance_expression_id": candidate.get("distance_expression_id"),
            "target_distance_bucket": candidate.get("target_distance_bucket"),
            "status": candidate.get("status", "ABORTED"),
            "reason": candidate.get("reason"),
            "pnl_pips": 0.0,
        }

    direction = str(candidate["direction"])
    anchor_index = int(candidate["anchor_index"])
    ttl_end = int(candidate["ttl_end_index"])
    entry_price = float(candidate["entry_price"])
    target_price = float(candidate["target_price"])
    stop_price = float(candidate["stop_price"])
    pip_size = 0.01 if "JPY" in str(candidate["profile_id"]).upper() else 0.0001
    exit_price = entry_price
    exit_reason = "TTL_EXIT"

    for idx in range(anchor_index + 1, ttl_end + 1):
        row = ticks[idx]
        bid = float(row["bid"])
        ask = float(row["ask"])
        if direction == "LONG":
            if bid >= target_price:
                exit_price = bid
                exit_reason = "TARGET_HIT"
                break
            if bid <= stop_price:
                exit_price = bid
                exit_reason = "STOP_HIT"
                break
            exit_price = bid
        else:
            if ask <= target_price:
                exit_price = ask
                exit_reason = "TARGET_HIT"
                break
            if ask >= stop_price:
                exit_price = ask
                exit_reason = "STOP_HIT"
                break
            exit_price = ask

    gross_pips = (exit_price - entry_price) / pip_size if direction == "LONG" else (entry_price - exit_price) / pip_size
    pnl_pips = gross_pips - commission_pips - slippage_pips
    return {
        "strategy_id": candidate["strategy_id"],
        "doctrine_id": candidate.get("doctrine_id"),
        "distance_expression_id": candidate.get("distance_expression_id"),
        "target_distance_bucket": candidate.get("target_distance_bucket"),
        "status": "FILLED",
        "reason": exit_reason,
        "pnl_pips": round(pnl_pips, 6),
        "gross_pips": round(gross_pips, 6),
        "direction": direction,
        "profile_id": candidate["profile_id"],
    }


def summarize_result_groups(
    results: List[Dict[str, Any]],
    *,
    group_fields: Sequence[str],
) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, ...], List[Dict[str, Any]]] = {}
    for row in results:
        group_key = tuple(str(row.get(field, "") or "") for field in group_fields)
        if not any(group_key):
            continue
        grouped.setdefault(group_key, []).append(row)

    summaries: List[Dict[str, Any]] = []
    for group_key, rows in grouped.items():
        filled = [row for row in rows if row.get("status") == "FILLED"]
        aborted = [row for row in rows if row.get("status") != "FILLED"]
        trade_count = len(filled)
        wins = sum(1 for row in filled if float(row.get("pnl_pips", 0.0) or 0.0) > 0.0)
        expectancy = sum(float(row.get("pnl_pips", 0.0) or 0.0) for row in filled) / max(trade_count, 1)
        net_pnl = sum(float(row.get("pnl_pips", 0.0) or 0.0) for row in filled)
        win_rate = wins / max(trade_count, 1)
        viable = trade_count >= 3 and expectancy > 0.0 and win_rate >= 0.40
        abort_reason_counts: Dict[str, int] = {}
        for row in aborted:
            reason = str(row.get("reason", row.get("status", "UNKNOWN")) or "UNKNOWN")
            abort_reason_counts[reason] = abort_reason_counts.get(reason, 0) + 1
        exit_reason_counts: Dict[str, int] = {}
        for row in filled:
            reason = str(row.get("reason", "UNKNOWN") or "UNKNOWN")
            exit_reason_counts[reason] = exit_reason_counts.get(reason, 0) + 1
        distance_expression_ids = sorted(
            {
                str(row.get("distance_expression_id", "") or "")
                for row in filled
                if str(row.get("distance_expression_id", "") or "")
            }
        )
        target_distance_buckets = sorted(
            {
                str(row.get("target_distance_bucket", "") or "")
                for row in filled
                if str(row.get("target_distance_bucket", "") or "")
            }
        )
        scenarios = sorted({str(row.get("scenario", "") or "") for row in rows if str(row.get("scenario", "") or "")})
        summary: Dict[str, Any] = {
            field: group_key[idx]
            for idx, field in enumerate(group_fields)
        }
        summaries.append(
            {
                **summary,
                "trade_count": trade_count,
                "aborted_count": len(aborted),
                "result_count": len(rows),
                "distance_expression_count": len(distance_expression_ids),
                "target_distance_bucket_count": len(target_distance_buckets),
                "distance_expression_ids": distance_expression_ids,
                "target_distance_buckets": target_distance_buckets,
                "win_rate": round(win_rate, 6),
                "expectancy_pips": round(expectancy, 6),
                "net_pnl_pips": round(net_pnl, 6),
                "viable": viable,
                "abort_reason_counts": abort_reason_counts,
                "exit_reason_counts": exit_reason_counts,
                "scenario_count": len(scenarios),
                "scenarios": scenarios,
            }
        )
    summaries.sort(key=lambda row: (row["viable"], row["expectancy_pips"], row["net_pnl_pips"]), reverse=True)
    return summaries


def summarize_strategy_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return summarize_result_groups(results, group_fields=["strategy_id"])
