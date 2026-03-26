#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _parse_ts(ts: str) -> float | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _target_key(v: Any) -> str:
    x = _safe_float(v)
    return f"{x:.1f}"


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_state_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8", errors="ignore") as f:
        return list(csv.DictReader(f))


@dataclass
class TradeOutcome:
    trade_id: str
    reason: str
    pips: float
    realized_r: float
    hold_sec: float
    max_profit: float
    sl_like: bool


def _get_rule_conditions(cfg: dict[str, Any], rule_id: str) -> dict[str, float]:
    for rule in cfg.get("base_rules", []):
        if str(rule.get("rule_id", "")).strip() == rule_id:
            cond = rule.get("conditions", {}) or {}
            return {k: _safe_float(v) for k, v in cond.items()}
    return {}


def _evaluate_aee_trade(rows: list[dict[str, str]], cfg: dict[str, Any]) -> TradeOutcome:
    first = rows[0]
    trade_id = str(first.get("trade_id", ""))
    direction = str(first.get("direction", "")).upper().strip() or "LONG"
    target = _target_key(first.get("target_distance", "0"))

    panic = _get_rule_conditions(cfg, "base_panic")
    decay = _get_rule_conditions(cfg, "base_decay")
    harvest = _get_rule_conditions(cfg, "base_harvest")

    dmods = (cfg.get("direction_modifiers", {}) or {}).get(direction, {}) or {}
    tmods = (cfg.get("target_modifiers", {}) or {}).get(target, {}) or {}

    proving_window = max(1, _safe_int(tmods.get("proving_window", 1), 1))

    panic_opp_pressure = _safe_float(dmods.get("panic_opposite_pressure", panic.get("opposite_direction_strength_min", 0.0)))
    harvest_profit_floor = _safe_float(dmods.get("harvest_profit_floor", harvest.get("profit_now_min", 0.0)))
    harvest_giveback_tol = _safe_float(tmods.get("harvest_giveback_tolerance", harvest.get("giveback_now_min", 0.0)))
    decay_tsp = _safe_float(tmods.get("decay_time_since_peak", decay.get("time_since_peak_min", 0.0)))

    max_profit = max(_safe_float(r.get("profit_now", 0.0)) for r in rows)
    target_distance = max(0.1, _safe_float(first.get("target_distance", 1.0), 1.0))

    for row in rows:
        bar_index = max(1, _safe_int(row.get("bar_index", 1), 1))
        if bar_index < proving_window:
            continue

        profit_now = _safe_float(row.get("profit_now", 0.0))
        velocity_now = _safe_float(row.get("velocity_now", 0.0))
        giveback_now = _safe_float(row.get("giveback_now", 0.0))
        opp = _safe_float(row.get("opposite_direction_strength", 0.0))
        time_open = _safe_float(row.get("time_open", bar_index))
        time_since_peak = _safe_float(row.get("time_since_peak", 0.0))
        progress_ratio = _safe_float(row.get("progress_ratio", 0.0))
        energy_ratio = _safe_float(row.get("energy_ratio", 0.0))

        panic_hit = (
            profit_now <= panic.get("profit_now_max", float("-inf"))
            and velocity_now <= panic.get("velocity_now_max", float("-inf"))
            and giveback_now >= panic.get("giveback_now_min", float("inf"))
            and opp >= max(panic.get("opposite_direction_strength_min", 0.0), panic_opp_pressure)
            and time_open >= panic.get("time_open_min", float("inf"))
        )
        if panic_hit:
            pips = profit_now
            return TradeOutcome(
                trade_id=trade_id,
                reason="AEE_BAND_FAST_FAILURE_EXIT",
                pips=pips,
                realized_r=pips / target_distance,
                hold_sec=float(bar_index * 60),
                max_profit=max_profit,
                sl_like=True,
            )

        decay_hit = (
            time_since_peak >= max(decay.get("time_since_peak_min", 0.0), decay_tsp)
            and giveback_now >= decay.get("giveback_now_min", float("inf"))
            and progress_ratio <= decay.get("progress_ratio_max", float("inf"))
            and energy_ratio <= decay.get("energy_ratio_max", float("inf"))
        )
        if decay_hit:
            pips = profit_now
            return TradeOutcome(
                trade_id=trade_id,
                reason="AEE_BAND_EXTENSION_DECAY_EXIT",
                pips=pips,
                realized_r=pips / target_distance,
                hold_sec=float(bar_index * 60),
                max_profit=max_profit,
                sl_like=pips < 0.0,
            )

        harvest_hit = (
            profit_now >= max(harvest.get("profit_now_min", 0.0), harvest_profit_floor)
            and giveback_now >= max(harvest.get("giveback_now_min", 0.0), harvest_giveback_tol)
            and progress_ratio >= harvest.get("progress_ratio_min", float("-inf"))
            and energy_ratio >= harvest.get("energy_ratio_min", float("-inf"))
        )
        if harvest_hit:
            pips = profit_now
            return TradeOutcome(
                trade_id=trade_id,
                reason="AEE_BAND_EARLY_PROFIT_LOCK",
                pips=pips,
                realized_r=pips / target_distance,
                hold_sec=float(bar_index * 60),
                max_profit=max_profit,
                sl_like=False,
            )

    last = rows[-1]
    fallback_pips = _safe_float(last.get("static_pips", _safe_float(last.get("profit_now", 0.0))))
    fallback_bar = max(1, _safe_int(last.get("bar_index", len(rows)), len(rows)))
    return TradeOutcome(
        trade_id=trade_id,
        reason="STATIC_TIMEOUT",
        pips=fallback_pips,
        realized_r=fallback_pips / target_distance,
        hold_sec=float(fallback_bar * 60),
        max_profit=max_profit,
        sl_like=fallback_pips < 0.0,
    )


def _evaluate_static_trade(rows: list[dict[str, str]]) -> TradeOutcome:
    first = rows[0]
    last = rows[-1]
    trade_id = str(first.get("trade_id", ""))
    static_pips = _safe_float(last.get("static_pips", 0.0))
    target_distance = max(0.1, _safe_float(first.get("target_distance", 1.0), 1.0))
    static_r = _safe_float(last.get("static_R", static_pips / target_distance))
    reason = str(last.get("static_reason", "STATIC"))
    static_exit_bar = max(1, _safe_int(last.get("static_exit_bar", last.get("bar_index", len(rows))), len(rows)))
    max_profit = max(_safe_float(r.get("profit_now", 0.0)) for r in rows)
    sl_like = reason in {"SL_HIT", "PANIC", "DECAY_EXIT"} or static_pips < 0.0

    return TradeOutcome(
        trade_id=trade_id,
        reason=reason,
        pips=static_pips,
        realized_r=static_r,
        hold_sec=float(static_exit_bar * 60),
        max_profit=max_profit,
        sl_like=sl_like,
    )


def _window_duration_hours(rows: list[dict[str, str]]) -> float:
    ts_values: list[float] = []
    for row in rows:
        ts = _parse_ts(str(row.get("timestamp", "")))
        if ts is not None:
            ts_values.append(ts)
    if len(ts_values) < 2:
        return 1.0
    span_sec = max(1.0, max(ts_values) - min(ts_values))
    return span_sec / 3600.0


def _compute_metrics(outcomes: list[TradeOutcome], duration_hr: float, usd_per_pip: float) -> dict[str, Any]:
    counts = Counter(o.reason for o in outcomes)
    exit_count = len(outcomes)
    total_pips = sum(o.pips for o in outcomes)
    total_usd = total_pips * usd_per_pip
    avg_hold_sec = (sum(o.hold_sec for o in outcomes) / exit_count) if exit_count else 0.0
    avg_r = (sum(o.realized_r for o in outcomes) / exit_count) if exit_count else 0.0

    green_rows = sum(1 for o in outcomes if o.max_profit > 0.0)
    green_roundtrip_losses = sum(1 for o in outcomes if o.max_profit > 0.0 and o.pips <= 0.0)
    sl_hits = sum(1 for o in outcomes if o.sl_like)

    close_cycle_capture_rate = (exit_count / duration_hr) if duration_hr > 0 else 0.0

    return {
        "window_duration_hr": duration_hr,
        "exit_count": exit_count,
        "top_aee_reasons": counts.most_common(12),
        "counts_per_reason": dict(counts),
        "realized_pips_per_hour": (total_pips / duration_hr) if duration_hr > 0 else 0.0,
        "realized_usd_per_hour": (total_usd / duration_hr) if duration_hr > 0 else 0.0,
        "close_cycle_capture_rate": close_cycle_capture_rate,
        "avg_hold_sec": avg_hold_sec,
        "avg_realized_r": avg_r,
        "green_roundtrip_loss_rate": (green_roundtrip_losses / green_rows) if green_rows > 0 else 0.0,
        "sl_hit_rate": (sl_hits / exit_count) if exit_count > 0 else 0.0,
        "capital_recycling_rate": close_cycle_capture_rate,
    }


def _apply_rejection_rules(candidate: dict[str, Any], baseline: dict[str, Any]) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    c = candidate["metrics"]
    b = baseline["metrics"]

    if c.get("exit_count", 0) <= 0 or c.get("realized_pips_per_hour", 0.0) <= 0.0:
        reasons.append("zero_or_negative_extraction")

    if c.get("avg_hold_sec", 0.0) > b.get("avg_hold_sec", 0.0) and c.get("realized_pips_per_hour", 0.0) <= b.get("realized_pips_per_hour", 0.0):
        reasons.append("hold_time_increase_without_extraction_gain")

    burden_c = c.get("green_roundtrip_loss_rate", 0.0) + c.get("sl_hit_rate", 0.0)
    burden_b = b.get("green_roundtrip_loss_rate", 0.0) + b.get("sl_hit_rate", 0.0)
    if burden_c > burden_b and c.get("realized_pips_per_hour", 0.0) <= b.get("realized_pips_per_hour", 0.0):
        reasons.append("higher_progress_failure_burden_without_extraction_gain")

    if c.get("avg_realized_r", 0.0) <= 0.0:
        reasons.append("weak_or_negative_avg_realized_r")

    return (len(reasons) > 0), reasons


def _resolve_default_configs(root: Path) -> list[tuple[str, Path]]:
    candidates = [
        ("canonical", root / "compiled_aee_stage_11_sessions_canonical" / "aee_rules" / "aee_rules.json"),
        ("canonical_v2", root / "compiled_aee_stage_11_sessions_canonical_v2" / "aee_rules" / "aee_rules.json"),
        ("canonical_v3", root / "compiled_aee_stage_11_sessions_canonical_v3" / "aee_rules" / "aee_rules.json"),
        ("seeded", root / "compiled_aee_stage_11_sessions_seeded" / "aee_rules" / "aee_rules.json"),
    ]
    return [(name, path) for name, path in candidates if path.exists()]


def _parse_config_arg(values: list[str], root: Path) -> list[tuple[str, Path]]:
    parsed: list[tuple[str, Path]] = []
    for raw in values:
        text = raw.strip()
        if not text:
            continue
        if "=" in text:
            name, p = text.split("=", 1)
            path = Path(p).expanduser()
            if not path.is_absolute():
                path = (root / path).resolve()
            parsed.append((name.strip(), path))
        else:
            path = Path(text).expanduser()
            if not path.is_absolute():
                path = (root / path).resolve()
            parsed.append((path.parent.parent.parent.name, path))
    return parsed


def _resolve_configs_from_globs(patterns: list[str], root: Path) -> list[tuple[str, Path]]:
    resolved: list[tuple[str, Path]] = []
    for pattern in patterns:
        text = pattern.strip()
        if not text:
            continue
        for p in root.glob(text):
            if not p.is_file():
                continue
            rel = p.relative_to(root)
            label = str(rel.parent.parent.parent)
            resolved.append((label, p.resolve()))
    return resolved


def _dedupe_configs(configs: list[tuple[str, Path]]) -> list[tuple[str, Path]]:
    seen: set[str] = set()
    out: list[tuple[str, Path]] = []
    for name, path in configs:
        k = str(path)
        if k in seen:
            continue
        seen.add(k)
        out.append((name, path))
    return out


def _resolve_streams_from_globs(patterns: list[str], root: Path) -> list[Path]:
    resolved: list[Path] = []
    for pattern in patterns:
        text = pattern.strip()
        if not text:
            continue
        for p in root.glob(text):
            if p.is_file():
                resolved.append(p.resolve())
    return resolved


def _dedupe_paths(paths: list[Path]) -> list[Path]:
    seen: set[str] = set()
    out: list[Path] = []
    for p in paths:
        s = str(p)
        if s in seen:
            continue
        seen.add(s)
        out.append(p)
    return out


def _source_node_from_config(name: str) -> str:
    parts = [p for p in name.split("/") if p]
    if len(parts) >= 2 and parts[0] == "compiled_market_nodes":
        return parts[1]
    return name


def _stream_label(path: Path, root: Path) -> str:
    try:
        rel = path.relative_to(root)
    except ValueError:
        rel = path
    parts = rel.parts
    if len(parts) >= 3 and parts[0].startswith("compiled_aee_stage_"):
        return parts[0]
    if len(parts) >= 4 and parts[0] == "compiled_market_nodes":
        return "/".join(parts[0:2])
    return str(rel)


def main() -> None:
    ap = argparse.ArgumentParser(description="Historical replay-first AEE system extraction scoreboard and config ranking.")
    ap.add_argument(
        "--state-stream",
        default="compiled_aee_stage_11_sessions_canonical/aee_state_stream/aee_state_stream.csv",
        help="Frozen state stream CSV for selected trade population.",
    )
    ap.add_argument(
        "--state-stream-glob",
        action="append",
        default=[],
        help="Workspace-relative glob for additional state streams (repeatable).",
    )
    ap.add_argument(
        "--config",
        action="append",
        default=[],
        help="Config in NAME=PATH format (repeatable). If omitted, canonical config set is used.",
    )
    ap.add_argument(
        "--config-glob",
        action="append",
        default=[],
        help="Workspace-relative glob for config files (repeatable). Example: compiled_market_nodes/**/aee_rules/aee_rules.json",
    )
    ap.add_argument("--min-delta-pph", type=float, default=0.02, help="Minimum pips/hour margin above static to count as a meaningful stream win.")
    ap.add_argument("--min-stream-win-rate", type=float, default=0.6, help="Minimum fraction of streams that must pass win gate for promotion.")
    ap.add_argument("--min-stream-wins", type=int, default=2, help="Minimum number of stream wins required for promotion.")
    ap.add_argument("--max-hold-ratio", type=float, default=1.5, help="Maximum allowed hold-time inflation ratio vs static for stream win eligibility.")
    ap.add_argument("--min-source-node-diversity", type=int, default=2, help="Minimum unique source nodes among promoted configs.")
    ap.add_argument("--usd-per-pip", type=float, default=0.8, help="USD value per pip for realized_usd_per_hour.")
    ap.add_argument("--out", default="aee_historical_system_scoreboard.json", help="Output JSON path.")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    state_paths: list[Path] = []
    state_path = Path(args.state_stream)
    if not state_path.is_absolute():
        state_path = (root / state_path).resolve()
    state_paths.append(state_path)
    if args.state_stream_glob:
        state_paths.extend(_resolve_streams_from_globs(args.state_stream_glob, root))
    state_paths = _dedupe_paths(state_paths)
    missing_streams = [p for p in state_paths if not p.exists()]
    if missing_streams:
        raise SystemExit(f"state stream not found: {missing_streams[0]}")

    config_paths: list[tuple[str, Path]] = []
    if args.config:
        config_paths.extend(_parse_config_arg(args.config, root))
    if args.config_glob:
        config_paths.extend(_resolve_configs_from_globs(args.config_glob, root))
    if not config_paths:
        config_paths = _resolve_default_configs(root)
    config_paths = _dedupe_configs(config_paths)
    if not config_paths:
        raise SystemExit("no configuration files resolved")

    stream_results: list[dict[str, Any]] = []
    aggregate: dict[str, dict[str, Any]] = {}

    for stream_path in state_paths:
        rows = _load_state_rows(stream_path)
        if not rows:
            continue

        by_trade: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in rows:
            by_trade[str(row.get("trade_id", ""))].append(row)
        for t_rows in by_trade.values():
            t_rows.sort(key=lambda r: _safe_int(r.get("bar_index", 0), 0))

        duration_hr = _window_duration_hours(rows)
        static_outcomes = [_evaluate_static_trade(t_rows) for t_rows in by_trade.values() if t_rows]
        static_metrics = _compute_metrics(static_outcomes, duration_hr, float(args.usd_per_pip))
        static_result = {
            "name": "static_baseline",
            "config_path": None,
            "metrics": static_metrics,
            "rejected": False,
            "rejection_reasons": [],
        }

        per_stream_configs: list[dict[str, Any]] = []
        for name, cfg_path in config_paths:
            if not cfg_path.exists():
                continue
            cfg = _read_json(cfg_path)
            outcomes = [_evaluate_aee_trade(t_rows, cfg) for t_rows in by_trade.values() if t_rows]
            metrics = _compute_metrics(outcomes, duration_hr, float(args.usd_per_pip))
            result = {
                "name": name,
                "config_path": str(cfg_path),
                "metrics": metrics,
            }
            rejected, reasons = _apply_rejection_rules(result, static_result)
            delta_pph = metrics.get("realized_pips_per_hour", 0.0) - static_metrics.get("realized_pips_per_hour", 0.0)
            hold_ratio = (
                (metrics.get("avg_hold_sec", 0.0) / max(1.0, static_metrics.get("avg_hold_sec", 0.0)))
                if static_metrics.get("avg_hold_sec", 0.0) > 0
                else 1.0
            )
            stream_win = (
                not rejected
                and delta_pph >= float(args.min_delta_pph)
                and metrics.get("avg_realized_r", 0.0) > 0.0
                and hold_ratio <= float(args.max_hold_ratio)
            )
            result["rejected"] = rejected
            result["rejection_reasons"] = reasons
            result["stream_win"] = stream_win
            result["delta_vs_static"] = {
                "realized_pips_per_hour": delta_pph,
                "realized_usd_per_hour": metrics.get("realized_usd_per_hour", 0.0) - static_metrics.get("realized_usd_per_hour", 0.0),
                "avg_realized_r": metrics.get("avg_realized_r", 0.0) - static_metrics.get("avg_realized_r", 0.0),
                "avg_hold_sec": metrics.get("avg_hold_sec", 0.0) - static_metrics.get("avg_hold_sec", 0.0),
                "hold_ratio": hold_ratio,
                "green_roundtrip_loss_rate": metrics.get("green_roundtrip_loss_rate", 0.0) - static_metrics.get("green_roundtrip_loss_rate", 0.0),
                "sl_hit_rate": metrics.get("sl_hit_rate", 0.0) - static_metrics.get("sl_hit_rate", 0.0),
                "capital_recycling_rate": metrics.get("capital_recycling_rate", 0.0) - static_metrics.get("capital_recycling_rate", 0.0),
            }
            per_stream_configs.append(result)

            agg = aggregate.setdefault(
                str(cfg_path),
                {
                    "name": name,
                    "config_path": str(cfg_path),
                    "source_node": _source_node_from_config(name),
                    "stream_count": 0,
                    "stream_win_count": 0,
                    "metrics_sum": defaultdict(float),
                    "deltas_sum": defaultdict(float),
                    "rejections": Counter(),
                },
            )
            agg["stream_count"] += 1
            if stream_win:
                agg["stream_win_count"] += 1
            for k in ("realized_pips_per_hour", "realized_usd_per_hour", "avg_realized_r", "avg_hold_sec", "green_roundtrip_loss_rate", "sl_hit_rate", "capital_recycling_rate"):
                agg["metrics_sum"][k] += metrics.get(k, 0.0)
            for k in ("realized_pips_per_hour", "realized_usd_per_hour", "avg_realized_r", "avg_hold_sec", "hold_ratio"):
                agg["deltas_sum"][k] += result["delta_vs_static"].get(k, 0.0)
            for rr in reasons:
                agg["rejections"][rr] += 1

        ranked_stream = sorted(
            per_stream_configs,
            key=lambda r: (
                r["metrics"].get("realized_usd_per_hour", 0.0),
                r["metrics"].get("realized_pips_per_hour", 0.0),
                r["metrics"].get("avg_realized_r", 0.0),
            ),
            reverse=True,
        )
        stream_results.append(
            {
                "stream_path": str(stream_path),
                "stream_label": _stream_label(stream_path, root),
                "trade_count": len(by_trade),
                "window_duration_hr": duration_hr,
                "static_baseline": static_result,
                "ranked_configs": ranked_stream,
                "eligible_ranked_configs": [r for r in ranked_stream if r.get("stream_win", False)],
            }
        )

    if not stream_results:
        raise SystemExit("all state streams were empty")

    ranked_configs: list[dict[str, Any]] = []
    for _cfg_path, agg in aggregate.items():
        n = max(1, int(agg["stream_count"]))
        win_count = int(agg["stream_win_count"])
        win_rate = win_count / n
        row = {
            "name": agg["name"],
            "config_path": agg["config_path"],
            "source_node": agg["source_node"],
            "stream_count": n,
            "stream_win_count": win_count,
            "stream_win_rate": win_rate,
            "avg_metrics": {k: (agg["metrics_sum"][k] / n) for k in agg["metrics_sum"]},
            "avg_delta_vs_static": {k: (agg["deltas_sum"][k] / n) for k in agg["deltas_sum"]},
            "rejection_counts": dict(agg["rejections"]),
        }
        promoted = (
            win_count >= int(args.min_stream_wins)
            and win_rate >= float(args.min_stream_win_rate)
            and row["avg_metrics"].get("avg_realized_r", 0.0) > 0.0
            and row["avg_delta_vs_static"].get("realized_pips_per_hour", 0.0) >= float(args.min_delta_pph)
            and row["avg_delta_vs_static"].get("hold_ratio", 1.0) <= float(args.max_hold_ratio)
        )
        row["promoted"] = promoted
        ranked_configs.append(row)

    ranked_configs.sort(
        key=lambda r: (
            r["avg_metrics"].get("realized_usd_per_hour", 0.0),
            r["avg_metrics"].get("realized_pips_per_hour", 0.0),
            r.get("stream_win_rate", 0.0),
            r["avg_metrics"].get("avg_realized_r", 0.0),
        ),
        reverse=True,
    )

    promoted_ranked = [r for r in ranked_configs if r.get("promoted", False)]
    promoted_source_diversity = len({_source_node_from_config(r.get("name", "")) for r in promoted_ranked})
    diversity_gate_pass = promoted_source_diversity >= int(args.min_source_node_diversity)

    payload = {
        "generated_at": _iso_now(),
        "source_state_streams": [str(p) for p in state_paths],
        "stream_count": len(stream_results),
        "usd_per_pip": float(args.usd_per_pip),
        "gates": {
            "min_delta_pph": float(args.min_delta_pph),
            "min_stream_win_rate": float(args.min_stream_win_rate),
            "min_stream_wins": int(args.min_stream_wins),
            "max_hold_ratio": float(args.max_hold_ratio),
            "min_source_node_diversity": int(args.min_source_node_diversity),
        },
        "stream_results": stream_results,
        "ranked_configs": ranked_configs,
        "promoted_ranked_configs": promoted_ranked,
        "promotion_summary": {
            "promoted_count": len(promoted_ranked),
            "promoted_source_node_diversity": promoted_source_diversity,
            "source_node_diversity_gate_pass": diversity_gate_pass,
        },
    }

    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = (root / out_path).resolve()
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    print(json.dumps({
        "out": str(out_path),
        "stream_count": payload["stream_count"],
        "best_promoted": promoted_ranked[0]["name"] if promoted_ranked else None,
        "promoted_count": len(promoted_ranked),
        "total_configs": len(ranked_configs),
        "source_diversity_gate_pass": diversity_gate_pass,
    }, indent=2))


if __name__ == "__main__":
    main()
