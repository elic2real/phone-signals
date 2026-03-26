#!/usr/bin/env python3
from __future__ import annotations

import json
from itertools import product
from pathlib import Path
from statistics import mean, median
from typing import Any

ROOT = Path('.')
UNIFIED_PATH = ROOT / 'entry_metric_ceiling_report_unified.json'


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding='utf-8'))


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2), encoding='utf-8')


def strategy_rows(unified: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    results = (unified.get('results') or {})
    for side, modes in results.items():
        for mode, dists in (modes or {}).items():
            for dist_key, payload in (dists or {}).items():
                pc = (payload or {}).get('profit_ceiling', {})
                rows = list(pc.get('rows') or [])
                if not rows:
                    continue
                try:
                    distance = float(dist_key)
                except Exception:
                    continue
                out.append(
                    {
                        'strategy_key': f"{side}:{mode}:{dist_key}",
                        'side': str(side).upper(),
                        'mode': str(mode).upper(),
                        'distance': distance,
                        'rows': rows,
                        'baseline_avg_R': float(pc.get('avg_R', 0.0) or 0.0),
                        'trade_count': int(pc.get('trade_count', len(rows)) or len(rows)),
                        'baseline_total_pips': float(pc.get('total_pips', 0.0) or 0.0),
                        'baseline_win_rate': float(pc.get('win_rate', 0.0) or 0.0),
                    }
                )
    return out


def baseline_r(row: dict[str, Any], distance: float) -> float:
    if isinstance(row.get('r'), (int, float)):
        return float(row.get('r'))
    pips = float(row.get('pips', 0.0) or 0.0)
    return pips / distance if distance > 0 else 0.0


def replay_r(row: dict[str, Any], distance: float, harvest_mult: float, giveback_mult: float, panic_mult: float) -> float:
    path = row.get('price_path') or []
    direction = str(row.get('direction', '')).upper()
    start = float(row.get('price_start', 0.0) or 0.0)

    if not path or not isinstance(path, list) or distance <= 0.0:
        return baseline_r(row, distance)

    pip = 0.0001 if 'JPY' not in str(row.get('cluster_id', '')) else 0.01

    def pnl(px: float) -> float:
        if direction == 'LONG':
            return (float(px) - start) / pip
        return (start - float(px)) / pip

    harvest_trigger = distance * harvest_mult
    giveback_trigger = max(0.4, distance * giveback_mult)
    panic_trigger = -(distance * panic_mult)

    peak = 0.0
    exit_pips = pnl(float(path[-1]))
    for raw in path[1:]:
        cur = pnl(float(raw))
        peak = max(peak, cur)
        if peak >= harvest_trigger and (peak - cur) >= giveback_trigger:
            exit_pips = cur
            break
        if cur <= panic_trigger:
            exit_pips = cur
            break
    return exit_pips / distance


def grids(mode: str) -> tuple[list[float], list[float], list[float]]:
    if mode == 'RUNNER':
        return (
            [0.9, 1.0, 1.1, 1.2, 1.3],
            [0.20, 0.28, 0.36, 0.45],
            [1.0, 1.2, 1.4, 1.6],
        )
    return (
        [0.7, 0.8, 0.9, 1.0, 1.1],
        [0.12, 0.18, 0.24, 0.32, 0.40],
        [0.8, 1.0, 1.2, 1.4],
    )


def optimize_one(s: dict[str, Any]) -> dict[str, Any]:
    rows = s['rows']
    distance = float(s['distance'])
    mode = s['mode']
    baseline = [baseline_r(r, distance) for r in rows]
    base_avg = mean(baseline) if baseline else 0.0

    h_grid, g_grid, p_grid = grids(mode)
    best: dict[str, Any] | None = None

    for h, g, p in product(h_grid, g_grid, p_grid):
        rs = [replay_r(r, distance, h, g, p) for r in rows]
        avg_r = mean(rs) if rs else 0.0
        total_r = sum(rs)
        wins = sum(1 for x in rs if x > 0)
        losses = sum(1 for x in rs if x < 0)
        cand = {
            'harvest_trigger_pips': distance * h,
            'giveback_trigger_pips': max(0.4, distance * g),
            'panic_trigger_pips': -(distance * p),
            'harvest_mult': h,
            'giveback_mult': g,
            'panic_mult': p,
            'optimized_avg_R': avg_r,
            'delta_R': avg_r - base_avg,
            'optimized_total_R': total_r,
            'optimized_win_rate': (wins / len(rs)) if rs else 0.0,
            'optimized_loss_rate': (losses / len(rs)) if rs else 0.0,
            'optimized_R_p50': median(rs) if rs else 0.0,
        }
        if best is None:
            best = cand
            continue
        # Primary objective: improve expectancy; tie-break on total and median R.
        if (
            cand['delta_R'] > best['delta_R']
            or (
                cand['delta_R'] == best['delta_R']
                and (
                    cand['optimized_total_R'] > best['optimized_total_R']
                    or (
                        cand['optimized_total_R'] == best['optimized_total_R']
                        and cand['optimized_R_p50'] > best['optimized_R_p50']
                    )
                )
            )
        ):
            best = cand

    assert best is not None
    sample = int(s['trade_count'])
    confidence = 'high' if sample >= 50 else ('medium' if sample >= 15 else 'low')
    if best['delta_R'] > 0.08:
        recommendation = 'keep_expand'
    elif best['delta_R'] > 0.0:
        recommendation = 'keep_tune'
    elif sample < 15:
        recommendation = 'inconclusive_more_sample'
    else:
        recommendation = 'suppress'

    return {
        'strategy_key': s['strategy_key'],
        'side': s['side'],
        'mode': s['mode'],
        'distance': distance,
        'trade_count': sample,
        'baseline_avg_R': base_avg,
        'baseline_total_pips': float(s['baseline_total_pips']),
        'baseline_win_rate': float(s['baseline_win_rate']),
        'best': best,
        'confidence': confidence,
        'recommendation': recommendation,
    }


def main() -> None:
    unified = load_json(UNIFIED_PATH)
    slices = strategy_rows(unified)
    optimized = [optimize_one(s) for s in slices]

    optimized.sort(key=lambda x: (x['best']['delta_R'], x['best']['optimized_total_R']), reverse=True)

    report = {
        'goal': 'material per-strategy optimization only',
        'source': str(UNIFIED_PATH.name),
        'strategy_count': len(optimized),
        'optimized_strategies': optimized,
    }
    write_json(ROOT / 'strategy_optimization_report.json', report)

    runtime = {
        'version': '2026-03-17-opt-only',
        'source': 'strategy_optimization_report.json',
        'strategy_overrides': {
            r['strategy_key']: {
                'harvest_trigger_pips': round(float(r['best']['harvest_trigger_pips']), 5),
                'giveback_trigger_pips': round(float(r['best']['giveback_trigger_pips']), 5),
                'panic_trigger_pips': round(float(r['best']['panic_trigger_pips']), 5),
                'panic_mode': 'dynamic',
            }
            for r in optimized
        },
        'recommendations': {
            r['strategy_key']: {
                'recommendation': r['recommendation'],
                'confidence': r['confidence'],
                'delta_R': round(float(r['best']['delta_R']), 6),
            }
            for r in optimized
        },
    }
    write_json(ROOT / 'strategy_runtime_overrides.json', runtime)

    # Compact execution sheet for quick tuning decisions.
    lines = []
    lines.append('# Strategy Optimization Execution')
    lines.append('')
    lines.append('Optimization-only output (no ranking dependency).')
    lines.append('')
    lines.append('| Strategy | Mode | Trades | Delta R | Recommendation | Harvest | Giveback | Panic |')
    lines.append('|---|---|---:|---:|---|---:|---:|---:|')
    for r in optimized:
        b = r['best']
        lines.append(
            f"| `{r['strategy_key']}` | {r['mode']} | {r['trade_count']} | {float(b['delta_R']):.6f} | {r['recommendation']} | {float(b['harvest_trigger_pips']):.5f} | {float(b['giveback_trigger_pips']):.5f} | {float(b['panic_trigger_pips']):.5f} |"
        )
    (ROOT / 'strategy_optimization_execution.md').write_text('\n'.join(lines) + '\n', encoding='utf-8')

    print('wrote strategy_optimization_report.json')
    print('wrote strategy_runtime_overrides.json')
    print('wrote strategy_optimization_execution.md')
    print(f'optimized_strategies={len(optimized)}')


if __name__ == '__main__':
    main()
