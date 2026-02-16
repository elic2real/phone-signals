# phone-signals

Repository includes the production bot plus a **no-touch simulation harness** for AEE replay validation.

- `phone_bot.py` - production runtime and strategy logic (left unchanged for sim runs)
- `phone_bot_logging.py` - logging helpers
- `sim_harness.py` - OANDA-like replay harness with runtime wire swap only
- `tick_generator.py` - synthetic tick scenario generator + scenario registry/mix sampler
- `aee_validator.py` - post-run validation report generator
- `quick_start.py` - one-command synthetic test flow

## Run production bot

```bash
python3 phone_bot.py
```

## AEE simulator harness (no production code edits)

`sim_harness.py` injects:
- pricing/candle responses via a simulated OANDA client
- tick-by-tick bid/ask updates
- runtime log capture for AEE decisions

It calls `phone_bot._aee_eval_for_trade(...)` directly with real `phone_bot` utilities
(`to_pips`, `pip_size`, state machine, AEE phase/exit logic), while restoring original
wiring when finished.

The simulator emits explicit `AEE_EVAL_TRACE` telemetry per evaluation with:
- `idx` (global merged tick index), `pair_eval_idx` (pair-local decision index), `ts`, `instrument`
- full `metrics` snapshot
- `aee_true` (all true conditions in priority order)
- `aee_chosen` (actual selected exit)

This makes cadence, priority, trigger-tick, and left-on-table validation auditable.

### Tick CSV format

Required columns:

- `instrument`
- `ts` (float epoch seconds or ISO timestamp)
- `bid`
- `ask`

### Example

```bash
python3 sim_harness.py \
  --ticks scenario_runner.csv \
  --pair EUR_USD \
  --direction LONG \
  --atr-pips 10 \
  --tp-atr 1.2 \
  --bucket-sec 5 \
  --out sim_results.json
```

## Quick synthetic validation

```bash
python3 quick_start.py test
```

Outputs:
- `sim_results.json`
- `aee_validation_report.json`


## Uncapped batch runs (recommended for robust stats)

Use `quick_start.py batch` with any run count (not limited to 50):

```bash
python3 quick_start.py batch \
  --runs 500 \
  --output-dir sim_outputs_500 \
  --audit-file extraction_audit_report_500.json
```

This executes the requested number of replay runs and then aggregates via
`sim_extraction_audit.py` into one report.

Batch runs now sample a scenario family each run from the registry:
- `trend_continuation`
- `chop_mean_reversion`
- `panic_reversal`
- `spread_widening`
- `whipsaw_spike_fade`
- `slow_grind_break`

The output JSON includes an `AEE_SCORECARD` block and `scenario_breakdown` table
so results are judged by a locked metrics contract (weighted pips/hour, median weighted
pips/hour, clip metrics, tail loss, hold distribution, and exit mix by reason/leg).

## Optimizer (distribution-first + stability gate)

```bash
python3 aee_optimizer.py --trials 12 --runs-per-trial 50 --seed 123 --output optimizer_report.json
```

The optimizer now:
- evaluates each candidate across run distributions (25/100/300),
- ranks by distribution-aware objectives (medians + risk/clip penalties),
- enforces structural constraints against baseline,
- emits Pareto front + full archive,
- performs 500-run stability checks for survivors,
- outputs `stable_candidates.json`, baseline-vs-best delta tables, and session-6 best/worst/failure-atlas artifacts,
- and only sets `best` when `stable_candidates` is non-empty.
