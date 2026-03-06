# Phone Bot Current Performance Summary
==========================================

Generated: 2026-03-05 15:33:00 UTC
Source: sim_results/ (48 simulation runs)

## Overall Performance Metrics

### Key Metrics
- **Total Trades**: 48 (24 LONG, 24 SHORT)
- **Weighted Pips/Hour**: -4.56 (negative due to balanced LONG/SHORT)
- **Median Weighted Pips/Hour**: 17.32
- **Avg Pips/Trade**: -1.26
- **Std Dev Pips**: 98.98
- **Avg Hold Time**: 318.4 seconds (5.3 minutes)
- **Median Hold Time**: 300 seconds (5 minutes)
- **Avg Capture %**: 47.8%

### Leg Performance
- **Core Pips/Hour**: -4.72
- **Runner Pips/Hour**: -3.92
- **Salvage Rate**: 50%
- **Core Capture %**: 42.7%
- **Runner Capture %**: 38.3%

## Performance by Scenario

### Best Performing Scenarios (LONG)
1. **trend_continuation_LONG**: 1748.97 pips/hour
2. **high_energy_trend_LONG**: 292.10 pips/hour
3. **spread_widening_LONG**: 211.78 pips/hour
4. **slow_grind_break_LONG**: 158.64 pips/hour
5. **energy_depletion_LONG**: 152.17 pips/hour

### Best Performing Scenarios (SHORT)
1. **multi_whipsaw_sweep_SHORT**: 126.47 pips/hour
2. **random_walk_SHORT**: 120.37 pips/hour
3. **whipsaw_spike_fade_SHORT**: -81.37 pips/hour (least negative)
4. **energy_transition_SHORT**: 8.83 pips/hour
5. **universal_energy_morph_SHORT**: -27.73 pips/hour

### Worst Performing Scenarios
1. **trend_continuation_SHORT**: -1748.97 pips/hour
2. **high_energy_trend_SHORT**: -414.14 pips/hour
3. **spread_widening_SHORT**: -226.87 pips/hour
4. **slow_grind_break_SHORT**: -167.52 pips/hour
5. **energy_depletion_SHORT**: -156.73 pips/hour

## Exit Reasons Analysis

### Exit Reason Distribution
- **WHIPSAW_SPIKE_FADE_OVERSHOOT_CAPTURE**: 28 trades (58.3%)
- **PULSE_STALL_CAPTURE**: 8 trades (16.7%)
- **SIM_EOD_CLOSE**: 6 trades (12.5%)
- **PANIC_EXIT**: 4 trades (8.3%)
- **WHIPSAW_REJECTION_CAPTURE**: 2 trades (4.2%)

## System Health Metrics

### Sanity Gates Status
- ✅ Short hold <10s rate: 0%
- ✅ Median hold >=30s: 300s
- ✅ Short hold <30s rate: 0%
- ✅ Scenario wall time floor: All >=600s
- ✅ Active time ratio: 1.28%
- ❌ Exposure ratio: 0.32 (below 0.5 threshold)
- ❌ Scenario clip metrics: Some negative returns

## Key Observations

1. **Directional Bias**: The bot shows strong performance in LONG trends but struggles in SHORT scenarios
2. **Whipsaw Handling**: Most exits (58.3%) are from whipsaw spike fade captures
3. **Consistent Hold Times**: Most trades hold for exactly 5 minutes (300s)
4. **Capture Efficiency**: ~48% average capture shows room for improvement
5. **Low Exposure**: 32% exposure ratio suggests conservative entry criteria

## Comparison to Baseline

From `baseline_performance.json`:
- **Global Average**: 15,835.90 pips/hour (baseline) vs -4.56 pips/hour (current)
- Note: Baseline appears to be absolute values, current is net LONG/SHORT

## Recommendations

1. **Investigate SHORT Performance**: Significant underperformance in SHORT scenarios
2. **Optimize Whipsaw Logic**: 58% of exits from whipsaw suggests over-sensitivity
3. **Increase Exposure**: 32% exposure ratio is quite conservative
4. **Improve Capture**: 48% capture could be improved with better exit timing
5. **Consider Directional Filters**: Strong LONG bias suggests market regime detection

## Simulation Infrastructure

- **Simulation Harness**: `sim_harness.py`
- **Batch Runner**: `run_batch_sim.py`
- **Audit Tool**: `sim_extraction_audit.py`
- **Test Scenarios**: 14 different market scenarios
- **Output Directory**: `sim_results/` with detailed tick-by-tick data
