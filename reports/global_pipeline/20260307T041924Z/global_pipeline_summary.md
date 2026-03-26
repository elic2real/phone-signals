# Global Training Pipeline Summary
- run_id: `20260307T041924Z`
- session: `LONDON`
- pairs: `EUR_USD USD_JPY AUD_JPY`
- stats_path: `stats/session_LONDON.json`

## Entry Baseline
- version: `entry-global-20260307T042026Z`
- score: `706.4121607755524`
- score_delta_vs_prev: `0.0`
- synthetic_pph_mean: `681.249337442219`
- synthetic_pips_mean: `122.81411666666672`
- synthetic_tail_loss_rate: `0.0`
- friction_severity_mult: `0.8`
- knobs:
  - `entry.tick.base_max_dist_atr`: `0.46`
  - `entry.tick.confirm_disp_atr`: `0.28`
  - `entry.tick.confirm_m1_closes`: `1`
  - `entry.tick.confirm_sec`: `0.0`
  - `entry.tick.pullback_atr_min`: `0.3`
  - `entry.tick.reclaim_tolerance_atr`: `0.08`
  - `entry.tick.require_pullback`: `True`
  - `entry.tick.require_reclaim`: `False`

## AEE Baseline
- version: `aee-global-20260307T042131Z`
- score: `740.8149781746954`
- score_delta_vs_prev: `0.0`
- pph_mean: `745.4746687211106`
- pips_mean: `134.3925166666669`
- capture_mean: `0.8191111393458614`
- giveback_mean: `23.960208333332904`
- dead_hold_rate: `0.0`
- tail_loss_rate: `0.0`
- knobs:
  - `aee.fail_windows`: `3`
  - `aee.near_tp_band_atr`: `0.2`
  - `aee.strictness_mult`: `1.2`

## Artifacts
- entry leaderboard: `reports/global_pipeline/20260307T041924Z/entry/entry_global_leaderboard.json`
- aee leaderboard: `reports/global_pipeline/20260307T041924Z/aee/aee_global_leaderboard.json`
- entry stdout: `reports/global_pipeline/20260307T041924Z/entry/train_entry_stdout.json`
- aee stdout: `reports/global_pipeline/20260307T041924Z/aee/train_aee_stdout.json`
