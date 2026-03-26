# Global Training Pipeline Summary
- run_id: `20260307T033733Z`
- session: `LONDON`
- pairs: `EUR_USD USD_JPY`
- stats_path: `stats/session_LONDON.json`

## Entry Baseline
- version: `entry-global-20260307T033737Z`
- score: `494.8800959322066`
- synthetic_pph_mean: `477.5613559322064`
- synthetic_pips_mean: `86.09370000000055`
- synthetic_tail_loss_rate: `0.0`
- friction_severity_mult: `1.0`
- knobs:
  - `entry.tick.base_max_dist_atr`: `0.22`
  - `entry.tick.confirm_disp_atr`: `0.2`
  - `entry.tick.confirm_m1_closes`: `2`
  - `entry.tick.confirm_sec`: `6.0`
  - `entry.tick.pullback_atr_min`: `0.4`
  - `entry.tick.reclaim_tolerance_atr`: `0.08`
  - `entry.tick.require_pullback`: `True`
  - `entry.tick.require_reclaim`: `True`

## AEE Baseline
- version: `aee-global-20260307T033742Z`
- score: `507.89944828706126`
- pph_mean: `536.1925423728858`
- pips_mean: `96.66360000000081`
- capture_mean: `0.8008842957087177`
- giveback_mean: `31.821749999999476`
- dead_hold_rate: `0.0`
- tail_loss_rate: `0.0`
- knobs:
  - `aee.fail_windows`: `4`
  - `aee.near_tp_band_atr`: `0.2`
  - `aee.strictness_mult`: `1.0`

## Artifacts
- entry leaderboard: `reports/global_pipeline_smoke/20260307T033733Z/entry/entry_global_leaderboard.json`
- aee leaderboard: `reports/global_pipeline_smoke/20260307T033733Z/aee/aee_global_leaderboard.json`
- entry stdout: `reports/global_pipeline_smoke/20260307T033733Z/entry/train_entry_stdout.json`
- aee stdout: `reports/global_pipeline_smoke/20260307T033733Z/aee/train_aee_stdout.json`
