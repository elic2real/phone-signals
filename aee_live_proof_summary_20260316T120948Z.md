# AEE Live Proof Summary (20260316T120948Z)

## Headline
- AEE is actively evaluating fresh fills and firing doctrine exits in live flow.
- Close path is causal with request/response evidence and zero 404 in this window.
- One fresh fill had no first eval, but it was sibling-coalesced after same-side flatten (`BROKER_ALREADY_CLOSED_BEFORE_AEE`), not reconciliation stealing ownership.

## Counters
- doctrine_exit_reasons_fired_live: 3
- doctrine_exit_reasons_breakdown: {'PANIC_EXIT': 2, 'NEVER_GREEN_FAST_EXIT': 1}
- exit_attempts: 2
- exit_responses: 2
- close_404_count: 0
- broker_stop_loss_confirmed: 0
- fresh_fills: 12
- fresh_fills_with_no_first_eval: 1
- aee_first_eval_missing_events: 0
- same_side_close_collisions: 0
- close_coalesced_sibling_satisfied: 3

## Interpretation
- Doctrine exits observed: `PANIC_EXIT` and `NEVER_GREEN_FAST_EXIT` fired live.
- No close 404s in this window, and coalescing evidence is present (`PAIR_CLOSE_COALESCED`).
- Remaining gap to monitor: sibling leg closures can complete before that leg receives first periodic eval. This is coordinated behavior, but still counts against strict "every fill first eval" if interpreted literally.

## Artifacts
- `aee_live_proof_report_20260316T120948Z.txt`
- `aee_live_proof_window_20260316T120948Z.log`
