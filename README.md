# AEE Strategy Optimization Status (Locked v1.0)

## Verified Performance (Physics-Compliant)
Tests run against **Locked v1.0 Golden Scenarios** with corrected pip physics.

| Scenario | Type | Pips/Hr | Notes |
| :--- | :--- | :--- | :--- |
| **Trend Continuation** | Trend | **1,744** | Full capture of 300-pip move (650s duration). |
| **Panic Reversal** | Reversal | **220** | Captures 82 pips on 1300s reversal event. Safe exit. |
| **Whipsaw Spike Fade** | Trap | **32** | Successfully fades the spike and captures reversion. |
| **Energy Depletion** | Exhaustion | **150** | Correctly identifies momentum loss. |

## Parameter Lockdown
The optimal parameters (High Yield + Safety) are now the defaults in `phone_bot.py`.
Reference configuration stored in `best_high_yield_config.json`.

## Simulation Infrastructure
- **Golden Artifacts:** `scenarios/golden/v1.0/` (SHA256 verified)
- **Validation:** `test_scenarios.py` ensures physics constraints (max 5000 pips/hr).
- **Audit:** `sim_extraction_audit.py` gates release quality.
- **Harness:** `sim_harness.py` supports unlocked TP (`--tp-atr 100`) for full run capture.

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

