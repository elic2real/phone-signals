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

