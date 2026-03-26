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

## Environment setup

1. Install Python **3.11** (matches CI toolchain) plus system `gcc`/`make` for NumPy/SciPy wheels.
2. Create a virtual environment and install pinned dependencies:
   ```bash
   python3.11 -m venv .venv
   source .venv/bin/activate
   python -m pip install --upgrade pip
   pip install -r requirements.txt
   ```
3. (Optional) populate `.env` with broker/OANDA credentials. Gates that require `python-dotenv` will fall back to process env vars if `.env` is absent.

## Run MVP locally/Termux

```bash
PYTHONUNBUFFERED=1 timeout 25s python3 phone_bot.py
```

For deterministic harness runs, ensure the expected compiled artifacts exist (e.g., `compiled_session_templates/<pair>__<session>/session_template_report.json`) or run the corresponding `build_*` scripts to materialize them.

## Artifact & data expectations

- **Calibration / session templates** – generated via the `build_*` pipeline into `compiled_*` directories. Validate freshness with the reports that live alongside the artifacts.
- **Proof & replay assets** – massive (>80 MB) JSON blobs are under `proof_artifacts/` together with historical `.venv` snapshots that power cold replay audits.
- **Data tapes** – canonical tick/history bundles live under `data_tape*/` and are addressed by path in build scripts; do not edit them manually.
- **Logs / runs** – transient run outputs live in `logs/` and `runs/`. These are safe to delete when not debugging.

You can reproduce artifacts via `quick_start.py` or the scripted compilers, but expect multi-hour runtimes and 100GB+ disk usage unless you trim the target set.

## Large artifact management

The repository currently checks in heavyweight trees such as `proof_artifacts/` (~105 GB), `compiled_market_nodes/` (~65 GB), and `compiled_session_templates/` (~11 GB). Consider:

1. **Publishing release bundles** – move immutable proof artifacts and compiled nodes into versioned archives (e.g., S3/GCS) and fetch them on demand via a `tools/fetch_artifacts.py` helper.
2. **Git-LFS or git submodules** – if artifacts must remain in-repo, store them via LFS to avoid bloating clones.
3. **Pruning generated outputs** – add targeted `.gitignore` rules for `runs/`, `logs/`, and other per-run outputs once reproducible build scripts exist.

Document any relocation plan in `LOCKED_CEILING_PROCESS.md` or a new `ARTIFACT_MANIFEST.md` so engineers know where to retrieve required bundles.

## CHANGELOG (2026-02-23)

- Root cause: unresolved merge-conflict artifacts in `phone_bot.py` introduced invalid syntax (`>>>>>>> ...`) and duplicate/dead pasted code blocks.
- Fixed: removed conflict artifacts, restored valid Python flow, and removed unreachable duplicated dashboard code after a return path.
- Hardened: restored single sizing entry path via `compute_units_recycling` compatibility wrapper to canonical geometric sizing, and fixed runtime-risk undefined symbol paths.
- Guardrails: added `.pre-commit-config.yaml` and `.github/workflows/python-hygiene.yml` to block conflict markers and enforce repo-wide `py_compile` in local hooks and CI.
- Proof commands used:
	- `python3 -m py_compile phone_bot.py`
	- repo-wide compile loop using `py_compile`
	- bounded startup check: `PYTHONUNBUFFERED=1 timeout 25s python3 phone_bot.py`

