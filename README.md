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

## RCP execution protocol (worktree + audit gates)

RCP is mandatory for coding tasks to enforce bounded scope, verification, and resumable state.

### Workspace and branch policy

- **Active coding workspace**: `..\phone-signals-publish-clean`
- **Operational branch**: `publish/clean-backup-20260326`
- **Hard rule**: do not perform new coding work in the old blocked tree; use it only for artifact recovery.

### Mandatory Git flow (each task block)

1. `git pull origin publish/clean-backup-20260326`
2. Complete only the scoped task.
3. `git add .`
4. `git commit -m "<task_id>: <what changed>"`
5. `git push origin publish/clean-backup-20260326`

Rules:
- Never push directly to `main`.
- Pull before push.
- Commit/push at end of each completed task block.
- If push fails, resolve sync/conflicts before continuing implementation.

### Mandatory audit gates (no code before pass)

Before writing code, the assistant must produce and pass these checks:

1. **Task classification**
   - Classify: Implementation, Architectural, Infrastructure, Data Contract, Objective,
     Logic, Abstraction, Kernel, Scenario, Economic, Interaction, Process.
   - Output: `AUDIT_CLASSIFICATION` with Primary, Secondary, Risk Areas.

2. **Abstraction check**
   - Confirm implementation level is correct.
   - If solving symptom instead of structure: **STOP and redesign**.

3. **Kernel check**
   - Confirm required global rule/kernel exists and is proven.
   - If missing: **STOP and define kernel first**.

4. **Scenario check**
   - Confirm scenario-specific vs universal logic is correct.
   - Create/extend scenario classification if needed.

5. **Objective check**
   - Confirm direct objective alignment (not proxy optimization without justification).

6. **Interaction check**
   - Evaluate conflicts with entry, AEE, stop, and runner logic.
   - Resolve conflicts before implementation.

7. **Infrastructure check**
   - Confirm isolated testability, replayability, baseline availability.
   - If not testable: redesign.

8. **Economic reality check**
   - Validate spread, slippage, latency, and broker constraints.

9. **Go / no-go decision**
   - Output `GO_DECISION` (`APPROVED` or `REJECTED`) with explicit reason.

10. **Post-implementation audit**
    - Verify objective alignment, abstraction integrity, interaction safety, and usable outputs/logs.

### Required RCP artifacts per phase

- `control/preflight_report.json` (intent + scope + validation plan)
- `control/validation_result.json` (commands run + pass/fail + evidence)
- `control/adjudication.json` (promote/hold/reject + next task + forbidden next actions)

### Stop conditions

Stop and report immediately when any of the following occurs:
- Kernel missing for the requested logic.
- Required audit gate fails.
- Required proof cannot be produced.
- Remote sync/push cannot be completed.

## RCP_LOCAL_VS_CODE — execution split

### Workspaces

**LOCAL**
- Environment: your PC / Termux / live runtime
- Folder: `phone-signals-publish-clean`
- Branch: `publish/clean-backup-20260326`

**CODESPACES**
- Environment: GitHub Codespaces
- Same repo + branch
- Clean, reproducible environment

### Core principle

- LOCAL = RUN + OBSERVE
- CODESPACES = BUILD + PROVE

### Local responsibilities (non-negotiable)

1. **Live bot runtime**
   - start/stop bot
   - monitor uptime
   - ensure loop stability

2. **Execution layer**
   - broker / webhook / API behavior
   - order placement validation
   - SL/TP execution correctness

3. **Stop / SL real behavior**
   - verify SL moves correctly
   - verify trailing behaves correctly
   - verify locked profit behaves as expected

4. **Notifications**
   - Must show: entry trigger, strategy ID, trade ID (harvester / runner), AEE decision,
     SL movement, close reason

5. **Demo operation**
   - run demo account
   - ensure trades execute, AEE manages live trades, and system does not crash

6. **Final validation**
   - before promotion, verify live behavior matches expected AEE logic

### Codespaces responsibilities (non-negotiable)

1. **AEE development**
   - global kernel
   - scenario classification
   - scenario playbooks
   - stop/lock/trailing logic

2. **Replay / simulation**
   - build and run replay harness
   - test AEE on fixed trade paths
   - compare vs baseline

3. **Architecture work**
   - modularize AEE
   - enforce separate trade IDs (harvester/runner), clean interfaces, reusable components

4. **Repo discovery**
   - search for existing AEE code, replay tools, logging systems
   - reuse before building new

5. **Reporting**
   - output performance vs baseline, scenario behavior, and decision logs

### Shared rules

**Git flow**

At start:

```bash
git checkout publish/clean-backup-20260326
git pull origin publish/clean-backup-20260326
```

At end:

```bash
git add .
git commit -m "<task_id>: <what changed>"
git push origin publish/clean-backup-20260326
```

**Sync rule**
- GitHub is the single source of truth.
- Always pull before work.
- Always push after task completion.

### What must not happen

**Do not do in LOCAL**
- heavy replay simulations
- large batch testing
- architecture refactors

**Do not do in CODESPACES**
- live bot execution
- broker debugging
- notification tuning

### Workflow loop

1. Codespaces: build or modify AEE / architecture
2. Push to GitHub
3. Local: pull changes, run bot, observe behavior
4. Push findings / fixes
5. Repeat

### Decision rule

If task requires:
- real-time behavior → LOCAL
- structural change → CODESPACES
- heavy testing → CODESPACES

### Success condition

System is correct when:
- AEE logic is built and tested in Codespaces
- behavior matches expectation in Local
- both environments stay in sync via GitHub

### Final rule

Never build blind:
- Codespaces proves logic
- Local proves reality
- both are required

