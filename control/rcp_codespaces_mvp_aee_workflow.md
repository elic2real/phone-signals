TITLE
RCP_CODE_SPACES_MVP_AEE_WORKFLOW

WORKSPACE

- Environment: Codespaces
- Repo: phone-signals
- Branch: publish/clean-backup-20260326

PURPOSE

Codespaces is the AEE research + kernel discovery engine.

Codespaces is used for:

- AEE development
- replay harness
- batch experiments
- kernel discovery (expand/refine/combine)
- baseline comparison
- report generation

Codespaces is not used for:

- live bot runtime
- broker execution
- notifications
- runtime debugging

CORE PRINCIPLE

"Codespaces = discover truth at scale"

GIT FLOW (MANDATORY)

Start of session:

```bash
git checkout publish/clean-backup-20260326
git pull origin publish/clean-backup-20260326
```

End of task block:

```bash
git add .
git commit -m "<task_id>: <what changed>"
git push origin publish/clean-backup-20260326
```

RULES

- NEVER work on main
- ALWAYS pull before coding
- ALWAYS push after completing a task block
- Codespaces is the clean source of truth for structure

PRIMARY OBJECTIVE

"Find the first kernel (or kernel combination) that beats baseline with proof."

OPERATING MODE: AUTONOMOUS DISCOVERY LOOP

LOOP

1. Generate kernel candidates
2. Run batch experiments
3. Produce report
4. Rank results
5. Repeat

STOP CONDITIONS

Stop ONLY if:

1. Win condition
  - beats 1:1 baseline, protective baseline, and current AEE
  - no major regression
2. Plateau
  - no improvement after N iterations
3. Structural issue
  - errors / invalid attribution
4. Scope violation
  - touches entry or runtime systems

CURRENT FOCUS

- replay harness adapter
- baseline comparison output
- deterministic test spine
- batch kernel testing
- scenario evaluation
- identify first winning kernel

SCOPE (ALLOWED WORK)

AEE CORE

- global kernel candidates
- scenario classification layer
- scenario playbooks
- stop/SL logic:
  - lock profit
  - trailing
  - protected squeeze

KERNEL DISCOVERY

Codespaces owns kernel discovery, including expansion of kernel definitions and controlled combination of kernels, provided experiments remain attributable and comparable against baseline.

Allowed discovery work:

- pure kernel discovery
- kernel definition refinement
- controlled composite discovery

Examples of allowed kernel dimensions:

- time
- pnl
- progress
- state-transition
- path-quality
- opportunity-cost

Examples of allowed composite discovery:

- time + pnl
- progress + pnl
- time + state-transition
- pnl + opportunity-cost

The hard rule in discovery:

- do not lose attribution

AEE INFRASTRUCTURE

- trade-state packet standardization
- AEE replay harness
- baseline comparison engine
- decision logging + reason codes

BATCH EXPERIMENT ENGINE

Every experiment must preserve attribution and emit:

- experiment_id
- kernel_id
- kernel_type
- components
- component_definitions
- parameter_set_id
- parameter_set
- total_delta_vs_baseline
- total_delta_vs_current
- per-scenario deltas
- reason-code breakdown
- transition breakdown
- regressions

BASELINE REQUIREMENTS

Every run must compare:

- current AEE
- static 1:1 baseline
- simple protective baseline

ENTRY SUPPORT (LIMITED)

- maintain working strategies only
- ensure:
  - strategy tagging
  - clean inputs to AEE

NO:

- deep entry redesign
- heavy optimization
- mapping systems

REPO DISCOVERY / REUSE

- search for existing AEE logic
- search for existing replay tools
- search for existing logging systems
- search for existing strategy components
- reuse before building new

OUT OF SCOPE

Do not do in Codespaces:

- live execution testing
- broker/webhook debugging
- notification system tuning
- device-specific behavior

DEVELOPMENT ORDER

1. Search repo for reusable components
2. Define/lock trade-state packet
3. Ensure replay spine is stable
4. Add batch experiment runner
5. Run many candidates
6. Rank results
7. Identify best-performing kernel

AUDIT GATES (MANDATORY)

Before coding, verify:

1. Abstraction
- Is this the correct layer?
- Is a higher-level concept missing?

2. Kernel
- Is this global logic or local?
- Is kernel defined first?

3. Scenario
- Is this universal or scenario-specific?

4. Objective
- Does this improve money vs baseline?

5. Attribution
- Can results be traced to this change?

If any gate fails: STOP and redesign.

OUTPUT REQUIREMENTS

Every AEE change must:

- produce reason-coded decisions
- be testable in replay
- be comparable vs baseline
- be push-ready to GitHub

Per trade reporting must include:

- trade_id
- final result
- baseline result
- delta
- reason_code
- state transition
- giveback
- time in trade
- locked profit

Aggregate reporting must include:

- total delta
- average delta
- win/loss counts

Scenario reporting must include:

- per-scenario delta
- reason concentration
- transition concentration

HARD RULES

- Do NOT test only one idea
- Do test many candidates
- Do NOT lose attribution
- Do use fixed replay slice
- Do compare every run to baseline

SUCCESS CONDITION

Codespaces work is successful when:

"A ranked set of kernel candidates that outperform baseline with attribution and no major regression"

FINAL RULE

"Codespaces discovers truth. Do not interrupt the loop unless a stop condition is hit."