TITLE
RCP_CODE_SPACES_MVP_AEE_WORKFLOW

WORKSPACE

- Environment: Codespaces
- Repo: phone-signals
- Branch: publish/clean-backup-20260326

PURPOSE

Codespaces is used for:

- clean architecture work
- AEE development
- replay / simulation
- repo-wide search and reuse
- structured testing

Codespaces is not used for:

- live bot runtime
- broker execution
- notification debugging

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

Build AEE correctly and fast.

SCOPE (ALLOWED WORK)

AEE CORE

- global kernel implementation
- scenario classification layer
- scenario playbooks
- stop/SL logic:
  - lock profit
  - trailing
  - protected squeeze
- harvester vs runner separation

AEE INFRASTRUCTURE

- trade-state packet standardization
- AEE replay harness
- baseline comparison engine
- decision logging + reason codes

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
3. Implement global kernel
4. Add scenario classification
5. Implement scenario playbooks
6. Add stop/SL logic
7. Build replay harness
8. Compare vs baselines

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
- Does this increase money?
- Or optimize a proxy?

5. Infrastructure
- Can this be tested in replay?

If any gate fails: STOP and redesign.

OUTPUT REQUIREMENTS

Every AEE change must:

- produce reason-coded decisions
- be testable in replay
- be comparable vs baseline
- be push-ready to GitHub

SUCCESS CONDITION

Codespaces work is successful when:

- AEE logic is modular
- replay harness exists
- kernel can be tested fast
- decisions are explainable
- improvements are measurable vs baseline

FINAL RULE

Codespaces builds the brain of the system.

Local runs the body of the system.

Do not mix them.