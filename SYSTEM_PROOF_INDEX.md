# SYSTEM PROOF INDEX

Canonical lock for current system proof status. Use this file as the source of truth before any further audit, refactor, or optimization work.

## 1. Doctrine Engine Proof

### Real Logic Fix
- File: `aee_live_doctrine.py`
- Change: green-state latch uses path best excursion.
- Locked logic: `went_green = ctx.best_r >= 0.10`
- Old issue: trades that went green then retraced were not latched as `went_green`.
- Result: real replay now emits `PARTIAL` correctly.

### Real Replay Coverage
- Artifacts:
- `aee_live_wiring_proof.py`
- `aee_live_wiring_proof.json`
- `aee_live_doctrine_proof_report.md`

Observed doctrine classes from real logs:
- `HOLD`: observed
- `PARTIAL`: observed
- `CLOSE`: observed
- `TIGHTEN`: not observed on real tape in this pass

## 2. Forced Doctrine Coverage

- Artifacts:
- `aee_tighten_handoff_proof.py`
- `aee_tighten_handoff_proof.json`

Locked forced result:
- `expected = TIGHTEN`
- `actual = TIGHTEN`
- `passed = true`

Meaning:
- Doctrine engine can produce `TIGHTEN`.
- Wiring path accepts and carries `TIGHTEN` action.

## 3. Forced Chain Compatibility

- Artifacts:
- `forced_full_chain_handoff_proof.py`
- `forced_full_chain_handoff_proof.json`
- `forced_full_chain_handoff_proof_report.md`

Stages verified:
- `entry_trigger`
- `candidate_creation`
- `priority_decision`
- `trade_open`
- `aee_action`
- `close_side_effect`

Locked result:
- `all_stages_passed = true`

Meaning:
- Full lifecycle plumbing is compatible end-to-end under controlled forced conditions.

## 4. Real Live Chain Capture

- Artifacts:
- `real_live_full_chain_capture_trade_3923.json`
- `real_live_full_chain_capture_report.md`

Captured real trade:
- `trade_id = 3923`
- `pair = GBP_USD`
- Source log: `logs/trades.jsonl.5`

Observed real chain:
- `SIGNAL_GENERATED`
- `ENTRY_GATE_EVAL` (`ALLOW`)
- `TRADE_ATTEMPT`
- `ENTRY_RESULT` (`FILLED`)
- `AEE_DECAY_EXIT`
- `EXIT_RESULT`
- `pair_close_complete`
- `HARVESTER_BANKED`

Meaning:
- Real production lifecycle evidence exists for one end-to-end trade path.

## 5. Code Changes

Modified code:
- `aee_live_doctrine.py`
- Logic correction: green-state latch now based on `best_r` threshold.

Not modified during proof run:
- `phone_bot.py`
- It was inspected only to map event emitters for:
- `SIGNAL_GENERATED`
- `ENTRY_GATE_EVAL`
- `TRADE_ATTEMPT`
- `ENTRY_RESULT`
- `AEE_DECAY_EXIT`
- `EXIT_RESULT`
- `HARVESTER_BANKED`
- `pair_close_complete`

## 6. Remaining Unknowns

Open optimization-phase questions:
- Throughput: does system reach target trade volume?
- Priority competition: are simultaneous signals ranked/executed correctly?
- AEE extraction quality: are path exits maximizing realized value?
- Opportunity capture: is realized capture moving toward opportunity ceiling?

## Locked Interpretation

This system is no longer in "does it work" validation phase.
It is in extraction and throughput optimization phase.

## Operational Split (Locked)

Two-layer model:

1. Working system (operational now)
- Detect entries
- Rank opportunities
- Open trades
- Manage with AEE
- Close trades
- Obey account constraints and execution physics

2. Mapping system (continuous optimizer)
- Improve node quality and enable/disable decisions
- Improve thresholds and ranking weights
- Improve extension probabilities and path extraction quality

Key distinction:
- Working system answers: "Can it trade correctly at all?"
- Mapping system answers: "Can it trade at the ceiling?"

Runtime policy while mapping is incomplete:
- Operate on baseline doctrine and current best-known thresholds
- Prefer harvester-first behavior
- Allow runner on stronger setups
- Keep proven entry, priority, and AEE wiring active
- Accept partial mapping truth and upgrade as better truth compiles

Upgrade loop:
- Current best logic -> live operation -> mapping refinement -> controlled upgrade

Interpretation:
- Mapping is not required for the bot to be alive.
- Mapping is required to push toward ceiling performance.

## Usage Rule

Before starting any new assistant session, reference this file first.
Do not re-open solved wiring/doctrine audit loops unless new contrary evidence appears.
