# Real Live Full-Chain Capture (Trade 3923)

## Verdict
PASS: One direct real trade path is linked end-to-end with timestamped evidence for all required nodes.

## Captured Trade
- pair: `GBP_USD`
- direction: `LONG`
- trade_id: `3923`
- broker_trade_id: `80187`
- source: `logs/trades.jsonl.5`

## Linked Node Chain
1. entry trigger
- `SIGNAL_GENERATED` with `reason_code=VOL_REIGNITE` and `priority_score=-0.0437`
- evidence: `logs/trades.jsonl.5:1`

2. candidate creation
- `STATE_PROMOTE_FROM_SIGNAL` moving `WATCH -> GET_READY`
- evidence: `logs/trades.jsonl.5:3`

3. priority decision
- `ENTRY_GATE_EVAL` with `decision=ALLOW`, `status=EXECUTE`
- evidence: `logs/trades.jsonl.5:17`

4. open path
- `TRADE_ATTEMPT` (`decision=PLACE`) then `ENTRY_RESULT` (`result=FILLED`, `trade_id=3923`)
- evidence: `logs/trades.jsonl.5:18`, `logs/trades.jsonl.5:22`
- managing transition: `logs/trades.jsonl.5:23`

5. AEE action
- `AEE_DECAY_EXIT` for `trade_id=3923`, `aee_reason=TIME_DECAY_PROFIT_CAPTURE`
- evidence: `logs/trades.jsonl.5:309`

6. close side effects
- close execution: `EXIT_RESULT` for `trade_id=3923`
- pair lifecycle close: `STATE_TRANSITION MANAGING -> WATCH` with `reason=pair_close_complete`
- aux close side effect: `HARVESTER_BANKED`
- evidence: `logs/trades.jsonl.5:311`, `logs/trades.jsonl.5:310`, `logs/trades.jsonl.5:313`

## Caveat
This proves direct observed live-chain linkage for this specific trade path. It is still a single-path proof, not universal proof of every runtime branch.
