# AEE Live Doctrine Replay Compatibility Proof

## Scope
- Purpose: verify whether AEE doctrine classes are exercised from real trade-state snapshots.
- Input source: real log snapshots from `logs/trades.jsonl*` and `logs/archive/trades*.jsonl`.
- Replay path: `aee_live_wiring_proof.py` -> `LiveDoctrineEngine`.
- Out of scope: unrelated workspace artifacts (including `ceiling_*` files).

## Method
- Event kinds replayed: `MANUAL_TEACHER`, `TEACH_HEARTBEAT`, `MANUAL_CLOSE`.
- Rows scanned: `82,700`.
- Rows with usable trade-state snapshots: `965`.
- Coverage artifact: `aee_live_wiring_proof.json`.

## Doctrine Class Counts
- `HOLD`: `452`
- `PARTIAL`: `5`
- `CLOSE`: `508`
- `TIGHTEN`: `0`

Observed action set: `CLOSE, HOLD, PARTIAL`

## Example Snapshot For Each Observed Class

### HOLD (observed)
- source file: `logs/trades.jsonl.5`
- kind: `MANUAL_TEACHER`
- trade_id: `3917`
- ts: `2026-03-13T12:54:36.441222+00:00`
- mode: `HARVESTER`
- snapshot:
```json
{
  "pnl_atr": -4.137380191692908e-05,
  "mfe_atr": 0.0,
  "mae_atr": 0.6265198531864843,
  "energy_ratio": 41.57550997165067,
  "time_in_trade_sec": 22.209348678588867
}
```

### PARTIAL (observed)
- source file: `logs/trades.jsonl.5`
- kind: `MANUAL_TEACHER`
- trade_id: `3935`
- ts: `2026-03-13T12:59:17.524902+00:00`
- mode: `HARVESTER`
- snapshot:
```json
{
  "pnl_atr": -4.432892249530179e-05,
  "mfe_atr": 0.10931764644873665,
  "mae_atr": 1.2434882283533932,
  "energy_ratio": 7.744711196390122,
  "time_in_trade_sec": 176.93858218193054
}
```

### CLOSE (observed)
- source file: `logs/trades.jsonl.5`
- kind: `MANUAL_TEACHER`
- trade_id: `3883`
- ts: `2026-03-13T12:54:35.516645+00:00`
- mode: `HARVESTER`
- snapshot:
```json
{
  "pnl_atr": 4.106335727818733e-17,
  "mfe_atr": 0.1947519999999808,
  "mae_atr": 0.8248319999994973,
  "energy_ratio": 3.3159844545441764e-12,
  "time_in_trade_sec": 294.17776679992676
}
```

## TIGHTEN Coverage Gap
- Status: `not yet observed in available tape`.
- Count: `0`.
- Exact gap: no replayed snapshot in the scanned real-log set produced `action = TIGHTEN` under current doctrine conditions.

## Interpretation
- This report is a **live-log replay compatibility proof**: real trade-state snapshots successfully exercise doctrine logic and produce `HOLD`, `PARTIAL`, and `CLOSE`.
- This is **not** final live-bot execution proof (it validates compatibility by replay, not direct in-run emitted-action capture).

## Forced TIGHTEN Handoff Proof
- Artifact: `aee_tighten_handoff_proof.json`
- Script: `aee_tighten_handoff_proof.py`

### 1) Input snapshot
```json
{
  "trade_key": "forced-tighten-001",
  "mode": "RUNNER",
  "now_s": 44.0,
  "current_r": 0.03,
  "mfe_r": 0.20,
  "mae_r": -0.01,
  "energy": 0.34,
  "force_close": false
}
```

### 2) Doctrine engine receives it
- The snapshot above is passed directly into `LiveDoctrineEngine.update(...)` after a priming step that established meaningful prior green-state (`best_r = 0.20`).

### 3) Action returned
- Returned action: `TIGHTEN`

### 4) Output artifact saved
- Saved file: `aee_tighten_handoff_proof.json`

### 5) Pass/fail summary updated
- Expected: `TIGHTEN`
- Actual: `TIGHTEN`
- Forced case pass: `true`

## Final Class-Proof Summary
- `HOLD`: `PASS` (real-tape replay)
- `PARTIAL`: `PASS` (real-tape replay)
- `CLOSE`: `PASS` (real-tape replay)
- `TIGHTEN`: `PASS` (forced synthetic handoff proof)

All four doctrine classes are now proven:
- three from real tape (`HOLD`, `PARTIAL`, `CLOSE`)
- one from forced handoff proof (`TIGHTEN`)