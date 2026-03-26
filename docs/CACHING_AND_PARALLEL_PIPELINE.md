# Caching + Parallel Compilation Workflow

This document describes the guardrails for deterministic caching and node-level parallelism.

## Session cache

- Shared helper lives in `session_cache.py` (built on `cache_key_utils.py`).
- Strict cache key = [`session_cache_v1`, `data_root`, `script_hash`, extra components].
- Set `SESSION_CACHE_DIR=/path/to/cache` to control location.
- Set `DISABLE_SESSION_CACHE=1` to bypass.
- `build_session_state_stream.py` uses this cache via `--session-cache-dir`.
- Cache entries are gzipped JSON slices per `session_id`, with manifests + directory signatures.
- `cache_audit.jsonl` logs hits/misses; treat the audit as part of deterministic proof.
- Never edit cache entries manually; delete directories to force recompute.

## Compile cache (up next)

- Pattern: wrap stage1–6 substrate outputs under `compiled_cache/<hash>/node_key`.
- Key components: script hashes, dataset lock hash, config hash, node scope.
- Cache should only trigger when **all** inputs are identical; otherwise miss.
- Add telemetry to `cache_audit.jsonl` for hits/misses per node compile.
- Safe target: stage1.5 deterministic substrate + seed/stream/context seeds (pure functions).

## Synthetic scenario cache

- Applicable to harness/tick generation flows (e.g., `quick_start.py`).
- Key components: `scenario`, `seed`, tick generator script hash.
- Output: CSV of generated ticks + corresponding metadata.
- Guard with env var `DISABLE_SCENARIO_CACHE=1` similar to session cache.

## Parallel market node compiler

- Entry point: `run_compile_batch.py`.
- Arguments:
  - `--lock-glob` or `--locks-file` to select dataset locks.
  - `--max-workers N` to control concurrency.
  - `--verify` to run serial baseline, then parallel run, compare directory signatures before publishing.
  - Additional compiler args (after `--`) forwarded to `run_market_node_compiler.py` (e.g., `--pipeline-mode entry-only`).
- Each worker receives its own staging folder (`__staging/<node_key>__UUID/`) containing:
  - Private temp dirs
  - Dedicated `compile.log`
- On success, staged node moves into `compiled_market_nodes/<NODE>`. Staging dir is removed.
- Failure leaves log + stage dir for inspection; script exits non-zero.

## Determinism guardrails

1. Cache keys must include script/config/dataset hashes; never rely on pair/session alone.
2. `run_compile_batch.py --verify` compares serial vs parallel directory signatures via `cache_key_utils.directory_signature`. Use before enabling new parallel configs.
3. Shared resources:
   - **Never** let workers write to shared dirs (logs, DB, manifests). Everything goes into per-worker staging.
   - Read-only inputs (dataset locks, data tapes) are fine.
4. After verifying determinism, capture directory signatures in change logs as proof.

## Integration checklist

1. Ensure `SESSION_CACHE_DIR` has sufficient disk (writes gzipped JSON per session). Clean up when switching data roots or script hashes.
2. Before first parallel run:
   - Run `python run_compile_batch.py --lock-glob <pattern> --max-workers 1 --verify`.
   - Confirm `cache_audit.jsonl` shows expected hit/miss counts.
3. For CI/regression, add a lightweight job:
   - `python run_compile_batch.py --locks-file locks/smoke.txt --max-workers 2 --verify`.
   - Fails fast on non-deterministic output.
4. When touching deterministic scripts, bump cache version (e.g., `_CACHE_VERSION`) to avoid stale reuse.
5. Document any new env flags or cache directories in `README.md`.
