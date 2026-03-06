#!/usr/bin/env bash
set -euo pipefail

TRADES_LOG="${1:-logs/trades.jsonl}"
RUNTIME_LOG="${2:-logs/runtime.log}"

python3 tools/runtime_proof_report.py \
  --trades-log "${TRADES_LOG}" \
  --runtime-log "${RUNTIME_LOG}"
