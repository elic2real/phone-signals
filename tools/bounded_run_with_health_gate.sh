#!/usr/bin/env bash
set -euo pipefail

# Wrapper: preflight gate -> bounded run command -> post-run health gate.
# Usage:
#   tools/bounded_run_with_health_gate.sh -- <your bounded run command>
# Example:
#   tools/bounded_run_with_health_gate.sh -- timeout 300s python phone_bot.py

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

PY_BIN="${PY_BIN:-$PROJECT_DIR/.venv/bin/python}"
if [[ ! -x "$PY_BIN" ]]; then
  if command -v python3 >/dev/null 2>&1; then
    PY_BIN="$(command -v python3)"
  else
    echo "ERROR: python executable not found (.venv/bin/python or python3)" >&2
    exit 2
  fi
fi

if [[ "${1:-}" != "--" ]]; then
  echo "Usage: $0 -- <bounded run command>" >&2
  exit 2
fi
shift

if [[ "$#" -eq 0 ]]; then
  echo "ERROR: missing bounded run command after --" >&2
  exit 2
fi

RUN_ID="bounded_health_$(date -u +%Y%m%dT%H%M%SZ)"
PRE_JSON="logs/run_health_preflight_${RUN_ID}.json"
PRE_MD="logs/run_health_preflight_${RUN_ID}.md"
POST_JSON="logs/run_health_post_${RUN_ID}.json"
POST_MD="logs/run_health_post_${RUN_ID}.md"

echo "[health-gate] preflight start run_id=$RUN_ID"
"$PY_BIN" tools/run_health_framework.py \
  --preflight-only \
  --scope latest \
  --run-id "$RUN_ID-preflight" \
  --out-json "$PRE_JSON" \
  --out-md "$PRE_MD" \
  --require-verdict NO_SAMPLE

# Preflight-only verdict NO_SAMPLE means preflight pass (runtime skipped by design).

echo "[health-gate] bounded run start"
"$@"
RUN_EXIT=$?

echo "[health-gate] bounded run exit_code=$RUN_EXIT"

echo "[health-gate] post-run audit start"
"$PY_BIN" tools/run_health_framework.py \
  --scope latest \
  --synthetic-proof \
  --run-id "$RUN_ID-post" \
  --out-json "$POST_JSON" \
  --out-md "$POST_MD" \
  --require-verdict VALID_SAMPLE,NO_SAMPLE
POST_EXIT=$?

if [[ "$RUN_EXIT" -ne 0 ]]; then
  echo "[health-gate] bounded run failed before verdict gating (exit=$RUN_EXIT)" >&2
  exit "$RUN_EXIT"
fi

if [[ "$POST_EXIT" -ne 0 ]]; then
  echo "[health-gate] post-run health gate failed (exit=$POST_EXIT). See $POST_MD" >&2
  exit "$POST_EXIT"
fi

echo "[health-gate] passed preflight + post-run verdict gate"
echo "[health-gate] artifacts: $PRE_MD $POST_MD"
