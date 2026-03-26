#!/usr/bin/env bash
set -u

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
BOT_FILE="$ROOT_DIR/phone_bot.py"
LOCK_FILE="$ROOT_DIR/.bot_supervisor.lock"
LOG_DIR="$ROOT_DIR/logs"
SUP_LOG="$LOG_DIR/supervisor.log"

mkdir -p "$LOG_DIR"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "[$(date -Is)] ERROR: Python venv not found at $PYTHON_BIN" | tee -a "$SUP_LOG"
  exit 1
fi

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "[$(date -Is)] Supervisor already running (lock busy)." | tee -a "$SUP_LOG"
  exit 0
fi

echo "[$(date -Is)] Supervisor started. root=$ROOT_DIR" | tee -a "$SUP_LOG"

restart_count=0
while true; do
  start_ts=$(date +%s)
  restart_count=$((restart_count + 1))

  echo "[$(date -Is)] Launch #$restart_count: starting phone_bot.py" | tee -a "$SUP_LOG"
  "$PYTHON_BIN" "$BOT_FILE" >> "$SUP_LOG" 2>&1
  exit_code=$?

  end_ts=$(date +%s)
  run_secs=$((end_ts - start_ts))
  echo "[$(date -Is)] Bot exited. code=$exit_code runtime_sec=$run_secs" | tee -a "$SUP_LOG"

  # Fast recovery for crashes, but avoid tight thrash loops.
  if (( run_secs < 30 )); then
    sleep_secs=5
  elif (( run_secs < 300 )); then
    sleep_secs=3
  else
    sleep_secs=1
  fi

  echo "[$(date -Is)] Restarting in ${sleep_secs}s..." | tee -a "$SUP_LOG"
  sleep "$sleep_secs"
done
