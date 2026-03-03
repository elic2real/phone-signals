#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="audit_run_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RUN_DIR"

export PERFORMANCE_MONITORING_ENABLED="${PERFORMANCE_MONITORING_ENABLED:-1}"
export PYTHONUNBUFFERED=1

# If bot writes JSONLs under ./logs, link them into the run dir for the monitor.
mkdir -p logs
ln -sf "$(pwd)/logs/trades.jsonl" "$RUN_DIR/trades.jsonl" || true
ln -sf "$(pwd)/logs/metrics.jsonl" "$RUN_DIR/metrics.jsonl" || true

echo "[BOOT] Starting bot..."
( python3 -u phone_bot.py 2>&1 | tee "$RUN_DIR/bot.log" ) &
BOT_PID=$!

echo "[BOOT] Starting monitor..."
( python3 -u monitor_audit.py \
    --run-dir "$RUN_DIR" \
    --bot-log "$RUN_DIR/bot.log" \
    --cadence-min 10 \
    --hourly 1 \
    2>&1 | tee "$RUN_DIR/monitor.log" ) &
MON_PID=$!

echo "[BOOT] Running for 4h..."
sleep $((4*60*60))

echo "[STOP] Stopping bot + monitor..."
kill -INT "$BOT_PID" 2>/dev/null || true
sleep 3
kill -TERM "$BOT_PID" 2>/dev/null || true
kill -TERM "$MON_PID" 2>/dev/null || true

wait "$BOT_PID" 2>/dev/null || true
wait "$MON_PID" 2>/dev/null || true

echo "[DONE] Logs in $RUN_DIR"
