#!/bin/bash
# run_monday_downstream.sh
# Runs Stage-7+ downstream pipeline for all Monday nodes via run_market_node_compiler.py.
# Stage-6 handoff (session_energy_state_stream.csv + node_identity.json +
# session_state_build_report.json) has already been rebuilt by rebuild_monday_stage6.py.

set -e

PAIRS=(
    "AUD_CAD" "AUD_JPY" "AUD_USD" "CHF_JPY"
    "EUR_CHF" "EUR_GBP" "EUR_JPY" "EUR_USD"
    "GBP_CHF" "GBP_JPY" "GBP_USD" "NZD_JPY"
    "NZD_USD" "USD_CAD" "USD_CHF" "USD_JPY"
)

SESSIONS=("sydney" "asia" "london" "new_york")

TOTAL=64
COUNT=0
FAILED=0
FAILED_NODES=()

echo "=========================================="
echo "Monday Stage-7+ Downstream Pipeline"
echo "Entry / Priority / Static Replay / AEE / Proof"
echo "Total nodes: $TOTAL"
echo "=========================================="
echo ""

for PAIR in "${PAIRS[@]}"; do
    for SESSION in "${SESSIONS[@]}"; do
        COUNT=$((COUNT + 1))
        NODE="${PAIR}__monday__${SESSION}"
        LOCK="dataset_lock__$(echo $PAIR | tr '[:upper:]' '[:lower:]')__monday__${SESSION}__11.json"

        echo "[$COUNT/$TOTAL] $NODE ..."

        if [ ! -f "$LOCK" ]; then
            echo "  SKIP: dataset lock not found: $LOCK"
            FAILED=$((FAILED + 1))
            FAILED_NODES+=("$NODE (no lock)")
            continue
        fi

        LOG="/tmp/monday_downstream_${PAIR}_${SESSION}.log"

        if python3 run_market_node_compiler.py \
            --dataset-lock "$LOCK" \
            --output-root "compiled_market_nodes/$NODE" \
            --pipeline-mode entry-only \
            > "$LOG" 2>&1; then
            echo "  OK"
        else
            echo "  FAIL — see $LOG"
            FAILED=$((FAILED + 1))
            FAILED_NODES+=("$NODE")
        fi

        if [ $((COUNT % 8)) -eq 0 ]; then
            echo ""
            echo "  Progress: $COUNT/$TOTAL complete, $FAILED failed so far"
            echo ""
        fi
    done
done

echo ""
echo "=========================================="
echo "Monday downstream run complete."
echo "  Succeeded: $((TOTAL - FAILED)) / $TOTAL"
echo "  Failed:    $FAILED"
if [ ${#FAILED_NODES[@]} -gt 0 ]; then
    echo ""
    echo "Failed nodes:"
    for NODE in "${FAILED_NODES[@]}"; do
        echo "  $NODE"
    done
fi
echo "=========================================="
