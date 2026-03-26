#!/bin/bash
# Recompile all 64 Thursday nodes with Stage 6 clustering

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

echo "=========================================="
echo "Recompiling 64 Thursday Nodes"
echo "Stage 6 Clustering Enabled"
echo "=========================================="
echo ""

for PAIR in "${PAIRS[@]}"; do
    for SESSION in "${SESSIONS[@]}"; do
        COUNT=$((COUNT + 1))
        NODE="${PAIR}__thursday__${SESSION}"
        LOCK="dataset_lock__$(echo $PAIR | tr '[:upper:]' '[:lower:]')__thursday__${SESSION}__11.json"
        
        echo "[$COUNT/$TOTAL] Compiling $NODE..."
        
        if [ ! -f "$LOCK" ]; then
            echo "  ⚠️  Dataset lock not found: $LOCK"
            FAILED=$((FAILED + 1))
            continue
        fi
        
        if python3 run_market_node_compiler.py \
            --dataset-lock "$LOCK" \
            --output-root "compiled_market_nodes/$NODE" \
            --pipeline-mode entry-only \
            > "/tmp/thursday_compile_${PAIR}_${SESSION}.log" 2>&1; then
            
            # Check if Stage 6 integrity report exists
            if [ -f "compiled_market_nodes/$NODE/target_entry_stage/stream_seed/session_state_build_report.json" ]; then
                CLUSTERS=$(python3 -c "import json; print(json.load(open('compiled_market_nodes/$NODE/target_entry_stage/stream_seed/session_state_build_report.json'))['stage_6_integrity']['cluster_count'])" 2>/dev/null || echo "N/A")
                echo "  ✅ Complete - Clusters: $CLUSTERS"
            else
                echo "  ✅ Complete (no Stage 6 report)"
            fi
        else
            echo "  ❌ Failed - check /tmp/thursday_compile_${PAIR}_${SESSION}.log"
            FAILED=$((FAILED + 1))
        fi
        
        echo ""
    done
done

echo "=========================================="
echo "Compilation Summary"
echo "=========================================="
echo "Total nodes: $TOTAL"
echo "Successful: $((TOTAL - FAILED))"
echo "Failed: $FAILED"
echo "=========================================="
