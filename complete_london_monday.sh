#!/bin/bash

# Sequential completion script for London Monday session
PAIRS=("eur_usd" "gbp_chf" "gbp_jpy" "gbp_usd" "nzd_usd" "usd_cad" "usd_chf" "usd_jpy" "aud_cad" "chf_jpy")

echo "=== Starting sequential completion of London Monday session ==="
echo "Pairs remaining: ${#PAIRS[@]}"
echo ""

for pair in "${PAIRS[@]}"; do
    echo "=== Starting $pair ==="
    
    # Check if already complete
    if [ -f "compiled_market_nodes/${pair^^}__monday__london/node_manifest.json" ]; then
        echo "$pair already complete, skipping..."
        continue
    fi
    
    # Clean up any partial runs
    rm -rf "compiled_market_nodes/${pair^^}__monday__london/aee_target_local_fixedpop" 2>/dev/null
    rm -rf "compiled_market_nodes/${pair^^}__monday__london/aee_target_theoretical_ceiling" 2>/dev/null
    rm -f "compiled_market_nodes/${pair^^}__monday__london/node_manifest.json" 2>/dev/null
    
    # Start the rebuild
    echo "Launching $pair..."
    nohup python3 -u rebuild_entry_and_downstream.py --dataset-lock "dataset_lock__${pair}__monday__london__11.json" > "rebuild_seq_logs/${pair}__monday__london__11.log" 2>&1 &
    echo $! > "rebuild_seq_logs/${pair}__monday__london__11.pid"
    
    # Monitor completion
    while true; do
        pid=$(cat "rebuild_seq_logs/${pair}__monday__london__11.pid" 2>/dev/null || true)
        if [ -n "$pid" ] && ps -p "$pid" >/dev/null 2>&1; then
            echo -n "."
            sleep 30
        else
            echo ""
            if [ -f "compiled_market_nodes/${pair^^}__monday__london/node_manifest.json" ]; then
                echo "✓ $pair COMPLETE"
            else
                echo "✗ $pair FAILED - check logs"
                tail -20 "rebuild_seq_logs/${pair}__monday__london__11.log"
            fi
            break
        fi
    done
done

echo ""
echo "=== London Monday session complete! ==="
echo "Checking all pairs..."
for pair in aud_jpy aud_usd eur_chf eur_gbp eur_jpy eur_usd gbp_chf gbp_jpy gbp_usd nzd_usd usd_cad usd_chf usd_jpy aud_cad chf_jpy; do
    if [ -f "compiled_market_nodes/${pair^^}__monday__london/node_manifest.json" ]; then
        echo "✓ $pair"
    else
        echo "✗ $pair"
    fi
done
