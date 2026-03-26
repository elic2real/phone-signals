#!/bin/bash

# Fast completion script for London Monday session
# Uses frozen entry rules, skips expensive optimization

PAIRS=("eur_usd" "gbp_chf" "gbp_jpy" "gbp_usd" "nzd_usd" "usd_cad" "usd_chf" "usd_jpy" "aud_cad" "chf_jpy")

echo "=== FAST LONDON MONDAY SESSION COMPLETION ==="
echo "Mode: Using frozen entry rules (no optimization)"
echo "Expected time: ~5-10 minutes per pair"
echo ""
echo "Already completed:"
for pair in aud_jpy aud_usd eur_chf eur_gbp eur_jpy; do
    if [ -f "compiled_market_nodes/${pair^^}__monday__london/node_manifest.json" ]; then
        echo "✓ $pair"
    fi
done
echo ""
echo "Starting remaining pairs..."

for pair in "${PAIRS[@]}"; do
    echo "=== Starting $pair (FAST MODE) ==="
    
    # Check if already complete
    if [ -f "compiled_market_nodes/${pair^^}__monday__london/node_manifest.json" ]; then
        echo "$pair already complete, skipping..."
        continue
    fi
    
    # Clean up any partial runs
    rm -rf "compiled_market_nodes/${pair^^}__monday__london/aee_target_local_fixedpop" 2>/dev/null
    rm -rf "compiled_market_nodes/${pair^^}__monday__london/aee_target_theoretical_ceiling" 2>/dev/null
    rm -f "compiled_market_nodes/${pair^^}__monday__london/node_manifest.json" 2>/dev/null
    
    # Start the FAST rebuild
    echo "Launching $pair in FAST MODE..."
    start_time=$(date +%s)
    nohup python3 -u rebuild_entry_and_downstream_fast.py --dataset-lock "dataset_lock__${pair}__monday__london__11.json" --fast-mode > "rebuild_seq_logs/${pair}__monday__london__11.log" 2>&1 &
    echo $! > "rebuild_seq_logs/${pair}__monday__london__11.pid"
    
    # Monitor completion
    while true; do
        pid=$(cat "rebuild_seq_logs/${pair}__monday__london__11.pid" 2>/dev/null || true)
        if [ -n "$pid" ] && ps -p "$pid" >/dev/null 2>&1; then
            echo -n "."
            sleep 10  # Check more frequently since it should be faster
        else
            echo ""
            end_time=$(date +%s)
            duration=$((end_time - start_time))
            echo "Time taken: ${duration} seconds ($(($duration / 60)) minutes)"
            
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
echo "Final status:"
for pair in aud_jpy aud_usd eur_chf eur_gbp eur_jpy eur_usd gbp_chf gbp_jpy gbp_usd nzd_usd usd_cad usd_chf usd_jpy aud_cad chf_jpy; do
    if [ -f "compiled_market_nodes/${pair^^}__monday__london/node_manifest.json" ]; then
        echo "✓ $pair"
    else
        echo "✗ $pair"
    fi
done
