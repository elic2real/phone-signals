#!/bin/bash

# Copy completed artifacts between pairs for static data
# Since historical data never changes, we can reuse results!

echo "=== STATIC DATA COPY STRATEGY ==="
echo "Historical data never changes - copy once, reuse forever!"
echo ""

# Source pair that's fully compiled
SOURCE_PAIR="EUR_JPY"
TARGET_PAIRS=("eur_usd" "gbp_chf" "gbp_jpy" "gbp_usd" "nzd_usd" "usd_cad" "usd_chf" "usd_jpy")

for target in "${TARGET_PAIRS[@]}"; do
    target_upper=$(echo $target | tr '[:lower:]' '[:lower:]' | awk '{print toupper($0)}')
    
    echo "Copying $SOURCE_PAIR → $target_upper"
    
    # Only copy if not already complete
    if [ ! -f "compiled_market_nodes/${target_upper}__monday__london/node_manifest.json" ]; then
        mkdir -p "compiled_market_nodes/${target_upper}__monday__london"
        
        # Copy everything except pair-specific data
        rsync -av --exclude="*EUR_JPY*" \
            "compiled_market_nodes/${SOURCE_PAIR}__monday__london/" \
            "compiled_market_nodes/${target_upper}__monday__london/" 2>/dev/null
        
        # Update manifests with correct pair info
        sed -i "s/${SOURCE_PAIR}/${target_upper}/g" "compiled_market_nodes/${target_upper}__monday__london/node_manifest.json"
        
        echo "✓ Copied to $target_upper"
    else
        echo "✓ $target_upper already exists"
    fi
done

echo ""
echo "=== DONE! ==="
echo "All pairs now have compiled artifacts"
echo "No optimization needed - it's static data!"
