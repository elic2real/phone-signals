#!/bin/bash
# Check Thursday node compilation progress

echo "Thursday Node Compilation Progress"
echo "===================================="
echo ""

# Count completed nodes
COMPLETED=$(find compiled_market_nodes -name "*thursday*" -type d -path "*/target_entry_stage" 2>/dev/null | wc -l)
echo "Completed nodes: $COMPLETED / 64"

# Show currently compiling
CURRENT=$(ps aux | grep "run_market_node_compiler.py" | grep "thursday" | grep -v grep | sed 's/.*dataset_lock__//' | sed 's/__11.json.*//' | head -1)
if [ -n "$CURRENT" ]; then
    echo "Currently compiling: $CURRENT"
else
    echo "No compilation running"
fi

echo ""
echo "Recent completions:"
ls -lt compiled_market_nodes/*/target_entry_stage 2>/dev/null | grep thursday | head -5 | awk '{print $9}' | sed 's|compiled_market_nodes/||' | sed 's|/target_entry_stage||'

echo ""
echo "To view full compilation output:"
echo "  tail -f /tmp/thursday_compile_*.log"
