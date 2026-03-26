import pandas as pd
import glob
import os

def analyze_node_profitability():
    csv_file = 'true_event_loop_monday_specialized.csv'
    if not os.path.exists(csv_file):
        print("Error: No trade log found. Run the simulator first.")
        return

    df = pd.read_csv(csv_file)
    if df.empty:
        print("No trades recorded.")
        return

    # Aggregate by Node
    node_stats = df.groupby('node').agg({
        'pnl': 'sum',
        'realized_R': 'mean',
        'entry_time': 'count'
    }).rename(columns={'entry_time': 'trade_count', 'pnl': 'total_pnl', 'realized_R': 'avg_R'})

    node_stats = node_stats.sort_values('total_pnl')

    print("\n--- PERFORMANCE BY NODE (MONDAY) ---")
    print(node_stats.to_string())

    losers = node_stats[node_stats['total_pnl'] < 0]
    print(f"\nTotal Losing Nodes: {len(losers)}")
    if not losers.empty:
        print("\nTOP LOSERS TO FIX:")
        print(losers.head(10).to_string())

if __name__ == "__main__":
    analyze_node_profitability()
