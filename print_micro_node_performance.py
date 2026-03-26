import pandas as pd

def print_performance():
    df = pd.read_csv('micro_harvest_monday_trades.csv')
    
    stats = df.groupby('node').agg(
        trade_count=('node', 'count'),
        total_pnl=('pnl', 'sum'),
        avg_R=('realized_R', 'mean'),
        win_rate=('realized_R', lambda x: (x > 0).mean() * 100)
    ).reset_index()

    stats = stats.sort_values('total_pnl', ascending=True)

    print("\n--- MICRO-EXTRACTION MONDAY NODE PERFORMANCE ---")
    print(f"{'Node'.ljust(30)} | {'PnL ($)'.rjust(10)} | {'Avg R'.rjust(8)} | {'Win Rate'.rjust(10)} | {'Trades'.rjust(6)}")
    print("-" * 75)
    
    total_losing_nodes = 0
    
    for _, row in stats.iterrows():
        node = row['node']
        pnl = row['total_pnl']
        avg_r = row['avg_R']
        win_rate = row['win_rate']
        trades = row['trade_count']
        
        if pnl < 0:
            total_losing_nodes += 1
            
        print(f"{node.ljust(30)} | ${pnl:9.2f} | {avg_r:7.3f}R | {win_rate:8.1f}% | {trades:6d}")

    print("-" * 75)
    print(f"Total Nodes Executing Trades: {len(stats)}")
    print(f"Nodes With Negative PnL: {total_losing_nodes}")
    print(f"Nodes With Positive PnL: {len(stats) - total_losing_nodes}")

if __name__ == "__main__":
    print_performance()
