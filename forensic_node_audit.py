import pandas as pd

def run_forensic_audit():
    df = pd.read_csv('micro_harvest_enriched_monday_trades.csv')
    
    # Calculate Node PnL first to identify strictly the negative ones
    node_pnl = df.groupby('node')['pnl'].sum().reset_index()
    negative_nodes = node_pnl[node_pnl['pnl'] < 0]['node'].tolist()
    
    df_neg = df[df['node'].isin(negative_nodes)]
    
    print("\n" + "="*110)
    print(f"FORENSIC NODE AUDIT: RED NODES ONLY".center(110))
    print("="*110)
    
    for node in sorted(negative_nodes):
        node_df = df_neg[df_neg['node'] == node]
        
        trade_count = len(node_df)
        wins = len(node_df[node_df['net_r'] > 0])
        losses = len(node_df[node_df['net_r'] <= 0])
        
        avg_win_r = node_df[node_df['net_r'] > 0]['net_r'].mean() if wins > 0 else 0
        avg_loss_r = node_df[node_df['net_r'] <= 0]['net_r'].mean() if losses > 0 else 0
        
        gross_r = node_df['gross_r'].sum()
        net_r = node_df['net_r'].sum()
        total_friction = node_df['friction'].sum()
        
        exit_counts = node_df['exit_type'].value_counts().to_dict()
        forced = exit_counts.get('CUT', 0)
        targets = exit_counts.get('TARGET', 0)
        stalls = exit_counts.get('STALL', 0)
        giveback = exit_counts.get('GIVEBACK', 0)
        
        avg_hold = node_df['hold_time_mins'].mean()
        
        mode_counts = node_df['mode'].value_counts().to_dict()
        harvester = mode_counts.get('HARVESTER', 0)
        runner = mode_counts.get('RUNNER', 0)
        
        threshold = 0.60
        
        print(f"\n[NODE]: {node}")
        print(f"  Trades: {trade_count:<4} | Wins: {wins:<3} | Losses: {losses:<3}")
        print(f"  Gross R: {gross_r:>7.3f} | Net R: {net_r:>7.3f} | Friction Paid: {total_friction:.3f} R")
        print(f"  Avg Win: {avg_win_r:>7.3f} | Avg Loss: {avg_loss_r:>7.3f} | Avg Hold: {avg_hold:.1f} mins")
        print(f"  Exits  : Target: {targets} | Stall/Chop: {stalls} | Forced Cut: {forced}")
        print(f"  Modes  : Harvester: {harvester} | Runner: {runner}   (Entry Thresh: {threshold})")
        
        # Attribution Conclusion (Algorithmic Logic check)
        conclusion = ""
        if trade_count <= 4:
            conclusion = "E - Tiny sample noise (insufficient data to make structural conclusions)"
        elif gross_r > 0 and net_r < 0:
            conclusion = "A - Friction choked (structural physical edge exists, but is bled out by spread/fees)"
        elif forced > (trade_count * 0.6):
            conclusion = "C - Entry logic failure (signal fires but market fundamentally disagrees immediately)"
        elif stalls > (trade_count * 0.5):
            conclusion = "B - Exit logic / Stall trapped (enters fine, but fails to reach .25R and chop kills it)"
        else:
            conclusion = "D - Structural Edge Absence / Bad Mode pairing"
            
        print(f"  -> PRIMARY ATTRIBUTION: {conclusion}")
    print("="*110 + "\n")

if __name__ == "__main__":
    run_forensic_audit()
