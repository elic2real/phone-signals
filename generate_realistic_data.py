
import csv
import tick_generator

def write_csv(filename, ticks):
    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['instrument', 'ts', 'bid', 'ask', 'mid'])
        for t in ticks:
            # Check keys, tick_generator usually returns 'instrument', 'ts', 'bid', 'ask', 'mid'
            writer.writerow([t['instrument'], t['ts'], t['bid'], t['ask'], t['mid']])
    print(f"Generated {filename} with {len(ticks)} ticks.")

# Generate Realistic Scenarios
try:
    write_csv('scenario_realistic_trend.csv', tick_generator.scenario_realistic_trend())
    write_csv('scenario_realistic_range.csv', tick_generator.scenario_realistic_range())
    write_csv('scenario_realistic_news.csv', tick_generator.scenario_realistic_news_spike())
except Exception as e:
    print(f"Error generating data: {e}")
