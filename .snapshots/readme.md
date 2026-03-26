# Phone Bot File Organization

This document outlines the structural organization of `phone_bot.py` based on code analysis.

## File Structure Breakdown

### 1. Infrastructure & Setup (Lines 1-300)
- Imports and emoji constants
- **OANDA API wrappers**: `_refresh_instruments_meta`, `get_instrument_meta`
- **Unit helpers**: `tick_size`, `pip_size`

### 2. Core Engines (Lines 680-1250)
- **Path-Space Engine**: `PathSpaceState`, `PathEngine`
- **Pricing Stream**: `TickData` handling
- **State Machine**: Formal state transitions (Ready -> Enter -> Manage)

### 3. Strategy Logic (Lines 6000-7000)
- **AEE (Adaptive Entry/Exit)**: `AEEState`, `calculate_tp_at_birth`
- **Sizing**: `calculate_spread_aware_size`
- **Feature Pipeline**: Data normalization and signal generation

### 4. Data & Resilience (Lines 7000-10000)
- **Resilience Controller**: Managing API timeouts and connection drops
- **Market Data Hub**: Central point for incoming price data
- **Safety Buffers**: Auto-inflation of stop losses during high volatility

### 5. Notifications ("Phone" aspect) (Lines 10486-10760)
- **Webhooks**: `send_webhook_notification` (sends JSON payloads to external URLs)
- **Alerts**: `alert_trade_entered`, `alert_exit_triggered` (formats messages for the user)

### 6. Persistence (Lines 10785-11800)
- **Database**: `DatabaseTransaction` (SQLite wrapper with retry logic)
- **Backups**: `backup_database`, `auto_backup_database`
- **Performance Tracking**: `update_strategy_performance`

### 7. Entry Point
- `if __name__ == "__main__":` at the very end (Line 11906)

## Summary
- **Bot**: 95% of the file (Trading logic, API, Database)
- **Phone**: ~5% (Notification/Webhook functions)
- **PWA**: 0% (No web interface found in this file)

## Key Notes
- PWA (Progressive Web App) functionality was searched for but not found
- The file is purely a backend trading bot with webhook notifications
- No HTML serving, web server routes, or frontend logic present

---

📈 PHONEFX — Path-Space Execution Engine (AEE-Driven Forex System)

Overview

PHONEFX is a path-space trading engine designed to extract profit from price movement itself — not prediction accuracy.

It is not a signal system.
It is not indicator-driven trading.
It is an execution + adaptive exit machine (AEE) that:

Treats price as a continuous path, not candles.

Uses strategies only to enter participation, not to forecast.

Uses the Adaptive Exit Engine (AEE) to actively manage trades tick-by-tick.

Prioritizes capital recycling and profit extraction, not win-rate or R:R.


> Entry is permission.
AEE is edge.




---

Core Philosophy

Traditional trading asks:

> “Will price go up or down?”



PHONEFX asks:

> “Is price currently capable of continuing, stalling, or reversing — and how do we extract equity from that?”



The system assumes:

Forex is bounded and mean-reverting intraday.

Price almost always moves somewhere.

The real edge is managing movement, not predicting it.



---

System Architecture

OANDA Data → Indicator Layer → Strategy Engine → State Machine → Execution → AEE Management

Always-On Data Feeds

For 7 selected pairs:

Feed	Purpose

Pricing (Bid/Ask stream)	True path movement (primary signal)
Multi-TF Candles (M1–H4)	Noise scale (ATR), structure context
OrderBook / PositionBook	Liquidity bias / trap detection
Execution API	Fill truth + trade verification


No artificial rate limiting — information density is maximized.


---

State Machine (Signal Lifecycle)

PASS → WAIT → WATCH → GET_READY → ENTER → MANAGING

State	Meaning

PASS	Ignore environment
WAIT	Monitoring structure
WATCH	Strategy conditions forming
GET_READY	Entry arming begins
ENTER	Trade placed
MANAGING	AEE controls trade


Every transition emits:

JSON log event

Terminal alert

Execution trace



---

The Seven Strategies (Entry Permission Only)

These do not manage trades.
They only create opportunities for AEE to operate.

1️⃣ Compression → Expansion
2️⃣ Continuation Push
3️⃣ Exhaustion Snapback
4️⃣ Failed Breakout
5️⃣ Liquidity Sweep
6️⃣ Volatility Re-Ignition
7️⃣ Intentional Runner (HTF-driven entry)

All strategies use:

ATR-normalized displacement

Efficiency (direction vs churn)

Speed / velocity

Acceptance time beyond levels

Liquidity density (orderbook context)


No candle patterns are used for decisions.


---

Mandatory 80/20 Split Execution

Every trade is automatically split:

Leg	Size	Purpose

80%	Fast capital recycle	Quick extraction, redeploy capital
20%	Runner	Stays alive to capture extended move


This ensures:

Continuous liquidity reuse

Participation in rare extended trends

No need to predict which trade becomes large



---

Capital Deployment Model (No Risk-Based Sizing)

Sizing is not risk-per-trade.

It is capital deployment:

deployment = margin_available × deployment_fraction × confidence_multiplier
units = deployment / price

There are:

❌ No risk caps
❌ No pair limits
❌ No concurrency limits
❌ No R-based sizing

The system self-regulates as margin is consumed and released.

Confidence and spread quality scale size up or down.


---

Adaptive Exit Engine (AEE)

AEE is the real trading system.

It continuously evaluates live trades using path metrics:

Metric	Meaning

Speed	Movement strength
Efficiency	Directional quality
Velocity	Acceleration / decay
Pullback	Adverse movement pressure
Acceptance	Time beyond key levels
Spread Regime	Execution friction


Exit Types (Priority Order)

1️⃣ Panic Exit — structural failure
2️⃣ Stall Capture — movement died
3️⃣ Pulse Harvest — fast spike exhaustion
4️⃣ Decay Exit — continuation degraded

No static TP/SL governs exits.

Broker SL/TP exist only as catastrophic safety rails.


---

Why This Is Different

Traditional Systems	PHONEFX

Predict direction	Measure survivability of movement
Use candles	Uses tick-path metrics
Static TP/SL	Dynamic extraction
Risk-based sizing	Capital recycling
Win-rate focus	Throughput focus
Trade selection edge	Execution edge



---

Technical Stack

Python (single-file deployment friendly for Termux/mobile)

OANDA v20 REST + Pricing Stream

Decimal-safe pricing math (no float drift)

Instrument metadata-driven pip/tick logic

JSONL structured logging for auditability



---

Design Goals

✔ Maximum net profit through continuous participation
✔ Minimize idle capital
✔ React faster than discretionary trading can
✔ Remove prediction dependency
✔ Survive across slow, medium, and fast regimes
✔ Operate fully from a mobile environment if required


---

What This System Is NOT

Not a backtest-optimized strategy.

Not indicator worship.

Not risk-managed retail trading.

Not trying to be “right.”


It is a real-time market interaction engine.


---

Current Development Status

Execution architecture defined

AEE logic specified

Strategy framework complete

Capital deployment model implemented

Indicator wiring + live proof harness in progress



---

Future Extensions

Adaptive regime classifier tuning thresholds dynamically

Execution latency profiling

Multi-broker abstraction

Expanded runner campaign logic



--
