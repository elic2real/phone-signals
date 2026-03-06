# AEE State Computation Audit

## Price Source
- **Live/Replay Price Source**: Broker pricing endpoint via `oanda_call(o.pricing, pair)` or `oanda_call(o.candles, pair, TF_EXEC, count)`
- **File Path**: `/home/elic/Documents/phone signals/phone_bot.py`, lines ~11743-11754 for exit loop pricing
- **Details**: Uses `oanda_call("pricing_exit", o.pricing, pair)` or falls back to candle data for mid calculation

## Trade State Structure
- **Trade Records**: Stored in SQLite DB, retrieved via `db_call("get_open_trades", db.get_open_trades)`
- **Fields**: `entry` (entry_price), `dir` (direction: "LONG" or "SHORT"), `id` (trade_id), `atr_entry`, `oanda_trade_id`, etc.
- **File Path**: `/home/elic/Documents/phone signals/phone_bot.py`, various DB calls
- **AEE Specific State**: `aee_states` dict with `AEEState` objects, keyed by trade_id
- **AEEState Fields**: entry_price, direction, tp_anchor, sl_price, phase, local_high, local_low, entry_time, last_tick_eval, mid_ring, spread_ring, atr14, k (ATR multiplier)
- **File Path**: `/home/elic/Documents/phone signals/phone_bot.py`, `_aee_eval_for_trade` function (lines ~8026-8100)

## PnL Calculations
- **Pips Conversion**: `to_pips(pair, price_diff)` function
- **ATR Calculation**: `atr(sync_candles, ATR_N)` for ATR_N=14
- **Favorable PnL**: `(mid - entry_price) if direction == "LONG" else (entry_price - mid)`
- **Favorable ATR**: `favorable / atr_entry`
- **File Path**: `/home/elic/Documents/phone signals/phone_bot.py`, exit loop calculations (lines ~11765-11768)

## AEE Metrics
- **Energy Ratio**: Computed in AEE evaluation, stored in `aee_eval["metrics"]["energy_ratio"]`
- **Velocity/Giveback/Stall**: Internal to AEE evaluation logic
- **File Path**: `/home/elic/Documents/phone signals/aee_engine.py` (wrapper), actual logic in imported modules (not visible)

## Missing Data Assessment
- **Entry Price**: Available in trade record (`tr["entry"]`)
- **Current Price**: Available via broker pricing or candles
- **Side**: Available (`tr["dir"]`)
- **ATR Entry**: Available (`tr["atr_entry"]`)
- **MFE/MAE**: Computed in AEE state (`local_high`, `local_low`)
- **Energy Ratio**: Available in AEE metrics
- **Modulators**: Velocity, giveback, stall proximity, time pressure - computed internally in AEE eval, not directly exposed

## TradeState Snapshot Implementation
- **Existing Function**: No single reusable function for TradeState snapshot
- **Recommendation**: Implement `build_trade_state_snapshot(tr, aee_state, aee_metrics, mid, spread_pips)` in phone_bot.py to centralize computation
- **Usage**: Reuse in manual teacher event (if implemented) and AEE eval snapshot
