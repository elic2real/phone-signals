# Concurrency Caps Audit Note
================================

Generated: 2026-03-05 16:48:00 UTC

## Existing Concurrency Variables

1. **MAX_CONCURRENCY**: 15 (in CeilingConfig dataclass, line 2202)
   - Used in `can_saturate()` method (line 2268)
   - Only checks absolute cap, not per-pair limits

2. **MAX_EXPOSURE_PER_CURRENCY**: 3 (in CeilingConfig, line 2198)
   - Used in `can_saturate()` for currency exposure
   - Counts trades with same currency (e.g., EUR in EUR_USD, EUR_JPY)

## Existing Order Submit Call Sites

1. **Primary Entry Placement** (line 14871):
   ```python
   resp1 = oanda_call("place_market_main", o.place_market, pair, units_main, ...)
   ```

2. **Runner Leg Placement** (line ~15150):
   ```python
   resp2 = oanda_call("place_market_run", o.place_market, pair, units_run, ...)
   ```

3. **Retry Path** (line 7426):
   ```python
   new_response = create_market_order(pair, units, f"{reason}_retry_{attempt}")
   ```

4. **Split Order Chunks** (line 7523):
   ```python
   resp = create_market_order(pair, chunk_units, f"{reason}_split_{i+1}")
   ```

5. **Partial Fill Completion** (line 7739):
   ```python
   new_order_resp = create_market_order(pair=pair, units=remaining_units, ...)
   ```

6. **Panic/Force Exit** (line 6988):
   ```python
   resp_ioc = oanda_call(f"panic_ioc_{exit_reason}", o._post, ...)
   ```

## Existing Blocker Logic

1. **can_enter()** function (line 4302):
   - Checks DRY_RUN_ONLY, ALLOW_ENTRIES, pair blocks
   - Does NOT check concurrency caps

2. **can_saturate()** method (line 2262):
   - Checks MAX_CONCURRENCY and MAX_EXPOSURE_PER_CURRENCY
   - Only used in extraction engine, NOT in main order flow

3. **ORDER_REJECT_BLOCK** cooldown (line 3042):
   - Per-pair cooldown after rejected orders
   - 30-second default cooldown

## Key Findings

1. **No Single Choke Point**: Orders are placed via multiple paths
2. **Concurrency Check Not Wired**: `can_saturate()` exists but not called before orders
3. **No Global Trade Count**: Need to compute from broker snapshot or DB
4. **Missing Per-Pair Cap**: Only currency exposure exists, not per-pair

## Recommended Implementation

1. Create `_place_order_with_guards()` as single choke point
2. Compute open trades from broker snapshot each cycle
3. Add new config variables for caps
4. Wire into all order placement paths
5. Add ORDER_BLOCKED logging with detailed reasons
