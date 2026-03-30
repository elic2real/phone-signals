# AEE CODING RCP
## Adaptive Exit Engine — Implementation Specification
### For developers. All formulas, variable definitions, wiring order, pseudocode.

---

## IMPLEMENTATION ORDER

Build and validate in this exact sequence. Do not skip ahead.

```
1. Data classes / state containers
2. Normalization utilities
3. Layer 1 — Economic state computation
4. Layer 2 — Temporal productivity computation
5. Layer 0 — Objective state machine (reads L1 + L2)
6. Layer 3 — Action-value scoring (reads L0 + L1 + L2)
7. Layer 4 — Lifecycle labels (optional, reads L3 + L1 only)
8. Layer 5 — Playbook router (reads L3 + L0)
9. Harvester/runner leg wiring
10. Replay harness for coefficient validation
```

---

## DATA STRUCTURES

### TradeState (per leg)
```python
@dataclass
class TradeState:
    trade_id: str
    leg_type: str                    # "HARVESTER" or "RUNNER"
    entry_price: float
    current_price: float
    stop_price: float
    peak_price: float                # best unrealized price seen
    initial_risk_r: float            # risk in price units at entry
    entry_time: float                # unix timestamp
    expected_duration_sec: float     # set at entry, tunable
    objective: str                   # current objective state
    objective_entered_at: float      # timestamp when objective last changed
```

### EconomicState (computed each tick)
```python
@dataclass
class EconomicState:
    open_pnl_r: float
    locked_floor_r: float
    giveback_from_peak_r: float
    capital_time_cost: float
    forward_potential_proxy: float
```

### TemporalState (computed each tick)
```python
@dataclass
class TemporalState:
    elapsed_time_sec: float
    t_norm: float
    time_unproductive_ratio: float
    time_since_last_progress_sec: float
    productivity_rate: float
```

### ActionValues (computed each tick)
```python
@dataclass
class ActionValues:
    value_close_now: float
    value_hold_now: float
    value_tighten_now: float
    value_extend_now: float
```

---

## NORMALIZATION — REQUIRED BEFORE SCORING

All inputs to Layer 3 must be in comparable range before arithmetic.

### Normalization targets

```python
# P and L are already in R — leave as-is (typical range 0.0 to 2.0+)
# G is in R — leave as-is (typical range 0.0 to 1.0+)
# F must be normalized to [0.0, 1.0]
# C = T * U — both T and U are [0,1] so C is naturally [0,1]
# All clamp to sensible ranges to prevent extreme values corrupting scores

def normalize_inputs(P, L, G, F, T, U):
    P = max(-2.0, min(P, 5.0))
    L = max(-2.0, min(L, 5.0))
    G = max(0.0, min(G, 3.0))
    F = max(0.0, min(F, 1.0))       # F MUST be [0,1] — enforce at proxy computation
    T = max(0.0, min(T, 2.0))
    U = max(0.0, min(U, 1.0))
    C = T * U                        # capital_time_cost
    return P, L, G, F, C
```

---

## LAYER 1 — ECONOMIC STATE COMPUTATION

```python
def compute_economic_state(trade: TradeState, current_price: float,
                           momentum_signal: float, structural_room: float) -> EconomicState:

    # open_pnl_r — unrealized P&L in R units
    if trade.leg_type == "LONG":
        raw_pnl = current_price - trade.entry_price
    else:
        raw_pnl = trade.entry_price - current_price
    open_pnl_r = raw_pnl / trade.initial_risk_r

    # locked_floor_r — guaranteed outcome if stop hit now
    if trade.leg_type == "LONG":
        raw_locked = trade.stop_price - trade.entry_price
    else:
        raw_locked = trade.entry_price - trade.stop_price
    locked_floor_r = raw_locked / trade.initial_risk_r

    # giveback_from_peak_r — leak from best state
    if trade.leg_type == "LONG":
        raw_giveback = trade.peak_price - current_price
    else:
        raw_giveback = current_price - trade.peak_price
    giveback_from_peak_r = max(0.0, raw_giveback / trade.initial_risk_r)

    # forward_potential_proxy — v1 implementation
    # Crude but required. Replace with better signal when available.
    # Both inputs must be normalized to [0,1] before this call.
    forward_potential_proxy = momentum_signal * structural_room
    forward_potential_proxy = max(0.0, min(forward_potential_proxy, 1.0))

    # capital_time_cost computed in temporal layer, placeholder here
    capital_time_cost = 0.0  # filled by temporal layer

    return EconomicState(
        open_pnl_r=open_pnl_r,
        locked_floor_r=locked_floor_r,
        giveback_from_peak_r=giveback_from_peak_r,
        capital_time_cost=capital_time_cost,
        forward_potential_proxy=forward_potential_proxy
    )
```

### Forward Potential Proxy — v1 Implementation Note

```
forward_potential_proxy = momentum_signal * structural_room

momentum_signal:
    - Source: normalized momentum indicator of your choice (RSI slope, EMA diff, tick momentum)
    - Range must be [0.0, 1.0] — normalize before use
    - 0.0 = momentum fully against or flat
    - 1.0 = momentum clearly in direction of trade

structural_room:
    - Source: distance from current price to next structural resistance/support
    - Normalize by ATR or session expected range to get [0.0, 1.0]
    - 0.0 = price at or past structure
    - 1.0 = full room remaining

# OPEN ITEM: v1 proxy is directional only. Needs concrete variable
# sourcing from existing trade infrastructure before production.
```

---

## LAYER 2 — TEMPORAL PRODUCTIVITY COMPUTATION

```python
def compute_temporal_state(trade: TradeState, econ: EconomicState,
                           now: float, last_progress_time: float,
                           total_unproductive_sec: float) -> TemporalState:

    elapsed_time_sec = now - trade.entry_time

    t_norm = elapsed_time_sec / trade.expected_duration_sec

    time_unproductive_ratio = total_unproductive_sec / max(elapsed_time_sec, 1.0)

    time_since_last_progress_sec = now - last_progress_time

    # productivity_rate: economic gain per normalized time unit
    productivity_rate = econ.open_pnl_r / max(t_norm, 0.01)

    return TemporalState(
        elapsed_time_sec=elapsed_time_sec,
        t_norm=t_norm,
        time_unproductive_ratio=time_unproductive_ratio,
        time_since_last_progress_sec=time_since_last_progress_sec,
        productivity_rate=productivity_rate
    )


def update_unproductive_time(econ: EconomicState, prev_open_pnl_r: float,
                              prev_locked_floor_r: float, tick_duration_sec: float,
                              total_unproductive_sec: float) -> float:
    # Tick is unproductive if neither open P&L nor floor advanced meaningfully
    pnl_progress = econ.open_pnl_r - prev_open_pnl_r
    floor_progress = econ.locked_floor_r - prev_locked_floor_r
    if pnl_progress <= 0.0 and floor_progress <= 0.0:
        total_unproductive_sec += tick_duration_sec
    return total_unproductive_sec
```

---

## LAYER 0 — OBJECTIVE STATE MACHINE

```python
# Thresholds — explicit placeholders, tunable
PROTECTED_PROFIT_THRESHOLD_R = 0.25
WEAK_PROFIT_THRESHOLD_R = 0.10
LATE_PHASE_THRESHOLD = 0.66
LOW_FORWARD_THRESHOLD = 0.20
STRONG_FORWARD_THRESHOLD = 0.60
GIVEBACK_FLOOR_TRIGGER = 0.33
LOW_GIVEBACK_THRESHOLD = 0.15
UNPRODUCTIVE_RATIO_THRESHOLD = 0.50
PROGRESS_STALL_THRESHOLD_SEC = 120  # placeholder — tune against replay

# OPEN ITEM: minimum dwell time per objective not yet defined
# OPEN ITEM: whipsaw dampening on threshold oscillation not yet defined
OBJECTIVE_MIN_DWELL_SEC = 30  # placeholder — must be validated


def evaluate_objective_transition(trade: TradeState, econ: EconomicState,
                                   temp: TemporalState, now: float) -> str:

    current = trade.objective
    dwell = now - trade.objective_entered_at

    # Enforce minimum dwell before any transition
    if dwell < OBJECTIVE_MIN_DWELL_SEC:
        return current

    P = econ.open_pnl_r
    L = econ.locked_floor_r
    G = econ.giveback_from_peak_r
    F = econ.forward_potential_proxy
    T = temp.t_norm
    U = temp.time_unproductive_ratio
    stall = temp.time_since_last_progress_sec

    # --- Transition to MAXIMIZE_FLOOR ---
    floor_trigger = (
        P >= PROTECTED_PROFIT_THRESHOLD_R
        or (
            L > 0
            and (
                F < LOW_FORWARD_THRESHOLD
                or G > GIVEBACK_FLOOR_TRIGGER
                or T >= LATE_PHASE_THRESHOLD
            )
        )
    )

    # --- Transition to RELEASE_CAPITAL ---
    release_trigger = (
        (T >= LATE_PHASE_THRESHOLD and P <= WEAK_PROFIT_THRESHOLD_R)
        or U >= UNPRODUCTIVE_RATIO_THRESHOLD
        or (stall >= PROGRESS_STALL_THRESHOLD_SEC and F < LOW_FORWARD_THRESHOLD)
    )

    # --- Re-upgrade to MAXIMIZE_CONTINUATION ---
    continuation_trigger = (
        F >= STRONG_FORWARD_THRESHOLD
        and G <= LOW_GIVEBACK_THRESHOLD
        and U <= 0.25  # tighter than unproductive threshold for re-upgrade
    )

    # Priority order: RELEASE > FLOOR > CONTINUATION upgrade > hold current
    if release_trigger:
        return "RELEASE_CAPITAL"
    if floor_trigger and current == "MAXIMIZE_CONTINUATION":
        return "MAXIMIZE_FLOOR"
    if continuation_trigger and current in ("MAXIMIZE_FLOOR", "RELEASE_CAPITAL"):
        return "MAXIMIZE_CONTINUATION"

    return current
```

---

## LAYER 3 — ACTION-VALUE SCORING

```python
def compute_action_values(econ: EconomicState, temp: TemporalState) -> ActionValues:

    P, L, G, F, C = normalize_inputs(
        econ.open_pnl_r,
        econ.locked_floor_r,
        econ.giveback_from_peak_r,
        econ.forward_potential_proxy,
        temp.t_norm,
        temp.time_unproductive_ratio
    )

    # Scoring functions
    # OPEN ITEM: coefficients 1.25 and 0.5 are placeholders
    # Must be fit against labeled replay paths before live trust

    value_close_now   = L + max(P - G, 0) - C
    value_hold_now    = P + F - G - C
    value_tighten_now = L + 0.5*F - 0.5*G - 0.5*C
    value_extend_now  = P + 1.25*F - 1.25*G - C

    return ActionValues(
        value_close_now=value_close_now,
        value_hold_now=value_hold_now,
        value_tighten_now=value_tighten_now,
        value_extend_now=value_extend_now
    )
```

---

## OBJECTIVE FILTER — MAPS OBJECTIVE TO ALLOWED ACTIONS

```python
def apply_objective_filter(values: ActionValues, objective: str,
                            econ: EconomicState) -> dict:
    """
    Returns dict of {action_name: score} with disallowed actions set to -999.
    Argmax of this dict is the selected action.
    """
    MASK = -999.0

    scores = {
        "CLOSE":   values.value_close_now,
        "HOLD":    values.value_hold_now,
        "TIGHTEN": values.value_tighten_now,
        "EXTEND":  values.value_extend_now,
    }

    F = econ.forward_potential_proxy
    G = econ.giveback_from_peak_r
    U_ratio = None  # pass in if needed

    if objective == "MAXIMIZE_CONTINUATION":
        # Reject EXTEND if F weak or G excessive
        if F < LOW_FORWARD_THRESHOLD or G > GIVEBACK_FLOOR_TRIGGER:
            scores["EXTEND"] = MASK
        # CLOSE and TIGHTEN remain available but will score low naturally

    elif objective == "MAXIMIZE_FLOOR":
        # Extension only if F very strong AND G low
        if not (F >= STRONG_FORWARD_THRESHOLD and G <= LOW_GIVEBACK_THRESHOLD):
            scores["EXTEND"] = MASK

    elif objective == "RELEASE_CAPITAL":
        # Only CLOSE allowed by default
        # TIGHTEN allowed only if floor is positive (brief protected squeeze)
        scores["HOLD"] = MASK
        scores["EXTEND"] = MASK
        if econ.locked_floor_r <= 0:
            scores["TIGHTEN"] = MASK

    return scores


def select_action(scores: dict) -> str:
    return max(scores, key=scores.get)
```

---

## ACTION STABILITY — OPEN ITEM

```python
# OPEN ITEM: hysteresis / minimum hold period not yet defined
# Without this, actions near threshold will thrash within seconds
# Recommended implementation pattern when ready:

# last_action: str
# last_action_time: float
# ACTION_MIN_HOLD_SEC: float = TBD

# if now - last_action_time < ACTION_MIN_HOLD_SEC:
#     if new_action != last_action:
#         required_margin = TBD  # score gap required to override
#         if scores[new_action] - scores[last_action] < required_margin:
#             return last_action  # stay — not worth switching yet
```

---

## LAYER 4 — LIFECYCLE LABELS (OPTIONAL)

```python
def compute_lifecycle_label(values: ActionValues, econ: EconomicState) -> str:
    """
    Read-only derived label from Layer 3 + Layer 1.
    May NOT be used to override Layer 3 action selection.
    """
    best = max(values.value_close_now, values.value_hold_now,
               values.value_tighten_now, values.value_extend_now)

    if values.value_extend_now == best and econ.forward_potential_proxy >= STRONG_FORWARD_THRESHOLD:
        return "RUNNER_CONFIRMED"
    if values.value_close_now == best and econ.open_pnl_r <= WEAK_PROFIT_THRESHOLD_R:
        return "LATE_RED"
    if values.value_tighten_now == best and econ.locked_floor_r > 0:
        return "GREEN_LOCKED_SQUEEZE"
    if values.value_hold_now == best and econ.open_pnl_r > 0:
        return "GREEN_SLOW"
    if values.value_close_now == best:
        return "CAPITAL_WASTE"
    return "UNCLASSIFIED"
```

---

## LAYER 5 — PLAYBOOK ROUTER

```python
def route_playbook(action: str, trade: TradeState,
                   econ: EconomicState) -> str:

    if action == "CLOSE":
        return "CLOSE"

    if action == "HOLD":
        objective = trade.objective
        if objective == "MAXIMIZE_CONTINUATION":
            return "HOLD"
        return "RUNNER_HOLD"

    if action == "TIGHTEN":
        return "MOVE_SL"  # specific level computed by SL engine

    if action == "EXTEND":
        if econ.locked_floor_r > 0:
            return "RUNNER_HOLD"
        return "HOLD"

    return "HOLD"  # safe default
```

---

## HARVESTER / RUNNER WIRING

```python
# At harvest trigger:
# 1. Close harvester leg -> CLOSE
# 2. Create runner leg with new TradeState, new objective, new expected_duration_sec
# 3. Runner leg runs through full AEE independently

# OPEN ITEM: position split ratio at harvest not defined
# What fraction of position becomes runner is a direct money variable
# Must be specified and validated against replay before live use
RUNNER_POSITION_FRACTION = None  # TBD
```

---

## FULL TICK LOOP

```python
def aee_tick(trade: TradeState, market_data: dict, now: float,
             last_progress_time: float, total_unproductive_sec: float,
             prev_open_pnl_r: float, prev_locked_floor_r: float) -> dict:

    # 1. Compute economic state
    econ = compute_economic_state(
        trade,
        market_data["current_price"],
        market_data["momentum_signal"],  # normalized [0,1]
        market_data["structural_room"]   # normalized [0,1]
    )

    # 2. Update unproductive time
    tick_dur = market_data.get("tick_duration_sec", 1.0)
    total_unproductive_sec = update_unproductive_time(
        econ, prev_open_pnl_r, prev_locked_floor_r,
        tick_dur, total_unproductive_sec
    )

    # 3. Compute temporal state
    temp = compute_temporal_state(
        trade, econ, now, last_progress_time, total_unproductive_sec
    )

    # 4. Update capital time cost into econ
    econ.capital_time_cost = temp.t_norm * temp.time_unproductive_ratio

    # 5. Evaluate objective transition
    new_objective = evaluate_objective_transition(trade, econ, temp, now)
    if new_objective != trade.objective:
        trade.objective = new_objective
        trade.objective_entered_at = now

    # 6. Score actions
    values = compute_action_values(econ, temp)

    # 7. Apply objective filter
    scores = apply_objective_filter(values, trade.objective, econ)

    # 8. Select action
    action = select_action(scores)

    # 9. Optional lifecycle label
    label = compute_lifecycle_label(values, econ)

    # 10. Route to playbook
    playbook_action = route_playbook(action, trade, econ)

    return {
        "action": playbook_action,
        "label": label,
        "objective": trade.objective,
        "scores": scores,
        "econ": econ,
        "temp": temp,
    }
```

---

## OPEN ITEMS — BLOCKING BEFORE PRODUCTION

| Item | Risk | Status |
|---|---|---|
| Coefficient fitting (1.25, 0.5) | Wrong action ranking | Not defined |
| Action stability / hysteresis rule | Thrashing on live spreads | Not defined |
| Harvester/runner split ratio | Direct P&L impact | Not defined |
| Ground truth definition for replay validation | Cannot validate Layer 3 | Not defined |
| Objective dwell time value | Whipsaw on transitions | Placeholder (30s) |
| Whipsaw dampening threshold | Objective flipping near trigger | Not defined |
| Forward potential proxy v1 concrete sourcing | F is placeholder | Not defined |

---

## THRESHOLDS (EXPLICIT PLACEHOLDERS)

```python
PROTECTED_PROFIT_THRESHOLD_R   = 0.25
WEAK_PROFIT_THRESHOLD_R        = 0.10
LATE_PHASE_THRESHOLD           = 0.66
LOW_FORWARD_THRESHOLD          = 0.20
STRONG_FORWARD_THRESHOLD       = 0.60
GIVEBACK_FLOOR_TRIGGER         = 0.33
LOW_GIVEBACK_THRESHOLD         = 0.15
UNPRODUCTIVE_RATIO_THRESHOLD   = 0.50
PROGRESS_STALL_THRESHOLD_SEC   = 120
OBJECTIVE_MIN_DWELL_SEC        = 30
```

All are explicit. All are tunable. None are sacred.

---

## VALIDATION CHECKLIST BEFORE LIVE

- [ ] All inputs to Layer 3 confirmed in same normalized domain
- [ ] F proxy produces values in [0.0, 1.0] on real market data
- [ ] Objective state machine tested on at least 3 synthetic paths covering all transitions
- [ ] Layer 4 confirmed to read Layer 3 output only — no independent state
- [ ] Action thrashing tested on oscillating synthetic input
- [ ] Harvester/runner split ratio defined and replay-tested
- [ ] Coefficient fitting procedure defined and run
- [ ] Ground truth replay definition written and validated
