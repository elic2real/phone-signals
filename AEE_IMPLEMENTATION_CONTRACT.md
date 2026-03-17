# AEE IMPLEMENTATION CONTRACT

Mission

The AEE is the primary live exit owner after fill.
Its job is to evaluate live trades, decide according to doctrine, and cause real broker actions before broker-side stop logic becomes the dominant controller.

If it does not causally drive broker actions, it is not working.


---

1. Required doctrine

Bucket A — Immediate failure

Trade never meaningfully goes green and path is weak/adverse.

Must do:

detect quickly

close quickly

not wait for broker SL

not require long hold delay


Expected result:

losing trades get cut before hard SL whenever possible



---

Bucket B — Green then decay

Trade had favorable excursion, then loses retained profit / stalls / weakens.

Must do:

detect that extraction existed

detect that extraction is being lost

close near stall/decay

not allow round-trip to red if avoidable


Expected result:

trades that went green are harvested instead of dying at SL



---

Bucket C — Continuation

Trade is still earning the right to live.

Must do:

hold when path quality is still good

optionally tighten / partial if runner policy says so

not close only because of age


Expected result:

strong paths are allowed to continue



---

Section 2 - Required live actions

AEE logical outputs:

HOLD

TIGHTEN

PARTIAL

CLOSE


These must map to real behavior:

HOLD → no broker action

TIGHTEN → real protection modification

PARTIAL → real size reduction if supported

CLOSE → real broker close request


If the action does not produce the real effect, AEE is not working.


---

Section 3 - Ownership requirements

3.1 Every fresh fill must enter AEE ownership

For every fill, prove:

fill timestamp

first AEE eval timestamp

first AEE decision timestamp


Failure:

fill occurs

trade leaves OPEN set

no first eval


This must never happen silently.



---

3.2 AEE must get control immediately

Must have:

immediate post-fill AEE admission

fast young-trade cadence

no long wait before immediate-failure logic is allowed


Failure:

broker SL owns the trade before AEE can act



---

3.3 Reconciliation cannot steal ownership too early

Reconciliation must not immediately classify a newly filled trade as closed on weak evidence.

Required:

post-fill grace window

broker flat unconfirmed separate from confirmed closure

no stale pre-fill broker snapshots used as proof of closure

no closing local trade before first AEE eval unless broker close is explicitly confirmed


Failure:

trade becomes closed before first eval



---

Section 4 - Close coordination requirements

4.1 One close owner per pair/direction

If multiple same-side trades exist on the same pair:

one successful side-flatten close must satisfy sibling trades

siblings must not send duplicate position_all closes after the side is already flat


Failure:

repeated 404 on sibling close requests



---

4.2 404 interpretation

A 404 close response means:

close request was late

or same-side already flat

or wrong target / stale ownership


It does not mean victory.
It means coordination/timing still has a flaw.


---

Section 5 - Required telemetry

For every fresh trade, be able to show:

fill time

first eval time

first decision time

first close request time

broker close time

close response time

whether AEE acted before broker close

whether broker side was already flat

whether sibling same-side close already succeeded


For every closure classification, emit explicit cause:

broker stop loss confirmed

broker take profit confirmed

broker flat unconfirmed

pair side already flat

sibling close satisfied

nonlocal close confirmed

human manual close confirmed


Do not use “manual close” as a catch-all.


---

Section 6 - Profit capture timing rule

Profit harvesting takes priority over all other exit logic. If a trade is in profit and the path weakens, oscillates, or stalls, the AEE must be free to harvest immediately.

Minimum hold is red-side protection only.

No minimum hold timer may block profit realization once favorable excursion exists.

Required:

`min_hold_green_sec` defaults to `0`

Green weakening/decay exits are time-unlocked

If code path would block a green exit due to hold timer, emit explicit violation telemetry


Failure:

green trade decays to red while hold timer prevents exit


Section 6 - Required logic structure

Use these primitives:

best_favorable

current_favorable

retained_fraction

giveback

velocity

speed

continuation_strength


Core decision structure:

if immediate_failure:
    return "CLOSE"

if had_green and extraction_is_being_lost:
    return "CLOSE"   # or TIGHTEN/PARTIAL if runner policy requires

if continuation_is_strong:
    return "HOLD"

return "HOLD"

Do not let TP-relative logic become the primary controller.

TP is reference information, not the core doctrine.


---

Section 7 - Required live proof standard

Do not claim success because:

code compiles

rules exist

telemetry exists

replay works


Success is only live proof.

For each fresh proof window, report:

count of doctrine exit reasons fired live

count of exit attempts

count of exit responses

count of 404

count of broker STOP_LOSS_ORDER

count of fresh fills

count of fresh fills with no first eval

count of same-side close collisions

whether close coalescing suppressed duplicates

per-trade timing:

fill

first eval

first decision

first close request

broker close

AEE acted before broker close: yes/no




---

Section 8 - Success definition

AEE is working properly only if:

every fresh fill reaches first eval

doctrine reasons fire live

AEE sends real broker closes

broker SL is not the normal manager

same-side duplicate close attempts are coordinated

stale 404 races are rare

reconciliation confirms reality without stealing ownership before first eval



---

Section 9 - Work order priority

Fix in this order:

1. ownership after fill


2. first eval admission


3. close coordination across same pair/direction


4. causal broker execution


5. reconciliation clarity


6. doctrine tuning


7. performance tuning



Do not keep adding more exit rules on top of ownership or execution bugs.


---

Section 10 - One-sentence standard

AEE must be the primary real-time extractor and exit owner of every filled trade, not a post-hoc classifier that logs around broker-managed outcomes.
