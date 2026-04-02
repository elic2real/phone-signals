**Tier 0 Recovery Notes**
Anchored from user memory on April 2, 2026.

**Core Idea**
Tier 0 is a multi-layer raw market extraction and mapping engine. It is not yet trade logic. Its job is to discover and quantify market movement, conditions, and context so Tier 1 can interpret them.

**Recovered Internal Layering**
1. `T0.1` Find every price move.
The first simple question is: where did price move?
The only early labels are direction (`LONG` or `SHORT`) and movement distance.
Distance is discovered, not forced, and later becomes target-size structure.

2. `T0.2` Find where movement covers cost.
The next layer asks which movements are large enough to clear execution cost.
This is still physics and economics of movement, not doctrine identity.

3. `T0.3` Measure what came before the event.
The next layer asks what conditions existed before the move happened.
At this stage Tier 0 will produce a very large number of raw results.

4. `T0.4` Independent market mapping layer.
This layer is separate from the event-discovery stream.
It maps the same exact market timeframes the samples came from and records all reasonable, useful, repeatable market structures:
- support
- resistance
- zones
- oscillations
- recurring energy-pattern movement
- any repeatable pattern that is structurally real in the tape

This layer is not trade-aware. It is a parallel structural map of the market.

5. `T0.5` Opportunity fit inside mapped patterns.
The next layer figures out where the discovered opportunities fit inside the mapped structures and repeated patterns.

6. `T0.6` Handoff compiler.
The last Tier 0 layer compiles the notable and quantifiable facts needed for Tier 1.
Tier 0 hands off structured market truth and event truth, not finished strategy decisions.

**Recovered Rules**
- Tier 0 should remain independent of profitability.
- Tier 0 should remain independent of priority or selection logic.
- Tier 0 should remain independent of AEE.
- Tier 0 should discover, map, quantify, and hand off.

**Intended Handoff To Tier 1**
Tier 1 should receive the compiled notable and quantifiable facts from Tier 0 and begin interpretation there, rather than forcing doctrine meaning too early inside Tier 0.
