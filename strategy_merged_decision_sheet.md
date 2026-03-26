# Strategy Merged Decision Sheet

- Sim minimum sample gate: `50`
- Columns: canonical key, live fills, live first-green, live realized pnl, sim trade count, sim delta-R, final label

| Canonical Key | Live Fills | Live First-Green | Live Realized PnL (pips) | Sim Trades | Sim Delta-R | Final Label |
|---|---:|---:|---:|---:|---:|---|
| `SHORT|HARVESTER|T3_5` | 0 | 0.00% | 0.00 | 132 | 0.3442 | KEEP |
| `SHORT|HARVESTER|T6_0` | 0 | 0.00% | 0.00 | 107 | 0.1759 | KEEP |
| `SHORT|HARVESTER|T2_5` | 0 | 0.00% | 0.00 | 104 | 0.2504 | KEEP |
| `SHORT|HARVESTER|T7_0` | 0 | 0.00% | 0.00 | 101 | 0.1332 | KEEP |
| `LONG|HARVESTER|T3_5` | 0 | 0.00% | 0.00 | 97 | 0.1287 | KEEP |
| `LONG|HARVESTER|T5_0` | 0 | 0.00% | 0.00 | 86 | 0.2628 | KEEP |
| `LONG|HARVESTER|T6_0` | 0 | 0.00% | 0.00 | 85 | 0.2173 | KEEP |
| `LONG|HARVESTER|T7_0` | 0 | 0.00% | 0.00 | 81 | 0.2330 | KEEP |
| `SHORT|HARVESTER|T5_0` | 0 | 0.00% | 0.00 | 77 | 0.3177 | KEEP |
| `LONG|HARVESTER|T8_0` | 0 | 0.00% | 0.00 | 74 | 0.1272 | KEEP |
| `SHORT|HARVESTER|T8_0` | 0 | 0.00% | 0.00 | 71 | 0.1903 | KEEP |
| `LONG|HARVESTER|T1_5` | 0 | 0.00% | 0.00 | 163 | -0.0389 | SUPPRESS |
| `LONG|HARVESTER|T2_5` | 0 | 0.00% | 0.00 | 144 | -0.0019 | SUPPRESS |
| `SHORT|HARVESTER|T1_5` | 0 | 0.00% | 0.00 | 113 | -0.0324 | SUPPRESS |
| `BREAK|RUNNER|T8_0` | 5 | 20.00% | -8.40 | 0 | 0.0000 | SUPPRESS |
| `LONG|RUNNER|T3_5` | 0 | 0.00% | 0.00 | 4 | 0.7786 | NEEDS_SAMPLE |
| `LONG|RUNNER|T2_5` | 0 | 0.00% | 0.00 | 4 | 0.5200 | NEEDS_SAMPLE |
| `LONG|RUNNER|T6_0` | 0 | 0.00% | 0.00 | 3 | 0.7833 | NEEDS_SAMPLE |
| `SHORT|RUNNER|T5_0` | 0 | 0.00% | 0.00 | 3 | 0.7600 | NEEDS_SAMPLE |
| `LONG|RUNNER|T7_0` | 0 | 0.00% | 0.00 | 3 | 0.6190 | NEEDS_SAMPLE |
| `SHORT|RUNNER|T2_5` | 0 | 0.00% | 0.00 | 3 | 0.6133 | NEEDS_SAMPLE |
| `LONG|RUNNER|T5_0` | 0 | 0.00% | 0.00 | 3 | 0.5500 | NEEDS_SAMPLE |
| `SHORT|RUNNER|T7_0` | 0 | 0.00% | 0.00 | 3 | 0.4571 | NEEDS_SAMPLE |
| `SHORT|RUNNER|T6_0` | 0 | 0.00% | 0.00 | 2 | 1.5417 | NEEDS_SAMPLE |
| `SHORT|RUNNER|T8_0` | 0 | 0.00% | 0.00 | 2 | 0.8438 | NEEDS_SAMPLE |
| `LONG|RUNNER|T8_0` | 0 | 0.00% | 0.00 | 2 | 0.5938 | NEEDS_SAMPLE |
| `SHORT|RUNNER|T3_5` | 0 | 0.00% | 0.00 | 1 | 0.7429 | NEEDS_SAMPLE |
| `PULLBACK_RECLAIM|HARVESTER|T8_0` | 5 | 20.00% | -4.00 | 0 | 0.0000 | NEEDS_SAMPLE |

## Priority Next Buckets

- `short:harvester:3.5` -> `SHORT|HARVESTER|T3_5` (trades=132, delta_R=0.3442)
- `short:harvester:2.5` -> `SHORT|HARVESTER|T2_5` (trades=104, delta_R=0.2504)
- `long:harvester:5` -> `LONG|HARVESTER|T5_0` (trades=86, delta_R=0.2628)
- `long:harvester:6` -> `LONG|HARVESTER|T6_0` (trades=85, delta_R=0.2173)
- `short:harvester:5` -> `SHORT|HARVESTER|T5_0` (trades=77, delta_R=0.3177)
- `short:harvester:8` -> `SHORT|HARVESTER|T8_0` (trades=71, delta_R=0.1903)
