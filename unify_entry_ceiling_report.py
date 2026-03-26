#!/usr/bin/env python3
from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(".")


def main() -> None:
    with (ROOT / "entry_metric_ceiling_report.json").open() as f:
        base = json.load(f)
    with (ROOT / "long_runner_payout_search_report.json").open() as f:
        payout = json.load(f)

    unified = base
    long_runner = unified["results"]["long"]["runner"]
    for dist_key, payload in payout["results"].items():
        if dist_key in long_runner:
            improved = payload["best_variant"]
            long_runner[dist_key]["profit_ceiling"] = improved
            long_runner[dist_key]["pips_per_hour_ceiling"] = improved
            long_runner[dist_key]["equity_per_hour_ceiling"] = improved
            long_runner[dist_key]["win_rate_ceiling"] = improved

    (ROOT / "entry_metric_ceiling_report_unified.json").write_text(json.dumps(unified, indent=2, default=str))


if __name__ == "__main__":
    main()
