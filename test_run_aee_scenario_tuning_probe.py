from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from run_aee_scenario_tuning_probe import run_scenario_tuning_probe


class TestRunAEEScenarioTuningProbe(unittest.TestCase):
    def _sample_slice(self) -> dict:
        return {
            "trades": [
                {
                    "trade_id": "T1",
                    "target_distance": 2.0,
                    "baseline_final_pips": 0.0,
                    "rows": [
                        {"bar_index": 1, "profit_now": 0.2, "velocity_now": 0.1, "progress_ratio": 0.1},
                        {"bar_index": 2, "profit_now": -1.8, "velocity_now": -0.2, "progress_ratio": -0.9},
                    ],
                },
                {
                    "trade_id": "T2",
                    "target_distance": 2.0,
                    "baseline_final_pips": 0.5,
                    "rows": [
                        {"bar_index": 1, "profit_now": 0.4, "velocity_now": 0.1, "progress_ratio": 0.2},
                        {"bar_index": 2, "profit_now": 0.8, "velocity_now": 0.1, "progress_ratio": 0.4},
                    ],
                },
            ]
        }

    def test_run_probe_outputs_rankings(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            inp = root / "slice.json"
            out = root / "probe_report.json"
            inp.write_text(json.dumps(self._sample_slice()), encoding="utf-8")

            summary = run_scenario_tuning_probe(benchmark_slice_path=inp, report_out=out)

            self.assertEqual(summary["trade_count"], 2)
            self.assertTrue(out.exists())

            report = json.loads(out.read_text(encoding="utf-8"))
            self.assertIn("policy_rankings", report)
            self.assertGreaterEqual(len(report["policy_rankings"]), 2)
            self.assertIn("policy_name", report["policy_rankings"][0])


if __name__ == "__main__":
    unittest.main()
