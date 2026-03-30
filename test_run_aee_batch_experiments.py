from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from run_aee_batch_experiments import run_batch_experiments


class TestRunAEEBatchExperiments(unittest.TestCase):
    def _sample_slice(self) -> dict:
        return {
            "trades": [
                {
                    "trade_id": "B1",
                    "target_distance": 2.0,
                    "baseline_final_pips": 0.5,
                    "meta": {"scenario": "s1"},
                    "rows": [
                        {"bar_index": 1, "profit_now": 0.2, "velocity_now": 0.1, "progress_ratio": 0.1},
                        {"bar_index": 2, "profit_now": -1.8, "velocity_now": -0.2, "progress_ratio": -0.9},
                    ],
                },
                {
                    "trade_id": "B2",
                    "target_distance": 2.0,
                    "baseline_final_pips": -0.5,
                    "meta": {"scenario": "s2"},
                    "rows": [
                        {"bar_index": 1, "profit_now": 0.4, "velocity_now": 0.1, "progress_ratio": 0.2},
                        {"bar_index": 2, "profit_now": 0.8, "velocity_now": 0.1, "progress_ratio": 0.4},
                    ],
                },
            ]
        }

    def test_batch_report_contract(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            inp = root / "slice.json"
            out = root / "batch_report.json"
            inp.write_text(json.dumps(self._sample_slice()), encoding="utf-8")

            summary = run_batch_experiments(trades_path=inp, report_out=out)
            self.assertTrue(out.exists())
            self.assertGreater(summary["experiment_count"], 1)

            report = json.loads(out.read_text(encoding="utf-8"))
            self.assertIn("ranked_experiments", report)
            self.assertIn("best_experiment", report)
            first = report["ranked_experiments"][0]
            self.assertIn("experiment_id", first)
            self.assertIn("kernel_id", first)
            self.assertIn("kernel_type", first)
            self.assertIn("components", first)
            self.assertIn("component_definitions", first)
            self.assertIn("parameter_set_id", first)
            self.assertIn("parameters", first)
            self.assertIn("total_delta_vs_baseline_pips", first)
            self.assertIn("total_delta_vs_current_pips", first)
            self.assertIn("per_scenario_delta", first)
            self.assertIn("per_scenario_delta_vs_current", first)
            self.assertIn("reason_code_breakdown", first)
            self.assertIn("transition_breakdown", first)
            self.assertIn("regressions", first)
            self.assertIn("per_trade", first)
            self.assertIn("delta_vs_current", first["per_trade"][0])


if __name__ == "__main__":
    unittest.main()
