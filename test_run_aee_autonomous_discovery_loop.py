from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from run_aee_autonomous_discovery_loop import _is_winner, run_autonomous_discovery_loop


class TestRunAEEAutonomousDiscoveryLoop(unittest.TestCase):
    def _sample_slice(self) -> dict:
        return {
            "trades": [
                {
                    "trade_id": "L1",
                    "target_distance": 2.0,
                    "baseline_final_pips": 0.5,
                    "meta": {"scenario": "s1"},
                    "rows": [
                        {"bar_index": 1, "profit_now": 0.2, "velocity_now": 0.1, "progress_ratio": 0.1},
                        {"bar_index": 2, "profit_now": -1.9, "velocity_now": -0.3, "progress_ratio": -0.9},
                    ],
                },
                {
                    "trade_id": "L2",
                    "target_distance": 2.0,
                    "baseline_final_pips": -0.5,
                    "meta": {"scenario": "s2"},
                    "rows": [
                        {"bar_index": 1, "profit_now": 0.4, "velocity_now": 0.1, "progress_ratio": 0.2},
                        {"bar_index": 2, "profit_now": 0.9, "velocity_now": 0.1, "progress_ratio": 0.45},
                    ],
                },
            ]
        }

    def test_winner_rule_contract(self) -> None:
        self.assertTrue(
            _is_winner(
                {
                    "total_delta_vs_1to1_baseline_pips": 0.1,
                    "total_delta_vs_protective_baseline_pips": 0.1,
                    "total_delta_vs_current_pips": 0.1,
                    "regressions": {"has_major_regression": False},
                }
            )
        )
        self.assertFalse(
            _is_winner(
                {
                    "total_delta_vs_1to1_baseline_pips": 0.1,
                    "total_delta_vs_protective_baseline_pips": -0.1,
                    "total_delta_vs_current_pips": 0.1,
                    "regressions": {"has_major_regression": False},
                }
            )
        )

    def test_autonomous_loop_report_contract(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            inp = root / "slice.json"
            out = root / "autonomous_report.json"
            inp.write_text(json.dumps(self._sample_slice()), encoding="utf-8")

            summary = run_autonomous_discovery_loop(
                trades_path=inp,
                report_out=out,
                max_iterations=2,
                plateau_window=1,
                improvement_epsilon=0.0,
                max_candidates=4,
            )

            self.assertTrue(out.exists())
            self.assertIn("stop_condition", summary)
            self.assertGreaterEqual(summary.get("iteration_count", 0), 1)

            report = json.loads(out.read_text(encoding="utf-8"))
            self.assertIn("protocol", report)
            self.assertIn("iterations", report)
            self.assertIn("best_overall", report)
            self.assertIn(report["stop_condition"], {"winner_found", "plateau", "iteration_limit", "structural_error"})
            self.assertGreaterEqual(len(report["iterations"]), 1)
            first_iter = report["iterations"][0]
            self.assertIn("report_path", first_iter)
            self.assertIn("status", first_iter)


if __name__ == "__main__":
    unittest.main()
