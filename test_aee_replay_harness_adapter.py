from __future__ import annotations

import unittest

from aee_replay_harness_adapter import build_baseline_comparison_report, replay_trade_path


class TestAEEReplayHarnessAdapter(unittest.TestCase):
    def test_replay_emits_packet_each_step_until_exit(self) -> None:
        trade = {
            "trade_id": "R-001",
            "target_distance": 2.0,
            "baseline_final_pips": 0.5,
            "rows": [
                {
                    "bar_index": 1,
                    "timestamp": "2026-03-30T00:00:00Z",
                    "profit_now": 0.2,
                    "velocity_now": 0.05,
                    "progress_ratio": 0.10,
                },
                {
                    "bar_index": 2,
                    "timestamp": "2026-03-30T00:01:00Z",
                    "profit_now": -1.8,
                    "velocity_now": -0.2,
                    "progress_ratio": -0.90,
                },
            ],
        }

        result = replay_trade_path(trade)

        self.assertGreaterEqual(result["packet_count"], 1)
        self.assertEqual(result["packets"][0]["trade_id"], "R-001")
        self.assertIn("final_reason_code", result)
        self.assertIn("final_state_transition", result)
        self.assertIn("final_money_result_pips", result)
        self.assertIn("time_in_trade_sec", result)
        self.assertIn("max_giveback_r", result)
        self.assertIn("locked_profit_pips", result)

    def test_report_groups_by_reason_and_transition(self) -> None:
        trade_a = {
            "trade_id": "R-A",
            "target_distance": 2.0,
            "baseline_final_pips": 0.4,
            "rows": [
                {
                    "bar_index": 1,
                    "profit_now": 0.1,
                    "velocity_now": 0.0,
                    "progress_ratio": 0.05,
                },
                {
                    "bar_index": 2,
                    "profit_now": 0.2,
                    "velocity_now": 0.0,
                    "progress_ratio": 0.08,
                },
            ],
        }
        trade_b = {
            "trade_id": "R-B",
            "target_distance": 2.0,
            "baseline_final_pips": 0.1,
            "rows": [
                {
                    "bar_index": 1,
                    "profit_now": -2.0,
                    "velocity_now": -0.2,
                    "progress_ratio": -1.0,
                },
            ],
        }

        r1 = replay_trade_path(trade_a)
        r2 = replay_trade_path(trade_b)
        report = build_baseline_comparison_report([r1, r2])

        self.assertIn("summary", report)
        self.assertIn("by_reason_code", report)
        self.assertIn("by_state_transition", report)
        self.assertIn("trade_results", report)

        self.assertEqual(report["summary"]["count"], 2)
        self.assertTrue(len(report["by_reason_code"]) >= 1)
        self.assertTrue(len(report["by_state_transition"]) >= 1)


if __name__ == "__main__":
    unittest.main()
