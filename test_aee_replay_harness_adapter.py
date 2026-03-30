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
        self.assertIn("baseline_1to1_money_result_pips", result)
        self.assertIn("baseline_protective_money_result_pips", result)
        self.assertIn("delta_vs_1to1_baseline_pips", result)
        self.assertIn("delta_vs_protective_baseline_pips", result)
        self.assertIn("state_transitions", result)

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
        self.assertIn("report_contract", report)
        self.assertIn("per_trade_delta", report)
        self.assertIn("trade_results", report)

        self.assertEqual(report["summary"]["count"], 2)
        self.assertTrue(len(report["by_reason_code"]) >= 1)
        self.assertTrue(len(report["by_state_transition"]) >= 1)

    def test_policy_override_annotation(self) -> None:
        trade = {
            "trade_id": "R-POLICY",
            "target_distance": 2.0,
            "baseline_final_pips": 0.0,
            "rows": [
                {
                    "bar_index": 1,
                    "profit_now": -1.9,
                    "velocity_now": -0.2,
                    "progress_ratio": -0.95,
                },
                {
                    "bar_index": 2,
                    "profit_now": -1.6,
                    "velocity_now": 0.3,
                    "progress_ratio": -0.80,
                },
            ],
        }

        result = replay_trade_path(
            trade,
            policy_name="panic_soften",
            policy_overrides={
                "disable_panic_inference": 1.0,
                "build_safety_giveback_r": 1.5,
            },
        )

        self.assertEqual(result["policy_name"], "panic_soften")
        self.assertEqual(result["packets"][0]["meta"]["policy_name"], "panic_soften")

    def test_replay_fallback_identity_and_timestamps_are_deterministic(self) -> None:
        trade = {
            "target_distance": 2.0,
            "rows": [
                {"bar_index": 1, "profit_now": 0.2, "velocity_now": 0.03, "progress_ratio": 0.10},
                {"bar_index": 2, "profit_now": 0.4, "velocity_now": 0.05, "progress_ratio": 0.20},
            ],
        }

        r1 = replay_trade_path(trade)
        r2 = replay_trade_path(trade)

        self.assertEqual(r1["trade_id"], r2["trade_id"])
        self.assertEqual(r1["packets"][0]["timestamp"], "1970-01-01T00:00:00Z")
        self.assertEqual(r1["packets"][1]["timestamp"], "1970-01-01T00:01:00Z")


if __name__ == "__main__":
    unittest.main()
