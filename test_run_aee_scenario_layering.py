from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from run_aee_scenario_layering import classify_scenario, run_scenario_layering


class TestRunAEEScenarioLayering(unittest.TestCase):
    def test_classify_scenario_fast_panic(self) -> None:
        scenario = classify_scenario(
            {
                "final_reason_code": "panic_trigger",
                "final_state_transition": "PROTECT->PANIC",
                "time_in_trade_sec": 120,
                "max_giveback_r": 0.4,
                "delta_vs_baseline_pips": -0.3,
                "locked_profit_pips": 0.0,
            }
        )
        self.assertEqual(scenario, "FAST_PANIC_FAILURE")

    def test_run_scenario_layering_outputs(self) -> None:
        kernel_report = {
            "summary": {"count": 2},
            "trade_results": [
                {
                    "final_reason_code": "panic_trigger",
                    "final_state_transition": "PROTECT->PANIC",
                    "final_money_result_pips": -1.0,
                    "baseline_money_result_pips": -0.5,
                    "delta_vs_baseline_pips": -0.5,
                    "time_in_trade_sec": 120,
                    "max_giveback_r": 0.8,
                    "locked_profit_pips": 0.0,
                },
                {
                    "final_reason_code": "build_safety_breach",
                    "final_state_transition": "BUILD->PANIC",
                    "final_money_result_pips": 0.2,
                    "baseline_money_result_pips": 0.1,
                    "delta_vs_baseline_pips": 0.1,
                    "time_in_trade_sec": 420,
                    "max_giveback_r": 1.2,
                    "locked_profit_pips": 0.0,
                },
            ],
        }

        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            inp = root / "kernel_report.json"
            out_report = root / "scenario_report.json"
            out_playbooks = root / "playbooks.json"
            inp.write_text(json.dumps(kernel_report), encoding="utf-8")

            summary = run_scenario_layering(
                kernel_report_path=inp,
                scenario_report_out=out_report,
                scenario_playbooks_out=out_playbooks,
            )

            self.assertGreaterEqual(summary["scenario_count"], 2)
            self.assertTrue(out_report.exists())
            self.assertTrue(out_playbooks.exists())

            report = json.loads(out_report.read_text(encoding="utf-8"))
            self.assertIn("by_scenario", report)
            self.assertIn("FAST_PANIC_FAILURE", report["by_scenario"])
            self.assertIn("BUILD_GIVEBACK_CASCADE", report["by_scenario"])
            self.assertIn("total_delta_vs_baseline_pips", report["by_scenario"]["FAST_PANIC_FAILURE"])


if __name__ == "__main__":
    unittest.main()
