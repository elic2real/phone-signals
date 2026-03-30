from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from run_aee_kernel_benchmark import extract_fixed_benchmark_slice, run_kernel_benchmark


class TestRunAEEKernelBenchmark(unittest.TestCase):
    def _sample_unified(self) -> dict:
        return {
            "results": {
                "long": {
                    "harvester": {
                        "1.5": {
                            "profit_ceiling": {
                                "rows": [
                                    {
                                        "cluster_id": "LONG_1.5_001",
                                        "distance": 1.5,
                                        "direction": "LONG",
                                        "timestamp_start": "2024-01-01T00:00:00Z",
                                        "price_start": 1.1000,
                                        "price_path": [1.1000, 1.1001, 1.1002],
                                        "pips": 1.5,
                                        "reason": "TP_HIT",
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        }

    def test_extract_fixed_benchmark_slice(self) -> None:
        trades = extract_fixed_benchmark_slice(self._sample_unified(), max_trades=5)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["trade_id"], "LONG_1.5_001::2024-01-01T00:00:00Z")
        self.assertEqual(trades[0]["baseline_final_pips"], 1.5)
        self.assertEqual(len(trades[0]["rows"]), 3)

    def test_run_kernel_benchmark_outputs_files(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            input_path = root / "unified.json"
            slice_path = root / "slice.json"
            report_path = root / "report.json"
            packets_path = root / "packets.json"

            input_path.write_text(json.dumps(self._sample_unified()), encoding="utf-8")
            summary = run_kernel_benchmark(
                unified_path=input_path,
                max_trades=5,
                benchmark_slice_out=slice_path,
                report_out=report_path,
                packets_out=packets_path,
            )

            self.assertEqual(summary["benchmark_trade_count"], 1)
            self.assertTrue(slice_path.exists())
            self.assertTrue(report_path.exists())
            self.assertTrue(packets_path.exists())

            report = json.loads(report_path.read_text(encoding="utf-8"))
            slice_payload = json.loads(slice_path.read_text(encoding="utf-8"))
            self.assertEqual(slice_payload["schema_version"], "AEE_REPLAY_SLICE_V1")
            self.assertIn("by_reason_code", report)
            self.assertIn("by_state_transition", report)
            self.assertEqual(report["kernel_benchmark"]["benchmark_trade_count"], 1)
            self.assertIn("baseline_comparisons", report["kernel_benchmark"])


if __name__ == "__main__":
    unittest.main()
