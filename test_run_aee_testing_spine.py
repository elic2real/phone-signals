from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from run_aee_testing_spine import run_testing_spine


class TestRunAEETestingSpine(unittest.TestCase):
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

    def test_run_testing_spine_writes_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            inp = root / "unified.json"
            inp.write_text(json.dumps(self._sample_unified()), encoding="utf-8")

            summary = run_testing_spine(
                unified_input=inp,
                max_trades=5,
                slice_out=root / "slice.json",
                kernel_report_out=root / "kernel_report.json",
                packets_out=root / "packets.json",
                scenario_report_out=root / "scenario_report.json",
                scenario_playbooks_out=root / "scenario_playbooks.json",
                manifest_out=root / "manifest.json",
            )

            self.assertEqual(summary["spine_version"], "AEE_TESTING_SPINE_V1")
            self.assertEqual(summary["trade_count"], 1)
            manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
            self.assertIn("order", manifest)
            self.assertIn("outputs", manifest)


if __name__ == "__main__":
    unittest.main()
