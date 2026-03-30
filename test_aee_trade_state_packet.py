from __future__ import annotations

import unittest

from aee_state_machine_v2 import AEEContext, transition_aee_state_with_packet
from aee_trade_state_packet import PACKET_SCHEMA_VERSION


class TestAEETradeStatePacket(unittest.TestCase):
    def test_packet_contains_reason_coded_decision(self) -> None:
        ctx = AEEContext(
            progress_r=0.10,
            unrealized_pips=0.3,
            giveback_r=0.05,
            continuation_score=0.20,
            stall_score=0.10,
            panic_trigger=False,
        )
        packet = transition_aee_state_with_packet(
            "PROTECT",
            ctx,
            trade_id="T-001",
            bar_index=3,
            timestamp="2026-03-30T00:00:00Z",
            meta={"pair": "EUR_USD"},
        )

        self.assertEqual(packet["schema_version"], PACKET_SCHEMA_VERSION)
        self.assertEqual(packet["trade_id"], "T-001")
        self.assertEqual(packet["state_before"], "PROTECT")
        self.assertEqual(packet["state_after"], "PROTECT")
        self.assertEqual(packet["action"], "TIGHTEN")
        self.assertEqual(packet["reason_code"], "protect_risk_control")
        self.assertEqual(packet["meta"]["pair"], "EUR_USD")

    def test_packet_transitions_to_panic(self) -> None:
        ctx = AEEContext(
            progress_r=0.90,
            unrealized_pips=3.5,
            giveback_r=0.10,
            continuation_score=0.80,
            stall_score=0.10,
            panic_trigger=True,
        )
        packet = transition_aee_state_with_packet(
            "RUNNER",
            ctx,
            trade_id="T-002",
            bar_index=5,
        )

        self.assertEqual(packet["state_after"], "PANIC")
        self.assertEqual(packet["action"], "FULL_EXIT")
        self.assertEqual(packet["reason_code"], "panic_trigger")
        self.assertEqual(packet["context"]["panic_trigger"], 1.0)


if __name__ == "__main__":
    unittest.main()
