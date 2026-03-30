from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


PACKET_SCHEMA_VERSION = "AEE_TRADE_STATE_PACKET_V1"


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class TradeStatePacket:
    schema_version: str
    trade_id: str
    bar_index: int
    timestamp: str
    state_before: str
    state_after: str
    action: str
    reason_code: str
    context: dict[str, float]
    meta: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "trade_id": self.trade_id,
            "bar_index": self.bar_index,
            "timestamp": self.timestamp,
            "state_before": self.state_before,
            "state_after": self.state_after,
            "action": self.action,
            "reason_code": self.reason_code,
            "context": dict(self.context),
            "meta": dict(self.meta),
        }


def build_trade_state_packet(
    *,
    trade_id: str,
    bar_index: int,
    state_before: str,
    state_after: str,
    action: str,
    reason_code: str,
    progress_r: float,
    unrealized_pips: float,
    giveback_r: float,
    continuation_score: float,
    stall_score: float,
    panic_trigger: bool,
    timestamp: str | None = None,
    meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    packet = TradeStatePacket(
        schema_version=PACKET_SCHEMA_VERSION,
        trade_id=str(trade_id),
        bar_index=max(0, int(bar_index)),
        timestamp=str(timestamp or _iso_now()),
        state_before=str(state_before),
        state_after=str(state_after),
        action=str(action),
        reason_code=str(reason_code),
        context={
            "progress_r": float(progress_r),
            "unrealized_pips": float(unrealized_pips),
            "giveback_r": float(giveback_r),
            "continuation_score": float(continuation_score),
            "stall_score": float(stall_score),
            "panic_trigger": 1.0 if bool(panic_trigger) else 0.0,
        },
        meta=dict(meta or {}),
    )
    return packet.to_dict()
