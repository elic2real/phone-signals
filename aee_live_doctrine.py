from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

ACTION_HOLD = "HOLD"
ACTION_PARTIAL = "PARTIAL"
ACTION_TIGHTEN = "TIGHTEN"
ACTION_CLOSE = "CLOSE"

MODE_HARVESTER = "HARVESTER"
MODE_RUNNER = "RUNNER"


@dataclass
class LiveDoctrineContext:
    mode: str
    best_r: float = 0.0
    worst_r: float = 0.0
    last_progress_s: float = 0.0
    went_green: bool = False
    partial_taken: bool = False
    tightened: bool = False


class LiveDoctrineEngine:
    """Stateful doctrine classifier for live AEE action classes.

    This is a shadow classifier: it outputs HOLD/PARTIAL/TIGHTEN/CLOSE from
    path-state primitives without forcing execution.
    """

    def __init__(self) -> None:
        self._ctx: Dict[str, LiveDoctrineContext] = {}

    def reset_trade(self, trade_key: str) -> None:
        self._ctx.pop(str(trade_key), None)

    def update(
        self,
        *,
        trade_key: str,
        mode: str,
        now_s: float,
        current_r: float,
        mfe_r: float,
        mae_r: float,
        energy: float,
        force_close: bool = False,
    ) -> Dict[str, object]:
        key = str(trade_key)
        mode_u = str(mode or MODE_HARVESTER).upper()
        if mode_u not in (MODE_HARVESTER, MODE_RUNNER):
            mode_u = MODE_HARVESTER

        ctx = self._ctx.get(key)
        if ctx is None or ctx.mode != mode_u:
            ctx = LiveDoctrineContext(mode=mode_u, last_progress_s=float(now_s))
            self._ctx[key] = ctx

        ctx.best_r = max(float(ctx.best_r), float(mfe_r), float(current_r))
        ctx.worst_r = min(float(ctx.worst_r), float(mae_r), float(current_r))
        # Path-state doctrine: once the trade has ever reached green, it stays green.
        if ctx.best_r >= 0.10:
            ctx.went_green = True
        if current_r >= ctx.best_r - 0.02:
            ctx.last_progress_s = float(now_s)

        giveback_r = max(0.0, ctx.best_r - float(current_r))
        stall_s = max(0.0, float(now_s) - float(ctx.last_progress_s))
        strong_cont = float(energy) >= 0.55 and float(current_r) >= (ctx.best_r - 0.05)

        action = ACTION_HOLD
        if force_close or float(current_r) <= -0.30:
            action = ACTION_CLOSE
        elif ctx.mode == MODE_HARVESTER:
            if ctx.went_green and giveback_r >= 0.12:
                action = ACTION_CLOSE
            elif ctx.went_green and stall_s >= 120.0 and float(energy) < 0.35:
                action = ACTION_CLOSE
            elif ctx.went_green and (not ctx.partial_taken) and (not strong_cont):
                action = ACTION_PARTIAL
        else:
            if (not ctx.partial_taken) and float(current_r) >= 0.30 and strong_cont:
                action = ACTION_PARTIAL
            elif ctx.partial_taken:
                if giveback_r >= 0.20 and (not ctx.tightened):
                    action = ACTION_TIGHTEN
                elif ctx.tightened and (giveback_r >= 0.25 or (stall_s >= 120.0 and float(energy) < 0.25)):
                    action = ACTION_CLOSE
            else:
                if ctx.went_green and giveback_r >= 0.14 and (not ctx.tightened):
                    action = ACTION_TIGHTEN
                elif ctx.tightened and float(current_r) <= 0.0:
                    action = ACTION_CLOSE

        if action == ACTION_PARTIAL:
            ctx.partial_taken = True
        elif action == ACTION_TIGHTEN:
            ctx.tightened = True

        return {
            "action": action,
            "mode": ctx.mode,
            "state": {
                "went_green": ctx.went_green,
                "partial_taken": ctx.partial_taken,
                "tightened": ctx.tightened,
                "best_r": round(ctx.best_r, 4),
                "worst_r": round(ctx.worst_r, 4),
                "stall_s": round(stall_s, 2),
                "giveback_r": round(giveback_r, 4),
                "energy": round(float(energy), 4),
                "current_r": round(float(current_r), 4),
            },
        }
