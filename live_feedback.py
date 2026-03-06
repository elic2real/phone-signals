#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class LiveFeedbackV2:
    alpha: dict[str, float] = field(default_factory=dict)
    beta: dict[str, float] = field(default_factory=dict)
    first_strike: set[str] = field(default_factory=set)

    def multiplier(self, key: str) -> float:
        a = self.alpha.get(key, 1.0)
        b = self.beta.get(key, 1.0)
        mean = a / max(1e-9, a + b)
        return max(0.7, min(1.3, 0.7 + (mean * 0.6)))

    def update(self, key: str, success: bool) -> dict:
        self.alpha[key] = self.alpha.get(key, 1.0) + (1.0 if success else 0.0)
        self.beta[key] = self.beta.get(key, 1.0) + (0.0 if success else 1.0)
        ev = {"kind": "LIVE_FEEDBACK_UPDATE", "key": key, "success": bool(success), "multiplier": self.multiplier(key)}
        if not success and key not in self.first_strike:
            self.first_strike.add(key)
            ev["blacklist_log_only"] = True
        return ev
