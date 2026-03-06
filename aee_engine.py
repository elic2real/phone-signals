"""AEE engine wrapper for per-leg policy selection and evaluation routing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict


@dataclass(frozen=True)
class LegPolicy:
    leg_type: str
    policy: str


@dataclass(frozen=True)
class AEEKnobs:
    strictness_mult: float
    near_tp_band_atr: float


def select_leg_policy(leg_type: str) -> LegPolicy:
    lt = str(leg_type or "").upper()
    if lt == "RUNNER" or lt == "RUN":
        return LegPolicy(leg_type="RUNNER", policy="RUNNER_TIGHT")
    return LegPolicy(leg_type="HARVESTER", policy="HARVESTER_AGGRO")


def apply_leg_policy(leg_type: str) -> dict:
    p = select_leg_policy(leg_type)
    return {"leg_type": p.leg_type, "policy": p.policy}


def evaluate_aee(evaluator: Callable[..., Dict[str, Any]], *, leg_type: str, aee_knobs: AEEKnobs | None = None, knobs_hash: str = "", source_level: str = "", source_key: str = "", **kwargs: Any) -> Dict[str, Any]:
    policy = apply_leg_policy(leg_type)
    res = dict(evaluator(aee_knobs=aee_knobs, **kwargs) or {})
    res.setdefault("leg_type", policy["leg_type"])
    res.setdefault("policy", policy["policy"])
    if aee_knobs is not None:
        res["knobs_hash"] = knobs_hash or ""
        res["source_level"] = source_level or ""
        res["source_key"] = source_key or ""
    return res


class AEEEngine:
    """Thin runtime wrapper so a single callsite governs active AEE policy eval."""

    def evaluate_with_leg_policy(self, leg_type: str, evaluator: Callable[..., Dict[str, Any]], aee_knobs: AEEKnobs | None = None, knobs_hash: str = "", source_level: str = "", source_key: str = "", **kwargs: Any) -> Dict[str, Any]:
        return evaluate_aee(evaluator, leg_type=leg_type, aee_knobs=aee_knobs, knobs_hash=knobs_hash, source_level=source_level, source_key=source_key, **kwargs)
