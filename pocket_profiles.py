from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


def _norm_token(value: Any, default: str = "*") -> str:
    raw = str(value or "").strip().lower()
    return raw if raw else default


def normalize_pocket_context(context: Optional[Dict[str, Any]]) -> Dict[str, str]:
    ctx = dict(context or {})
    return {
        "session": _norm_token(ctx.get("session")),
        "weekday": _norm_token(ctx.get("weekday")),
        "session_quarter": _norm_token(ctx.get("session_quarter")),
        "pair": _norm_token(ctx.get("pair")),
    }


def resolve_pocket_key(context: Optional[Dict[str, Any]]) -> str:
    c = normalize_pocket_context(context)
    return f"{c['session']}|{c['weekday']}|{c['session_quarter']}|{c['pair']}"


def _candidate_keys(context: Dict[str, str]) -> List[str]:
    # Most-specific to least-specific fallback.
    s = context["session"]
    d = context["weekday"]
    q = context["session_quarter"]
    p = context["pair"]
    return [
        f"{s}|{d}|{q}|{p}",
        f"{s}|{d}|{q}|*",
        f"{s}|{d}|*|{p}",
        f"{s}|{d}|*|*",
        f"{s}|*|{q}|{p}",
        f"{s}|*|{q}|*",
        f"{s}|*|*|{p}",
        f"{s}|*|*|*",
        f"*|{d}|{q}|{p}",
        f"*|{d}|{q}|*",
        f"*|{d}|*|{p}",
        f"*|{d}|*|*",
        f"*|*|{q}|{p}",
        f"*|*|{q}|*",
        f"*|*|*|{p}",
        "*|*|*|*",
    ]


def load_pocket_profiles(path: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    p = Path(path)
    if not p.is_absolute():
        p = Path.cwd() / p

    if not p.exists():
        return ({"pockets": {}}, {"loaded": False, "path": str(p), "reason": "missing"})

    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
        pockets = dict(raw.get("pockets") or {})
        return (
            {
                "version": str(raw.get("version") or ""),
                "source": str(raw.get("source") or ""),
                "pockets": pockets,
            },
            {
                "loaded": True,
                "path": str(p),
                "count": len(pockets),
            },
        )
    except Exception as exc:
        return ({"pockets": {}}, {"loaded": False, "path": str(p), "reason": str(exc)})


def _split_pocket_key(key: str) -> Optional[Tuple[str, str, str, str]]:
    parts = [str(p).strip().lower() for p in str(key or "").split("|")]
    if len(parts) != 4:
        return None
    return (parts[0], parts[1], parts[2], parts[3])


def _specificity(parts: Tuple[str, str, str, str]) -> int:
    return sum(1 for p in parts if p != "*")


def _compatible_match_score(
    *,
    parts: Tuple[str, str, str, str],
    context: Dict[str, str],
) -> Optional[int]:
    ctx_parts = (context["session"], context["weekday"], context["session_quarter"], context["pair"])
    score = 0
    for pocket_part, ctx_part in zip(parts, ctx_parts):
        if pocket_part == "*":
            score += 1
            continue
        if pocket_part == ctx_part:
            score += 3
            continue
        return None
    return score


def _structural_closeness_score(
    *,
    parts: Tuple[str, str, str, str],
    context: Dict[str, str],
) -> int:
    # "Closest" fallback when no compatible wildcard path exists.
    # Exact token match is best, wildcard is acceptable, explicit mismatch is worst.
    ctx_parts = (context["session"], context["weekday"], context["session_quarter"], context["pair"])
    score = 0
    for pocket_part, ctx_part in zip(parts, ctx_parts):
        if pocket_part == ctx_part:
            score += 4
        elif pocket_part == "*":
            score += 2
        else:
            score -= 1
    return score


def select_pocket_record(profiles: Dict[str, Any], context: Optional[Dict[str, Any]]) -> Tuple[Optional[str], Dict[str, Any], str]:
    pockets = dict((profiles or {}).get("pockets") or {})
    if not pockets:
        return None, {}, "none"

    norm_ctx = normalize_pocket_context(context)
    parsed_items: List[Tuple[str, Tuple[str, str, str, str], Dict[str, Any]]] = []
    for key, rec in pockets.items():
        if not isinstance(rec, dict):
            continue
        parts = _split_pocket_key(str(key or ""))
        if parts is None:
            continue
        parsed_items.append((str(key), parts, rec))

    if not parsed_items:
        return None, {}, "none"

    # Primary path: choose best compatible wildcard match.
    best_compatible: Optional[Tuple[int, int, str, Dict[str, Any]]] = None
    for key, parts, rec in parsed_items:
        score = _compatible_match_score(parts=parts, context=norm_ctx)
        if score is None:
            continue
        candidate = (score, _specificity(parts), key, rec)
        if best_compatible is None or candidate[:2] > best_compatible[:2]:
            best_compatible = candidate
    if best_compatible is not None:
        return best_compatible[2], best_compatible[3], "compatible"

    # Secondary path: nearest structural fallback, even with one-or-more explicit mismatches.
    # This prevents hard drops to global defaults when a nearly identical pocket exists.
    best_nearest: Optional[Tuple[int, int, str, Dict[str, Any]]] = None
    for key, parts, rec in parsed_items:
        candidate = (_structural_closeness_score(parts=parts, context=norm_ctx), _specificity(parts), key, rec)
        if best_nearest is None or candidate[:2] > best_nearest[:2]:
            best_nearest = candidate
    if best_nearest is not None:
        return best_nearest[2], best_nearest[3], "nearest"

    return None, {}, "none"


def _normalize_key_set(values: Any) -> Set[str]:
    if not isinstance(values, list):
        return set()
    return {str(v).strip().upper() for v in values if str(v).strip()}


def _apply_energy_policy_modifiers(policy: Dict[str, Set[str]], energy_block: Dict[str, Any]) -> None:
    modifiers = dict(energy_block.get("policy_modifiers") or {})

    for section in ("enable", "suppress", "needs_sample", "quarantine"):
        add_key = f"{section}_add"
        remove_key = f"{section}_remove"
        policy[section] |= _normalize_key_set(modifiers.get(add_key))
        policy[section] -= _normalize_key_set(modifiers.get(remove_key))


def resolve_effective_policy(
    *,
    base_policy: Dict[str, Any],
    profiles: Dict[str, Any],
    context: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    out_policy: Dict[str, Set[str]] = {
        "enable": set(base_policy.get("enable", set()) or set()),
        "suppress": set(base_policy.get("suppress", set()) or set()),
        "needs_sample": set(base_policy.get("needs_sample", set()) or set()),
        "quarantine": set(base_policy.get("quarantine", set()) or set()),
    }
    strict = bool(base_policy.get("strict", True))

    selected_key, selected_record, match_mode = select_pocket_record(profiles, context)
    mechanical = dict(selected_record.get("mechanical") or {}) if isinstance(selected_record, dict) else {}
    energy = dict(selected_record.get("energy") or {}) if isinstance(selected_record, dict) else {}

    # Mechanical layer is foundational. If present, these sections replace base policy.
    for section in ("enable", "suppress", "needs_sample", "quarantine"):
        if section in mechanical and isinstance(mechanical.get(section), list):
            out_policy[section] = _normalize_key_set(mechanical.get(section))

    energy_valid = bool(energy.get("valid", False))
    if energy_valid:
        _apply_energy_policy_modifiers(out_policy, energy)

    return {
        "policy": {
            "enable": out_policy["enable"],
            "suppress": out_policy["suppress"],
            "needs_sample": out_policy["needs_sample"],
            "quarantine": out_policy["quarantine"],
            "strict": strict,
        },
        "pocket": {
            "requested": resolve_pocket_key(context),
            "matched": selected_key,
            "match_mode": match_mode,
            "mechanical_loaded": bool(mechanical),
            "energy_present": bool(energy),
            "energy_valid": energy_valid,
            "mechanical": mechanical,
            "energy": energy,
        },
    }
