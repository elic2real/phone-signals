from __future__ import annotations


RECOGNITION_TO_DOCTRINE_ID = {
    "OSCILLATION_EDGE_LONG": "OSCILLATION_EDGE_LONG_SCALP",
    "OSCILLATION_EDGE_SHORT": "OSCILLATION_EDGE_SHORT_SCALP",
    "TRANSITION_RELEASE_LONG": "TRANSITION_RELEASE_LONG_STANDARD",
    "TRANSITION_RELEASE_SHORT": "TRANSITION_RELEASE_SHORT_STANDARD",
}


INTENTIONALLY_NON_DOCTRINE_STATES = {
    "BALANCED_ROTATION_LONG",
    "BALANCED_ROTATION_SHORT",
    "COILED_COMPRESSION_NEUTRAL",
    "COILED_TRANSITION_LONG",
    "PRESSURE_DRIVE_NEUTRAL",
    "TRANSITION_RELEASE_NEUTRAL",
    "INTENTIONAL_REJECT_NO_DIRECTION",
}


def normalize_recognition_state_to_doctrine_id(state: str) -> str:
    state = str(state or "").upper()
    return RECOGNITION_TO_DOCTRINE_ID.get(state, state)


def recognition_state_is_doctrine_candidate(state: str) -> bool:
    state = str(state or "").upper()
    if not state or state == "UNMATCHED":
        return False
    if state in INTENTIONALLY_NON_DOCTRINE_STATES:
        return False
    return True
