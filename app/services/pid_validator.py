"""Thin service-layer wrappers around the PID schema functions.

Why a separate module?  The `schemas/pid.py` module owns the regex logic.
This module exposes two convenience functions used by service and route code
without importing Pydantic's `BaseModel` machinery.

Typical usage:
    from app.services.pid_validator import validate_and_normalise, pid_to_json

    pid_type, pid_value = validate_and_normalise(raw_pid)   # raises ValueError on bad input
    pid_dict = pid_to_json(pid_type, pid_value)             # {"type": "doi", "value": "10..."}
"""

from app.schemas.pid import detect_pid_type, normalise_pid


def validate_and_normalise(raw_pid: str) -> tuple[str, str]:
    """Return (pid_type, normalised_value) or raise ValueError.

    This is the primary entry point for PID validation in service and route code.
    For HTTP routes, catch ValueError and convert to HTTP 400.

    Args:
        raw_pid: Raw PID string as received from the user (may include URL prefix,
                 URL encoding, or whitespace).

    Returns:
        Tuple of (pid_type, normalised_value), e.g. ("doi", "10.1234/example").

    Raises:
        ValueError: if the PID format is not recognised.
    """
    pid_type = detect_pid_type(raw_pid)
    if pid_type is None:
        raise ValueError(
            f"Unsupported PID format: '{raw_pid}'. "
            "Accepted types: DOI (10.xxx/...), Handle (NNN/...), URN:NBN, ARK."
        )
    return pid_type, normalise_pid(raw_pid)


def pid_to_json(pid_type: str, value: str) -> dict:
    """Return a PID dict in the canonical JSONB storage format.

    All PID arrays in the `digital_object.pids` column use this structure.
    The `@>` containment queries in enrichment.py depend on the exact key names.

    Example:
        pid_to_json("doi", "10.1234/example") → {"type": "doi", "value": "10.1234/example"}
    """
    return {"type": pid_type, "value": value}
