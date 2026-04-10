"""PID detection, normalisation, and Pydantic input validation.

Supported PID types and their canonical forms after normalisation:
  - DOI:     `10.XXXX/...`           (URL prefix `https://doi.org/` stripped)
  - URN:NBN: `urn:nbn:<cc>:...`      (passed through unchanged)
  - ARK:     `ark:/NNNNN/...`        (passed through unchanged)
  - Handle:  `NNN/...`               (passed through unchanged)

All other formats are rejected with a ValueError / HTTP 400.

The `PidInput` model is used for Pydantic-validated request bodies.
For path/query params that require manual handling, use the standalone
`detect_pid_type` and `normalise_pid` functions directly.
"""

import re
from urllib.parse import unquote

from pydantic import BaseModel, field_validator

# Patterns are applied AFTER stripping DOI URL prefixes and URL-decoding.
_DOI_RE = re.compile(r"^10\.\d{4,9}/.+", re.IGNORECASE)
_URN_NBN_RE = re.compile(r"^urn:nbn:[a-z]{2}:.+", re.IGNORECASE)
_ARK_RE = re.compile(r"^ark:/\d{5}/.+", re.IGNORECASE)
_HANDLE_RE = re.compile(r"^\d+/.+")  # Must come last — DOIs also match this if not pre-checked

# Common DOI URL forms accepted as input; all are stripped to bare `10.xxx/...` form.
_DOI_URL_PREFIXES = ("https://doi.org/", "http://doi.org/", "doi:")


def detect_pid_type(value: str) -> str | None:
    """Detect the PID type from a raw string.

    URL-decodes and strips whitespace before matching.  Returns a type string
    (`"doi"`, `"urn_nbn"`, `"ark"`, `"handle"`) or `None` if unrecognised.
    """
    v = unquote(value).strip()

    # Strip DOI URL prefixes before matching so `https://doi.org/10.xxx/yyy`
    # is recognised as a DOI rather than falling through to Handle detection.
    for prefix in _DOI_URL_PREFIXES:
        if v.lower().startswith(prefix.lower()):
            v = v[len(prefix):]
            break

    if _DOI_RE.match(v):
        return "doi"
    if _URN_NBN_RE.match(v):
        return "urn_nbn"
    if _ARK_RE.match(v):
        return "ark"
    if _HANDLE_RE.match(v):
        return "handle"
    return None


def normalise_pid(value: str) -> str:
    """Return the canonical PID value: URL-decoded, whitespace-stripped, DOI prefix removed.

    Does NOT validate the format — call `detect_pid_type` first if you need validation.
    """
    v = unquote(value).strip()
    for prefix in _DOI_URL_PREFIXES:
        if v.lower().startswith(prefix.lower()):
            return v[len(prefix):]
    return v


class PidInput(BaseModel):
    """Pydantic model for a single PID string.

    The `pid` field is validated and normalised on assignment.
    Use this as a request body schema when you need Pydantic's validation pipeline.
    For query-param validation, use `services/pid_validator.validate_and_normalise` instead.
    """

    pid: str

    @field_validator("pid")
    @classmethod
    def validate_pid(cls, v: str) -> str:
        pid_type = detect_pid_type(v)
        if pid_type is None:
            raise ValueError(
                "Unsupported PID format. Accepted types: DOI, Handle, URN:NBN, ARK."
            )
        return normalise_pid(v)
