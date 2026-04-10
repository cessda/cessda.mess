"""GET /status — cache status check without triggering enrichment.

Useful for monitoring scripts, admin dashboards, or pre-flight checks that want to
know whether a PID is already in the cache and how fresh it is, without incurring
the cost of a full enrichment pass.

Unlike `/enrich`, this endpoint:
  - Never calls any external APIs.
  - Never writes to the database.
  - Returns `found: false` if the PID has never been enriched.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_session
from app.schemas.status import StatusResponse
from app.services.cache import is_fresh
from app.services.enrichment import lookup_by_pid
from app.services.pid_validator import validate_and_normalise

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/status", response_model=StatusResponse)
async def status(
    pid: str,
    session: AsyncSession = Depends(get_session),
) -> StatusResponse:
    """Return the cache status for a PID without triggering enrichment.

    Args:
        pid: PID in any supported format (same as `/enrich`).

    Returns:
        `{"pid": "...", "found": bool, "fresh": bool|null, "last_checked": datetime|null,
          "object_type": str|null}`

    Raises:
        400: PID format not recognised.
    """
    try:
        pid_type, pid_value = validate_and_normalise(pid)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    obj = await lookup_by_pid(session, pid_type, pid_value)
    if obj is None:
        return StatusResponse(pid=pid_value, found=False)

    fresh = is_fresh(obj.last_checked, settings.mess_cache_ttl_hours)
    return StatusResponse(
        pid=pid_value,
        found=True,
        fresh=fresh,
        last_checked=obj.last_checked,
        object_type=obj.object_type,
    )
