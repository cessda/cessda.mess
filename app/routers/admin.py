"""Admin endpoints — require `X-Admin-Key` header authentication.

These endpoints are NOT intended for public use.  They expose operational controls:
  - `GET /admin/stats`   — database row counts and cache health summary

To refresh the knowledge graph, run the standalone export script directly:
    docker compose run --rm mess-api python scripts/export_triples.py

Authentication:
  All routes in this router use the `require_admin` dependency, which checks the
  `X-Admin-Key` header against `settings.mess_admin_key`.  Requests with a missing
  or incorrect key receive HTTP 403.
"""

import logging

from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_session
from app.models.digital_object import DigitalObject
from app.models.relationship import Relationship
from app.schemas.stats import StatsResponse

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin")


def require_admin(x_admin_key: str = Header(...)) -> None:
    """FastAPI dependency that enforces admin authentication.

    Raises HTTP 403 if the `X-Admin-Key` header is missing, empty, or does not
    match `settings.mess_admin_key`.  Applied via `dependencies=[Depends(require_admin)]`
    on each admin route.
    """
    if not settings.mess_admin_key or x_admin_key != settings.mess_admin_key:
        raise HTTPException(status_code=403, detail="Invalid or missing admin key")


@router.get("/stats", response_model=StatsResponse, dependencies=[Depends(require_admin)])
async def stats(session: AsyncSession = Depends(get_session)) -> StatsResponse:
    """Return database row counts and cache health summary.

    Provides a quick operational overview without hitting any external APIs.
    All counts are computed in a single database round-trip via scalar subqueries.

    Returns:
        StatsResponse with:
          - total_objects / total_relationships
          - fresh_objects (within TTL) / stale_objects (beyond TTL)
          - objects_by_type  — breakdown by "dataset", "publication", etc.
          - objects_by_origin — breakdown by "source_endpoint", "scholexplorer", etc.
    """
    from datetime import UTC, datetime, timedelta

    ttl_cutoff = datetime.now(UTC) - timedelta(hours=settings.mess_cache_ttl_hours)

    total_objects = await session.scalar(select(func.count()).select_from(DigitalObject))
    total_relationships = await session.scalar(select(func.count()).select_from(Relationship))
    fresh_objects = await session.scalar(
        select(func.count())
        .select_from(DigitalObject)
        .where(DigitalObject.last_checked >= ttl_cutoff)
    )

    type_rows = await session.execute(
        select(DigitalObject.object_type, func.count().label("cnt"))
        .group_by(DigitalObject.object_type)
    )
    objects_by_type = {row.object_type: row.cnt for row in type_rows}

    origin_rows = await session.execute(
        select(DigitalObject.origin, func.count().label("cnt"))
        .group_by(DigitalObject.origin)
    )
    objects_by_origin = {row.origin: row.cnt for row in origin_rows}

    return StatsResponse(
        total_objects=total_objects or 0,
        total_relationships=total_relationships or 0,
        fresh_objects=fresh_objects or 0,
        stale_objects=(total_objects or 0) - (fresh_objects or 0),
        objects_by_type=objects_by_type,
        objects_by_origin=objects_by_origin,
    )
