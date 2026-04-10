"""Paginated list of source objects in the database.

Exposes `GET /api/objects` for the browse UI homepage.  Returns only objects with
`origin = 'source_endpoint'` (i.e. objects that were directly enriched by a user,
not objects discovered as related works).

Sort options:
  enrichment_time    — most recently enriched first (`last_checked DESC`)
  openaire_relations — most Scholexplorer-discovered relationships first
  source_citations   — highest OpenAlex citation count for the source object first
  related_citations  — highest total citation count across all related objects first
"""

import logging

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.database import get_session
from app.models.digital_object import DigitalObject
from app.models.relationship import Relationship
from app.schemas.objects import ObjectListItem, ObjectListResponse, SortOption

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api")

_MAX_PAGE_SIZE = 100


@router.get("/objects", response_model=ObjectListResponse)
async def list_objects(
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=1, le=_MAX_PAGE_SIZE),
    sort: SortOption = Query(default=SortOption.enrichment_time),
    session: AsyncSession = Depends(get_session),
) -> ObjectListResponse:
    """Return a paginated list of source digital objects.

    Only objects with `origin = 'source_endpoint'` are included (i.e. objects
    that were directly submitted for enrichment, not discovered as related works).

    The `openaire_relation_count` field counts relationships where `provenance =
    'scholexplorer'`.  The `related_citation_sum` field sums the `citation_count`
    of all related target objects.
    """
    # Alias for DigitalObject used as the relationship target in the citation-sum subquery.
    TargetDO = aliased(DigitalObject, name="target_do")

    # Correlated subquery: count Scholexplorer relationships for each source object.
    rel_count_sq = (
        select(func.count(Relationship.id))
        .where(
            Relationship.source_id == DigitalObject.id,
            Relationship.provenance == "scholexplorer",
        )
        .correlate(DigitalObject)
        .scalar_subquery()
    )

    # Correlated subquery: sum citation_count of all related (target) objects.
    rel_cite_sq = (
        select(func.coalesce(func.sum(TargetDO.citation_count), 0))
        .join(Relationship, Relationship.target_id == TargetDO.id)
        .where(Relationship.source_id == DigitalObject.id)
        .correlate(DigitalObject)
        .scalar_subquery()
    )

    # Base query with computed columns.
    base_q = (
        select(
            DigitalObject,
            rel_count_sq.label("openaire_relation_count"),
            rel_cite_sq.label("related_citation_sum"),
        )
        .where(DigitalObject.origin == "source_endpoint")
    )

    # Apply sort.
    if sort == SortOption.enrichment_time:
        base_q = base_q.order_by(DigitalObject.last_checked.desc())
    elif sort == SortOption.openaire_relations:
        base_q = base_q.order_by(rel_count_sq.desc())
    elif sort == SortOption.source_citations:
        base_q = base_q.order_by(DigitalObject.citation_count.desc().nulls_last())
    elif sort == SortOption.related_citations:
        base_q = base_q.order_by(rel_cite_sq.desc())

    # Total count (without pagination).
    total = await session.scalar(
        select(func.count())
        .select_from(DigitalObject)
        .where(DigitalObject.origin == "source_endpoint")
    ) or 0

    # Paginated rows.
    offset = (page - 1) * page_size
    rows = await session.execute(base_q.offset(offset).limit(page_size))

    items = []
    for row in rows:
        obj: DigitalObject = row[0]
        items.append(
            ObjectListItem(
                id=obj.id,
                title=obj.title,
                publication_date=obj.publication_date,
                creators=obj.creators,
                pids=obj.pids,
                access=obj.access,
                source_local_id=obj.source_local_id,
                external_ids=obj.external_ids or [],
                last_checked=obj.last_checked,
                citation_count=obj.citation_count,
                openaire_relation_count=int(row[1] or 0),
                related_citation_sum=int(row[2] or 0),
            )
        )

    logger.info(
        "Object list served",
        extra={"page": page, "page_size": page_size, "sort": sort, "total": total},
    )
    return ObjectListResponse(total=total, page=page, page_size=page_size, items=items)
