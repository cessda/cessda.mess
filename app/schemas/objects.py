"""Pydantic schemas for the paginated source-object list endpoint."""

from datetime import datetime
from enum import Enum

from pydantic import BaseModel


class SortOption(str, Enum):
    enrichment_time = "enrichment_time"
    openaire_relations = "openaire_relations"
    source_citations = "source_citations"
    related_citations = "related_citations"


class ObjectListItem(BaseModel):
    id: int
    title: str | None
    publication_date: str | None
    creators: list | None
    pids: list
    access: dict | None
    source_local_id: str | None
    external_ids: list
    last_checked: datetime
    citation_count: int | None
    openaire_relation_count: int
    related_citation_sum: int

    model_config = {"from_attributes": True}


class ObjectListResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: list[ObjectListItem]
