"""ORM model for the `digital_object` table.

`DigitalObject` is the single entity type for ALL research objects in the system —
both the source dataset that was looked up and every related object discovered via
Scholexplorer, OpenAIRE, or OpenAlex.  Using one table simplifies graph queries and
deduplication: if the same publication appears in two Scholexplorer links, only one
row is created (identified by matching PIDs).

PID deduplication:
  The `pids` JSONB array is the deduplication key.  On every upsert, incoming PIDs
  are compared against existing rows using the PostgreSQL `@>` containment operator,
  which requires the GIN index created in migration 0001.  See `services/enrichment.py`
  for the upsert logic.

Cache freshness:
  `last_checked` is updated on every enrichment pass.  The `/status` and `/enrich`
  endpoints compare it against `settings.mess_cache_ttl_hours` via `services/cache.py`
  to decide whether to re-fetch from external APIs.

Raw response storage:
  `raw_responses` stores the original JSON payloads from each API (keyed by source
  name, e.g. `{"source_endpoint": {...}, "openaire": {...}}`).  This enables
  reprocessing without re-fetching and supports debugging enrichment issues.
"""

from datetime import UTC, datetime

from sqlalchemy import DateTime, Float, Integer, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base

# Valid values for `object_type` — maps to SKG-IF ResearchProduct sub-types.
OBJECT_TYPES = ("dataset", "publication", "software", "other")

# Records where the object was first discovered.
ORIGIN_VALUES = ("source_endpoint", "scholexplorer", "openaire", "openalex")


class DigitalObject(Base):
    """A research digital object: dataset, publication, software, or other.

    Created by `services/enrichment.upsert_digital_object`.
    Serialized to SKG-IF JSON-LD by `schemas/skg_if.build_json_ld`.
    Materialized as RDF triples by `scripts/export_triples.build_graph`.
    """

    __tablename__ = "digital_object"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Array of PID dicts, each with `{"type": "<pid_type>", "value": "<pid_value>"}`.
    # GIN-indexed for fast @> containment queries.  Example:
    #   [{"type": "doi", "value": "10.1234/example"}, {"type": "handle", "value": "11234/1"}]
    pids: Mapped[list] = mapped_column(JSONB, nullable=False)

    # One of OBJECT_TYPES above.
    object_type: Mapped[str] = mapped_column(Text, nullable=False)

    # Primary display title (English when available).
    title: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Multilingual titles as `{"en": ["Title in English"], "de": ["Titel auf Deutsch"]}`.
    titles: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # List of creator/contributor dicts, e.g. `[{"name": "Smith, J.", "identifier": "..."}]`.
    creators: Mapped[list | None] = mapped_column(JSONB, nullable=True)

    # Free-text keyword strings, e.g. `["survey data", "social science"]`.
    keywords: Mapped[list | None] = mapped_column(JSONB, nullable=True)

    # Structured topic/subject classification entries from CESSDA or OpenAlex.
    topics: Mapped[list | None] = mapped_column(JSONB, nullable=True)

    # Access rights, e.g. `{"type": "OPEN"}` or `{"type": "restricted", "url": "..."}`.
    access: Mapped[dict | None] = mapped_column(JSONB, nullable=True)

    # Data collection method descriptions (social-science–specific).
    methods: Mapped[list | None] = mapped_column(JSONB, nullable=True)

    # Funding projects linked to this object (from OpenAIRE).
    # Each entry: {"id": "...", "code": "...", "title": "...", "funder": "...",
    #              "funder_short": "...", "funder_jurisdiction": "...", "source": "openaire"}
    projects: Mapped[list | None] = mapped_column(JSONB, nullable=True)

    # Local identifier from the source endpoint (e.g. "FSD3217").
    source_local_id: Mapped[str | None] = mapped_column(Text, nullable=True)

    # External system identifiers from third-party APIs.  Each entry has:
    #   {"source": "<system_name>", "id": "<identifier_value>"}
    # One entry per source (deduped by `source` key).  GIN-indexed for containment queries.
    # Example: [{"source": "openaire", "id": "50|doi_dedup___::8ec9..."},
    #           {"source": "openalex",  "id": "https://openalex.org/W123"}]
    external_ids: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)

    # Publication date from the source endpoint (`dateIssued` in SKG-IF), e.g. "2021" or "2021-03-15".
    publication_date: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Citation count from OpenAlex (`cited_by_count`).
    citation_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Field-Weighted Citation Impact from OpenAlex (float, can be > 1.0).
    fwci: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Where the object was first discovered — one of ORIGIN_VALUES.
    origin: Mapped[str] = mapped_column(Text, nullable=False)

    # Raw API responses keyed by source name; merged on each enrichment pass.
    raw_responses: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)

    # Immutable creation timestamp.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )

    # Updated on every enrichment pass; used for cache-freshness checks.
    last_checked: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
