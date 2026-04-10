"""ORM model for the `relationship` table.

Each row represents a directed edge in the knowledge graph:
  source (dataset) → [relation_type] → target (publication / software / dataset)

Edges are discovered by Scholexplorer and stored with `provenance = "scholexplorer"`.
The `UNIQUE(source_id, target_id, relation_type)` constraint means the same pair of
objects can be linked by multiple relation types (e.g. both "Cites" and "IsRelatedTo"),
but the same typed link is never duplicated.

Relation type vocabulary comes from Scholix 3.0 / DataCite metadata schema.
RDF materialization maps these to SKG-O predicates in `scripts/export_triples.py`.
"""

from datetime import UTC, datetime

from sqlalchemy import DateTime, ForeignKey, Integer, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base

# Relation types sourced from Scholix 3.0 / DataCite.  Mapped to SKG-O predicates in
# `scripts/export_triples.RELATION_PREDICATES`.  Unknown values from the API fall back to
# "IsRelatedTo".
RELATION_TYPES = (
    "Cites",
    "IsSupplementTo",
    "IsRelatedTo",
    "References",
    "IsNewVersionOf",
    "IsPartOf",
    "HasPart",
    "IsDocumentedBy",
    "IsCompiledBy",
    "IsVariantFormOf",
    "IsDerivedFrom",
    "IsSourceOf",
    "IsObsoletedBy",
    "IsReferencedBy",
    "IsSupplementedBy",
)

# Which external service asserted this edge.
PROVENANCE_VALUES = ("scholexplorer", "openaire", "openalex")


class Relationship(Base):
    """A directed edge between two DigitalObjects.

    Created by `services/enrichment.upsert_relationship` using ON CONFLICT DO NOTHING,
    so duplicate inserts are safe.
    Materialized as RDF triples by `scripts/export_triples.build_graph`.
    """

    __tablename__ = "relationship"
    __table_args__ = (
        # Prevents duplicate typed edges; multiple relation types between the same
        # pair are allowed (e.g. a paper can both Cite and be a Supplement to a dataset).
        UniqueConstraint("source_id", "target_id", "relation_type", name="uq_relationship_edge"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # The "from" node — typically the source dataset being enriched.
    source_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("digital_object.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # The "to" node — the related publication, dataset, or software.
    target_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("digital_object.id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Scholix/DataCite relation type string — one of RELATION_TYPES above.
    relation_type: Mapped[str] = mapped_column(Text, nullable=False)

    # Which API asserted this edge — one of PROVENANCE_VALUES above.
    provenance: Mapped[str] = mapped_column(Text, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
