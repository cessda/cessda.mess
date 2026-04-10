#!/usr/bin/env python3
"""Export all digital objects and relationships from PostgreSQL to an RDF N-Triples file.

Standalone script — decoupled from the FastAPI runtime. Run manually to refresh the
knowledge graph before rebuilding the QLever index.

Usage (inside the mess-api container):
    python scripts/export_triples.py [--output path] [--format nt|ttl]

Example:
    docker compose run --rm mess-api python scripts/export_triples.py

The output file is then consumed by QLever's IndexBuilderMain to build a binary index.
QLever and the mess-api container share the same Docker volume at /export.

SKG-IF profile mappings are defined as top-level dicts below — edit them to adjust
how PostgreSQL columns map to RDF predicates without touching the graph-building logic.
"""

import argparse
import logging
import sys
from pathlib import Path

from rdflib import RDF, Graph, Literal, Namespace, URIRef
from rdflib.term import BNode
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

# Ensure the app package is importable when the script is run from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings
from app.models.digital_object import DigitalObject
from app.models.relationship import Relationship

logger = logging.getLogger(__name__)

# ── Namespace declarations ────────────────────────────────────────────────────

SKG = Namespace("https://w3id.org/skg-if/ontology/")
DCTERMS = Namespace("http://purl.org/dc/terms/")
SCHEMA = Namespace("https://schema.org/")

# ── Explicit SKG-IF mappings — edit here to adjust the graph schema ───────────

# digital_object.object_type → list of RDF class URIs to assign
OBJECT_TYPE_CLASS: dict[str, list[URIRef]] = {
    "dataset":     [SKG.Dataset,     SKG.ResearchProduct],
    "publication": [SKG.Publication, SKG.ResearchProduct],
    "software":    [SKG.Software,    SKG.ResearchProduct],
    "other":       [SKG.ResearchProduct],
}

# relationship.relation_type → SKG-IF predicate URI
RELATION_PREDICATES: dict[str, URIRef] = {
    "Cites":          SKG.cites,
    "IsRelatedTo":    SKG.isRelatedTo,
    "IsSupplementTo": SKG.isSupplementTo,
    "References":     SKG.references,
    "IsNewVersionOf": SKG.isNewVersionOf,
    "IsPartOf":       SKG.isPartOf,
    "HasPart":        SKG.hasPart,
    "IsDerivedFrom":  SKG.isDerivedFrom,
    "IsSourceOf":     SKG.isSourceOf,
}

# pid.type → URI prefix (DOI is checked first; for other types the prefix is prepended)
PID_URI_PATTERNS: dict[str, str] = {
    "doi":    "https://doi.org/",
    "handle": "urn:handle:",
    "urn":    "",     # URNs are self-describing; use verbatim
    "ark":    "ark:",
}

# external_ids[].source → SKG-IF predicate for the external system identifier literal
EXTERNAL_ID_PREDICATES: dict[str, URIRef] = {
    "openaire": SKG.openAIREId,
    "openalex": SKG.openAlexId,
}


# ── URI generation ────────────────────────────────────────────────────────────

def _primary_uri(obj: DigitalObject) -> URIRef:
    """Derive a stable, dereferenceable URI for a DigitalObject from its PIDs.

    Priority: DOI (produces https://doi.org/ linked-data URI) → first known PID type
    using PID_URI_PATTERNS → urn:{type}:{value} for unknown types → internal fallback.
    """
    for pid in obj.pids or []:
        if pid.get("type") == "doi":
            return URIRef(f"https://doi.org/{pid['value']}")
    for pid in obj.pids or []:
        pid_type = pid.get("type", "")
        value = pid.get("value", "")
        if pid_type in PID_URI_PATTERNS:
            return URIRef(f"{PID_URI_PATTERNS[pid_type]}{value}")
        return URIRef(f"urn:{pid_type}:{value}")
    return URIRef(f"https://w3id.org/mess/object/{obj.id}")


# ── Graph builder ─────────────────────────────────────────────────────────────

def build_graph(objects: list[DigitalObject], relationships: list[Relationship]) -> Graph:
    """Build a flat rdflib Graph from all DB rows using the explicit SKG-IF mappings above.

    Each digital_object row → one or more rdf:type triples + title, identifiers,
    citation count, FWCI, and external system IDs.
    Each relationship row → one predicate triple between two subject URIs.
    """
    graph = Graph()
    graph.bind("skg", SKG)
    graph.bind("dcterms", DCTERMS)
    graph.bind("schema", SCHEMA)

    obj_map: dict[int, URIRef] = {}

    for obj in objects:
        uri = _primary_uri(obj)
        obj_map[obj.id] = uri

        for cls in OBJECT_TYPE_CLASS.get(obj.object_type, [SKG.ResearchProduct]):
            graph.add((uri, RDF.type, cls))

        if obj.title:
            graph.add((uri, DCTERMS.title, Literal(obj.title)))

        # Multilingual titles as language-tagged literals
        if obj.titles and isinstance(obj.titles, dict):
            for lang, vals in obj.titles.items():
                if isinstance(vals, list):
                    for v in vals:
                        if v:
                            graph.add((uri, DCTERMS.title, Literal(str(v), lang=lang)))
                elif vals:
                    graph.add((uri, DCTERMS.title, Literal(str(vals), lang=lang)))

        # Each creator/contributor → blank node with schema:name
        for creator in obj.creators or []:
            name = creator.get("name") if isinstance(creator, dict) else str(creator)
            if name:
                agent = BNode()
                graph.add((uri, DCTERMS.creator, agent))
                graph.add((agent, SCHEMA.name, Literal(name)))

        # Each PID → blank node with skg:scheme + skg:value predicates
        for pid in obj.pids or []:
            id_node = BNode()
            graph.add((uri, SKG.identifier, id_node))
            graph.add((id_node, SKG.scheme, Literal(pid.get("type", ""))))
            graph.add((id_node, SKG.value, Literal(pid.get("value", ""))))

        if obj.citation_count is not None:
            graph.add((uri, SKG.citationCount, Literal(obj.citation_count)))

        if obj.fwci is not None:
            graph.add((uri, SKG.fwci, Literal(obj.fwci)))

        for entry in obj.external_ids or []:
            predicate = EXTERNAL_ID_PREDICATES.get(entry.get("source", ""))
            if predicate and entry.get("id"):
                graph.add((uri, predicate, Literal(entry["id"])))

    for rel in relationships:
        source_uri = obj_map.get(rel.source_id)
        target_uri = obj_map.get(rel.target_id)
        if not source_uri or not target_uri:
            logger.warning("Relationship %d references unknown object — skipping", rel.id)
            continue
        predicate = RELATION_PREDICATES.get(rel.relation_type, SKG.isRelatedTo)
        graph.add((source_uri, predicate, target_uri))

    return graph


# ── CLI entry point ───────────────────────────────────────────────────────────

def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(
        description="Export PostgreSQL data to RDF for QLever indexing."
    )
    parser.add_argument(
        "--output",
        default=settings.sparql_export_path,
        help="Output file path (default: SPARQL_EXPORT_PATH env var)",
    )
    parser.add_argument(
        "--format",
        choices=["nt", "ttl"],
        default="nt",
        help="RDF serialisation format (default: nt — required by QLever indexer)",
    )
    args = parser.parse_args()

    engine = create_engine(settings.database_url_sync)
    with Session(engine) as session:
        objects = list(session.execute(select(DigitalObject)).scalars().all())
        relationships = list(session.execute(select(Relationship)).scalars().all())

    logger.info("Loaded %d objects, %d relationships from PostgreSQL", len(objects), len(relationships))

    if not objects:
        logger.warning("No objects to export — writing empty graph")

    graph = build_graph(objects, relationships)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    graph.serialize(destination=str(out_path), format=args.format)
    logger.info("Wrote %d triples to %s", len(graph), out_path)


if __name__ == "__main__":
    main()
