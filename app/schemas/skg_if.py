"""SKG-IF JSON-LD serialisation.

Builds a compliant JSON-LD document from ORM objects.  No RDF library is used on the
API response path — the document is constructed as plain Python dicts for performance.

Output format:
  SKG-IF v1.1.0 (Science Knowledge Graph Interoperability Framework)
  Context: https://w3id.org/skg-if/context/skg-if.json

Structure of the returned document:
  {
    "@context": "https://w3id.org/skg-if/context/skg-if.json",
    "@graph": [
      {
        "@type": "ResearchProduct",
        "@id": "<primary PID URI>",
        "identifiers": [...],
        "type": "dataset" | "publication" | "software" | "other",
        "title": "...",
        "cites": [{"@id": "<target URI>"}, ...],    ← outgoing relationships
        ...other metadata fields...
      },
      ...one node per related object...
    ]
  }

URI generation (`_primary_pid_uri`):
  DOI  → `https://doi.org/{value}`
  Other → `urn:{type}:{value}`
  Fallback → `urn:mess:id:{obj.id}`
"""

from app.models.digital_object import DigitalObject
from app.models.relationship import Relationship

SKG_IF_CONTEXT = "https://w3id.org/skg-if/context/skg-if.json"
SKG_IF_PRODUCT_TYPE = "ResearchProduct"


def build_json_ld(
    source: DigitalObject,
    related: list[tuple[DigitalObject, Relationship]],
) -> dict:
    """Build a SKG-IF JSON-LD document for the enrichment API response.

    The source object node includes all outgoing relationship predicates (e.g. `cites`,
    `isSupplementTo`) pointing to related objects by URI.  Related objects appear as
    separate nodes in `@graph` with their own metadata.

    Args:
        source:  The primary DigitalObject that was looked up.
        related: List of (target_object, relationship_edge) pairs from
                 `services/enrichment._load_relationships`.

    Returns:
        A JSON-serialisable dict conforming to SKG-IF v1.1.0.
    """
    graph = []

    source_node = _build_research_product(source)

    # Group relationship targets by predicate so we can emit arrays:
    # e.g. `"cites": [{"@id": "..."}, {"@id": "..."}, ...]`
    rel_map: dict[str, list[str]] = {}
    for obj, rel in related:
        predicate = _relation_type_to_predicate(rel.relation_type)
        target_uri = _primary_pid_uri(obj)
        rel_map.setdefault(predicate, []).append(target_uri)

    # Attach relationship arrays to the source node.
    for predicate, targets in rel_map.items():
        source_node[predicate] = [{"@id": t} for t in targets]

    graph.append(source_node)

    # Add each related object as a standalone node in the graph.
    for obj, _ in related:
        graph.append(_build_research_product(obj))

    return {
        "@context": SKG_IF_CONTEXT,
        "@graph": graph,
    }


def _build_research_product(obj: DigitalObject) -> dict:
    """Build a single ResearchProduct node dict from a DigitalObject.

    Only includes fields that have non-None values to keep responses clean.
    Relationship predicates are added by `build_json_ld` after this function runs.
    A `_mess` block is appended with provenance metadata so consumers can trace
    which API contributed each field value.
    """
    node: dict = {
        "@type": SKG_IF_PRODUCT_TYPE,
        "@id": _primary_pid_uri(obj),
        "identifiers": [_pid_to_identifier(p) for p in (obj.pids or [])],
        "type": obj.object_type,
    }

    if obj.title:
        node["title"] = obj.title
    if obj.titles:
        node["titles"] = obj.titles
    if obj.creators:
        node["contributors"] = obj.creators   # SKG-IF uses "contributors" for creators
    if obj.keywords:
        node["keywords"] = obj.keywords
    if obj.topics:
        node["topics"] = obj.topics
    if obj.projects:
        node["projects"] = obj.projects
    if obj.access:
        node["access"] = obj.access
    if obj.methods:
        node["methods"] = obj.methods
    if obj.citation_count is not None:
        node["citationCount"] = obj.citation_count
    if obj.fwci is not None:
        node["fwci"] = obj.fwci

    node["_mess"] = _build_mess_provenance(obj)

    return node


def _build_mess_provenance(obj: DigitalObject) -> dict:
    """Build a provenance metadata block for a DigitalObject.

    Derives provenance from `raw_responses` keys and `origin` so consumers can
    see which external APIs contributed to each field value.  Non-standard keys
    prefixed with `_mess` are intentionally outside the SKG-IF context.
    """
    raw = obj.raw_responses or {}

    # Determine which external APIs contributed data to this object.
    data_sources: list[str] = []
    if "source_endpoint" in raw:
        data_sources.append("source_endpoint")
    if "openaire" in raw:
        data_sources.append("openaire")
    if "scholexplorer" in raw:
        data_sources.append("scholexplorer")
    if "openalex" in raw:
        data_sources.append("openalex")

    provenance: dict = {
        "origin": obj.origin,
        "data_sources": data_sources,
    }

    if obj.publication_date:
        provenance["publication_date"] = obj.publication_date

    # Citation count provenance: show the best DOI used and all per-DOI counts
    # so the user can compare citation numbers across different DOIs.
    if obj.citation_count is not None:
        provenance["citation_count_source"] = "openalex"
        best_doi = raw.get("openalex_best_doi")
        if best_doi:
            provenance["citation_count_doi"] = best_doi
        per_doi = raw.get("openalex_per_doi")
        if per_doi:
            provenance["citation_count_per_doi"] = {
                doi: r.get("cited_by_count")
                for doi, r in per_doi.items()
                if r.get("cited_by_count") is not None
            }

    # For Scholexplorer-discovered related objects: which source DOI found this link.
    found_via = raw.get("_found_via_doi")
    if found_via:
        provenance["found_via_doi"] = found_via

    # Direct links to the source systems so the user can verify numbers and browse
    # the full lists themselves.  Only included when the relevant ID/DOI is present.
    source_links: dict = {}

    # Use the first DOI in pids for Scholexplorer links (both directions).
    first_doi = next(
        (p["value"] for p in (obj.pids or []) if p.get("type") == "doi"), None
    )
    if first_doi:
        source_links["scholexplorer_as_source"] = (
            f"https://api-beta.scholexplorer.openaire.eu/v3/Links?sourcePid={first_doi}"
        )
        source_links["scholexplorer_as_target"] = (
            f"https://api-beta.scholexplorer.openaire.eu/v3/Links?targetPid={first_doi}"
        )

    openalex_id = _get_external_id(obj, "openalex")
    if openalex_id:
        source_links["openalex_work"] = openalex_id  # already a full URI, e.g. https://openalex.org/W...
    cited_by_url = _get_external_id(obj, "openalex_cited_by_url")
    if cited_by_url:
        source_links["openalex_cited_by"] = cited_by_url

    openaire_id = _get_external_id(obj, "openaire")
    if openaire_id:
        source_links["openaire_explore"] = (
            f"https://explore.openaire.eu/search/dataset?datasetId={openaire_id}"
        )

    if source_links:
        provenance["source_links"] = source_links

    return provenance


def _get_external_id(obj: DigitalObject, source: str) -> str | None:
    """Return the id string for a given source from an object's external_ids list."""
    for entry in (obj.external_ids or []):
        if entry.get("source") == source:
            return entry.get("id")
    return None


def _pid_to_identifier(pid: dict) -> dict:
    """Convert an internal `{"type", "value"}` PID dict to a SKG-IF identifier node."""
    return {"scheme": pid.get("type", "unknown"), "value": pid.get("value", "")}


def _primary_pid_uri(obj: DigitalObject) -> str:
    """Derive the canonical URI for a DigitalObject's primary identifier.

    Priority: DOI (becomes `https://doi.org/...`) > first other PID > fallback ID.
    """
    for pid in obj.pids or []:
        if pid.get("type") == "doi":
            return f"https://doi.org/{pid['value']}"
    for pid in obj.pids or []:
        return f"urn:{pid.get('type', 'unknown')}:{pid.get('value', '')}"
    # Last resort: stable internal URI using the DB primary key.
    return f"urn:mess:id:{obj.id}"


def _relation_type_to_predicate(relation_type: str) -> str:
    """Map a Scholix relation type string to its SKG-IF JSON-LD predicate name.

    Unmapped relation types fall back to "isRelatedTo" (the most general SKG-IF predicate).
    The full Scholix vocabulary is defined in `models/relationship.RELATION_TYPES`;
    only the subset with a direct SKG-IF mapping is listed here.
    """
    mapping = {
        "Cites": "cites",
        "IsSupplementTo": "isSupplementTo",
        "IsRelatedTo": "isRelatedTo",
        "References": "references",
        "IsNewVersionOf": "isNewVersionOf",
        "IsPartOf": "isPartOf",
        "HasPart": "hasPart",
        "IsDerivedFrom": "isDerivedFrom",
        "IsSourceOf": "isSourceOf",
    }
    return mapping.get(relation_type, "isRelatedTo")
