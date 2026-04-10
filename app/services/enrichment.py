"""Enrichment orchestrator — coordinates all external API calls and DB upserts.

This is the core of MESS.  The main entry point is `run_enrichment`, which implements
the full enrichment pipeline for a single PID:

  1. Cache check  — if the PID is already in PostgreSQL and its `last_checked` is within
                    the TTL window, return cached data immediately (no external API calls).

  2. Source fetch — if not cached, fetch the primary metadata from the SKG-IF source
                    endpoint, including contributors (with ORCIDs if available), topics,
                    keywords, methods, and access rights.

  3. OpenAIRE enrichment of source — query OpenAIRE Graph by DOI to get: the dedup ID,
     all merged PIDs (expands the pids array), authors with ORCIDs, domain topics
     (Fields of Science, SDG), free-text keywords, funding projects (with funder name,
     grant code, jurisdiction), and access rights.

  4. Scholexplorer — loop over every DOI in the (now-expanded) source pids array,
     collecting all dataset<->publication links.  Each link is tagged with the source
     DOI that found it (`_found_via_doi` in raw_responses) so the user can trace
     which PID produced each related object.  `upsert_relationship` uses
     ON CONFLICT DO NOTHING so duplicate edges from multiple source DOIs are safe.
     For each discovered related object: upsert basic metadata.

  5. Source OpenAlex enrichment — citation counts and FWCI for the source object,
     queried with ALL DOIs in the expanded pids list (after Scholexplorer may have
     added more).  All per-DOI results are stored in raw_responses["openalex_per_doi"]
     for provenance comparison; the highest citation count is used for `citation_count`.

  6. Related object OpenAlex enrichment — citation counts for every Scholexplorer-
     discovered related object that has a DOI.  Per-DOI results stored the same way.

  All of steps 2-6 run inside a single database transaction, committed atomically.

PostgreSQL JSONB note:
  PID lookups use the `@>` containment operator on the GIN-indexed `pids` JSONB column.
  SQLAlchemy sends the RHS as a VARCHAR string by default, which PostgreSQL rejects
  ("operator does not exist: jsonb @> character varying").  The fix is to explicitly
  cast the RHS to `JSONB` using `cast(..., JSONB)` -- see `lookup_by_pid` and
  `upsert_digital_object`.
"""

import logging
from datetime import UTC, datetime

from sqlalchemy import cast, select, text, update
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.digital_object import DigitalObject
from app.models.relationship import Relationship
from app.services.cache import is_fresh
from app.services.openaire import OpenAIREClient
from app.services.openalex import OpenAlexClient
from app.services.pid_validator import pid_to_json, validate_and_normalise
from app.services.scholexplorer import ScholexplorerClient
from app.services.source_endpoint import SourceEndpointClient

logger = logging.getLogger(__name__)


async def lookup_by_pid(session: AsyncSession, pid_type: str, pid_value: str) -> DigitalObject | None:
    """Find a DigitalObject whose `pids` array contains the given PID.

    Uses the PostgreSQL `@>` (contains) operator on the GIN-indexed `pids` JSONB column.

    IMPORTANT -- parameter binding with asyncpg and JSONB:
      The RHS of `@>` must be a `cast(Python_list, JSONB)` -- NOT a JSON string.
      asyncpg (the async PostgreSQL driver) requires Python objects (dict/list) for
      JSONB-typed parameters and serializes them natively.  Passing a pre-serialized
      JSON string causes asyncpg to encode it as a JSONB text value, which breaks the
      `@>` containment check silently (no error, zero rows returned).

    Args:
        session:   Active SQLAlchemy async session.
        pid_type:  Normalised PID type string, e.g. "doi".
        pid_value: Normalised PID value, e.g. "10.1234/example".

    Returns:
        The matching DigitalObject, or None if not found.
    """
    pid_json = pid_to_json(pid_type, pid_value)
    result = await session.execute(
        select(DigitalObject).where(
            # Pass a Python list -- asyncpg serialises it to JSONB for the @> operator.
            # Do NOT convert to a JSON string here; that breaks JSONB parameter binding.
            DigitalObject.pids.op("@>")(cast([pid_json], JSONB))
        )
    )
    return result.scalar_one_or_none()


async def upsert_digital_object(session: AsyncSession, fields: dict) -> DigitalObject:
    """Insert a new DigitalObject or update an existing one matched by PID.

    Deduplication strategy:
      Iterates through each incoming PID and queries the `pids` JSONB column using
      `@>` containment.  The first match is used as the existing record to update.
      If no match is found, a new row is inserted.

    Merge behaviour on update:
      - `pids`: union of existing and incoming (no duplicates by type+value).
      - `raw_responses`: shallow merge (incoming keys overwrite matching existing keys).
      - All other metadata fields: updated only if the incoming value is not None
        (existing non-null values are never overwritten with None).

    Args:
        session: Active SQLAlchemy async session (must be inside a transaction).
        fields:  Dict of column values; see `DigitalObject` model for keys.

    Returns:
        The upserted DigitalObject (refreshed from DB after update/insert).
    """
    incoming_pids: list[dict] = fields.get("pids", [])
    existing: DigitalObject | None = None

    # Check each incoming PID against the database -- stop at the first match.
    # Use Python list (not JSON string) for asyncpg JSONB parameter binding -- see lookup_by_pid.
    for pid_dict in incoming_pids:
        result = await session.execute(
            select(DigitalObject).where(
                DigitalObject.pids.op("@>")(cast([pid_dict], JSONB))
            )
        )
        existing = result.scalar_one_or_none()
        if existing:
            break

    if existing:
        # Merge PIDs: keep existing + add any new ones not already present.
        merged_pids = _merge_pids(existing.pids or [], incoming_pids)
        # Merge external_ids: deduplicated by source, incoming value wins.
        merged_external_ids = _merge_external_ids(
            existing.external_ids or [], fields.get("external_ids", [])
        )
        # Merge raw responses: shallow dict merge (incoming wins on key conflicts).
        merged_raw = {**(existing.raw_responses or {}), **fields.get("raw_responses", {})}

        update_fields = {
            "pids": merged_pids,
            "external_ids": merged_external_ids,
            "raw_responses": merged_raw,
            "last_checked": datetime.now(UTC),
        }
        # Only overwrite metadata fields if the incoming value is not None --
        # richer data from the source endpoint should not be replaced by a
        # sparse Scholexplorer record.
        for col in (
            "title", "titles", "creators", "keywords", "topics", "projects", "access", "methods",
            "source_local_id", "publication_date", "citation_count", "fwci",
        ):
            val = fields.get(col)
            if val is not None:
                update_fields[col] = val

        await session.execute(
            update(DigitalObject)
            .where(DigitalObject.id == existing.id)
            .values(**update_fields)
        )
        await session.flush()
        await session.refresh(existing)
        return existing

    # No existing record matched -- insert a new row.
    obj = DigitalObject(
        pids=incoming_pids,
        external_ids=fields.get("external_ids", []),
        object_type=fields.get("object_type", "other"),
        title=fields.get("title"),
        titles=fields.get("titles"),
        creators=fields.get("creators"),
        keywords=fields.get("keywords"),
        topics=fields.get("topics"),
        projects=fields.get("projects"),
        access=fields.get("access"),
        methods=fields.get("methods"),
        source_local_id=fields.get("source_local_id"),
        publication_date=fields.get("publication_date"),
        citation_count=fields.get("citation_count"),
        fwci=fields.get("fwci"),
        origin=fields.get("origin", "other"),
        raw_responses=fields.get("raw_responses", {}),
    )
    session.add(obj)
    await session.flush()
    return obj


async def upsert_relationship(
    session: AsyncSession,
    source_id: int,
    target_id: int,
    relation_type: str,
    provenance: str,
) -> None:
    """Insert a relationship edge, silently ignoring duplicates.

    Uses `ON CONFLICT DO NOTHING` on the unique constraint
    `(source_id, target_id, relation_type)`, so calling this multiple times with the
    same arguments is safe and idempotent.

    Args:
        session:       Active SQLAlchemy async session.
        source_id:     ID of the source DigitalObject (the enriched dataset).
        target_id:     ID of the target DigitalObject (related publication/software/dataset).
        relation_type: Scholix relation type string, e.g. "Cites".
        provenance:    Which API asserted this edge, e.g. "scholexplorer".
    """
    await session.execute(
        text(
            """
            INSERT INTO relationship (source_id, target_id, relation_type, provenance, created_at)
            VALUES (:source_id, :target_id, :relation_type, :provenance, now())
            ON CONFLICT (source_id, target_id, relation_type) DO NOTHING
            """
        ),
        {
            "source_id": source_id,
            "target_id": target_id,
            "relation_type": relation_type,
            "provenance": provenance,
        },
    )


async def run_enrichment(
    pid: str,
    session: AsyncSession,
    source_client: SourceEndpointClient,
    scholexplorer_client: ScholexplorerClient,
    openaire_client: OpenAIREClient,
    openalex_client: OpenAlexClient,
) -> tuple[DigitalObject, list[tuple[DigitalObject, Relationship]], bool]:
    """Run the full enrichment pipeline for a PID.

    This function implements the complete MESS workflow (see module docstring).
    All DB writes happen inside a single `session.begin()` transaction.

    Args:
        pid:                   Normalised PID value (already validated by caller).
        session:               SQLAlchemy async session (provided by FastAPI dependency).
        source_client:         SKG-IF source endpoint client.
        scholexplorer_client:  Scholexplorer v3 client.
        openaire_client:       OpenAIRE Graph API client.
        openalex_client:       OpenAlex client.

    Returns:
        Tuple of:
          - source_obj:  The DigitalObject for the requested PID.
          - related:     List of (DigitalObject, Relationship) pairs for related objects.
          - was_cached:  True if the result was served from the PostgreSQL cache.
    """
    pid_type, pid_value = validate_and_normalise(pid)

    async with session.begin():
        # -- Step 1: Cache check --------------------------------------------------
        source_obj = await lookup_by_pid(session, pid_type, pid_value)

        if source_obj and is_fresh(source_obj.last_checked, settings.mess_cache_ttl_hours):
            related = await _load_relationships(session, source_obj.id)
            return source_obj, related, True

        # -- Step 2: Fetch from source endpoint -----------------------------------
        if source_obj is None:
            product_data = await source_client.fetch_by_pid(pid_value)
            if product_data:
                fields = source_client.parse_product(product_data)
                # Ensure the queried PID is in the stored pids array even if the
                # source endpoint identifies it differently (e.g. uppercase "DOI").
                pid_entry = pid_to_json(pid_type, pid_value)
                if pid_entry not in fields["pids"]:
                    fields["pids"].append(pid_entry)
            else:
                # Source endpoint has no record -- create a minimal stub so we can
                # still store Scholexplorer relationships against this PID.
                fields = {
                    "pids": [pid_to_json(pid_type, pid_value)],
                    "object_type": "dataset",
                    "origin": "source_endpoint",
                    "raw_responses": {},
                }
            source_obj = await upsert_digital_object(session, fields)

        # -- Step 3: OpenAIRE enrichment of source --------------------------------
        # Query OpenAIRE Graph by DOI to expand the source pids array (OpenAIRE
        # deduplicates across repositories and may know additional DOIs), store the
        # dedup ID, add authors with ORCIDs, FOS/SDG domain topics, free-text keywords,
        # funding projects (with funder name, code, jurisdiction), and access metadata.
        await _enrich_source_with_openaire(session, source_obj, openaire_client)

        # -- Step 4: Scholexplorer -- loop over all source DOIs -------------------
        # After OpenAIRE expansion source_obj.pids may include additional DOIs;
        # query Scholexplorer with each to maximise link coverage.
        # Each related object's raw_responses includes `_found_via_doi` so the user
        # can trace which source PID produced each discovered link.
        # upsert_relationship is idempotent -- duplicate edges from multiple DOIs are safe.
        scholex_target_ids: set[int] = set()

        for doi in _get_all_dois(source_obj):
            links = await scholexplorer_client.find_links(doi)
            logger.info(
                "Scholexplorer links found",
                extra={"doi": doi, "count": len(links)},
            )

            for link in links:
                parsed = scholexplorer_client.parse_link(link)
                if not parsed:
                    continue

                try:
                    # Tag the target's raw_responses with the source DOI that found it
                    # so the provenance is visible in the API response.
                    target_fields = parsed["target"]
                    target_fields["raw_responses"]["_found_via_doi"] = doi

                    target_obj = await upsert_digital_object(session, target_fields)
                    await upsert_relationship(
                        session, source_obj.id, target_obj.id, parsed["relation_type"], "scholexplorer"
                    )
                    scholex_target_ids.add(target_obj.id)
                except Exception as exc:
                    logger.warning(
                        "process_link failed",
                        extra={"error": str(exc), "doi": doi},
                    )

        # -- Step 5: OpenAlex enrichment of source --------------------------------
        # Citation counts using ALL DOIs in the (now-expanded) pids list (after
        # Scholexplorer may have contributed additional PIDs via merged records).
        # All per-DOI results are stored in raw_responses["openalex_per_doi"] so
        # the user can compare citation counts across different DOIs for the same object.
        if _get_all_dois(source_obj):
            try:
                await _enrich_openalex(session, source_obj, openalex_client)
            except Exception as exc:
                logger.warning(
                    "Enrichment sub-task failed",
                    extra={"object_id": source_obj.id, "source": "openalex", "error": str(exc)},
                )

        # -- Step 6: OpenAlex enrichment of Scholexplorer-discovered objects ------
        # Fetch citation counts for every related object found in Step 4.
        # Runs after source enrichment so it does not delay the source object's
        # citation data.  Skip objects already enriched (reachable via multiple source DOIs).
        enriched_ids: set[int] = set()
        for target_id in scholex_target_ids:
            # Reload from session to get the current pids (may have been updated by upsert).
            result = await session.execute(
                select(DigitalObject).where(DigitalObject.id == target_id)
            )
            target_obj = result.scalar_one_or_none()
            if target_obj and target_obj.id not in enriched_ids and _get_all_dois(target_obj):
                try:
                    await _enrich_openalex(session, target_obj, openalex_client)
                    enriched_ids.add(target_obj.id)
                except Exception as exc:
                    logger.warning(
                        "process_link failed",
                        extra={"error": str(exc), "object_id": target_obj.id},
                    )

    # Transaction committed -- reload the source object and its relationships.
    await session.refresh(source_obj)
    related = await _load_relationships(session, source_obj.id)
    return source_obj, related, False


async def _load_relationships(
    session: AsyncSession, source_id: int
) -> list[tuple[DigitalObject, Relationship]]:
    """Load all outgoing relationships and their target objects for a source DigitalObject.

    Returns a list of (target_object, relationship_edge) pairs, ordered by DB insertion.
    """
    result = await session.execute(
        select(DigitalObject, Relationship)
        .join(Relationship, Relationship.target_id == DigitalObject.id)
        .where(Relationship.source_id == source_id)
    )
    return list(result.all())


async def _enrich_source_with_openaire(
    session: AsyncSession, obj: DigitalObject, client: OpenAIREClient
) -> None:
    """Query OpenAIRE by DOI, merge extra PIDs, dedup ID, and metadata into the source object.

    Tries each DOI in the object's pids list and stops at the first successful hit.
    Merges incoming pids and external_ids rather than overwriting them.
    """
    for doi in _get_all_dois(obj):
        try:
            product = await client.fetch_by_doi(doi)
        except Exception as exc:
            logger.warning(
                "Enrichment sub-task failed",
                extra={"object_id": obj.id, "source": "openaire", "error": str(exc)},
            )
            continue

        if not product:
            continue

        updates = client.parse_product(product)

        # Merge PIDs and external_ids rather than overwriting.
        updates["pids"] = _merge_pids(obj.pids or [], updates.get("pids", []))
        updates["external_ids"] = _merge_external_ids(
            obj.external_ids or [], updates.get("external_ids", [])
        )
        # Merge creators: add OpenAIRE authors (with ORCIDs) alongside source_endpoint
        # contributors.  Deduplicates by ORCID first, then by name.
        if updates.get("creators"):
            updates["creators"] = _merge_creators(obj.creators, updates["creators"])
        # Merge projects: union by project ID so re-enrichment is idempotent.
        if updates.get("projects"):
            updates["projects"] = _merge_projects(obj.projects or [], updates["projects"])
        merged_raw = {**(obj.raw_responses or {}), "openaire": product}
        updates["raw_responses"] = merged_raw

        await session.execute(
            update(DigitalObject).where(DigitalObject.id == obj.id).values(**updates)
        )
        await session.flush()
        await session.refresh(obj)
        return  # stop at first DOI that returns a result


async def _enrich_openalex(
    session: AsyncSession, obj: DigitalObject, client: OpenAlexClient
) -> None:
    """Fetch OpenAlex citation metrics for `obj`, trying all DOIs in its pids list.

    Datasets often have multiple DOIs (different versions, deposits, or aliases).
    OpenAlex may only index some of them, and different DOIs can have different
    citation counts.  We try every DOI and:
    - Keep the result with the highest citation count for `citation_count` / `fwci`.
    - Store ALL per-DOI results in raw_responses["openalex_per_doi"] so the user
      can compare citation counts across DOIs (provenance transparency).
    - Record which DOI produced the winning result in raw_responses["openalex_best_doi"].
    """
    best_work: dict | None = None
    best_citations: int = -1
    best_doi: str | None = None
    per_doi_results: dict[str, dict] = {}

    for doi in _get_all_dois(obj):
        work = await client.fetch_by_doi(doi)
        if work:
            citations = work.get("cited_by_count") or 0
            per_doi_results[doi] = {
                "cited_by_count": citations,
                "fwci": work.get("fwci"),
                "id": work.get("id"),
            }
            if citations > best_citations:
                best_citations = citations
                best_work = work
                best_doi = doi
            logger.debug(
                "OpenAlex DOI hit",
                extra={"object_id": obj.id, "doi": doi, "citations": citations},
            )

    if not best_work:
        return

    updates = client.parse_work(best_work)
    merged_raw = {
        **(obj.raw_responses or {}),
        "openalex": best_work,
        "openalex_per_doi": per_doi_results,
        "openalex_best_doi": best_doi,
    }
    updates["raw_responses"] = merged_raw
    # Merge external_ids so the openalex entry doesn't overwrite existing entries
    # (e.g. an openaire ID already stored from the OpenAIRE enrichment step).
    updates["external_ids"] = _merge_external_ids(
        obj.external_ids or [], updates.get("external_ids", [])
    )
    await session.execute(
        update(DigitalObject).where(DigitalObject.id == obj.id).values(**updates)
    )


def _get_all_dois(obj: DigitalObject) -> list[str]:
    """Return all DOI values from an object's PID list (deduped, in order).

    Datasets frequently have multiple DOIs -- different versions, repository deposits,
    or publisher aliases.  Used by `_enrich_openalex` to try each DOI until the best
    OpenAlex match is found.
    """
    seen: set[str] = set()
    dois: list[str] = []
    for pid in obj.pids or []:
        if pid.get("type", "").lower() == "doi":
            val = pid["value"]
            if val not in seen:
                seen.add(val)
                dois.append(val)
    return dois


def _get_external_id(obj: DigitalObject, source: str) -> str | None:
    """Return the ID string for a given source from an object's external_ids list.

    Args:
        obj:    DigitalObject whose `external_ids` to search.
        source: Source system name, e.g. "openaire" or "openalex".

    Returns:
        The ID string for that source, or None if not present.
    """
    for entry in (obj.external_ids or []):
        if entry.get("source") == source:
            return entry.get("id")
    return None


def _merge_external_ids(existing: list[dict], incoming: list[dict]) -> list[dict]:
    """Union two external_ids lists, deduplicating by source.

    The `source` field is the dedup key -- only one entry per source is kept,
    and incoming values overwrite existing ones for the same source.

    Args:
        existing: Current `external_ids` list from the DB.
        incoming: New entries to merge in.

    Returns:
        Merged list with one entry per source, preserving order of first appearance.
    """
    merged: dict[str, str] = {e["source"]: e["id"] for e in existing}
    for e in incoming:
        if e.get("source") and e.get("id"):
            merged[e["source"]] = e["id"]
    return [{"source": s, "id": i} for s, i in merged.items()]


def _merge_creators(
    existing: list[dict] | None, incoming: list[dict] | None
) -> list[dict] | None:
    """Union two creator lists, deduplicating by ORCID first, then by name.

    Preserves all existing entries.  Incoming entries are appended only if they
    introduce a new ORCID or a new name not already present.  This prevents
    OpenAIRE authors from overwriting source-endpoint contributors while still
    adding any new people or ORCIDs discovered through OpenAIRE.
    """
    if not incoming:
        return existing
    if not existing:
        return incoming
    seen_orcids: set[str] = {c["orcid"] for c in existing if c.get("orcid")}
    seen_names: set[str] = {c.get("name", "").lower() for c in existing}
    merged = list(existing)
    for c in incoming:
        orcid = c.get("orcid")
        name = c.get("name", "").lower()
        if orcid and orcid in seen_orcids:
            continue
        if not orcid and name and name in seen_names:
            continue
        merged.append(c)
        if orcid:
            seen_orcids.add(orcid)
        if name:
            seen_names.add(name)
    return merged


def _merge_projects(existing: list[dict], incoming: list[dict]) -> list[dict]:
    """Union two project lists, deduplicating by project `id`.

    Incoming values overwrite existing entries for the same project ID so that
    richer metadata (e.g. funder details added in a later enrichment pass) takes
    precedence.
    """
    merged: dict[str, dict] = {p["id"]: p for p in existing if p.get("id")}
    for p in incoming:
        if p.get("id"):
            merged[p["id"]] = p
    return list(merged.values())


def _merge_pids(existing: list[dict], incoming: list[dict]) -> list[dict]:
    """Union two PID lists, deduplicating by (type, value) pair.

    Normalises type to lowercase before comparing so that PIDs from different
    ingestion paths (e.g. source endpoint returning "DOI" vs Scholexplorer returning
    "doi") are treated as identical and not duplicated.

    Preserves ordering: existing PIDs come first, new ones are appended.
    All returned entries have their type lowercased.
    """
    # Normalise existing entries and seed the seen set.
    normalised: list[dict] = [{"type": p["type"].lower(), "value": p["value"]} for p in existing]
    seen = {(p["type"], p["value"]) for p in normalised}
    merged = list(normalised)
    for p in incoming:
        key = (p["type"].lower(), p["value"])
        if key not in seen:
            merged.append({"type": p["type"].lower(), "value": p["value"]})
            seen.add(key)
    return merged
