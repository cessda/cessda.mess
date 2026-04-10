"""POST /enrich — main enrichment endpoint.

Accepts a PID query parameter, runs the full enrichment pipeline (or returns cached
data), and returns a SKG-IF JSON-LD document.

Flow:
  1. Validate and normalise the PID (HTTP 400 on invalid format).
  2. Delegate to `services/enrichment.run_enrichment` which handles cache + pipeline.
  3. Serialise the result to SKG-IF JSON-LD via `schemas/skg_if.build_json_ld`.
  4. Return `EnrichResponse(pid, cached, data)`.

Error handling:
  Any unhandled exception from the enrichment pipeline is caught and re-raised as
  HTTP 502 (bad gateway — downstream service failure), not 500.
"""

import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.schemas.enrich import EnrichResponse
from app.schemas.skg_if import build_json_ld
from app.services.enrichment import run_enrichment
from app.services.pid_validator import validate_and_normalise

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/enrich", response_model=EnrichResponse)
async def enrich(
    pid: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> EnrichResponse:
    """Enrich a research dataset by PID and return SKG-IF JSON-LD.

    Checks the PostgreSQL cache first.  If the object is fresh (within TTL), returns
    cached data immediately.  Otherwise, calls Scholexplorer → OpenAIRE → OpenAlex
    and stores results before responding.

    Args:
        pid: Persistent identifier in any supported format:
             DOI (`10.xxx/...` or `https://doi.org/...`), Handle (`NNN/...`),
             URN:NBN (`urn:nbn:xx:...`), ARK (`ark:/NNNNN/...`).

    Returns:
        `{"pid": "...", "cached": bool, "data": {<SKG-IF JSON-LD>}}`

    Raises:
        400: PID format not recognised.
        502: Enrichment pipeline failed (downstream API error or DB issue).
    """
    try:
        pid_type, pid_value = validate_and_normalise(pid)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    source_client = request.app.state.source_client
    scholexplorer_client = request.app.state.scholexplorer_client
    openaire_client = request.app.state.openaire_client
    openalex_client = request.app.state.openalex_client

    try:
        source_obj, related, was_cached = await run_enrichment(
            pid=pid_value,
            session=session,
            source_client=source_client,
            scholexplorer_client=scholexplorer_client,
            openaire_client=openaire_client,
            openalex_client=openalex_client,
        )
    except Exception as exc:
        logger.error("Enrichment failed for PID %s: %s", pid_value, exc, exc_info=True)
        raise HTTPException(status_code=502, detail=f"Enrichment failed: {exc}") from exc

    json_ld = build_json_ld(source_obj, related)

    logger.info(
        "Enrichment complete",
        extra={"pid": pid_value, "cached": was_cached, "related_count": len(related)},
    )

    return EnrichResponse(pid=pid_value, cached=was_cached, data=json_ld)
