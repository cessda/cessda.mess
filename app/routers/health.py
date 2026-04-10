"""GET /health — liveness and readiness probe.

Checks connectivity to PostgreSQL and, optionally, the QLever SPARQL server.
Intended for Docker/Kubernetes health checks and uptime monitors.

Response codes:
  200 — PostgreSQL reachable (SPARQL status does not affect overall health)
  503 — PostgreSQL unreachable, `{"status": "degraded", ...}`

SPARQL status values:
  "ok"          — QLever is running and responding
  "unavailable" — QLever is not reachable (not started yet or index not built) — not an error
  "error"       — QLever returned an unexpected 5xx response

PostgreSQL check: executes `SELECT 1`.
SPARQL check: GET / on the QLever server (short 3-second timeout; failure = unavailable).
"""

import logging

import httpx
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.config import settings
from app.database import get_session
from app.schemas.health import HealthResponse

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse | JSONResponse:
    """Liveness/readiness probe — checks PostgreSQL and QLever SPARQL connectivity.

    Returns HTTP 200 if PostgreSQL is reachable.  QLever unavailability reports
    sparql="unavailable" but does not degrade the overall status — QLever is optional
    and may not be running before the index has been built.
    """
    postgres_status = "ok"
    sparql_status = "unavailable"

    try:
        async for session in get_session():
            await session.execute(text("SELECT 1"))
            break
    except Exception as exc:
        logger.error("PostgreSQL health check failed: %s", exc)
        postgres_status = "error"

    try:
        async with httpx.AsyncClient(base_url=settings.sparql_query_url) as client:
            resp = await client.get("/", timeout=3.0)
            sparql_status = "error" if resp.status_code >= 500 else "ok"
    except Exception:
        sparql_status = "unavailable"

    # Overall status reflects only PostgreSQL — QLever is optional.
    overall = "ok" if postgres_status == "ok" else "degraded"
    result = HealthResponse(status=overall, postgres=postgres_status, sparql=sparql_status)

    if overall != "ok":
        return JSONResponse(status_code=503, content=result.model_dump())

    return result
