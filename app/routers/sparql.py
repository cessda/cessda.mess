"""GET /sparql — read-only SPARQL 1.1 query proxy.

Proxies SELECT, CONSTRUCT, and ASK queries to the QLever SPARQL server.
Acts as a logged gateway in front of direct QLever access.

Two ways to reach the SPARQL endpoint:
  1. Via this proxy (logged):   GET http://localhost:8000/sparql?query=...
  2. Directly on QLever:        GET http://localhost:7001/?query=...

The proxy forwards the client's `Accept` header to QLever so that the response
format (JSON, XML, Turtle, etc.) is determined by the caller.

Accepts both GET and POST (application/x-www-form-urlencoded) so that standard
SPARQL clients including YASGUI can communicate with it.
Returns HTTP 503 if QLever is not running (index not yet built or server not started).
"""

import logging

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response

from app.config import settings

logger = logging.getLogger(__name__)
router = APIRouter()


async def _run_sparql_query(query: str, accept: str) -> Response:
    """Forward a SPARQL query to QLever and return the raw response."""
    try:
        async with httpx.AsyncClient(base_url=settings.sparql_query_url) as client:
            resp = await client.get(
                "/",
                params={"query": query},
                headers={"Accept": accept},
                timeout=30.0,
            )
    except httpx.RequestError as exc:
        logger.error("QLever SPARQL request failed: %s", exc)
        raise HTTPException(
            status_code=503,
            detail="SPARQL endpoint unavailable — QLever may not be running or indexed yet",
        ) from exc

    return Response(
        content=resp.content,
        status_code=resp.status_code,
        media_type=resp.headers.get("content-type", "application/sparql-results+json"),
    )


@router.get("/sparql")
async def sparql_proxy_get(
    query: str = Query(..., description="SPARQL 1.1 SELECT/CONSTRUCT/ASK query string"),
    request: Request = None,
) -> Response:
    """Proxy a SPARQL GET query to QLever."""
    accept = request.headers.get("Accept", "application/sparql-results+json")
    return await _run_sparql_query(query, accept)


@router.post("/sparql")
async def sparql_proxy_post(request: Request) -> Response:
    """Proxy a SPARQL POST query (application/x-www-form-urlencoded) to QLever.

    Standard SPARQL clients (e.g. YASGUI) send long queries as POST to avoid
    URL length limits.  The body must contain a `query` form field.
    Parsed manually to avoid requiring the python-multipart dependency.
    """
    from urllib.parse import parse_qs
    raw = await request.body()
    params = parse_qs(raw.decode("utf-8", errors="replace"))
    query_list = params.get("query")
    if not query_list:
        raise HTTPException(status_code=400, detail="Missing 'query' form field")
    accept = request.headers.get("Accept", "application/sparql-results+json")
    return await _run_sparql_query(query_list[0], accept)
