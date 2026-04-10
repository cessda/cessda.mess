"""FastAPI application factory and startup/shutdown lifecycle.

One shared `httpx.AsyncClient` is created per external service during startup and
stored on `app.state`.  Route handlers access them via `request.app.state.*`.
Using shared clients (rather than creating one per request) gives us connection
pooling, persistent TCP connections, and a single point for timeout configuration.

`app.state` keys (all set during lifespan startup):
  - `source_client`        — SourceEndpointClient (SKG-IF products API)
  - `scholexplorer_client` — ScholexplorerClient (Scholix link discovery)
  - `openaire_client`      — OpenAIREClient (funder/access metadata)
  - `openalex_client`      — OpenAlexClient (citation metrics, topics)

The SPARQL proxy (/sparql) and health check create short-lived httpx clients pointing
to `settings.sparql_query_url` (QLever) on each request — QLever is optional and may
not be running, so we do not hold a persistent connection to it.

All config comes from `app.config.settings` (environment variables).
"""

import logging
import logging.config
from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.routers import admin, enrich, health, objects, sparql, status
from app.services.openaire import OpenAIREClient
from app.services.openalex import OpenAlexClient
from app.services.scholexplorer import ScholexplorerClient
from app.services.source_endpoint import SourceEndpointClient


def _configure_logging() -> None:
    """Set up structured JSON logging to stdout.

    All log output is emitted as newline-delimited JSON, suitable for ingestion by
    log aggregators (Elasticsearch, Loki, etc.).  The log level is controlled by
    `settings.log_level` (env var `LOG_LEVEL`).

    To read logs in human-readable form during development:
        docker-compose logs -f mess-api | jq -r '\"\\(.asctime) \\(.levelname): \\(.message)\"'
    """
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "json": {
                "()": "pythonjsonlogger.jsonlogger.JsonFormatter",
                "format": "%(asctime)s %(levelname)s %(name)s %(message)s",
            }
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "json",
                "stream": "ext://sys.stdout",
            }
        },
        "root": {"level": settings.log_level.upper(), "handlers": ["console"]},
    }
    logging.config.dictConfig(log_config)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage application startup and graceful shutdown.

    Everything before `yield` runs once on startup; everything after `yield` runs
    on shutdown (including Ctrl-C and SIGTERM from Docker).

    All HTTP clients share a 30-second read timeout and 5-second connect timeout.
    The OpenAIRE clients (Scholexplorer + Graph API) share an Authorization header
    when `OPENAIRE_ACCESS_TOKEN` is set.
    """
    _configure_logging()
    logger = logging.getLogger(__name__)
    logger.info("Starting %s", settings.mess_service_name)

    # Build Authorization header for OpenAIRE services if a token is configured.
    openaire_headers = {}
    if settings.openaire_access_token:
        openaire_headers["Authorization"] = f"Bearer {settings.openaire_access_token}"

    timeout = httpx.Timeout(30.0, connect=5.0)

    # One shared httpx client per external service — enables connection pooling.
    source_http = httpx.AsyncClient(
        base_url=settings.mess_source_endpoint,
        timeout=timeout,
        follow_redirects=True,
    )
    scholexplorer_http = httpx.AsyncClient(
        headers=openaire_headers,
        timeout=timeout,
        follow_redirects=True,
    )
    openaire_http = httpx.AsyncClient(
        base_url=settings.openaire_api_url,
        headers=openaire_headers,
        timeout=timeout,
        follow_redirects=True,
    )
    openalex_http = httpx.AsyncClient(
        base_url=settings.openalex_api_url,
        timeout=timeout,
        follow_redirects=True,
    )

    # Wrap clients in service objects and attach to app.state.
    app.state.source_client = SourceEndpointClient(source_http)
    app.state.scholexplorer_client = ScholexplorerClient(scholexplorer_http)
    app.state.openaire_client = OpenAIREClient(openaire_http)
    app.state.openalex_client = OpenAlexClient(openalex_http)

    logger.info("All HTTP clients initialised")

    yield  # Application runs here

    # ── Graceful shutdown: close all HTTP connections ──────────────────────
    for client in (source_http, scholexplorer_http, openaire_http, openalex_http):
        await client.aclose()

    logger.info("Shutdown complete")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.

    Registers all routers.  Called once at module load time; the resulting `app`
    object is what Uvicorn serves.
    """
    app = FastAPI(
        title=settings.mess_service_name,
        description="Metadata Enrichment Semantic Service — SKG-IF enrichment and SPARQL graph.",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.include_router(health.router)
    app.include_router(enrich.router)
    app.include_router(status.router)
    app.include_router(sparql.router)
    app.include_router(admin.router)
    app.include_router(objects.router)

    app.mount("/static", StaticFiles(directory="app/static"), name="static")

    @app.get("/")
    async def root():
        return RedirectResponse(url="/static/browse.html")

    return app


app = create_app()
