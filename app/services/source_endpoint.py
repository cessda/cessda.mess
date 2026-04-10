"""Client for the configurable SKG-IF source endpoint.

The source endpoint is a SKG-IF-compliant API (default: CESSDA staging) that holds
the primary research-product metadata.  It is the first stop in the enrichment pipeline:
MESS queries it to get the canonical metadata for the requested PID before calling
Scholexplorer, OpenAIRE, and OpenAlex.

Configuration:
  - `MESS_SOURCE_ENDPOINT` — base URL of the products API
  - `MESS_SOURCE_PID_FILTER` — query-string template with `{pid}` placeholder

Response format expected: SKG-IF JSON-LD with a `@graph` array of ResearchProduct nodes.
"""

import logging

import httpx

from app.config import settings
from app.services.http_utils import request_with_backoff

logger = logging.getLogger(__name__)


class SourceEndpointClient:
    """Fetches and parses product metadata from the SKG-IF source endpoint.

    One instance is created at startup (in `app/main.py`) and stored on `app.state`.
    """

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def fetch_by_pid(self, pid: str) -> dict | None:
        """Fetch the SKG-IF JSON-LD product node for a given PID.

        Constructs the request URL by substituting `{pid}` in the configured filter
        template, then returns the first ResearchProduct node from `@graph`.

        Returns:
            The first product dict from `@graph`, or None if not found / empty.
        """
        filter_param = settings.mess_source_pid_filter.replace("{pid}", pid)
        url = f"{settings.mess_source_endpoint}?{filter_param}"

        logger.info("Querying source endpoint", extra={"url": url, "pid": pid})

        response = await request_with_backoff(lambda: self._client.get(url))

        if response.status_code == 404:
            return None
        response.raise_for_status()

        data = response.json()
        graph = data.get("@graph", [])
        if not graph:
            return None
        return graph[0]

    def parse_product(self, product: dict) -> dict:
        """Extract normalised fields from an SKG-IF ResearchProduct node.

        Maps SKG-IF field names to our internal schema.  The `titles` dict may contain
        multiple languages; we prefer English but fall back to the first available language
        for the flat `title` field used in display contexts.

        Returns:
            Dict compatible with `services/enrichment.upsert_digital_object`.
        """
        identifiers = product.get("identifiers", [])
        # Normalise scheme to lowercase so PIDs from different sources dedup correctly.
        # Source endpoints sometimes return uppercase schemes ("DOI", "Handle") — without
        # normalisation these collide with Scholexplorer's lowercase equivalents and create
        # duplicate entries in the pids JSONB array.
        pids = [
            {"type": ident.get("scheme", "unknown").lower(), "value": ident.get("value", "")}
            for ident in identifiers
            if ident.get("value")
        ]

        titles_raw = product.get("titles", {})
        title = None
        if isinstance(titles_raw, dict):
            # Prefer English title for the flat `title` field.
            for lang in ("en", "eng"):
                if lang in titles_raw and titles_raw[lang]:
                    vals = titles_raw[lang]
                    title = vals[0] if isinstance(vals, list) else vals
                    break
            if title is None:
                # Fall back to the first available language.
                for vals in titles_raw.values():
                    title = vals[0] if isinstance(vals, list) else vals
                    break

        return {
            "pids": pids,
            "object_type": product.get("type", "dataset"),
            "title": title,
            "titles": titles_raw if isinstance(titles_raw, dict) else None,
            "creators": product.get("contributors"),
            "keywords": product.get("keywords"),
            "topics": product.get("topics"),
            "access": product.get("access"),
            "methods": product.get("methods"),
            # The source may use either `localIdentifier` (SKG-IF) or `local_identifier`.
            "source_local_id": product.get("localIdentifier") or product.get("local_identifier"),
            # Publication date from SKG-IF `dateIssued` (ISO 8601 or year-only string).
            "publication_date": product.get("dateIssued") or product.get("publicationDate"),
            "origin": "source_endpoint",
            "raw_responses": {"source_endpoint": product},
        }
