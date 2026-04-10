"""OpenAlex API client.

Used to enrich related objects with citation metrics, field-weighted citation impact
(FWCI), topic classifications, and open-access URLs.

API reference: https://docs.openalex.org/

IMPORTANT — `include_xpac=true` parameter:
  This parameter must always be included.  Without it, DataCite-registered datasets
  are silently excluded from OpenAlex responses, returning 404 for DOIs that do exist
  in the index.  There is no error — the API simply acts as if the record doesn't exist.

Authentication:
  - Without a key: "polite pool", limited to ~100 API credits/day per IP.
  - With `OPENALEX_API_KEY`: higher limits.
    Obtain at https://openalex.org/
"""

import logging

import httpx

from app.config import settings
from app.services.http_utils import request_with_backoff

logger = logging.getLogger(__name__)


class OpenAlexClient:
    """Fetches and parses Work records from the OpenAlex API.

    One instance is created at startup (in `app/main.py`) and stored on `app.state`.
    Only called for objects that have a DOI PID (extracted by `_get_doi` in enrichment.py).
    """

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def fetch_by_doi(self, doi: str) -> dict | None:
        """Fetch an OpenAlex Work record by DOI.

        Args:
            doi: Bare DOI string, e.g. "10.1234/example" (no URL prefix).

        Returns:
            Work dict from the API, or None on 404.

        Note:
            `include_xpac=true` is mandatory for DataCite dataset records to be
            returned.  Without it, DataCite DOIs silently return 404.
        """
        url = f"{settings.openalex_api_url}/works/doi:{doi}"
        params: dict = {"include_xpac": "true"}  # Do NOT remove — see module docstring
        if settings.openalex_api_key:
            params["api_key"] = settings.openalex_api_key

        logger.info("Querying OpenAlex", extra={"doi": doi})

        response = await request_with_backoff(
            lambda: self._client.get(url, params=params)
        )

        if response.status_code == 404:
            return None
        response.raise_for_status()

        return response.json()

    def parse_work(self, work: dict) -> dict:
        """Extract citation metrics, topics, keywords, and access info from an OpenAlex Work.

        Returns:
            Partial update dict compatible with `upsert_digital_object`.
            Keys with None values are filtered out so they don't overwrite existing data.
        """
        openalex_id = work.get("id")
        cited_by_url = work.get("cited_by_api_url")
        updates: dict = {
            "citation_count": work.get("cited_by_count"),
            "fwci": work.get("fwci"),
            # Store the canonical OpenAlex URI and the ready-made cited-by API URL so
            # the response layer can surface direct links without re-parsing raw_responses.
            "external_ids": [
                *([{"source": "openalex", "id": openalex_id}] if openalex_id else []),
                *([{"source": "openalex_cited_by_url", "id": cited_by_url}] if cited_by_url else []),
            ],
        }

        # OpenAlex topic classifications (more granular than CESSDA subject headings).
        topics = work.get("topics", [])
        if topics:
            updates["topics"] = [
                {
                    "id": t.get("id"),
                    "label": t.get("display_name"),
                    "scheme": "openalex_topic",
                }
                for t in topics
                if t.get("display_name")
            ]

        # Free-text keywords from the paper itself.
        keywords = work.get("keywords", [])
        if keywords:
            updates["keywords"] = [k.get("display_name") for k in keywords if k.get("display_name")]

        # Open-access status and URL.
        open_access = work.get("open_access", {})
        if open_access:
            updates["access"] = {
                "type": "open" if open_access.get("is_oa") else "restricted",
                "url": open_access.get("oa_url"),
            }

        # Strip None values to avoid overwriting richer data from earlier enrichment steps.
        return {k: v for k, v in updates.items() if v is not None}
