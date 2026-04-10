"""OpenAIRE Graph API client.

Used to enrich the **source object** with extra metadata: funder/project associations,
access-rights information, keyword subjects, and additional merged PIDs.

API reference: https://graph.openaire.eu/docs/
Version: v2 (https://api.openaire.eu/graph/v2)

Authentication:
  - Without a token: anonymous access, limited to ~60 requests/hour per IP.
  - With `OPENAIRE_ACCESS_TOKEN`: OAuth 2.0 bearer token, higher limits.
    Obtain at https://develop.openaire.eu/

Lookup strategy:
  Call `fetch_by_doi` with a DOI; the response contains the dedup record including
  all merged PIDs (under `pids`) and the dedup ID (under `id`).  One call is enough —
  no separate dedup-ID lookup needed.
"""

import logging

import httpx

from app.config import settings
from app.services.http_utils import request_with_backoff

logger = logging.getLogger(__name__)


class OpenAIREClient:
    """Fetches and parses enrichment data from the OpenAIRE Graph v2 API.

    One instance is created at startup (in `app/main.py`) and stored on `app.state`.
    Only called for the source object (not for Scholexplorer-discovered related objects).
    """

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def fetch_by_doi(self, doi: str) -> dict | None:
        """Fetch a research product from the OpenAIRE Graph by DOI.

        Uses `GET /researchProducts?pid={doi}&pageSize=1`.  The `pid` query parameter
        accepts raw DOI values (e.g. "10.4232/1.14377") without any scheme prefix.
        The response wraps results in `{"header": {...}, "results": [...]}`.

        Returns:
            Product dict (same structure as a direct dedup-ID lookup), or None if
            OpenAIRE has no record for this DOI.
        """
        url = f"{settings.openaire_api_url}/researchProducts"
        logger.info("Querying OpenAIRE Graph by DOI", extra={"doi": doi})

        response = await request_with_backoff(
            lambda: self._client.get(url, params={"pid": doi, "pageSize": 1})
        )

        if response.status_code == 404:
            return None
        response.raise_for_status()

        results = response.json().get("results") or []
        return results[0] if results else None

    def parse_product(self, product: dict) -> dict:
        """Extract enrichment fields from an OpenAIRE Graph v2 research product.

        Extracts:
        - `pids`: all merged PIDs from the deduplicated record (e.g. multiple DOIs for
          different dataset versions).  Used to expand the source object's pids array.
        - `external_ids`: the OpenAIRE dedup ID (stored as `{"source": "openaire", ...}`).
        - `creators`: authors with ORCIDs from the `authors` array.
        - `keywords`: free-text subjects (scheme="keyword" or untyped).
        - `topics`: domain/field subjects (scheme="fos" Fields of Science, scheme="sdg"
          Sustainable Development Goals).  Stored separately from free-text keywords.
        - `projects`: funding project metadata (id, code, title, funder name, funder short
          name, jurisdiction).  Stored in its own column — NOT in `topics`.
        - `access`: best access-right label.

        Returns:
            Partial update dict compatible with `upsert_digital_object`.
            Only non-empty fields are included.
        """
        updates: dict = {}

        # Merged PIDs from OpenAIRE's deduplicated record.
        # Field is `pids` (plural) with `{"scheme": "doi", "value": "..."}` entries.
        pids_raw = product.get("pids") or []
        if pids_raw:
            updates["pids"] = [
                {"type": p["scheme"].lower(), "value": p["value"]}
                for p in pids_raw
                if p.get("scheme") and p.get("value")
            ]

        # Dedup ID stored as openaire external_id.
        dedup_id = product.get("id")
        if dedup_id:
            updates["external_ids"] = [{"source": "openaire", "id": dedup_id}]

        # Authors with ORCIDs — OpenAIRE Graph v2 `authors` array.
        # Each entry may have a `pid` block: {"id": {"scheme": "orcid", "value": "..."}, ...}
        # or a top-level `orcid` shortcut field.
        authors = product.get("authors") or []
        if authors:
            creators = []
            for a in authors:
                name = a.get("fullName") or a.get("name") or ""
                orcid = a.get("orcid")
                if not orcid:
                    pid_block = a.get("pid") or {}
                    pid_id = pid_block.get("id") or {}
                    if isinstance(pid_id, dict) and pid_id.get("scheme", "").lower() == "orcid":
                        orcid = pid_id.get("value")
                if name or orcid:
                    entry: dict = {"name": name, "source": "openaire"}
                    if orcid:
                        entry["orcid"] = orcid
                    creators.append(entry)
            if creators:
                updates["creators"] = creators

        # Subjects — partition by scheme:
        #   scheme "fos"  → domain topics (Fields of Science classification)
        #   scheme "sdg"  → domain topics (Sustainable Development Goals)
        #   anything else → free-text keywords
        subjects = product.get("subjects") or []
        keywords: list[str] = []
        domain_topics: list[dict] = []
        for s in subjects:
            # Support both nested {"subject": {"scheme": ..., "value": ...}} and flat dicts.
            subj = s.get("subject") or s
            scheme = (subj.get("scheme") or "").lower()
            value = subj.get("value") or subj.get("label") or ""
            if not value:
                continue
            if scheme in ("fos", "sdg"):
                domain_topics.append({
                    "id": subj.get("id"),
                    "label": value,
                    "scheme": scheme,
                    "source": "openaire",
                })
            else:
                keywords.append(value)
        if keywords:
            updates["keywords"] = keywords
        if domain_topics:
            updates["topics"] = domain_topics

        # Projects/funders — stored in the dedicated `projects` column (not `topics`).
        # Extracts: id, code, title, funder name, funder short name, jurisdiction.
        projects = product.get("projects") or []
        if projects:
            parsed_projects = []
            for p in projects:
                if not p.get("id"):
                    continue
                proj: dict = {"id": p["id"], "source": "openaire"}
                if p.get("code"):
                    proj["code"] = p["code"]
                if p.get("title"):
                    proj["title"] = p["title"]
                funder = p.get("funder") or {}
                if funder.get("name"):
                    proj["funder"] = funder["name"]
                if funder.get("shortName"):
                    proj["funder_short"] = funder["shortName"]
                if funder.get("jurisdiction"):
                    proj["funder_jurisdiction"] = funder["jurisdiction"]
                parsed_projects.append(proj)
            if parsed_projects:
                updates["projects"] = parsed_projects

        # Access rights: prefer top-level `bestAccessRight`, fall back to first instance.
        best = product.get("bestAccessRight") or {}
        if not best:
            instances = product.get("instances") or []
            best = (instances[0].get("accessRight") or {}) if instances else {}
        if best.get("label"):
            updates["access"] = {"type": best["label"]}

        return updates
