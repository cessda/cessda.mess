"""Scholexplorer v3 API client.

Scholexplorer (https://scholexplorer.openaire.eu) is an OpenAIRE service that indexes
dataset–publication links from data repositories and publishers.  It uses the Scholix 3.0
data model (https://scholix.org).

Key behaviour:
  - Links are DIRECTIONAL in Scholix: a publication that cites a dataset is stored as
    source=publication, target=dataset.  To find ALL objects linked to a PID — both
    objects the dataset links to AND objects that link to the dataset — we must make
    two queries per DOI:
      1. `sourcePid=<doi>` → our dataset is the source; related objects are on the target side.
      2. `targetPid=<doi>` → our dataset is the target; related objects are on the source side.
    Without the targetPid query, publications that CITE the dataset are never discovered.

  - Pagination uses a `resumptionToken` returned in each response — NOT page numbers.
    We collect all pages before returning.

  - Each link returned by `find_links` is tagged with a private `_related_side` key
    ("target" or "source") so that `parse_link` knows which Scholix side holds the
    related object (i.e. the one that is NOT our dataset).

Configuration:
  - `SCHOLEXPLORER_API_URL` — default: https://api-beta.scholexplorer.openaire.eu/v3/Links
  - Optionally uses `OPENAIRE_ACCESS_TOKEN` for higher rate limits (shared with OpenAIRE).
"""

import logging

import httpx

from app.config import settings
from app.services.http_utils import request_with_backoff

logger = logging.getLogger(__name__)

# Results per page.  Scholexplorer's practical maximum is around 100.
_PAGE_SIZE = 100


class ScholexplorerClient:
    """Fetches and parses Scholix 3.0 link records for a given PID.

    One instance is created at startup (in `app/main.py`) and stored on `app.state`.
    """

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def find_links(self, pid: str) -> list[dict]:
        """Return all Scholix link records involving `pid`, querying both directions.

        Makes two paginated calls:
          - `sourcePid=pid`: links where our dataset is the source → related object
            is on the **target** side (e.g. "dataset IsRelatedTo publication").
          - `targetPid=pid`: links where our dataset is the target → related object
            is on the **source** side (e.g. "publication Cites dataset").

        Each returned dict is a raw Scholix link record with one added private key:
          `_related_side`: "target" or "source" — consumed by `parse_link` to select
          the correct side as the related object.

        Returns:
            Combined list from both queries (unparsed).  Pass each through `parse_link`.
        """
        as_source = await self._paginate({"sourcePid": pid})
        for link in as_source:
            link["_related_side"] = "target"

        as_target = await self._paginate({"targetPid": pid})
        for link in as_target:
            link["_related_side"] = "source"

        logger.info(
            "Scholexplorer links found",
            extra={
                "pid": pid,
                "as_source": len(as_source),
                "as_target": len(as_target),
                "total": len(as_source) + len(as_target),
            },
        )
        return as_source + as_target

    async def _paginate(self, base_params: dict) -> list[dict]:
        """Fetch all pages for a Scholexplorer query, following resumption tokens.

        Args:
            base_params: Initial query parameters, e.g. `{"sourcePid": "10.xxx/yyy"}`.

        Returns:
            All result dicts from all pages combined.
        """
        all_links: list[dict] = []
        params: dict = {**base_params, "rows": _PAGE_SIZE}

        while True:
            response = await request_with_backoff(
                lambda p=params: self._client.get(
                    settings.scholexplorer_api_url, params=p
                )
            )
            response.raise_for_status()
            data = response.json()

            result = data.get("result", [])
            all_links.extend(result)

            token = data.get("resumptionToken")
            if not token or not result:
                break

            # Next page: resumption token replaces all other params.
            params = {"resumptionToken": token, "rows": _PAGE_SIZE}

        return all_links

    def parse_link(self, link: dict) -> dict | None:
        """Extract normalised fields from a single Scholix 3.0 link record.

        Uses `_related_side` (set by `find_links`) to select which Scholix object
        is the related object (the one that is NOT our dataset):
          - `_related_side == "target"` (sourcePid query): related object is `Target`.
          - `_related_side == "source"` (targetPid query): related object is `Source`.

        The Scholexplorer v3 API returns TitleCase field names at the link level
        (`"RelationshipType"`, `"Target"`, `"Source"`) and within identifier objects
        (`"ID"`, `"IDScheme"`).  Both spellings are tried throughout.

        Returns:
            A dict with:
              - `"target"`: fields dict for the related object, compatible with
                `upsert_digital_object` (named "target" regardless of which Scholix
                side it came from — it is always the object being linked TO our dataset).
              - `"relation_type"`: Scholix relation type string (e.g. "Cites").
              - `"source_pids"`: PID list of our dataset's side (informational).
              - `"source_external_ids"`: OpenAIRE ID of our dataset's side (if present).
            Or `None` if the link cannot be parsed (missing related object or PIDs).
        """
        related_side = link.get("_related_side", "target")

        target_raw = link.get("target") or link.get("Target")
        source_raw = link.get("source") or link.get("Source")

        # Select which Scholix side is the related object vs. our dataset.
        if related_side == "source":
            related_raw = source_raw
            dataset_raw = target_raw
        else:
            related_raw = target_raw
            dataset_raw = source_raw

        if not related_raw:
            return None

        # Relation type: API returns "RelationshipType": {"Name": "..."} (TitleCase).
        rel_type_obj = link.get("RelationshipType") or link.get("relationshipType") or {}
        relation_type = (
            rel_type_obj.get("Name") or rel_type_obj.get("name")
            or link.get("LinkType")
            or "IsRelatedTo"
        )

        related_pids = self._extract_pids(related_raw)
        if not related_pids:
            # Cannot deduplicate or store a related object without any PID — skip.
            return None

        title = self._extract_title(related_raw)
        creators = self._extract_creators(related_raw)
        object_type = self._map_type(related_raw.get("objectType") or related_raw.get("Type", ""))

        # Extract the OpenAIRE dedup ID from the related object side.
        related_openaire_id = self._extract_openaire_id(related_raw)

        # Extract the OpenAIRE dedup ID from our dataset's side, used to update
        # the source object's external_ids in the enrichment pipeline.
        source_openaire_id = self._extract_openaire_id(dataset_raw) if dataset_raw else None

        return {
            "target": {
                "pids": related_pids,
                "object_type": object_type,
                "title": title,
                "creators": creators,
                "external_ids": [{"source": "openaire", "id": related_openaire_id}] if related_openaire_id else [],
                "origin": "scholexplorer",
                "raw_responses": {"scholexplorer": link},
            },
            "relation_type": relation_type,
            "source_pids": self._extract_pids(dataset_raw) if dataset_raw else [],
            "source_external_ids": [{"source": "openaire", "id": source_openaire_id}] if source_openaire_id else [],
        }

    def _extract_openaire_id(self, obj: dict) -> str | None:
        """Return the OpenAIRE dedup ID from a Scholix object node, or None."""
        for ident in (obj.get("Identifier") or obj.get("identifier") or []):
            scheme = (ident.get("IDScheme") or ident.get("identifierType") or "").lower()
            if "openaire" in scheme:
                return ident.get("ID") or ident.get("identifier")
        return None

    def _extract_pids(self, obj: dict) -> list[dict]:
        """Parse Scholix identifier array into canonical `{"type", "value"}` dicts.

        Handles both Scholix v3 TitleCase format (`"Identifier"` array with `"ID"` /
        `"IDScheme"` fields) and the older camelCase format (`"identifier"` array with
        `"identifier"` / `"identifierType"` fields).
        """
        pids = []
        identifiers = obj.get("Identifier") or obj.get("identifier") or []
        for ident in identifiers:
            id_type = (ident.get("IDScheme") or ident.get("identifierType") or "").lower()
            id_val = (ident.get("ID") or ident.get("identifier") or "").strip()
            if not id_val or "openaire" in id_type:
                # Skip OpenAIRE internal IDs — they are not resolvable PIDs.
                continue
            if id_type == "doi":
                # Strip URL prefix if present (e.g. "https://dx.doi.org/10.xxx").
                for prefix in ("https://doi.org/", "http://doi.org/", "https://dx.doi.org/", "doi:"):
                    if id_val.lower().startswith(prefix):
                        id_val = id_val[len(prefix):]
                        break
                pids.append({"type": "doi", "value": id_val})
            elif id_type == "handle":
                pids.append({"type": "handle", "value": id_val})
            elif id_type in ("urn", "urn:nbn"):
                pids.append({"type": "urn_nbn", "value": id_val})
            elif id_type == "ark":
                pids.append({"type": "ark", "value": id_val})
            else:
                # Preserve unknown types verbatim rather than discarding.
                pids.append({"type": id_type or "unknown", "value": id_val})
        return pids

    def _extract_title(self, obj: dict) -> str | None:
        """Return the title string from a Scholix object node.

        Scholexplorer v3 returns `"Title": "string"`.
        Older format returned `"title": [{"title": "..."}]` or `"title": "string"`.
        """
        title = obj.get("Title")
        if isinstance(title, str) and title:
            return title
        titles = obj.get("title", [])
        if isinstance(titles, list) and titles:
            return titles[0].get("title") if isinstance(titles[0], dict) else str(titles[0])
        if isinstance(titles, str):
            return titles
        return None

    def _extract_creators(self, obj: dict) -> list | None:
        """Parse Scholix creator array into canonical `{"name"}` dicts.

        Handles both `"Creator"` (TitleCase, v3) and `"creator"` (older format).
        """
        creators = obj.get("Creator") or obj.get("creator") or []
        if not creators:
            return None
        result = []
        for c in creators:
            if isinstance(c, dict):
                name = c.get("name") or c.get("creatorName", "")
                if name:
                    result.append({"name": name})
            elif isinstance(c, str):
                result.append({"name": c})
        return result or None

    def _map_type(self, raw_type: str) -> str:
        """Map Scholix object type to our internal OBJECT_TYPES vocabulary."""
        mapping = {
            "publication": "publication",
            "literature": "publication",   # Scholix uses "literature" for papers
            "dataset": "dataset",
            "software": "software",
            "other": "other",
        }
        return mapping.get(raw_type.lower(), "other")
