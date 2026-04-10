"""Integration tests for /enrich endpoint with mocked external APIs."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import respx
from httpx import ASGITransport, AsyncClient

from app.database import get_session
from app.main import create_app

FIXTURES = Path(__file__).parent.parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
async def enrich_client(db_session):
    app = create_app()

    async def override_session():
        yield db_session

    app.dependency_overrides[get_session] = override_session

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac, app


@respx.mock
async def test_enrich_returns_skg_if(enrich_client, db_session):
    client, app = enrich_client

    source_data = _load("skg_if_source_response.json")
    scholex_data = _load("scholexplorer_response.json")
    openalex_data = _load("openalex_response.json")

    # Mock source endpoint
    source_mock = AsyncMock()
    source_mock.fetch_by_pid = AsyncMock(return_value=source_data["@graph"][0])
    source_mock.parse_product = MagicMock(
        return_value={
            "pids": [{"type": "doi", "value": "10.1234/dataset-001"}],
            "object_type": "dataset",
            "title": "Example Survey Dataset",
            "origin": "source_endpoint",
            "raw_responses": {"source_endpoint": source_data["@graph"][0]},
        }
    )

    # Mock Scholexplorer — find_links now returns tagged links from both directions.
    # Combine as_source (dataset→related) and as_target (publication→dataset) results.
    tagged_links = (
        scholex_data["as_source"]["result"] + scholex_data["as_target"]["result"]
    )
    scholex_mock = AsyncMock()
    scholex_mock.find_links = AsyncMock(return_value=tagged_links)
    scholex_mock.parse_link = MagicMock(
        side_effect=lambda link: {
            "target": {
                "pids": [{"type": "doi", "value": "10.5678/pub-001"}],
                "object_type": link.get("_related_side") == "source" and "publication" or "dataset",
                "title": "Analysis of Survey Data",
                "external_ids": [{"source": "openaire", "id": "doi_dedup___::abc123"}],
                "origin": "scholexplorer",
                "raw_responses": {"scholexplorer": link},
            },
            "relation_type": link.get("relationshipType", {}).get("name", "IsRelatedTo"),
            "source_pids": [{"type": "doi", "value": "10.1234/dataset-001"}],
            "source_external_ids": [],
        }
    )

    # Mock OpenAIRE
    openaire_mock = AsyncMock()
    openaire_mock.fetch_by_doi = AsyncMock(return_value=_load("openaire_response.json"))
    openaire_mock.parse_product = MagicMock(return_value={
        "pids": [{"type": "doi", "value": "10.1234/dataset-001"}],
        "external_ids": [{"source": "openaire", "id": "doi_dedup___::abc123"}],
        "keywords": ["Social Sciences"],
    })

    # Mock OpenAlex
    openalex_mock = AsyncMock()
    openalex_mock.fetch_by_doi = AsyncMock(return_value=openalex_data)
    openalex_mock.parse_work = MagicMock(
        return_value={
            "citation_count": 42,
            "fwci": 1.85,
            "external_ids": [{"source": "openalex", "id": "https://openalex.org/W123"}],
        }
    )

    app.state.source_client = source_mock
    app.state.scholexplorer_client = scholex_mock
    app.state.openaire_client = openaire_mock
    app.state.openalex_client = openalex_mock

    response = await client.get("/enrich?pid=10.1234/dataset-001")

    assert response.status_code == 200
    data = response.json()
    assert data["pid"] == "10.1234/dataset-001"
    assert "@context" in data["data"]
    assert "@graph" in data["data"]
    assert data["data"]["@context"] == "https://w3id.org/skg-if/context/skg-if.json"


async def test_enrich_invalid_pid(enrich_client):
    client, app = enrich_client
    response = await client.get("/enrich?pid=not-a-valid-pid")
    assert response.status_code == 400
    assert "Unsupported PID" in response.json()["detail"]


async def test_enrich_doi_url_normalised(enrich_client):
    """DOI URLs should be normalised before lookup."""
    client, app = enrich_client

    source_mock = AsyncMock()
    source_mock.fetch_by_pid = AsyncMock(return_value=None)
    source_mock.parse_product = MagicMock(return_value={
        "pids": [{"type": "doi", "value": "10.1234/dataset-001"}],
        "object_type": "dataset",
        "origin": "source_endpoint",
        "raw_responses": {},
    })
    scholex_mock = AsyncMock()
    scholex_mock.find_links = AsyncMock(return_value=[])
    scholex_mock.parse_link = MagicMock(return_value=None)
    openaire_mock = AsyncMock()
    openalex_mock = AsyncMock()

    app.state.source_client = source_mock
    app.state.scholexplorer_client = scholex_mock
    app.state.openaire_client = openaire_mock
    app.state.openalex_client = openalex_mock

    response = await client.get("/enrich?pid=https%3A%2F%2Fdoi.org%2F10.1234%2Fdataset-001")
    assert response.status_code == 200
    assert response.json()["pid"] == "10.1234/dataset-001"
