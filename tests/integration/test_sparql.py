"""Integration tests for /sparql proxy endpoint."""

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from httpx import ASGITransport, AsyncClient

from app.main import create_app


@pytest.fixture
async def sparql_client():
    app = create_app()

    # Build a mock that behaves as an async context manager returning a mock client.
    mock_inner = AsyncMock(spec=httpx.AsyncClient)
    mock_inner.get = AsyncMock(
        return_value=httpx.Response(
            200,
            json={"results": {"bindings": []}},
            headers={"content-type": "application/sparql-results+json"},
        )
    )
    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_inner)
    mock_cm.__aexit__ = AsyncMock(return_value=False)
    mock_class = MagicMock(return_value=mock_cm)

    with patch("app.routers.sparql.httpx.AsyncClient", mock_class):
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            yield ac, mock_inner


async def test_sparql_proxy_forwards_query(sparql_client):
    client, mock_inner = sparql_client
    query = "SELECT * WHERE { ?s ?p ?o } LIMIT 10"
    response = await client.get(f"/sparql?query={query}")
    assert response.status_code == 200
    mock_inner.get.assert_called_once()
    call_kwargs = mock_inner.get.call_args
    assert "query" in call_kwargs.kwargs.get("params", {}) or "query" in str(call_kwargs)


async def test_sparql_missing_query_param(sparql_client):
    client, _ = sparql_client
    response = await client.get("/sparql")
    assert response.status_code == 422
