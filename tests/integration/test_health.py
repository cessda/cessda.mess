"""Integration tests for /health endpoint."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import create_app


@pytest.fixture
async def health_client():
    """Test client with mocked PostgreSQL; QLever is not running (unavailable)."""
    app = create_app()

    from app.database import get_session
    from sqlalchemy.ext.asyncio import AsyncSession

    mock_session = AsyncMock(spec=AsyncSession)
    mock_session.execute = AsyncMock(return_value=MagicMock())

    async def override_get_session():
        yield mock_session

    app.dependency_overrides[get_session] = override_get_session

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


async def test_health_ok(health_client):
    """Health returns 200 even when QLever is unavailable — SPARQL is optional."""
    response = await health_client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["postgres"] == "ok"
    # QLever is not running in tests — sparql will be "unavailable", not "error"
    assert data["sparql"] in ("ok", "unavailable")
