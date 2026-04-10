"""Integration tests for /admin endpoints."""

import pytest
from httpx import ASGITransport, AsyncClient

from app.config import settings
from app.database import get_session
from app.main import create_app


@pytest.fixture
async def admin_client(db_session):
    app = create_app()

    async def override_session():
        yield db_session

    app.dependency_overrides[get_session] = override_session

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac


async def test_stats_requires_admin_key(admin_client):
    response = await admin_client.get("/admin/stats")
    assert response.status_code == 422  # missing header


async def test_stats_invalid_key(admin_client):
    response = await admin_client.get("/admin/stats", headers={"X-Admin-Key": "wrong-key"})
    assert response.status_code == 403


async def test_stats_returns_counts(admin_client, monkeypatch):
    monkeypatch.setattr(settings, "mess_admin_key", "test-key")
    response = await admin_client.get("/admin/stats", headers={"X-Admin-Key": "test-key"})
    assert response.status_code == 200
    data = response.json()
    assert "total_objects" in data
    assert "total_relationships" in data
    assert data["total_objects"] == 0


