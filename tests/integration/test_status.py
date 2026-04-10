"""Integration tests for /status endpoint."""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_session
from app.main import create_app
from app.models.digital_object import DigitalObject
from app.services.enrichment import upsert_digital_object


@pytest.fixture
async def status_client(db_session):
    app = create_app()

    async def override_session():
        yield db_session

    app.dependency_overrides[get_session] = override_session

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac, db_session


async def test_status_not_found(status_client):
    client, _ = status_client
    response = await client.get("/status?pid=10.9999/nonexistent")
    assert response.status_code == 200
    data = response.json()
    assert data["found"] is False
    assert data["fresh"] is None


async def test_status_found_fresh(status_client):
    client, session = status_client

    async with session.begin():
        await upsert_digital_object(
            session,
            {
                "pids": [{"type": "doi", "value": "10.1234/status-test"}],
                "object_type": "dataset",
                "origin": "source_endpoint",
                "raw_responses": {},
            },
        )

    response = await client.get("/status?pid=10.1234/status-test")
    assert response.status_code == 200
    data = response.json()
    assert data["found"] is True
    assert data["fresh"] is True


async def test_status_invalid_pid(status_client):
    client, _ = status_client
    response = await client.get("/status?pid=bad-pid-format")
    assert response.status_code == 400


async def test_status_found_stale(status_client):
    client, session = status_client

    stale_time = datetime.now(UTC) - timedelta(hours=100)
    async with session.begin():
        obj = await upsert_digital_object(
            session,
            {
                "pids": [{"type": "doi", "value": "10.1234/stale-test"}],
                "object_type": "dataset",
                "origin": "source_endpoint",
                "raw_responses": {},
            },
        )

    # Force last_checked to be stale
    from sqlalchemy import update
    async with session.begin():
        await session.execute(
            update(DigitalObject)
            .where(DigitalObject.id == obj.id)
            .values(last_checked=stale_time)
        )

    response = await client.get("/status?pid=10.1234/stale-test")
    assert response.status_code == 200
    data = response.json()
    assert data["found"] is True
    assert data["fresh"] is False
