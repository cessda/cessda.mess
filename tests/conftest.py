"""Shared pytest fixtures."""

import json
from pathlib import Path
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.database import get_session
from app.main import create_app
from app.models.base import Base

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES_DIR / name).read_text())


# ---------------------------------------------------------------------------
# In-process test database (uses testcontainers for real PostgreSQL)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def postgres_url():
    """Spin up a real PostgreSQL container for the test session."""
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16.9") as pg:
        yield pg.get_connection_url().replace("psycopg2", "asyncpg")


@pytest_asyncio.fixture(scope="session")
async def test_engine(postgres_url):
    engine = create_async_engine(postgres_url, echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(test_engine) -> AsyncGenerator[AsyncSession, None]:
    """Provide a test DB session that rolls back after each test."""
    session_factory = async_sessionmaker(test_engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session
        await session.rollback()


# ---------------------------------------------------------------------------
# FastAPI test app
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def test_app(db_session):
    """Create a FastAPI test app with the DB session overridden."""
    app = create_app()

    async def override_get_session():
        yield db_session

    app.dependency_overrides[get_session] = override_get_session
    return app


@pytest_asyncio.fixture
async def client(test_app) -> AsyncGenerator[AsyncClient, None]:
    """Async test client for the FastAPI app."""
    async with AsyncClient(
        transport=ASGITransport(app=test_app), base_url="http://test"
    ) as ac:
        yield ac
