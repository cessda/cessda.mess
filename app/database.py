"""SQLAlchemy async engine and session factory.

A single engine is shared across the entire app lifetime (created at module import).
Each FastAPI request gets its own `AsyncSession` via the `get_session` dependency.

Key settings:
  - `pool_pre_ping=True`   — discards stale connections before use (avoids "server closed
                             the connection unexpectedly" errors after DB restarts).
  - `expire_on_commit=False` — ORM objects remain usable after `session.commit()` without
                               issuing extra SELECT queries; important for async where lazy
                               loading is not allowed.
  - `pool_size=10 / max_overflow=20` — up to 30 concurrent DB connections.

Do NOT call `engine.dispose()` in tests — use a separate test engine instead.
"""

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings

engine = create_async_engine(
    settings.database_url,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency that yields one `AsyncSession` per request.

    Usage in route handlers:
        async def my_route(session: AsyncSession = Depends(get_session)) -> ...:

    The session is closed (and its connection returned to the pool) automatically
    after the request completes, even if an exception is raised.  Transactions must
    be managed explicitly — call `async with session.begin()` or `await session.commit()`
    inside your service functions.
    """
    async with AsyncSessionLocal() as session:
        yield session
