import os
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hopper.api.main import create_app
from hopper.config import get_settings
from tests.helpers import run_alembic

# Tests default to the local Compose stack; CI overrides these via the environment.
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://hopper:hopper@127.0.0.1:5432/hopper")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")


@pytest.fixture
async def scratch_db_url() -> AsyncIterator[str]:
    """A throwaway database, so tests may migrate up and down without touching dev data."""
    base = make_url(os.environ["DATABASE_URL"])
    name = f"hopper_test_{uuid.uuid4().hex[:12]}"
    admin = create_async_engine(base.set(database="postgres"), isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        await conn.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        yield base.set(database=name).render_as_string(hide_password=False)
    finally:
        async with admin.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        await admin.dispose()


@pytest.fixture
async def migrated_engine(scratch_db_url: str) -> AsyncIterator[AsyncEngine]:
    result = run_alembic(scratch_db_url, "upgrade", "head")
    assert result.returncode == 0, result.stderr
    # Pool sized for the concurrency tests (10 claimers at once).
    engine = create_async_engine(scratch_db_url, pool_size=12, max_overflow=0)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def api_app(
    migrated_engine: AsyncEngine, scratch_db_url: str, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[FastAPI]:
    """The real FastAPI app, lifespan running, pointed at the migrated scratch database."""
    monkeypatch.setenv("DATABASE_URL", scratch_db_url)
    get_settings.cache_clear()
    app = create_app()
    async with app.router.lifespan_context(app):
        yield app
    get_settings.cache_clear()


@pytest.fixture
async def client(api_app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    transport = httpx.ASGITransport(app=api_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
