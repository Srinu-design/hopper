import asyncio
import os
import uuid
from collections.abc import AsyncIterator, Iterator

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.engine import URL, make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hopper.api.main import create_app
from hopper.config import get_settings
from tests.helpers import TenantCreds, make_tenant, run_alembic

# Tests default to the local Compose stack; CI overrides these via the environment.
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://hopper:hopper@127.0.0.1:5432/hopper")
os.environ.setdefault("REDIS_URL", "redis://127.0.0.1:6379/0")
os.environ.setdefault("API_KEY_PEPPER", "test-pepper-not-a-secret")
os.environ.setdefault("JWT_SECRET", "test-jwt-secret-not-a-secret-0123456789")


def _admin_url() -> URL:
    return make_url(os.environ["DATABASE_URL"]).set(database="postgres")


def _db_url(name: str) -> str:
    return (
        make_url(os.environ["DATABASE_URL"])
        .set(database=name)
        .render_as_string(hide_password=False)
    )


async def _create_db(name: str, template: str | None = None) -> None:
    admin = create_async_engine(_admin_url(), isolation_level="AUTOCOMMIT")
    try:
        async with admin.connect() as conn:
            clause = f' TEMPLATE "{template}"' if template else ""
            await conn.execute(text(f'CREATE DATABASE "{name}"{clause}'))
    finally:
        await admin.dispose()


async def _drop_db(name: str) -> None:
    admin = create_async_engine(_admin_url(), isolation_level="AUTOCOMMIT")
    try:
        async with admin.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
    finally:
        await admin.dispose()


def _new_name() -> str:
    return f"hopper_test_{uuid.uuid4().hex[:12]}"


@pytest.fixture(scope="session")
def migrated_template() -> Iterator[str]:
    """Migrate once per session with the real alembic CLI; tests clone this database.

    CREATE DATABASE ... TEMPLATE copies it in milliseconds, so each test gets a fresh,
    fully migrated database without paying for an alembic subprocess every time.
    """
    name = f"{_new_name()}_tmpl"
    asyncio.run(_create_db(name))
    try:
        result = run_alembic(_db_url(name), "upgrade", "head")
        assert result.returncode == 0, result.stderr
        yield name
    finally:
        asyncio.run(_drop_db(name))


@pytest.fixture
async def scratch_db_url() -> AsyncIterator[str]:
    """An empty throwaway database, for tests that run migrations themselves."""
    name = _new_name()
    await _create_db(name)
    try:
        yield _db_url(name)
    finally:
        await _drop_db(name)


@pytest.fixture
async def migrated_db_url(migrated_template: str) -> AsyncIterator[str]:
    """A throwaway database already at alembic head, cloned from the session template."""
    name = _new_name()
    await _create_db(name, template=migrated_template)
    try:
        yield _db_url(name)
    finally:
        await _drop_db(name)


@pytest.fixture
async def migrated_engine(migrated_db_url: str) -> AsyncIterator[AsyncEngine]:
    # Pool sized for the concurrency tests (10 claimers at once).
    engine = create_async_engine(migrated_db_url, pool_size=12, max_overflow=0)
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def api_app(
    migrated_engine: AsyncEngine, migrated_db_url: str, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[FastAPI]:
    """The real FastAPI app, lifespan running, pointed at the migrated scratch database."""
    monkeypatch.setenv("DATABASE_URL", migrated_db_url)
    get_settings.cache_clear()
    app = create_app()
    async with app.router.lifespan_context(app):
        yield app
    get_settings.cache_clear()


def _client(app: FastAPI, bearer: str | None = None) -> httpx.AsyncClient:
    headers = {"Authorization": f"Bearer {bearer}"} if bearer else {}
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test", headers=headers)


@pytest.fixture
async def tenant(api_app: FastAPI) -> TenantCreds:
    """Tenant A, with an API key. `client` acts as this tenant."""
    return await make_tenant(api_app.state.engine, "acme")


@pytest.fixture
async def other_tenant(api_app: FastAPI) -> TenantCreds:
    """Tenant B, for isolation tests."""
    return await make_tenant(api_app.state.engine, "globex")


@pytest.fixture
async def client(api_app: FastAPI, tenant: TenantCreds) -> AsyncIterator[httpx.AsyncClient]:
    async with _client(api_app, tenant.api_key) as c:
        yield c


@pytest.fixture
async def other_client(
    api_app: FastAPI, other_tenant: TenantCreds
) -> AsyncIterator[httpx.AsyncClient]:
    async with _client(api_app, other_tenant.api_key) as c:
        yield c


@pytest.fixture
async def anon_client(api_app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """No credentials at all."""
    async with _client(api_app) as c:
        yield c
