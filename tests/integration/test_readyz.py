from collections.abc import AsyncIterator, Iterator

import httpx
import pytest

from hopper.api.main import create_app
from hopper.config import get_settings


@pytest.fixture(autouse=True)
def fresh_settings() -> Iterator[None]:
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def _client() -> AsyncIterator[httpx.AsyncClient]:
    app = create_app()
    # ASGITransport does not run the lifespan, so run it here to create the engine and Redis client.
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async for c in _client():
        yield c


@pytest.fixture
async def broken_client(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[httpx.AsyncClient]:
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://hopper:hopper@127.0.0.1:1/hopper")
    monkeypatch.setenv("REDIS_URL", "redis://127.0.0.1:1/0")
    get_settings.cache_clear()
    async for c in _client():
        yield c


async def test_readyz_ok_when_postgres_and_redis_are_up(client: httpx.AsyncClient) -> None:
    resp = await client.get("/readyz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "checks": {"postgres": "ok", "redis": "ok"}}


async def test_readyz_503_when_dependencies_are_down(broken_client: httpx.AsyncClient) -> None:
    resp = await broken_client.get("/readyz")
    assert resp.status_code == 503
    assert resp.json() == {
        "status": "unavailable",
        "checks": {"postgres": "unavailable", "redis": "unavailable"},
    }


async def test_healthz_stays_green_when_dependencies_are_down(
    broken_client: httpx.AsyncClient,
) -> None:
    resp = await broken_client.get("/healthz")
    assert resp.status_code == 200
