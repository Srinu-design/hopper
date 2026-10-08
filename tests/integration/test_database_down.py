"""With Postgres unreachable the API answers 503 with Retry-After, never a bare 500."""

from collections.abc import AsyncIterator, Iterator

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import create_async_engine

from hopper.api.main import create_app
from hopper.auth import keys
from hopper.config import get_settings

DOWN = "postgresql+asyncpg://hopper:hopper@127.0.0.1:1/hopper"  # nothing listens on port 1


@pytest.fixture
def fresh_settings() -> Iterator[None]:
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture
async def down_client(
    fresh_settings: None, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[httpx.AsyncClient]:
    monkeypatch.setenv("DATABASE_URL", DOWN)
    get_settings.cache_clear()
    app = create_app()
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield client


def _assert_unavailable(resp: httpx.Response) -> None:
    assert resp.status_code == 503, resp.text
    assert resp.headers["Retry-After"] == "5"
    error = resp.json()["error"]
    assert error["code"] == "database_unavailable"
    # The answer passed through the request-id middleware like any other.
    assert resp.headers["X-Request-ID"] == error["request_id"]


async def test_a_key_lookup_with_postgres_down_is_a_503(down_client: httpx.AsyncClient) -> None:
    key = keys.generate(get_settings().api_key_pepper.get_secret_value().encode())
    resp = await down_client.get("/v1/jobs", headers={"Authorization": f"Bearer {key.plaintext}"})
    _assert_unavailable(resp)


async def test_postgres_going_down_after_auth_is_a_503(
    api_app: FastAPI, client: httpx.AsyncClient
) -> None:
    body = {"task": "sleep", "payload": {"ms": 1}}
    assert (await client.post("/v1/jobs", json=body)).status_code == 201  # key now cached
    working = api_app.state.engine
    api_app.state.engine = create_async_engine(DOWN)
    try:
        _assert_unavailable(await client.post("/v1/jobs", json=body))
        _assert_unavailable(await client.get("/v1/dlq"))
    finally:
        await api_app.state.engine.dispose()
        api_app.state.engine = working
    assert (await client.post("/v1/jobs", json=body)).status_code == 201  # and back again
