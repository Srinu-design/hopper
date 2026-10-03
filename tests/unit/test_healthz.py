from collections.abc import AsyncIterator

import httpx
import pytest

from hopper.api.main import create_app


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_healthz_returns_ok(client: httpx.AsyncClient) -> None:
    resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


async def test_healthz_needs_no_backing_services(client: httpx.AsyncClient) -> None:
    # Liveness must stay green even when Postgres and Redis are unreachable;
    # readiness (/readyz) is the probe that reports them.
    resp = await client.get("/healthz")
    assert resp.status_code == 200
