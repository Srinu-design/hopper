import tomllib
from collections.abc import AsyncIterator

import httpx
import pytest

from hopper.api.main import create_app
from tests.helpers import ROOT, client_for


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    async with client_for(create_app()) as c:
        yield c


async def test_healthz_returns_ok(client: httpx.AsyncClient) -> None:
    resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


def test_the_api_reports_the_project_version() -> None:
    """/docs and /openapi.json show pyproject.toml's version, never a second copy of it."""
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    assert create_app().version == project["version"]
