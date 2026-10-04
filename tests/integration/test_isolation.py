"""Tenant isolation: tenant B's key against every one of tenant A's resources.

Every route is accounted for below. A new route makes test_every_route_is_covered fail until
someone decides how it is isolated and adds it here.
"""

import uuid
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text

from tests.helpers import TenantCreds, job_row

# Routes that take one of A's ids: B must get 404 (not 403, so ids do not leak).
BY_ID = {
    ("GET", "/v1/jobs/{job_id}"),
    ("POST", "/v1/jobs/{job_id}/cancel"),
    ("POST", "/v1/jobs/{job_id}/replay"),
    ("GET", "/v1/schedules/{schedule_id}"),
    ("PATCH", "/v1/schedules/{schedule_id}"),
    ("DELETE", "/v1/schedules/{schedule_id}"),
}
# Collection routes: B sees and touches only its own rows.
COLLECTIONS = {
    ("GET", "/v1/jobs"),
    ("POST", "/v1/jobs"),
    ("GET", "/v1/dlq"),
    ("POST", "/v1/dlq/replay"),
    ("GET", "/v1/schedules"),
    ("POST", "/v1/schedules"),
}
# Admin routes take a JWT; owner-versus-owner isolation is in test_auth_api.py.
ADMIN = {
    ("POST", "/admin/tenants"),
    ("GET", "/admin/api-keys"),
    ("POST", "/admin/api-keys"),
    ("DELETE", "/admin/api-keys/{key_id}"),
}
PUBLIC = {("GET", "/healthz"), ("GET", "/readyz"), ("POST", "/auth/token")}


def routes(app: FastAPI) -> set[tuple[str, str]]:
    return {
        (method.upper(), path)
        for path, operations in app.openapi()["paths"].items()
        for method in operations
    }


def test_every_route_is_covered(api_app: FastAPI) -> None:
    assert routes(api_app) == BY_ID | COLLECTIONS | ADMIN | PUBLIC


@pytest.mark.parametrize(("method", "path"), sorted(BY_ID | COLLECTIONS | ADMIN))
async def test_every_protected_route_rejects_anonymous_requests(
    anon_client: httpx.AsyncClient, method: str, path: str
) -> None:
    url = path.replace("{job_id}", str(uuid.uuid4()))
    url = url.replace("{schedule_id}", str(uuid.uuid4())).replace("{key_id}", str(uuid.uuid4()))
    resp = await anon_client.request(method, url, json={})
    assert resp.status_code == 401, (method, path, resp.text)


@dataclass
class ResourcesOfA:
    queued_job: str
    dead_job: str
    schedule: str


@pytest.fixture
async def a(client: httpx.AsyncClient, api_app: FastAPI, tenant: TenantCreds) -> ResourcesOfA:
    queued = (
        await client.post(
            "/v1/jobs",
            json={"task": "sleep", "payload": {"ms": 1}},
            headers={"Idempotency-Key": "shared-key"},
        )
    ).json()["id"]
    dead = (await client.post("/v1/jobs", json={"task": "sleep", "payload": {"ms": 1}})).json()
    async with api_app.state.engine.begin() as conn:
        await conn.execute(
            text("UPDATE jobs SET status = 'dead', dead_at = now() WHERE id = :i"),
            {"i": dead["id"]},
        )
    schedule = (
        await client.post(
            "/v1/schedules",
            json={"name": "nightly", "cron": "0 3 * * *", "task": "sleep", "payload": {"ms": 1}},
        )
    ).json()["id"]
    return ResourcesOfA(queued, dead["id"], schedule)


def a_url(path: str, a: ResourcesOfA) -> str:
    job = a.dead_job if path.endswith("/replay") else a.queued_job
    return path.replace("{job_id}", job).replace("{schedule_id}", a.schedule)


@pytest.mark.parametrize(("method", "path"), sorted(BY_ID))
async def test_tenant_b_gets_404_on_every_tenant_a_resource(
    other_client: httpx.AsyncClient,
    api_app: FastAPI,
    a: ResourcesOfA,
    method: str,
    path: str,
) -> None:
    before = await snapshot(api_app, a)
    body: dict[str, Any] | None = {"enabled": False} if method == "PATCH" else None
    resp = await other_client.request(method, a_url(path, a), json=body)
    assert (resp.status_code, resp.json()["error"]["code"]) == (404, "not_found"), resp.text
    assert await snapshot(api_app, a) == before  # and nothing of A's changed


async def snapshot(app: FastAPI, a: ResourcesOfA) -> list[Any]:
    rows = [await job_row(app.state.engine, uuid.UUID(a.queued_job))]
    rows.append(await job_row(app.state.engine, uuid.UUID(a.dead_job)))
    async with app.state.engine.connect() as conn:
        rows.append(
            dict(
                (
                    await conn.execute(
                        text("SELECT * FROM schedules WHERE id = :i"), {"i": a.schedule}
                    )
                )
                .one()
                ._mapping
            )
        )
    return rows


async def test_tenant_b_lists_see_nothing_of_tenant_a(
    other_client: httpx.AsyncClient, a: ResourcesOfA
) -> None:
    assert (await other_client.get("/v1/jobs")).json()["jobs"] == []
    assert (await other_client.get("/v1/dlq")).json()["jobs"] == []
    assert (await other_client.get("/v1/schedules")).json()["schedules"] == []


async def test_tenant_b_bulk_replay_cannot_reach_tenant_a_jobs(
    other_client: httpx.AsyncClient, client: httpx.AsyncClient, a: ResourcesOfA
) -> None:
    by_ids = await other_client.post("/v1/dlq/replay", json={"ids": [a.dead_job]})
    by_filter = await other_client.post("/v1/dlq/replay", json={"filter": {}})
    assert by_ids.json()["replayed"] == by_filter.json()["replayed"] == 0
    assert (await client.get(f"/v1/jobs/{a.dead_job}")).json()["status"] == "dead"


async def test_idempotency_keys_and_schedule_names_are_per_tenant(
    other_client: httpx.AsyncClient, a: ResourcesOfA
) -> None:
    """B reusing A's key or schedule name gets its own new row, never A's."""
    job = await other_client.post(
        "/v1/jobs",
        json={"task": "sleep", "payload": {"ms": 1}},
        headers={"Idempotency-Key": "shared-key"},
    )
    assert job.status_code == 201
    assert job.json()["id"] != a.queued_job
    schedule = await other_client.post(
        "/v1/schedules",
        json={"name": "nightly", "cron": "0 3 * * *", "task": "sleep", "payload": {"ms": 1}},
    )
    assert schedule.status_code == 201
    assert schedule.json()["id"] != a.schedule
