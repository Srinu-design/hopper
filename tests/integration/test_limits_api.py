"""Rate limits and backpressure through the real API: 429 and 503 with Retry-After, the
X-RateLimit headers, one bucket per tenant and route class, two API replicas sharing Redis,
login throttling, and failing open when no depth has been published."""

import asyncio
import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import FastAPI
from prometheus_client import REGISTRY
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.api.main import create_app
from hopper.auth import passwords, store
from hopper.config import get_settings
from hopper.scheduler.depth import DepthLoop
from tests.helpers import TenantCreds, make_tenant, seed_jobs

JOB = {"task": "sleep", "payload": {"ms": 1}}


def client_for(app: FastAPI, key: str | None) -> httpx.AsyncClient:
    headers = {"Authorization": f"Bearer {key}"} if key else {}
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test", headers=headers
    )


def counter(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


async def tenant_with(app: FastAPI, **limits: float) -> TenantCreds:
    return await make_tenant(app.state.engine, f"t-{uuid.uuid4().hex[:8]}", **limits)  # type: ignore[arg-type]


async def publish_depth(app: FastAPI) -> None:
    """One tick of the scheduler's depth loop, into this app's Redis namespace."""
    await DepthLoop(
        app.state.engine, app.state.redis, namespace=get_settings().redis_namespace
    ).tick()


# --- the token bucket -------------------------------------------------------------------------


async def test_every_response_says_how_much_is_left(api_app: FastAPI) -> None:
    creds = await tenant_with(api_app, rate_per_sec=0.001, burst=5)
    async with client_for(api_app, creds.api_key) as c:
        first = await c.post("/v1/jobs", json=JOB)
        second = await c.get(f"/v1/jobs/{first.json()['id']}")
    assert first.status_code == 201
    assert (first.headers["X-RateLimit-Limit"], first.headers["X-RateLimit-Remaining"]) == (
        "5",
        "4",
    )
    # Reads have a bucket of their own: the enqueue above did not spend from it.
    assert second.headers["X-RateLimit-Remaining"] == "4"


async def test_over_the_burst_is_429_with_retry_after(api_app: FastAPI) -> None:
    creds = await tenant_with(api_app, rate_per_sec=0.5, burst=3)
    before = counter("hopper_ratelimit_rejections_total", route_class="enqueue")
    async with client_for(api_app, creds.api_key) as c:
        codes = [(await c.post("/v1/jobs", json=JOB)).status_code for _ in range(3)]
        refused = await c.post("/v1/jobs", json=JOB)
    assert codes == [201, 201, 201]
    assert refused.status_code == 429
    error = refused.json()["error"]
    assert error["code"] == "rate_limited"
    # 1 token at 0.5 per second: just under 2,000 ms (a little refilled while the three ran).
    assert 1000 < error["retry_after_ms"] <= 2000
    assert refused.headers["Retry-After"] == "2"
    assert refused.headers["X-RateLimit-Remaining"] == "0"
    assert error["request_id"] == refused.headers["X-Request-ID"]
    assert counter("hopper_ratelimit_rejections_total", route_class="enqueue") - before == 1


async def test_a_refused_enqueue_creates_no_job(api_app: FastAPI) -> None:
    creds = await tenant_with(api_app, rate_per_sec=0.001, burst=1)
    async with client_for(api_app, creds.api_key) as c:
        await c.post("/v1/jobs", json=JOB)
        assert (await c.post("/v1/jobs", json=JOB)).status_code == 429
        assert len((await c.get("/v1/jobs")).json()["jobs"]) == 1


async def test_tenants_have_separate_buckets(api_app: FastAPI) -> None:
    noisy = await tenant_with(api_app, rate_per_sec=0.001, burst=2)
    quiet = await tenant_with(api_app, rate_per_sec=0.001, burst=2)
    async with client_for(api_app, noisy.api_key) as n, client_for(api_app, quiet.api_key) as q:
        assert [(await n.post("/v1/jobs", json=JOB)).status_code for _ in range(3)] == [
            201,
            201,
            429,
        ]
        assert (await q.post("/v1/jobs", json=JOB)).status_code == 201


async def test_a_drained_enqueue_bucket_does_not_block_reads(api_app: FastAPI) -> None:
    creds = await tenant_with(api_app, rate_per_sec=0.001, burst=1)
    async with client_for(api_app, creds.api_key) as c:
        job = (await c.post("/v1/jobs", json=JOB)).json()
        assert (await c.post("/v1/jobs", json=JOB)).status_code == 429
        assert (await c.get(f"/v1/jobs/{job['id']}")).status_code == 200


async def test_anonymous_requests_spend_no_tenant_tokens(
    api_app: FastAPI, anon_client: httpx.AsyncClient
) -> None:
    creds = await tenant_with(api_app, rate_per_sec=0.001, burst=1)
    for _ in range(5):
        resp = await anon_client.post("/v1/jobs", json=JOB)
        assert resp.status_code == 401
        assert "X-RateLimit-Limit" not in resp.headers
    async with client_for(api_app, creds.api_key) as c:
        assert (await c.post("/v1/jobs", json=JOB)).status_code == 201


async def test_two_api_replicas_share_one_bucket(
    api_app: FastAPI, migrated_engine: AsyncEngine
) -> None:
    """The guide's proof: burst 100, near-zero refill, 200 concurrent requests across two
    replicas, exactly 100 succeed. Each replica has its own key cache and Redis client."""
    creds = await tenant_with(api_app, rate_per_sec=0.001, burst=100)
    second = create_app()
    async with (
        second.router.lifespan_context(second),
        client_for(api_app, creds.api_key) as a,
        client_for(second, creds.api_key) as b,
    ):
        responses = await asyncio.gather(
            *((a if i % 2 else b).post("/v1/jobs", json=JOB) for i in range(200))
        )
    codes = [r.status_code for r in responses]
    assert (codes.count(201), codes.count(429)) == (100, 100)
    async with migrated_engine.connect() as conn:
        count = text("SELECT count(*) FROM jobs WHERE tenant_id = :t")
        jobs = (await conn.execute(count, {"t": creds.id})).scalar_one()
    assert jobs == 100


# --- backpressure -----------------------------------------------------------------------------


async def test_over_the_tenant_quota_is_429_queue_quota_exceeded(api_app: FastAPI) -> None:
    creds = await tenant_with(api_app, max_queue_depth=3)
    before = counter("hopper_backpressure_rejections_total", reason="quota")
    async with client_for(api_app, creds.api_key) as c:
        for _ in range(3):
            assert (await c.post("/v1/jobs", json=JOB)).status_code == 201
        await publish_depth(api_app)
        refused = await c.post("/v1/jobs", json=JOB)
        assert refused.status_code == 429
        assert refused.json()["error"]["code"] == "queue_quota_exceeded"
        assert refused.headers["Retry-After"] == "5"
        # Reads are never held back by depth.
        assert (await c.get("/v1/jobs")).status_code == 200
    assert counter("hopper_backpressure_rejections_total", reason="quota") - before == 1


async def test_under_the_quota_enqueues_go_through(api_app: FastAPI) -> None:
    creds = await tenant_with(api_app, max_queue_depth=3)
    async with client_for(api_app, creds.api_key) as c:
        await c.post("/v1/jobs", json=JOB)
        await c.post("/v1/jobs", json=JOB)
        await publish_depth(api_app)
        assert (await c.post("/v1/jobs", json=JOB)).status_code == 201  # 2 queued < 3


async def test_another_tenant_is_not_held_back_by_a_full_one(api_app: FastAPI) -> None:
    full = await tenant_with(api_app, max_queue_depth=1)
    other = await tenant_with(api_app, max_queue_depth=1)
    async with client_for(api_app, full.api_key) as f, client_for(api_app, other.api_key) as o:
        await f.post("/v1/jobs", json=JOB)
        await publish_depth(api_app)
        assert (await f.post("/v1/jobs", json=JOB)).status_code == 429
        assert (await o.post("/v1/jobs", json=JOB)).status_code == 201


async def test_the_quota_also_covers_replays(api_app: FastAPI) -> None:
    creds = await tenant_with(api_app, max_queue_depth=1)
    async with client_for(api_app, creds.api_key) as c:
        await c.post("/v1/jobs", json=JOB)
        await publish_depth(api_app)
        one = await c.post(f"/v1/jobs/{uuid.uuid4()}/replay")
        bulk = await c.post("/v1/dlq/replay", json={"filter": {}})
    assert [one.status_code, bulk.status_code] == [429, 429]


async def test_over_the_global_limit_is_503_overloaded(
    migrated_engine: AsyncEngine, migrated_db_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("DATABASE_URL", migrated_db_url)
    monkeypatch.setenv("REDIS_NAMESPACE", f"test-{uuid.uuid4().hex[:12]}")
    monkeypatch.setenv("GLOBAL_MAX_QUEUE_DEPTH", "5")
    get_settings.cache_clear()
    app = create_app()
    try:
        async with app.router.lifespan_context(app):
            creds = await tenant_with(app, max_queue_depth=1000)
            busy = await tenant_with(app)
            await seed_jobs(migrated_engine, busy.id, 5)  # another tenant fills the system
            await publish_depth(app)
            before = counter("hopper_backpressure_rejections_total", reason="overloaded")
            async with client_for(app, creds.api_key) as c:
                refused = await c.post("/v1/jobs", json=JOB)
    finally:
        get_settings.cache_clear()
    assert refused.status_code == 503
    assert refused.json()["error"]["code"] == "overloaded"
    assert refused.headers["Retry-After"] == "5"
    assert counter("hopper_backpressure_rejections_total", reason="overloaded") - before == 1


async def test_no_published_depth_fails_open(api_app: FastAPI) -> None:
    """No scheduler has counted (or its snapshot expired): enqueues are not held back."""
    creds = await tenant_with(api_app, max_queue_depth=1)
    async with client_for(api_app, creds.api_key) as c:
        codes = [(await c.post("/v1/jobs", json=JOB)).status_code for _ in range(3)]
    assert codes == [201, 201, 201]


async def test_a_drained_queue_is_let_through_on_the_next_count(api_app: FastAPI) -> None:
    creds = await tenant_with(api_app, max_queue_depth=1)
    async with client_for(api_app, creds.api_key) as c:
        job = (await c.post("/v1/jobs", json=JOB)).json()
        await publish_depth(api_app)
        assert (await c.post("/v1/jobs", json=JOB)).status_code == 429
        await c.post(f"/v1/jobs/{job['id']}/cancel")
        await publish_depth(api_app)
        assert (await c.post("/v1/jobs", json=JOB)).status_code == 201


# --- login ------------------------------------------------------------------------------------


@pytest.fixture
async def owner(api_app: FastAPI) -> str:
    await store.create_tenant(
        api_app.state.engine,
        name="login-test",
        owner=("owner@login.test", await passwords.hash_password("correct horse battery")),
    )
    return "owner@login.test"


async def test_login_is_throttled_per_email(anon_client: httpx.AsyncClient, owner: str) -> None:
    wrong = {"email": owner, "password": "not the password at all"}
    codes = [(await anon_client.post("/auth/token", json=wrong)).status_code for _ in range(10)]
    assert codes == [401] * 10
    refused = await anon_client.post(
        "/auth/token", json={"email": owner.upper(), "password": "correct horse battery"}
    )
    # Even the right password waits: the bucket is per email, case-insensitive.
    assert refused.status_code == 429
    assert refused.json()["error"]["code"] == "rate_limited"
    assert 1 <= int(refused.headers["Retry-After"]) <= 6  # one attempt every 6 s
    # Another email is unaffected, so the throttle cannot be used to lock out everyone.
    other = await anon_client.post(
        "/auth/token", json={"email": "nobody@login.test", "password": "x" * 12}
    )
    assert other.status_code == 401


async def test_unknown_emails_are_throttled_the_same_way(anon_client: httpx.AsyncClient) -> None:
    """A 429 must not reveal whether an email has an account."""
    body = {"email": "ghost@login.test", "password": "y" * 12}
    codes = [(await anon_client.post("/auth/token", json=body)).status_code for _ in range(11)]
    assert codes == [401] * 10 + [429]


# --- every /v1 route is limited -----------------------------------------------------------------


@pytest.fixture
async def limited_client(api_app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    creds = await tenant_with(api_app, rate_per_sec=1, burst=50)
    async with client_for(api_app, creds.api_key) as c:
        yield c


async def test_every_v1_route_spends_a_token(
    api_app: FastAPI, limited_client: httpx.AsyncClient
) -> None:
    """A route added without ReadTenant or EnqueueTenant would answer without the headers."""
    job = (await limited_client.post("/v1/jobs", json=JOB)).json()
    schedule = (
        await limited_client.post("/v1/schedules", json={"name": "s", "cron": "0 3 * * *", **JOB})
    ).json()
    ids = {"{job_id}": job["id"], "{schedule_id}": schedule["id"]}
    v1 = sorted(
        (method.upper(), path)
        for path, ops in api_app.openapi()["paths"].items()
        if path.startswith("/v1/")
        for method in ops
    )
    assert len(v1) == 12
    for method, path in v1:
        url = path
        for placeholder, value in ids.items():
            url = url.replace(placeholder, value)
        body = {"enabled": True} if method == "PATCH" else JOB if method == "POST" else None
        if path == "/v1/dlq/replay":
            body = {"filter": {}}
        resp = await limited_client.request(method, url, json=body)
        assert "X-RateLimit-Remaining" in resp.headers, (method, path, resp.status_code)
