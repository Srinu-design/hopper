"""The scheduler's depth loop: counts by queue and state, the oldest ready job, the per-tenant
snapshot it publishes to Redis for backpressure, and what happens when Redis is down."""

import os
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta

import pytest
import redis.asyncio as redis_asyncio
from prometheus_client import REGISTRY
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.config import Settings, get_settings
from hopper.db import create_redis
from hopper.queue import sql
from hopper.scheduler.depth import DepthLoop
from tests.helpers import insert_job, seed_jobs, seed_tenant


@pytest.fixture
async def redis() -> AsyncIterator[redis_asyncio.Redis]:
    client = create_redis(get_settings())
    yield client
    await client.aclose()


@pytest.fixture
def namespace() -> str:
    return f"test-{uuid.uuid4().hex[:12]}"


def gauge(name: str, **labels: str) -> float | None:
    return REGISTRY.get_sample_value(name, labels)


async def test_depth_by_queue_and_state(
    migrated_engine: AsyncEngine, redis: redis_asyncio.Redis, namespace: str
) -> None:
    tenant = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant, 2)  # ready
    await seed_jobs(migrated_engine, tenant, 3, run_at_offset=timedelta(hours=1))  # delayed
    await insert_job(migrated_engine, tenant, task="sleep", status="running")
    await insert_job(migrated_engine, tenant, task="sleep", status="dead")
    await insert_job(
        migrated_engine, tenant, task="sleep", status="succeeded"
    )  # not counted anywhere

    depth = await DepthLoop(migrated_engine, redis, namespace=namespace).tick()

    expected = {"ready": 2, "delayed": 3, "running": 1, "dead": 1}
    assert {s: depth.by_queue[("default", s)] for s in expected} == expected
    for state, jobs in expected.items():
        assert gauge("hopper_queue_depth", queue="default", state=state) == jobs


async def test_queues_no_worker_serves_are_counted_as_other(
    migrated_engine: AsyncEngine, redis: redis_asyncio.Redis, namespace: str
) -> None:
    """Tenants name queues freely; each name becoming a label would be unbounded."""
    tenant = await seed_tenant(migrated_engine)
    for queue in ("reports", "emails", f"q-{uuid.uuid4().hex}"):
        await seed_jobs(migrated_engine, tenant, 1, queue=queue)
    depth = await DepthLoop(migrated_engine, redis, namespace=namespace).tick()
    assert depth.by_queue[("other", "ready")] == 3
    assert {queue for queue, _ in depth.by_queue} == {"default", "other"}
    assert gauge("hopper_queue_depth", queue="other", state="ready") == 3


async def test_oldest_ready_job_age(
    migrated_engine: AsyncEngine, redis: redis_asyncio.Redis, namespace: str
) -> None:
    tenant = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant, 1, run_at_offset=timedelta(seconds=-90))
    await seed_jobs(migrated_engine, tenant, 1, run_at_offset=timedelta(seconds=-5))
    await seed_jobs(migrated_engine, tenant, 1, run_at_offset=timedelta(hours=2))  # not ready
    depth = await DepthLoop(migrated_engine, redis, namespace=namespace).tick()
    assert 89 <= depth.oldest_ready_seconds["default"] <= 95
    assert depth.oldest_ready_seconds["other"] == 0
    assert 89 <= (gauge("hopper_oldest_ready_job_age_seconds", queue="default") or 0) <= 95


async def test_gauges_fall_to_zero_when_a_queue_drains(
    migrated_engine: AsyncEngine, redis: redis_asyncio.Redis, namespace: str
) -> None:
    tenant = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant, 4)
    loop = DepthLoop(migrated_engine, redis, namespace=namespace)
    await loop.tick()
    assert gauge("hopper_queue_depth", queue="default", state="ready") == 4
    async with migrated_engine.begin() as conn:
        await conn.execute(text("UPDATE jobs SET status = 'succeeded'"))
    await loop.tick()
    assert gauge("hopper_queue_depth", queue="default", state="ready") == 0
    assert gauge("hopper_oldest_ready_job_age_seconds", queue="default") == 0


async def test_queued_jobs_per_tenant_are_published_for_backpressure(
    migrated_engine: AsyncEngine, redis: redis_asyncio.Redis, namespace: str
) -> None:
    a = await seed_tenant(migrated_engine, "a")
    b = await seed_tenant(migrated_engine, "b")
    idle = await seed_tenant(migrated_engine, "idle")
    await seed_jobs(migrated_engine, a, 3)
    await seed_jobs(migrated_engine, a, 2, queue="elsewhere", run_at_offset=timedelta(hours=1))
    await seed_jobs(migrated_engine, b, 1)
    await insert_job(migrated_engine, b, task="sleep", status="running")  # running is not queued

    await DepthLoop(migrated_engine, redis, namespace=namespace).tick()

    snapshot = await redis.hgetall(f"{namespace}:depth")
    assert snapshot == {b"total": b"6", str(a).encode(): b"5", str(b).encode(): b"1"}
    assert str(idle).encode() not in snapshot  # no field means none queued
    assert 0 < await redis.pttl(f"{namespace}:depth") <= 15_000


async def test_the_snapshot_is_replaced_whole(
    migrated_engine: AsyncEngine, redis: redis_asyncio.Redis, namespace: str
) -> None:
    """A tenant whose queue drained must lose its field, not keep its old count."""
    tenant = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant, 2)
    loop = DepthLoop(migrated_engine, redis, namespace=namespace)
    await loop.tick()
    async with migrated_engine.begin() as conn:
        await conn.execute(text("UPDATE jobs SET status = 'cancelled'"))
    await loop.tick()
    assert await redis.hgetall(f"{namespace}:depth") == {b"total": b"0"}


async def test_redis_down_still_updates_the_gauges(migrated_engine: AsyncEngine) -> None:
    unreachable = create_redis(Settings(redis_url="redis://127.0.0.1:1/0"))
    tenant = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant, 7)
    try:
        depth = await DepthLoop(migrated_engine, unreachable, namespace="unused").tick()
    finally:
        await unreachable.aclose()
    assert depth.total_queued == 7
    assert gauge("hopper_queue_depth", queue="default", state="ready") == 7


async def test_the_depth_queries_read_the_partial_indexes(migrated_engine: AsyncEngine) -> None:
    """With many finished rows, counting must not scan the whole jobs table every second."""
    tenant = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant, 20_000)
    async with migrated_engine.begin() as conn:
        await conn.execute(text("UPDATE jobs SET status = 'succeeded'"))
        await conn.execute(text("ANALYZE jobs"))
    await seed_jobs(migrated_engine, tenant, 10)
    async with migrated_engine.connect() as conn:
        plans = [
            "\n".join(r[0] for r in await conn.execute(text(f"EXPLAIN {query}")))
            for query in (sql.QUEUED_DEPTH, sql.RUNNING_AND_DEAD_DEPTH)
        ]
    assert "jobs_ready_idx" in plans[0], plans[0]
    assert "jobs_lease_idx" in plans[1] and "jobs_dead_idx" in plans[1], plans[1]
    assert "Seq Scan" not in plans[0] + plans[1]


def test_redis_url_in_tests_is_the_local_one() -> None:
    # Sanity check for the fixtures above: they talk to the same Redis as the API tests.
    assert get_settings().redis_url == os.environ["REDIS_URL"]
