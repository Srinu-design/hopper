"""Retention: finished jobs past the retention period are deleted with their attempt rows,
which frees their idempotency keys. Queued, running, dead and recent jobs are never touched."""

import asyncio
import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue import jobs as job_store
from hopper.queue.postgres import PostgresBroker
from hopper.scheduler.retention import Retention, delete_finished
from tests.helpers import LEASE, attempts_of, insert_job, seed_jobs, seed_tenant, wait_for


async def finished_days_ago(engine: AsyncEngine, days: int, *job_ids: uuid.UUID) -> None:
    """Move the jobs' finish time `days` into the past, by the database clock."""
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE jobs SET finished_at = now() - make_interval(days => CAST(:d AS integer)) "
                "WHERE id = ANY(CAST(:ids AS uuid[]))"
            ),
            {"d": days, "ids": list(job_ids)},
        )


async def seed_finished(engine: AsyncEngine, tenant_id: uuid.UUID, count: int, days: int) -> None:
    """`count` jobs that succeeded `days` ago."""
    await seed_jobs(engine, tenant_id, count)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE jobs SET status = 'succeeded', "
                "finished_at = now() - make_interval(days => CAST(:d AS integer)) "
                "WHERE status = 'queued'"
            ),
            {"d": days},
        )


async def job_ids(engine: AsyncEngine) -> set[uuid.UUID]:
    async with engine.connect() as conn:
        return set((await conn.execute(text("SELECT id FROM jobs"))).scalars())


async def test_deletes_old_finished_jobs_with_their_attempts_and_nothing_else(
    migrated_engine: AsyncEngine,
) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    broker = PostgresBroker(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 4)
    old_done, new_done, almost, running = await broker.claim("default", "w1", 4, LEASE)
    for job in (old_done, new_done, almost):
        assert await broker.ack(job, "w1", None)
    old_cancelled = await insert_job(migrated_engine, tenant_id, task="sleep")
    assert await job_store.cancel_job(migrated_engine, tenant_id=tenant_id, job_id=old_cancelled)
    old_queued = await insert_job(migrated_engine, tenant_id, task="sleep")
    dead = await insert_job(migrated_engine, tenant_id, task="sleep")
    async with migrated_engine.begin() as conn:
        # Old enough by every clock, and the dead job even carries a finish time: only the
        # status keeps them, which is the point.
        await conn.execute(
            text(
                "UPDATE jobs SET created_at = now() - interval '30 days', "
                "run_at = now() - interval '30 days' WHERE id IN (:q, :d)"
            ),
            {"q": old_queued, "d": dead},
        )
        await conn.execute(
            text(
                "UPDATE jobs SET status = 'dead', dead_at = now() - interval '30 days', "
                "finished_at = now() - interval '30 days' WHERE id = :d"
            ),
            {"d": dead},
        )
    await finished_days_ago(migrated_engine, 8, old_done.id, old_cancelled)
    await finished_days_ago(migrated_engine, 6, almost.id)  # inside the 7 days: kept
    assert len(await attempts_of(migrated_engine, old_done.id)) == 1
    before = await job_ids(migrated_engine)

    assert await Retention(migrated_engine, days=7).delete_once() == 2

    assert await job_ids(migrated_engine) == before - {old_done.id, old_cancelled}
    assert await attempts_of(migrated_engine, old_done.id) == []  # gone with the job
    assert len(await attempts_of(migrated_engine, new_done.id)) == 1
    assert {running.id, old_queued, dead} <= await job_ids(migrated_engine)
    assert await Retention(migrated_engine, days=7).delete_once() == 0  # nothing left to delete


async def test_retention_frees_the_idempotency_key(migrated_engine: AsyncEngine) -> None:
    """The guide's rule: an idempotency key lives as long as its job row."""
    tenant_id = await seed_tenant(migrated_engine)
    broker = PostgresBroker(migrated_engine)
    enqueue: dict[str, Any] = {
        "tenant_id": tenant_id,
        "queue": "default",
        "task": "sleep",
        "payload": {"ms": 0},
        "priority": 0,
        "max_attempts": 5,
        "timeout_seconds": 30,
        "idempotency_key": "order-42",
        "request_hash": b"same body",
    }
    first, created = await job_store.insert_job(migrated_engine, **enqueue)
    assert created
    [job] = await broker.claim("default", "w1", 1, LEASE)
    assert await broker.ack(job, "w1", None)
    again, created = await job_store.insert_job(migrated_engine, **enqueue)
    assert (again["id"], created) == (first["id"], False)  # within retention: still taken

    await finished_days_ago(migrated_engine, 8, first["id"])
    assert await Retention(migrated_engine, days=7).delete_once() == 1

    fresh, created = await job_store.insert_job(migrated_engine, **enqueue)
    assert created
    assert fresh["id"] != first["id"]


async def test_retention_works_in_batches(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_finished(migrated_engine, tenant_id, 7, days=8)

    # One statement never takes more than its limit; a pass loops until done.
    assert await delete_finished(migrated_engine, days=7, limit=3) == 3
    assert await Retention(migrated_engine, days=7, batch_size=3).delete_once() == 4
    assert await job_ids(migrated_engine) == set()


async def test_concurrent_retention_deletes_each_job_once(migrated_engine: AsyncEngine) -> None:
    """Two scheduler replicas are safe: SKIP LOCKED gives each pass disjoint rows."""
    tenant_id = await seed_tenant(migrated_engine)
    await seed_finished(migrated_engine, tenant_id, 300, days=8)

    totals = await asyncio.gather(
        *(Retention(migrated_engine, days=7, batch_size=20).delete_once() for _ in range(5))
    )

    assert sum(totals) == 300
    assert await job_ids(migrated_engine) == set()


async def test_a_stop_ends_a_long_pass_after_the_current_batch(
    migrated_engine: AsyncEngine,
) -> None:
    """A big backlog must not hold up a scheduler's shutdown."""
    tenant_id = await seed_tenant(migrated_engine)
    await seed_finished(migrated_engine, tenant_id, 10, days=8)
    retention = Retention(migrated_engine, days=7, batch_size=3)
    retention.stop()

    assert await retention.delete_once() == 3
    assert len(await job_ids(migrated_engine)) == 7


async def test_retention_loop_runs_on_its_interval_and_stops_promptly(
    migrated_engine: AsyncEngine,
) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    retention = Retention(migrated_engine, days=7, interval=0.1)
    runner = asyncio.create_task(retention.run())

    async def all_deleted() -> bool:
        return await job_ids(migrated_engine) == set()

    try:
        await seed_finished(migrated_engine, tenant_id, 3, days=8)
        await wait_for(all_deleted, timeout=5)
    finally:
        retention.stop()
        await asyncio.wait_for(runner, timeout=1)


async def test_hourly_retention_loop_stops_without_waiting_out_the_hour(
    migrated_engine: AsyncEngine,
) -> None:
    retention = Retention(migrated_engine)  # the defaults: 7 days, every hour
    runner = asyncio.create_task(retention.run())
    await asyncio.sleep(0.2)

    retention.stop()
    await asyncio.wait_for(runner, timeout=1)
