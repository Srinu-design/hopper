import asyncio
import time
import uuid
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue.postgres import PostgresBroker
from hopper.tasks.builtin import SleepPayload
from hopper.tasks.registry import task
from hopper.worker.loop import Worker
from tests.integration.test_claim import seed_jobs, seed_tenant

_probe = {"current": 0, "max": 0}


@task("test_probe", payload=SleepPayload)
async def probe(payload: SleepPayload) -> dict[str, Any] | None:
    """Records how many copies run at once, to prove the slot limit holds."""
    _probe["current"] += 1
    _probe["max"] = max(_probe["max"], _probe["current"])
    try:
        await asyncio.sleep(payload.ms / 1000)
    finally:
        _probe["current"] -= 1
    return None


def make_worker(engine: AsyncEngine, name: str = "w1", slots: int = 10) -> Worker:
    return Worker(
        PostgresBroker(engine),
        worker_id=name,
        queues=["default"],
        slots=slots,
        poll_interval=0.02,
        max_idle_backoff=0.1,
    )


async def counts(engine: AsyncEngine) -> dict[str, int]:
    async with engine.connect() as conn:
        rows = await conn.execute(text("SELECT status, count(*) FROM jobs GROUP BY status"))
        return {status: n for status, n in rows}


async def run_until(
    workers: list[Worker], engine: AsyncEngine, done: Any, timeout: float = 20.0
) -> None:
    """Run workers until done(counts) is true, then stop them and wait for them to exit."""
    runners = [asyncio.create_task(w.run()) for w in workers]
    deadline = time.monotonic() + timeout
    try:
        while not done(await counts(engine)):
            assert time.monotonic() < deadline, f"timed out; counts={await counts(engine)}"
            await asyncio.sleep(0.05)
    finally:
        for w in workers:
            w.stop()
        await asyncio.gather(*runners)


async def insert_job(engine: AsyncEngine, tenant_id: uuid.UUID, **cols: Any) -> uuid.UUID:
    cols = {"queue": "default", "payload": '{"ms": 0}', **cols}
    names = ", ".join(["tenant_id", *cols])
    params = ", ".join(
        [":tenant_id", *(f"CAST(:{c} AS jsonb)" if c == "payload" else f":{c}" for c in cols)]
    )
    async with engine.begin() as conn:
        row = await conn.execute(
            text(f"INSERT INTO jobs ({names}) VALUES ({params}) RETURNING id"),
            {"tenant_id": tenant_id, **cols},
        )
        job_id: uuid.UUID = row.scalar_one()
    return job_id


async def test_worker_drains_the_queue(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 200)
    worker = make_worker(migrated_engine)

    await run_until([worker], migrated_engine, lambda c: c == {"succeeded": 200})

    async with migrated_engine.connect() as conn:
        attempts = (await conn.execute(text("SELECT count(*) FROM job_attempts"))).scalar_one()
        distinct = (
            await conn.execute(text("SELECT count(DISTINCT job_id) FROM job_attempts"))
        ).scalar_one()
    assert attempts == distinct == 200  # exactly one successful attempt per job


async def test_two_workers_share_the_queue_without_duplicate_runs(
    migrated_engine: AsyncEngine,
) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 300)
    workers = [make_worker(migrated_engine, "wa", slots=5), make_worker(migrated_engine, "wb", 5)]

    await run_until(workers, migrated_engine, lambda c: c == {"succeeded": 300})

    async with migrated_engine.connect() as conn:
        by_worker = dict(
            (
                await conn.execute(text("SELECT worker_id, count(*) FROM job_attempts GROUP BY 1"))
            ).all()
        )
        max_attempts = (await conn.execute(text("SELECT max(attempts) FROM jobs"))).scalar_one()
    assert sum(by_worker.values()) == 300
    assert set(by_worker) == {"wa", "wb"}  # both did work
    assert max_attempts == 1


async def test_in_flight_jobs_never_exceed_the_slot_limit(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    for _ in range(40):
        await insert_job(migrated_engine, tenant_id, task="test_probe", payload='{"ms": 40}')
    _probe.update(current=0, max=0)
    worker = make_worker(migrated_engine, slots=5)

    await run_until([worker], migrated_engine, lambda c: c == {"succeeded": 40})

    assert _probe["max"] == 5  # saturated the slots, never went over


async def test_failed_handler_is_not_acked_and_does_not_stop_the_worker(
    migrated_engine: AsyncEngine,
) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    bad = await insert_job(migrated_engine, tenant_id, task="flaky", payload='{"p": 1}')
    await seed_jobs(migrated_engine, tenant_id, 20)
    worker = make_worker(migrated_engine)

    await run_until([worker], migrated_engine, lambda c: c.get("succeeded") == 20)

    async with migrated_engine.connect() as conn:
        status = (
            await conn.execute(text("SELECT status FROM jobs WHERE id = :i"), {"i": bad})
        ).scalar_one()
        attempts = (
            await conn.execute(
                text("SELECT count(*) FROM job_attempts WHERE job_id = :i"), {"i": bad}
            )
        ).scalar_one()
    assert status == "running"  # never acked; Week 3/4 will retry or reclaim it
    assert attempts == 0


async def test_handler_timeout_is_not_acked(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    slow = await insert_job(
        migrated_engine, tenant_id, task="sleep", payload='{"ms": 5000}', timeout_seconds=1
    )
    await seed_jobs(migrated_engine, tenant_id, 3)
    worker = make_worker(migrated_engine)
    runner = asyncio.create_task(worker.run())
    try:
        deadline = time.monotonic() + 5
        while (await counts(migrated_engine)).get("succeeded") != 3:
            assert time.monotonic() < deadline
            await asyncio.sleep(0.05)
        await asyncio.sleep(1.3)  # past the 1 s timeout, well before the 5 s sleep ends
        assert worker.inflight == 0  # the timed-out handler was cancelled, freeing its slot
    finally:
        worker.stop()
        await runner
    async with migrated_engine.connect() as conn:
        status = (
            await conn.execute(text("SELECT status FROM jobs WHERE id = :i"), {"i": slow})
        ).scalar_one()
    assert status == "running"  # never acked


async def test_stop_waits_for_in_flight_jobs_then_returns(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    job_id = await insert_job(migrated_engine, tenant_id, task="sleep", payload='{"ms": 400}')
    worker = make_worker(migrated_engine)
    runner = asyncio.create_task(worker.run())
    async with asyncio.timeout(5):
        while worker.inflight == 0:
            await asyncio.sleep(0.01)

    worker.stop()
    await asyncio.wait_for(runner, timeout=5)

    async with migrated_engine.connect() as conn:
        status = (
            await conn.execute(text("SELECT status FROM jobs WHERE id = :i"), {"i": job_id})
        ).scalar_one()
    assert status == "succeeded"  # finished before run() returned


async def test_stopped_worker_claims_nothing_more(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    worker = make_worker(migrated_engine)
    worker.stop()
    await seed_jobs(migrated_engine, tenant_id, 5)
    await asyncio.wait_for(worker.run(), timeout=5)
    assert await counts(migrated_engine) == {"queued": 5}


async def test_idle_worker_survives_an_empty_queue(migrated_engine: AsyncEngine) -> None:
    worker = make_worker(migrated_engine)
    runner = asyncio.create_task(worker.run())
    await asyncio.sleep(0.3)
    assert not runner.done()
    worker.stop()
    await asyncio.wait_for(runner, timeout=5)


def test_worker_needs_a_queue(migrated_engine: AsyncEngine) -> None:
    with pytest.raises(ValueError, match="at least one queue"):
        Worker(PostgresBroker(migrated_engine), worker_id="w", queues=[])
