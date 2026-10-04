import asyncio
import time
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue.postgres import PostgresBroker
from hopper.tasks.builtin import SleepPayload
from hopper.tasks.registry import task
from hopper.worker.loop import Worker
from tests.helpers import counts, insert_job, make_worker, run_until, seed_jobs, seed_tenant

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


async def test_failed_handler_is_nacked_for_retry_and_the_worker_carries_on(
    migrated_engine: AsyncEngine,
) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    bad = await insert_job(migrated_engine, tenant_id, task="flaky", payload='{"p": 1}')
    await seed_jobs(migrated_engine, tenant_id, 20)
    worker = make_worker(migrated_engine)

    await run_until([worker], migrated_engine, lambda c: c.get("succeeded") == 20)

    async with migrated_engine.connect() as conn:
        job = (
            await conn.execute(
                text("SELECT status, run_at > now() AS later, last_error FROM jobs WHERE id = :i"),
                {"i": bad},
            )
        ).one()
        outcomes = (
            (
                await conn.execute(
                    text("SELECT outcome FROM job_attempts WHERE job_id = :i"), {"i": bad}
                )
            )
            .scalars()
            .all()
        )
    # flaky uses the default 2 s backoff base, so it may have run more than once by now,
    # but it is never acked and is always either waiting to retry or running again.
    assert job.status in {"queued", "running"}
    assert job.last_error == "RuntimeError: flaky task failed"
    assert outcomes and set(outcomes) == {"failed"}


async def test_handler_timeout_is_retried_as_timed_out(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    slow = await insert_job(
        migrated_engine, tenant_id, task="sleep", payload='{"ms": 5000}', timeout_seconds=1
    )
    worker = make_worker(migrated_engine)
    started = time.monotonic()

    await run_until(
        [worker], migrated_engine, lambda c: c == {"queued": 1} and time.monotonic() - started > 0.5
    )

    assert time.monotonic() - started < 4  # cancelled at the 1 s deadline, not after 5 s
    async with migrated_engine.connect() as conn:
        last_error = (
            await conn.execute(text("SELECT last_error FROM jobs WHERE id = :i"), {"i": slow})
        ).scalar_one()
        outcome = (
            await conn.execute(
                text("SELECT outcome FROM job_attempts WHERE job_id = :i"), {"i": slow}
            )
        ).scalar_one()
    assert outcome == "timed_out"
    assert last_error == "JobTimedOut: timed out after 1s"


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
