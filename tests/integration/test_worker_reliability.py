"""The worker side of Week 4: heartbeats, cancelling on a lost lease, graceful shutdown."""

import asyncio
import time
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue.postgres import PostgresBroker
from hopper.scheduler.reaper import Reaper
from hopper.tasks.builtin import SleepPayload
from hopper.tasks.registry import task
from hopper.worker.loop import Worker
from tests.helpers import (
    attempts_of,
    insert_job,
    job_row,
    make_worker,
    seed_tenant,
    status_in,
    wait_for,
)

_cancelled = {"count": 0}


@task("test_cancel_probe", payload=SleepPayload)
async def cancel_probe(payload: SleepPayload) -> dict[str, Any] | None:
    """Sleeps, and records it if it was cancelled instead of finishing."""
    try:
        await asyncio.sleep(payload.ms / 1000)
    except asyncio.CancelledError:
        _cancelled["count"] += 1
        raise
    return {"finished": True}


def inflight_is(worker: Worker, n: int) -> Callable[[], Awaitable[bool]]:
    async def check() -> bool:
        return worker.inflight == n

    return check


async def test_heartbeats_keep_a_job_longer_than_its_lease_alive(
    migrated_engine: AsyncEngine,
) -> None:
    """A 2.5 s job under a 1 s lease, with the reaper running: heartbeats must win."""
    tenant_id = await seed_tenant(migrated_engine)
    job_id = await insert_job(migrated_engine, tenant_id, task="sleep", payload='{"ms": 2500}')
    worker = make_worker(migrated_engine, lease_seconds=1, heartbeat_interval=0.2)
    reaper = Reaper(PostgresBroker(migrated_engine), interval=0.1)
    runners = [asyncio.create_task(worker.run()), asyncio.create_task(reaper.run())]
    try:
        await wait_for(status_in(migrated_engine, job_id, "running"))
        # Without heartbeats the lease would expire, and every rerun too, until dead.
        await wait_for(status_in(migrated_engine, job_id, "succeeded", "dead"), timeout=15)
    finally:
        worker.stop()
        reaper.stop()
        await asyncio.gather(*runners)

    row = await job_row(migrated_engine, job_id)
    assert (row["status"], row["attempts"]) == ("succeeded", 1)
    assert [a["outcome"] for a in await attempts_of(migrated_engine, job_id)] == ["succeeded"]


async def test_a_lost_lease_cancels_the_handler_and_the_job_is_never_acked(
    migrated_engine: AsyncEngine,
) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    job_id = await insert_job(
        migrated_engine, tenant_id, task="test_cancel_probe", payload='{"ms": 10000}'
    )
    _cancelled["count"] = 0
    worker = make_worker(migrated_engine, heartbeat_interval=0.1)
    runner = asyncio.create_task(worker.run())
    try:
        await wait_for(inflight_is(worker, 1))
        # The reaper took the job and another worker claimed it: the token changed.
        async with migrated_engine.begin() as conn:
            new_token = (
                await conn.execute(
                    text(
                        "UPDATE jobs SET lease_token = gen_random_uuid(), lease_owner = 'w2' "
                        "WHERE id = :i RETURNING lease_token"
                    ),
                    {"i": job_id},
                )
            ).scalar_one()
        started = time.monotonic()
        await wait_for(inflight_is(worker, 0), timeout=5)
        assert time.monotonic() - started < 2  # cancelled at the next heartbeat, not after 10 s
    finally:
        worker.stop()
        await runner

    assert _cancelled["count"] == 1
    row = await job_row(migrated_engine, job_id)
    # Untouched: w1 neither acked nor nacked the job it no longer owns.
    assert (row["status"], row["lease_owner"], row["lease_token"]) == ("running", "w2", new_token)
    assert row["result"] is None and row["last_error"] is None
    assert await attempts_of(migrated_engine, job_id) == []


async def test_stop_finishes_short_jobs_and_releases_the_rest_after_the_grace_period(
    migrated_engine: AsyncEngine,
) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    short = await insert_job(migrated_engine, tenant_id, task="sleep", payload='{"ms": 200}')
    long = await insert_job(
        migrated_engine, tenant_id, task="test_cancel_probe", payload='{"ms": 30000}'
    )
    _cancelled["count"] = 0
    worker = make_worker(migrated_engine, shutdown_grace=0.8)
    runner = asyncio.create_task(worker.run())
    await wait_for(inflight_is(worker, 2))

    started = time.monotonic()
    worker.stop()
    await asyncio.wait_for(runner, timeout=5)

    assert 0.7 < time.monotonic() - started < 3  # waited out the grace period, no longer
    assert (await job_row(migrated_engine, short))["status"] == "succeeded"  # done in the grace
    assert _cancelled["count"] == 1
    row = await job_row(migrated_engine, long)
    assert (row["status"], row["attempts"], row["lease_token"]) == ("queued", 0, None)
    assert [(a["attempt"], a["outcome"]) for a in await attempts_of(migrated_engine, long)] == [
        (1, "released")
    ]


async def test_stop_with_zero_grace_releases_at_once(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    job_id = await insert_job(migrated_engine, tenant_id, task="sleep", payload='{"ms": 30000}')
    worker = make_worker(migrated_engine, shutdown_grace=0)
    runner = asyncio.create_task(worker.run())
    await wait_for(inflight_is(worker, 1))

    worker.stop()
    await asyncio.wait_for(runner, timeout=2)

    row = await job_row(migrated_engine, job_id)
    assert (row["status"], row["attempts"]) == ("queued", 0)


async def test_reaper_loop_reclaims_on_its_interval_and_stops_promptly(
    migrated_engine: AsyncEngine,
) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    job_id = await insert_job(migrated_engine, tenant_id, task="sleep")
    await PostgresBroker(migrated_engine).claim("default", "crashed", 1, 0.2)
    reaper = Reaper(PostgresBroker(migrated_engine), interval=0.1)
    runner = asyncio.create_task(reaper.run())
    try:
        await wait_for(status_in(migrated_engine, job_id, "queued"), timeout=5)
    finally:
        reaper.stop()
        await asyncio.wait_for(runner, timeout=1)

    assert (await job_row(migrated_engine, job_id))["last_error"] == "lease expired on crashed"


def test_heartbeat_must_be_shorter_than_the_lease(migrated_engine: AsyncEngine) -> None:
    with pytest.raises(ValueError, match="shorter than the lease"):
        Worker(
            PostgresBroker(migrated_engine),
            worker_id="w",
            queues=["default"],
            lease_seconds=10,
            heartbeat_interval=10,
        )
