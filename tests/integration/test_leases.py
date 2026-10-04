"""Leases at the SQL level: heartbeat, reaper, release, and the poison-pill path to the DLQ."""

import asyncio
import uuid
from dataclasses import replace

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue.postgres import PostgresBroker
from hopper.scheduler.reaper import Reaper
from tests.helpers import LEASE, attempts_of, insert_job, job_row, seed_jobs, seed_tenant

SHORT_LEASE = 0.2  # seconds; long enough to claim, short enough to wait out in a test


async def lease_left(engine: AsyncEngine, job_id: uuid.UUID) -> float:
    """Seconds until the job's lease expires, by the database clock."""
    async with engine.connect() as conn:
        seconds: float = (
            await conn.execute(
                text("SELECT extract(epoch FROM lease_expires_at - now()) FROM jobs WHERE id = :i"),
                {"i": job_id},
            )
        ).scalar_one()
    return seconds


async def test_heartbeat_extends_the_lease(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 1)
    broker = PostgresBroker(migrated_engine)
    [job] = await broker.claim("default", "w1", 1, 5)
    assert await lease_left(migrated_engine, job.id) <= 5

    assert await broker.heartbeat([job], LEASE) == {job.id}

    assert 25 < await lease_left(migrated_engine, job.id) <= 30
    row = await job_row(migrated_engine, job.id)
    assert (row["status"], row["lease_token"], row["attempts"]) == ("running", job.lease_token, 1)


async def test_heartbeat_renews_many_jobs_at_once_and_reports_lost_leases(
    migrated_engine: AsyncEngine,
) -> None:
    """One statement for every job; the ids that come back are the leases still held."""
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 5)
    broker = PostgresBroker(migrated_engine)
    jobs = await broker.claim("default", "w1", 5, 5)
    stolen, finished, *kept = jobs
    async with migrated_engine.begin() as conn:  # reclaimed and claimed by someone else
        await conn.execute(
            text("UPDATE jobs SET lease_token = gen_random_uuid() WHERE id = :i"), {"i": stolen.id}
        )
    assert await broker.ack(finished, "w1", None)

    renewed = await broker.heartbeat(jobs, LEASE)

    assert renewed == {job.id for job in kept}
    assert await lease_left(migrated_engine, stolen.id) <= 5  # the new owner's lease untouched
    assert await broker.heartbeat([], LEASE) == set()


async def test_reaper_requeues_an_expired_lease_and_records_the_attempt(
    migrated_engine: AsyncEngine,
) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 1)
    broker = PostgresBroker(migrated_engine)
    [job] = await broker.claim("default", "w1", 1, SHORT_LEASE)
    await asyncio.sleep(SHORT_LEASE + 0.2)  # the worker "died": no heartbeat, no ack

    [reaped] = await broker.reap(500)

    assert (reaped.id, reaped.status, reaped.attempt, reaped.lease_owner) == (
        job.id,
        "queued",
        1,
        "w1",
    )
    row = await job_row(migrated_engine, job.id)
    assert (row["status"], row["attempts"], row["dead_at"]) == ("queued", 1, None)
    assert (row["lease_owner"], row["lease_token"], row["lease_expires_at"]) == (None, None, None)
    assert row["last_error"] == "lease expired on w1"
    [attempt] = await attempts_of(migrated_engine, job.id)
    assert (attempt["attempt"], attempt["worker_id"], attempt["outcome"], attempt["error"]) == (
        1,
        "w1",
        "lease_expired",
        "lease expired on w1",
    )
    assert [j.id for j in await broker.claim("default", "w2", 1, LEASE)] == [job.id]  # ready now


async def test_reaper_leaves_live_leases_and_other_states_alone(
    migrated_engine: AsyncEngine,
) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    broker = PostgresBroker(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 3)
    live, expired, done = await broker.claim("default", "w1", 3, LEASE)
    assert await broker.ack(done, "w1", None)
    async with migrated_engine.begin() as conn:
        await conn.execute(
            text("UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id = :i"),
            {"i": expired.id},
        )
    waiting = await insert_job(migrated_engine, tenant_id, task="sleep")

    assert [j.id for j in await broker.reap(500)] == [expired.id]

    assert (await job_row(migrated_engine, live.id))["status"] == "running"
    assert (await job_row(migrated_engine, done.id))["status"] == "succeeded"
    assert (await job_row(migrated_engine, waiting))["status"] == "queued"
    assert await broker.reap(500) == []  # nothing left to reclaim


async def test_after_lease_expiry_a_second_worker_owns_the_job_and_the_old_ack_is_fenced(
    migrated_engine: AsyncEngine,
) -> None:
    """The build guide's lease-expiry test: stop heartbeating, run the reaper, and a second
    worker claims with a new token while the first worker's late ack updates 0 rows."""
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 1)
    broker = PostgresBroker(migrated_engine)
    [first] = await broker.claim("default", "w1", 1, SHORT_LEASE)
    await asyncio.sleep(SHORT_LEASE + 0.2)
    await broker.reap(500)

    [second] = await broker.claim("default", "w2", 1, LEASE)
    assert second.id == first.id
    assert second.lease_token != first.lease_token
    assert second.attempt == 2

    assert await broker.ack(first, "w1", {"by": "zombie"}) is False  # w1 woke up too late
    row = await job_row(migrated_engine, first.id)
    assert (row["status"], row["lease_owner"], row["result"]) == ("running", "w2", None)

    assert await broker.ack(second, "w2", {"by": "w2"}) is True
    outcomes = [
        (a["attempt"], a["worker_id"], a["outcome"])
        for a in await attempts_of(migrated_engine, first.id)
    ]
    assert outcomes == [(1, "w1", "lease_expired"), (2, "w2", "succeeded")]


async def test_poison_pill_is_dead_after_max_attempts_of_lease_expiry(
    migrated_engine: AsyncEngine,
) -> None:
    """A job that kills its worker every time still ends in the DLQ instead of looping."""
    tenant_id = await seed_tenant(migrated_engine)
    job_id = await insert_job(migrated_engine, tenant_id, task="sleep", max_attempts=3)
    broker = PostgresBroker(migrated_engine)

    statuses = []
    for n in range(1, 4):
        assert len(await broker.claim("default", f"w{n}", 1, SHORT_LEASE)) == 1
        await asyncio.sleep(SHORT_LEASE + 0.2)  # worker n crashes mid-job
        [reaped] = await broker.reap(500)
        statuses.append(reaped.status)

    assert statuses == ["queued", "queued", "dead"]
    row = await job_row(migrated_engine, job_id)
    assert (row["status"], row["attempts"], row["last_error"]) == ("dead", 3, "lease expired on w3")
    assert row["dead_at"] is not None
    outcomes = [(a["attempt"], a["outcome"]) for a in await attempts_of(migrated_engine, job_id)]
    assert outcomes == [(1, "lease_expired"), (2, "lease_expired"), (3, "lease_expired")]
    assert await broker.claim("default", "w4", 1, LEASE) == []  # dead jobs are not claimed


async def test_reaper_works_in_batches(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 7)
    broker = PostgresBroker(migrated_engine)
    await broker.claim("default", "w1", 7, SHORT_LEASE)
    await asyncio.sleep(SHORT_LEASE + 0.2)

    assert len(await broker.reap(3)) == 3  # one statement never takes more than its limit
    assert await Reaper(broker, batch_size=3).reap_once() == 4  # a pass loops until done
    async with migrated_engine.connect() as conn:
        statuses = (await conn.execute(text("SELECT DISTINCT status FROM jobs"))).scalars().all()
    assert statuses == ["queued"]


async def test_concurrent_reapers_reclaim_each_job_exactly_once(
    migrated_engine: AsyncEngine,
) -> None:
    """Two scheduler replicas are safe: SKIP LOCKED gives each reaper disjoint rows."""
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 300)
    broker = PostgresBroker(migrated_engine)
    assert len(await broker.claim("default", "w1", 300, SHORT_LEASE)) == 300
    await asyncio.sleep(SHORT_LEASE + 0.2)

    totals = await asyncio.gather(*(Reaper(broker, batch_size=20).reap_once() for _ in range(5)))

    assert sum(totals) == 300
    async with migrated_engine.connect() as conn:
        rows, jobs = (
            await conn.execute(
                text(
                    "SELECT count(*), count(DISTINCT job_id) FROM job_attempts "
                    "WHERE outcome = 'lease_expired'"
                )
            )
        ).one()
    assert rows == jobs == 300  # one lease_expired row per job: none reaped twice


async def test_release_requeues_without_using_an_attempt(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 2)
    broker = PostgresBroker(migrated_engine)
    jobs = await broker.claim("default", "w1", 2, LEASE)

    assert await broker.release(jobs, "w1") == {job.id for job in jobs}

    for job in jobs:
        row = await job_row(migrated_engine, job.id)
        assert (row["status"], row["attempts"], row["last_error"]) == ("queued", 0, None)
        assert (row["lease_owner"], row["lease_token"], row["lease_expires_at"]) == (
            None,
            None,
            None,
        )
        [attempt] = await attempts_of(migrated_engine, job.id)
        assert (attempt["attempt"], attempt["worker_id"], attempt["outcome"]) == (
            1,
            "w1",
            "released",
        )
    again = await broker.claim("default", "w2", 2, LEASE)
    assert sorted(j.attempt for j in again) == [1, 1]  # the next run reuses attempt 1


async def test_release_with_a_lost_lease_changes_nothing(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 1)
    broker = PostgresBroker(migrated_engine)
    [job] = await broker.claim("default", "w1", 1, LEASE)

    assert await broker.release([replace(job, lease_token=uuid.uuid4())], "w1") == set()

    row = await job_row(migrated_engine, job.id)
    assert (row["status"], row["attempts"], row["lease_token"]) == ("running", 1, job.lease_token)
    assert await attempts_of(migrated_engine, job.id) == []
    assert await broker.release([], "w1") == set()
