import asyncio
import uuid
from dataclasses import replace
from datetime import timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue.postgres import PostgresBroker
from tests.helpers import LEASE, job_row, seed_jobs, seed_tenant


async def test_no_double_claim_under_concurrency(migrated_engine: AsyncEngine) -> None:
    """10 concurrent claimers against 1,000 jobs: every job id is claimed exactly once."""
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 1000)
    broker = PostgresBroker(migrated_engine)

    async def claimer(worker_id: str) -> list[uuid.UUID]:
        claimed: list[uuid.UUID] = []
        while True:
            batch = await broker.claim("default", worker_id, 7, LEASE)
            if not batch:
                return claimed
            claimed.extend(job.id for job in batch)

    results = await asyncio.gather(*(claimer(f"w{i}") for i in range(10)))
    all_ids = [job_id for ids in results for job_id in ids]

    assert len(all_ids) == 1000
    assert len(set(all_ids)) == 1000  # no duplicates
    assert sum(1 for ids in results if ids) > 1  # the work really was shared
    async with migrated_engine.connect() as conn:
        statuses = (
            await conn.execute(text("SELECT status, count(*) FROM jobs GROUP BY status"))
        ).all()
    assert [tuple(r) for r in statuses] == [("running", 1000)]


async def test_claim_sets_lease_and_attempt(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 1)
    [job] = await PostgresBroker(migrated_engine).claim("default", "w1", 5, LEASE)

    row = await job_row(migrated_engine, job.id)
    assert row["status"] == "running"
    assert row["attempts"] == 1 == job.attempt
    assert row["lease_owner"] == "w1"
    assert row["lease_token"] == job.lease_token
    assert row["started_at"] is not None
    async with migrated_engine.connect() as conn:
        seconds = (
            await conn.execute(
                text("SELECT extract(epoch FROM lease_expires_at - now()) FROM jobs")
            )
        ).scalar_one()
    assert 25 < seconds <= 30  # lease is ~30 s from the database clock


async def test_claim_respects_limit_queue_and_run_at(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 5, queue="default")
    await seed_jobs(migrated_engine, tenant_id, 3, queue="other")
    await seed_jobs(
        migrated_engine, tenant_id, 4, queue="default", run_at_offset=timedelta(hours=1)
    )
    broker = PostgresBroker(migrated_engine)

    assert len(await broker.claim("default", "w", 2, LEASE)) == 2  # limit
    assert len(await broker.claim("default", "w", 100, LEASE)) == 3  # rest of ready; future skipped
    assert await broker.claim("default", "w", 100, LEASE) == []
    assert len(await broker.claim("other", "w", 100, LEASE)) == 3  # queues are independent
    assert await broker.claim("default", "w", 0, LEASE) == []


async def test_claim_takes_highest_priority_first(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 3, priority=1)
    await seed_jobs(migrated_engine, tenant_id, 1, priority=20)
    await seed_jobs(migrated_engine, tenant_id, 2, priority=5)
    broker = PostgresBroker(migrated_engine)

    order = []
    for _ in range(6):
        [job] = await broker.claim("default", "w", 1, LEASE)
        order.append((await job_row(migrated_engine, job.id))["priority"])
    assert order == [20, 5, 5, 1, 1, 1]


async def test_claim_takes_oldest_run_at_first_within_a_priority(
    migrated_engine: AsyncEngine,
) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    async with migrated_engine.begin() as conn:
        ids = []
        for minutes_ago in (1, 30, 10):
            row = await conn.execute(
                text(
                    "INSERT INTO jobs (tenant_id, task, run_at) "
                    "VALUES (:t, 'sleep', now() - make_interval(mins => :m)) RETURNING id"
                ),
                {"t": tenant_id, "m": minutes_ago},
            )
            ids.append(row.scalar_one())
    broker = PostgresBroker(migrated_engine)

    claimed = [(await broker.claim("default", "w", 1, LEASE))[0].id for _ in range(3)]
    assert claimed == [ids[1], ids[2], ids[0]]  # 30, 10, then 1 minute ago


async def test_ack_marks_succeeded_and_records_attempt(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 1)
    broker = PostgresBroker(migrated_engine)
    [job] = await broker.claim("default", "w1", 1, LEASE)

    assert await broker.ack(job, "w1", {"answer": 42}) is True

    row = await job_row(migrated_engine, job.id)
    assert row["status"] == "succeeded"
    assert row["finished_at"] is not None
    assert (row["lease_owner"], row["lease_token"], row["lease_expires_at"]) == (None, None, None)
    assert row["result"] == {"answer": 42}
    async with migrated_engine.connect() as conn:
        attempts = (
            await conn.execute(
                text("SELECT attempt, worker_id, outcome FROM job_attempts WHERE job_id = :i"),
                {"i": job.id},
            )
        ).all()
    assert [tuple(a) for a in attempts] == [(1, "w1", "succeeded")]


async def test_ack_with_wrong_token_changes_nothing(migrated_engine: AsyncEngine) -> None:
    """A worker that lost its lease cannot ack: the fencing token no longer matches."""
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 1)
    broker = PostgresBroker(migrated_engine)
    [job] = await broker.claim("default", "w1", 1, LEASE)

    stale_job = replace(job, lease_token=uuid.uuid4())
    assert await broker.ack(stale_job, "w2", None) is False

    row = await job_row(migrated_engine, job.id)
    assert row["status"] == "running"
    assert row["lease_token"] == job.lease_token
    async with migrated_engine.connect() as conn:
        count = (await conn.execute(text("SELECT count(*) FROM job_attempts"))).scalar_one()
    assert count == 0


async def test_double_ack_is_rejected(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 1)
    broker = PostgresBroker(migrated_engine)
    [job] = await broker.claim("default", "w1", 1, LEASE)

    assert await broker.ack(job, "w1", None) is True
    assert await broker.ack(job, "w1", None) is False
    async with migrated_engine.connect() as conn:
        count = (await conn.execute(text("SELECT count(*) FROM job_attempts"))).scalar_one()
    assert count == 1


async def test_concurrent_acks_of_the_same_job_succeed_exactly_once(
    migrated_engine: AsyncEngine,
) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 1)
    broker = PostgresBroker(migrated_engine)
    [job] = await broker.claim("default", "w1", 1, LEASE)

    outcomes = await asyncio.gather(*(broker.ack(job, "w1", None) for _ in range(10)))
    assert outcomes.count(True) == 1
    async with migrated_engine.connect() as conn:
        count = (await conn.execute(text("SELECT count(*) FROM job_attempts"))).scalar_one()
    assert count == 1
