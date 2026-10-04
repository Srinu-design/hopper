"""Retries with full-jitter backoff, permanent errors and the move to the DLQ."""

import time
import uuid
from dataclasses import replace
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue.postgres import PostgresBroker
from hopper.tasks.errors import PermanentError, RetryableError
from hopper.tasks.registry import TaskPayload, task
from tests.helpers import (
    LEASE,
    insert_job,
    job_row,
    make_worker,
    run_until,
    seed_jobs,
    seed_tenant,
)


class NoPayload(TaskPayload):
    pass


@task("test_always_fails", payload=NoPayload, max_attempts=3, backoff_base=0.01, backoff_cap=0.05)
async def always_fails(payload: NoPayload) -> dict[str, Any] | None:
    raise RuntimeError("boom")


@task("test_backoff_probe", payload=NoPayload, max_attempts=10, backoff_base=10, backoff_cap=600)
async def backoff_probe(payload: NoPayload) -> dict[str, Any] | None:
    raise RuntimeError("still down")


@task("test_permanent", payload=NoPayload)
async def permanent(payload: NoPayload) -> dict[str, Any] | None:
    raise PermanentError("cannot ever work")


@task("test_retry_after", payload=NoPayload, backoff_base=0.01, backoff_cap=0.01)
async def retry_after(payload: NoPayload) -> dict[str, Any] | None:
    raise RetryableError("rate limited", retry_after=120)


async def claim_and_process(engine: AsyncEngine) -> None:
    """Claim the one ready job and run it to its ack or nack, without the polling loop.

    Driving one job at a time keeps retry tests deterministic: the loop could re-claim a
    retry whose jittered delay happened to be ~0 before the test looks at it.
    """
    [job] = await PostgresBroker(engine).claim("default", "w1", 1, LEASE)
    await make_worker(engine).process(job)


async def make_ready(engine: AsyncEngine, job_id: uuid.UUID) -> None:
    """Pretend the retry delay has passed."""
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE jobs SET run_at = now() WHERE id = :i"), {"i": job_id})


async def attempts_of(engine: AsyncEngine, job_id: uuid.UUID) -> list[dict[str, Any]]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text("SELECT * FROM job_attempts WHERE job_id = :i ORDER BY id"), {"i": job_id}
        )
        return [dict(r._mapping) for r in rows]


async def test_each_retry_run_at_is_inside_the_full_jitter_window(
    migrated_engine: AsyncEngine,
) -> None:
    """After failed attempt n, run_at - failure time is in [0, base * 2^(n-1)] (base 10 s)."""
    tenant_id = await seed_tenant(migrated_engine)
    job_id = await insert_job(
        migrated_engine, tenant_id, task="test_backoff_probe", payload="{}", max_attempts=10
    )

    for n in range(1, 5):
        await claim_and_process(migrated_engine)
        job = await job_row(migrated_engine, job_id)
        failed_at = (await attempts_of(migrated_engine, job_id))[-1]["finished_at"]
        delay = (job["run_at"] - failed_at).total_seconds()
        assert (job["status"], job["attempts"]) == ("queued", n)
        assert 0 <= delay <= 10 * 2 ** (n - 1), (n, delay)
        await make_ready(migrated_engine, job_id)


async def test_job_is_dead_after_max_attempts(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    job_id = await insert_job(
        migrated_engine, tenant_id, task="test_always_fails", payload="{}", max_attempts=3
    )
    worker = make_worker(migrated_engine)

    await run_until([worker], migrated_engine, lambda c: c == {"dead": 1})

    job = await job_row(migrated_engine, job_id)
    assert job["attempts"] == 3
    assert job["dead_at"] is not None
    assert job["last_error"] == "RuntimeError: boom"
    assert job["lease_token"] is None
    attempts = await attempts_of(migrated_engine, job_id)
    assert [(a["attempt"], a["outcome"], a["error"]) for a in attempts] == [
        (1, "failed", "RuntimeError: boom"),
        (2, "failed", "RuntimeError: boom"),
        (3, "failed", "RuntimeError: boom"),
    ]


async def test_permanent_error_goes_to_dead_at_once(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    job_id = await insert_job(migrated_engine, tenant_id, task="test_permanent", payload="{}")
    worker = make_worker(migrated_engine)

    await run_until([worker], migrated_engine, lambda c: c == {"dead": 1})

    job = await job_row(migrated_engine, job_id)
    assert (job["attempts"], job["max_attempts"]) == (1, 5)  # attempts were left: still dead
    assert job["last_error"] == "PermanentError: cannot ever work"
    assert [a["outcome"] for a in await attempts_of(migrated_engine, job_id)] == ["failed"]


async def test_invalid_payload_at_the_worker_is_permanent(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    # The API validates payloads, so this can only come from elsewhere (an old row, a bug).
    job_id = await insert_job(migrated_engine, tenant_id, task="sleep", payload='{"ms": "soon"}')
    worker = make_worker(migrated_engine)

    await run_until([worker], migrated_engine, lambda c: c == {"dead": 1})

    job = await job_row(migrated_engine, job_id)
    assert job["attempts"] == 1
    assert job["last_error"].startswith("PermanentError: invalid payload")


async def test_unknown_task_at_the_worker_is_retried(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    job_id = await insert_job(migrated_engine, tenant_id, task="from_a_newer_release", payload="{}")

    await claim_and_process(migrated_engine)

    job = await job_row(migrated_engine, job_id)
    assert job["status"] == "queued"
    assert job["last_error"] == (
        "UnknownTaskError: no handler registered for task 'from_a_newer_release'"
    )


async def test_retry_after_from_the_handler_is_respected(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    job_id = await insert_job(migrated_engine, tenant_id, task="test_retry_after", payload="{}")

    await claim_and_process(migrated_engine)

    job = await job_row(migrated_engine, job_id)
    failed_at = (await attempts_of(migrated_engine, job_id))[-1]["finished_at"]
    delay = (job["run_at"] - failed_at).total_seconds()
    assert 120 <= delay < 121  # max(retry_after=120, backoff of at most 0.01 s)


async def test_nack_retries_until_the_last_attempt_then_dead(migrated_engine: AsyncEngine) -> None:
    """The broker SQL alone: queued while attempts < max_attempts, dead on the last one."""
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 1)
    async with migrated_engine.begin() as conn:
        await conn.execute(text("UPDATE jobs SET max_attempts = 2"))
    broker = PostgresBroker(migrated_engine)

    [job] = await broker.claim("default", "w", 1, LEASE)
    status = await broker.nack(
        job, "w", error="e1", outcome="failed", delay_seconds=30, permanent=False
    )
    assert status == "queued"
    row = await job_row(migrated_engine, job.id)
    assert row["lease_token"] is None and row["lease_owner"] is None
    assert row["dead_at"] is None

    async with migrated_engine.begin() as conn:
        await conn.execute(text("UPDATE jobs SET run_at = now()"))
    [job] = await broker.claim("default", "w", 1, LEASE)
    assert job.attempt == 2
    status = await broker.nack(
        job, "w", error="e2", outcome="timed_out", delay_seconds=30, permanent=False
    )
    assert status == "dead"
    row = await job_row(migrated_engine, job.id)
    assert row["dead_at"] is not None
    assert row["last_error"] == "e2"
    outcomes = [a["outcome"] for a in await attempts_of(migrated_engine, job.id)]
    assert outcomes == ["failed", "timed_out"]


async def test_nack_with_a_stale_token_changes_nothing(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 1)
    broker = PostgresBroker(migrated_engine)
    [job] = await broker.claim("default", "w1", 1, LEASE)

    stale = replace(job, lease_token=uuid.uuid4())
    status = await broker.nack(
        stale, "w2", error="late", outcome="failed", delay_seconds=0, permanent=True
    )

    assert status is None
    row = await job_row(migrated_engine, job.id)
    assert (row["status"], row["lease_token"], row["last_error"]) == (
        "running",
        job.lease_token,
        None,
    )
    assert await attempts_of(migrated_engine, job.id) == []


async def test_retrying_job_is_not_claimed_before_its_run_at(migrated_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    await seed_jobs(migrated_engine, tenant_id, 1)
    broker = PostgresBroker(migrated_engine)
    [job] = await broker.claim("default", "w", 1, LEASE)
    await broker.nack(job, "w", error="e", outcome="failed", delay_seconds=60, permanent=False)

    started = time.monotonic()
    assert await broker.claim("default", "w", 1, LEASE) == []
    assert time.monotonic() - started < 5
