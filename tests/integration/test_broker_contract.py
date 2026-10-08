"""One contract, two brokers: the Postgres broker and the mini broker must behave the same way
for everything the worker relies on. Each test runs once against each.

The mini broker runs in this process on a free port, with a real TCP client, its own log in a
temporary folder, and its expiry loop switched off so the tests decide when leases expire (as
the Postgres tests call the reaper).
"""

import asyncio
import json
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.minibroker.client import Client, MiniBroker
from hopper.minibroker.log import Log
from hopper.minibroker.server import BrokerServer
from hopper.minibroker.store import Store
from hopper.queue.broker import Broker, ClaimedJob
from hopper.queue.postgres import PostgresBroker
from hopper.worker.loop import Worker
from tests.helpers import seed_tenant

LEASE = 0.3


@dataclass
class Harness:
    broker: Broker
    push: Callable[[int, int, str], Awaitable[list[uuid.UUID]]]  # count, max_attempts, task
    expire: Callable[[], Awaitable[None]]  # what the reaper does
    pending: Callable[[], Awaitable[int]]  # jobs not finished yet (queued or running)


async def _postgres(engine: AsyncEngine) -> Harness:
    tenant = await seed_tenant(engine)
    broker = PostgresBroker(engine)

    async def push(count: int, max_attempts: int, task: str) -> list[uuid.UUID]:
        async with engine.begin() as conn:
            rows = await conn.execute(
                text(
                    "INSERT INTO jobs (tenant_id, task, payload, max_attempts) "
                    "SELECT :t, :task, CAST(:p AS jsonb), :m FROM generate_series(1, :n) "
                    "RETURNING id"
                ),
                {
                    "t": tenant,
                    "task": task,
                    "p": json.dumps(PAYLOADS[task]),
                    "m": max_attempts,
                    "n": count,
                },
            )
            return [r.id for r in rows]

    async def expire() -> None:
        await broker.reap(500)

    async def pending() -> int:
        async with engine.connect() as conn:
            return int(
                await conn.scalar(
                    text("SELECT count(*) FROM jobs WHERE status IN ('queued', 'running')")
                )
                or 0
            )

    return Harness(broker, push, expire, pending)


PAYLOADS = {"sleep": {"ms": 0}, "fail_always": {"permanent": True}}


@pytest.fixture(params=["postgres", "mini"])
async def harness(
    request: pytest.FixtureRequest, tmp_path: Path, migrated_engine: AsyncEngine
) -> AsyncIterator[Harness]:
    if request.param == "postgres":
        yield await _postgres(migrated_engine)
        return
    log = Log(tmp_path / "broker.log")
    store = Store(log)
    server = BrokerServer(store, log, "always", expire_interval=3600)
    port = await server.start("127.0.0.1", 0)
    client = Client(f"tcp://127.0.0.1:{port}", size=12)
    broker = MiniBroker(client)
    tenant = uuid.uuid4()

    async def push(count: int, max_attempts: int, task: str) -> list[uuid.UUID]:
        return [
            await broker.push(
                queue="default",
                task=task,
                payload=PAYLOADS[task],
                tenant_id=tenant,
                max_attempts=max_attempts,
            )
            for _ in range(count)
        ]

    async def expire() -> None:
        store.expire()

    async def pending() -> int:
        return sum(not m.dead for m in store.messages.values())

    try:
        yield Harness(broker, push, expire, pending)
    finally:
        await client.close()
        await server.stop()


async def claim_one(h: Harness, worker: str = "w1") -> ClaimedJob:
    [job] = await h.broker.claim("default", worker, 1, LEASE)
    return job


async def test_concurrent_claimers_never_get_the_same_job(harness: Harness) -> None:
    pushed = await harness.push(300, 5, "sleep")
    claimed: list[uuid.UUID] = []

    async def claimer(name: str) -> None:
        while jobs := await harness.broker.claim("default", name, 7, 30):
            claimed.extend(j.id for j in jobs)

    await asyncio.gather(*(claimer(f"w{i}") for i in range(10)))
    assert sorted(claimed) == sorted(pushed)  # every job, each exactly once


async def test_ack_succeeds_once(harness: Harness) -> None:
    await harness.push(1, 5, "sleep")
    job = await claim_one(harness)
    assert job.attempt == 1
    assert await harness.broker.ack(job, "w1", None)
    assert not await harness.broker.ack(job, "w1", None)
    assert await harness.pending() == 0


async def test_after_a_lease_expires_the_old_token_changes_nothing(harness: Harness) -> None:
    await harness.push(1, 5, "sleep")
    late = await claim_one(harness, "paused")
    await asyncio.sleep(LEASE + 0.1)
    await harness.expire()
    fresh = await claim_one(harness, "w2")
    assert (fresh.id, fresh.attempt) == (late.id, 2)
    assert fresh.lease_token != late.lease_token
    assert await harness.broker.heartbeat([late], LEASE) == set()
    assert not await harness.broker.ack(late, "paused", None)
    assert (
        await harness.broker.nack(
            late, "paused", error="x", outcome="failed", delay_seconds=0, permanent=False
        )
        is None
    )
    assert await harness.broker.ack(fresh, "w2", None)


async def test_a_heartbeat_keeps_the_lease(harness: Harness) -> None:
    await harness.push(1, 5, "sleep")
    job = await claim_one(harness)
    for _ in range(3):
        await asyncio.sleep(LEASE / 2)
        assert await harness.broker.heartbeat([job], LEASE) == {job.id}
    await harness.expire()
    assert await harness.broker.claim("default", "w2", 1, LEASE) == []
    assert await harness.broker.ack(job, "w1", None)


async def test_nack_retries_then_moves_to_the_dlq(harness: Harness) -> None:
    await harness.push(1, 2, "sleep")
    job = await claim_one(harness)
    nack = harness.broker.nack
    assert await nack(job, "w1", error="x", outcome="failed", delay_seconds=0, permanent=False) == (
        "queued"
    )
    job = await claim_one(harness)
    assert job.attempt == 2
    assert await nack(job, "w1", error="x", outcome="failed", delay_seconds=0, permanent=False) == (
        "dead"
    )
    assert await harness.broker.claim("default", "w1", 1, LEASE) == []
    assert await harness.pending() == 0


async def test_a_permanent_failure_is_dead_at_once(harness: Harness) -> None:
    await harness.push(1, 5, "sleep")
    job = await claim_one(harness)
    status = await harness.broker.nack(
        job, "w1", error="x", outcome="failed", delay_seconds=0, permanent=True
    )
    assert status == "dead"


async def test_a_retry_waits_for_its_delay(harness: Harness) -> None:
    await harness.push(1, 5, "sleep")
    job = await claim_one(harness)
    await harness.broker.nack(
        job, "w1", error="x", outcome="failed", delay_seconds=0.5, permanent=False
    )
    assert await harness.broker.claim("default", "w1", 1, LEASE) == []
    await asyncio.sleep(0.6)
    assert (await claim_one(harness)).attempt == 2


async def test_release_hands_the_job_back_without_using_an_attempt(harness: Harness) -> None:
    await harness.push(1, 5, "sleep")
    job = await claim_one(harness)
    assert await harness.broker.release([job], "w1") == {job.id}
    again = await claim_one(harness)
    assert (again.id, again.attempt) == (job.id, 1)


async def test_a_poison_pill_is_dead_after_its_attempts_expire(harness: Harness) -> None:
    await harness.push(1, 2, "sleep")
    for _ in range(2):
        await claim_one(harness)
        await asyncio.sleep(LEASE + 0.1)
        await harness.expire()
    assert await harness.broker.claim("default", "w1", 1, LEASE) == []
    assert await harness.pending() == 0


async def test_the_same_worker_runs_jobs_to_the_end(harness: Harness) -> None:
    """The production Worker class, unchanged, on either broker."""
    await harness.push(200, 5, "sleep")
    await harness.push(3, 5, "fail_always")
    worker = Worker(
        harness.broker,
        worker_id="w",
        queues=["default"],
        slots=20,
        poll_interval=0.02,
        max_idle_backoff=0.05,
        lease_seconds=5,
    )
    runner = asyncio.create_task(worker.run())
    try:
        for _ in range(500):
            if await harness.pending() == 0:
                break
            await asyncio.sleep(0.02)
        assert await harness.pending() == 0  # 200 succeeded, 3 dead after one attempt each
    finally:
        worker.stop()
        await runner
