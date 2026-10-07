"""CHAOS_ACK_BEFORE_RUN, the chaos test's negative control, against the normal worker.

The same crash, a worker killed with SIGKILL in the middle of an effect job, is harmless with
the normal worker (it acks after running: at-least-once) and loses the job with the switch on
(it acks before running: at-most-once). The chaos test relies on exactly this difference.
"""

import asyncio
import subprocess
import uuid
from collections.abc import Iterator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue.postgres import PostgresBroker
from hopper.scheduler.reaper import Reaper
from hopper.tasks import effect
from hopper.worker.loop import Worker
from tests.helpers import (
    attempts_of,
    insert_job,
    job_row,
    make_worker,
    run_until,
    seed_tenant,
    start_worker_process,
    status_in,
    wait_for,
)

FAST = {
    "LEASE_SECONDS": "2",
    "HEARTBEAT_SECONDS": "0.5",
    "WORKER_POLL_INTERVAL": "0.05",
    "WORKER_MAX_IDLE_BACKOFF": "0.1",
}
STARTUP_TIMEOUT = 30  # a cold Python start with every import can be slow on CI
SLOW_EFFECT = '{"min_ms": 3000, "max_ms": 3000}'  # long enough to kill the worker mid-run


@pytest.fixture
def effect_engine(migrated_engine: AsyncEngine) -> Iterator[AsyncEngine]:
    effect.configure(migrated_engine)
    yield migrated_engine
    effect.configure(None)


async def recorded(engine: AsyncEngine, job_id: uuid.UUID) -> tuple[int, int]:
    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT (SELECT count(*) FROM job_executions WHERE job_id = :j),"
                    "       (SELECT count(*) FROM job_effects WHERE job_id = :j)"
                ),
                {"j": job_id},
            )
        ).one()
    return int(row[0]), int(row[1])


async def kill_when(
    proc: subprocess.Popen[bytes], engine: AsyncEngine, job_id: uuid.UUID, status: str
) -> None:
    """SIGKILL the worker process as soon as the job reaches `status`: no cleanup at all."""
    try:
        await wait_for(status_in(engine, job_id, status), timeout=STARTUP_TIMEOUT)
        proc.kill()
        await asyncio.to_thread(proc.wait, 10)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


async def test_acking_after_running_survives_a_kill_mid_run(
    effect_engine: AsyncEngine, migrated_db_url: str
) -> None:
    tenant_id = await seed_tenant(effect_engine)
    job_id = await insert_job(effect_engine, tenant_id, task="effect", payload=SLOW_EFFECT)
    await kill_when(start_worker_process(migrated_db_url, **FAST), effect_engine, job_id, "running")

    reaper = Reaper(PostgresBroker(effect_engine), interval=0.1)
    survivor = make_worker(effect_engine, "survivor")
    runners = [asyncio.create_task(reaper.run()), asyncio.create_task(survivor.run())]
    try:
        await wait_for(status_in(effect_engine, job_id, "succeeded"), timeout=30)
    finally:
        reaper.stop()
        survivor.stop()
        await asyncio.gather(*runners)
    # The killed run never reached its effect; the survivor's run did, once.
    assert await recorded(effect_engine, job_id) == (1, 1)
    outcomes = [a["outcome"] for a in await attempts_of(effect_engine, job_id)]
    assert outcomes == ["lease_expired", "succeeded"]


async def test_acking_before_running_loses_the_job_on_the_same_kill(
    effect_engine: AsyncEngine, migrated_db_url: str
) -> None:
    tenant_id = await seed_tenant(effect_engine)
    job_id = await insert_job(effect_engine, tenant_id, task="effect", payload=SLOW_EFFECT)
    proc = start_worker_process(migrated_db_url, CHAOS_ACK_BEFORE_RUN="true", **FAST)
    # Acked before the handler runs: "succeeded" while the effect is still seconds away.
    await kill_when(proc, effect_engine, job_id, "succeeded")

    # Nothing is left for the reaper or any worker: the queue thinks the job is done.
    reaper = Reaper(PostgresBroker(effect_engine), interval=0.1)
    survivor = make_worker(effect_engine, "survivor")
    runners = [asyncio.create_task(reaper.run()), asyncio.create_task(survivor.run())]
    await asyncio.sleep(4)  # two lease lengths
    reaper.stop()
    survivor.stop()
    await asyncio.gather(*runners)
    assert (await job_row(effect_engine, job_id))["status"] == "succeeded"
    assert await recorded(effect_engine, job_id) == (0, 0)  # the effect never happened: lost


async def test_with_the_switch_a_failure_is_never_retried(effect_engine: AsyncEngine) -> None:
    tenant_id = await seed_tenant(effect_engine)
    job_id = await insert_job(effect_engine, tenant_id, task="fail_always", payload="{}")
    worker = Worker(
        PostgresBroker(effect_engine),
        worker_id="at-most-once",
        queues=["default"],
        poll_interval=0.02,
        max_idle_backoff=0.1,
        ack_before_run=True,
    )
    await run_until([worker], effect_engine, lambda c: c.get("succeeded"))
    await asyncio.sleep(0.2)  # let the handler fail after the ack
    row = await job_row(effect_engine, job_id)
    assert (row["status"], row["attempts"], row["last_error"]) == ("succeeded", 1, None)
    assert [a["outcome"] for a in await attempts_of(effect_engine, job_id)] == ["succeeded"]
