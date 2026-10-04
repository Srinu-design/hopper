"""Real worker processes: kill -9 in the middle of a job, and SIGTERM with jobs in flight.

The worker runs as `python -m hopper.worker`, exactly as in its container, with short lease
timings so each test takes seconds (production: 30 s lease, 10 s heartbeat).
"""

import asyncio
import signal
import subprocess
import sys
import time

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue.postgres import PostgresBroker
from hopper.scheduler.reaper import Reaper
from tests.helpers import (
    attempts_of,
    insert_job,
    job_row,
    make_worker,
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


async def wait_exit(proc: subprocess.Popen[bytes], timeout: float) -> int:
    return await asyncio.to_thread(proc.wait, timeout)


def ensure_dead(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is None:
        proc.kill()
        proc.wait()


async def test_kill_9_mid_job_then_another_worker_finishes_it(
    migrated_engine: AsyncEngine, migrated_db_url: str
) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    job_id = await insert_job(migrated_engine, tenant_id, task="sleep", payload='{"ms": 3000}')
    proc = start_worker_process(migrated_db_url, **FAST)
    try:
        await wait_for(status_in(migrated_engine, job_id, "running"), timeout=STARTUP_TIMEOUT)
        victim = (await job_row(migrated_engine, job_id))["lease_owner"]
        proc.kill()  # SIGKILL (TerminateProcess on Windows): no cleanup, no release, no ack
        await wait_exit(proc, 10)
    finally:
        ensure_dead(proc)

    # Nobody heartbeats the job now. The reaper requeues it once the lease runs out, and
    # a surviving worker claims it and runs it to the end.
    reaper = Reaper(PostgresBroker(migrated_engine), interval=0.1)
    survivor = make_worker(migrated_engine, "survivor")
    runners = [asyncio.create_task(reaper.run()), asyncio.create_task(survivor.run())]
    try:
        await wait_for(status_in(migrated_engine, job_id, "succeeded"), timeout=20)
    finally:
        reaper.stop()
        survivor.stop()
        await asyncio.gather(*runners)

    row = await job_row(migrated_engine, job_id)
    assert (row["status"], row["attempts"], row["result"]) == ("succeeded", 2, {"slept_ms": 3000})
    history = [
        (a["attempt"], a["worker_id"], a["outcome"])
        for a in await attempts_of(migrated_engine, job_id)
    ]
    assert history == [(1, victim, "lease_expired"), (2, "survivor", "succeeded")]


@pytest.mark.skipif(
    sys.platform == "win32", reason="Windows has no SIGTERM: send_signal() kills outright"
)
async def test_sigterm_finishes_short_jobs_releases_long_ones_and_exits_0(
    migrated_engine: AsyncEngine, migrated_db_url: str
) -> None:
    tenant_id = await seed_tenant(migrated_engine)
    short = await insert_job(migrated_engine, tenant_id, task="sleep", payload='{"ms": 1000}')
    long = await insert_job(migrated_engine, tenant_id, task="sleep", payload='{"ms": 60000}')
    proc = start_worker_process(migrated_db_url, **FAST, SHUTDOWN_GRACE_SECONDS="3")
    try:
        await wait_for(status_in(migrated_engine, long, "running"), timeout=STARTUP_TIMEOUT)
        proc.send_signal(signal.SIGTERM)
        sent_at = time.monotonic()
        code = await wait_exit(proc, 20)
        took = time.monotonic() - sent_at
    finally:
        ensure_dead(proc)

    assert proc.stderr is not None
    assert code == 0, proc.stderr.read().decode()
    assert 2.5 < took < 10  # waited out the 3 s grace for the long job, then exited
    assert (await job_row(migrated_engine, short))["status"] == "succeeded"
    row = await job_row(migrated_engine, long)
    assert (row["status"], row["attempts"], row["lease_token"]) == ("queued", 0, None)
    assert [a["outcome"] for a in await attempts_of(migrated_engine, long)] == ["released"]
