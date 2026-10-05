"""Shared test helpers. Test modules import from here, never from each other: pytest loads
test files as top-level modules, so importing one from another would execute it twice."""

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.auth import keys, store
from hopper.config import get_settings
from hopper.queue.postgres import PostgresBroker
from hopper.worker.loop import Worker

ROOT = Path(__file__).resolve().parents[1]


def run_alembic(
    database_url: str, *args: str, timeout: float = 120.0
) -> subprocess.CompletedProcess[str]:
    """Run the real alembic CLI against database_url, exactly as the migrate container does.
    A migration stuck behind a lock fails the test after `timeout` instead of hanging it."""
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout,
    )


LEASE = 30.0


@dataclass(frozen=True)
class TenantCreds:
    id: uuid.UUID
    name: str
    api_key: str
    signing_secret: bytes


async def make_tenant(
    engine: AsyncEngine,
    name: str,
    *,
    rate_per_sec: float = 10_000,
    burst: int = 10_000,
    max_queue_depth: int = 100_000,
) -> TenantCreds:
    """A tenant and one API key, written straight to the database (no admin API round trip).

    The default limits are high so that tests about other things never meet a 429; the
    rate-limit and backpressure tests pass their own.
    """
    tenant, _ = await store.create_tenant(
        engine,
        name=name,
        rate_per_sec=Decimal(str(rate_per_sec)),
        burst=burst,
        max_queue_depth=max_queue_depth,
    )
    new = keys.generate(get_settings().api_key_pepper.get_secret_value().encode())
    assert await store.create_api_key(engine, tenant_id=tenant.id, name="test", key=new)
    return TenantCreds(tenant.id, name, new.plaintext, tenant.signing_secret)


async def seed_tenant(engine: AsyncEngine, name: str = "t") -> uuid.UUID:
    async with engine.begin() as conn:
        result = await conn.execute(
            text("INSERT INTO tenants (name) VALUES (:n) RETURNING id"), {"n": name}
        )
        tenant_id: uuid.UUID = result.scalar_one()
    return tenant_id


async def seed_jobs(
    engine: AsyncEngine,
    tenant_id: uuid.UUID,
    count: int,
    *,
    queue: str = "default",
    priority: int = 0,
    run_at_offset: timedelta = timedelta(0),
) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO jobs (tenant_id, queue, task, payload, priority, run_at) "
                "SELECT :t, :q, 'sleep', CAST(:p AS jsonb), :prio, now() + :off "
                "FROM generate_series(1, :n)"
            ),
            {
                "t": tenant_id,
                "q": queue,
                "p": json.dumps({"ms": 0}),
                "prio": priority,
                "off": run_at_offset,
                "n": count,
            },
        )


async def job_row(engine: AsyncEngine, job_id: uuid.UUID) -> dict[str, Any]:
    async with engine.connect() as conn:
        row = (await conn.execute(text("SELECT * FROM jobs WHERE id = :i"), {"i": job_id})).one()
    return dict(row._mapping)


def make_worker(
    engine: AsyncEngine,
    name: str = "w1",
    slots: int = 10,
    *,
    lease_seconds: float = LEASE,
    heartbeat_interval: float | None = None,
    shutdown_grace: float = 25.0,
) -> Worker:
    return Worker(
        PostgresBroker(engine),
        worker_id=name,
        queues=["default"],
        slots=slots,
        poll_interval=0.02,
        max_idle_backoff=0.1,
        lease_seconds=lease_seconds,
        heartbeat_interval=heartbeat_interval,
        shutdown_grace=shutdown_grace,
    )


async def wait_for(
    condition: Callable[[], Awaitable[bool]], timeout: float = 10.0, interval: float = 0.02
) -> None:
    """Poll an async condition until it is true."""
    deadline = time.monotonic() + timeout
    while not await condition():
        assert time.monotonic() < deadline, "condition not met in time"
        await asyncio.sleep(interval)


def status_in(
    engine: AsyncEngine, job_id: uuid.UUID, *statuses: str
) -> Callable[[], Awaitable[bool]]:
    """A wait_for condition: the job's status is one of `statuses`."""

    async def check() -> bool:
        return (await job_row(engine, job_id))["status"] in statuses

    return check


async def attempts_of(engine: AsyncEngine, job_id: uuid.UUID) -> list[dict[str, Any]]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text("SELECT * FROM job_attempts WHERE job_id = :i ORDER BY id"), {"i": job_id}
        )
        return [dict(r._mapping) for r in rows]


def start_worker_process(database_url: str, **settings: str) -> subprocess.Popen[bytes]:
    """Start a real `python -m hopper.worker` process, as the Compose worker container does."""
    env = {**os.environ, "DATABASE_URL": database_url, "LOG_LEVEL": "WARNING", **settings}
    return subprocess.Popen(
        [sys.executable, "-m", "hopper.worker"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
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
