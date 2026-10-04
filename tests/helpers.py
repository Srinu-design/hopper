"""Shared test helpers. Test modules import from here, never from each other: pytest loads
test files as top-level modules, so importing one from another would execute it twice."""

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue.postgres import PostgresBroker
from hopper.worker.loop import Worker

ROOT = Path(__file__).resolve().parents[1]


def run_alembic(database_url: str, *args: str) -> subprocess.CompletedProcess[str]:
    """Run the real alembic CLI against database_url, exactly as the migrate container does."""
    return subprocess.run(
        [sys.executable, "-m", "alembic", *args],
        cwd=ROOT,
        env={**os.environ, "DATABASE_URL": database_url},
        capture_output=True,
        text=True,
        check=False,
    )


LEASE = 30.0


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
