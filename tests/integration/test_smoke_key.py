"""python -m hopper.bootstrap --smoke-key: the key deploy.sh smoke-tests every release with."""

import asyncio
import re

from fastapi import FastAPI
from sqlalchemy import text

from hopper.bootstrap import BENCH_TENANT, SMOKE_TENANT
from tests.helpers import client_for, run_bootstrap

KEY = re.compile(r"hop_live_[a-z0-9]{8}_[A-Za-z0-9_-]{43}")


async def test_prints_only_a_working_key(api_app: FastAPI, migrated_db_url: str) -> None:
    made = await asyncio.to_thread(run_bootstrap, migrated_db_url, "--smoke-key")
    assert made.returncode == 0, made.stderr
    assert KEY.fullmatch(made.stdout.strip()), made.stdout  # nothing else on stdout
    async with client_for(api_app, made.stdout.strip()) as c:
        job = await c.post("/v1/jobs", json={"task": "sleep", "payload": {"ms": 1}})
    assert job.status_code == 201
    assert job.headers["X-RateLimit-Limit"] == "20"  # a small tenant on purpose


async def test_a_new_key_revokes_the_old_one(api_app: FastAPI, migrated_db_url: str) -> None:
    first = (await asyncio.to_thread(run_bootstrap, migrated_db_url, "--smoke-key")).stdout.strip()
    second = (await asyncio.to_thread(run_bootstrap, migrated_db_url, "--smoke-key")).stdout.strip()
    assert first != second
    async with client_for(api_app, first) as old, client_for(api_app, second) as new:
        assert (await old.get("/v1/jobs")).status_code == 401
        assert (await new.get("/v1/jobs")).status_code == 200
    async with api_app.state.engine.connect() as conn:
        tenants = (
            await conn.execute(
                text("SELECT count(*) FROM tenants WHERE name = :n"), {"n": SMOKE_TENANT}
            )
        ).scalar_one()
    assert tenants == 1  # rotated, not duplicated


def test_refuses_to_run_without_the_pepper() -> None:
    # Nothing listens on port 1: the pepper check must come before any database work.
    unreachable = "postgresql+asyncpg://hopper:hopper@127.0.0.1:1/hopper"
    result = run_bootstrap(unreachable, "--smoke-key", API_KEY_PEPPER="")
    assert result.returncode == 2 and "API_KEY_PEPPER" in result.stderr


async def test_the_bench_key_belongs_to_a_tenant_with_raised_limits(
    api_app: FastAPI, migrated_db_url: str
) -> None:
    """The load test measures Hopper, not its own tenant's rate limit."""
    made = await asyncio.to_thread(run_bootstrap, migrated_db_url, "--bench-key")
    assert made.returncode == 0, made.stderr
    assert KEY.fullmatch(made.stdout.strip()), made.stdout
    async with client_for(api_app, made.stdout.strip()) as c:
        job = await c.post("/v1/jobs", json={"task": "sleep", "payload": {"ms": 1}})
    assert job.status_code == 201
    assert job.headers["X-RateLimit-Limit"] == "100000"
    async with api_app.state.engine.connect() as conn:
        limits = (
            await conn.execute(
                text("SELECT max_queue_depth FROM tenants WHERE name = :n"), {"n": BENCH_TENANT}
            )
        ).scalar_one()
    assert limits == 10_000_000


async def test_bench_and_smoke_keys_are_separate_tenants(
    api_app: FastAPI, migrated_db_url: str
) -> None:
    smoke = (await asyncio.to_thread(run_bootstrap, migrated_db_url, "--smoke-key")).stdout.strip()
    bench = (await asyncio.to_thread(run_bootstrap, migrated_db_url, "--bench-key")).stdout.strip()
    async with client_for(api_app, smoke) as s, client_for(api_app, bench) as b:
        assert (await s.get("/v1/jobs")).status_code == 200  # rotating one keeps the other
        assert (await b.get("/v1/jobs")).status_code == 200
