"""python -m hopper.bootstrap --smoke-key: the key deploy.sh smoke-tests every release with."""

import asyncio
import os
import re
import subprocess
import sys

import httpx
from fastapi import FastAPI
from sqlalchemy import text

from hopper.bootstrap import SMOKE_TENANT
from tests.helpers import ROOT

KEY = re.compile(r"hop_live_[a-z0-9]{8}_[A-Za-z0-9_-]{43}")


def smoke_key(db_url: str) -> subprocess.CompletedProcess[str]:
    """The real CLI, as deploy.sh runs it: `compose run migrate python -m hopper.bootstrap`."""
    return subprocess.run(
        [sys.executable, "-m", "hopper.bootstrap", "--smoke-key"],
        cwd=ROOT,
        env={**os.environ, "DATABASE_URL": db_url},
        capture_output=True,
        text=True,
        check=False,
    )


def client(app: FastAPI, key: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": f"Bearer {key}"},
    )


async def test_prints_only_a_working_key(api_app: FastAPI, migrated_db_url: str) -> None:
    made = await asyncio.to_thread(smoke_key, migrated_db_url)
    assert made.returncode == 0, made.stderr
    assert KEY.fullmatch(made.stdout.strip()), made.stdout  # nothing else on stdout
    async with client(api_app, made.stdout.strip()) as c:
        job = await c.post("/v1/jobs", json={"task": "sleep", "payload": {"ms": 1}})
    assert job.status_code == 201
    assert job.headers["X-RateLimit-Limit"] == "20"  # a small tenant on purpose


async def test_a_new_key_revokes_the_old_one(api_app: FastAPI, migrated_db_url: str) -> None:
    first = (await asyncio.to_thread(smoke_key, migrated_db_url)).stdout.strip()
    second = (await asyncio.to_thread(smoke_key, migrated_db_url)).stdout.strip()
    assert first != second
    async with client(api_app, first) as old, client(api_app, second) as new:
        assert (await old.get("/v1/jobs")).status_code == 401
        assert (await new.get("/v1/jobs")).status_code == 200
    async with api_app.state.engine.connect() as conn:
        tenants = (
            await conn.execute(
                text("SELECT count(*) FROM tenants WHERE name = :n"), {"n": SMOKE_TENANT}
            )
        ).scalar_one()
    assert tenants == 1  # rotated, not duplicated


def test_refuses_to_run_without_the_pepper(migrated_db_url: str) -> None:
    env = {**os.environ, "DATABASE_URL": migrated_db_url, "API_KEY_PEPPER": ""}
    result = subprocess.run(
        [sys.executable, "-m", "hopper.bootstrap", "--smoke-key"],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 2 and "API_KEY_PEPPER" in result.stderr
