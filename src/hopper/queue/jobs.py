"""Tenant-scoped job reads and writes for the API. Every function requires tenant_id."""

import json
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue import sql


def _job(row: Any) -> dict[str, Any]:
    return dict(row._mapping)


async def insert_job(
    engine: AsyncEngine,
    *,
    tenant_id: UUID,
    queue: str,
    task: str,
    payload: dict[str, Any],
    priority: int,
    max_attempts: int,
    timeout_seconds: int,
    idempotency_key: str | None = None,
    request_hash: bytes | None = None,
) -> tuple[dict[str, Any], bool]:
    """Insert a job. Returns (job, created).

    created=False only with an idempotency key that already exists for this tenant: the job
    returned is the existing one, and the caller compares its request_hash.
    """
    params = {
        "tenant_id": tenant_id,
        "queue": queue,
        "task": task,
        "payload": json.dumps(payload),
        "priority": priority,
        "max_attempts": max_attempts,
        "timeout_seconds": timeout_seconds,
        "idempotency_key": idempotency_key,
        "request_hash": request_hash,
    }
    # Two rounds cover the rare case where the conflicting job is deleted between our
    # INSERT and SELECT (retention), which frees the key for a fresh insert.
    for _ in range(2):
        async with engine.begin() as conn:
            row = (await conn.execute(text(sql.INSERT_JOB), params)).first()
            if row is not None:
                return _job(row), True
            if idempotency_key is None:
                raise RuntimeError("insert without an idempotency key returned no row")
            # ON CONFLICT waited for the other transaction to commit, and this new statement
            # takes a fresh snapshot, so the winning row is visible here.
            existing = (
                await conn.execute(
                    text(sql.GET_JOB_BY_IDEMPOTENCY_KEY),
                    {"tenant_id": tenant_id, "idempotency_key": idempotency_key},
                )
            ).first()
            if existing is not None:
                return _job(existing), False
    raise RuntimeError("could not insert or load the job for this idempotency key")


async def get_job(
    engine: AsyncEngine, *, tenant_id: UUID, job_id: UUID
) -> tuple[dict[str, Any], list[dict[str, Any]]] | None:
    """The job and its attempt history, or None if it does not exist for this tenant."""
    async with engine.connect() as conn:
        row = (
            await conn.execute(text(sql.GET_JOB), {"id": job_id, "tenant_id": tenant_id})
        ).first()
        if row is None:
            return None
        attempts = await conn.execute(text(sql.GET_ATTEMPTS), {"job_id": job_id})
        return _job(row), [dict(a._mapping) for a in attempts]


async def get_or_create_tenant(engine: AsyncEngine, name: str) -> UUID:
    async with engine.begin() as conn:
        result = await conn.execute(text(sql.UPSERT_TENANT), {"name": name})
        tenant_id: UUID = result.scalar_one()
    return tenant_id
