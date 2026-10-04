import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue.jobs import get_or_create_tenant
from tests.helpers import job_row


@pytest.fixture
async def tenant_id(api_app: FastAPI) -> uuid.UUID:
    """The tenant every request runs as until API keys arrive (Week 5)."""
    return await get_or_create_tenant(api_app.state.engine, "default")


@pytest.fixture
def engine(api_app: FastAPI) -> AsyncEngine:
    db: AsyncEngine = api_app.state.engine
    return db


async def dead_job(
    engine: AsyncEngine,
    tenant_id: uuid.UUID,
    *,
    queue: str = "default",
    task: str = "sleep",
    died_minutes_ago: float = 0,
    error: str = "RuntimeError: boom",
) -> uuid.UUID:
    async with engine.begin() as conn:
        job_id: uuid.UUID = (
            await conn.execute(
                text(
                    "INSERT INTO jobs (tenant_id, queue, task, payload, status, attempts, "
                    "  dead_at, last_error, started_at) "
                    "VALUES (:t, :q, :task, '{\"ms\": 0}', 'dead', 5, "
                    "  now() - make_interval(secs => :ago), :err, now()) RETURNING id"
                ),
                {
                    "t": tenant_id,
                    "q": queue,
                    "task": task,
                    "ago": died_minutes_ago * 60,
                    "err": error,
                },
            )
        ).scalar_one()
        await conn.execute(
            text(
                "INSERT INTO job_attempts (job_id, attempt, worker_id, started_at, finished_at, "
                "  outcome, error) VALUES (:j, 5, 'w', now(), now(), 'failed', :err)"
            ),
            {"j": job_id, "err": error},
        )
    return job_id


async def queued_job(engine: AsyncEngine, tenant_id: uuid.UUID) -> uuid.UUID:
    async with engine.begin() as conn:
        job_id: uuid.UUID = (
            await conn.execute(
                text("INSERT INTO jobs (tenant_id, task) VALUES (:t, 'sleep') RETURNING id"),
                {"t": tenant_id},
            )
        ).scalar_one()
    return job_id


def ids(page: dict[str, Any]) -> list[str]:
    return [job["id"] for job in page["jobs"]]


# --- listing -------------------------------------------------------------------------------


async def test_dlq_lists_only_dead_jobs_newest_first(
    client: httpx.AsyncClient, engine: AsyncEngine, tenant_id: uuid.UUID
) -> None:
    older = await dead_job(engine, tenant_id, died_minutes_ago=10)
    newer = await dead_job(engine, tenant_id, died_minutes_ago=1)
    await queued_job(engine, tenant_id)

    resp = await client.get("/v1/dlq")

    assert resp.status_code == 200
    page = resp.json()
    assert ids(page) == [str(newer), str(older)]
    assert page["next_cursor"] is None
    job = page["jobs"][0]
    assert job["status"] == "dead"
    assert job["last_error"] == "RuntimeError: boom"
    assert job["dead_at"] is not None
    assert "attempt_history" not in job  # the list is a summary; GET /v1/jobs/{id} has it


async def test_dlq_filters_by_queue_task_and_since(
    client: httpx.AsyncClient, engine: AsyncEngine, tenant_id: uuid.UUID
) -> None:
    emails = await dead_job(engine, tenant_id, queue="emails", died_minutes_ago=30)
    flaky = await dead_job(engine, tenant_id, task="flaky", died_minutes_ago=5)
    recent = await dead_job(engine, tenant_id, died_minutes_ago=1)

    assert ids((await client.get("/v1/dlq", params={"queue": "emails"})).json()) == [str(emails)]
    assert ids((await client.get("/v1/dlq", params={"task": "flaky"})).json()) == [str(flaky)]
    since = (datetime.now(UTC) - timedelta(minutes=10)).isoformat()
    resp = await client.get("/v1/dlq", params={"since": since})
    assert ids(resp.json()) == [str(recent), str(flaky)]


async def test_dlq_pages_with_a_cursor(
    client: httpx.AsyncClient, engine: AsyncEngine, tenant_id: uuid.UUID
) -> None:
    created = [await dead_job(engine, tenant_id, died_minutes_ago=m) for m in range(5)]

    seen: list[str] = []
    cursor = None
    pages = 0
    while True:
        params: dict[str, Any] = {"limit": 2} | ({"cursor": cursor} if cursor else {})
        page = (await client.get("/v1/dlq", params=params)).json()
        seen += ids(page)
        pages += 1
        cursor = page["next_cursor"]
        if cursor is None:
            break

    assert pages == 3
    assert seen == [str(i) for i in created]  # newest (0 minutes ago) first, no gaps or repeats


async def test_dlq_rejects_bad_query_parameters(client: httpx.AsyncClient) -> None:
    resp = await client.get("/v1/dlq", params={"cursor": "not-a-cursor"})
    assert (resp.status_code, resp.json()["error"]["code"]) == (422, "invalid_cursor")
    resp = await client.get("/v1/dlq", params={"since": "2026-01-01T00:00:00"})  # no timezone
    assert (resp.status_code, resp.json()["error"]["code"]) == (422, "validation_error")
    resp = await client.get("/v1/dlq", params={"limit": 500})
    assert resp.status_code == 422


# --- replay one ------------------------------------------------------------------------------


async def test_replay_requeues_a_dead_job_with_fresh_attempts(
    client: httpx.AsyncClient, engine: AsyncEngine, tenant_id: uuid.UUID
) -> None:
    job_id = await dead_job(engine, tenant_id)

    resp = await client.post(f"/v1/jobs/{job_id}/replay")

    assert resp.status_code == 200
    job = resp.json()
    assert job["status"] == "queued"
    assert (job["attempts"], job["replay_count"]) == (0, 1)
    assert job["dead_at"] is None and job["last_error"] is None
    assert [a["outcome"] for a in job["attempt_history"]] == ["failed"]  # history is kept
    row = await job_row(engine, job_id)
    async with engine.connect() as conn:
        now = (await conn.execute(text("SELECT now()"))).scalar_one()
    assert row["run_at"] <= now  # a single replay runs now, no spread


async def test_replaying_a_job_that_is_not_dead_changes_nothing(
    client: httpx.AsyncClient, engine: AsyncEngine, tenant_id: uuid.UUID
) -> None:
    job_id = await dead_job(engine, tenant_id)
    assert (await client.post(f"/v1/jobs/{job_id}/replay")).status_code == 200
    before = await job_row(engine, job_id)

    again = await client.post(f"/v1/jobs/{job_id}/replay")

    assert again.status_code == 409
    assert again.json()["error"]["code"] == "not_dead"
    assert await job_row(engine, job_id) == before


async def test_replaying_an_unknown_job_is_404(client: httpx.AsyncClient) -> None:
    resp = await client.post(f"/v1/jobs/{uuid.uuid4()}/replay")
    assert (resp.status_code, resp.json()["error"]["code"]) == (404, "not_found")


# --- bulk replay ----------------------------------------------------------------------------


async def test_bulk_replay_by_ids_spreads_run_at(
    client: httpx.AsyncClient, engine: AsyncEngine, tenant_id: uuid.UUID
) -> None:
    dead = [await dead_job(engine, tenant_id) for _ in range(20)]
    alive = await queued_job(engine, tenant_id)

    resp = await client.post(
        "/v1/dlq/replay",
        json={"ids": [str(i) for i in [*dead, alive]], "spread_seconds": 60},
    )

    assert resp.status_code == 200
    result = resp.json()
    assert result["replayed"] == 20
    assert sorted(result["job_ids"]) == sorted(str(i) for i in dead)  # the live job is skipped
    assert result["has_more"] is False
    async with engine.connect() as conn:
        offsets = (
            (
                await conn.execute(
                    text(
                        "SELECT extract(epoch FROM run_at - now()) FROM jobs WHERE id = ANY(:ids)"
                    ),
                    {"ids": dead},
                )
            )
            .scalars()
            .all()
        )
    assert all(0 <= s <= 60 for s in offsets)
    assert max(offsets) - min(offsets) > 5  # really spread, not all at one instant


async def test_bulk_replay_by_filter(
    client: httpx.AsyncClient, engine: AsyncEngine, tenant_id: uuid.UUID
) -> None:
    emails = [await dead_job(engine, tenant_id, queue="emails") for _ in range(3)]
    other = await dead_job(engine, tenant_id, queue="default")

    resp = await client.post(
        "/v1/dlq/replay", json={"filter": {"queue": "emails"}, "spread_seconds": 0}
    )

    assert resp.json()["replayed"] == 3
    assert sorted(resp.json()["job_ids"]) == sorted(str(i) for i in emails)
    assert (await job_row(engine, other))["status"] == "dead"


async def test_bulk_replay_works_in_batches_of_1000(
    client: httpx.AsyncClient, engine: AsyncEngine, tenant_id: uuid.UUID
) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO jobs (tenant_id, task, status, dead_at) "
                "SELECT :t, 'sleep', 'dead', now() FROM generate_series(1, 1005)"
            ),
            {"t": tenant_id},
        )

    first = (await client.post("/v1/dlq/replay", json={"filter": {}})).json()
    second = (await client.post("/v1/dlq/replay", json={"filter": {}})).json()

    assert (first["replayed"], first["has_more"]) == (1000, True)
    assert (second["replayed"], second["has_more"]) == (5, False)


@pytest.mark.parametrize(
    "body",
    [
        {},  # neither ids nor filter
        {"ids": [str(uuid.uuid4())], "filter": {}},  # both
        {"ids": []},
        {"ids": [str(uuid.uuid4()) for _ in range(1001)]},
        {"filter": {}, "spread_seconds": 7200},
        {"filter": {"status": "queued"}},  # unknown filter field
    ],
)
async def test_bulk_replay_rejects_bad_requests(
    client: httpx.AsyncClient, body: dict[str, Any]
) -> None:
    resp = await client.post("/v1/dlq/replay", json=body)
    assert (resp.status_code, resp.json()["error"]["code"]) == (422, "validation_error")
