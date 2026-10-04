"""Cron schedules: the API, and the cron loop that turns due schedules into jobs."""

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue.schedules import cron_idempotency_key, fire_due
from hopper.scheduler.cron import CronLoop
from tests.helpers import TenantCreds, make_worker, run_until

NIGHTLY = {"name": "nightly", "cron": "0 3 * * *", "task": "sleep", "payload": {"ms": 1}}


async def create(client: httpx.AsyncClient, **fields: Any) -> dict[str, Any]:
    resp = await client.post("/v1/schedules", json=NIGHTLY | fields)
    assert resp.status_code == 201, resp.text
    body: dict[str, Any] = resp.json()
    return body


async def make_due(engine: AsyncEngine, schedule_id: str, ago: timedelta = timedelta(0)) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text("UPDATE schedules SET next_run_at = now() - CAST(:ago AS interval) WHERE id = :i"),
            {"i": schedule_id, "ago": ago},
        )


async def jobs_of(engine: AsyncEngine, schedule_id: str) -> list[dict[str, Any]]:
    async with engine.connect() as conn:
        rows = await conn.execute(
            text("SELECT * FROM jobs WHERE schedule_id = :i ORDER BY created_at"),
            {"i": schedule_id},
        )
        return [dict(r._mapping) for r in rows]


async def schedule_row(engine: AsyncEngine, schedule_id: str) -> dict[str, Any]:
    async with engine.connect() as conn:
        row = (
            await conn.execute(text("SELECT * FROM schedules WHERE id = :i"), {"i": schedule_id})
        ).one()
    return dict(row._mapping)


# --- the API ---------------------------------------------------------------------------------


async def test_create_computes_the_first_run_in_the_schedules_timezone(
    client: httpx.AsyncClient,
) -> None:
    schedule = await create(client, cron="0 9 * * *", timezone="Asia/Kolkata")
    first = datetime.fromisoformat(schedule["next_run_at"])
    assert first > datetime.now(UTC)
    assert (first.astimezone(UTC).hour, first.astimezone(UTC).minute) == (3, 30)  # 09:00 IST
    assert schedule["enabled"] is True and schedule["last_run_at"] is None


async def test_bad_schedules_are_422_with_a_specific_code(client: httpx.AsyncClient) -> None:
    cases = [
        ({"cron": "* * * * * *"}, "invalid_cron"),  # seconds: more often than once a minute
        ({"cron": "0 0 31 2 *"}, "invalid_cron"),
        ({"timezone": "Atlantis/Lost"}, "invalid_timezone"),
        ({"task": "nope"}, "unknown_task"),
        ({"payload": {"ms": "soon"}}, "invalid_payload"),
        ({"name": "has spaces"}, "validation_error"),
    ]
    for fields, code in cases:
        resp = await client.post("/v1/schedules", json=NIGHTLY | fields)
        assert (resp.status_code, resp.json()["error"]["code"]) == (422, code), fields


async def test_a_duplicate_name_is_409(client: httpx.AsyncClient) -> None:
    await create(client)
    resp = await client.post("/v1/schedules", json=NIGHTLY)
    assert (resp.status_code, resp.json()["error"]["code"]) == (409, "schedule_exists")


async def test_list_get_and_pages(client: httpx.AsyncClient) -> None:
    made = [await create(client, name=f"s{i}") for i in range(5)]
    names: list[str] = []
    cursor = None
    while True:
        params: dict[str, Any] = {"limit": 2} | ({"cursor": cursor} if cursor else {})
        page = (await client.get("/v1/schedules", params=params)).json()
        names += [s["name"] for s in page["schedules"]]
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert names == ["s0", "s1", "s2", "s3", "s4"]
    one = (await client.get(f"/v1/schedules/{made[2]['id']}")).json()
    assert one == made[2]


async def test_disable_then_enable_restarts_the_clock(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    schedule = await create(client)
    off = await client.patch(f"/v1/schedules/{schedule['id']}", json={"enabled": False})
    assert off.json()["enabled"] is False
    await make_due(api_app.state.engine, schedule["id"], ago=timedelta(days=3))

    on = (await client.patch(f"/v1/schedules/{schedule['id']}", json={"enabled": True})).json()
    assert on["enabled"] is True
    assert datetime.fromisoformat(on["next_run_at"]) > datetime.now(UTC)  # no stale tick
    assert await fire_due(api_app.state.engine, limit=100) == []


async def test_deleting_a_schedule_keeps_the_jobs_it_made(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    schedule = await create(client)
    await make_due(api_app.state.engine, schedule["id"])
    [fired] = await fire_due(api_app.state.engine, limit=100)

    assert (await client.delete(f"/v1/schedules/{schedule['id']}")).status_code == 204
    assert (await client.get(f"/v1/schedules/{schedule['id']}")).status_code == 404
    job = (await client.get(f"/v1/jobs/{fired.job_id}")).json()
    assert job["status"] == "queued" and job["schedule_id"] is None
    assert (await client.delete(f"/v1/schedules/{schedule['id']}")).status_code == 404


# --- the cron loop ---------------------------------------------------------------------------


async def test_a_due_schedule_fires_one_job_and_moves_on(
    client: httpx.AsyncClient, api_app: FastAPI, tenant: TenantCreds
) -> None:
    schedule = await create(client, cron="*/5 * * * *", payload={"ms": 7}, queue="reports")
    await make_due(api_app.state.engine, schedule["id"])
    due_at = (await schedule_row(api_app.state.engine, schedule["id"]))["next_run_at"]

    [fired] = await fire_due(api_app.state.engine, limit=100)

    [job] = await jobs_of(api_app.state.engine, schedule["id"])
    assert fired.job_id == job["id"]
    assert (job["tenant_id"], job["task"], job["payload"], job["queue"]) == (
        tenant.id,
        "sleep",
        {"ms": 7},
        "reports",
    )
    assert job["idempotency_key"] == cron_idempotency_key(uuid.UUID(schedule["id"]), due_at)
    after = await schedule_row(api_app.state.engine, schedule["id"])
    assert after["last_run_at"] == due_at
    assert after["next_run_at"] > datetime.now(UTC)
    assert after["next_run_at"].minute % 5 == 0
    assert await fire_due(api_app.state.engine, limit=100) == []  # nothing due any more


async def test_two_scheduler_loops_on_one_due_schedule_create_exactly_one_job(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    schedule = await create(client)
    await make_due(api_app.state.engine, schedule["id"])
    loops = [CronLoop(api_app.state.engine) for _ in range(2)]

    fired = await asyncio.gather(*(loop.tick() for loop in loops for _ in range(5)))

    assert sum(fired) == 1
    assert len(await jobs_of(api_app.state.engine, schedule["id"])) == 1


async def test_two_scheduler_loops_share_many_due_schedules_without_doubling(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    made = [await create(client, name=f"s{i}") for i in range(60)]
    async with api_app.state.engine.begin() as conn:
        await conn.execute(text("UPDATE schedules SET next_run_at = now()"))
    loops = [CronLoop(api_app.state.engine, batch_size=7) for _ in range(2)]

    fired = await asyncio.gather(*(loop.tick() for loop in loops))

    assert sum(fired) == 60
    async with api_app.state.engine.connect() as conn:
        per_schedule = (
            await conn.execute(text("SELECT count(*), count(DISTINCT schedule_id) FROM jobs"))
        ).one()
    assert tuple(per_schedule) == (60, len(made))


async def test_misfire_after_downtime_fires_once_then_jumps_ahead(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    """Five hours down on an hourly schedule: one catch-up run, not five."""
    schedule = await create(client, cron="0 * * * *")
    await make_due(api_app.state.engine, schedule["id"], ago=timedelta(hours=5))

    await CronLoop(api_app.state.engine).tick()

    assert len(await jobs_of(api_app.state.engine, schedule["id"])) == 1
    upcoming = (await schedule_row(api_app.state.engine, schedule["id"]))["next_run_at"]
    assert datetime.now(UTC) < upcoming <= datetime.now(UTC) + timedelta(hours=1)


async def test_a_disabled_schedule_does_not_fire(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    schedule = await create(client, enabled=False)
    await make_due(api_app.state.engine, schedule["id"])
    assert await CronLoop(api_app.state.engine).tick() == 0
    assert await jobs_of(api_app.state.engine, schedule["id"]) == []


async def test_the_idempotency_key_stops_a_double_fire(
    client: httpx.AsyncClient, api_app: FastAPI, tenant: TenantCreds
) -> None:
    """The second safety net: if a tick's job already exists, firing again adds nothing."""
    schedule = await create(client)
    await make_due(api_app.state.engine, schedule["id"])
    due_at = (await schedule_row(api_app.state.engine, schedule["id"]))["next_run_at"]
    async with api_app.state.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO jobs (tenant_id, task, payload, idempotency_key, schedule_id) "
                "VALUES (:t, 'sleep', CAST(:p AS jsonb), :k, :s)"
            ),
            {
                "t": tenant.id,
                "p": json.dumps({"ms": 1}),
                "k": cron_idempotency_key(uuid.UUID(schedule["id"]), due_at),
                "s": schedule["id"],
            },
        )

    [fired] = await fire_due(api_app.state.engine, limit=100)

    assert fired.job_id is None  # the insert found the key and did nothing
    assert len(await jobs_of(api_app.state.engine, schedule["id"])) == 1
    assert (await schedule_row(api_app.state.engine, schedule["id"]))["next_run_at"] > due_at


async def test_a_broken_expression_is_switched_off_without_blocking_others(
    client: httpx.AsyncClient, api_app: FastAPI, tenant: TenantCreds
) -> None:
    good = await create(client)
    async with api_app.state.engine.begin() as conn:
        bad_id = (
            await conn.execute(
                text(
                    "INSERT INTO schedules (tenant_id, name, cron, task, next_run_at) "
                    "VALUES (:t, 'legacy', 'not cron', 'sleep', now()) RETURNING id"
                ),
                {"t": tenant.id},
            )
        ).scalar_one()
    await make_due(api_app.state.engine, good["id"])

    await CronLoop(api_app.state.engine).tick()

    assert len(await jobs_of(api_app.state.engine, good["id"])) == 1
    assert (await schedule_row(api_app.state.engine, str(bad_id)))["enabled"] is False


async def test_a_cron_job_runs_through_a_worker(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    schedule = await create(client)
    await make_due(api_app.state.engine, schedule["id"])
    await CronLoop(api_app.state.engine).tick()

    await run_until(
        [make_worker(api_app.state.engine)],
        api_app.state.engine,
        lambda c: c == {"succeeded": 1},
    )

    [job] = await jobs_of(api_app.state.engine, schedule["id"])
    assert (job["status"], job["result"]) == ("succeeded", {"slept_ms": 1})
