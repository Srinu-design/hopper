"""Delayed jobs, listing a tenant's jobs, and cancelling a waiting job."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from hopper.queue.postgres import PostgresBroker
from tests.helpers import LEASE, job_row

SLEEP = {"task": "sleep", "payload": {"ms": 1}}


async def enqueue(client: httpx.AsyncClient, **extra: Any) -> dict[str, Any]:
    resp = await client.post("/v1/jobs", json=SLEEP | extra)
    assert resp.status_code == 201, resp.text
    body: dict[str, Any] = resp.json()
    return body


async def test_delay_seconds_is_measured_on_the_database_clock(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    job = await enqueue(client, delay_seconds=120)
    row = await job_row(api_app.state.engine, uuid.UUID(job["id"]))
    assert row["run_at"] - row["created_at"] == timedelta(seconds=120)


async def test_run_at_keeps_the_instant_whatever_the_offset(
    client: httpx.AsyncClient,
) -> None:
    india = timezone(timedelta(hours=5, minutes=30))
    when = (datetime.now(UTC) + timedelta(hours=2)).replace(microsecond=0)
    job = await enqueue(client, run_at=when.astimezone(india).isoformat())
    assert datetime.fromisoformat(job["run_at"]) == when


async def test_a_delayed_job_is_claimed_only_when_due(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    job = await enqueue(client, delay_seconds=1)
    broker = PostgresBroker(api_app.state.engine)
    assert await broker.claim("default", "w", 10, LEASE) == []  # not yet
    await asyncio.sleep(1.2)
    assert [j.id for j in await broker.claim("default", "w", 10, LEASE)] == [uuid.UUID(job["id"])]


async def test_a_run_at_in_the_past_runs_now(client: httpx.AsyncClient, api_app: FastAPI) -> None:
    job = await enqueue(client, run_at="2020-01-01T00:00:00Z")
    claimed = await PostgresBroker(api_app.state.engine).claim("default", "w", 10, LEASE)
    assert [j.id for j in claimed] == [uuid.UUID(job["id"])]


@pytest.mark.parametrize(
    "extra",
    [
        {"delay_seconds": 5, "run_at": "2030-01-01T00:00:00Z"},  # one or the other
        {"delay_seconds": -1},
        {"delay_seconds": 31 * 24 * 3600},  # over 30 days
        {"run_at": (datetime.now(UTC) + timedelta(days=31)).isoformat()},
        {"run_at": "2030-01-01T00:00:00"},  # no time zone
    ],
)
async def test_bad_start_times_are_422(client: httpx.AsyncClient, extra: dict[str, Any]) -> None:
    resp = await client.post("/v1/jobs", json=SLEEP | extra)
    assert (resp.status_code, resp.json()["error"]["code"]) == (422, "validation_error")


async def test_the_delay_is_part_of_an_idempotent_request(client: httpx.AsyncClient) -> None:
    key = {"Idempotency-Key": "delayed-once"}
    first = await client.post("/v1/jobs", json=SLEEP | {"delay_seconds": 60}, headers=key)
    again = await client.post("/v1/jobs", json=SLEEP | {"delay_seconds": 60}, headers=key)
    other = await client.post("/v1/jobs", json=SLEEP | {"delay_seconds": 61}, headers=key)
    assert (first.status_code, again.status_code, other.status_code) == (201, 200, 422)


async def test_cron_idempotency_keys_are_reserved(client: httpx.AsyncClient) -> None:
    resp = await client.post("/v1/jobs", json=SLEEP, headers={"Idempotency-Key": "cron:x:y"})
    assert (resp.status_code, resp.json()["error"]["code"]) == (422, "reserved_idempotency_key")


async def test_list_is_newest_first_filtered_and_paged(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    made = [(await enqueue(client))["id"] for _ in range(5)]
    emails = (await enqueue(client, queue="emails"))["id"]
    await client.post(f"/v1/jobs/{made[0]}/cancel")

    seen: list[str] = []
    cursor = None
    pages = 0
    while True:
        params: dict[str, Any] = {"limit": 2, "queue": "default"}
        if cursor:
            params["cursor"] = cursor
        page = (await client.get("/v1/jobs", params=params)).json()
        seen += [j["id"] for j in page["jobs"]]
        pages += 1
        cursor = page["next_cursor"]
        if cursor is None:
            break
    assert seen == made[::-1] and pages == 3  # newest first, no gaps, no repeats

    only_emails = (await client.get("/v1/jobs", params={"queue": "emails"})).json()["jobs"]
    assert [j["id"] for j in only_emails] == [emails]
    cancelled = (await client.get("/v1/jobs", params={"status": "cancelled"})).json()["jobs"]
    assert [j["id"] for j in cancelled] == [made[0]]
    bad = await client.get("/v1/jobs", params={"status": "exploded"})
    assert bad.status_code == 422


async def test_cancel_a_queued_job_and_no_worker_ever_claims_it(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    job = await enqueue(client)
    resp = await client.post(f"/v1/jobs/{job['id']}/cancel")
    assert resp.status_code == 200
    assert resp.json()["status"] == "cancelled" and resp.json()["finished_at"] is not None
    assert await PostgresBroker(api_app.state.engine).claim("default", "w", 10, LEASE) == []

    again = await client.post(f"/v1/jobs/{job['id']}/cancel")
    assert (again.status_code, again.json()["error"]["code"]) == (409, "not_queued")


async def test_a_running_job_cannot_be_cancelled(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    job = await enqueue(client)
    await PostgresBroker(api_app.state.engine).claim("default", "w", 10, LEASE)
    resp = await client.post(f"/v1/jobs/{job['id']}/cancel")
    assert resp.status_code == 409
    assert "running" in resp.json()["error"]["message"]


async def test_cancelling_an_unknown_job_is_404(client: httpx.AsyncClient) -> None:
    assert (await client.post(f"/v1/jobs/{uuid.uuid4()}/cancel")).status_code == 404
