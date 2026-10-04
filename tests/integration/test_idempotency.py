import asyncio
from typing import Any

import httpx
from fastapi import FastAPI
from sqlalchemy import text

from hopper.api.jobs import EnqueueRequest, request_hash

JOB = {"task": "sleep", "payload": {"ms": 5}}


async def job_count(api_app: FastAPI) -> int:
    async with api_app.state.engine.connect() as conn:
        count: int = (await conn.execute(text("SELECT count(*) FROM jobs"))).scalar_one()
    return count


async def post(client: httpx.AsyncClient, body: dict[str, Any], key: str | None) -> httpx.Response:
    headers = {"Idempotency-Key": key} if key is not None else {}
    return await client.post("/v1/jobs", json=body, headers=headers)


async def test_same_key_and_body_returns_the_same_job_with_200(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    first = await post(client, JOB, "order-42")
    retry = await post(client, JOB, "order-42")

    assert first.status_code == 201
    assert "idempotent-replayed" not in first.headers
    assert retry.status_code == 200
    assert retry.headers["idempotent-replayed"] == "true"
    assert retry.json()["id"] == first.json()["id"]
    assert await job_count(api_app) == 1


async def test_an_equivalent_body_counts_as_the_same_request(client: httpx.AsyncClient) -> None:
    first = await post(client, JOB, "k1")
    # Same meaning: defaults spelled out, keys in another order.
    spelled_out = {"priority": 0, "payload": {"ms": 5}, "queue": "default", "task": "sleep"}
    retry = await post(client, spelled_out, "k1")
    assert (retry.status_code, retry.json()["id"]) == (200, first.json()["id"])


async def test_same_key_with_a_different_body_is_422(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    await post(client, JOB, "order-43")
    resp = await post(client, {"task": "sleep", "payload": {"ms": 6}}, "order-43")

    assert resp.status_code == 422
    assert resp.json()["error"]["code"] == "idempotency_key_reused"
    assert await job_count(api_app) == 1


async def test_without_a_key_every_request_creates_a_job(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    await post(client, JOB, None)
    await post(client, JOB, None)
    await post(client, JOB, "a")
    await post(client, JOB, "b")
    assert await job_count(api_app) == 4


async def test_a_replayed_response_shows_the_job_as_it_is_now(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    job_id = (await post(client, JOB, "k2")).json()["id"]
    async with api_app.state.engine.begin() as conn:
        await conn.execute(
            text("UPDATE jobs SET status = 'succeeded', result = '{\"ok\": true}' WHERE id = :i"),
            {"i": job_id},
        )

    retry = (await post(client, JOB, "k2")).json()

    assert (retry["status"], retry["result"]) == ("succeeded", {"ok": True})


async def test_50_concurrent_requests_with_one_key_create_exactly_one_job(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    responses = await asyncio.gather(*(post(client, JOB, "burst") for _ in range(50)))

    statuses = sorted(r.status_code for r in responses)
    assert statuses == [200] * 49 + [201]
    assert len({r.json()["id"] for r in responses}) == 1
    assert await job_count(api_app) == 1


async def test_bad_keys_are_rejected(client: httpx.AsyncClient) -> None:
    for key in ("x" * 256, "has space", ""):
        resp = await post(client, JOB, key)
        assert resp.status_code == 422, key
        assert resp.json()["error"]["code"] == "validation_error"


def test_request_hash_is_canonical() -> None:
    a = EnqueueRequest.model_validate({"task": "sleep", "payload": {"b": 1, "a": 2}})
    b = EnqueueRequest.model_validate(
        {"payload": {"a": 2, "b": 1}, "task": "sleep", "queue": "default", "priority": 0}
    )
    c = EnqueueRequest.model_validate({"task": "sleep", "payload": {"a": 2, "b": 1}, "priority": 1})
    assert request_hash(a) == request_hash(b)
    assert request_hash(a) != request_hash(c)
    assert len(request_hash(a)) == 32  # SHA-256
