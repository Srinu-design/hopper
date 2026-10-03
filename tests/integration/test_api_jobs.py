import json
import uuid
from collections.abc import AsyncIterator

import httpx
from fastapi import FastAPI
from sqlalchemy import text


async def test_enqueue_creates_a_queued_job(client: httpx.AsyncClient) -> None:
    resp = await client.post("/v1/jobs", json={"task": "sleep", "payload": {"ms": 10}})

    assert resp.status_code == 201
    job = resp.json()
    assert uuid.UUID(job["id"])
    assert job["status"] == "queued"
    assert job["task"] == "sleep"
    assert job["queue"] == "default"
    assert job["priority"] == 0
    assert job["payload"] == {"ms": 10}
    assert job["attempts"] == 0
    assert job["max_attempts"] == 5 and job["timeout_seconds"] == 30  # from the task spec
    assert job["attempt_history"] == []


async def test_enqueue_honours_queue_and_priority(client: httpx.AsyncClient) -> None:
    resp = await client.post(
        "/v1/jobs", json={"task": "sleep", "payload": {"ms": 0}, "queue": "emails", "priority": 7}
    )
    assert resp.status_code == 201
    assert (resp.json()["queue"], resp.json()["priority"]) == ("emails", 7)


async def test_get_job_returns_it(client: httpx.AsyncClient) -> None:
    created = (await client.post("/v1/jobs", json={"task": "sleep", "payload": {"ms": 1}})).json()
    resp = await client.get(f"/v1/jobs/{created['id']}")
    assert resp.status_code == 200
    assert resp.json() == created


async def test_unknown_job_is_404_with_the_error_shape(client: httpx.AsyncClient) -> None:
    resp = await client.get(f"/v1/jobs/{uuid.uuid4()}", headers={"X-Request-ID": "req-123"})
    assert resp.status_code == 404
    assert resp.headers["x-request-id"] == "req-123"
    assert resp.json() == {
        "error": {"code": "not_found", "message": "job not found", "request_id": "req-123"}
    }


async def test_malformed_job_id_is_a_validation_error(client: httpx.AsyncClient) -> None:
    resp = await client.get("/v1/jobs/not-a-uuid")
    assert resp.status_code == 422
    assert set(resp.json()["error"]) == {"code", "message", "request_id"}
    assert resp.json()["error"]["code"] == "validation_error"


async def test_unknown_task_is_422(client: httpx.AsyncClient) -> None:
    resp = await client.post("/v1/jobs", json={"task": "nope", "payload": {}})
    assert resp.status_code == 422
    error = resp.json()["error"]
    assert error["code"] == "unknown_task"
    assert "sleep" in error["message"]  # lists what is available


async def test_invalid_task_payload_is_422(client: httpx.AsyncClient) -> None:
    for payload in ({"ms": -1}, {}, {"ms": 1, "typo": True}, {"ms": "abc"}):
        resp = await client.post("/v1/jobs", json={"task": "sleep", "payload": payload})
        assert resp.status_code == 422, payload
        assert resp.json()["error"]["code"] == "invalid_payload"


async def test_bad_request_bodies_are_422(client: httpx.AsyncClient) -> None:
    bad_bodies = [
        {},  # task is required
        {"task": "sleep", "payload": {"ms": 1}, "queue": "has space"},
        {"task": "sleep", "payload": {"ms": 1}, "priority": 1000},
        {"task": "sleep", "payload": {"ms": 1}, "tenant_id": str(uuid.uuid4())},  # never from body
    ]
    for body in bad_bodies:
        resp = await client.post("/v1/jobs", json=body)
        assert resp.status_code == 422, body
        assert resp.json()["error"]["code"] == "validation_error"


async def test_oversized_body_is_413(client: httpx.AsyncClient) -> None:
    big = {"task": "sleep", "payload": {"ms": 1, "pad": "x" * (300 * 1024)}}
    resp = await client.post("/v1/jobs", json=big)  # Content-Length declared up front
    assert resp.status_code == 413
    assert resp.json()["error"]["code"] == "payload_too_large"


async def test_oversized_streamed_body_is_413_without_reading_it_all(
    client: httpx.AsyncClient,
) -> None:
    chunks_sent = 0

    async def chunks() -> AsyncIterator[bytes]:
        nonlocal chunks_sent
        for _ in range(100):  # 6.4 MB if fully read
            chunks_sent += 1
            yield b"x" * 64 * 1024

    # No Content-Length: the body is chunked, so only counting bytes can catch it.
    resp = await client.post(
        "/v1/jobs", content=chunks(), headers={"content-type": "application/json"}
    )
    assert resp.status_code == 413
    assert resp.json()["error"]["code"] == "payload_too_large"
    assert chunks_sent < 10  # stopped just past 256 KB


async def test_body_just_under_the_limit_is_accepted(client: httpx.AsyncClient) -> None:
    raw = json.dumps({"task": "sleep", "payload": {"ms": 1}}).encode()
    # JSON allows whitespace before the closing brace: pad the body to 250 KB.
    padded = raw[:-1] + b" " * (250 * 1024 - len(raw)) + b"}"
    resp = await client.post(
        "/v1/jobs", content=padded, headers={"content-type": "application/json"}
    )
    assert resp.status_code == 201, resp.text


async def test_unsafe_request_ids_are_replaced(client: httpx.AsyncClient) -> None:
    resp = await client.get(f"/v1/jobs/{uuid.uuid4()}", headers={"X-Request-ID": "x" * 500})
    returned = resp.headers["x-request-id"]
    assert returned != "x" * 500
    assert len(returned) == 32
    assert resp.json()["error"]["request_id"] == returned


async def test_enqueue_writes_exactly_one_row(client: httpx.AsyncClient, api_app: FastAPI) -> None:
    await client.post("/v1/jobs", json={"task": "sleep", "payload": {"ms": 1}})
    await client.post("/v1/jobs", json={"task": "nope", "payload": {}})  # rejected: no row
    async with api_app.state.engine.connect() as conn:
        count = (await conn.execute(text("SELECT count(*) FROM jobs"))).scalar_one()
    assert count == 1
