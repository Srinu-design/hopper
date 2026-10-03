import asyncio
import time

import httpx
from fastapi import FastAPI

from hopper.queue.postgres import PostgresBroker
from hopper.worker.loop import Worker


async def test_job_goes_from_api_to_worker_to_succeeded(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    """Week 2 end to end: POST /v1/jobs, a worker claims, runs and acks, GET shows the result."""
    ids = []
    for ms in range(20):
        resp = await client.post("/v1/jobs", json={"task": "sleep", "payload": {"ms": ms}})
        assert resp.status_code == 201
        ids.append(resp.json()["id"])

    worker = Worker(
        PostgresBroker(api_app.state.engine),
        worker_id="e2e-worker",
        queues=["default"],
        slots=8,
        poll_interval=0.02,
        max_idle_backoff=0.1,
    )
    runner = asyncio.create_task(worker.run())
    try:
        deadline = time.monotonic() + 20
        pending = set(ids)
        while pending:
            assert time.monotonic() < deadline, f"still pending: {len(pending)}"
            for job_id in list(pending):
                if (await client.get(f"/v1/jobs/{job_id}")).json()["status"] == "succeeded":
                    pending.discard(job_id)
            await asyncio.sleep(0.05)
    finally:
        worker.stop()
        await runner

    for ms, job_id in enumerate(ids):
        job = (await client.get(f"/v1/jobs/{job_id}")).json()
        assert job["status"] == "succeeded"
        assert job["attempts"] == 1
        assert job["result"] == {"slept_ms": ms}
        assert job["finished_at"] is not None
        [attempt] = job["attempt_history"]
        assert (attempt["attempt"], attempt["worker_id"], attempt["outcome"]) == (
            1,
            "e2e-worker",
            "succeeded",
        )
