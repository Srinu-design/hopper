import asyncio
import time
from typing import Any

import httpx
from fastapi import FastAPI

from hopper.queue.postgres import PostgresBroker
from hopper.tasks.errors import PermanentError
from hopper.tasks.registry import TaskPayload, task
from hopper.worker.loop import Worker

# Stands in for a downstream dependency that is broken, then fixed.
_dependency = {"broken": True}


class NoPayload(TaskPayload):
    pass


@task("test_e2e_dependency", payload=NoPayload)
async def call_dependency(payload: NoPayload) -> dict[str, Any] | None:
    if _dependency["broken"]:
        raise PermanentError("dependency rejected the request")
    return {"delivered": True}


async def wait_for_status(
    client: httpx.AsyncClient, job_id: str, status: str, timeout: float = 10
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        job: dict[str, Any] = (await client.get(f"/v1/jobs/{job_id}")).json()
        if job["status"] == status:
            return job
        assert time.monotonic() < deadline, f"job stuck in {job['status']}"
        await asyncio.sleep(0.05)


async def test_dead_job_is_listed_replayed_and_then_succeeds(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    """Week 3 end to end: fail -> DLQ -> fix the cause -> replay -> succeeded."""
    _dependency["broken"] = True
    worker = Worker(
        PostgresBroker(api_app.state.engine),
        worker_id="e2e",
        queues=["default"],
        poll_interval=0.02,
        max_idle_backoff=0.1,
    )
    runner = asyncio.create_task(worker.run())
    try:
        job_id = (
            await client.post("/v1/jobs", json={"task": "test_e2e_dependency", "payload": {}})
        ).json()["id"]
        dead = await wait_for_status(client, job_id, "dead")
        assert dead["last_error"] == "PermanentError: dependency rejected the request"

        dlq = (await client.get("/v1/dlq")).json()
        assert [j["id"] for j in dlq["jobs"]] == [job_id]

        _dependency["broken"] = False
        replay = await client.post(f"/v1/jobs/{job_id}/replay")
        assert replay.status_code == 200

        done = await wait_for_status(client, job_id, "succeeded")
    finally:
        worker.stop()
        await runner

    assert done["result"] == {"delivered": True}
    assert done["replay_count"] == 1
    assert done["attempts"] == 1  # attempts restarted with the replay
    assert [(a["attempt"], a["outcome"]) for a in done["attempt_history"]] == [
        (1, "failed"),
        (1, "succeeded"),
    ]
    assert (await client.get("/v1/dlq")).json()["jobs"] == []
