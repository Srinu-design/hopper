import asyncio
import uuid
from typing import Any

import httpx
from fastapi import FastAPI

from hopper.tasks.errors import PermanentError
from hopper.tasks.registry import TaskPayload, task
from tests.helpers import make_worker, status_in, wait_for

# Stands in for a downstream dependency that is broken, then fixed.
_dependency = {"broken": True}


class NoPayload(TaskPayload):
    pass


@task("test_e2e_dependency", payload=NoPayload)
async def call_dependency(payload: NoPayload) -> dict[str, Any] | None:
    if _dependency["broken"]:
        raise PermanentError("dependency rejected the request")
    return {"delivered": True}


async def test_dead_job_is_listed_replayed_and_then_succeeds(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    """Week 3 end to end: fail -> DLQ -> fix the cause -> replay -> succeeded."""
    _dependency["broken"] = True
    engine = api_app.state.engine
    worker = make_worker(engine, "e2e")
    runner = asyncio.create_task(worker.run())
    try:
        job_id = (
            await client.post("/v1/jobs", json={"task": "test_e2e_dependency", "payload": {}})
        ).json()["id"]
        await wait_for(status_in(engine, uuid.UUID(job_id), "dead"))
        dead = (await client.get(f"/v1/jobs/{job_id}")).json()
        assert dead["last_error"] == "PermanentError: dependency rejected the request"

        dlq = (await client.get("/v1/dlq")).json()
        assert [j["id"] for j in dlq["jobs"]] == [job_id]

        _dependency["broken"] = False
        replay = await client.post(f"/v1/jobs/{job_id}/replay")
        assert replay.status_code == 200

        await wait_for(status_in(engine, uuid.UUID(job_id), "succeeded"))
    finally:
        worker.stop()
        await runner

    done = (await client.get(f"/v1/jobs/{job_id}")).json()
    assert done["result"] == {"delivered": True}
    assert done["replay_count"] == 1
    assert done["attempts"] == 1  # attempts restarted with the replay
    assert [(a["attempt"], a["outcome"]) for a in done["attempt_history"]] == [
        (1, "failed"),
        (1, "succeeded"),
    ]
    assert (await client.get("/v1/dlq")).json()["jobs"] == []
