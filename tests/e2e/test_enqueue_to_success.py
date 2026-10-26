import httpx
from fastapi import FastAPI

from tests.helpers import make_worker, run_until


async def test_job_goes_from_api_to_worker_to_succeeded(
    client: httpx.AsyncClient, api_app: FastAPI
) -> None:
    """Week 2 end to end: POST /v1/jobs, a worker claims, runs and acks, GET shows the result."""
    ids = []
    for ms in range(20):
        resp = await client.post("/v1/jobs", json={"task": "sleep", "payload": {"ms": ms}})
        assert resp.status_code == 201
        ids.append(resp.json()["id"])

    engine = api_app.state.engine
    worker = make_worker(engine, "e2e-worker", slots=8)
    await run_until([worker], engine, lambda c: c == {"succeeded": 20})

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
