from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from hopper.api.errors import ApiError
from hopper.auth.tenant import current_tenant_id
from hopper.queue.jobs import get_job, insert_job
from hopper.tasks import registry

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])


class EnqueueRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: str = Field(default="default", pattern=r"^[A-Za-z0-9_.-]{1,64}$")
    priority: int = Field(default=0, ge=-100, le=100)  # higher runs first


class AttemptOut(BaseModel):
    attempt: int
    worker_id: str
    started_at: datetime
    finished_at: datetime | None
    outcome: str
    error: str | None


class JobOut(BaseModel):
    id: UUID
    task: str
    queue: str
    priority: int
    status: str
    payload: dict[str, Any]
    attempts: int
    max_attempts: int
    timeout_seconds: int
    run_at: datetime
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    last_error: str | None
    result: dict[str, Any] | None
    attempt_history: list[AttemptOut] = Field(default_factory=list)


def _out(job: dict[str, Any], attempts: list[dict[str, Any]]) -> JobOut:
    return JobOut.model_validate({**job, "attempt_history": attempts})


@router.post("", status_code=201, response_model=JobOut)
async def enqueue(
    body: EnqueueRequest,
    request: Request,
    tenant_id: Annotated[UUID, Depends(current_tenant_id)],
) -> JobOut:
    # Bodies over 256 KB never get this far: BodySizeLimitMiddleware answers 413.
    spec = registry.get_task(body.task)
    if spec is None:
        raise ApiError(
            422,
            "unknown_task",
            f"unknown task {body.task!r}; known tasks: {', '.join(registry.task_names())}",
        )
    try:
        spec.payload_model.model_validate(body.payload)
    except ValidationError as exc:
        first = exc.errors()[0]
        where = ".".join(str(part) for part in first["loc"]) or "payload"
        raise ApiError(422, "invalid_payload", f"{where}: {first['msg']}") from exc

    job = await insert_job(
        request.app.state.engine,
        tenant_id=tenant_id,
        queue=body.queue,
        task=body.task,
        payload=body.payload,
        priority=body.priority,
        max_attempts=spec.max_attempts,
        timeout_seconds=spec.timeout_seconds,
    )
    return _out(job, [])


@router.get("/{job_id}", response_model=JobOut)
async def read_job(
    job_id: UUID,
    request: Request,
    tenant_id: Annotated[UUID, Depends(current_tenant_id)],
) -> JobOut:
    found = await get_job(request.app.state.engine, tenant_id=tenant_id, job_id=job_id)
    if found is None:
        # Same answer for "no such job" and "someone else's job", so ids never leak.
        raise ApiError(404, "not_found", "job not found")
    job, attempts = found
    return _out(job, attempts)
