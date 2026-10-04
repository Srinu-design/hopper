import hashlib
import json
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Request, Response
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from hopper.api.errors import ApiError
from hopper.api.schemas import JobOut, job_out
from hopper.auth.tenant import current_tenant_id
from hopper.queue.dlq import DeadFilter, replay
from hopper.queue.jobs import get_job, insert_job
from hopper.tasks import registry

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])

TenantId = Annotated[UUID, Depends(current_tenant_id)]


class EnqueueRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: str = Field(default="default", pattern=r"^[A-Za-z0-9_.-]{1,64}$")
    priority: int = Field(default=0, ge=-100, le=100)  # higher runs first


def request_hash(body: EnqueueRequest) -> bytes:
    """SHA-256 of the canonical request: defaults filled in, keys sorted, no whitespace.

    Two requests that mean the same job hash the same even if one spells out a default.
    """
    canonical = json.dumps(body.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).digest()


async def _load(request: Request, tenant_id: UUID, job_id: UUID) -> JobOut:
    found = await get_job(request.app.state.engine, tenant_id=tenant_id, job_id=job_id)
    if found is None:
        # Same answer for "no such job" and "someone else's job", so ids never leak.
        raise ApiError(404, "not_found", "job not found")
    return job_out(*found)


@router.post("", status_code=201, response_model=JobOut)
async def enqueue(
    body: EnqueueRequest,
    request: Request,
    response: Response,
    tenant_id: TenantId,
    idempotency_key: Annotated[
        str | None,
        Header(alias="Idempotency-Key", min_length=1, max_length=255, pattern=r"^[\x21-\x7e]+$"),
    ] = None,
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

    digest = request_hash(body)
    job, created = await insert_job(
        request.app.state.engine,
        tenant_id=tenant_id,
        queue=body.queue,
        task=body.task,
        payload=body.payload,
        priority=body.priority,
        max_attempts=spec.max_attempts,
        timeout_seconds=spec.timeout_seconds,
        idempotency_key=idempotency_key,
        request_hash=digest if idempotency_key is not None else None,
    )
    if created:
        return job_out(job, [])
    if job["request_hash"] != digest:
        raise ApiError(
            422,
            "idempotency_key_reused",
            "this Idempotency-Key was already used for a different request",
        )
    # The same request again (a client retry): return the job it created, as it is now.
    response.status_code = 200
    response.headers["Idempotent-Replayed"] = "true"
    return await _load(request, tenant_id, job["id"])


@router.get("/{job_id}", response_model=JobOut)
async def read_job(job_id: UUID, request: Request, tenant_id: TenantId) -> JobOut:
    return await _load(request, tenant_id, job_id)


@router.post("/{job_id}/replay", response_model=JobOut)
async def replay_job(job_id: UUID, request: Request, tenant_id: TenantId) -> JobOut:
    """Requeue one dead job now, with a fresh set of attempts."""
    replayed, _ = await replay(
        request.app.state.engine,
        tenant_id=tenant_id,
        where=DeadFilter(ids=[job_id]),
        spread_seconds=0,
    )
    job = await _load(request, tenant_id, job_id)  # 404 if it is not this tenant's job
    if not replayed:
        # Nothing changed: replaying a job that is no longer dead is a no-op.
        raise ApiError(409, "not_dead", f"job is {job.status}, not dead")
    return job
