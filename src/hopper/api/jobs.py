import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal, Self
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query, Request, Response
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from hopper.api.errors import ApiError
from hopper.api.pagination import decode_cursor, encode_cursor
from hopper.api.schemas import JobOut, JobSummary, job_out
from hopper.api.validation import checked_task
from hopper.auth.tenant import current_tenant_id
from hopper.queue.dlq import DeadFilter, replay
from hopper.queue.jobs import JobFilter, cancel_job, get_job, insert_job, list_jobs

router = APIRouter(prefix="/v1/jobs", tags=["jobs"])

TenantId = Annotated[UUID, Depends(current_tenant_id)]
QUEUE_PATTERN = r"^[A-Za-z0-9_.-]{1,64}$"
MAX_DELAY = timedelta(days=30)
JobStatus = Literal["queued", "running", "succeeded", "dead", "cancelled"]


class EnqueueRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: str = Field(default="default", pattern=QUEUE_PATTERN)
    priority: int = Field(default=0, ge=-100, le=100)  # higher runs first
    # Run later: a delay, or an absolute time, at most 30 days out. Precision is the worker
    # poll interval (under 0.5 s). A run_at in the past simply runs now.
    delay_seconds: float | None = Field(default=None, ge=0, le=MAX_DELAY.total_seconds())
    run_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def one_start_time(self) -> Self:
        if self.delay_seconds is not None and self.run_at is not None:
            raise ValueError("give delay_seconds or run_at, not both")
        if self.run_at is not None:
            # Same instant, same hash, whatever offset the client wrote it in.
            self.run_at = self.run_at.astimezone(UTC)
            if self.run_at > datetime.now(UTC) + MAX_DELAY:
                raise ValueError("run_at is more than 30 days away")
        return self


class JobPage(BaseModel):
    jobs: list[JobSummary]
    next_cursor: str | None


def request_hash(body: EnqueueRequest) -> bytes:
    """SHA-256 of the canonical request: defaults filled in, keys sorted, no whitespace.

    Two requests that mean the same job hash the same even if one spells out a default.
    Unset optional fields are left out, so adding one never changes an existing hash.
    """
    fields = body.model_dump(mode="json", exclude_none=True)
    canonical = json.dumps(fields, sort_keys=True, separators=(",", ":"))
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
    if idempotency_key is not None and idempotency_key.startswith("cron:"):
        raise ApiError(422, "reserved_idempotency_key", "keys starting with cron: are reserved")
    spec = checked_task(body.task, body.payload)
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
        run_at=body.run_at,
        delay_seconds=body.delay_seconds or 0.0,
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


@router.get("", response_model=JobPage)
async def list_tenant_jobs(
    request: Request,
    tenant_id: TenantId,
    status: JobStatus | None = None,
    queue: Annotated[str | None, Query(pattern=QUEUE_PATTERN)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: str | None = None,
) -> JobPage:
    """This tenant's jobs, newest first. Pass next_cursor back as cursor for the next page."""
    rows = await list_jobs(
        request.app.state.engine,
        tenant_id=tenant_id,
        where=JobFilter(status=status, queue=queue),
        limit=limit + 1,  # one extra row tells us whether another page exists
        after=decode_cursor(cursor) if cursor else None,
    )
    page = rows[:limit]
    more = len(rows) > limit
    next_cursor = encode_cursor(page[-1]["created_at"], page[-1]["id"]) if more else None
    return JobPage(jobs=[JobSummary.model_validate(r) for r in page], next_cursor=next_cursor)


@router.get("/{job_id}", response_model=JobOut)
async def read_job(job_id: UUID, request: Request, tenant_id: TenantId) -> JobOut:
    return await _load(request, tenant_id, job_id)


@router.post("/{job_id}/cancel", response_model=JobOut)
async def cancel(job_id: UUID, request: Request, tenant_id: TenantId) -> JobOut:
    """Cancel a job that is still waiting. A running job cannot be cancelled (409)."""
    cancelled = await cancel_job(request.app.state.engine, tenant_id=tenant_id, job_id=job_id)
    job = await _load(request, tenant_id, job_id)  # 404 if it is not this tenant's job
    if not cancelled:
        raise ApiError(
            409, "not_queued", f"job is {job.status}; only a queued job can be cancelled"
        )
    return job


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
