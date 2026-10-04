import base64
import binascii
from datetime import datetime
from typing import Annotated, Self
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from hopper.api.errors import ApiError
from hopper.api.schemas import JobSummary
from hopper.auth.tenant import current_tenant_id
from hopper.queue import dlq

router = APIRouter(prefix="/v1/dlq", tags=["dlq"])

TenantId = Annotated[UUID, Depends(current_tenant_id)]
QUEUE_PATTERN = r"^[A-Za-z0-9_.-]{1,64}$"


class DlqPage(BaseModel):
    jobs: list[JobSummary]
    next_cursor: str | None


class DlqFilterIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    queue: str | None = Field(default=None, pattern=QUEUE_PATTERN)
    task: str | None = Field(default=None, min_length=1, max_length=64)
    since: AwareDatetime | None = None


class ReplayRequest(BaseModel):
    """Exactly one of `ids` or `filter`. An empty filter means every dead job of the tenant."""

    model_config = ConfigDict(extra="forbid")

    ids: list[UUID] | None = Field(default=None, min_length=1, max_length=dlq.REPLAY_BATCH_LIMIT)
    filter: DlqFilterIn | None = None
    spread_seconds: int = Field(default=60, ge=0, le=3600)

    @model_validator(mode="after")
    def one_selector(self) -> Self:
        if (self.ids is None) == (self.filter is None):
            raise ValueError("give exactly one of ids or filter")
        return self


class ReplayResult(BaseModel):
    replayed: int
    job_ids: list[UUID]
    has_more: bool  # more dead jobs match: call again to replay the next batch


def encode_cursor(dead_at: datetime, job_id: UUID) -> str:
    return base64.urlsafe_b64encode(f"{dead_at.isoformat()}|{job_id}".encode()).decode()


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        dead_at, job_id = base64.urlsafe_b64decode(cursor.encode()).decode().split("|")
        return datetime.fromisoformat(dead_at), UUID(job_id)
    except (binascii.Error, UnicodeDecodeError, ValueError) as exc:
        raise ApiError(422, "invalid_cursor", "cursor is not valid") from exc


@router.get("", response_model=DlqPage)
async def list_dlq(
    request: Request,
    tenant_id: TenantId,
    queue: Annotated[str | None, Query(pattern=QUEUE_PATTERN)] = None,
    task: Annotated[str | None, Query(min_length=1, max_length=64)] = None,
    since: AwareDatetime | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: str | None = None,
) -> DlqPage:
    """Dead jobs, newest first. Pass next_cursor back as cursor for the next page."""
    rows = await dlq.list_dead(
        request.app.state.engine,
        tenant_id=tenant_id,
        where=dlq.DeadFilter(queue=queue, task=task, since=since),
        limit=limit + 1,  # one extra row tells us whether another page exists
        after=decode_cursor(cursor) if cursor else None,
    )
    page = rows[:limit]
    next_cursor = encode_cursor(page[-1]["dead_at"], page[-1]["id"]) if len(rows) > limit else None
    return DlqPage(jobs=[JobSummary.model_validate(r) for r in page], next_cursor=next_cursor)


@router.post("/replay", response_model=ReplayResult)
async def replay_dlq(body: ReplayRequest, request: Request, tenant_id: TenantId) -> ReplayResult:
    """Requeue up to 1,000 dead jobs, spreading their run_at over spread_seconds."""
    where = (
        dlq.DeadFilter(ids=body.ids)
        if body.ids is not None
        else dlq.DeadFilter(
            queue=body.filter.queue if body.filter else None,
            task=body.filter.task if body.filter else None,
            since=body.filter.since if body.filter else None,
        )
    )
    replayed, more = await dlq.replay(
        request.app.state.engine,
        tenant_id=tenant_id,
        where=where,
        spread_seconds=body.spread_seconds,
    )
    return ReplayResult(replayed=len(replayed), job_ids=replayed, has_more=more)
