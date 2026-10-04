"""Cron schedules: the cron loop in the scheduler process turns each tick into a job."""

from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from hopper.api.errors import ApiError
from hopper.api.limits import ReadTenant
from hopper.api.validation import checked_task
from hopper.queue import schedules
from hopper.scheduler.crontab import cron_error, timezone_error

router = APIRouter(prefix="/v1/schedules", tags=["schedules"])

NAME_PATTERN = r"^[A-Za-z0-9_.-]{1,64}$"


class ScheduleIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=NAME_PATTERN)  # unique per tenant
    cron: str = Field(min_length=9, max_length=128)  # five fields, in `timezone`
    timezone: str = Field(default="UTC", max_length=64)
    task: str = Field(min_length=1, max_length=64)
    payload: dict[str, Any] = Field(default_factory=dict)
    queue: str = Field(default="default", pattern=NAME_PATTERN)
    enabled: bool = True


class SchedulePatch(BaseModel):
    """Only enabling and disabling. To change the timing, delete and create again."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool


class ScheduleOut(BaseModel):
    id: UUID
    name: str
    cron: str
    timezone: str
    task: str
    payload: dict[str, Any]
    queue: str
    enabled: bool
    next_run_at: datetime
    last_run_at: datetime | None


class SchedulePage(BaseModel):
    schedules: list[ScheduleOut]
    next_cursor: str | None  # the last name on this page; pass it back as cursor


def _check(body: ScheduleIn) -> None:
    problem = cron_error(body.cron)
    if problem:
        raise ApiError(422, "invalid_cron", problem)
    problem = timezone_error(body.timezone)
    if problem:
        raise ApiError(422, "invalid_timezone", problem)
    checked_task(body.task, body.payload)


@router.post("", status_code=201, response_model=ScheduleOut)
async def create(body: ScheduleIn, request: Request, tenant_id: ReadTenant) -> ScheduleOut:
    _check(body)
    row = await schedules.create_schedule(
        request.app.state.engine,
        tenant_id=tenant_id,
        name=body.name,
        cron=body.cron,
        timezone=body.timezone,
        queue=body.queue,
        task=body.task,
        payload=body.payload,
        enabled=body.enabled,
    )
    if row is None:
        raise ApiError(409, "schedule_exists", f"a schedule named {body.name!r} exists")
    return ScheduleOut.model_validate(row)


@router.get("", response_model=SchedulePage)
async def list_all(
    request: Request,
    tenant_id: ReadTenant,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    cursor: Annotated[str | None, Query(max_length=64)] = None,
) -> SchedulePage:
    rows = await schedules.list_schedules(
        request.app.state.engine, tenant_id=tenant_id, limit=limit + 1, after=cursor or ""
    )
    page = rows[:limit]
    next_cursor = page[-1]["name"] if len(rows) > limit else None
    return SchedulePage(
        schedules=[ScheduleOut.model_validate(r) for r in page], next_cursor=next_cursor
    )


@router.get("/{schedule_id}", response_model=ScheduleOut)
async def read(schedule_id: UUID, request: Request, tenant_id: ReadTenant) -> ScheduleOut:
    row = await schedules.get_schedule(
        request.app.state.engine, tenant_id=tenant_id, schedule_id=schedule_id
    )
    if row is None:
        raise ApiError(404, "not_found", "schedule not found")
    return ScheduleOut.model_validate(row)


@router.patch("/{schedule_id}", response_model=ScheduleOut)
async def update(
    schedule_id: UUID, body: SchedulePatch, request: Request, tenant_id: ReadTenant
) -> ScheduleOut:
    """Enable or disable. Enabling restarts the clock from now: no stale tick fires."""
    row = await schedules.set_enabled(
        request.app.state.engine, tenant_id=tenant_id, schedule_id=schedule_id, enabled=body.enabled
    )
    if row is None:
        raise ApiError(404, "not_found", "schedule not found")
    return ScheduleOut.model_validate(row)


@router.delete("/{schedule_id}", status_code=204)
async def delete(schedule_id: UUID, request: Request, tenant_id: ReadTenant) -> Response:
    """Jobs the schedule already created are kept."""
    deleted = await schedules.delete_schedule(
        request.app.state.engine, tenant_id=tenant_id, schedule_id=schedule_id
    )
    if not deleted:
        raise ApiError(404, "not_found", "schedule not found")
    return Response(status_code=204)
