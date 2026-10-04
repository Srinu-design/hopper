from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel


class AttemptOut(BaseModel):
    attempt: int
    worker_id: str
    started_at: datetime
    finished_at: datetime | None
    outcome: str
    error: str | None


class JobSummary(BaseModel):
    id: UUID
    task: str
    queue: str
    priority: int
    status: str
    payload: dict[str, Any]
    attempts: int
    max_attempts: int
    timeout_seconds: int
    replay_count: int
    schedule_id: UUID | None  # set when the cron loop created the job
    run_at: datetime
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    dead_at: datetime | None
    last_error: str | None
    result: dict[str, Any] | None


class JobOut(JobSummary):
    attempt_history: list[AttemptOut]


def job_out(job: dict[str, Any], attempts: list[dict[str, Any]]) -> JobOut:
    return JobOut.model_validate({**job, "attempt_history": attempts})
