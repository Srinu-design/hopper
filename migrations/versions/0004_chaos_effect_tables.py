"""job_executions and job_effects, written by the effect task for the chaos test

Revision ID: 0004
Revises: 0003
Create Date: 2026-10-07

The effect task records every run in job_executions and its side effect in job_effects, once
per job (ON CONFLICT DO NOTHING). Comparing the two after workers are killed mid-run shows
at-least-once delivery at work: executions above the number of jobs are duplicate runs, and
exactly one effect per succeeded job shows the duplicates were harmless.

Backward compatible with the Week 7 release, so a rollback can run against this schema: two
new tables that the previous release never reads or writes. Rows go with their job
(ON DELETE CASCADE), so the retention loop cleans them up too.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STATEMENTS = [
    """
    CREATE TABLE job_executions (
      id           bigserial PRIMARY KEY,
      job_id       uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
      attempt      integer NOT NULL,
      worker_id    text NOT NULL,
      executed_at  timestamptz NOT NULL DEFAULT now()
    )
    """,
    "CREATE INDEX job_executions_job_idx ON job_executions (job_id)",
    """
    CREATE TABLE job_effects (
      job_id      uuid PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
      created_at  timestamptz NOT NULL DEFAULT now()
    )
    """,
]


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    op.execute("DROP TABLE job_effects")
    op.execute("DROP TABLE job_executions")
