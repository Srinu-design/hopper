"""jobs.request_id for log correlation; tenant limits must be positive

Revision ID: 0003
Revises: 0002
Create Date: 2026-10-04

Backward compatible with the Week 5 release, so a rollback can run against this schema: a
nullable column without a default (a catalog-only change, no table rewrite) that the previous
release never reads or writes, and a check that every existing row already passes.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STATEMENTS = [
    # The X-Request-ID of the API call that enqueued the job, so one id ties the API's log
    # line to every worker log line for that job. NULL for cron jobs.
    "ALTER TABLE jobs ADD COLUMN request_id text",
    # The token bucket divides by rate_per_sec, and a bucket needs room for one token.
    """
    ALTER TABLE tenants ADD CONSTRAINT tenants_limits_check
      CHECK (rate_per_sec > 0 AND burst >= 1 AND max_queue_depth >= 1)
    """,
]


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    op.execute("ALTER TABLE tenants DROP CONSTRAINT tenants_limits_check")
    op.execute("ALTER TABLE jobs DROP COLUMN request_id")
