"""index jobs.schedule_id, so deleting a schedule does not scan every job

Revision ID: 0005
Revises: 0004
Create Date: 2026-10-08

Deleting a schedule sets schedule_id to NULL on the jobs it created (ON DELETE SET NULL,
migration 0002). With no index on the column, Postgres found those jobs by reading the whole
jobs table, once per deleted schedule. The index is partial: jobs enqueued through the API
have no schedule, so only cron jobs are indexed.

It is built CONCURRENTLY, so the live release keeps inserting and claiming jobs while it
builds: a plain CREATE INDEX would block every write to jobs until it finished. That cannot
run inside a transaction, hence the autocommit block, which needs its own lock_timeout (the
one env.py sets is local to the transaction). If the build fails, it leaves an invalid index
behind, so a retry drops it first.

Backward compatible with the Week 8 release, so a rollback can run against this schema: an
index changes no table, and the previous release never names it.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS jobs_schedule_idx")
        op.execute(
            "CREATE INDEX CONCURRENTLY jobs_schedule_idx ON jobs (schedule_id) "
            "WHERE schedule_id IS NOT NULL"
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("SET lock_timeout = '5s'")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS jobs_schedule_idx")
