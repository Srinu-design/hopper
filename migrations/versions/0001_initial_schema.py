"""initial schema: tenants, api_keys, schedules, jobs, job_attempts

Revision ID: 0001
Revises:
Create Date: 2026-10-03

The dead-letter queue is not a table: it is jobs with status = 'dead'.
Every statement is plain SQL so the schema can be read and defended line by line.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STATEMENTS = [
    """
    CREATE TABLE tenants (
      id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      name             text NOT NULL UNIQUE,
      rate_per_sec     numeric NOT NULL DEFAULT 50,
      burst            integer NOT NULL DEFAULT 100,
      max_queue_depth  integer NOT NULL DEFAULT 100000,
      created_at       timestamptz NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE api_keys (
      id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      tenant_id     uuid NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
      prefix        text NOT NULL UNIQUE,
      key_hash      bytea NOT NULL,
      name          text NOT NULL,
      created_at    timestamptz NOT NULL DEFAULT now(),
      last_used_at  timestamptz,
      revoked_at    timestamptz
    )
    """,
    """
    CREATE TABLE schedules (
      id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      tenant_id    uuid NOT NULL REFERENCES tenants(id),
      name         text NOT NULL,
      cron         text NOT NULL,
      timezone     text NOT NULL DEFAULT 'UTC',
      queue        text NOT NULL DEFAULT 'default',
      task         text NOT NULL,
      payload      jsonb NOT NULL DEFAULT '{}',
      enabled      boolean NOT NULL DEFAULT true,
      next_run_at  timestamptz NOT NULL,
      last_run_at  timestamptz,
      UNIQUE (tenant_id, name)
    )
    """,
    """
    CREATE TABLE jobs (
      id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      tenant_id         uuid NOT NULL REFERENCES tenants(id),
      queue             text NOT NULL DEFAULT 'default',
      task              text NOT NULL,
      payload           jsonb NOT NULL DEFAULT '{}',
      priority          smallint NOT NULL DEFAULT 0,
      status            text NOT NULL DEFAULT 'queued'
                        CHECK (status IN ('queued','running','succeeded','dead','cancelled')),
      run_at            timestamptz NOT NULL DEFAULT now(),
      attempts          integer NOT NULL DEFAULT 0,
      max_attempts      integer NOT NULL DEFAULT 5,
      timeout_seconds   integer NOT NULL DEFAULT 30,
      lease_owner       text,
      lease_token       uuid,
      lease_expires_at  timestamptz,
      idempotency_key   text,
      request_hash      bytea,
      schedule_id       uuid REFERENCES schedules(id),
      last_error        text,
      result            jsonb,
      replay_count      integer NOT NULL DEFAULT 0,
      created_at        timestamptz NOT NULL DEFAULT now(),
      started_at        timestamptz,
      finished_at       timestamptz,
      dead_at           timestamptz
    )
    """,
    """
    CREATE TABLE job_attempts (
      id           bigserial PRIMARY KEY,
      job_id       uuid NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
      attempt      integer NOT NULL,
      worker_id    text NOT NULL,
      started_at   timestamptz NOT NULL,
      finished_at  timestamptz,
      outcome      text NOT NULL,
      error        text
    )
    """,
    # claim path: only ready rows are indexed
    """
    CREATE INDEX jobs_ready_idx ON jobs (queue, priority DESC, run_at)
      WHERE status = 'queued'
    """,
    # reaper path: only running rows are indexed
    """
    CREATE INDEX jobs_lease_idx ON jobs (lease_expires_at)
      WHERE status = 'running'
    """,
    # DLQ listing
    """
    CREATE INDEX jobs_dead_idx ON jobs (tenant_id, queue, dead_at DESC)
      WHERE status = 'dead'
    """,
    # tenant job listing (keyset pagination on created_at, id)
    "CREATE INDEX jobs_tenant_idx ON jobs (tenant_id, created_at DESC)",
    # idempotency: one key per tenant, for as long as the job row lives
    """
    CREATE UNIQUE INDEX jobs_idem_uidx ON jobs (tenant_id, idempotency_key)
      WHERE idempotency_key IS NOT NULL
    """,
    # cron loop
    "CREATE INDEX schedules_due_idx ON schedules (next_run_at) WHERE enabled",
    "CREATE INDEX job_attempts_job_idx ON job_attempts (job_id)",
    # jobs is updated constantly; vacuum it early (schema rule in the build guide)
    "ALTER TABLE jobs SET (autovacuum_vacuum_scale_factor = 0.02)",
]


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    # Dropping a table drops its indexes, so only tables need listing,
    # children before parents.
    for table in ("job_attempts", "jobs", "schedules", "api_keys", "tenants"):
        op.execute(f"DROP TABLE IF EXISTS {table}")
