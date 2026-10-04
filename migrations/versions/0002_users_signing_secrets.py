"""users for admin login, per-tenant signing secrets, schedules deletable

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-04

Backward compatible with the Week 4 release, so a rollback can run against this schema:
one new table, one new column with a default, and a foreign key relaxed to SET NULL.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STATEMENTS = [
    # People who manage Hopper log in with email + password and get a short-lived JWT.
    # A platform admin manages every tenant; an owner manages exactly one.
    """
    CREATE TABLE users (
      id             uuid PRIMARY KEY DEFAULT gen_random_uuid(),
      email          text NOT NULL UNIQUE CHECK (email = lower(email)),
      password_hash  text NOT NULL,
      role           text NOT NULL CHECK (role IN ('owner', 'platform_admin')),
      tenant_id      uuid REFERENCES tenants(id) ON DELETE CASCADE,
      created_at     timestamptz NOT NULL DEFAULT now(),
      CHECK ((role = 'platform_admin') = (tenant_id IS NULL))
    )
    """,
    # The http task signs every request with HMAC-SHA256 under this per-tenant secret.
    # gen_random_uuid() draws from pg_strong_random(), so two of them give 244 random bits
    # without the pgcrypto extension. The default is volatile, so every existing tenant gets
    # its own secret, and the previous release can still insert tenants.
    """
    ALTER TABLE tenants ADD COLUMN signing_secret bytea NOT NULL
      DEFAULT (uuid_send(gen_random_uuid()) || uuid_send(gen_random_uuid()))
    """,
    # Deleting a schedule keeps the jobs it created; they just lose the link.
    "ALTER TABLE jobs DROP CONSTRAINT jobs_schedule_id_fkey",
    """
    ALTER TABLE jobs ADD CONSTRAINT jobs_schedule_id_fkey
      FOREIGN KEY (schedule_id) REFERENCES schedules(id) ON DELETE SET NULL
    """,
    "CREATE INDEX api_keys_tenant_idx ON api_keys (tenant_id)",
]


def upgrade() -> None:
    for statement in STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    op.execute("DROP INDEX api_keys_tenant_idx")
    op.execute("ALTER TABLE jobs DROP CONSTRAINT jobs_schedule_id_fkey")
    op.execute(
        "ALTER TABLE jobs ADD CONSTRAINT jobs_schedule_id_fkey "
        "FOREIGN KEY (schedule_id) REFERENCES schedules(id)"
    )
    op.execute("ALTER TABLE tenants DROP COLUMN signing_secret")
    op.execute("DROP TABLE users")
