"""tenants.internal_hosts: private host names a tenant's http jobs may call

Revision ID: 0006
Revises: 0005
Create Date: 2026-10-12

Sift (Project 2) runs its ingestion as http jobs that call sift-ingest by name on Hopper's own
Docker network: a private address the SSRF guard rightly refuses. A platform admin can now
allow named hosts for one tenant (PUT /admin/tenants/{id}/internal-hosts); every other tenant
keeps the strict guard, and link-local addresses (the cloud metadata endpoint) stay refused
even for an allowed name.

Backward compatible with the previous release, so a rollback can run against this schema: a
column with a constant default is a catalog-only change (no table rewrite), and the previous
release never reads it.
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("ALTER TABLE tenants ADD COLUMN internal_hosts text[] NOT NULL DEFAULT '{}'")


def downgrade() -> None:
    op.execute("ALTER TABLE tenants DROP COLUMN internal_hosts")
