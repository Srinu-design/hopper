import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.helpers import run_alembic

EXPECTED_TABLES = {"tenants", "api_keys", "schedules", "jobs", "job_attempts", "users"}
EXPECTED_INDEXES = {
    "jobs_ready_idx",
    "jobs_lease_idx",
    "jobs_dead_idx",
    "jobs_tenant_idx",
    "jobs_idem_uidx",
    "schedules_due_idx",
    "job_attempts_job_idx",
    "api_keys_tenant_idx",
}


async def _names(engine: AsyncEngine, sql: str) -> set[str]:
    async with engine.connect() as conn:
        return {row[0] for row in await conn.execute(text(sql))}


async def test_upgrade_creates_all_tables_and_indexes(migrated_engine: AsyncEngine) -> None:
    tables = await _names(
        migrated_engine, "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
    )
    assert tables >= EXPECTED_TABLES
    indexes = await _names(
        migrated_engine, "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"
    )
    assert indexes >= EXPECTED_INDEXES


async def test_hot_path_indexes_are_partial(migrated_engine: AsyncEngine) -> None:
    async with migrated_engine.connect() as conn:
        rows = await conn.execute(
            text("SELECT indexname, indexdef FROM pg_indexes WHERE indexname LIKE 'jobs_%_idx'")
        )
        defs = {name: definition for name, definition in rows}
    assert "WHERE (status = 'queued'::text)" in defs["jobs_ready_idx"]
    assert "WHERE (status = 'running'::text)" in defs["jobs_lease_idx"]
    assert "WHERE (status = 'dead'::text)" in defs["jobs_dead_idx"]


async def test_jobs_autovacuum_is_tuned(migrated_engine: AsyncEngine) -> None:
    async with migrated_engine.connect() as conn:
        opts = (
            await conn.execute(text("SELECT reloptions FROM pg_class WHERE relname = 'jobs'"))
        ).scalar_one()
    assert "autovacuum_vacuum_scale_factor=0.02" in opts


async def test_status_check_constraint_rejects_unknown_state(
    migrated_engine: AsyncEngine,
) -> None:
    async with migrated_engine.begin() as conn:
        tenant_id = (
            await conn.execute(text("INSERT INTO tenants (name) VALUES ('t') RETURNING id"))
        ).scalar_one()
    with pytest.raises(IntegrityError):
        async with migrated_engine.begin() as conn:
            await conn.execute(
                text("INSERT INTO jobs (tenant_id, task, status) VALUES (:t, 'sleep', 'bogus')"),
                {"t": tenant_id},
            )


async def test_job_defaults_match_the_data_model(migrated_engine: AsyncEngine) -> None:
    async with migrated_engine.begin() as conn:
        tenant_id = (
            await conn.execute(text("INSERT INTO tenants (name) VALUES ('t') RETURNING id"))
        ).scalar_one()
        row = (
            (
                await conn.execute(
                    text("INSERT INTO jobs (tenant_id, task) VALUES (:t, 'sleep') RETURNING *"),
                    {"t": tenant_id},
                )
            )
            .one()
            ._mapping
        )
    assert row["status"] == "queued"
    assert row["queue"] == "default"
    assert row["attempts"] == 0
    assert row["max_attempts"] == 5
    assert row["timeout_seconds"] == 30
    assert row["lease_token"] is None


async def test_idempotency_key_is_unique_per_tenant(migrated_engine: AsyncEngine) -> None:
    async with migrated_engine.begin() as conn:
        a = (
            await conn.execute(text("INSERT INTO tenants (name) VALUES ('a') RETURNING id"))
        ).scalar_one()
        b = (
            await conn.execute(text("INSERT INTO tenants (name) VALUES ('b') RETURNING id"))
        ).scalar_one()
        insert = text("INSERT INTO jobs (tenant_id, task, idempotency_key) VALUES (:t, 'x', :k)")
        await conn.execute(insert, {"t": a, "k": "same"})
        await conn.execute(insert, {"t": b, "k": "same"})  # another tenant: allowed
        # NULL keys never collide
        await conn.execute(text("INSERT INTO jobs (tenant_id, task) VALUES (:t, 'x')"), {"t": a})
        await conn.execute(text("INSERT INTO jobs (tenant_id, task) VALUES (:t, 'x')"), {"t": a})
    with pytest.raises(IntegrityError):
        async with migrated_engine.begin() as conn:
            await conn.execute(insert, {"t": a, "k": "same"})


async def test_downgrade_then_upgrade_round_trips(scratch_db_url: str) -> None:
    for args in (("upgrade", "head"), ("downgrade", "base"), ("upgrade", "head")):
        result = run_alembic(scratch_db_url, *args)
        assert result.returncode == 0, result.stderr


async def test_every_tenant_gets_its_own_signing_secret(migrated_engine: AsyncEngine) -> None:
    async with migrated_engine.begin() as conn:
        secrets = (
            (
                await conn.execute(
                    text("INSERT INTO tenants (name) VALUES ('a'), ('b') RETURNING signing_secret")
                )
            )
            .scalars()
            .all()
        )
    assert [len(s) for s in secrets] == [32, 32]
    assert secrets[0] != secrets[1]


@pytest.mark.parametrize(
    ("email", "role", "with_tenant"),
    [
        ("root@x.test", "platform_admin", True),  # a platform admin belongs to no tenant
        ("boss@x.test", "owner", False),  # an owner belongs to exactly one
        ("Boss@X.test", "owner", True),  # emails are stored lowercase
        ("who@x.test", "superuser", False),
    ],
)
async def test_users_table_refuses_inconsistent_rows(
    migrated_engine: AsyncEngine, email: str, role: str, with_tenant: bool
) -> None:
    async with migrated_engine.begin() as conn:
        tenant_id = (
            await conn.execute(text("INSERT INTO tenants (name) VALUES ('t') RETURNING id"))
        ).scalar_one()
    with pytest.raises(IntegrityError):
        async with migrated_engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO users (email, password_hash, role, tenant_id) "
                    "VALUES (:e, 'x', :r, :t)"
                ),
                {"e": email, "r": role, "t": tenant_id if with_tenant else None},
            )
