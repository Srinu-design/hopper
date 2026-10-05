"""Tenants, admin users and API keys. Hand-written SQL, like the queue.

Functions that touch a tenant's data take tenant_id. Where a platform admin may act on any
tenant, the argument is still required and None means "every tenant", so it is never
forgotten by accident.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from hopper.auth.keys import NewKey
from hopper.auth.tokens import Role

INSERT_TENANT = """
INSERT INTO tenants (name, rate_per_sec, burst, max_queue_depth)
VALUES (:name, :rate_per_sec, :burst, :max_queue_depth)
ON CONFLICT (name) DO NOTHING
RETURNING id, name, rate_per_sec, burst, max_queue_depth, created_at, signing_secret
"""

INSERT_USER = """
INSERT INTO users (email, password_hash, role, tenant_id)
VALUES (:email, :password_hash, :role, :tenant_id)
ON CONFLICT (email) DO NOTHING
RETURNING id
"""

GET_USER_BY_EMAIL = "SELECT id, password_hash, role, tenant_id FROM users WHERE email = :email"

INSERT_API_KEY = """
INSERT INTO api_keys (tenant_id, prefix, key_hash, name)
SELECT id, :prefix, :key_hash, :name FROM tenants WHERE id = :tenant_id
RETURNING id, tenant_id, prefix, name, created_at, last_used_at, revoked_at
"""

# The tenant's limits come back with the key, so they are cached with it and rate limiting
# costs no extra query per request.
GET_API_KEY_BY_PREFIX = """
SELECT k.id, k.tenant_id, k.key_hash, k.revoked_at IS NOT NULL AS revoked,
       t.rate_per_sec, t.burst, t.max_queue_depth
FROM api_keys k JOIN tenants t ON t.id = k.tenant_id
WHERE k.prefix = :prefix
"""

# At most one write a minute per key, whichever API replica gets there first.
TOUCH_API_KEY = """
UPDATE api_keys SET last_used_at = now()
WHERE id = :id AND (last_used_at IS NULL OR last_used_at < now() - interval '1 minute')
"""

LIST_API_KEYS = """
SELECT id, tenant_id, prefix, name, created_at, last_used_at, revoked_at
FROM api_keys
WHERE (CAST(:tenant_id AS uuid) IS NULL OR tenant_id = :tenant_id)
ORDER BY created_at, id
"""

# coalesce keeps the first revocation time, so revoking twice is harmless.
REVOKE_API_KEY = """
UPDATE api_keys SET revoked_at = coalesce(revoked_at, now())
WHERE id = :id AND (CAST(:tenant_id AS uuid) IS NULL OR tenant_id = :tenant_id)
RETURNING prefix
"""

# Only live keys, so a key revoked earlier keeps its revocation time.
REVOKE_TENANT_API_KEYS = """
UPDATE api_keys SET revoked_at = now()
WHERE tenant_id = :tenant_id AND revoked_at IS NULL
"""

GET_TENANT_ID_BY_NAME = "SELECT id FROM tenants WHERE name = :name"


class AlreadyExists(Exception):
    pass


@dataclass(frozen=True, slots=True)
class Tenant:
    id: UUID
    name: str
    rate_per_sec: Decimal
    burst: int
    max_queue_depth: int
    created_at: datetime
    signing_secret: bytes


@dataclass(frozen=True, slots=True)
class User:
    id: UUID
    password_hash: str
    role: Role
    tenant_id: UUID | None


@dataclass(frozen=True, slots=True)
class KeyRecord:
    id: UUID
    tenant_id: UUID
    key_hash: bytes
    revoked: bool
    rate_per_sec: Decimal
    burst: int
    max_queue_depth: int


async def _insert_user(
    conn: AsyncConnection, email: str, password_hash: str, role: Role, tenant_id: UUID | None
) -> UUID:
    user_id: UUID | None = (
        await conn.execute(
            text(INSERT_USER),
            {"email": email, "password_hash": password_hash, "role": role, "tenant_id": tenant_id},
        )
    ).scalar_one_or_none()
    if user_id is None:
        raise AlreadyExists("email")
    return user_id


async def create_tenant(
    engine: AsyncEngine,
    *,
    name: str,
    rate_per_sec: Decimal = Decimal(50),
    burst: int = 100,
    max_queue_depth: int = 100_000,
    owner: tuple[str, str] | None = None,  # (email, password hash)
) -> tuple[Tenant, UUID | None]:
    """Create a tenant, and optionally its owner, in one transaction. Returns the owner id."""
    async with engine.begin() as conn:
        row = (
            await conn.execute(
                text(INSERT_TENANT),
                {
                    "name": name,
                    "rate_per_sec": rate_per_sec,
                    "burst": burst,
                    "max_queue_depth": max_queue_depth,
                },
            )
        ).first()
        if row is None:
            raise AlreadyExists("tenant")
        tenant = Tenant(**row._mapping)
        owner_id = None
        if owner is not None:
            owner_id = await _insert_user(conn, owner[0], owner[1], "owner", tenant.id)
    return tenant, owner_id


async def create_user(
    engine: AsyncEngine, *, email: str, password_hash: str, role: Role, tenant_id: UUID | None
) -> UUID:
    async with engine.begin() as conn:
        return await _insert_user(conn, email, password_hash, role, tenant_id)


async def get_user_by_email(engine: AsyncEngine, email: str) -> User | None:
    async with engine.connect() as conn:
        row = (await conn.execute(text(GET_USER_BY_EMAIL), {"email": email})).first()
    return User(**row._mapping) if row else None


async def create_api_key(
    engine: AsyncEngine, *, tenant_id: UUID, name: str, key: NewKey
) -> dict[str, Any] | None:
    """None if the tenant does not exist."""
    async with engine.begin() as conn:
        row = (
            await conn.execute(
                text(INSERT_API_KEY),
                {
                    "tenant_id": tenant_id,
                    "prefix": key.prefix,
                    "key_hash": key.key_hash,
                    "name": name,
                },
            )
        ).first()
    return dict(row._mapping) if row else None


async def get_api_key(engine: AsyncEngine, prefix: str) -> KeyRecord | None:
    async with engine.connect() as conn:
        row = (await conn.execute(text(GET_API_KEY_BY_PREFIX), {"prefix": prefix})).first()
    return KeyRecord(**row._mapping) if row else None


async def touch_api_key(engine: AsyncEngine, key_id: UUID) -> None:
    async with engine.begin() as conn:
        await conn.execute(text(TOUCH_API_KEY), {"id": key_id})


async def list_api_keys(engine: AsyncEngine, *, tenant_id: UUID | None) -> list[dict[str, Any]]:
    async with engine.connect() as conn:
        rows = await conn.execute(text(LIST_API_KEYS), {"tenant_id": tenant_id})
        return [dict(r._mapping) for r in rows]


async def get_tenant_id_by_name(engine: AsyncEngine, name: str) -> UUID | None:
    async with engine.connect() as conn:
        tenant_id: UUID | None = (
            await conn.execute(text(GET_TENANT_ID_BY_NAME), {"name": name})
        ).scalar_one_or_none()
    return tenant_id


async def revoke_tenant_api_keys(engine: AsyncEngine, *, tenant_id: UUID) -> int:
    """Revoke every live key of one tenant. Returns how many."""
    async with engine.begin() as conn:
        result = await conn.execute(text(REVOKE_TENANT_API_KEYS), {"tenant_id": tenant_id})
    return int(getattr(result, "rowcount", 0))


async def revoke_api_key(
    engine: AsyncEngine, *, tenant_id: UUID | None, key_id: UUID
) -> str | None:
    """Revoke a key. Returns its prefix, or None if there is no such key in scope."""
    async with engine.begin() as conn:
        prefix: str | None = (
            await conn.execute(text(REVOKE_API_KEY), {"id": key_id, "tenant_id": tenant_id})
        ).scalar_one_or_none()
    return prefix
