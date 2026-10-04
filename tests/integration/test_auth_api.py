"""API keys for machines, JWTs for people, and who may do what."""

import uuid
from typing import Any

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text

from hopper.auth import passwords, store
from hopper.auth.api_keys import ApiKeyAuthenticator
from hopper.config import get_settings
from tests.helpers import TenantCreds, make_tenant

ADMIN_EMAIL, ADMIN_PASSWORD = "root@hopper.test", "platform-admin-password"
OWNER_EMAIL, OWNER_PASSWORD = "owner@acme.test", "acme-owner-password"


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


async def login(client: httpx.AsyncClient, email: str, password: str) -> str:
    resp = await client.post("/auth/token", json={"email": email, "password": password})
    assert resp.status_code == 200, resp.text
    body: dict[str, Any] = resp.json()
    assert (body["token_type"], body["expires_in"]) == ("bearer", 900)
    token: str = body["access_token"]
    return token


@pytest.fixture
async def platform_token(api_app: FastAPI, anon_client: httpx.AsyncClient) -> str:
    await store.create_user(
        api_app.state.engine,
        email=ADMIN_EMAIL,
        password_hash=await passwords.hash_password(ADMIN_PASSWORD),
        role="platform_admin",
        tenant_id=None,
    )
    return await login(anon_client, ADMIN_EMAIL, ADMIN_PASSWORD)


async def owner_of(api_app: FastAPI, client: httpx.AsyncClient, tenant: TenantCreds) -> str:
    email = f"owner@{tenant.name}.test"
    await store.create_user(
        api_app.state.engine,
        email=email,
        password_hash=await passwords.hash_password(OWNER_PASSWORD),
        role="owner",
        tenant_id=tenant.id,
    )
    return await login(client, email, OWNER_PASSWORD)


# --- API keys on /v1 -------------------------------------------------------------------------


async def test_v1_without_a_key_is_401_with_the_error_shape(anon_client: httpx.AsyncClient) -> None:
    resp = await anon_client.get("/v1/jobs", headers={"X-Request-ID": "req-401"})
    assert resp.status_code == 401
    assert resp.headers["www-authenticate"] == "Bearer"
    assert resp.json()["error"]["code"] == "unauthorized"
    assert resp.json()["error"]["request_id"] == "req-401"


@pytest.mark.parametrize(
    "authorization",
    [
        "Bearer not-a-key",
        "Bearer hop_live_abcdefgh_" + "A" * 43,  # well formed, no such prefix
        "Basic dXNlcjpwYXNz",
        "Bearer ",
    ],
)
async def test_bad_credentials_are_401(
    anon_client: httpx.AsyncClient, tenant: TenantCreds, authorization: str
) -> None:
    resp = await anon_client.get("/v1/jobs", headers={"Authorization": authorization})
    assert resp.status_code == 401


async def test_a_right_prefix_with_a_wrong_secret_is_401(
    anon_client: httpx.AsyncClient, tenant: TenantCreds
) -> None:
    forged = tenant.api_key[:-4] + ("AAAA" if not tenant.api_key.endswith("AAAA") else "BBBB")
    resp = await anon_client.get("/v1/jobs", headers=bearer(forged))
    assert resp.status_code == 401


async def test_last_used_at_is_written_at_most_once_a_minute(
    client: httpx.AsyncClient, api_app: FastAPI, tenant: TenantCreds
) -> None:
    async def last_used() -> Any:
        async with api_app.state.engine.connect() as conn:
            return (
                await conn.execute(
                    text("SELECT last_used_at FROM api_keys WHERE tenant_id = :t"),
                    {"t": tenant.id},
                )
            ).scalar_one()

    assert await last_used() is None
    assert (await client.get("/v1/jobs")).status_code == 200
    first = await last_used()
    for _ in range(5):
        assert (await client.get("/v1/jobs")).status_code == 200
    assert first is not None and await last_used() == first  # not a write per request


async def test_a_cached_key_stays_valid_elsewhere_until_the_cache_expires(
    api_app: FastAPI, tenant: TenantCreds
) -> None:
    """Another API replica's cache: revocation reaches it after cache_seconds, not at once."""
    now = [1000.0]
    replica = ApiKeyAuthenticator(
        api_app.state.engine,
        get_settings().api_key_pepper.get_secret_value().encode(),
        cache_seconds=60,
        clock=lambda: now[0],
    )
    assert await replica.authenticate(tenant.api_key) == tenant.id
    async with api_app.state.engine.begin() as conn:
        await conn.execute(text("UPDATE api_keys SET revoked_at = now()"))

    now[0] += 59
    assert await replica.authenticate(tenant.api_key) == tenant.id  # the documented trade-off
    now[0] += 2
    assert await replica.authenticate(tenant.api_key) is None


# --- the admin flow --------------------------------------------------------------------------


async def test_platform_admin_onboards_a_tenant_whose_owner_issues_a_working_key(
    anon_client: httpx.AsyncClient, platform_token: str
) -> None:
    created = await anon_client.post(
        "/admin/tenants",
        headers=bearer(platform_token),
        json={
            "name": "initech",
            "rate_per_sec": 25,
            "burst": 50,
            "owner": {"email": "Boss@Initech.test", "password": "a-long-owner-password"},
        },
    )
    assert created.status_code == 201, created.text
    tenant = created.json()
    assert (tenant["name"], tenant["rate_per_sec"], tenant["burst"]) == ("initech", 25.0, 50)
    assert len(tenant["signing_secret"]) == 44  # 32 bytes, base64url, shown once

    owner_token = await login(anon_client, "boss@initech.test", "a-long-owner-password")
    key = await anon_client.post(
        "/admin/api-keys", headers=bearer(owner_token), json={"name": "billing service"}
    )
    assert key.status_code == 201, key.text
    secret = key.json()["key"]
    assert secret.startswith(f"hop_live_{key.json()['prefix']}_")
    assert key.json()["tenant_id"] == tenant["id"]

    job = await anon_client.post(
        "/v1/jobs", headers=bearer(secret), json={"task": "sleep", "payload": {"ms": 1}}
    )
    assert job.status_code == 201

    listed = (await anon_client.get("/admin/api-keys", headers=bearer(owner_token))).json()
    assert [k["prefix"] for k in listed] == [key.json()["prefix"]]
    assert "key" not in listed[0]  # never shown again
    assert listed[0]["last_used_at"] is not None

    revoked = await anon_client.delete(
        f"/admin/api-keys/{key.json()['id']}", headers=bearer(owner_token)
    )
    assert revoked.status_code == 204
    # Evicted from this replica's cache at once.
    assert (await anon_client.get("/v1/jobs", headers=bearer(secret))).status_code == 401


async def test_wrong_password_and_unknown_email_get_the_same_answer(
    anon_client: httpx.AsyncClient, platform_token: str
) -> None:
    wrong = await anon_client.post(
        "/auth/token", json={"email": ADMIN_EMAIL, "password": "not-the-password"}
    )
    unknown = await anon_client.post(
        "/auth/token", json={"email": "nobody@hopper.test", "password": "not-the-password"}
    )
    assert wrong.status_code == unknown.status_code == 401
    assert wrong.json()["error"]["message"] == unknown.json()["error"]["message"]


async def test_credentials_only_work_where_they_belong(
    anon_client: httpx.AsyncClient, api_app: FastAPI, tenant: TenantCreds, platform_token: str
) -> None:
    owner_token = await owner_of(api_app, anon_client, tenant)

    # An owner is not a platform admin.
    resp = await anon_client.post(
        "/admin/tenants", headers=bearer(owner_token), json={"name": "sneaky"}
    )
    assert (resp.status_code, resp.json()["error"]["code"]) == (403, "forbidden")
    # An API key is not an admin token, and an admin token is not an API key.
    assert (
        await anon_client.get("/admin/api-keys", headers=bearer(tenant.api_key))
    ).status_code == 401
    assert (await anon_client.get("/v1/jobs", headers=bearer(platform_token))).status_code == 401
    assert (await anon_client.post("/admin/tenants", json={"name": "x"})).status_code == 401


async def test_an_owner_cannot_touch_another_tenants_keys(
    anon_client: httpx.AsyncClient, api_app: FastAPI, tenant: TenantCreds
) -> None:
    other = await make_tenant(api_app.state.engine, "umbrella")
    owner_token = await owner_of(api_app, anon_client, tenant)
    async with api_app.state.engine.connect() as conn:
        other_key_id = (
            await conn.execute(
                text("SELECT id FROM api_keys WHERE tenant_id = :t"), {"t": other.id}
            )
        ).scalar_one()

    create = await anon_client.post(
        "/admin/api-keys",
        headers=bearer(owner_token),
        json={"name": "x", "tenant_id": str(other.id)},
    )
    listing = await anon_client.get(
        "/admin/api-keys", headers=bearer(owner_token), params={"tenant_id": str(other.id)}
    )
    revoke = await anon_client.delete(
        f"/admin/api-keys/{other_key_id}", headers=bearer(owner_token)
    )
    assert create.status_code == listing.status_code == revoke.status_code == 404
    assert (await anon_client.get("/v1/jobs", headers=bearer(other.api_key))).status_code == 200


async def test_platform_admin_manages_keys_for_a_named_tenant(
    anon_client: httpx.AsyncClient, tenant: TenantCreds, platform_token: str
) -> None:
    missing = await anon_client.post(
        "/admin/api-keys", headers=bearer(platform_token), json={"name": "smoke"}
    )
    assert (missing.status_code, missing.json()["error"]["code"]) == (422, "tenant_id_required")
    nowhere = await anon_client.post(
        "/admin/api-keys",
        headers=bearer(platform_token),
        json={"name": "smoke", "tenant_id": str(uuid.uuid4())},
    )
    assert nowhere.status_code == 404
    made = await anon_client.post(
        "/admin/api-keys",
        headers=bearer(platform_token),
        json={"name": "smoke", "tenant_id": str(tenant.id)},
    )
    assert made.status_code == 201
    every = (await anon_client.get("/admin/api-keys", headers=bearer(platform_token))).json()
    assert {k["tenant_id"] for k in every} == {str(tenant.id)}
    assert len(every) == 2


async def test_duplicate_tenant_names_and_emails_are_409(
    anon_client: httpx.AsyncClient, tenant: TenantCreds, platform_token: str
) -> None:
    same_name = await anon_client.post(
        "/admin/tenants", headers=bearer(platform_token), json={"name": tenant.name}
    )
    assert (same_name.status_code, same_name.json()["error"]["code"]) == (409, "tenant_exists")
    same_email = await anon_client.post(
        "/admin/tenants",
        headers=bearer(platform_token),
        json={"name": "newco", "owner": {"email": ADMIN_EMAIL, "password": "long-enough-pw"}},
    )
    assert (same_email.status_code, same_email.json()["error"]["code"]) == (409, "email_taken")
    # The whole create rolled back: no half-made tenant without its owner.
    retry = await anon_client.post(
        "/admin/tenants", headers=bearer(platform_token), json={"name": "newco"}
    )
    assert retry.status_code == 201


async def test_short_owner_passwords_are_refused(
    anon_client: httpx.AsyncClient, platform_token: str
) -> None:
    resp = await anon_client.post(
        "/admin/tenants",
        headers=bearer(platform_token),
        json={"name": "weak", "owner": {"email": "a@weak.test", "password": "short"}},
    )
    assert resp.status_code == 422
