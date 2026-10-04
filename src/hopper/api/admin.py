"""People managing Hopper: log in for a JWT, create tenants, create and revoke API keys."""

import base64
import hashlib
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field

from hopper.api.errors import ApiError
from hopper.api.limits import take_token
from hopper.auth import keys, passwords, store, tokens
from hopper.auth.admin import AdminUser, PlatformAdmin
from hopper.config import get_settings

router = APIRouter(tags=["admin"])

EMAIL_PATTERN = r"^[^@\s]{1,64}@[^@\s]+\.[^@\s]+$"
NAME_PATTERN = r"^[A-Za-z0-9_.-]{1,64}$"
# Password guessing: 10 attempts at once per email, then one every 6 s (10 a minute).
LOGIN_BURST = 10
LOGIN_RATE_PER_SEC = 10 / 60


class TokenRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: str = Field(max_length=254)
    password: str = Field(max_length=1024)


class TokenOut(BaseModel):
    access_token: str
    token_type: Literal["bearer"] = "bearer"
    expires_in: int


class OwnerIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: str = Field(max_length=254, pattern=EMAIL_PATTERN)
    password: str = Field(min_length=passwords.MIN_LENGTH, max_length=1024)


class TenantIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=NAME_PATTERN)
    rate_per_sec: float = Field(default=50, gt=0, le=100_000)  # token bucket refill (Week 6)
    burst: int = Field(default=100, ge=1, le=1_000_000)
    max_queue_depth: int = Field(default=100_000, ge=1)
    owner: OwnerIn | None = None


class TenantOut(BaseModel):
    id: UUID
    name: str
    rate_per_sec: float
    burst: int
    max_queue_depth: int
    created_at: datetime
    owner_id: UUID | None
    # Verifies the Hopper-Signature header on http task requests. Shown only here, once.
    signing_secret: str


class ApiKeyIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=64)
    tenant_id: UUID | None = None  # platform admins only: owners always get their own tenant


class ApiKeyOut(BaseModel):
    id: UUID
    tenant_id: UUID
    name: str
    prefix: str
    created_at: datetime
    last_used_at: datetime | None
    revoked_at: datetime | None


class ApiKeyCreated(ApiKeyOut):
    key: str  # the only time the full key is ever shown


def _tenant_scope(admin: tokens.Admin, requested: UUID | None) -> UUID | None:
    """The tenant an admin acts on. Owners are pinned to their own; None means every tenant."""
    if admin.role == "platform_admin":
        return requested
    if requested is not None and requested != admin.tenant_id:
        raise ApiError(404, "not_found", "tenant not found")  # not 403: ids do not leak
    return admin.tenant_id


@router.post("/auth/token", response_model=TokenOut)
async def issue_token(body: TokenRequest, request: Request) -> TokenOut:
    settings = get_settings()
    email = body.email.strip().lower()
    # One bucket per email, known or not, so a 429 says nothing about which emails exist.
    # The key holds a hash, not the address. Trade-off: someone hammering an email can delay
    # its owner's login by seconds (ADR-0008).
    await take_token(
        request,
        "login",
        f"login:{hashlib.sha256(email.encode()).hexdigest()[:32]}",
        capacity=LOGIN_BURST,
        rate=LOGIN_RATE_PER_SEC,
    )
    user = await store.get_user_by_email(request.app.state.engine, email)
    # verify_password runs even for an unknown email, so both failures take the same time.
    ok = await passwords.verify_password(user.password_hash if user else None, body.password)
    if user is None or not ok:
        raise ApiError(401, "invalid_credentials", "email or password is wrong")
    token = tokens.issue(
        tokens.Admin(user.id, user.role, user.tenant_id),
        secret=settings.jwt_secret.get_secret_value(),
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        ttl_seconds=settings.jwt_ttl_seconds,
    )
    return TokenOut(access_token=token, expires_in=settings.jwt_ttl_seconds)


@router.post("/admin/tenants", status_code=201, response_model=TenantOut)
async def create_tenant(body: TenantIn, request: Request, _: PlatformAdmin) -> TenantOut:
    owner = None
    if body.owner is not None:
        owner = (body.owner.email.lower(), await passwords.hash_password(body.owner.password))
    try:
        tenant, owner_id = await store.create_tenant(
            request.app.state.engine,
            name=body.name,
            rate_per_sec=Decimal(str(body.rate_per_sec)),
            burst=body.burst,
            max_queue_depth=body.max_queue_depth,
            owner=owner,
        )
    except store.AlreadyExists as exc:
        if str(exc) == "tenant":
            raise ApiError(409, "tenant_exists", f"a tenant named {body.name!r} exists") from exc
        raise ApiError(409, "email_taken", "a user with this email exists") from exc
    return TenantOut(
        id=tenant.id,
        name=tenant.name,
        rate_per_sec=float(tenant.rate_per_sec),
        burst=tenant.burst,
        max_queue_depth=tenant.max_queue_depth,
        created_at=tenant.created_at,
        owner_id=owner_id,
        signing_secret=base64.urlsafe_b64encode(tenant.signing_secret).decode(),
    )


@router.post("/admin/api-keys", status_code=201, response_model=ApiKeyCreated)
async def create_api_key(body: ApiKeyIn, request: Request, admin: AdminUser) -> ApiKeyCreated:
    tenant_id = _tenant_scope(admin, body.tenant_id)
    if tenant_id is None:
        raise ApiError(422, "tenant_id_required", "a platform admin must name the tenant_id")
    new = keys.generate(get_settings().api_key_pepper.get_secret_value().encode())
    row = await store.create_api_key(
        request.app.state.engine, tenant_id=tenant_id, name=body.name, key=new
    )
    if row is None:
        raise ApiError(404, "not_found", "tenant not found")
    return ApiKeyCreated(**row, key=new.plaintext)


@router.get("/admin/api-keys", response_model=list[ApiKeyOut])
async def list_api_keys(
    request: Request,
    admin: AdminUser,
    tenant_id: Annotated[UUID | None, Query()] = None,
) -> list[ApiKeyOut]:
    """Keys are listed without their secrets; only the prefix identifies them."""
    scope = _tenant_scope(admin, tenant_id)
    rows = await store.list_api_keys(request.app.state.engine, tenant_id=scope)
    return [ApiKeyOut(**r) for r in rows]


@router.delete("/admin/api-keys/{key_id}", status_code=204)
async def revoke_api_key(key_id: UUID, request: Request, admin: AdminUser) -> Response:
    """Revoke a key. Other API replicas notice within the key cache time (60 s)."""
    scope = _tenant_scope(admin, None)
    prefix = await store.revoke_api_key(request.app.state.engine, tenant_id=scope, key_id=key_id)
    if prefix is None:
        raise ApiError(404, "not_found", "API key not found")
    request.app.state.api_keys.evict(prefix)
    return Response(status_code=204)
