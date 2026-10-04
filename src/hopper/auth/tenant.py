"""Tenant context for /v1 requests.

The tenant always comes from the credential (the API key), never from the request body,
path or query, so a client cannot act as another tenant by changing an id.
"""

from uuid import UUID

import structlog
from fastapi import Request

from hopper.api.errors import ApiError


def bearer_token(request: Request) -> str | None:
    scheme, _, credential = request.headers.get("authorization", "").partition(" ")
    if scheme.lower() != "bearer" or not credential.strip():
        return None
    return credential.strip()


async def current_tenant_id(request: Request) -> UUID:
    presented = bearer_token(request)
    tenant_id: UUID | None = None
    if presented is not None:
        tenant_id = await request.app.state.api_keys.authenticate(presented)
    if tenant_id is None:
        # One answer for missing, malformed, unknown, revoked and wrong keys.
        raise ApiError(
            401,
            "unauthorized",
            "a valid API key is required: Authorization: Bearer hop_live_...",
            headers={"WWW-Authenticate": "Bearer"},
        )
    structlog.contextvars.bind_contextvars(tenant_id=str(tenant_id))
    return tenant_id
