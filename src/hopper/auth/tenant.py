"""Tenant context for requests.

PLACEHOLDER until Week 5: every request is attributed to one configured tenant. The tenant must
come from the credential (API key), never from the request, so this is a dependency that the
API-key auth replaces without touching any route. Do not deploy publicly before that swap.
"""

from uuid import UUID

from fastapi import Request

from hopper.config import get_settings
from hopper.queue.jobs import get_or_create_tenant


async def current_tenant_id(request: Request) -> UUID:
    tenant_id: UUID | None = getattr(request.app.state, "default_tenant_id", None)
    if tenant_id is None:
        tenant_id = await get_or_create_tenant(
            request.app.state.engine, get_settings().default_tenant_name
        )
        request.app.state.default_tenant_id = tenant_id
    return tenant_id
