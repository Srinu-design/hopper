"""Admin context for /admin requests: a short-lived JWT from POST /auth/token."""

from typing import Annotated

from fastapi import Depends, Request

from hopper.api.errors import ApiError
from hopper.auth import tokens
from hopper.auth.tenant import bearer_token
from hopper.config import get_settings


async def current_admin(request: Request) -> tokens.Admin:
    presented = bearer_token(request)
    settings = get_settings()
    try:
        if presented is None:
            raise tokens.InvalidToken("no token")
        return tokens.decode(
            presented,
            secret=settings.jwt_secret.get_secret_value(),
            issuer=settings.jwt_issuer,
            audience=settings.jwt_audience,
        )
    except tokens.InvalidToken as exc:
        raise ApiError(
            401,
            "unauthorized",
            "a valid admin token is required: POST /auth/token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc


AdminUser = Annotated[tokens.Admin, Depends(current_admin)]


async def platform_admin(admin: AdminUser) -> tokens.Admin:
    if admin.role != "platform_admin":
        raise ApiError(403, "forbidden", "only a platform admin can do this")
    return admin


PlatformAdmin = Annotated[tokens.Admin, Depends(platform_admin)]
