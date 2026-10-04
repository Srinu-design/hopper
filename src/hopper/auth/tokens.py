"""Short-lived admin JWTs (HS256).

Decoding passes an explicit algorithms=["HS256"] list and checks iss and aud, which rejects
the classic alg "none" token and algorithm-confusion attacks (a token signed with a different
algorithm, or minted for another service, never verifies).
"""

import time
from dataclasses import dataclass
from typing import Literal, cast
from uuid import UUID

import jwt

Role = Literal["owner", "platform_admin"]
ALGORITHM = "HS256"


class InvalidToken(Exception):
    pass


@dataclass(frozen=True, slots=True)
class Admin:
    user_id: UUID
    role: Role
    tenant_id: UUID | None  # None for a platform admin


def issue(admin: Admin, *, secret: str, issuer: str, audience: str, ttl_seconds: int) -> str:
    now = int(time.time())
    claims = {
        "sub": str(admin.user_id),
        "tenant_id": str(admin.tenant_id) if admin.tenant_id else None,
        "role": admin.role,
        "iat": now,
        "exp": now + ttl_seconds,
        "iss": issuer,
        "aud": audience,
    }
    return jwt.encode(claims, secret, algorithm=ALGORITHM)


def decode(token: str, *, secret: str, issuer: str, audience: str) -> Admin:
    try:
        claims = jwt.decode(
            token,
            secret,
            algorithms=[ALGORITHM],
            issuer=issuer,
            audience=audience,
            options={"require": ["sub", "role", "iat", "exp", "iss", "aud"]},
        )
        role = claims["role"]
        if role not in ("owner", "platform_admin"):
            raise InvalidToken("unknown role")
        tenant = claims.get("tenant_id")
        admin = Admin(UUID(claims["sub"]), cast(Role, role), UUID(tenant) if tenant else None)
    except (jwt.InvalidTokenError, ValueError, TypeError, KeyError) as exc:
        raise InvalidToken(str(exc)) from exc
    if (admin.role == "owner") != (admin.tenant_id is not None):
        raise InvalidToken("an owner token needs a tenant, a platform admin token has none")
    return admin
