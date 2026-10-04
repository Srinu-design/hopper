"""Admin passwords, hashed with Argon2id (argon2-cffi defaults: RFC 9106 low-memory profile).

Hashing is deliberately slow (tens of milliseconds) and CPU-bound, so the API runs it in a
thread: on the event loop it would stall every other request.
"""

import asyncio

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError

_hasher = PasswordHasher()
# Verified against when the email is unknown, so "no such user" takes as long as
# "wrong password" and response timing does not reveal which emails exist.
_DUMMY_HASH = _hasher.hash("hopper-timing-equaliser")

MIN_LENGTH = 12


async def hash_password(password: str) -> str:
    return await asyncio.to_thread(_hasher.hash, password)


async def verify_password(password_hash: str | None, password: str) -> bool:
    def check() -> bool:
        try:
            return _hasher.verify(password_hash or _DUMMY_HASH, password)
        except (VerificationError, InvalidHashError):
            return False

    ok = await asyncio.to_thread(check)
    return ok and password_hash is not None
