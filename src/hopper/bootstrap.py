"""Create the first platform admin.

    python -m hopper.bootstrap --email you@example.com

The password is read from HOPPER_ADMIN_PASSWORD when it is set (for scripts), otherwise it
is prompted for without echoing. After this, everything else goes through the admin API:
POST /auth/token, then POST /admin/tenants.
"""

import argparse
import asyncio
import getpass
import os
import re
import sys

from hopper.auth import passwords, store
from hopper.config import get_settings
from hopper.db import create_engine

_EMAIL = re.compile(r"[^@\s]{1,64}@[^@\s]+\.[^@\s]+")


async def create_platform_admin(email: str, password: str) -> str:
    engine = create_engine(get_settings())
    try:
        user_id = await store.create_user(
            engine,
            email=email,
            password_hash=await passwords.hash_password(password),
            role="platform_admin",
            tenant_id=None,
        )
    finally:
        await engine.dispose()
    return str(user_id)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m hopper.bootstrap", description=__doc__)
    parser.add_argument("--email", required=True)
    args = parser.parse_args(argv)
    email = args.email.strip().lower()
    if not _EMAIL.fullmatch(email):
        print(f"not an email address: {email!r}", file=sys.stderr)
        return 2
    password = os.environ.get("HOPPER_ADMIN_PASSWORD") or getpass.getpass("Password: ")
    if len(password) < passwords.MIN_LENGTH:
        print(f"use a password of at least {passwords.MIN_LENGTH} characters", file=sys.stderr)
        return 2
    try:
        user_id = asyncio.run(create_platform_admin(email, password))
    except store.AlreadyExists:
        print(f"a user with email {email} already exists", file=sys.stderr)
        return 1
    print(f"platform admin {email} created (id {user_id})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
