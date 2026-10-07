"""Set up what the API cannot create for itself.

    python -m hopper.bootstrap --email you@example.com   # the first platform admin
    python -m hopper.bootstrap --smoke-key               # the deploy smoke test's API key
    python -m hopper.bootstrap --bench-key               # the load and chaos tests' API key

The admin password is read from HOPPER_ADMIN_PASSWORD when it is set (for scripts), otherwise
it is prompted for without echoing. After this, everything else goes through the admin API:
POST /auth/token, then POST /admin/tenants.

--smoke-key creates the tenant "hopper-smoke" if needed, revokes its old keys and prints a new
one on stdout, alone, so a script can capture it. The deploy script uses it to push a real job
through each release before calling the release healthy.

--bench-key does the same for the tenant "hopper-bench", whose limits are high enough that the
load test measures Hopper rather than its own rate limit (the build guide: "test tenant's
limits raised"). The global queue-depth limit still applies.
"""

import argparse
import asyncio
import getpass
import os
import re
import sys
from decimal import Decimal

from hopper.auth import keys, passwords, store
from hopper.config import get_settings
from hopper.db import create_engine

_EMAIL = re.compile(r"[^@\s]{1,64}@[^@\s]+\.[^@\s]+")
SMOKE_TENANT = "hopper-smoke"
BENCH_TENANT = "hopper-bench"


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


async def rotate_tenant_key(
    tenant: str, *, rate_per_sec: Decimal, burst: int, max_queue_depth: int, key_name: str
) -> str:
    """A fresh API key for `tenant`, created with these limits if it does not exist yet;
    every older key of that tenant stops working."""
    settings = get_settings()
    engine = create_engine(settings)
    try:
        tenant_id = await store.get_tenant_id_by_name(engine, tenant)
        if tenant_id is None:
            try:
                created, _ = await store.create_tenant(
                    engine,
                    name=tenant,
                    rate_per_sec=rate_per_sec,
                    burst=burst,
                    max_queue_depth=max_queue_depth,
                )
                tenant_id = created.id
            except store.AlreadyExists:  # created by a concurrent run a moment ago
                tenant_id = await store.get_tenant_id_by_name(engine, tenant)
                assert tenant_id is not None
        await store.revoke_tenant_api_keys(engine, tenant_id=tenant_id)
        new = keys.generate(settings.api_key_pepper.get_secret_value().encode())
        await store.create_api_key(engine, tenant_id=tenant_id, name=key_name, key=new)
    finally:
        await engine.dispose()
    return new.plaintext


async def rotate_smoke_key() -> str:
    """A fresh API key for the smoke tenant; every older key of that tenant stops working.

    Small limits on purpose: a smoke test needs a handful of requests, and a leaked smoke key
    should not be able to do much.
    """
    return await rotate_tenant_key(
        SMOKE_TENANT,
        rate_per_sec=Decimal(5),
        burst=20,
        max_queue_depth=100,
        key_name="deploy smoke test",
    )


async def rotate_bench_key() -> str:
    """A fresh API key for the load-test tenant, whose limits stay out of the measurement.

    Rotating revokes the previous key, so a key handed out for one test run stops working at
    the next one (after the API's key cache, up to API_KEY_CACHE_SECONDS).
    """
    return await rotate_tenant_key(
        BENCH_TENANT,
        rate_per_sec=Decimal(100_000),
        burst=100_000,
        max_queue_depth=10_000_000,
        key_name="load and chaos tests",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m hopper.bootstrap",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    what = parser.add_mutually_exclusive_group(required=True)
    what.add_argument("--email", help="create the first platform admin with this email")
    what.add_argument("--smoke-key", action="store_true", help="print a new smoke-test API key")
    what.add_argument(
        "--bench-key", action="store_true", help="print a new load/chaos-test API key"
    )
    args = parser.parse_args(argv)
    if args.smoke_key or args.bench_key:
        if len(get_settings().api_key_pepper.get_secret_value()) < 16:
            print("API_KEY_PEPPER must be set, as for the API", file=sys.stderr)
            return 2
        print(asyncio.run(rotate_smoke_key() if args.smoke_key else rotate_bench_key()))
        return 0
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
