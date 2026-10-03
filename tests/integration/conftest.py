import os
import uuid
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from tests.helpers import run_alembic


@pytest.fixture
async def scratch_db_url() -> AsyncIterator[str]:
    """A throwaway database, so tests may migrate up and down without touching dev data."""
    base = make_url(os.environ["DATABASE_URL"])
    name = f"hopper_test_{uuid.uuid4().hex[:12]}"
    admin = create_async_engine(base.set(database="postgres"), isolation_level="AUTOCOMMIT")
    async with admin.connect() as conn:
        await conn.execute(text(f'CREATE DATABASE "{name}"'))
    try:
        yield base.set(database=name).render_as_string(hide_password=False)
    finally:
        async with admin.connect() as conn:
            await conn.execute(text(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)'))
        await admin.dispose()


@pytest.fixture
async def migrated_engine(scratch_db_url: str) -> AsyncIterator[AsyncEngine]:
    result = run_alembic(scratch_db_url, "upgrade", "head")
    assert result.returncode == 0, result.stderr
    engine = create_async_engine(scratch_db_url)
    try:
        yield engine
    finally:
        await engine.dispose()
