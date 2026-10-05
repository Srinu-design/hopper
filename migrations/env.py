import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from hopper.config import get_settings

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name, disable_existing_loggers=False)

# Schema is hand-written SQL, so there is no SQLAlchemy metadata to autogenerate from.
target_metadata = None

# Migrations run against the live database while the previous release serves (ADR-0011). An
# ALTER that waits for a lock queues every later query on its table behind it, which would
# stall the API and the workers: give up after this long instead, and the deploy stops.
LOCK_TIMEOUT = "5s"


def run_migrations_offline() -> None:
    context.configure(
        url=get_settings().database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
        context.run_migrations()


async def run_migrations_online() -> None:
    engine = create_async_engine(get_settings().database_url, poolclass=pool.NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
