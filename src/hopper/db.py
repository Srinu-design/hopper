import redis.asyncio as redis_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hopper.config import Settings


def create_engine(settings: Settings) -> AsyncEngine:
    return create_async_engine(
        settings.database_url,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
    )


def create_redis(settings: Settings) -> redis_asyncio.Redis:
    """A Redis client with short timeouts and a blocking connection pool.

    The default pool raises at once when every connection is busy, which under a burst of
    requests looks exactly like Redis being down and would send the rate limiter into its
    in-process fallback (a test caught 300 requests let through for a burst of 100). A
    blocking pool makes the burst queue for a connection, for up to the timeout.
    """
    pool = redis_asyncio.BlockingConnectionPool.from_url(
        settings.redis_url,
        max_connections=settings.redis_max_connections,
        timeout=settings.redis_timeout_seconds,
        socket_timeout=settings.redis_timeout_seconds,
        socket_connect_timeout=settings.redis_timeout_seconds,
    )
    return redis_asyncio.Redis.from_pool(pool)
