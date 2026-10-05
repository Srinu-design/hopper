import asyncio
import contextlib
import signal

from hopper import metrics
from hopper.config import get_settings
from hopper.db import create_engine, create_redis
from hopper.logging import configure_logging
from hopper.queue.postgres import PostgresBroker
from hopper.scheduler.cron import CronLoop
from hopper.scheduler.depth import DepthLoop
from hopper.scheduler.reaper import Reaper
from hopper.scheduler.retention import Retention


async def main() -> None:
    """The scheduler process: the cron, reaper, depth and retention loops, side by side.

    Run two replicas for availability; cron, the reaper and retention take their rows with
    SKIP LOCKED, and both replicas count depth (the dashboard takes the max).
    """
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = create_engine(settings)
    redis = create_redis(settings)
    cron = CronLoop(
        engine, interval=settings.cron_interval_seconds, batch_size=settings.cron_batch_size
    )
    reaper = Reaper(
        PostgresBroker(engine),
        interval=settings.reaper_interval_seconds,
        batch_size=settings.reaper_batch_size,
    )
    depth = DepthLoop(
        engine,
        redis,
        namespace=settings.redis_namespace,
        interval=settings.depth_interval_seconds,
    )
    retention = Retention(
        engine,
        days=settings.retention_days,
        interval=settings.retention_interval_seconds,
        batch_size=settings.retention_batch_size,
    )
    loops = (cron, reaper, depth, retention)

    def stop() -> None:
        for each in loops:
            each.stop()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        # Not supported on Windows event loops; the container runs Linux.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop)
    stop_metrics = metrics.serve(settings.metrics_port)
    try:
        await asyncio.gather(*(each.run() for each in loops))
    finally:
        stop_metrics()
        await redis.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
