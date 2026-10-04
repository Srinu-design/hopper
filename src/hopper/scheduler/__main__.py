import asyncio
import contextlib
import signal

from hopper.config import get_settings
from hopper.db import create_engine
from hopper.logging import configure_logging
from hopper.queue.postgres import PostgresBroker
from hopper.scheduler.cron import CronLoop
from hopper.scheduler.reaper import Reaper


async def main() -> None:
    """The scheduler process: the cron loop and the reaper loop, side by side.

    Run two replicas for availability; both loops claim their rows with SKIP LOCKED.
    """
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = create_engine(settings)
    cron = CronLoop(
        engine, interval=settings.cron_interval_seconds, batch_size=settings.cron_batch_size
    )
    reaper = Reaper(
        PostgresBroker(engine),
        interval=settings.reaper_interval_seconds,
        batch_size=settings.reaper_batch_size,
    )

    def stop() -> None:
        cron.stop()
        reaper.stop()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        # Not supported on Windows event loops; the container runs Linux.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop)
    try:
        await asyncio.gather(cron.run(), reaper.run())
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
