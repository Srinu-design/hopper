import asyncio
import contextlib
import signal

from hopper.config import get_settings
from hopper.db import create_engine
from hopper.logging import configure_logging
from hopper.queue.postgres import PostgresBroker
from hopper.scheduler.reaper import Reaper


async def main() -> None:
    """The scheduler process. Week 4: the reaper loop. The cron loop joins it in Week 5."""
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = create_engine(settings)
    reaper = Reaper(
        PostgresBroker(engine),
        interval=settings.reaper_interval_seconds,
        batch_size=settings.reaper_batch_size,
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        # Not supported on Windows event loops; the container runs Linux.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, reaper.stop)
    try:
        await reaper.run()
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
