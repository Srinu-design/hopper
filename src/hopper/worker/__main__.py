import asyncio
import contextlib
import os
import signal
import socket
import uuid

from hopper.config import get_settings
from hopper.db import create_engine
from hopper.logging import configure_logging
from hopper.queue.postgres import PostgresBroker
from hopper.tasks import http
from hopper.worker.loop import Worker


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)
    engine = create_engine(settings)
    await http.configure(
        http.HttpSettings(
            allow_private=settings.http_allow_private_networks,
            connect_timeout=settings.http_connect_timeout,
            read_timeout=settings.http_read_timeout,
        )
    )
    worker = Worker(
        PostgresBroker(engine),
        worker_id=f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}",
        queues=settings.queues,
        slots=settings.worker_slots,
        poll_interval=settings.worker_poll_interval,
        max_idle_backoff=settings.worker_max_idle_backoff,
        lease_seconds=settings.lease_seconds,
        heartbeat_interval=settings.heartbeat_seconds,
        shutdown_grace=settings.shutdown_grace_seconds,
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        # Not supported on Windows event loops; the container runs Linux.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, worker.stop)
    try:
        await worker.run()
    finally:
        await http.aclose()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
