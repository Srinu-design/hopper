"""Run the mini broker:

    python -m hopper.minibroker --port 6390 --data ./broker-data --fsync everysec

On start it reads its log back (cutting off a torn last record), then serves. SIGTERM or
SIGINT stops it cleanly, with everything written and fsynced.
"""

import argparse
import asyncio
import contextlib
import signal
from pathlib import Path

import structlog

from hopper.logging import configure_logging
from hopper.minibroker.log import FSYNC_POLICIES, FsyncPolicy, Log
from hopper.minibroker.server import BrokerServer
from hopper.minibroker.store import Store

log = structlog.get_logger()
LOG_NAME = "broker.log"


def recover(data: Path) -> tuple[Store, Log]:
    """Memory rebuilt from the log in `data`, and the log open for appending."""
    data.mkdir(parents=True, exist_ok=True)
    path = data / LOG_NAME
    records, torn = Log.read(path)
    store = Store()
    store.load(records)
    store_log = Log(path)
    store.attach(store_log)
    log.info("broker_recovered", records=len(records), torn_bytes_cut=torn, **store.stats())
    return store, store_log


async def serve(
    store: Store, store_log: Log, host: str, port: int, fsync: FsyncPolicy, compact_min: int
) -> None:
    server = BrokerServer(store, store_log, fsync, compact_min_bytes=compact_min)
    bound = await server.start(host, port)
    log.info("broker_started", port=bound, fsync=fsync)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    await server.stop()
    log.info("broker_stopped")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m hopper.minibroker", description=__doc__)
    p.add_argument("--host", default="127.0.0.1", help="0.0.0.0 inside a container")
    p.add_argument("--port", type=int, default=6390)
    p.add_argument("--data", type=Path, default=Path("broker-data"))
    p.add_argument("--fsync", choices=FSYNC_POLICIES, default="everysec")
    p.add_argument("--compact-min-mb", type=int, default=64, help="never compact a smaller log")
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args(argv)
    configure_logging(args.log_level)
    store, store_log = recover(args.data)
    asyncio.run(
        serve(store, store_log, args.host, args.port, args.fsync, args.compact_min_mb << 20)
    )


if __name__ == "__main__":
    main()
