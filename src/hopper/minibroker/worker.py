"""A Hopper worker on the mini broker, for its benchmark and chaos test:

    MINIBROKER_URL=tcp://127.0.0.1:6390 python -m hopper.minibroker.worker

It is the same Worker class as `python -m hopper.worker`, with the same leases, heartbeats,
fencing tokens, retries and graceful shutdown; only the Broker behind it changes, MiniBroker in
place of PostgresBroker. Production workers never import this module.

It also registers `mb_effect`, the chaos test's stand-in for a side effect, as `effect` is on
Postgres. Jobs on the mini broker have no row in Postgres, so this one records in Redis: one
MULTI that counts the run (HINCRBY runs <key> 1) and adds the job's key to a set of effects
(SADD effects <key>). The set makes the effect idempotent: a second run adds nothing. The key
is the client's, like an idempotency key: a PUSH retried after the broker crashed before
answering may create a second message for the same key, and that is one job, run twice.
"""

import asyncio
import contextlib
import os
import random
import signal
import socket
import uuid
from typing import Any

from pydantic import Field

from hopper import metrics
from hopper.config import get_settings
from hopper.db import create_redis
from hopper.logging import configure_logging
from hopper.minibroker.client import DEFAULT_URL, Client, MiniBroker
from hopper.tasks import registry
from hopper.worker.loop import Worker

_redis: Any = None


class MbEffectPayload(registry.TaskPayload):
    run: str = Field(max_length=64, pattern=r"^[A-Za-z0-9_.:-]+$")
    key: str = Field(max_length=128)
    min_ms: int = Field(default=50, ge=0, le=10_000)
    max_ms: int = Field(default=500, ge=0, le=10_000)


def keys(namespace: str, run: str) -> tuple[str, str]:
    """The Redis keys of one chaos run: runs per job key (a hash), and the job keys whose
    effect happened (a set)."""
    return f"{namespace}:mbchaos:{run}:runs", f"{namespace}:mbchaos:{run}:effects"


# Its timeout is longer than the lease, as for `effect`: a worker frozen past its lease must
# wake up and finish the job that was meanwhile run elsewhere.
@registry.task("mb_effect", payload=MbEffectPayload, timeout=60)
async def mb_effect(payload: MbEffectPayload) -> dict[str, Any] | None:
    """Sleep 50 to 500 ms, then record this run and the job's one effect in Redis."""
    if _redis is None:
        raise RuntimeError("mb_effect: no Redis configured in this worker")
    await asyncio.sleep(random.uniform(payload.min_ms, payload.max_ms) / 1000)
    runs, effects = keys(get_settings().redis_namespace, payload.run)
    async with _redis.pipeline(transaction=True) as pipe:
        pipe.hincrby(runs, payload.key, 1)
        pipe.sadd(effects, payload.key)
        await pipe.execute()
    return None


async def main() -> None:
    global _redis
    settings = get_settings()
    configure_logging(settings.log_level)
    _redis = create_redis(settings)
    client = Client(os.environ.get("MINIBROKER_URL", DEFAULT_URL))
    worker = Worker(
        MiniBroker(client),
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
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, worker.stop)
    stop_metrics = metrics.serve(settings.metrics_port)
    try:
        await worker.run()
    finally:
        stop_metrics()
        await client.close()
        await _redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
