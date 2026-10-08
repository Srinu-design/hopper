"""The broker benchmark's client. It runs inside the Hopper image on the stack's Docker network
(loadtest/broker_bench.py starts it), so it reaches the mini broker and Postgres the same way.

Each of N connections loops: push a message, pull one, ack it. One loop is a round trip, timed
from the push to the ack. For the mini broker that is three commands on the loop's own
connection; for Postgres it is the production broker's own statements, each in its own
transaction (insert the job, claim with SKIP LOCKED, fenced ack), on a pool of N connections.

Prints one JSON object: for each connection count, the round trips per second and the p50 and
p99 latency of each run.
"""

import argparse
import asyncio
import json
import statistics
import time
import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from hopper.minibroker.client import Client, MiniBroker
from hopper.queue.broker import Broker
from hopper.queue.postgres import PostgresBroker

QUEUE = "broker-bench"
TENANT = "hopper-broker-bench"


class Target:
    """Push, pull and ack on one broker."""

    async def open(self, connections: int) -> list[Any]:
        """Every connection is open before the clock starts: connecting is not measured."""
        raise NotImplementedError

    async def round_trip(self, conn: Any) -> bool:
        raise NotImplementedError

    async def close(self) -> None:
        pass


class Mini(Target):
    def __init__(self, url: str) -> None:
        self.url = url
        self.clients: list[Client] = []
        self.tenant = uuid.uuid4()

    async def open(self, connections: int) -> list[Any]:
        self.clients = [Client(self.url, size=1) for _ in range(connections)]
        await asyncio.gather(*(c.call("PING") for c in self.clients))
        return [MiniBroker(c) for c in self.clients]

    async def round_trip(self, broker: MiniBroker) -> bool:
        await broker.push(queue=QUEUE, task="sleep", payload={"ms": 0}, tenant_id=self.tenant)
        return await pull_and_ack(broker)

    async def close(self) -> None:
        for c in self.clients:
            await c.close()


class Postgres(Target):
    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self.engine: AsyncEngine | None = None
        self.tenant: uuid.UUID | None = None

    async def open(self, connections: int) -> list[Any]:
        self.engine = create_async_engine(self.dsn, pool_size=connections, max_overflow=0)
        async with self.engine.begin() as conn:
            self.tenant = await conn.scalar(
                text(
                    "INSERT INTO tenants (name) VALUES (:n) ON CONFLICT (name) "
                    "DO UPDATE SET name = excluded.name RETURNING id"
                ),
                {"n": TENANT},
            )
            await conn.execute(text("DELETE FROM jobs WHERE tenant_id = :t"), {"t": self.tenant})
        engine = self.engine

        async def hold() -> None:  # all at once, so the pool opens every connection
            async with engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
                await asyncio.sleep(0.2)

        await asyncio.gather(*(hold() for _ in range(connections)))
        return [PostgresBroker(self.engine)] * connections

    async def round_trip(self, broker: PostgresBroker) -> bool:
        assert self.engine is not None
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    "INSERT INTO jobs (tenant_id, queue, task, payload) "
                    "VALUES (:t, :q, 'sleep', CAST('{\"ms\": 0}' AS jsonb))"
                ),
                {"t": self.tenant, "q": QUEUE},
            )
        return await pull_and_ack(broker)

    async def close(self) -> None:
        if self.engine is not None:
            async with self.engine.begin() as conn:
                await conn.execute(
                    text("DELETE FROM jobs WHERE tenant_id = :t"), {"t": self.tenant}
                )
            await self.engine.dispose()


async def pull_and_ack(broker: Broker) -> bool:
    for _ in range(100):
        jobs = await broker.claim(QUEUE, "bench", 1, 30)
        if jobs:
            return await broker.ack(jobs[0], "bench", None)
        await asyncio.sleep(0)  # another connection took it: ours is next
    return False


async def measure(
    target: Target, connections: int, seconds: float, warmup: float
) -> dict[str, Any]:
    conns = await target.open(connections)
    latencies: list[float] = []
    failed = 0
    started = time.monotonic()
    measure_from = started + warmup
    stop_at = measure_from + seconds

    async def loop(conn: Any) -> None:
        nonlocal failed
        while (now := time.monotonic()) < stop_at:
            ok = await target.round_trip(conn)
            if now >= measure_from:
                latencies.append(time.monotonic() - now)
                failed += not ok

    try:
        await asyncio.gather(*(loop(c) for c in conns))
    finally:
        await target.close()
    latencies.sort()
    q = (
        statistics.quantiles(latencies, n=100, method="inclusive")
        if len(latencies) > 1
        else [0] * 99
    )
    return {
        "connections": connections,
        "round_trips_per_s": len(latencies) / seconds,
        "p50_ms": q[49] * 1000,
        "p99_ms": q[98] * 1000,
        "failed": failed,
    }


async def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--target", choices=["mini", "postgres"], required=True)
    p.add_argument("--url", default="tcp://minibroker:6390")
    p.add_argument("--dsn", default="")
    p.add_argument("--connections", default="1,8,32,64")
    p.add_argument("--seconds", type=float, default=10)
    p.add_argument("--warmup", type=float, default=2)
    args = p.parse_args()
    results = []
    for n in (int(c) for c in args.connections.split(",")):
        target: Target = Mini(args.url) if args.target == "mini" else Postgres(args.dsn)
        results.append(await measure(target, n, args.seconds, args.warmup))
    print(json.dumps(results))


if __name__ == "__main__":
    asyncio.run(main())
