"""An asyncio client for the mini broker, and MiniBroker: the worker's Broker interface on top
of it, so the same Worker that runs on Postgres runs on the mini broker unchanged.

A message's body is the job as JSON: tenant, task, payload, attempts allowed, timeout and
request id. The broker itself counts attempts and decides between a retry and the dead-letter
queue (max_attempts also travels as a PUSH option); the body's copy is for the handler to read.
"""

import asyncio
import contextlib
import json
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

from hopper.minibroker.protocol import Error, ReplyError, Simple, Value, command, read_reply
from hopper.queue.broker import ClaimedJob, FailureOutcome

DEFAULT_URL = "tcp://127.0.0.1:6390"


def parse_url(url: str) -> tuple[str, int]:
    """tcp://host:port, as MINIBROKER_URL gives it."""
    parts = urlsplit(url)
    if parts.scheme != "tcp" or not parts.hostname:
        raise ValueError(f"expected tcp://host:port, not {url!r}")
    return parts.hostname, parts.port or 6390


class Client:
    """A small pool of connections. A connection that fails is thrown away, and the next call
    opens a new one, so a broker restart costs the calls in flight and nothing else."""

    def __init__(self, url: str = DEFAULT_URL, *, size: int = 4, timeout: float = 10.0) -> None:
        self.host, self.port = parse_url(url)
        self._timeout = timeout
        self._idle: asyncio.Queue[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = (
            asyncio.Queue()
        )
        self._slots = asyncio.Semaphore(size)
        self._open: set[asyncio.StreamWriter] = set()

    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(self.host, self.port), self._timeout
        )
        self._open.add(writer)
        return reader, writer

    async def pipeline(self, commands: Sequence[Sequence[bytes | str | int]]) -> list[Value]:
        """Send several commands on one connection, then read all the replies, in order."""
        async with self._slots:
            try:
                reader, writer = self._idle.get_nowait()
            except asyncio.QueueEmpty:
                reader, writer = await self._connect()
            try:
                writer.write(b"".join(command(*c) for c in commands))
                await writer.drain()
                replies = [
                    await asyncio.wait_for(read_reply(reader), self._timeout) for _ in commands
                ]
            except BaseException:
                self._open.discard(writer)
                writer.close()
                raise
            self._idle.put_nowait((reader, writer))
        for reply in replies:
            if isinstance(reply, Error):
                raise ReplyError(reply.text)
        return replies

    async def call(self, *args: bytes | str | int) -> Value:
        return (await self.pipeline([args]))[0]

    async def close(self) -> None:
        for writer in list(self._open):
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
        self._open.clear()


class MiniBroker:
    """The worker's Broker on the mini broker. Ids and lease tokens are UUIDs on both
    brokers, so ClaimedJob is the same."""

    def __init__(self, client: Client) -> None:
        self._client = client

    async def push(
        self,
        *,
        queue: str,
        task: str,
        payload: dict[str, Any],
        tenant_id: UUID,
        max_attempts: int = 5,
        timeout_seconds: int = 30,
        delay_seconds: float = 0.0,
        request_id: str | None = None,
    ) -> UUID:
        """Enqueue a job (the API's part, for benchmarks and the chaos test)."""
        body = json.dumps(
            {
                "tenant_id": str(tenant_id),
                "task": task,
                "payload": payload,
                "max_attempts": max_attempts,
                "timeout_seconds": timeout_seconds,
                "request_id": request_id,
            },
            separators=(",", ":"),
        )
        reply = await self._client.call(
            "PUSH",
            queue,
            body,
            "DELAY",
            int(delay_seconds * 1000),
            "MAXATTEMPTS",
            max_attempts,
        )
        assert isinstance(reply, bytes)
        return UUID(reply.decode())

    async def claim(
        self, queue: str, worker_id: str, limit: int, lease_seconds: float
    ) -> list[ClaimedJob]:
        if limit <= 0:
            return []
        reply = await self._client.call("PULL", queue, int(lease_seconds * 1000), "COUNT", limit)
        assert isinstance(reply, list)
        jobs = []
        for item in reply:
            assert isinstance(item, list)
            mid, token, attempt, wait_ms, raw = item
            assert isinstance(mid, bytes) and isinstance(token, bytes) and isinstance(raw, bytes)
            assert isinstance(attempt, int) and isinstance(wait_ms, int)
            body = json.loads(raw)
            jobs.append(
                ClaimedJob(
                    id=UUID(mid.decode()),
                    tenant_id=UUID(body["tenant_id"]),
                    queue=queue,
                    task=body["task"],
                    payload=body["payload"],
                    attempt=attempt,
                    max_attempts=body["max_attempts"],
                    timeout_seconds=body["timeout_seconds"],
                    lease_token=UUID(token.decode()),
                    request_id=body.get("request_id"),
                    wait_seconds=wait_ms / 1000,
                )
            )
        return jobs

    async def heartbeat(self, jobs: Sequence[ClaimedJob], lease_seconds: float) -> set[UUID]:
        if not jobs:
            return set()
        lease = int(lease_seconds * 1000)
        replies = await self._client.pipeline(
            [("EXTEND", str(j.id), str(j.lease_token), lease) for j in jobs]
        )
        return {j.id for j, held in zip(jobs, replies, strict=True) if held == 1}

    async def ack(self, job: ClaimedJob, worker_id: str, result: dict[str, Any] | None) -> bool:
        # The result is not kept: the mini broker forgets a message once it is acked.
        return await self._client.call("ACK", str(job.id), str(job.lease_token)) == 1

    async def nack(
        self,
        job: ClaimedJob,
        worker_id: str,
        *,
        error: str,
        outcome: FailureOutcome,
        delay_seconds: float,
        permanent: bool,
    ) -> str | None:
        args: list[bytes | str | int] = [
            "NACK",
            str(job.id),
            str(job.lease_token),
            int(delay_seconds * 1000),
        ]
        if permanent:
            args.append("DEAD")
        reply = await self._client.call(*args)
        return reply.text if isinstance(reply, Simple) else None

    async def release(self, jobs: Sequence[ClaimedJob], worker_id: str) -> set[UUID]:
        if not jobs:
            return set()
        replies = await self._client.pipeline(
            [("RELEASE", str(j.id), str(j.lease_token)) for j in jobs]
        )
        return {j.id for j, released in zip(jobs, replies, strict=True) if released == 1}
