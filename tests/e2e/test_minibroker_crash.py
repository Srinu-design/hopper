"""Real processes: kill -9 the mini broker while clients push, and kill -9 a worker on it.

The broker runs as `python -m hopper.minibroker` and the worker as
`python -m hopper.minibroker.worker`, as they would in their containers.
"""

import asyncio
import os
import socket
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

from hopper.minibroker.client import Client, MiniBroker
from hopper.minibroker.protocol import Value
from tests.helpers import ROOT

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals and fsync")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def start_broker(data: Path, port: int, fsync: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen(
        [sys.executable, "-m", "hopper.minibroker", "--port", str(port), "--data", str(data),
         "--fsync", fsync, "--log-level", "WARNING"],
        cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
    )  # fmt: skip


def start_worker(url: str) -> subprocess.Popen[bytes]:
    env = {
        **os.environ,
        "MINIBROKER_URL": url,
        "LEASE_SECONDS": "2",
        "HEARTBEAT_SECONDS": "0.5",
        "WORKER_POLL_INTERVAL": "0.05",
        "WORKER_MAX_IDLE_BACKOFF": "0.1",
        "LOG_LEVEL": "WARNING",
    }
    return subprocess.Popen(
        [sys.executable, "-m", "hopper.minibroker.worker"],
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


async def until_up(url: str) -> Client:
    for _ in range(300):
        client = Client(url, size=8, timeout=2)
        try:
            await client.call("PING")
            return client
        except OSError:
            await client.close()
            await asyncio.sleep(0.05)
    raise AssertionError("the broker did not start")


def stats(reply: Value) -> dict[str, int]:
    assert isinstance(reply, list)
    names, values = reply[::2], reply[1::2]
    return {n.decode(): v for n, v in zip(names, values, strict=True)}


@pytest.mark.parametrize("fsync", ["always", "everysec", "no"])
async def test_no_acknowledged_push_is_lost_when_the_broker_is_killed(
    tmp_path: Path, fsync: str
) -> None:
    """Every PUSH that got its reply is still there after kill -9, in every fsync mode: the
    record reaches the kernel before the reply, and a killed process does not take the
    kernel's page cache with it. (fsync is what protects against the machine going down.)"""
    port = free_port()
    url = f"tcp://127.0.0.1:{port}"
    broker = start_broker(tmp_path, port, fsync)
    acked: list[bytes] = []
    try:
        client = await until_up(url)

        async def pusher(n: int) -> None:
            i = 0
            while True:
                try:
                    acked.append(await client.call("PUSH", "q", b"%d-%d" % (n, i)))
                except (OSError, asyncio.IncompleteReadError):
                    return
                i += 1

        pushers = [asyncio.create_task(pusher(n)) for n in range(8)]
        await asyncio.sleep(1.0)
        broker.kill()  # SIGKILL, mid-stream
        await asyncio.to_thread(broker.wait, 10)
        await asyncio.gather(*pushers)
        await client.close()
    finally:
        if broker.poll() is None:
            broker.kill()
    assert len(acked) > 100

    broker = start_broker(tmp_path, port, fsync)
    try:
        client = await until_up(url)
        found: set[bytes] = set()
        while batch := await client.call("PULL", "q", 60_000, "COUNT", 1000):
            assert isinstance(batch, list)
            found.update(item[0] for item in batch)
        await client.close()
    finally:
        broker.terminate()
        broker.wait(10)
    assert set(acked) <= found  # none lost; a few more may exist (written, reply never sent)
    assert len(found) - len(acked) <= 8  # at most one unanswered PUSH per connection


async def test_kill_9_a_worker_mid_job_and_the_job_comes_back(tmp_path: Path) -> None:
    port = free_port()
    url = f"tcp://127.0.0.1:{port}"
    broker = start_broker(tmp_path, port, "always")
    worker: subprocess.Popen[bytes] | None = None
    try:
        client = await until_up(url)
        job = await MiniBroker(client).push(
            queue="default", task="sleep", payload={"ms": 10_000}, tenant_id=uuid.uuid4()
        )
        worker = start_worker(url)
        for _ in range(600):
            if stats(await client.call("STATS"))["leased"] == 1:
                break
            await asyncio.sleep(0.05)
        await asyncio.sleep(3)  # longer than the 2 s lease: only the heartbeats keep it
        assert stats(await client.call("STATS"))["leased"] == 1
        worker.kill()
        await asyncio.to_thread(worker.wait, 10)

        # Nobody heartbeats now: the lease runs out and the job is ready again, attempt 2.
        for _ in range(100):
            if stats(await client.call("STATS"))["ready"] == 1:
                break
            await asyncio.sleep(0.1)
        [[mid, _token, attempt, _wait, _body]] = await client.call("PULL", "default", 1000)
        assert (mid, attempt) == (str(job).encode(), 2)
        await client.close()
    finally:
        for proc in (worker, broker):
            if proc is not None and proc.poll() is None:
                proc.kill()
                proc.wait(10)
