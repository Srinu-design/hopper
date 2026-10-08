"""The examples on the page's "How to use it" tab, run exactly as written: each curl block in
bash and the Python one as a script, against a real API server process, with a worker running
the jobs. An example that stops working fails here, not in a reader's terminal."""

import asyncio
import os
import socket
import sys
import time
import uuid
from collections.abc import AsyncIterator
from html.parser import HTMLParser
from pathlib import Path

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from tests.helpers import ROOT, make_tenant, make_worker, status_in, wait_for

PAGE = ROOT / "src/hopper/web/static/index.html"


class _Examples(HTMLParser):
    """The text of every <pre data-run="name">, by name, in page order."""

    def __init__(self) -> None:
        super().__init__()
        self.found: dict[str, str] = {}
        self._name: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "pre":
            self._name = dict(attrs).get("data-run")
            if self._name:
                self.found[self._name] = ""

    def handle_endtag(self, tag: str) -> None:
        if tag == "pre":
            self._name = None

    def handle_data(self, data: str) -> None:
        if self._name:
            self.found[self._name] += data


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


@pytest.fixture
async def server(migrated_db_url: str, tmp_path: Path) -> AsyncIterator[str]:
    """`uvicorn hopper.api.main:app` as its own process, on the test database."""
    port = _free_port()
    env = {
        **os.environ,
        "DATABASE_URL": migrated_db_url,
        "REDIS_NAMESPACE": f"test-{uuid.uuid4().hex[:12]}",
        "LOG_LEVEL": "WARNING",
    }
    log = (tmp_path / "server.log").open("wb")
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "uvicorn",
        "hopper.api.main:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=log,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        async with httpx.AsyncClient() as client:
            deadline = time.monotonic() + 30
            while True:
                try:
                    if (await client.get(f"{url}/readyz")).status_code == 200:
                        break
                except httpx.TransportError:
                    pass
                assert proc.returncode is None, (tmp_path / "server.log").read_text()
                assert time.monotonic() < deadline, "the API server did not start"
                await asyncio.sleep(0.1)
        yield url
    finally:
        proc.terminate()
        await proc.wait()
        log.close()


async def test_every_example_on_the_page_works(
    server: str, migrated_engine: AsyncEngine, tmp_path: Path
) -> None:
    parser = _Examples()
    parser.feed(PAGE.read_text())
    examples = parser.found
    # A new example must be added here too, with the job it needs as $JOB_ID, if any.
    assert list(examples) == [
        "enqueue",
        "status",
        "list",
        "later",
        "cancel",
        "idempotency",
        "fail",
        "dlq",
        "replay",
        "replay-many",
        "schedule",
        "python",
    ]
    tenant = await make_tenant(migrated_engine, "acme")
    worker = make_worker(migrated_engine)
    runner = asyncio.create_task(worker.run())
    try:
        headers = {"Authorization": f"Bearer {tenant.api_key}"}
        async with httpx.AsyncClient(base_url=server, headers=headers) as client:

            async def enqueue(**body: object) -> str:
                response = await client.post("/v1/jobs", json=body)
                assert response.status_code == 201
                job_id: str = response.json()["id"]
                return job_id

            done = await enqueue(task="sleep", payload={"ms": 0})
            waiting = await enqueue(task="sleep", payload={"ms": 0}, delay_seconds=3600)
            dead = await enqueue(task="fail_always", payload={"permanent": True})
        await wait_for(status_in(migrated_engine, uuid.UUID(dead), "dead"))
        job_ids = {"status": done, "cancel": waiting, "replay": dead}

        for name, code in examples.items():
            if name == "python":
                script = tmp_path / "example.py"
                script.write_text(code)
                command = [sys.executable, str(script)]
            else:
                # --fail-with-body: any answer that is not 2xx fails the block, and the test.
                curl = 'curl() { command curl --silent --show-error --fail-with-body "$@"; }\n'
                command = ["bash", "-euo", "pipefail", "-c", curl + code]
            env = {
                **os.environ,
                "HOPPER_URL": server,
                "HOPPER_API_KEY": tenant.api_key,
                "JOB_ID": job_ids.get(name, ""),
            }
            proc = await asyncio.create_subprocess_exec(
                *command,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            out, err = await asyncio.wait_for(proc.communicate(), 60)
            assert proc.returncode == 0, f"{name}:\n{out.decode()}\n{err.decode()}"
            if name == "python":
                assert out.decode().splitlines()[-1].startswith("succeeded"), out.decode()
    finally:
        worker.stop()
        await runner
