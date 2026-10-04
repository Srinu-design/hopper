"""Week 5 end to end: bootstrap CLI -> admin login -> tenant and owner -> API key ->
an http job, signed with the tenant's secret, delivered by a real worker loop."""

import asyncio
import base64
import hashlib
import hmac
import os
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from hopper.queue.postgres import PostgresBroker
from hopper.tasks import http
from hopper.worker.loop import Worker
from tests.helpers import ROOT

received: list[tuple[dict[str, str], bytes]] = []


class Hook(BaseHTTPRequestHandler):
    def log_message(self, *args: Any) -> None:
        pass

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers["content-length"]))
        received.append(({k.lower(): v for k, v in self.headers.items()}, body))
        self.send_response(204)
        self.end_headers()


@pytest.fixture
def hook() -> Iterator[int]:
    received.clear()
    server = ThreadingHTTPServer(("127.0.0.1", 0), Hook)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()


def bootstrap(db_url: str, email: str) -> subprocess.CompletedProcess[str]:
    """The real CLI, exactly as `docker compose run --rm api python -m hopper.bootstrap`."""
    env = {**os.environ, "DATABASE_URL": db_url, "HOPPER_ADMIN_PASSWORD": "bootstrap-password"}
    return subprocess.run(
        [sys.executable, "-m", "hopper.bootstrap", "--email", email],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


async def loopback_only(host: str, port: int) -> list[str]:
    return ["127.0.0.1"]


async def test_from_bootstrap_to_a_signed_webhook(
    anon_client: httpx.AsyncClient, api_app: FastAPI, migrated_db_url: str, hook: int
) -> None:
    made = await asyncio.to_thread(bootstrap, migrated_db_url, "Root@Hopper.test")
    assert made.returncode == 0, made.stderr
    assert "platform admin root@hopper.test created" in made.stdout
    again = await asyncio.to_thread(bootstrap, migrated_db_url, "root@hopper.test")
    assert again.returncode == 1 and "already exists" in again.stderr

    async def token(email: str, password: str) -> dict[str, str]:
        resp = await anon_client.post("/auth/token", json={"email": email, "password": password})
        assert resp.status_code == 200, resp.text
        return {"Authorization": f"Bearer {resp.json()['access_token']}"}

    platform = await token("root@hopper.test", "bootstrap-password")
    tenant = (
        await anon_client.post(
            "/admin/tenants",
            headers=platform,
            json={
                "name": "acme",
                "owner": {"email": "ops@acme.test", "password": "ops-password-1"},
            },
        )
    ).json()
    signing_secret = base64.urlsafe_b64decode(tenant["signing_secret"])
    owner = await token("ops@acme.test", "ops-password-1")
    key = (await anon_client.post("/admin/api-keys", headers=owner, json={"name": "app"})).json()
    as_tenant = {"Authorization": f"Bearer {key['key']}"}

    job = await anon_client.post(
        "/v1/jobs",
        headers=as_tenant,
        json={
            "task": "http",
            "payload": {"url": f"http://localhost:{hook}/invoices", "body": {"invoice": 7}},
        },
    )
    assert job.status_code == 201, job.text
    job_id = job.json()["id"]

    await http.configure(http.HttpSettings(allow_private=True, resolver=loopback_only))
    worker = Worker(
        PostgresBroker(api_app.state.engine),
        worker_id="e2e",
        queues=["default"],
        poll_interval=0.02,
        max_idle_backoff=0.1,
    )
    runner = asyncio.create_task(worker.run())
    try:
        deadline = time.monotonic() + 15
        while (await anon_client.get(f"/v1/jobs/{job_id}", headers=as_tenant)).json()[
            "status"
        ] != "succeeded":
            assert time.monotonic() < deadline, "http job did not succeed"
            await asyncio.sleep(0.05)
    finally:
        worker.stop()
        await runner
        await http.configure(http.HttpSettings())

    [(headers, body)] = received
    assert body == b'{"invoice":7}'
    assert headers["hopper-job-id"] == job_id
    t, v1 = (part.split("=", 1)[1] for part in headers["hopper-signature"].split(","))
    expected = hmac.new(signing_secret, f"{t}.".encode() + body, hashlib.sha256).hexdigest()
    assert hmac.compare_digest(v1, expected)  # verifiable with the secret shown at onboarding
    done = (await anon_client.get(f"/v1/jobs/{job_id}", headers=as_tenant)).json()
    assert done["result"] == {"status_code": 204, "body": ""}
