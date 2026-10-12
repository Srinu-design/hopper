"""The http task against a real HTTP server on this machine."""

import hashlib
import hmac
import json
import threading
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue.postgres import PostgresBroker
from hopper.tasks import http
from tests.helpers import (
    LEASE,
    TenantCreds,
    attempts_of,
    insert_job,
    job_row,
    make_tenant,
    make_worker,
)


@dataclass
class Seen:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes


class Target:
    """A tiny server whose paths each answer one way."""

    def __init__(self) -> None:
        self.seen: list[Seen] = []
        target = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args: Any) -> None:
                pass

            def do_GET(self) -> None:
                self._answer()

            def do_POST(self) -> None:
                self._answer()

            def _answer(self) -> None:
                length = int(self.headers.get("content-length") or 0)
                body = self.rfile.read(length) if length else b""
                headers = {k.lower(): v for k, v in self.headers.items()}
                target.seen.append(Seen(self.command, self.path, headers, body))
                if self.path == "/ok":
                    self._send(200, b"thanks")
                elif self.path == "/busy":
                    self._send(503, b"busy", {"Retry-After": "120"})
                elif self.path == "/gone":
                    self._send(404, b"no such hook")
                elif self.path == "/moved":
                    self._send(302, b"", {"Location": "http://169.254.169.254/latest/meta-data/"})
                elif self.path == "/huge":
                    self._send(200, b"x" * (1024 * 1024))
                elif self.path == "/slow":
                    time.sleep(2)
                    self._send(200, b"late")

            def _send(
                self, status: int, body: bytes, headers: dict[str, str] | None = None
            ) -> None:
                try:
                    self.send_response(status)
                    for name, value in (headers or {}).items():
                        self.send_header(name, value)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    pass  # the client stopped reading, as it should for /huge

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.server.daemon_threads = True
        self.port = self.server.server_address[1]

    def url(self, path: str) -> str:
        return f"http://localhost:{self.port}{path}"


@pytest.fixture
def target() -> Iterator[Target]:
    server = Target()
    thread = threading.Thread(target=server.server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.server.shutdown()
        server.server.server_close()


async def loopback_only(host: str, port: int) -> list[str]:
    return ["127.0.0.1"]


@pytest.fixture
async def local_targets_allowed() -> AsyncIterator[None]:
    """Workers may call this machine, as in a local demo. Production refuses it."""
    await http.configure(
        http.HttpSettings(
            allow_private=True, connect_timeout=2, read_timeout=0.5, resolver=loopback_only
        )
    )
    try:
        yield
    finally:
        await http.configure(http.HttpSettings())


@pytest.fixture
async def acme(migrated_engine: AsyncEngine) -> TenantCreds:
    return await make_tenant(migrated_engine, "acme")


async def run_http_job(engine: AsyncEngine, tenant: TenantCreds, **payload: Any) -> dict[str, Any]:
    job_id = await insert_job(
        engine, tenant.id, task="http", payload=json.dumps(payload), timeout_seconds=15
    )
    [job] = await PostgresBroker(engine).claim("default", "w1", 1, LEASE)
    await make_worker(engine).process(job)
    return await job_row(engine, job_id)


@pytest.mark.usefixtures("local_targets_allowed")
async def test_a_successful_call_is_signed_and_carries_dedup_headers(
    migrated_engine: AsyncEngine, acme: TenantCreds, target: Target
) -> None:
    job = await run_http_job(
        migrated_engine,
        acme,
        url=target.url("/ok"),
        body={"order": 42},
        headers={"X-Tenant-Ref": "abc"},
    )

    assert (job["status"], job["result"]) == ("succeeded", {"status_code": 200, "body": "thanks"})
    [request] = target.seen
    assert (request.method, request.path, request.body) == ("POST", "/ok", b'{"order":42}')
    assert request.headers["hopper-job-id"] == str(job["id"])
    assert request.headers["idempotency-key"] == str(job["id"])
    assert request.headers["hopper-attempt"] == "1"
    assert request.headers["content-type"] == "application/json"
    assert request.headers["x-tenant-ref"] == "abc"
    # The receiver's side: recompute the signature with the tenant's secret.
    t, v1 = (part.split("=", 1)[1] for part in request.headers["hopper-signature"].split(","))
    expected = hmac.new(acme.signing_secret, f"{t}.".encode() + request.body, hashlib.sha256)
    assert hmac.compare_digest(v1, expected.hexdigest())
    assert abs(int(t) - time.time()) < 60


@pytest.mark.usefixtures("local_targets_allowed")
async def test_a_5xx_is_retried_no_sooner_than_retry_after(
    migrated_engine: AsyncEngine, acme: TenantCreds, target: Target
) -> None:
    job = await run_http_job(migrated_engine, acme, url=target.url("/busy"))
    assert (job["status"], job["last_error"]) == (
        "queued",
        "RetryableError: target answered HTTP 503",
    )
    [attempt] = await attempts_of(migrated_engine, job["id"])
    assert (job["run_at"] - attempt["finished_at"]).total_seconds() >= 120


@pytest.mark.usefixtures("local_targets_allowed")
async def test_a_4xx_goes_to_the_dlq_at_once(
    migrated_engine: AsyncEngine, acme: TenantCreds, target: Target
) -> None:
    job = await run_http_job(migrated_engine, acme, url=target.url("/gone"))
    assert (job["status"], job["attempts"], job["last_error"]) == (
        "dead",
        1,
        "PermanentError: target answered HTTP 404",
    )


@pytest.mark.usefixtures("local_targets_allowed")
async def test_a_redirect_is_not_followed(
    migrated_engine: AsyncEngine, acme: TenantCreds, target: Target
) -> None:
    """A redirect to the metadata endpoint is a classic SSRF bypass; it goes nowhere."""
    job = await run_http_job(migrated_engine, acme, url=target.url("/moved"))
    assert job["status"] == "dead"
    assert "redirects are not followed" in job["last_error"]
    assert [r.path for r in target.seen] == ["/moved"]


@pytest.mark.usefixtures("local_targets_allowed")
async def test_a_huge_response_is_cut_off(
    migrated_engine: AsyncEngine, acme: TenantCreds, target: Target
) -> None:
    job = await run_http_job(migrated_engine, acme, url=target.url("/huge"), method="GET")
    assert job["status"] == "succeeded"
    assert job["result"]["body"] == "x" * http.RESULT_BODY_CHARS


@pytest.mark.usefixtures("local_targets_allowed")
async def test_a_read_timeout_is_retried(
    migrated_engine: AsyncEngine, acme: TenantCreds, target: Target
) -> None:
    job = await run_http_job(migrated_engine, acme, url=target.url("/slow"))
    assert (job["status"], job["last_error"]) == (
        "queued",
        "RetryableError: ReadTimeout calling the target",
    )


@pytest.mark.usefixtures("local_targets_allowed")
async def test_a_refused_connection_is_retried(
    migrated_engine: AsyncEngine, acme: TenantCreds, target: Target
) -> None:
    closed_port = Target()  # bound, then closed straight away: nothing listens there
    closed_port.server.server_close()
    job = await run_http_job(migrated_engine, acme, url=closed_port.url("/ok"))
    assert job["status"] == "queued"
    assert job["last_error"].startswith("RetryableError: ConnectError")


async def test_the_real_client_refuses_this_machine_by_default(
    migrated_engine: AsyncEngine, acme: TenantCreds, target: Target
) -> None:
    """The guard is on the production client's path: localhost resolves, is refused, and the
    server never sees a request. This also catches an httpx upgrade that bypassed the guard."""
    await http.configure(http.HttpSettings())  # production settings, system resolver
    job = await run_http_job(migrated_engine, acme, url=target.url("/ok"))
    assert (job["status"], job["attempts"]) == ("dead", 1)
    assert job["last_error"].startswith("BlockedAddress: refused: localhost resolves to non-public")
    assert target.seen == []


async def test_the_job_id_header_matches_on_a_retry(
    migrated_engine: AsyncEngine, acme: TenantCreds, target: Target
) -> None:
    """Hopper-Attempt goes up across retries; Idempotency-Key stays the job id."""
    await http.configure(
        http.HttpSettings(allow_private=True, read_timeout=0.5, resolver=loopback_only)
    )
    try:
        job = await run_http_job(migrated_engine, acme, url=target.url("/busy"))
        async with migrated_engine.begin() as conn:
            await conn.execute(text("UPDATE jobs SET run_at = now()"))
        [claimed] = await PostgresBroker(migrated_engine).claim("default", "w1", 1, LEASE)
        await make_worker(migrated_engine).process(claimed)
    finally:
        await http.configure(http.HttpSettings())
    first, second = target.seen
    assert (first.headers["hopper-attempt"], second.headers["hopper-attempt"]) == ("1", "2")
    keys = {first.headers["idempotency-key"], second.headers["idempotency-key"]}
    assert keys == {str(job["id"])}
    assert uuid.UUID(first.headers["hopper-job-id"]) == job["id"]


async def allow_internal(engine: AsyncEngine, tenant: TenantCreds, *hosts: str) -> None:
    async with engine.begin() as conn:
        await conn.execute(
            text("UPDATE tenants SET internal_hosts = :h WHERE id = :t"),
            {"h": list(hosts), "t": tenant.id},
        )


@pytest.fixture
async def strict_guard_on_loopback() -> AsyncIterator[None]:
    """Production's guard (no allow_private), with every name resolving to this machine."""
    await http.configure(
        http.HttpSettings(connect_timeout=2, read_timeout=0.5, resolver=loopback_only)
    )
    try:
        yield
    finally:
        await http.configure(http.HttpSettings())


@pytest.mark.usefixtures("strict_guard_on_loopback")
async def test_an_allowed_internal_host_is_reached_by_its_tenant_only(
    migrated_engine: AsyncEngine, acme: TenantCreds, target: Target
) -> None:
    """Sift's case: sift-ingest is a private address on Hopper's network. The tenant a
    platform admin allowed reaches it; another tenant naming the same URL is refused."""
    await allow_internal(migrated_engine, acme, "sift-ingest.test")
    url = f"http://sift-ingest.test:{target.port}/ok"

    allowed = await run_http_job(migrated_engine, acme, url=url)
    stranger = await make_tenant(migrated_engine, "other")
    other = await run_http_job(migrated_engine, stranger, url=url)

    assert allowed["status"] == "succeeded"
    assert (other["status"], other["attempts"]) == ("dead", 1)
    assert other["last_error"].startswith("BlockedAddress: refused")
    assert [r.path for r in target.seen] == ["/ok"]


@pytest.mark.usefixtures("strict_guard_on_loopback")
async def test_the_allowlist_is_by_name_not_by_address(
    migrated_engine: AsyncEngine, acme: TenantCreds, target: Target
) -> None:
    await allow_internal(migrated_engine, acme, "sift-ingest.test")
    job = await run_http_job(migrated_engine, acme, url=target.url("/ok"))  # localhost
    assert job["status"] == "dead"
    assert target.seen == []


@pytest.mark.usefixtures("local_targets_allowed")
async def test_a_job_can_wait_longer_than_the_default_read_timeout(
    migrated_engine: AsyncEngine, acme: TenantCreds, target: Target
) -> None:
    """/slow answers after 2 s; the workers' read timeout here is 0.5 s."""
    job = await run_http_job(migrated_engine, acme, url=target.url("/slow"), timeout=5)
    assert (job["status"], job["result"]["body"]) == ("succeeded", "late")
