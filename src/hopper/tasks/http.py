"""The http task: call a tenant's URL with a JSON body, and retry when the call fails.

Guard rails:
- SSRF. Hopper resolves the host itself and checks every address before connecting, then
  connects to an address it checked, so DNS cannot answer differently between the check and
  the connect (DNS rebinding). Private, loopback, link-local (including 169.254.169.254, the
  cloud metadata endpoint), shared, reserved and multicast addresses are refused.
- Only http and https, no credentials in the URL, and redirects are never followed.
- Hopper-Job-Id, Hopper-Attempt and Idempotency-Key: <job id>, so receivers can drop repeats.
- Hopper-Signature: t=<unix time>,v1=<hex HMAC-SHA256(tenant secret, "<t>.<body>")>.
- Connect and read timeouts, and at most 64 KB of the response is read.
"""

import asyncio
import hashlib
import hmac
import ipaddress
import json
import re
import socket
import time
import typing
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, Literal
from urllib.parse import urlsplit

import httpcore
import httpx
from pydantic import Field, field_validator

from hopper.tasks.context import current_job
from hopper.tasks.errors import PermanentError, RetryableError
from hopper.tasks.registry import TaskPayload, task

MAX_RESPONSE_BYTES = 64 * 1024
RESULT_BODY_CHARS = 1024  # what the job result keeps of the response body
MAX_RETRY_AFTER = 3600.0
# Set by Hopper on every request; a payload cannot override them.
RESERVED_HEADERS = frozenset(
    {
        "host",
        "content-length",
        "content-type",
        "transfer-encoding",
        "connection",
        "idempotency-key",
        "hopper-job-id",
        "hopper-attempt",
        "hopper-signature",
    }
)
_HEADER_NAME = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]{1,64}")

IPAddress = ipaddress.IPv4Address | ipaddress.IPv6Address
Resolver = Callable[[str, int], Awaitable[list[str]]]


class BlockedAddress(PermanentError):
    """The target is not on the public internet. Retrying cannot help."""


def is_blocked(ip: IPAddress) -> bool:
    if isinstance(ip, ipaddress.IPv6Address):
        # ::ffff:127.0.0.1 and 2002:7f00:1:: are IPv4 addresses in IPv6 clothing.
        embedded = ip.ipv4_mapped or ip.sixtofour
        if embedded is not None:
            ip = embedded
    # is_global is False for private, loopback, link-local, shared (100.64/10) and reserved
    # ranges, but True for multicast, so multicast is refused explicitly.
    return not ip.is_global or ip.is_multicast


class HttpPayload(TaskPayload):
    url: str = Field(max_length=2048)
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "POST"
    headers: dict[str, str] = Field(default_factory=dict, max_length=20)
    body: Any = None  # any JSON value, sent as the request body

    @field_validator("url")
    @classmethod
    def _check_url(cls, url: str) -> str:
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https"):
            raise ValueError("only http and https URLs")
        if not parts.hostname:
            raise ValueError("the URL needs a host")
        if parts.username is not None or parts.password is not None:
            raise ValueError("no credentials in the URL; send them in headers")
        _ = parts.port  # raises ValueError for a port that is not a number in range
        try:
            literal = ipaddress.ip_address(parts.hostname)
        except ValueError:
            return url  # a name: checked after resolving, at call time
        if is_blocked(literal):
            raise ValueError("the URL points at a non-public address")
        return url

    @field_validator("headers")
    @classmethod
    def _check_headers(cls, headers: dict[str, str]) -> dict[str, str]:
        for name, value in headers.items():
            if not _HEADER_NAME.fullmatch(name):
                raise ValueError(f"invalid header name {name!r}")
            if name.lower() in RESERVED_HEADERS:
                raise ValueError(f"header {name!r} is set by Hopper")
            if len(value) > 1024 or "\r" in value or "\n" in value:
                raise ValueError(f"invalid value for header {name!r}")
        return headers


async def system_resolver(host: str, port: int) -> list[str]:
    infos = await asyncio.get_running_loop().getaddrinfo(host, port, type=socket.SOCK_STREAM)
    return [str(info[4][0]) for info in infos]


class GuardedBackend(httpcore.AsyncNetworkBackend):
    """httpcore network backend that refuses non-public addresses at connect time.

    Resolving here, not before the request, is what defeats DNS rebinding: the address that
    was checked is the address that is dialled. TLS still uses the URL's host name for SNI and
    certificate checks, because httpcore wraps this stream with the original origin.
    """

    def __init__(
        self,
        *,
        allow_private: bool = False,
        resolver: Resolver = system_resolver,
        inner: httpcore.AsyncNetworkBackend | None = None,
    ) -> None:
        self._allow_private = allow_private
        self._resolver = resolver
        self._inner = inner or httpcore.AnyIOBackend()

    async def connect_tcp(
        self,
        host: str,
        port: int,
        timeout: float | None = None,  # noqa: ASYNC109  (httpcore's signature)
        local_address: str | None = None,
        socket_options: typing.Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        async with asyncio.timeout(timeout):
            answers = await self._resolver(host, port)
        addresses = [ipaddress.ip_address(a.split("%")[0]) for a in answers]
        if not addresses:
            raise httpcore.ConnectError(f"{host} did not resolve")
        if not self._allow_private:
            for ip in addresses:
                if is_blocked(ip):
                    # Every answer must be public: one bad answer is enough to refuse.
                    raise BlockedAddress(f"refused: {host} resolves to non-public {ip}")
        last: Exception | None = None
        for ip in addresses:  # every one was checked; try them in the resolver's order
            try:
                return await self._inner.connect_tcp(
                    str(ip),
                    port,
                    timeout=timeout,
                    local_address=local_address,
                    socket_options=socket_options,
                )
            except (httpcore.ConnectError, httpcore.ConnectTimeout, OSError) as exc:
                last = exc
        raise httpcore.ConnectError(f"could not connect to {host}") from last

    async def connect_unix_socket(
        self,
        path: str,
        timeout: float | None = None,  # noqa: ASYNC109  (httpcore's signature)
        socket_options: typing.Iterable[httpcore.SOCKET_OPTION] | None = None,
    ) -> httpcore.AsyncNetworkStream:
        raise BlockedAddress("refused: unix sockets")

    async def sleep(self, seconds: float) -> None:
        await self._inner.sleep(seconds)


@dataclass(frozen=True, slots=True)
class HttpSettings:
    allow_private: bool = False  # tests and local demos only
    connect_timeout: float = 5.0
    read_timeout: float = 10.0
    resolver: Resolver = system_resolver


def build_client(settings: HttpSettings) -> httpx.AsyncClient:
    """One client per worker process. trust_env=False: no proxy from the environment, so the
    connection always goes where the guard says."""
    transport = httpx.AsyncHTTPTransport(trust_env=False)
    # httpx has no public hook for the network backend, so the pool is rebuilt with one.
    # A test sends a real request through this client to 127.0.0.1 and expects a refusal,
    # so an httpx upgrade that bypassed this would fail loudly.
    transport._pool = httpcore.AsyncConnectionPool(
        ssl_context=httpx.create_ssl_context(trust_env=False),
        max_connections=100,
        max_keepalive_connections=20,
        keepalive_expiry=5.0,
        network_backend=GuardedBackend(
            allow_private=settings.allow_private, resolver=settings.resolver
        ),
    )
    timeout = httpx.Timeout(settings.read_timeout, connect=settings.connect_timeout)
    return httpx.AsyncClient(
        transport=transport, timeout=timeout, follow_redirects=False, trust_env=False
    )


class _Http:
    def __init__(self) -> None:
        self.settings = HttpSettings()
        self._client: httpx.AsyncClient | None = None

    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = build_client(self.settings)
        return self._client

    async def configure(self, settings: HttpSettings) -> None:
        await self.aclose()
        self.settings = settings

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None


_http = _Http()
configure = _http.configure
aclose = _http.aclose


def sign(secret: bytes, timestamp: int, body: bytes) -> str:
    """The receiver recomputes this and rejects stale timestamps, which stops replays."""
    mac = hmac.new(secret, str(timestamp).encode() + b"." + body, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={mac}"


def retry_after_seconds(value: str | None) -> float | None:
    """Retry-After as seconds or as an HTTP date, capped at an hour."""
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            when = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=UTC)
        seconds = (when - datetime.now(UTC)).total_seconds()
    return min(max(seconds, 0.0), MAX_RETRY_AFTER)


async def _read_capped(response: httpx.Response, limit: int) -> bytes:
    received = bytearray()
    async for chunk in response.aiter_bytes():
        received += chunk
        if len(received) >= limit:
            break  # closing the response drops the rest unread
    return bytes(received[:limit])


@task("http", payload=HttpPayload, max_attempts=8, timeout=15, backoff_base=10, backoff_cap=3600)
async def http(payload: HttpPayload) -> dict[str, Any] | None:
    job = current_job()
    body = b"" if payload.body is None else json.dumps(payload.body, separators=(",", ":")).encode()
    headers = {
        **payload.headers,
        "Hopper-Job-Id": str(job.job_id),
        "Hopper-Attempt": str(job.attempt),
        "Idempotency-Key": str(job.job_id),
        "Hopper-Signature": sign(job.signing_secret, int(time.time()), body),
    }
    if payload.body is not None:
        headers["Content-Type"] = "application/json"
    try:
        async with _http.client().stream(
            payload.method, payload.url, headers=headers, content=body
        ) as response:
            head = await _read_capped(response, MAX_RESPONSE_BYTES)
    except httpx.TimeoutException as exc:
        raise RetryableError(f"{type(exc).__name__} calling the target") from exc
    except httpx.TransportError as exc:
        raise RetryableError(f"{type(exc).__name__} calling the target: {exc}") from exc

    status = response.status_code
    if 200 <= status < 300:
        return {"status_code": status, "body": head.decode("utf-8", "replace")[:RESULT_BODY_CHARS]}
    # The URL is left out of errors: its query string may hold the tenant's own secrets.
    if status in (408, 429) or status >= 500:
        raise RetryableError(
            f"target answered HTTP {status}",
            retry_after=retry_after_seconds(response.headers.get("retry-after")),
        )
    if 300 <= status < 400:
        raise PermanentError(f"target answered HTTP {status}; redirects are not followed")
    raise PermanentError(f"target answered HTTP {status}")
