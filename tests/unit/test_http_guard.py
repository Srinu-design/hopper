"""The http task's guard rails, without a network: address checks, payload rules, signing."""

import hashlib
import hmac
import ipaddress
from datetime import UTC, datetime, timedelta
from email.utils import format_datetime
from typing import Any

import httpcore
import pytest
from pydantic import ValidationError

from hopper.tasks.errors import PermanentError
from hopper.tasks.http import (
    BlockedAddress,
    GuardedBackend,
    HttpPayload,
    is_blocked,
    retry_after_seconds,
    sign,
)


@pytest.mark.parametrize(
    "address",
    [
        "169.254.169.254",  # EC2 / cloud metadata endpoint
        "127.0.0.1",
        "10.1.2.3",
        "172.16.0.1",
        "192.168.1.1",
        "100.64.0.1",  # carrier-grade NAT, shared address space
        "0.0.0.0",
        "224.0.0.1",  # multicast: is_global alone would let this through
        "255.255.255.255",
        "::1",
        "fd00::1",  # IPv6 unique local
        "fe80::1",  # IPv6 link-local
        "::ffff:169.254.169.254",  # IPv4-mapped IPv6
        "2002:a9fe:a9fe::1",  # 6to4 wrapping 169.254.169.254
        "ff02::1",  # IPv6 multicast
    ],
)
def test_non_public_addresses_are_blocked(address: str) -> None:
    assert is_blocked(ipaddress.ip_address(address))


@pytest.mark.parametrize("address", ["93.184.215.14", "8.8.8.8", "2606:4700:4700::1111"])
def test_public_addresses_are_allowed(address: str) -> None:
    assert not is_blocked(ipaddress.ip_address(address))


@pytest.mark.parametrize(
    "url",
    [
        "ftp://example.com/file",
        "file:///etc/passwd",
        "gopher://example.com",
        "http:///no-host",
        "https://user:pass@example.com/",  # credentials belong in headers
        "http://169.254.169.254/latest/meta-data/",  # literal blocked IPs fail at enqueue
        "http://127.0.0.1:8080/",
        "http://[::1]/",
        "http://example.com:99999/",
    ],
)
def test_bad_urls_are_rejected_in_the_payload(url: str) -> None:
    with pytest.raises(ValidationError):
        HttpPayload.model_validate({"url": url})


@pytest.mark.parametrize(
    "headers",
    [
        {"Host": "internal.example"},
        {"hopper-signature": "forged"},
        {"Idempotency-Key": "mine"},
        {"Content-Length": "0"},
        {"bad name": "x"},
        {"X-Ok": "line\r\nInjected: yes"},
    ],
)
def test_reserved_or_unsafe_headers_are_rejected(headers: dict[str, str]) -> None:
    with pytest.raises(ValidationError):
        HttpPayload.model_validate({"url": "https://example.com/hook", "headers": headers})


def test_a_good_payload_validates_with_defaults() -> None:
    payload = HttpPayload.model_validate(
        {"url": "https://example.com/hook?x=1", "headers": {"X-Tenant-Ref": "42"}, "body": [1]}
    )
    assert (payload.method, payload.body) == ("POST", [1])


def test_signature_is_hmac_sha256_over_timestamp_and_body() -> None:
    secret, body = b"tenant-secret", b'{"order":42}'
    header = sign(secret, 1_760_000_000, body)
    t, v1 = (part.split("=", 1)[1] for part in header.split(","))
    expected = hmac.new(secret, b"1760000000." + body, hashlib.sha256).hexdigest()
    assert (t, v1) == ("1760000000", expected)
    assert sign(b"other-secret", 1_760_000_000, body) != header


def test_retry_after_parsing() -> None:
    assert retry_after_seconds("120") == 120
    assert retry_after_seconds("-5") == 0
    assert retry_after_seconds("999999") == 3600  # capped at an hour
    assert retry_after_seconds("soon") is None
    assert retry_after_seconds(None) is None
    in_a_minute = format_datetime(datetime.now(UTC) + timedelta(seconds=60), usegmt=True)
    seconds = retry_after_seconds(in_a_minute)
    assert seconds is not None and 55 <= seconds <= 60


class RecordingBackend(httpcore.AsyncNetworkBackend):
    """Stands in for the real network: records what would have been dialled."""

    def __init__(self) -> None:
        self.dialled: list[str] = []

    async def connect_tcp(self, host: str, port: int, *args: Any, **kwargs: Any) -> Any:
        self.dialled.append(host)
        raise httpcore.ConnectError("test backend does not connect")


def resolver_for(answers: list[str]) -> Any:
    async def resolve(host: str, port: int) -> list[str]:
        return answers

    return resolve


async def test_a_name_that_resolves_to_a_metadata_address_is_never_dialled() -> None:
    """DNS rebinding: a harmless-looking name answering with an internal address."""
    inner = RecordingBackend()
    backend = GuardedBackend(resolver=resolver_for(["169.254.169.254"]), inner=inner)
    with pytest.raises(BlockedAddress) as caught:
        await backend.connect_tcp("innocent.example", 80)
    assert isinstance(caught.value, PermanentError)  # the job goes to the DLQ, no retries
    assert inner.dialled == []


async def test_one_private_answer_among_public_ones_is_enough_to_refuse() -> None:
    inner = RecordingBackend()
    backend = GuardedBackend(resolver=resolver_for(["93.184.215.14", "10.0.0.5"]), inner=inner)
    with pytest.raises(BlockedAddress):
        await backend.connect_tcp("mixed.example", 443)
    assert inner.dialled == []


async def test_the_checked_address_is_the_one_dialled() -> None:
    inner = RecordingBackend()
    backend = GuardedBackend(resolver=resolver_for(["93.184.215.14", "8.8.8.8"]), inner=inner)
    with pytest.raises(httpcore.ConnectError):
        await backend.connect_tcp("public.example", 443)
    assert inner.dialled == ["93.184.215.14", "8.8.8.8"]  # IPs, never the name again


async def test_unix_sockets_are_refused() -> None:
    with pytest.raises(BlockedAddress):
        await GuardedBackend().connect_unix_socket("/var/run/docker.sock")
