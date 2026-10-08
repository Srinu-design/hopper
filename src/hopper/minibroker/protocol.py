"""RESP2, the Redis wire protocol. The mini broker speaks it, so `redis-cli -p 6390` can talk to
it while debugging, and the same parser reads the append-only log (every record is one RESP
array, as in Redis's AOF).

A command is an array of bulk strings: PING is *1\\r\\n$4\\r\\nPING\\r\\n. Replies are simple
strings (+OK), errors (-ERR ...), integers (:1), bulk strings ($3\\r\\nabc, or $-1 for nil)
and arrays of these. A line that does not start with * is an inline command, split on spaces,
which is what telnet or `nc` sends.
"""

import asyncio
from collections.abc import Sequence
from dataclasses import dataclass

# Limits, so a bad client cannot make the broker allocate without end. A Hopper payload is
# at most 256 KB; a PULL reply is an array of arrays, read only by the client.
MAX_BULK = 1 << 20
MAX_ITEMS = 1 << 16
MAX_INLINE = 64 * 1024


class ProtocolError(Exception):
    """The bytes are not valid RESP; the connection cannot go on."""


class ReplyError(Exception):
    """An error reply (-ERR ...) from the broker."""


@dataclass(frozen=True, slots=True)
class Simple:
    """A simple string reply, such as +OK or +queued."""

    text: str


@dataclass(frozen=True, slots=True)
class Error:
    """An error reply: -ERR <text>."""

    text: str


type Value = int | bytes | str | Simple | Error | Sequence[Value] | None


def encode(value: Value) -> bytes:
    """One reply, or one log record, as RESP bytes."""
    if value is None:
        return b"$-1\r\n"
    if isinstance(value, bool):  # bool is an int; never sent, so refuse it loudly
        raise TypeError("RESP has no booleans: send 0 or 1")
    if isinstance(value, int):
        return b":%d\r\n" % value
    if isinstance(value, str):
        value = value.encode()
    if isinstance(value, bytes):
        return b"$%d\r\n%s\r\n" % (len(value), value)
    if isinstance(value, Simple):
        return b"+%s\r\n" % value.text.encode()
    if isinstance(value, Error):
        return b"-ERR %s\r\n" % value.text.encode()
    return b"*%d\r\n" % len(value) + b"".join(encode(item) for item in value)


def command(*args: bytes | str | int) -> bytes:
    """A command (or log record) as an array of bulk strings."""
    parts = [a if isinstance(a, bytes) else str(a).encode() for a in args]
    return b"*%d\r\n" % len(parts) + b"".join(b"$%d\r\n%s\r\n" % (len(p), p) for p in parts)


async def _line(reader: asyncio.StreamReader) -> bytes:
    try:
        line = await reader.readuntil(b"\r\n")
    except asyncio.LimitOverrunError as exc:
        raise ProtocolError("line too long") from exc
    return line[:-2]


def _int(raw: bytes) -> int:
    try:
        return int(raw)
    except ValueError:
        raise ProtocolError(f"not an integer: {raw[:20]!r}") from None


async def _bulk(reader: asyncio.StreamReader, size: int) -> bytes:
    if not 0 <= size <= MAX_BULK:
        raise ProtocolError(f"bulk length {size} out of range")
    data = await reader.readexactly(size + 2)
    if data[-2:] != b"\r\n":
        raise ProtocolError("bulk string not followed by CRLF")
    return data[:-2]


async def read_command(reader: asyncio.StreamReader) -> list[bytes] | None:
    """The next command from a client: its arguments, or None when the client hung up."""
    try:
        line = await _line(reader)
    except asyncio.IncompleteReadError:
        return None
    if not line.startswith(b"*"):
        if len(line) > MAX_INLINE:
            raise ProtocolError("inline command too long")
        return line.split()
    count = _int(line[1:])
    if not 0 <= count <= MAX_ITEMS:
        raise ProtocolError(f"array length {count} out of range")
    args: list[bytes] = []
    for _ in range(count):
        header = await _line(reader)
        if not header.startswith(b"$"):
            raise ProtocolError("a command is an array of bulk strings")
        args.append(await _bulk(reader, _int(header[1:])))
    return args


async def read_reply(reader: asyncio.StreamReader) -> Value:
    """The next reply from the broker. An error reply comes back as an Error value."""
    line = await _line(reader)
    kind, rest = line[:1], line[1:]
    if kind == b"+":
        return Simple(rest.decode())
    if kind == b"-":
        return Error(rest.decode().removeprefix("ERR "))
    if kind == b":":
        return _int(rest)
    if kind == b"$":
        size = _int(rest)
        return None if size == -1 else await _bulk(reader, size)
    if kind == b"*":
        count = _int(rest)
        if count == -1:
            return None
        if not 0 <= count <= MAX_ITEMS:
            raise ProtocolError(f"array length {count} out of range")
        return [await read_reply(reader) for _ in range(count)]
    raise ProtocolError(f"unknown reply type {kind!r}")


def parse_records(data: bytes) -> tuple[list[list[bytes]], int]:
    """Log records from the bytes of a log file: the whole records, and how many bytes they
    take. A process killed mid-write leaves a torn record at the end; it is not returned, and
    the caller truncates the file to the returned length."""
    records: list[list[bytes]] = []
    pos = good = 0
    end = len(data)
    while pos < end:
        record: list[bytes] = []
        try:
            nl = data.index(b"\r\n", pos)
            if data[pos : pos + 1] != b"*":
                break
            count = int(data[pos + 1 : nl])
            pos = nl + 2
            for _ in range(count):
                nl = data.index(b"\r\n", pos)
                if data[pos : pos + 1] != b"$":
                    raise ValueError("not a bulk string")
                size = int(data[pos + 1 : nl])
                start, stop = nl + 2, nl + 2 + size
                if stop + 2 > end or data[stop : stop + 2] != b"\r\n":
                    raise ValueError("torn bulk string")
                record.append(data[start:stop])
                pos = stop + 2
        except ValueError:  # includes a missing CRLF (index) and a bad length (int)
            break
        records.append(record)
        good = pos
    return records, good
