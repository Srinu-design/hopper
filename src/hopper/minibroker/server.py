"""The mini broker's TCP server: RESP commands in, replies out, the log written before each reply.

Commands (case does not matter):

    PING                                   +PONG
    PUSH queue body [DELAY ms] [MAXATTEMPTS n]
                                           the new message id
    PULL queue lease_ms [COUNT n]          up to n leased messages, each [id, token, attempt,
                                           wait_ms, body]; an empty array when none is ready
    ACK id token                           1, or 0 if the lease was lost
    NACK id token delay_ms [DEAD]          +queued or +dead, or nil if the lease was lost
    EXTEND id token lease_ms               1, or 0 if the lease was lost
    RELEASE id token                       1 (back to the queue, attempt refunded), or 0
    DEAD queue [COUNT n]                   dead messages, each [id, attempts, body]
    REPLAY id                              1 if a dead message is queued again, else 0
    STATS                                  name, value, name, value, ...

One asyncio task per connection runs its commands in order, so pipelined replies come back in
order. A command that changed something (PUSH, ACK, NACK, REPLAY) does not reply until its log
record is written, and with fsync=always, fsynced. With fsync=always, everything that arrived
while one fsync ran shares the next one (group commit): one fsync can cover hundreds of
requests, which is what makes `always` usable at all.

There is no authentication: like Redis without a password, it must only listen on a private
network (Compose's, or localhost).
"""

import asyncio
import contextlib
from collections.abc import Callable
from dataclasses import dataclass

import structlog

from hopper.minibroker.log import FsyncPolicy, Log
from hopper.minibroker.protocol import (
    Error,
    ProtocolError,
    Simple,
    Value,
    encode,
    read_command,
)
from hopper.minibroker.store import Store

log = structlog.get_logger()

MAX_LEASE_MS = 24 * 3600 * 1000
MAX_COUNT = 1000
MAX_QUEUE_NAME = 64


class CommandError(Exception):
    """The command is wrong (unknown, bad arguments); the reply is -ERR and the connection
    stays open."""


def _int(raw: bytes, name: str, low: int, high: int) -> int:
    try:
        value = int(raw)
    except ValueError:
        raise CommandError(f"{name} must be an integer") from None
    if not low <= value <= high:
        raise CommandError(f"{name} must be between {low} and {high}")
    return value


def _str(raw: bytes) -> str:
    try:
        return raw.decode()
    except UnicodeDecodeError:
        raise CommandError("ids, tokens and queue names are UTF-8 text") from None


def _queue(raw: bytes) -> str:
    name = _str(raw)
    if not 0 < len(name) <= MAX_QUEUE_NAME:
        raise CommandError(f"a queue name has 1 to {MAX_QUEUE_NAME} characters")
    return name


def _options(args: list[bytes], allowed: dict[bytes, bool]) -> dict[bytes, bytes | None]:
    """Trailing keyword options, such as DELAY 500 or DEAD. allowed maps an option to
    whether it takes a value."""
    found: dict[bytes, bytes | None] = {}
    i = 0
    while i < len(args):
        name = args[i].upper()
        if name not in allowed:
            raise CommandError(f"unknown option {args[i][:20]!r}")
        if allowed[name]:
            if i + 1 >= len(args):
                raise CommandError(f"{name.decode()} needs a value")
            found[name], i = args[i + 1], i + 2
        else:
            found[name], i = None, i + 1
    return found


@dataclass
class Counters:
    connections: int = 0
    commands: int = 0
    fsyncs: int = 0
    compactions: int = 0


class BrokerServer:
    def __init__(
        self,
        store: Store,
        log_file: Log,
        fsync: FsyncPolicy,
        *,
        expire_interval: float = 0.1,
        compact_min_bytes: int = 64 << 20,
    ) -> None:
        self.store = store
        self.log_file = log_file
        self.fsync = fsync
        self.counters = Counters()
        self._expire_interval = expire_interval
        self._compact_min_bytes = compact_min_bytes
        # Held while a thread fsyncs the log file and while compaction replaces it.
        self._disk = asyncio.Lock()
        self._waiters: list[asyncio.Future[None]] = []
        self._committing = False
        self._commit_task: asyncio.Task[None] | None = None
        self._server: asyncio.Server | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._connections: set[asyncio.Task[None]] = set()
        self._handlers: dict[bytes, Callable[[list[bytes]], Value]] = {
            b"PING": self._ping,
            b"PUSH": self._push,
            b"PULL": self._pull,
            b"ACK": self._ack,
            b"NACK": self._nack,
            b"EXTEND": self._extend,
            b"RELEASE": self._release,
            b"DEAD": self._dead,
            b"REPLAY": self._replay,
            b"STATS": self._stats,
        }

    # -- lifecycle --------------------------------------------------------------------------

    async def start(self, host: str, port: int) -> int:
        """Listen, and start the background loops. Returns the port (useful with port 0)."""
        self._server = await asyncio.start_server(self._connection, host, port)
        self._tasks = [asyncio.create_task(self._expire_loop(), name="expire")]
        if self.fsync == "everysec":
            self._tasks.append(asyncio.create_task(self._fsync_loop(), name="fsync"))
        self._tasks.append(asyncio.create_task(self._compact_loop(), name="compact"))
        sock = self._server.sockets[0]
        return int(sock.getsockname()[1])

    async def stop(self) -> None:
        """Stop accepting, finish, and leave everything on disk."""
        if self._server is not None:
            self._server.close()
        for task in [*self._tasks, *self._connections]:
            task.cancel()
        for task in [*self._tasks, *self._connections]:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        async with self._disk:
            self.log_file.close()

    # -- durability -------------------------------------------------------------------------

    async def _durable(self) -> None:
        """Return once this command's log records are as durable as the policy promises."""
        if not self.log_file.pending:
            return
        if self.fsync != "always":
            self.log_file.write()
            return
        waiter = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        if not self._committing:
            self._committing = True
            self._commit_task = asyncio.create_task(self._group_commit())
        await waiter

    async def _group_commit(self) -> None:
        try:
            while self._waiters:
                waiters, self._waiters = self._waiters, []
                try:
                    async with self._disk:
                        self.log_file.write()
                        await asyncio.to_thread(self.log_file.fsync)
                except Exception as exc:
                    # The disk refused: nothing written since the last fsync is known to be
                    # safe. Fail every waiter; their clients get an error, not a false ack.
                    log.exception("broker_commit_failed")
                    for w in waiters:
                        if not w.done():
                            w.set_exception(exc)
                    continue
                self.counters.fsyncs += 1
                for w in waiters:
                    if not w.done():
                        w.set_result(None)
        finally:
            self._committing = False

    async def _fsync_loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            if self.log_file.pending:
                self.log_file.write()
            if self.log_file.unsynced:
                async with self._disk:
                    await asyncio.to_thread(self.log_file.fsync)
                self.counters.fsyncs += 1

    async def _expire_loop(self) -> None:
        while True:
            await asyncio.sleep(self._expire_interval)
            for m, status in self.store.expire():
                log.info("broker_lease_expired", id=m.id, queue=m.queue, status=status)
            if self.log_file.pending:
                self.log_file.write()  # the fsync policy covers it from here

    async def _compact_loop(self) -> None:
        while True:
            await asyncio.sleep(1.0)
            size = self.log_file.size
            if size < self._compact_min_bytes or size < 4 * self.store.live_bytes:
                continue
            async with self._disk:
                # Runs on the event loop: nothing else changes the store meanwhile.
                self.log_file.rewrite(self.store.snapshot())
            self.counters.compactions += 1
            log.info("broker_log_compacted", before=size, after=self.log_file.size)

    # -- connections ------------------------------------------------------------------------

    async def _connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        assert task is not None
        self._connections.add(task)
        self.counters.connections += 1
        try:
            while True:
                try:
                    args = await read_command(reader)
                except ProtocolError as exc:
                    writer.write(encode(Error(f"protocol error: {exc}")))
                    break
                if args is None:
                    break
                if not args:
                    continue
                writer.write(await self.execute(args))
                await writer.drain()  # returns at once unless the client is slow to read
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            self._connections.discard(task)
            writer.close()
            with contextlib.suppress(ConnectionError, asyncio.CancelledError):
                await writer.wait_closed()

    async def execute(self, args: list[bytes]) -> bytes:
        """One command, as its encoded reply."""
        self.counters.commands += 1
        handler = self._handlers.get(args[0].upper())
        if handler is None:
            return encode(Error(f"unknown command {args[0][:20]!r}"))
        try:
            value = handler(args[1:])
        except CommandError as exc:
            return encode(Error(str(exc)))
        except Exception:
            log.exception("broker_command_failed", command=args[0][:20].decode(errors="replace"))
            return encode(Error("internal error"))
        try:
            await self._durable()
        except Exception as exc:
            return encode(Error(f"not durable: {type(exc).__name__}: {exc}"))
        return encode(value)

    # -- commands ---------------------------------------------------------------------------

    @staticmethod
    def _arity(args: list[bytes], low: int, high: int | None = None) -> None:
        if len(args) < low or (high is not None and len(args) > high):
            raise CommandError("wrong number of arguments")

    def _ping(self, args: list[bytes]) -> Value:
        self._arity(args, 0, 1)
        return args[0] if args else Simple("PONG")

    def _push(self, args: list[bytes]) -> Value:
        self._arity(args, 2)
        opts = _options(args[2:], {b"DELAY": True, b"MAXATTEMPTS": True})
        delay = opts.get(b"DELAY")
        attempts = opts.get(b"MAXATTEMPTS")
        return self.store.push(
            _queue(args[0]),
            args[1],
            delay_ms=_int(delay, "DELAY", 0, 30 * 24 * 3600 * 1000) if delay else 0,
            max_attempts=_int(attempts, "MAXATTEMPTS", 1, 1000) if attempts else 5,
        )

    def _pull(self, args: list[bytes]) -> Value:
        self._arity(args, 2)
        opts = _options(args[2:], {b"COUNT": True})
        count = opts.get(b"COUNT")
        pulled = self.store.pull(
            _queue(args[0]),
            _int(args[1], "lease_ms", 1, MAX_LEASE_MS),
            _int(count, "COUNT", 1, MAX_COUNT) if count else 1,
        )
        return [
            [p.message.id, p.token, p.message.attempts, p.wait_ms, p.message.body] for p in pulled
        ]

    def _ack(self, args: list[bytes]) -> Value:
        self._arity(args, 2, 2)
        return int(self.store.ack(_str(args[0]), _str(args[1])))

    def _nack(self, args: list[bytes]) -> Value:
        self._arity(args, 3)
        opts = _options(args[3:], {b"DEAD": False})
        status = self.store.nack(
            _str(args[0]),
            _str(args[1]),
            _int(args[2], "delay_ms", 0, 30 * 24 * 3600 * 1000),
            dead=b"DEAD" in opts,
        )
        return None if status is None else Simple(status)

    def _extend(self, args: list[bytes]) -> Value:
        self._arity(args, 3, 3)
        lease = _int(args[2], "lease_ms", 1, MAX_LEASE_MS)
        return int(self.store.extend(_str(args[0]), _str(args[1]), lease))

    def _release(self, args: list[bytes]) -> Value:
        self._arity(args, 2, 2)
        return int(self.store.release(_str(args[0]), _str(args[1])))

    def _dead(self, args: list[bytes]) -> Value:
        self._arity(args, 1)
        opts = _options(args[1:], {b"COUNT": True})
        count = opts.get(b"COUNT")
        found = self.store.dead(
            _queue(args[0]), _int(count, "COUNT", 1, MAX_COUNT) if count else 100
        )
        return [[m.id, m.attempts, m.body] for m in found]

    def _replay(self, args: list[bytes]) -> Value:
        self._arity(args, 1, 1)
        return int(self.store.replay(_str(args[0])))

    def _stats(self, args: list[bytes]) -> Value:
        self._arity(args, 0, 0)
        stats = {
            **self.store.stats(),
            "log_bytes": self.log_file.size,
            "connections": len(self._connections),
            "commands": self.counters.commands,
            "fsyncs": self.counters.fsyncs,
            "compactions": self.counters.compactions,
        }
        flat: list[Value] = []
        for name, value in stats.items():
            flat += [name, value]
        return flat
