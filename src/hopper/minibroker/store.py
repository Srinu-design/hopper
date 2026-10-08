"""The broker's memory: messages, the ready heaps, the lease deadlines. No I/O except appending
log records; the server decides when they reach the disk.

A message is ready, leased or dead:

- ready: in its queue's heap, ordered by ready time (a delayed or retrying message has a ready
  time in the future and is skipped until then);
- leased: pulled by a worker with a fresh token and a deadline; every ACK, NACK, EXTEND and
  RELEASE must name that token, so a worker whose lease ran out changes nothing (fencing, as in
  the Postgres broker);
- dead: out of attempts or failed for good; kept until it is replayed.

Heaps never delete from the middle. A heap entry carries what was true when it was pushed
(ready time, or deadline and token), and an entry that no longer matches the message is simply
skipped when it reaches the top. Each operation is O(log n).

Leases are not logged: a PULL costs no disk write. After a restart every message that was
leased is ready again, which is at-least-once delivery, the same promise the broker makes for a
crashed worker.
"""

import heapq
import itertools
import time
import uuid
from collections.abc import Callable, Iterator
from dataclasses import dataclass

from hopper.minibroker.log import Log
from hopper.minibroker.protocol import command


def wall_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(slots=True)
class Message:
    id: str
    queue: str
    body: bytes
    max_attempts: int
    attempts: int
    ready_at: int  # ms since the epoch, on the broker's clock
    token: str | None = None  # set while leased
    deadline: int = 0  # lease end while leased
    dead: bool = False


@dataclass(frozen=True, slots=True)
class Pulled:
    message: Message
    token: str
    wait_ms: int  # ready time to pull


class Store:
    def __init__(self, log: Log | None = None, clock: Callable[[], int] = wall_ms) -> None:
        self._log = log
        self._clock = clock
        self.messages: dict[str, Message] = {}
        self._ready: dict[str, list[tuple[int, int, str]]] = {}  # queue -> (ready_at, seq, id)
        self._leases: list[tuple[int, int, str, str]] = []  # (deadline, seq, id, token)
        self._seq = itertools.count()
        self.live_bytes = 0  # what a compacted log would take, about

    # -- logging ----------------------------------------------------------------------------

    def attach(self, log: Log) -> None:
        """Log every change from now on (after load() has rebuilt memory from the log)."""
        self._log = log

    def _record(self, *args: bytes | str | int) -> None:
        if self._log is not None:
            self._log.append(command(*args))

    @staticmethod
    def _size(m: Message) -> int:
        return len(m.body) + len(m.id) + len(m.queue) + 64

    def _enqueue(self, m: Message) -> None:
        heapq.heappush(self._ready.setdefault(m.queue, []), (m.ready_at, next(self._seq), m.id))

    # -- commands ---------------------------------------------------------------------------

    def push(self, queue: str, body: bytes, *, max_attempts: int = 5, delay_ms: int = 0) -> str:
        m = Message(
            str(uuid.uuid4()), queue, body, max_attempts, 0, self._clock() + max(0, delay_ms)
        )
        self.messages[m.id] = m
        self._enqueue(m)
        self.live_bytes += self._size(m)
        self._record("PUSH", m.id, m.queue, m.ready_at, m.max_attempts, m.attempts, m.body)
        return m.id

    def pull(self, queue: str, lease_ms: int, count: int = 1) -> list[Pulled]:
        """Lease up to `count` ready messages of `queue`, oldest ready time first."""
        now = self._clock()
        heap = self._ready.get(queue)
        out: list[Pulled] = []
        while heap and len(out) < count and heap[0][0] <= now:
            ready_at, _, mid = heapq.heappop(heap)
            m = self.messages.get(mid)
            if m is None or m.token is not None or m.dead or m.ready_at != ready_at:
                continue  # acked, leased or moved since this entry was pushed
            m.attempts += 1
            m.token = str(uuid.uuid4())
            m.deadline = now + lease_ms
            heapq.heappush(self._leases, (m.deadline, next(self._seq), m.id, m.token))
            out.append(Pulled(m, m.token, now - ready_at))
        return out

    def _leased(self, mid: str, token: str) -> Message | None:
        m = self.messages.get(mid)
        return m if m is not None and m.token is not None and m.token == token else None

    def ack(self, mid: str, token: str) -> bool:
        m = self._leased(mid, token)
        if m is None:
            return False
        del self.messages[mid]
        self.live_bytes -= self._size(m)
        self._record("ACK", mid)
        return True

    def nack(self, mid: str, token: str, delay_ms: int, *, dead: bool = False) -> str | None:
        """A failed run: 'queued' to retry after delay_ms, 'dead' when it failed for good or has
        no attempts left, None when the lease was lost and nothing changed."""
        m = self._leased(mid, token)
        if m is None:
            return None
        if dead or m.attempts >= m.max_attempts:
            self._kill(m)
            return "dead"
        self._requeue(m, self._clock() + max(0, delay_ms))
        return "queued"

    def extend(self, mid: str, token: str, lease_ms: int) -> bool:
        m = self._leased(mid, token)
        if m is None:
            return False
        m.deadline = self._clock() + lease_ms
        heapq.heappush(self._leases, (m.deadline, next(self._seq), m.id, token))
        return True

    def release(self, mid: str, token: str) -> bool:
        """Back to the queue now, with the attempt refunded (a worker shutting down)."""
        m = self._leased(mid, token)
        if m is None:
            return False
        m.attempts -= 1
        m.token = None
        m.ready_at = self._clock()
        self._enqueue(m)
        return True

    def replay(self, mid: str) -> bool:
        """A dead message back to the queue now, with all its attempts again."""
        m = self.messages.get(mid)
        if m is None or not m.dead:
            return False
        m.dead = False
        m.attempts = 0
        self._requeue(m, self._clock())
        return True

    def expire(self) -> list[tuple[Message, str]]:
        """Leases whose deadline passed: back to the queue, or dead if that was the last
        attempt. Like the Postgres reaper, an expired run counts as an attempt."""
        now = self._clock()
        out: list[tuple[Message, str]] = []
        while self._leases and self._leases[0][0] <= now:
            deadline, _, mid, token = heapq.heappop(self._leases)
            m = self.messages.get(mid)
            if m is None or m.token != token or m.deadline != deadline:
                continue  # acked, nacked or extended since
            if m.attempts >= m.max_attempts:
                self._kill(m)
                out.append((m, "dead"))
            else:
                self._requeue(m, now)
                out.append((m, "queued"))
        return out

    def _requeue(self, m: Message, ready_at: int) -> None:
        m.token = None
        m.ready_at = ready_at
        self._enqueue(m)
        self._record("NACK", m.id, m.ready_at, m.attempts)

    def _kill(self, m: Message) -> None:
        m.token = None
        m.dead = True
        self._record("DEAD", m.id, m.attempts)

    def dead(self, queue: str, count: int) -> list[Message]:
        found = [m for m in self.messages.values() if m.dead and m.queue == queue]
        return found[:count]

    def stats(self) -> dict[str, int]:
        now = self._clock()
        ready = delayed = leased = dead = 0
        for m in self.messages.values():
            if m.dead:
                dead += 1
            elif m.token is not None:
                leased += 1
            elif m.ready_at <= now:
                ready += 1
            else:
                delayed += 1
        return {
            "messages": len(self.messages),
            "ready": ready,
            "delayed": delayed,
            "leased": leased,
            "dead": dead,
        }

    # -- restart and compaction -------------------------------------------------------------

    def load(self, records: list[list[bytes]]) -> None:
        """Rebuild memory from log records, without logging them again."""
        log, self._log = self._log, None
        try:
            for record in records:
                op, args = record[0], record[1:]
                if op == b"PUSH":
                    mid, queue, ready_at, max_attempts, attempts, body = args
                    m = Message(
                        mid.decode(),
                        queue.decode(),
                        body,
                        int(max_attempts),
                        int(attempts),
                        int(ready_at),
                    )
                    self.messages[m.id] = m
                    self.live_bytes += self._size(m)
                elif op == b"ACK":
                    gone = self.messages.pop(args[0].decode(), None)
                    if gone is not None:
                        self.live_bytes -= self._size(gone)
                elif op == b"NACK":
                    m = self.messages[args[0].decode()]
                    m.ready_at, m.attempts, m.dead = int(args[1]), int(args[2]), False
                elif op == b"DEAD":
                    m = self.messages[args[0].decode()]
                    m.attempts, m.dead = int(args[1]), True
                else:
                    raise ValueError(f"unknown log record {op!r}")
            for m in self.messages.values():
                if not m.dead:
                    self._enqueue(m)
        finally:
            self._log = log

    def snapshot(self) -> Iterator[bytes]:
        """Log records that rebuild exactly the live messages: what compaction writes. A leased
        message is written as ready, as it would be after a restart."""
        for m in self.messages.values():
            ready_at = m.ready_at if m.token is None else self._clock()
            yield command("PUSH", m.id, m.queue, ready_at, m.max_attempts, m.attempts, m.body)
            if m.dead:
                yield command("DEAD", m.id, m.attempts)
