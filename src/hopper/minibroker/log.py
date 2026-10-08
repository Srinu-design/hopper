"""The append-only log: every PUSH, ACK, NACK and death is appended before the broker replies.

On start the log is read back to rebuild the broker's memory. A process killed in the middle of
a write leaves a torn record at the end; it is cut off, which is safe because the client never
got a reply for it. Compaction rewrites the log with only the live messages, so it does not grow
for ever.

When the bytes reach the disk is the fsync policy, as in Redis's appendfsync:

- always:   write and fsync before every reply. Requests that arrive together share one fsync
            (group commit). Survives a process crash, an OS crash and a power cut.
- everysec: write before every reply, fsync once a second. Survives a process crash; an OS
            crash or a power cut can lose up to about a second of acknowledged writes.
- no:       write before every reply, never fsync; the OS flushes when it likes (tens of
            seconds). Survives a process crash; an OS crash can lose more.

In every mode the write() happens before the reply, and bytes handed to the kernel survive the
broker process being killed: fsync only protects against the machine itself going down.
"""

import os
from collections.abc import Iterable
from pathlib import Path
from typing import Literal

from hopper.minibroker.protocol import parse_records

FsyncPolicy = Literal["always", "everysec", "no"]
FSYNC_POLICIES: tuple[FsyncPolicy, ...] = ("always", "everysec", "no")


class Log:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        self.size = os.fstat(self._fd).st_size
        self._pending = bytearray()  # appended, not yet handed to the kernel
        self.unsynced = False  # written, not yet fsynced

    @staticmethod
    def read(path: Path) -> tuple[list[list[bytes]], int]:
        """The records in the log file, and how many torn bytes at its end were cut off."""
        if not path.exists():
            return [], 0
        data = path.read_bytes()
        records, good = parse_records(data)
        torn = len(data) - good
        if torn:
            with path.open("r+b") as f:
                f.truncate(good)
                f.flush()
                os.fsync(f.fileno())
        return records, torn

    def append(self, record: bytes) -> None:
        self._pending += record

    @property
    def pending(self) -> bool:
        return bool(self._pending)

    def write(self) -> None:
        """Hand everything appended so far to the kernel."""
        data = bytes(self._pending)
        self._pending.clear()
        view = memoryview(data)
        while view:
            written = os.write(self._fd, view)
            view = view[written:]
        self.size += len(data)
        self.unsynced = True

    def fsync(self) -> None:
        """Make everything written so far durable. Safe to run in a thread while the event loop
        appends: it only touches the file descriptor."""
        self.unsynced = False
        os.fsync(self._fd)

    def rewrite(self, records: Iterable[bytes]) -> None:
        """Compaction: replace the log with `records`, atomically. The new file is complete and
        fsynced before it takes the old one's name, so a crash leaves one or the other."""
        self.write()
        tmp = self.path.with_suffix(".rewrite")
        size = 0
        with tmp.open("wb") as f:
            for record in records:
                f.write(record)
                size += len(record)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
        directory = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)  # the rename itself must survive a crash
        finally:
            os.close(directory)
        os.close(self._fd)
        self._fd = os.open(self.path, os.O_WRONLY | os.O_APPEND)
        self.size = size
        self.unsynced = False

    def close(self) -> None:
        if self._pending:
            self.write()
        os.fsync(self._fd)
        os.close(self._fd)
