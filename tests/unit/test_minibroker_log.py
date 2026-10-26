"""The log file when the disk refuses a write: nothing torn, nothing lost, order kept."""

import os
from pathlib import Path

import pytest

from hopper.minibroker import log as log_module
from hopper.minibroker.log import Log
from hopper.minibroker.protocol import command


def test_a_failed_write_leaves_no_torn_record_and_retries_in_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "broker.log"
    log = Log(path)
    log.append(command("PUSH", "a"))
    log.write()

    real_write = os.write

    def half_then_full(fd: int, data: memoryview) -> int:
        real_write(fd, data[: len(data) // 2])  # the disk takes half a record, then fills up
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(log_module.os, "write", half_then_full)
    log.append(command("PUSH", "b"))
    with pytest.raises(OSError):
        log.write()
    assert path.stat().st_size == log.size  # the half record was cut off again

    monkeypatch.setattr(log_module.os, "write", real_write)
    log.append(command("PUSH", "c"))
    log.write()  # retries b, then c
    log.close()
    records, torn = Log.read(path)
    assert (records, torn) == ([[b"PUSH", b"a"], [b"PUSH", b"b"], [b"PUSH", b"c"]], 0)
