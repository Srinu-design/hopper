"""The mini broker's memory and log, with a fake clock: leases, fencing, retries, death,
recovery from the log (torn tail included), and compaction."""

import asyncio
from pathlib import Path

import pytest

from hopper.minibroker.log import Log
from hopper.minibroker.protocol import (
    Error,
    Simple,
    command,
    encode,
    parse_records,
    read_command,
    read_reply,
)
from hopper.minibroker.store import Store


class Clock:
    def __init__(self) -> None:
        self.ms = 1_000_000

    def __call__(self) -> int:
        return self.ms


@pytest.fixture
def clock() -> Clock:
    return Clock()


@pytest.fixture
def store(clock: Clock) -> Store:
    return Store(clock=clock)


# -- protocol -----------------------------------------------------------------------------------


def test_replies_encode_as_resp() -> None:
    assert encode(None) == b"$-1\r\n"
    assert encode(7) == b":7\r\n"
    assert encode(b"ab") == b"$2\r\nab\r\n"
    assert encode(Simple("OK")) == b"+OK\r\n"
    assert encode(Error("nope")) == b"-ERR nope\r\n"
    assert encode([b"a", 1, None]) == b"*3\r\n$1\r\na\r\n:1\r\n$-1\r\n"
    assert command("PULL", "q", 30000) == b"*3\r\n$4\r\nPULL\r\n$1\r\nq\r\n$5\r\n30000\r\n"


async def _feed(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


async def test_commands_and_replies_read_back() -> None:
    reader = await _feed(command("PUSH", "q", b"a\r\nb") + b"PING hello\r\n")
    assert await read_command(reader) == [b"PUSH", b"q", b"a\r\nb"]  # binary-safe body
    assert await read_command(reader) == [b"PING", b"hello"]  # inline, as telnet sends
    assert await read_command(reader) is None  # the client hung up
    reply = await _feed(encode([[b"id", 2, None], Simple("dead"), Error("x")]))
    assert await read_reply(reply) == [[b"id", 2, None], Simple("dead"), Error("x")]


def test_a_torn_last_record_is_left_out() -> None:
    whole = command("PUSH", "a", 1) + command("ACK", "x")
    torn = command("PUSH", "b", b"0123456789")[:-5]
    records, good = parse_records(whole + torn)
    assert records == [[b"PUSH", b"a", b"1"], [b"ACK", b"x"]]
    assert good == len(whole)


# -- leases and fencing -------------------------------------------------------------------------


def test_pull_leases_ready_messages_oldest_first(store: Store, clock: Clock) -> None:
    first = store.push("q", b"1")
    clock.ms += 1
    second = store.push("q", b"2")
    store.push("other", b"x")
    pulled = store.pull("q", lease_ms=1000, count=10)
    assert [p.message.id for p in pulled] == [first, second]
    assert [p.message.attempts for p in pulled] == [1, 1]
    assert store.pull("q", 1000, 10) == []  # leased messages are not handed out twice


def test_a_delayed_message_waits_for_its_time(store: Store, clock: Clock) -> None:
    mid = store.push("q", b"x", delay_ms=500)
    assert store.pull("q", 1000) == []
    clock.ms += 500
    [pulled] = store.pull("q", 1000)
    assert (pulled.message.id, pulled.wait_ms) == (mid, 0)


def test_every_write_is_fenced_on_the_lease_token(store: Store) -> None:
    store.push("q", b"x")
    [p] = store.pull("q", 1000)
    stale = "not-the-token"
    assert not store.ack(p.message.id, stale)
    assert store.nack(p.message.id, stale, 0) is None
    assert not store.extend(p.message.id, stale, 1000)
    assert not store.release(p.message.id, stale)
    assert store.ack(p.message.id, p.token)
    assert not store.ack(p.message.id, p.token)  # once: the message is gone


def test_an_expired_lease_goes_back_with_a_new_token(store: Store, clock: Clock) -> None:
    store.push("q", b"x")
    [first] = store.pull("q", lease_ms=1000)
    clock.ms += 999
    assert store.expire() == []
    clock.ms += 1
    [(_, status)] = store.expire()
    assert status == "queued"
    [second] = store.pull("q", 1000)
    assert second.token != first.token and second.message.attempts == 2
    assert not store.ack(first.message.id, first.token)  # the late worker changes nothing
    assert store.ack(second.message.id, second.token)


def test_extend_keeps_the_lease(store: Store, clock: Clock) -> None:
    store.push("q", b"x")
    [p] = store.pull("q", lease_ms=1000)
    clock.ms += 900
    assert store.extend(p.message.id, p.token, 1000)
    clock.ms += 900
    assert store.expire() == []  # the old deadline passed, the new one did not
    clock.ms += 100
    assert [s for _, s in store.expire()] == ["queued"]


def test_nack_retries_after_the_delay_then_dies(store: Store, clock: Clock) -> None:
    store.push("q", b"x", max_attempts=2)
    [p] = store.pull("q", 1000)
    assert store.nack(p.message.id, p.token, 500) == "queued"
    assert store.pull("q", 1000) == []
    clock.ms += 500
    [p] = store.pull("q", 1000)
    assert store.nack(p.message.id, p.token, 500) == "dead"  # the last attempt
    clock.ms += 10_000
    assert store.pull("q", 1000) == []
    assert [m.id for m in store.dead("q", 10)] == [p.message.id]


def test_a_permanent_failure_dies_at_once(store: Store) -> None:
    store.push("q", b"x", max_attempts=5)
    [p] = store.pull("q", 1000)
    assert store.nack(p.message.id, p.token, 0, dead=True) == "dead"


def test_a_poison_pill_dies_after_max_attempts_of_expiry(store: Store, clock: Clock) -> None:
    store.push("q", b"x", max_attempts=3)
    for expected in ("queued", "queued", "dead"):
        assert len(store.pull("q", 100)) == 1
        clock.ms += 100
        assert [s for _, s in store.expire()] == [expected]
    assert store.stats()["dead"] == 1


def test_release_refunds_the_attempt(store: Store) -> None:
    store.push("q", b"x")
    [p] = store.pull("q", 1000)
    assert store.release(p.message.id, p.token)
    [again] = store.pull("q", 1000)
    assert again.message.attempts == 1


def test_replay_brings_a_dead_message_back_with_fresh_attempts(store: Store) -> None:
    store.push("q", b"x", max_attempts=1)
    [p] = store.pull("q", 1000)
    store.nack(p.message.id, p.token, 0)
    assert store.replay(p.message.id)
    assert not store.replay(p.message.id)  # no longer dead: a no-op
    [again] = store.pull("q", 1000)
    assert again.message.attempts == 1


# -- the log ------------------------------------------------------------------------------------


def _reopen(path: Path, clock: Clock) -> Store:
    records, _ = Log.read(path)
    store = Store(clock=clock)
    store.load(records)
    return store


def test_a_restart_rebuilds_memory_from_the_log(tmp_path: Path, clock: Clock) -> None:
    path = tmp_path / "broker.log"
    log = Log(path)
    store = Store(log, clock=clock)
    acked, retried, dead, leased, waiting = (store.push("q", b"%d" % i) for i in range(5))
    pulled = {p.message.id: p for p in store.pull("q", 1000, count=4)}
    store.ack(acked, pulled[acked].token)
    store.nack(retried, pulled[retried].token, 5000)
    store.nack(dead, pulled[dead].token, 0, dead=True)
    log.write()

    again = _reopen(path, clock)
    assert set(again.messages) == {retried, dead, leased, waiting}
    assert again.messages[retried].ready_at == clock.ms + 5000
    assert again.messages[dead].dead
    # A lease is not logged: after a restart the leased message is simply ready again.
    assert {p.message.id for p in again.pull("q", 1000, count=10)} == {leased, waiting}


def test_a_torn_record_is_cut_off_on_restart(tmp_path: Path, clock: Clock) -> None:
    path = tmp_path / "broker.log"
    log = Log(path)
    store = Store(log, clock=clock)
    kept = store.push("q", b"kept")
    log.write()
    with path.open("ab") as f:  # a write cut short by kill -9
        f.write(command("PUSH", "torn", "q", 0, 5, 0, b"body")[:-3])
    records, torn = Log.read(path)
    assert torn > 0
    assert [r[1].decode() for r in records] == [kept]
    assert path.stat().st_size == len(command("PUSH", kept, "q", clock.ms, 5, 0, b"kept"))


def test_compaction_keeps_exactly_the_live_messages(tmp_path: Path, clock: Clock) -> None:
    path = tmp_path / "broker.log"
    log = Log(path)
    store = Store(log, clock=clock)
    for i in range(200):
        mid = store.push("q", b"%d" % i)
        [p] = store.pull("q", 1000)
        store.ack(mid, p.token)
    live = store.push("q", b"live", delay_ms=60_000)
    store.push("q", b"doomed", max_attempts=1)
    [p] = store.pull("q", 1000)
    store.nack(p.message.id, p.token, 0)
    log.write()
    before = path.stat().st_size

    log.rewrite(store.snapshot())
    assert path.stat().st_size < before / 20
    again = _reopen(path, clock)
    assert set(again.messages) == {live, p.message.id}
    assert again.messages[live].ready_at == clock.ms + 60_000
    assert again.messages[p.message.id].dead
    store.push("q", b"after")  # the log is still appendable after the rewrite
    log.write()
    assert len(_reopen(path, clock).messages) == 3
