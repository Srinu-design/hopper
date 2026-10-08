# ADR-0012: A broker written from scratch, behind the same Broker interface

Status: Accepted · Date: 2026-10-08 (the build guide's stretch goal)

## Context

Hopper's queue is Postgres (ADR-0001), and its measured limit was the API, not Postgres. But a
Postgres queue pays for things a broker built for the job would not: every push, claim and ack is a
transaction with a WAL fsync, rows churn and need vacuum, and an index scan finds the next job. The build
guide's stretch goal asks to build a small broker, plug it in behind the same `Broker` interface, and measure
both, to learn exactly what a dedicated broker buys and what it costs.

The workers already talk to the queue only through `Broker` (`claim`, `heartbeat`, `ack`, `nack`, `release`;
`src/hopper/queue/broker.py`), so a second implementation needs no change to the worker.

## Decision

**A single-node broker, `src/hopper/minibroker`, in Python asyncio** (the guide's option A):

- **Protocol: RESP**, the Redis wire protocol, so `redis-cli -p 6390` can talk to it. Commands:
  `PUSH queue body [DELAY ms] [MAXATTEMPTS n]`, `PULL queue lease_ms [COUNT n]`, `ACK id token`,
  `NACK id token delay_ms [DEAD]`, `EXTEND id token lease_ms`, `RELEASE id token`, `DEAD`, `REPLAY`, `STATS`.
- **Memory:** a ready heap per queue, ordered by ready time; a map of messages; a heap of lease deadlines
  that sends expired messages back to the queue. Heaps never delete from the middle: an entry that no longer
  matches its message is skipped when it reaches the top.
- **Leases and fencing, as on Postgres:** every PULL gives a new token, and every ACK, NACK, EXTEND and RELEASE
  must name it. An expired lease counts as an attempt, and the last attempt goes to the dead-letter queue.
- **Durability: an append-only log.** Every PUSH, ACK, NACK and death is written before the reply. On start the
  log is read back, and a torn last record (a write cut short by `kill -9`) is cut off. When the log is four times
  the size of the live messages, compaction rewrites it with only those, into a new file that replaces the old
  one atomically.
- **The fsync knob, like Redis's `appendfsync`:** `always` (fsync before every reply, shared by every request
  that arrived meanwhile: group commit), `everysec`, or `no`.
- **Leases are not logged.** A PULL costs no disk write. After a broker restart, every leased message is ready
  again, which is the at-least-once promise the broker already makes for a crashed worker.
- **The same worker:** `python -m hopper.minibroker.worker` is the production `Worker` class with `MiniBroker`
  in place of `PostgresBroker`.

## Alternatives considered

- **Go.** Faster, and the guide suggests it. But it is a second language to build, test and defend line by
  line, in a project whose every other part is Python; the guide says to stay in Python if switching would put
  finishing at risk.
- **Option B, a mini Redis.** Easier to benchmark (the real `redis-benchmark` works against it), but it would
  not plug into Hopper: no leases, no fencing, no DLQ.
- **Log every PULL too.** Attempt counts and leases would survive a broker restart, at the price of a disk write
  (and with `always`, an fsync) per PULL. A restart is rare and at-least-once already covers it.
- **SQLite or a key-value store as the log.** Durable and simple, but that would be measuring someone else's
  storage engine; the point is to build the core myself.
- **Replication.** Out of scope: one node, stated plainly. A broker host that dies takes its queue with it until
  it is back, and `always` is what makes "back" lose nothing.

## Consequences

- Measured in [loadtest/results/](../../loadtest/results/) and [chaos/results/](../../chaos/results/): see
  [benchmarks.md](../benchmarks.md#stretch-the-mini-broker-against-postgres).
- Tests: the same 10 contract tests run against both brokers (`tests/integration/test_broker_contract.py`), the
  store and log have unit tests with a fake clock (`tests/unit/test_minibroker_store.py`), and real processes are
  killed with `kill -9` (`tests/e2e/test_minibroker_crash.py`).
- **Worse than Postgres in what it keeps:** an acked message is forgotten (no result, no attempt history), there
  is no tenant data, so the `http` task cannot sign its requests on it, and nothing can be queried
  except `STATS` and `DEAD`.
- **All messages live in memory**, so the backlog is limited by RAM, not disk. Compaction stops the broker while
  it rewrites the log (fast for a short queue, slow for millions of waiting messages).
- **No authentication**: like Redis without a password, it must only listen on a private network.
- **fsync's protection was not tested end to end.** `kill -9` cannot show the difference between the modes (see
  benchmarks.md); only a power cut or a kernel crash can.

## When we would revisit

If Hopper needed more throughput than one Postgres gives for the queue's hot path, this is the shape the next
step would take: but with a production-grade log (Kafka, Redis Streams) or this broker in a faster language, with
replication, and with Postgres keeping job state (which brings back the dual write ADR-0001 avoided).
