# ADR-0001: Use PostgreSQL as the job queue

Status: Accepted · Date: 2026-10-03

## Context

Hopper must run jobs now, later or on a schedule and never lose one. Each job has state (attempts, lease,
last error, result) that tenants query, and that state has to change atomically with queue operations.
A separate broker would split "what is queued" from "what happened to the job" across two stores.
No load numbers exist yet; the throughput ceiling is measured in Week 8 (scenario D).

## Decision

Postgres is both the source of truth and the queue. Workers claim ready rows in a short transaction with
`SELECT ... FOR UPDATE SKIP LOCKED`, which gives concurrent workers disjoint batches without waiting on each
other. Every state transition is one guarded `UPDATE`. Redis holds only rate-limit buckets and cached counters,
so losing it never loses a job.

## Alternatives considered

- **Redis Streams with `XAUTOCLAIM`.** Faster, but job state splits across two stores and every enqueue becomes
  a dual write that can disagree after a crash.
- **RabbitMQ.** Mature, but one more system to run, secure and explain, and job history would still need a database.
- **Holding a row lock for the whole job.** Needs one open transaction and connection per running job and blocks
  vacuum. Leases (ADR-0003) decouple job length from transactions.

## Consequences

- Better: one transactional store, no dual write, and enqueue plus idempotency check commit together.
  Production libraries (Oban, River, Solid Queue, pg-boss) use the same pattern.
- Worse: every job costs several writes to `jobs` (insert, claim, heartbeats, ack, attempt row), so the
  table churns and autovacuum must keep up; `jobs` is created with `autovacuum_vacuum_scale_factor = 0.02`.
- We must keep a `Broker` interface in code (claim, heartbeat, ack, nack, release) so the backend can change.

## When we would revisit

When load-test scenario D shows Postgres write load, lock waits or connections saturating before the API does,
with p95 queue wait above 5 s or errors above 1% at a rate we want to support. The next step would be a
dedicated log for the hot path with Postgres keeping job state.
