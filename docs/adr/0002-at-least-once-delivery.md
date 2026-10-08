# ADR-0002: At-least-once delivery; handlers must be idempotent

Status: Accepted · Date: 2026-10-08 (decided in Week 2, written down in Week 8)

## Context

A worker can crash at any moment, and across a network a worker cannot tell "slow" from "dead". So a queue must
choose what happens to a job whose worker dies mid-run. The choice comes down to one thing: **when the job is
marked done, relative to running it.**

| Ack timing | Crash after the ack, before the work | Crash after the work, before the ack | Guarantee |
|---|---|---|---|
| Ack before running | the job never runs: **lost** | cannot happen | at-most-once |
| Ack after running | cannot happen | the job runs again: **duplicate** | at-least-once |

No queue can promise exactly-once delivery when workers crash. What a system can build is *effectively once*:
at-least-once delivery plus handlers that make a second run harmless.

## Decision

**Hopper acks after the handler returns.** A job is marked `succeeded` only by a fenced `ACK` once its handler has
finished (`src/hopper/worker/loop.py`, `process`). Every way a run can end without an ack (crash, lease lost,
SIGKILL) brings the job back to `queued`, so it runs again. Handlers must therefore be **idempotent**: running one
twice must have the same effect as running it once.

Hopper helps handlers with that:

- Every job has a stable id. The `http` task sends it as `Idempotency-Key: <job id>` and `Hopper-Job-Id`, so the
  tenant's endpoint can drop a repeat.
- Every run knows its attempt number (`Hopper-Attempt`, and `JobContext.attempt`).
- The fencing token (ADR-0003) makes sure a stale run can never write the job's final state.

The ways Hopper can run a job twice, all expected:

1. A worker crashes after the side effect but before the ack.
2. A worker pauses past its lease; the reaper hands the job to another worker while the first is still running.
   The token blocks the first worker's ack, but not a side effect it already made outside Hopper.
3. An `http` job times out on Hopper's side after the target already did the work.
4. A dead job that had partly succeeded is replayed from the DLQ.
5. A client retries an enqueue without an `Idempotency-Key` (that makes two jobs; ADR-0006 prevents it).

## Alternatives considered

- **At-most-once (ack before running).** No duplicates, but a crash loses the job for good, and nothing shows it.
  The chaos test's negative control runs exactly this mode (`CHAOS_ACK_BEFORE_RUN`): it **lost 69 of 10,000 jobs**.
  For webhooks and payments, a lost job is worse than a duplicate that carries a dedup key.
- **Exactly-once inside Hopper.** Run the side effect and the ack in one database transaction. This only works when
  the effect lives in Hopper's own Postgres, not for an HTTP call or an email, so it cannot be the general rule.
- **Hold the row lock while the job runs.** A crash then releases the lock, so the job is not lost. But it costs one
  open transaction per running job, and it is still at-least-once (ADR-0001, ADR-0003).

## Consequences

- Measured: the chaos test killed workers 38 times on the EC2 server while 10,000 jobs ran. 0 were lost and 9 ran
  twice. Every job's effect still happened exactly once, because the `effect` task writes it with
  `ON CONFLICT (job_id) DO NOTHING`. On the laptop: 36 kills, 0 lost, 8 duplicates.
- Tenants must write idempotent handlers. The usual ways, with Hopper examples:
  - pass the job id downstream as an idempotency key (the `http` task does this);
  - a unique constraint or upsert (`ON CONFLICT DO NOTHING`, as in the chaos test's `job_effects`);
  - a conditional update (`UPDATE ... WHERE version = :v`), as every one of Hopper's own queue writes is fenced;
  - set a value instead of adding to it (`balance = 100`, not `balance = balance + 10`);
  - the outbox pattern, when the effect lives in the same database.
- Duplicates are rare but real: about 1 in 1,000 jobs while workers were killed every few seconds. Without
  crashes or stalls none are expected, because a job is only run again when its lease is lost.

## When we would revisit

If a common task type cannot be made idempotent (for example, an external API with no idempotency key), add a
transactional handler mode for effects that live in Hopper's database, and document which tasks need care.
