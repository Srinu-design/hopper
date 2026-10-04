# ADR-0003: Leases with heartbeats and fencing tokens

Status: Accepted · Date: 2026-10-04

## Context

A worker can die in the middle of a job: `kill -9`, an out-of-memory kill, a lost host. Until Week 4 such a job
stayed `running` forever, because only the worker that claimed it could ack or nack it. Hopper needs to notice a
dead worker without a coordinator, hand its jobs to another worker, and stop a worker that was only *paused*
(a long GC pause, a network blip) from overwriting the new run when it wakes up. Jobs run up to their timeout
(30 s by default), so whatever marks a job as owned must outlive a single short transaction (ADR-0001).

## Decision

A claim takes a **lease**: `lease_expires_at = now() + 30 s` and a fresh `lease_token`. Every 10 s (lease / 3) each
worker renews the lease on all its in-flight jobs in **one** statement,
`UPDATE ... FROM unnest(:ids, :tokens) ... RETURNING id`, fenced per job on its token; a job missing from the answer
was reclaimed, so the worker cancels its handler and never acks it. A **reaper** in the scheduler process runs every
5 s and, in one `FOR UPDATE SKIP LOCKED` statement, requeues up to 500 running jobs whose lease has expired, writing a
`lease_expired` attempt row. The expired run counts as an attempt, so a job that crashes its worker every time
(a poison pill) is dead after `max_attempts`. Every write for a run (heartbeat, ack, nack, release) is
**fenced** on `lease_token`, so a worker that lost its lease changes nothing. On SIGTERM a worker waits up to 25 s,
then **releases** unfinished jobs in one statement without an attempt penalty.

## Numbers

| Setting | Value | Consequence |
|---|---|---|
| Lease | 30 s | A lease survives two missed heartbeats |
| Heartbeat | 10 s, one statement per worker | 8 workers x 20 slots = 160 jobs cost 0.8 statements/s (up to 16 row updates/s) |
| Reaper | every 5 s, batches of 500 | Worst-case recovery 30 s + 5 s = **35 s**, plus one poll (under 0.5 s) |
| False expiry | stall of 20 s or more | A worker must freeze for longer than lease minus heartbeat interval |
| Shutdown grace | 25 s | Under Compose's 30 s `stop_grace_period`, so SIGKILL never arrives first |

Measured on the Docker stack (3 workers): after `docker kill` on the worker running a 20 s job, the job was claimed
again 30 s later (its lease had 24.3 s left) and finished on another worker.

## Alternatives considered

- **Hold the row lock for the whole job.** No reaper needed, but one open transaction and connection per running
  job, and long transactions block vacuum (ADR-0001).
- **A lease with no heartbeat, sized to the longest job.** Simpler, but recovery takes as long as the longest
  timeout (an hour for a slow `http` job), and a job that overruns its lease runs twice.
- **One heartbeat statement per job.** Same safety, but 20x the statements per worker; `unnest` renews every
  lease in one round trip.
- **Session-level advisory locks.** Postgres frees them when the connection drops, but each running job pins a
  connection, a half-open TCP connection can take minutes to be noticed, and a partitioned worker still looks alive.
- **Fence on `lease_owner` instead of a token.** Not enough: the same worker can claim the same job again after it
  was reaped, so only a token that is new on every claim tells two runs apart.
- **A shorter lease (10 s lease, 3 s heartbeat).** Recovery in about 15 s, but 3.3x the heartbeat writes and false
  expiries after a 7 s stall.

## Consequences

- Better: crash recovery with no coordinator and no leader; a paused "zombie" worker cannot overwrite a newer run;
  job length is independent of transaction length; poison pills end in the DLQ instead of looping.
- Worse: delivery is at-least-once. A worker that stalls past its lease may finish its side effect after another
  worker has started, so handlers must be idempotent (ADR-0002). Recovery takes up to 35 s. Heartbeats add writes
  to the hot `jobs` table, which is why its autovacuum threshold is lowered.
- We must: keep handlers off the event loop when they are CPU-bound (`asyncio.to_thread`), because a blocked loop
  stops heartbeats; keep `SHUTDOWN_GRACE_SECONDS` under the orchestrator's kill timeout; never write a running job
  without the token in the `WHERE` clause.
- Release refunds the attempt, so frequent deploys never push a job into the DLQ. A lease expiry does not, which is
  what bounds poison pills.

## When we would revisit

If the load test shows heartbeat updates as a large share of writes to `jobs` or autovacuum falling behind; if
tenants need recovery faster than 35 s; or if `lease_expired` attempts show up for workers that were still alive
(false expiries), which would mean the lease is too short for real pauses.
