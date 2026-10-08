# ADR-0004: Full-jitter exponential backoff with per-task overrides

Status: Accepted · Date: 2026-10-08 (decided in Week 3, written down in Week 8)

## Context

A failed job should be tried again, but not at once and not forever. When a dependency goes down, many jobs fail at
the same moment. If they all retry after the same delay, they hit the dependency together again, in waves, just as
it tries to recover. Different tasks also need different patience: a `sleep` test job can retry in seconds, while an
`http` call to a tenant's server may need to wait minutes and honour the server's own `Retry-After`.

## Decision

After failed attempt *n* (counting from 1), the job goes back to `queued` with

```
run_at = now() + uniform(0, min(cap, base × 2^(n-1)))
```

This is **full jitter**: the delay is a random point anywhere in the window, not the window's end
(`src/hopper/worker/backoff.py`). The worker does not sleep. It nacks at once, and the `NACK` statement moves
`run_at` forward on the database clock, so the job's slot and lease are free while it waits.

| Setting | Default | `http` task |
|---|---|---|
| attempts | 5 | 8 |
| base | 2 s | 10 s |
| cap | 600 s | 1 h |
| timeout per run | 30 s | 15 s |

Each task sets its own values in `@task(...)`. A handler can raise `RetryableError(retry_after=...)`, and the
larger of that and the backoff wins; the `http` task does this with the target's `Retry-After` header (capped at
1 h). `PermanentError`, or a payload that fails validation, skips the retries and goes straight to the DLQ.

## Alternatives considered

- **A fixed delay.** Simple, but every job that failed together retries together, every time.
- **Exponential without jitter.** Spreads attempts out over time, but not jobs from each other: 1,000 jobs that
  failed at the same second still retry at the same second. That is the "waves" problem.
- **Equal jitter or decorrelated jitter.** Also good. In Marc Brooker's simulation on the AWS Architecture Blog,
  full jitter did the least total work, and it is the easiest to reason about and to test.
- **Sleep inside the worker before retrying.** Keeps the job's slot, lease and heartbeats busy while nothing
  happens, and a crash during the sleep loses the delay. Moving `run_at` costs one write.
- **One policy for every task.** Simpler code, but the `http` task needs to wait much longer than a test task.

## Consequences

- With the defaults, all 5 attempts fit in under about 30 s of backoff (2 + 4 + 8 + 16 s at most), so a bad job
  reaches the DLQ quickly. An `http` job waits up to about 21 minutes in total before the DLQ, enough for a short
  outage on the tenant's side.
- A retry can come almost at once, because full jitter's lower bound is 0. On average it waits half the window.
- Tests check the bounds on every retry, not one sample (`tests/integration/test_retries.py`); `Retry-After` wins
  when it is larger; a permanent error is dead after one attempt; and a job is dead after exactly `max_attempts`.
- In load scenario C, 5% `flaky` jobs at 300 jobs/s caused about 4,500 retries in 10 minutes, while the queue
  wait stayed at p95 189 ms (`docs/benchmarks.md`).
- Delays are on the database clock, so workers' clocks never matter.

## When we would revisit

If tenants need to set a policy per job instead of per task (add optional fields to the enqueue body), or if a
long outage at one target sends many jobs to the DLQ (add a per-target circuit breaker, so jobs wait instead of
failing).
