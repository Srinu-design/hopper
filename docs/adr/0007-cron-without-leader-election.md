# ADR-0007: Cron without leader election; a missed tick fires once

Status: Accepted · Date: 2026-10-04

## Context

Tenants schedule recurring jobs with cron expressions in their own time zone. The scheduler process that turns
due schedules into jobs must not be a single point of failure, so Compose runs two replicas. Two replicas must
never fire the same tick twice, and a scheduler that was down must not flood a tenant with catch-up runs when it
comes back. Daylight-saving changes must not fire a daily job twice or skip it.

## Decision

Every second, each scheduler runs one transaction: `SELECT ... FROM schedules WHERE enabled AND next_run_at <=
now() ... FOR UPDATE SKIP LOCKED LIMIT 100`, then for each row inserts the job with the idempotency key
`cron:<schedule id>:<due time>` (`ON CONFLICT DO NOTHING`) and moves `next_run_at` to the first time strictly after
`now()`. The insert and the move commit together. **Misfire policy: fire once, then jump to the next future
time.** Next times are computed by walking local wall-clock time, so each wall-clock time fires at most once a day.

## Alternatives considered

- **One scheduler.** Simplest, but while it is down no cron job runs and no expired lease is reclaimed.
- **Leader election through a Redis lock or a Postgres advisory lock.** Works, but adds lock renewal, fencing and a
  failover delay, and a stale leader can still fire. `SKIP LOCKED` already gives each due row to one replica.
- **Catch-up (fire every missed tick).** Faithful, but a scheduler down for a day would run an hourly job 24 times at
  once. Tenants who need every tick should make the job read its own window.
- **Iterate in aware (UTC-offset) time.** croniter's default. On the autumn change a daily `30 1 * * *` fires
  twice (01:30 EDT and 01:30 EST), which is wrong for anything like billing.

## Consequences

- Better: no leader, no extra moving part; two (or more) schedulers share the work and survive each other.
- The job insert and `next_run_at` update are one transaction, so a tick is never lost or doubled; the
  deterministic idempotency key is a second safety net (a test inserts the tick's job first and proves the tick adds
  nothing). The `cron:` prefix is reserved in the public API so a tenant cannot collide with it.
- Precision is the loop interval (1 s) plus worker poll time; measured on the Docker stack, ticks were enqueued 0.1 to
  0.9 s after the minute.
- DST, in a schedule's own zone: a time inside the spring gap (02:30) runs when the gap ends (03:30); a time in the
  repeated autumn hour runs once, at the first occurrence; an hourly schedule therefore skips the repeated hour.
- Validation: exactly five fields (no seconds field, so nothing more frequent than once a minute) and croniter's
  strict mode, which rejects dates that never happen such as 31 February. A stored expression that stops parsing is
  switched off and logged instead of failing every other schedule's tick.
- Re-enabling a disabled schedule restarts its clock from now, so no stale tick fires.

## When we would revisit

If the due-schedule query shows up in the slow-query log (many thousands of schedules due in the same second),
batch inserts per tick; if tenants ask for catch-up semantics, add a per-schedule misfire policy.
