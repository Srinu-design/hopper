# ADR-0005: The dead-letter queue is a job status, not a separate table

Status: Accepted · Date: 2026-10-08 (decided in Week 1, written down in Week 8)

## Context

A job that fails every attempt, or fails with a permanent error, must not be retried forever, and must not vanish.
A tenant needs to list these jobs, see why they failed, and replay them once the cause is fixed. Many queues keep
dead messages somewhere else: a dead-letter topic or exchange, a separate table, a "dead set".

## Decision

**A dead job is a row in `jobs` with `status = 'dead'`.** It keeps its id, payload, `last_error`, `dead_at` and its
full attempt history in `job_attempts`. A partial index serves the DLQ listing:

```sql
CREATE INDEX jobs_dead_idx ON jobs (tenant_id, queue, dead_at DESC) WHERE status = 'dead';
```

- `GET /v1/dlq` lists a tenant's dead jobs, newest first, with filters (`queue`, `task`, `since`) and keyset
  pagination.
- `POST /v1/jobs/{id}/replay` and `POST /v1/dlq/replay` (up to 1,000 at a time, by ids or filter) move them back
  with one guarded `UPDATE`: `status = 'queued'`, `attempts = 0`, `replay_count + 1`. A bulk replay spreads `run_at`
  over `spread_seconds` (default 60), so it does not hit the dependency that caused the failures all at once.
- Replay matches only rows that are still `dead`, so replaying twice changes nothing the second time (replaying
  one job answers 409).
- The retention loop deletes old `succeeded` and `cancelled` jobs, but **never dead ones**: they stay until a
  tenant replays them.

## Alternatives considered

- **A separate `dead_jobs` table.** Moving a job means a delete and an insert in one transaction. The attempt rows
  must move or point across tables, and replay moves everything back. There are two schemas to keep in sync, and
  more code paths where a job could be lost between them.
- **A special queue called `dlq`.** The job's real queue would have to be stored somewhere else to replay it, and
  a worker serving `dlq` by mistake would run dead jobs.
- **Delete jobs once they die, and only log them.** Nothing to replay, and a tenant cannot see what failed.

## Consequences

- Better: one job id for the whole life of a job, through death and replay. A tenant can follow it with
  `GET /v1/jobs/{id}`. The attempt history keeps every run, including runs before a replay. Death is one `UPDATE`
  (in `NACK` or `REAP`), so no job is ever "between tables".
- The claim query never sees dead rows: `jobs_ready_idx` covers only `status = 'queued'`.
- The chaos test counts a dead job as "not lost": it is visible and replayable.
- Worse: dead rows stay in `jobs` until replayed, so a tenant that never cleans up makes the table grow. There is
  no endpoint to delete dead jobs yet.

## When we would revisit

If dead rows become a large share of `jobs` (then add a purge endpoint, or move dead jobs older than N days to an
archive table), or if tenants need alerts per dead job (then add a webhook on death).
