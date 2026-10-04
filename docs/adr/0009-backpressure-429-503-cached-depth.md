# ADR-0009: Backpressure: 429 for tenant quota, 503 for overload, cached depth

Status: Accepted · Date: 2026-10-04

## Context

A rate limit caps how fast work arrives, not how much piles up. A tenant enqueuing at its allowed 50 jobs/s with
slow handlers, or with jobs for a queue no worker serves, builds a backlog without end. A deep `jobs` table makes
claims, vacuum and every count slower for everyone, and claiming is by priority and time, not by tenant, so one
tenant's backlog delays the others. Hopper needs to refuse new work at a depth limit, per tenant and in total, and
checking the depth must not cost a query on the table that is already under pressure.

## Decision

Three layers, as in the build guide:

| Layer | Limit | Answer |
|---|---|---|
| Tenant quota | queued jobs of the tenant ≥ `tenants.max_queue_depth` (default 100,000) | 429 `queue_quota_exceeded`, `Retry-After: 5` |
| Global overload | queued jobs of all tenants ≥ `GLOBAL_MAX_QUEUE_DEPTH` (default 1,000,000) | 503 `overloaded`, `Retry-After: 5` |
| Worker intake | 20 slots per worker; claim `LIMIT` = free slots | workers never take more than they can run |

"Queued" means `status = 'queued'`: ready now, delayed, or waiting to retry. **The depth is counted once a second by
each scheduler**, with two `GROUP BY` queries that read only the partial indexes (`jobs_ready_idx`, `jobs_lease_idx`,
`jobs_dead_idx`). It is published as one Redis hash (`total`, plus one field per tenant with queued jobs), replaced in
a single `MULTI` and expiring after 15 s. On enqueue, replay and bulk replay, the API reads two fields with `HMGET`;
it never counts. The same count sets the `hopper_queue_depth` and `hopper_oldest_ready_job_age_seconds` gauges.
429 means "you are over your own limit" and 503 means "the service is overloaded"; both say when to retry.

## Alternatives considered

- **`count(*)` on every enqueue.** Exact, but it adds a query per request whose cost grows with the backlog, so it
  gets slower exactly when the system is already overloaded.
- **Exact counters kept on every state change (triggers, or Redis INCR/DECR).** No counting query, but every claim,
  ack and retry writes the counter too: a hot row in Postgres, or a dual write to Redis that drifts after a crash.
- **Planner estimates (`pg_class.reltuples`).** Free, but not per tenant, and not for a partial set of rows.
- **A global limit only.** One tenant could fill it and shut every other tenant out.
- **One scheduler counts (leader or advisory lock).** Half the counting, but a dead leader leaves the gauges and the
  snapshot stale until someone notices. Both count, and the dashboard takes the max.

## Consequences

- The limit is soft. The snapshot is up to about a second old, so enqueues inside that second can overshoot by up
  to the tenant's burst plus one second of its rate, and a bulk replay can add 1,000 at once. On the Docker stack, a
  tenant with a quota of 20 had 20 delayed jobs accepted; after the next count, the 21st got 429 while its reads
  still worked. An API started with a global limit of 10 answered 503 `overloaded` while 20 jobs were queued.
- Counting reads only the partial indexes. A test confirms the plans use them, with no sequential scan, over 20,000
  finished rows. Two schedulers × two queries a second is the cost; how it grows with a 100,000-job backlog is a
  Week 8 measurement.
- If no scheduler has published for 15 s, the hash expires and the API **fails open** (no depth limits), rather
  than trusting a count that no longer moves; the `NoScheduler` alert covers that window. If Redis is down, the API
  uses the last counts it read: stale, but still a limit.
- Known gaps: cron jobs are not checked against the quota (a schedule fires at most once a minute, so it cannot
  flood). The check runs before the idempotency lookup, on purpose (it costs no query). So during overload, a client
  retrying a request that already created its job also gets 429 or 503 and must retry later.

## When we would revisit

If the depth queries show up in the slow-query log at load-test depths (then use incremental counters or
estimates); if tenants need quotas per queue; or if one tenant's backlog visibly delays another's jobs, which calls
for per-tenant in-flight caps or round-robin claiming across tenants.
