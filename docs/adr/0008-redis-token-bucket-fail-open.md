# ADR-0008: Redis Lua token bucket; fail open when Redis is down

Status: Accepted · Date: 2026-10-04

## Context

Every tenant shares the API, the workers and one Postgres. A tenant whose client loops on `POST /v1/jobs` must get
a fast 429 with `Retry-After`, not slow everyone else down. The limit has to hold across both API replicas, so it
needs shared state, and the limiter must not become the reason the API is down. People logging in to `/auth/token`
need a limit too, or a password can be guessed as fast as Argon2 allows (ADR-0010).

## Decision

Each tenant has a **token bucket per route class**: `enqueue` (enqueue, replay, bulk replay) and `read` (every other
`/v1` call). Capacity is the tenant's `burst` (default 100) and refill is its `rate_per_sec` (default 50). The bucket
is a Redis hash, and **one Lua script** (`src/hopper/ratelimit/token_bucket.lua`, loaded with `register_script` and
called by SHA) does the read, refill, take and write, so it is atomic across replicas. The clock is **Redis `TIME`**;
only tests pass their own. The tenant's limits come back with its API key and are cached with it (60 s).

A refused request gets 429 `rate_limited`, `Retry-After: max(1, ceil(retry_ms / 1000))` and `retry_after_ms` in the
body. Every `/v1` response carries `X-RateLimit-Limit` (the burst) and `X-RateLimit-Remaining`. `/auth/token` has a
bucket per email (10 at once, then one every 6 s), keyed by a hash of the address, the same for known and unknown
emails.

**When Redis cannot be reached, the limiter fails open** to an in-process bucket with the same maths. Redis calls
time out after 0.25 s, and after a failure each process skips Redis for 5 s, so an outage costs one slow request per
process per 5 s, not one per request. `hopper_ratelimit_fallback_total` counts these decisions. `/readyz` answers
200 `degraded` instead of 503, so a load balancer keeps sending traffic.

## Alternatives considered

- **Fixed window counter (INCR with a TTL).** One command, but a client can send the full limit at the end of one
  window and again at the start of the next: twice the limit in a moment.
- **Sliding log (a sorted set of timestamps).** Exact, but memory and work grow with the limit, per request.
- **GET, then SET from the API.** Two replicas can both read the last token and both allow. The concurrency test
  catches exactly this.
- **MULTI/EXEC or WATCH.** MULTI cannot branch on a value it read; WATCH retries pile up on a hot tenant.
- **In-process buckets only.** No Redis, but each replica enforces the limit on its own, so two replicas allow twice
  the rate, and a scaled-up API allows more again.
- **Fail closed when Redis is down.** Strictly enforced limits, but a Redis outage becomes an outage of the whole
  API while Postgres, the actual source of truth, is fine.
- **Rate limits in Postgres.** A write per request to the very database the limiter is meant to protect.

## Consequences

- Measured: 500 concurrent takes from three limiters (three Redis clients) on one bucket of 100 with almost no
  refill allowed exactly 100. Through the real API, 200 concurrent enqueues split over two app instances gave
  100 × 201 and 100 × 429, with exactly 100 jobs in the table. On the Docker stack, a tenant with burst 5 and 1/s
  got 5 × 201 then 429 with `Retry-After: 1`, and a 201 again 1.1 s later.
- A test found a real failure mode. redis-py 8.1's default connection pool raises `MaxConnectionsError` at once
  when its connections are all busy. Under a burst that looks just like Redis being down, so the fallback allowed
  300 requests where the limit was 100. Hopper's Redis client now uses a blocking pool (50 connections per process,
  0.25 s wait), so a burst queues for a connection instead.
- Redis down, on the Docker stack: `/readyz` said `degraded` with 200. The first request paid the 0.25 s timeout
  and the rest took about 5 ms. The burst-5 tenant got 6 of 8 quick requests through (the burst plus refill), and
  the API logged one warning, not one per request.
- Worse: during a Redis outage a tenant can get up to (API replicas ×) its limit. A change to a tenant's limits
  takes up to 60 s to apply (the API key cache). Each `/v1` request costs one Redis round trip.
- The login bucket is per email, not per IP, because behind a proxy (Week 7) the client address needs
  trusted-proxy configuration first. The price: someone who knows an admin's email can keep that admin's login
  waiting for a few seconds at a time by hammering it.

## When we would revisit

If the load test shows Redis round trips limiting enqueue throughput (then take tokens in batches per process);
if a Redis outage's multiplied limits matter (then fail closed for `enqueue` only); if tenants need limits per API
key or per queue; or once a trusted proxy header exists, add a per-IP login bucket.
