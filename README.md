# Hopper

[![ci](https://github.com/Srinu-design/hopper/actions/workflows/ci.yml/badge.svg)](https://github.com/Srinu-design/hopper/actions/workflows/ci.yml)

A multi-tenant job queue and scheduler on PostgreSQL: send a job over HTTP; it runs at least once,
retries with backoff, parks in a dead-letter queue if it keeps failing, and can be replayed.

> **Status: Week 6 of 8 (limits and observability).** Tenants enqueue jobs over HTTP with an API key (now, after
> a delay, at a set time, or on a cron schedule), optionally with an `Idempotency-Key`. Workers claim them with
> `FOR UPDATE SKIP LOCKED` under a heartbeated lease, retry with full-jitter backoff, and park failures in a
> dead-letter queue you can list and replay. The `http` task calls a tenant's URL behind an SSRF guard and signs
> every request. Each tenant is rate-limited by a Redis token bucket (429 with `Retry-After`), deep queues are
> refused (429 per tenant, 503 overall), and every process reports to Prometheus, with a provisioned Grafana
> dashboard and five tested alerts. Deployment and the load and chaos tests arrive in Weeks 7 and 8. Nothing here
> claims behaviour that is not built and tested yet.

The demo video, headline numbers, architecture diagram and live links will go at the top of this file in
Week 8, once there is something measured to show.

## Quickstart

Requirements: Docker with the Compose plugin. For development also [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env         # includes development-only secrets; generate your own for anything real
make up                      # Postgres + Redis, migrations, the API, 3 workers, 2 schedulers, Prometheus, Grafana
curl localhost:8000/healthz  # {"status":"ok"}
curl localhost:8000/readyz   # {"status":"ok","checks":{"postgres":"ok","redis":"ok"}}
# Grafana: http://localhost:3000 opens the Hopper dashboard (view without logging in; admin password in .env)
# Prometheus: http://localhost:9090 (Alerts page: QueueBacklog, HighFailureRate, DLQGrowing, NoWorkers, NoScheduler)

# 1. create the first platform admin (asks for a password, 12+ characters)
docker compose -f docker/compose.yaml --env-file .env run --rm api python -m hopper.bootstrap --email admin@example.com

# 2. log in, and create a tenant with its owner. The response's signing_secret is shown once:
#    keep it to verify the Hopper-Signature header on http task calls. Limits are optional:
#    rate_per_sec (50) and burst (100) for the token buckets, max_queue_depth (100000) for backpressure.
curl -X POST localhost:8000/auth/token -H 'content-type: application/json' -d '{"email":"admin@example.com","password":"<password>"}'
curl -X POST localhost:8000/admin/tenants -H 'Authorization: Bearer <admin token>' -H 'content-type: application/json' \
  -d '{"name":"acme","rate_per_sec":50,"burst":100,"owner":{"email":"ops@acme.example","password":"<owner password>"}}'

# 3. as the owner, create an API key (shown once) and use it for everything under /v1
curl -X POST localhost:8000/auth/token -H 'content-type: application/json' -d '{"email":"ops@acme.example","password":"<owner password>"}'
curl -X POST localhost:8000/admin/api-keys -H 'Authorization: Bearer <owner token>' -H 'content-type: application/json' -d '{"name":"my app"}'
KEY='Authorization: Bearer hop_live_...'   # the "key" field from the response
J='content-type: application/json'

# enqueue a job now, after a delay, or at a set time (at most 30 days out), then check on it.
# -i shows X-RateLimit-Limit and X-RateLimit-Remaining; past the burst you get 429 with Retry-After.
curl -i -X POST localhost:8000/v1/jobs -H "$KEY" -H "$J" -d '{"task":"sleep","payload":{"ms":100}}'
curl -X POST localhost:8000/v1/jobs -H "$KEY" -H "$J" -d '{"task":"sleep","payload":{"ms":100},"delay_seconds":30}'
curl -X POST localhost:8000/v1/jobs -H "$KEY" -H "$J" -d '{"task":"sleep","payload":{"ms":100},"run_at":"2026-12-01T09:00:00+05:30"}'
curl localhost:8000/v1/jobs/<id> -H "$KEY"                   # status, result and attempt history
curl "localhost:8000/v1/jobs?status=queued" -H "$KEY"        # newest first; ?queue=&limit=&cursor=
curl -X POST localhost:8000/v1/jobs/<id>/cancel -H "$KEY"    # only while still queued

# safe to retry: the same Idempotency-Key and body return the same job (200, Idempotent-Replayed: true)
curl -X POST localhost:8000/v1/jobs -H "$KEY" -H "$J" -H 'Idempotency-Key: order-42' -d '{"task":"sleep","payload":{"ms":100}}'

# cron: five fields, in the schedule's own time zone; PATCH turns it off and on
curl -X POST localhost:8000/v1/schedules -H "$KEY" -H "$J" \
  -d '{"name":"morning-report","cron":"0 9 * * 1-5","timezone":"Asia/Kolkata","task":"sleep","payload":{"ms":10}}'
curl localhost:8000/v1/schedules -H "$KEY"
curl -X PATCH localhost:8000/v1/schedules/<id> -H "$KEY" -H "$J" -d '{"enabled":false}'

# the http task: Hopper calls your URL with the body, signed, and retries on 408, 429 and 5xx
curl -X POST localhost:8000/v1/jobs -H "$KEY" -H "$J" -d '{"task":"http","payload":{"url":"https://example.com/","method":"GET"}}'

# fill the dead-letter queue, look at it, replay
curl -X POST localhost:8000/v1/jobs -H "$KEY" -H "$J" -d '{"task":"fail_always","payload":{"permanent":true}}'
curl localhost:8000/v1/dlq -H "$KEY"                         # ?queue=&task=&since=&limit=&cursor=
curl -X POST localhost:8000/v1/jobs/<id>/replay -H "$KEY"    # one job, runs now
curl -X POST localhost:8000/v1/dlq/replay -H "$KEY" -H "$J" -d '{"filter":{"task":"fail_always"},"spread_seconds":60}'

# crash recovery: kill -9 the worker running a job, and another worker finishes it within about 35 s
curl -X POST localhost:8000/v1/jobs -H "$KEY" -H "$J" -d '{"task":"sleep","payload":{"ms":20000}}'
docker compose -f docker/compose.yaml --env-file .env exec postgres psql -U hopper -c "select id, lease_owner from jobs where status = 'running'"
docker kill <container id>         # the part of lease_owner before the first dash, e.g. 6a07c0d4678c
curl localhost:8000/v1/jobs/<id> -H "$KEY"   # attempt_history: lease_expired on the dead worker, then succeeded

# graceful shutdown: running jobs get up to 25 s, the rest go back to the queue with no attempt used
docker compose -f docker/compose.yaml --env-file .env stop worker
```

Built-in tasks: `sleep` (`{"ms": 100}`), `flaky` (`{"p": 0.3}` fails with that probability), `fail_always`
(`{}` retries until it is dead; `{"permanent": true}` goes to the DLQ at once) and `http` (`{"url", "method",
"headers", "body"}`; 8 attempts, 10 s backoff base, 1 h cap, 15 s timeout). Admin tokens last 15 minutes, and
`/auth/token` allows 10 attempts per email, then one every 6 s. HTTPS arrives in Week 7, so keep the API on
localhost until then. Scale workers with `docker compose -f docker/compose.yaml --env-file .env up -d --scale
worker=N`; Prometheus finds new replicas by itself within 10 s.

| Answer | When | What to do |
|---|---|---|
| 429 `rate_limited` | the tenant's token bucket for this route class is empty | wait `Retry-After` seconds (`retry_after_ms` in the body) |
| 429 `queue_quota_exceeded` | the tenant already has `max_queue_depth` jobs queued (enqueue and replay only) | retry once some have run |
| 503 `overloaded` | all tenants together have `GLOBAL_MAX_QUEUE_DEPTH` jobs queued | retry after `Retry-After` |

Without `make` (for example on Windows), run what `make up` runs:

```bash
cp .env.example .env
docker compose -f docker/compose.yaml --env-file .env up -d --build postgres redis
docker compose -f docker/compose.yaml --env-file .env run --rm --build migrate
docker compose -f docker/compose.yaml --env-file .env up -d --build
```

`make down` stops the stack; add `-v` to the compose `down` to also delete the Postgres and Redis volumes.
API docs are served at <http://localhost:8000/docs>.

## Development

```bash
uv sync                # install from uv.lock
docker compose -f docker/compose.yaml --env-file .env up -d postgres redis
make test              # pytest against real Postgres and Redis
make lint              # ruff check, ruff format --check, mypy --strict on src/
make alerts            # promtool: check the Prometheus config and unit-test the alert rules
```

Tests use real Postgres and Redis, because mocks cannot prove `SKIP LOCKED` behaviour. Integration tests
create a throwaway database per test, so they never touch your dev data. Host-side URLs use `127.0.0.1`,
not `localhost`: on Windows `localhost` resolves to `::1` first and the published ports are IPv4 only.

## Layout

| Path | Contents |
|---|---|
| `src/hopper/` | `api/` (routers, rate limits and backpressure, HTTP metrics), `auth/` (API keys, passwords, JWT, tenant context), `ratelimit/` (`token_bucket.lua`, the limiter and its in-process fallback, the depth gate), `queue/` (Broker interface and all queue SQL), `worker/` (run loop, heartbeats, shutdown), `scheduler/` (cron, reaper and queue-depth loops), `tasks/` (registry, built-ins, `http`), `bootstrap.py` (first admin), `metrics.py`, `config.py`, `logging.py`, `db.py` |
| `migrations/` | Alembic revisions. Hand-written SQL, backward compatible with the previous release |
| `docker/` | `Dockerfile` (one image, every role) and `compose.yaml` |
| `deploy/` | `prometheus/` (`prometheus.yml`, `alerts.yml`, `alerts_test.yml`) and `grafana/provisioning/` (data source, dashboard JSON) |
| `tests/` | `unit/`, `integration/`, `e2e/` |
| `loadtest/`, `chaos/` | Reserved for Week 8 |
| `docs/` | `adr/` decision records, `ai-usage.md` |

## Roadmap and changelog

| Week | Milestone | State |
|---|---|---|
| 1 | Skeleton: repo, uv, ruff and mypy, Dockerfile, Compose, Alembic schema, `/healthz`, CI | done |
| 2 | Queue core: enqueue, `SKIP LOCKED` claim, fenced ack, worker loop | done |
| 3 | Failure handling: retries, DLQ and replay, idempotency keys | done |
| 4 | Reliability: leases, heartbeats, reaper, graceful shutdown | done |
| 5 | Scheduling and tenancy: delayed and cron jobs, API keys, JWT, `http` task | done |
| 6 | Limits and observability: token bucket, backpressure, metrics, Grafana | done |
| 7 | Delivery: GHCR, EC2, deploy with rollback | |
| 8 | Proof and write-up: load tests, chaos test, design doc, demo | |

### Week 1
- Repo layout, `pyproject.toml` with a committed `uv.lock`, ruff and `mypy --strict` on `src/`.
- Alembic migration `0001` creates `tenants`, `api_keys`, `schedules`, `jobs` and `job_attempts`, with the
  partial indexes for the claim, reaper, DLQ and cron paths, and a lowered autovacuum scale factor on `jobs`.
- FastAPI app with `GET /healthz` (liveness, touches nothing) and `GET /readyz` (Postgres and Redis reachable).
- One Docker image; Compose runs Postgres 16.15, Redis 7.4.11, a one-off `migrate` service and the API.
- GitHub Actions `ci.yml`: lint and type check, tests against Postgres and Redis service containers, image build.

### Week 2
- `POST /v1/jobs` enqueues (task, payload, queue, priority); `GET /v1/jobs/{id}` returns status, result and attempt
  history. Unknown tasks and invalid payloads return 422, payloads over 256 KB return 413, and every error uses
  the `{"error": {"code", "message", "request_id"}}` shape.
- Hand-written claim SQL (`FOR UPDATE SKIP LOCKED`, fresh lease token on every claim) and a fenced ack that only
  succeeds for the current lease token and writes the attempt row in the same statement (`src/hopper/queue/sql.py`).
- `Broker` interface with a Postgres implementation. Workers hold at most `WORKER_SLOTS` (20) jobs, claim with
  `LIMIT = free slots`, poll with jitter and back off (capped at 0.5 s) when idle.
- Task registry with `@task(...)` and the `sleep` and `flaky` built-ins; Compose runs 3 worker containers.
- Proven by tests: 10 concurrent claimers over 1,000 jobs claim each job exactly once
  (`tests/integration/test_claim.py`), a stale token cannot ack, in-flight jobs never exceed the slot limit, and an
  API to worker to `succeeded` end-to-end test (`tests/e2e/`).
- Known gaps by design: a job whose handler fails or times out is not acked and stays `running`. Retries arrive
  in Week 3 and lease reclaim in Week 4.

### Week 3
- **Retries with full jitter.** After failed attempt *n* the job is requeued with
  `run_at = now() + uniform(0, min(cap, base * 2^(n-1)))`; defaults base 2 s, cap 600 s, 5 attempts, and each task
  can override all three. A handler can raise `RetryableError(retry_after=...)` and the larger delay wins. The
  failure is one fenced `UPDATE` (`NACK` in `src/hopper/queue/sql.py`) that also writes the attempt row.
- **Retryable vs permanent.** Timeouts (`timed_out`), unknown tasks and unexpected exceptions retry. `PermanentError`
  and a payload that fails validation go to the DLQ at once. Out of attempts also means dead.
- **Dead-letter queue.** Dead jobs keep their payload, `last_error` and full attempt history. `GET /v1/dlq` lists
  them newest first (filters `queue`, `task`, `since`; keyset pagination). `POST /v1/jobs/{id}/replay` requeues one
  now (409 if it is not dead, so replay is a no-op the second time). `POST /v1/dlq/replay` requeues up to 1,000 by
  ids or filter and spreads `run_at` over `spread_seconds` (default 60) so a replay does not stampede.
- **Idempotency keys.** `Idempotency-Key` on `POST /v1/jobs`: the same key and an equivalent body (SHA-256 of the
  canonical JSON) return the original job with 200 and `Idempotent-Replayed: true`; a different body returns 422
  `idempotency_key_reused`. 50 concurrent requests with one key create exactly one job.
- `fail_always` built-in task; the job JSON now includes `dead_at` and `replay_count`.
- Tests: jitter bounds on every retry, dead after `max_attempts`, permanent errors, Retry-After, stale-token nack,
  DLQ filters, pagination and batching, replay idempotency, the idempotency-key cases, and an end-to-end
  fail, dead, replay, succeeded run (`tests/e2e/test_dead_letter_and_replay.py`). Each test now clones a database
  migrated once per session, which took the suite from about 140 s to about 40 s.
- Choice made: the guide's backoff table implies 6 attempts by default, but the schema default is 5; Hopper uses 5
  everywhere so the column default and the task default agree.

### Week 4
- **Leases and heartbeats.** A claim takes a 30 s lease. Every 10 s each worker renews the lease on all of its
  in-flight jobs in one fenced statement (`HEARTBEAT` in `src/hopper/queue/sql.py`, `UPDATE ... FROM unnest(ids,
  tokens)`); the ids that come back are the leases it still holds. A job missing from the answer was reclaimed, so
  its handler is cancelled and it is never acked.
- **Reaper.** A new scheduler process (`python -m hopper.scheduler`, Compose service `scheduler`) runs the reaper
  every 5 s: one `FOR UPDATE SKIP LOCKED` statement requeues up to 500 running jobs whose lease expired and writes a
  `lease_expired` attempt row (`REAP`). Two schedulers can run at once; each expired job is reaped once.
- **Poison pills.** The expired run counts as an attempt, so a job that takes down its worker every time is `dead`
  after `max_attempts` instead of looping forever.
- **Graceful shutdown.** On SIGTERM a worker stops claiming, waits up to 25 s (`SHUTDOWN_GRACE_SECONDS`, under
  Compose's 30 s `stop_grace_period`), cancels what is still running and releases it in one statement (`RELEASE`):
  back to `queued` with the attempt refunded and the run recorded as `released`. Then it exits 0. Heartbeats keep
  running during the drain, so no lease lapses while the worker waits.
- **First kill -9 recovery (manual, Docker stack, 3 workers).** `docker kill` on the worker running a 20 s `sleep`
  job, about 4 s into it. The lease had 24.3 s left; 30 s after the kill the reaper had requeued the job and another
  worker had claimed it, and it succeeded 51 s after the kill. History: `1 lease_expired` (the dead worker), then
  `2 succeeded` (another worker). The killed container exited with 137.
- **SIGTERM (manual).** `docker compose stop worker` with a 4 s and a 28 s job in flight on one worker: the 4 s job
  finished, the 28 s job was released at 25.0 s, all three workers exited 0, and the stop took 26 s. After a restart
  the released job ran as attempt 1 again and succeeded.
- Tests (116, up from 96): one heartbeat renews many leases and reports the lost ones; the reaper requeues only
  expired leases, works in batches, and five concurrent reapers reclaim 300 jobs exactly once; after expiry a second
  worker claims with a new token while the first worker's late ack updates 0 rows; a poison pill is dead after 3
  expiries; release refunds the attempt and is fenced; heartbeats keep a 2.5 s job alive under a 1 s lease with the
  reaper running; a lost lease cancels the handler within one heartbeat; stop finishes short jobs and releases the
  rest. Two tests run a real `python -m hopper.worker` process: `kill -9` mid-job, after which another worker finishes
  the job, and SIGTERM with jobs in flight, which exits 0 (Linux only: Windows has no SIGTERM). Each new guarantee
  was also broken on purpose to check that its test fails.
- New settings: `HEARTBEAT_SECONDS` (10, must be below `LEASE_SECONDS`), `SHUTDOWN_GRACE_SECONDS` (25),
  `REAPER_INTERVAL_SECONDS` (5), `REAPER_BATCH_SIZE` (500). No migration: the Week 1 schema already had the lease
  columns and the partial `jobs_lease_idx` the reaper scans.
- Choice made: release refunds the attempt, so frequent deploys never push a job into the DLQ; a lease expiry does
  not, which is what bounds poison pills. Lease numbers and the alternatives are in ADR-0003.

### Week 5
- **Tenants and API keys.** Every `/v1` call needs `Authorization: Bearer hop_live_<prefix>_<secret>`, and the
  tenant comes only from the key. Keys store just the prefix and `HMAC-SHA256(pepper, secret)`, are compared in
  constant time, cached per API process for 60 s, and record `last_used_at` at most once a minute. The Week 1 to 4
  "default tenant" placeholder is gone.
- **Admins.** `python -m hopper.bootstrap` creates the first platform admin. `POST /auth/token` checks an Argon2id
  password (unknown email and wrong password look the same) and returns a 15-minute HS256 JWT with `iss` and `aud`.
  Platform admins create tenants (`POST /admin/tenants`, optionally with the owner); owners create, list and revoke
  their own API keys (`/admin/api-keys`). Revoking evicts the key from that replica's cache at once.
- **Isolation.** Another tenant's job, schedule or key answers 404, never 403. One test calls every id route with
  tenant B's key against tenant A's rows and checks nothing changed; another fails when a new route is added without
  being classified; every protected route is checked to reject anonymous calls.
- **Delayed jobs.** `delay_seconds` (database clock) or `run_at` (any offset, stored as the same instant), at most 30
  days out; a past `run_at` runs now. Also `GET /v1/jobs` (status and queue filters, keyset pagination on
  `(created_at, id)`) and `POST /v1/jobs/{id}/cancel` (409 unless still queued).
- **Cron.** `/v1/schedules` (create, list, get, `PATCH` enable or disable, delete). The scheduler's cron loop runs
  every second: `SKIP LOCKED` on due schedules, then the job insert (key `cron:<schedule>:<due time>`) and the move of
  `next_run_at` in one transaction. Misfire policy: fire once, then jump ahead. Five-field expressions only (nothing
  more often than once a minute), croniter strict mode, IANA time zones, and wall-clock DST handling: a daily 01:30
  fires once on the autumn change, and 02:30 runs at 03:30 on the spring change. Compose runs 2 schedulers.
- **The `http` task.** Calls the tenant's URL with a JSON body. The SSRF guard is an httpcore network backend that
  resolves the host, refuses any private, loopback, link-local (169.254.169.254), shared, reserved or multicast
  answer, and dials the address it checked (no DNS rebinding window), while TLS still verifies the real host name.
  No redirects, no credentials in URLs, 5 s connect and 10 s read timeouts, at most 64 KB of the response read.
  Each request carries `Hopper-Job-Id`, `Hopper-Attempt`, `Idempotency-Key: <job id>` and
  `Hopper-Signature: t=<unix>,v1=<HMAC-SHA256(tenant secret, "t.body")>`. 2xx succeeds; 408, 429, 5xx, timeouts
  and connection errors retry (honouring `Retry-After`, capped at 1 h); other 4xx and 3xx go to the DLQ.
- **Migration 0002** (backward compatible): a `users` table, a per-tenant `signing_secret` with a random default,
  and `jobs.schedule_id` set to NULL when its schedule is deleted.
- **Checked on the Docker stack.** The bootstrap CLI, admin login, tenant and owner creation and an owner's API key
  all worked through the real API; the owner got 403 on `/admin/tenants`. An `http` GET to `https://example.com/`
  succeeded with 200 through the guard; `http://postgres:5432/` was refused (it resolves to 172.18.0.7) and went to
  the DLQ after one attempt; `http://169.254.169.254/` was rejected at enqueue with 422. A `* * * * *` schedule fired
  three ticks, 0.1 to 0.9 s after each minute, once each, shared between the two schedulers (1, 2, 1) with no
  duplicate. A 20 s delayed job started 20.14 s after it was enqueued. A second tenant got 404 on the first
  tenant's job and an empty job list.
- Tests: 284 (up from 116), on Windows and in a Linux container. They cover key format and verification, the 60 s
  cache trade-off, `alg: none`, HS512, expired, wrong-audience and forged JWTs, the admin flow, isolation on every
  route, delays and cancel, cron (two loops on one due schedule make one job, 60 schedules shared without doubling,
  misfire, the idempotency-key safety net, DST in New York), and the `http` task against a real local server
  (signature verified by the receiver, retry, DLQ, redirect, timeout, size cap, and the guard on the real client).
  One end-to-end test goes from the bootstrap CLI to a signed webhook. Nine guarantees were also broken on purpose
  to confirm a test fails for each.
- New settings: `API_KEY_PEPPER` and `JWT_SECRET` (required; the API refuses to start without them),
  `JWT_TTL_SECONDS`, `API_KEY_CACHE_SECONDS`, `HTTP_ALLOW_PRIVATE_NETWORKS` (local demos only),
  `HTTP_CONNECT_TIMEOUT`, `HTTP_READ_TIMEOUT`, `CRON_INTERVAL_SECONDS`, `CRON_BATCH_SIZE`.
- Choices made: the signature covers a timestamp as well as the body (`t.body`), so receivers can reject replays;
  `PATCH` on a schedule only enables or disables it (delete and recreate to change its timing).

### Week 6
- **Rate limits.** Each tenant has one token bucket per route class: `enqueue` (enqueue, replay, bulk replay) and
  `read` (every other `/v1` call). Capacity is its `burst` (100) and refill its `rate_per_sec` (50). One Redis Lua
  script reads, refills, takes and writes (`src/hopper/ratelimit/token_bucket.lua`) on the Redis clock, so the
  limit holds across API replicas. Over the limit: 429 `rate_limited` with `Retry-After` and `retry_after_ms`. Every
  `/v1` response carries `X-RateLimit-Limit` and `X-RateLimit-Remaining`. `/auth/token` allows 10 attempts per
  email, then one every 6 s, the same for unknown emails.
- **Redis down: fail open.** Redis calls time out after 0.25 s. After an error each API process uses an in-process
  bucket (same maths) for 5 s and counts `hopper_ratelimit_fallback_total`. When only Redis is down, `/readyz`
  answers 200 `degraded`, not 503, so a load balancer keeps the API in rotation.
- **Backpressure.** Each scheduler counts queued jobs once a second (two queries that read only the partial indexes)
  and publishes the counts per tenant to Redis. Enqueue and replays get 429 `queue_quota_exceeded` at the tenant's
  `max_queue_depth`, and 503 `overloaded` when all tenants together reach `GLOBAL_MAX_QUEUE_DEPTH` (1,000,000),
  both with `Retry-After: 5`. Reads are never held back.
- **Metrics.** Every process serves Prometheus metrics on the internal port 9100 (never published). That covers the
  guide's catalogue: jobs enqueued, runs by outcome, dead jobs, run-time and queue-wait histograms, depth by state,
  oldest ready job, jobs in flight, reclaimed leases, refused acks, requests and latency by route template, and
  rate-limit rejections and fallbacks. Backpressure rejections are counted too. Labels never carry a tenant or job
  id. Queue names not listed in `WORKER_QUEUES` are reported as `other`, because tenants choose them freely.
- **Prometheus, alerts and Grafana.** Compose now runs Prometheus v3.15.0, which finds every replica through
  Docker's DNS and scrapes every 5 s, and Grafana 13.2.3. Grafana's data source and a 20-panel dashboard (rows:
  overview, queue, latency, reliability, API) are provisioned from `deploy/grafana`, and anyone can view without
  logging in. There are five alert rules: `QueueBacklog`, `HighFailureRate`, `DLQGrowing`, `NoWorkers` and
  `NoScheduler`. Each has `promtool` unit tests (firing and quiet cases), which a new CI job runs.
- **Log correlation.** Migration `0003` adds `jobs.request_id`: the enqueue call's `X-Request-ID`. It appears on
  every worker log line for the job (`job_claimed`, `job_succeeded`, `job_retry_scheduled`, `job_dead`,
  `lease_reclaimed`), with `job_id`, `tenant_id`, `attempt` and `worker_id`. The migration also requires tenant
  limits to be positive, because the bucket divides by the rate.
- **Checked on the Docker stack.**
  - Prometheus scraped all 7 targets (1 API, 3 workers, 2 schedulers, itself); port 9100 is not reachable from the
    host.
  - A tenant with burst 5 at 1/s got `201 × 5, 429 × 3` with `Retry-After: 1` and remaining `4 3 2 1 0 0 0 0`, and
    a 201 again 1.1 s later. A tenant with a quota of 20 had 20 delayed jobs accepted, and the 21st got 429
    `queue_quota_exceeded`, while its reads still worked. An API started with a global limit of 10 answered 503
    `overloaded`. The 11th login attempt for one email got 429.
  - A request id sent on enqueue showed up on the worker's `job_claimed` and `job_succeeded` lines.
  - With Redis stopped: `/readyz` said `degraded` (200), one request paid the 0.25 s timeout, the rest took about
    5 ms, the burst-5 tenant got 6 of 8 through (in process), and the API logged one warning. When Redis came back,
    everything recovered on its own.
  - Alerts, fired for real with 12 jobs/s of mixed traffic: `HighFailureRate` (40%) and `DLQGrowing` (44 dead in 10
    minutes) fired 5 minutes in. With the workers stopped, `NoWorkers` fired after 1 minute and `QueueBacklog`
    after 5 minutes over 60 s. When the 3 workers came back, about 6,000 queued jobs drained in about 30 s, and
    `NoWorkers` and `QueueBacklog` resolved 12 s and 27 s after the restart. In the same 17 minutes, a tenant
    allowed 1/s got 1,019 × 201 and 2,041 × 429. Stopping both schedulers fired `NoScheduler` after 71 s, and
    the depth snapshot had expired within 20 s (backpressure fails open). Both came back within 25 s of the
    restart.
- Tests: 362 (up from 284), on Windows and in a Linux container. They cover the bucket against real Redis: exactly
  100 of 500 concurrent takes from three clients, `Retry-After` from the refill maths, refill and its cap, expiry,
  the Redis clock, fail-open with one Redis attempt per cool-down, and script errors that are not swallowed. Through
  the API they cover: exactly 100 of 200 across two app instances, headers, per-tenant and per-class buckets,
  login throttling, quota and overload at the configured depths, and failing open with no snapshot. Also: the depth
  counts and the snapshot, that the depth queries use the partial indexes (with no sequential scan over 20,000
  finished rows), every metric the worker, reaper, cron and API emit, that no label carries an id, and that the
  dashboard and alert rules only query metrics that exist. Fourteen guarantees were broken on purpose, and a test
  failed for each.
- New settings: `GLOBAL_MAX_QUEUE_DEPTH`, `BACKPRESSURE_RETRY_AFTER_SECONDS`, `DEPTH_INTERVAL_SECONDS`,
  `REDIS_TIMEOUT_SECONDS`, `REDIS_MAX_CONNECTIONS`, `REDIS_RETRY_SECONDS`, `REDIS_NAMESPACE`, `METRICS_PORT`, and
  `PROMETHEUS_PORT`, `GRAFANA_PORT` and `GRAFANA_ADMIN_PASSWORD` for Compose.
- Choices made:
  - `/readyz` no longer fails on Redis alone (the guide lists Redis as a readiness check), because rate limiting
    fails open without it.
  - A sixth alert, `NoScheduler`, since without a scheduler there is no cron, no reaper and no depth count.
  - The Redis client uses a blocking connection pool: redis-py's default pool fails at once when busy, and a test
    showed that turned a burst into a fallback that let 300 requests through a limit of 100 (ADR-0008).

## Docs

- [ADR-0001: PostgreSQL as the job queue](docs/adr/0001-postgres-as-queue.md)
- [ADR-0003: Leases with heartbeats and fencing tokens](docs/adr/0003-leases-heartbeats-fencing.md)
- [ADR-0007: Cron without leader election; a missed tick fires once](docs/adr/0007-cron-without-leader-election.md)
- [ADR-0008: Redis Lua token bucket; fail open when Redis is down](docs/adr/0008-redis-token-bucket-fail-open.md)
- [ADR-0009: Backpressure: 429 for tenant quota, 503 for overload, cached depth](docs/adr/0009-backpressure-429-503-cached-depth.md)
- [ADR-0010: API keys hashed with HMAC-SHA256 and a pepper; JWT only for admins](docs/adr/0010-api-keys-and-admin-jwt.md)
- [AI usage notes](docs/ai-usage.md)
