# Hopper changelog

Week by week, newest at the end. The [README](../README.md) has the current state.

## Roadmap and changelog

| Week | Milestone | State |
|---|---|---|
| 1 | Skeleton: repo, uv, ruff and mypy, Dockerfile, Compose, Alembic schema, `/healthz`, CI | done |
| 2 | Queue core: enqueue, `SKIP LOCKED` claim, fenced ack, worker loop | done |
| 3 | Failure handling: retries, DLQ and replay, idempotency keys | done |
| 4 | Reliability: leases, heartbeats, reaper, graceful shutdown | done |
| 5 | Scheduling and tenancy: delayed and cron jobs, API keys, JWT, `http` task | done |
| 6 | Limits and observability: token bucket, backpressure, metrics, Grafana | done |
| 7 | Delivery: GHCR, EC2, deploy with rollback | done; live on EC2 |
| 8 | Proof and write-up: load tests, chaos test, design doc, demo | done |
| 9–10 | Stretch: a broker written from scratch, with its benchmark and chaos run | done |

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
  - A fifth alert beyond the guide's four, `NoScheduler`, since without a scheduler there is no cron, no reaper
    and no depth count.
  - The Redis client uses a blocking connection pool: redis-py's default pool fails at once when busy, and a test
    showed that turned a burst into a fallback that let 300 requests through a limit of 100 (ADR-0008).

### Week 7
- **The image is the release.** CI tags one image per commit to main with its sha on GHCR (public, no secrets
  inside). It also carries that release's deploy bundle: `compose.prod.yaml`, the Caddy, Prometheus and Grafana
  config, `deploy.sh` and the smoke test. So config always ships with its code, and a rollback restores both.
- **Production stack** (`docker/compose.prod.yaml`): Caddy is the only thing with published ports (80 and 443, with
  automatic HTTPS when a domain is set). Two named API replicas sit behind it, with active `/readyz` probes and
  retries. Then 3 workers, 2 schedulers, Postgres, Redis, Prometheus, and Grafana under `/grafana/` (view only).
  Logs are capped, the project name is fixed so volumes survive every release, and secrets are required, never
  defaulted.
- **`deploy/deploy.sh <sha>`** runs these steps:
  1. pull the image and unpack its bundle;
  2. migrate;
  3. install the config and reload Caddy and Prometheus in place (Grafana restarts only when its data sources
     changed);
  4. **drain** `api-1` (`/readyz` answers 503 `draining` while it serves), replace it and wait until healthy; then
     the same for `api-2`;
  5. replace the workers and the rest;
  6. **smoke test**: a real job through Caddy must reach `succeeded`.

  On failure the previous release comes back the same way, without migrations, and is smoke-tested. Exit 1 means
  rolled back; exit 3 means the rollback failed too. It also has `--rollback` and `--status`. A lock stops two deploys
  running at once.
- **`python -m hopper.bootstrap --smoke-key`** creates or rotates the `hopper-smoke` tenant's API key (small limits).
  The first deploy stores it in the server's `.env`. `deploy/smoke.py` uses only the standard library, so it runs
  with the host's own Python.
- **GitHub Actions.**
  - `deploy.yml` deploys main after a green CI run, one deploy at a time, or any sha (or `--rollback`) on demand.
    On the server its SSH key is a forced command that can only pass one word to `deploy.sh`.
  - `drill.yml` (the rollback drill) builds a release with `/readyz`, or the workers, broken on purpose from the
    live image, and deploys it. It passes only if the server rolls back by itself; its log is the recording.
  - In `ci.yml`, a new `delivery` job runs shellcheck, actionlint, a `compose.prod.yaml` validation and the full
    rehearsal. The image is pushed only after it passes.
- **Rehearsal** (`deploy/rehearse.sh`, in CI on a fresh runner, locally in `docker:dind`). With main's image as the
  first release, it does a first deploy, an upgrade, a broken `/readyz` and dead workers (both must roll back on their
  own), and a manual rollback to main's image on the new schema, which is the check that migrations stay backward
  compatible. A prober counts every request through Caddy.
- **Server setup** (`deploy/host-setup.sh`, Ubuntu 24.04): Docker from Docker's apt repository, capped logs and
  `live-restore`, 2 GB swap, SSH with keys only (no passwords, no root), `/opt/hopper` with generated secrets
  (mode 600), and the restricted deploy key, which a rerun with a new key replaces. Tested in a stock `ubuntu:24.04`
  container with a real OpenSSH server:
  - it installed Docker 29.8.2 with Compose v5.6, and a second run changed nothing;
  - after a rerun with a new key, the old key was refused and a personal login key still worked;
  - it refused a private key and a reused login key, and changed nothing when it did.
- **Checked, in an isolated Docker host (`docker:dind`).** Ten full runs passed every check, each with 0 failed
  requests out of about 3,100 sent through Caddy every 100 ms. Numbers from the last run, with the final code (raw
  log in [docs/delivery](delivery/rehearsal-2026-10-04.log)):
  - first deploy 42 s, upgrade 44 s (sent through the forced command, as CI sends it);
  - the broken `/readyz` was caught after 42 s and the old release was serving again at 78 s, while the other API
    replica served throughout;
  - the dead workers were caught only by the smoke test, after three 30 s tries, and rolled back at 3 min 0 s;
  - the manual rollback took 39 s;
  - 0 failed requests out of 3,093, and the whole stack (12 containers) used 854 MiB of memory (817 to 1,085 MiB
    across the last seven runs).
- **Found by the rehearsal and fixed:**
  - Caddy's passive health check marked a restarting replica down for 10 s. When the other replica's turn came
    inside that window, Caddy saw neither.
  - Replicas were stopped without draining, so Caddy kept sending them requests until its next probe.

  Together these cost 2 to 7 failed requests per run. Draining plus the active probe alone took it to 0. The first
  run also failed on a Windows path in the local wrapper.
- **Found in review and fixed:**
  - `ssh host --status` and `ssh host --rollback` failed with "unknown option -- -": OpenSSH reads them as its own
    flags (shown against a real OpenSSH 9.6). The workflows now pass `--` before the host.
  - The runbook allowed SSH only from your IP, which would have blocked GitHub's runners. Port 22 is now open to
    all, with keys only.
  - A dropped SSH connection would kill a deploy halfway, and could fail a healthy release's smoke test and roll it
    back. The deploy now runs detached on the host and keeps its log there. Tested by killing the SSH client
    mid-deploy: the deploy ran to its end.
  - After a failed redeploy of the live tag, `--status` would still name the failed release as live.
  - Old releases' images were never deleted, so the disk would slowly fill.
  - The smoke key was on the command line, readable by any user of the host; it now travels in the environment.
  - A rollback needed GHCR to answer. It now falls back to the image already on the host.
  - A release that changed Grafana's data source would never have applied it: Grafana reads data sources only at
    start. It now restarts when they change.
  - `host-setup.sh` accepted any `DEPLOY_PUBKEY`. It now refuses a private key, and a key that already logs in
    without restrictions (sshd would use that line and ignore the forced command).
- **Found in a review of the whole project and fixed** (code from earlier weeks, shipped with this one):
  - The `http` task's SSRF guard let NAT64 addresses through: `64:ff9b::a9fe:a9fe` is 169.254.169.254, the cloud
    metadata endpoint, on a network that translates it, and Python calls the prefix global. The guard now judges
    the IPv4 address inside, as it already did for IPv4-mapped and 6to4 addresses.
  - The rate limiter's in-process fallback kept every bucket it ever made, and login buckets are per email, so
    during a Redis outage anyone could grow it by trying new emails. Refilled buckets are now forgotten, as in the
    Lua script, and the table is cleared when Redis answers again (ADR-0008).
  - Migrations had no lock timeout. Now that deploys migrate the live database, an `ALTER` stuck behind a lock
    would queue every query on its table and stall the API; it now gives up after 5 s and the deploy stops
    (ADR-0011). A test holds a lock on `jobs` and checks that the migration fails fast.
  - Smaller: one test asserted nothing (`metrics.serve(0)`); five settings were missing from `.env.example`, which
    a test now keeps complete; the app reported version 0.6.0.
- Tests: 425 (up from 362). They cover the smoke script against a fake API (a ready API whose workers never run
  fails), `--smoke-key` against a real database (rotation revokes the old key), and the draining `/readyz`. Also:
  - `deploy.sh`'s decisions against a fake docker: rollback without migrations, exit 3 when the rollback fails too,
    a broken first release, a failed redeploy of the live tag, a rollback while the registry is down, a Grafana
    data source change, pruning, the lock, and the forced command keeping its log;
  - the forced command refusing anything but one word (`;`, `$(...)`, backticks, newlines, spaces);

  and static checks on the production stack:
  - only Caddy publishes ports;
  - app services never build;
  - migrations never run on `up`;
  - config is mounted from the installed copy;
  - images are pinned;
  - secrets are required;
  - rollback skips migrations;
  - the CI key can only name a tag;
  - every `ssh` in the workflows passes `--` before the host.
- Not done yet, because it needs an AWS account: launching the instance, the GitHub secrets, the first real deploy
  and recording the drill. [docs/deploy.md](deploy.md) walks through each step.

### Before Week 8
- **Retention** (found in a full check after Week 7). The guide's schema rules call for a loop that deletes finished
  jobs after 7 days in batches of 1,000, which also frees their idempotency keys. It was never built, so `jobs`,
  `job_attempts` and the keys would have grown forever, and a comment in `queue/jobs.py` already relied on it. Each
  scheduler now runs it every hour (`src/hopper/scheduler/retention.py`): one `SKIP LOCKED` statement deletes up to
  1,000 `succeeded` or `cancelled` jobs that finished more than 7 days ago, their attempt rows go with them, and a
  pass repeats until a batch comes back short. Dead jobs are never deleted: they are the DLQ. No migration: with no
  index on `finished_at` a pass may scan the table, which on 200,000 jobs took 8 ms for a full batch and 37 ms when
  there was nothing to delete.
- A comment in `alerts_test.yml` pointed to `make rules`; the target is `make alerts`.
- Tests: 434 (up from 425). Old succeeded and cancelled jobs are deleted with their attempts, while queued, running,
  dead and recent jobs stay (even a dead job carrying an old finish time). A deleted job's idempotency key can be used
  again. Also: batches, five concurrent passes deleting 300 jobs once each, a stop ending a long pass after the
  current batch, the hourly loop stopping at once, and `RETENTION_DAYS=0` refused. Three bugs were put in on purpose,
  and a test failed for each.
- New settings: `RETENTION_DAYS` (7, at least 1), `RETENTION_INTERVAL_SECONDS` (3600), `RETENTION_BATCH_SIZE` (1,000).

### Week 8
- **Live on EC2.** The server from [docs/deploy.md](deploy.md) runs in ap-southeast-2 on a c7i-flex.large
  (2 vCPUs, 4 GiB). Merging Week 8 deployed it in 53 s, smoke test included, and the release before it is kept for
  rollback. The rollback drill (`drill.yml`) passed on the real server: its broken release was rolled back by itself.
- **Chaos test** (`chaos/kill_workers.py`, `make chaos`). 10,000 `effect` jobs go in through the API over 5 minutes
  while a random worker is killed with SIGKILL every 3 to 10 s, plus one SIGTERM, one worker frozen 40 s past its
  30 s lease, and one Redis restart. The `effect` task writes each run to `job_executions` and its side effect, once
  per job, to `job_effects` (migration `0004`), so SQL tells lost jobs, duplicate runs and repeated effects apart.

  | Run | SIGKILLs | Jobs cut off mid-run | Lost | Duplicate runs | Acks refused by fencing | Recovery, median and max |
  |---|---|---|---|---|---|---|
  | Laptop | 36 | 69 | **0** | 8 | 5 | 32.3 s, 64.6 s |
  | EC2 server (`chaos/on-server.sh`) | 38 | 102 | **0** | 9 | 1 | 31.7 s, 59.6 s |

  Every job ran its effect exactly once. The duplicates come from kills between a job's effect and its ack, and
  from the frozen worker finishing jobs that had meanwhile run elsewhere, whose acks the fencing token refused.
  Reports: [chaos/results](../chaos/results/).
- **Negative control.** With `CHAOS_ACK_BEFORE_RUN=true` workers ack before running (at-most-once). It can only be
  turned on through `chaos/compose.ack-before-run.yaml`, and a test keeps it out of everything that ships. The same
  test then lost 69 jobs out of 10,000, and the checks caught every one, which shows they can see a loss.
  `nightly-chaos.yml` runs the test every night, and either mode on demand.
- **Load test** (`loadtest/bench.py`, `make bench`). k6 2.3.0 in Docker against two API replicas, three runs per
  scenario, on a laptop on mains power. The script refuses to run on battery or in power-saver mode: a first run on
  battery was thrown away. Medians ([full tables and raw output](../loadtest/results/2026-10-07-Zenbook-Q420VA/summary.md)):
  - A, enqueue: p95 6.9 ms at 100 req/s, 17 ms at 250 and 376 ms at 500; asked for 1,000, the API took about 545.
  - B, drain 100,000 jobs: 956 jobs/s with 1 worker, 1,600 with 2, 2,137 with 4 and 2,773 with 8.
  - C, 300 jobs/s for 10 minutes with 5% `flaky` jobs: API p95 20 ms, queue wait p95 189 ms, enqueue to done p95
    254 ms, no errors.
  - D, breaking point: 400 req/s held in all three runs; at 500 the API accepted 463 to 491.
  - What breaks first: the API. Each replica is one Python process, and both were near 100% CPU while Postgres used
    under one core.
- **Postgres metrics.** `postgres-exporter` joins both stacks, and the dashboard has a Postgres row: connections by
  state, dead tuples on `jobs`, transactions per second and the longest open transaction.
- New tasks: `cpu` (SHA-256 rounds in a thread, for CPU-bound load) and `effect`. `python -m hopper.bootstrap
  --bench-key` rotates the key of the `hopper-bench` tenant, whose limits stay out of the measurement. New setting:
  `CHAOS_ACK_BEFORE_RUN` (the negative control only).
- **Found in a full check after Week 8 and fixed:**
  - The app reported version 0.7.0 while `pyproject.toml` said 0.1.0. It now reads the one version from the package
    (0.8.0), and a test keeps it that way.
  - The negative control's report said its kills cut off no job mid-run, next to a table counting 10: a job is
    briefly running between its claim and the early ack. The wording is fixed and the report re-rendered from its
    raw data.
  - [docs/deploy.md](deploy.md) made the CI deploy key and the host-key file in the repository folder, where
    `git add .` would have committed the private key. They go in `~/.ssh` now, and `.gitignore` refuses key files.
  - Its ssh config for day-to-day commands lacked `IdentitiesOnly yes`: with the deploy key in an ssh agent, ssh
    offers that key first, and its forced command refused every command. `chaos/on-server.sh` had the same problem
    before its first run.
  - CI's shellcheck skipped `chaos/on-server.sh`.
- Tests: 482 (up from 434). They cover the `effect` and `cpu` tasks, the negative control (a real worker process
  killed mid-job loses the job only with the switch on), the bench key, the new tables, the Postgres scrape, and the
  chaos and load-test scripts' verdicts, including the power check.

### Write-up, fixes and the stretch goal
- **Write-up.** [docs/design.md](design.md) covers the architecture and job states, at-least-once versus
  at-most-once, why handlers must be idempotent, what breaks first at 10x, failure modes, security and limits.
  [docs/benchmarks.md](benchmarks.md) has every load, chaos and drill number with its raw data. New ADRs:
  0002 (at-least-once), 0004 (full-jitter backoff), 0005 (the DLQ as a status), 0006 (idempotency keys) and 0012
  (the broker). The README now opens with a crash-recovery GIF, the live links, the headline numbers, the
  architecture diagram and each feature's test. `make demo` (`chaos/demo.py`) runs the one-minute demo one step per
  Enter, and [docs/demo.md](demo.md) is the recording script.
- **Found and fixed:**
  - With Postgres down, every API call answered 500 `internal_error`. It is now 503 `database_unavailable` with
    `Retry-After: 5`; any other database error is still a 500. Checked on the Docker stack: 201, then 503 with
    Postgres stopped, then 201 again once it was back.
  - Deleting a schedule scanned the whole `jobs` table to clear `schedule_id` on its jobs. Migration `0005` adds a
    partial index, built `CONCURRENTLY` so the live release keeps writing to `jobs` meanwhile.
  - The image shipped the releases broken on purpose and the rehearsal scripts; they stay in the repository now.
  - `make fmt` was listed but did not exist.
- **uvloop and httptools for the API**, measured against asyncio and `h11` on the same day and machine. At
  500 req/s the p95 fell from 155 to 55 ms. Asked for 1,000 req/s, the old server collapsed (275 req/s accepted, 7%
  errors) while the new one took 701 req/s with 0.4% errors. The highest rate held stayed at 600 req/s.
  - The jump from the first day's 400 req/s came from moving off Docker Desktop's VM to the native Docker engine,
    and the benchmarks say so.
- **Stretch: the mini broker** (`src/hopper/minibroker`), described above and in ADR-0012:
  - The same 10 contract tests run against both brokers.
  - At 64 connections it did 3,917 durable round trips/s, against Postgres's best of 1,125, and about 11,000
    with fsync once a second.
  - Its chaos run killed 38 workers and the broker itself during 10,000 jobs, and lost none.
  - Killed 9 more times under push load, it lost no answered push in any fsync mode. A process kill cannot show
    what fsync is for (surviving a power cut), and the benchmarks page explains why.
  - Bugs found while building it:
    - the log writer resized its buffer while a view of it was still open, which made every fsynced PUSH hang;
    - the startup check took a connection Docker's port proxy closed for a broker that was down;
    - the first chaos report could not tell refused acks from failed ones.
- Tests: 553 (up from 482). They cover:
  - the 503 path, from the key lookup and after authentication;
  - the index and its plan;
  - the broker's store and log with a fake clock (fencing, retries, death, replay, recovery, a torn record,
    compaction);
  - the broker contract on both brokers;
  - `kill -9` of the broker in each fsync mode and of a worker on it;
  - the demo's refusal to run against anything but this machine;
  - the new scripts' reports.

### The web page
- **A page at the server's root** (at `/hopper/` since PR #16), which used to answer 404. It has three tabs:
  - **About:** what Hopper does, the crash-recovery GIF, the architecture, the database (tables, the claim query,
    the indexes), the life of a job, what happens when each part fails, security, operations, the measured
    numbers (each marked laptop or EC2), the stretch broker, the diagrams, the ADRs and the known limits. Big
    topics stay short on the page and link to the full docs here; big diagrams open on a click.
  - **How to use it:** curl for each step (enqueue, status, delays, cancel, idempotency keys, the DLQ and replay,
    cron), a Python client that retries on 429 and 503 as long as `Retry-After` says, the error table and the
    built-in tasks.
  - **Try it:** paste an API key, enqueue one job or up to 50, and watch them run. Open a job for its payload,
    result and every attempt, cancel it, or replay one dead job or all of them.
- Simple and flat: white space, one blue, thin grey lines. Light and dark themes follow the system, and a
  button in the header switches them; the choice is remembered in the browser.
- Plain HTML, CSS and JavaScript in `src/hopper/web/`, served by the API: no build step and no new server logic.
  The console calls the same `/v1` routes as any client. The key stays in the browser tab (`sessionStorage`) and
  is gone when the tab closes.
- The page sends a strict Content-Security-Policy: only its own script and style, and requests only to this
  server. Everything the API returns is put on the page as text, never as HTML.
- **Twelve UML diagrams** in [docs/diagrams.md](diagrams.md): use cases, components, deployment, classes,
  the database (every table, column, key and index), job states, five sequences (enqueue, run, a worker dying,
  cron with two schedulers, deploy and roll back) and the failure activity. They are Mermaid text, so GitHub
  draws them; `make diagrams` draws the same text as SVG for the page, and a test fails if one is out of date.
- `docs/images` stays the only copy of the images; the Dockerfile copies them into the image.
- Found while writing the examples: the README's `run_at` example was 53 days ahead, over the 30-day limit, so it
  got 422. It is now within the limit.
- Tests: 564 (up from 553). The test suite runs every curl and Python example on the page, as written, against
  a real API process with a worker. Other tests check the policy header, that nothing on the page needs inline
  script, that every file, anchor and GitHub link it names exists, that every number on it matches the README
  or the benchmarks, and that every diagram's SVG was drawn from its current text.

### Hosting Sift, the portfolio home page, and a cleanup (PRs #14 to #19)
- **For Sift's ingestion (#14).** Migration `0006` adds `tenants.internal_hosts`, a catalog-only change the
  previous release never reads, so rollbacks still work. `PUT /admin/tenants/{id}/internal-hosts` (platform admins
  only, host names only) lets a tenant's `http` jobs call those hosts on the Docker network, through a separate
  client that still refuses link-local addresses (the metadata endpoint) and multicast. An `http` payload may set
  `timeout`, up to 120 s; the task's own deadline is 130 s.
- **Sift on the same host (#15).** Postgres is now `pgvector/pgvector:0.8.6-pg16-trixie` in development and
  production, the same build as `postgres:16.15`, so data files, collations and indexes are unchanged. Sift keeps
  its own database on it, and Caddy routes `/sift/` to `sift-api`.
- **A portfolio home page (#16).** Caddy serves `deploy/site` at exactly `/`, and Hopper's page moved to
  `/hopper/`. Every other path still reaches the API, so clients and smoke tests are unchanged. Prometheus scrapes
  `sift-api`, and Grafana has Sift's dashboard.
- **Redesign (#17 to #19).** The home page and Hopper's page were rebuilt from Stitch designs, ending on the
  "Lumio" design. Its markup is compiled with its Tailwind config (`deploy/site-src`) into a static `site.css`,
  because the page's policy allows no other origin. All three pages share one nav bar, and Caddy sends the home
  page the same strict Content-Security-Policy the apps send with theirs.
- **Cleanup** (the `cleanup` branch):
  - Smaller README images: the crash-recovery animation is a WebP (1.66 MB to 0.52 MB), and the architecture
    diagram an SVG.
  - `make up` is one `docker compose up -d --build`: the API, workers and schedulers wait for the `migrate`
    service. `make migrate` runs `uv run --env-file .env alembic upgrade head`.
  - The worker's database engine runs in autocommit, because each claim, ack, nack and heartbeat is one
    statement: 2 round trips per call instead of 6, and 485 to about 270 µs per call (local micro-benchmark).
  - The API's request-context middleware is plain ASGI: `/healthz` in-process went from 534 to 363 µs per
    request (local micro-benchmark).
  - CI's `image` job runs only on pushes to main; on a pull request, `delivery` already builds the image.
  - Caddy sends `Cache-Control: no-cache` with the home page (`/` and `/site/*`), so a deploy shows at once.
  - Mini broker: a failed log write no longer tears the log (the records stay pending and are written again in
    order), and its background loops log an error and carry on instead of stopping.
  - This changelog moved out of the README.
