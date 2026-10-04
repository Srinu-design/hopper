# Hopper

[![ci](https://github.com/Srinu-design/hopper/actions/workflows/ci.yml/badge.svg)](https://github.com/Srinu-design/hopper/actions/workflows/ci.yml)

A multi-tenant job queue and scheduler on PostgreSQL: send a job over HTTP; it runs at least once,
retries with backoff, parks in a dead-letter queue if it keeps failing, and can be replayed.

> **Status: Week 4 of 8 (reliability).** Jobs are enqueued over HTTP (optionally with an
> `Idempotency-Key`), claimed by concurrent workers with `FOR UPDATE SKIP LOCKED` under a heartbeated lease,
> retried with full-jitter backoff, and parked in a dead-letter queue you can list and replay. A killed worker's
> job is picked up by another worker within about 35 s, and SIGTERM drains a worker without losing work.
> Scheduling, auth and the rest arrive in the milestones below. Nothing here claims behaviour that is not built
> and tested yet.

The demo video, headline numbers, architecture diagram and live links will go at the top of this file in
Week 8, once there is something measured to show.

## Quickstart

Requirements: Docker with the Compose plugin. For development also [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env
make up                      # Postgres + Redis, migrations, then the API, 3 workers and the scheduler
curl localhost:8000/healthz  # {"status":"ok"}
curl localhost:8000/readyz   # {"status":"ok","checks":{"postgres":"ok","redis":"ok"}}

# enqueue a job, then check on it (use the id from the first response)
curl -X POST localhost:8000/v1/jobs -H 'content-type: application/json' -d '{"task":"sleep","payload":{"ms":100}}'
curl localhost:8000/v1/jobs/<id>   # status, result and attempt history

# safe to retry: the same Idempotency-Key and body return the same job (200, Idempotent-Replayed: true)
curl -X POST localhost:8000/v1/jobs -H 'content-type: application/json' -H 'Idempotency-Key: order-42' -d '{"task":"sleep","payload":{"ms":100}}'

# fill the dead-letter queue, look at it, replay
curl -X POST localhost:8000/v1/jobs -H 'content-type: application/json' -d '{"task":"fail_always","payload":{"permanent":true}}'
curl localhost:8000/v1/dlq                          # ?queue=&task=&since=&limit=&cursor=
curl -X POST localhost:8000/v1/jobs/<id>/replay     # one job, runs now
curl -X POST localhost:8000/v1/dlq/replay -H 'content-type: application/json' -d '{"filter":{"task":"fail_always"},"spread_seconds":60}'

# crash recovery: kill -9 the worker running a job, and another worker finishes it within about 35 s
curl -X POST localhost:8000/v1/jobs -H 'content-type: application/json' -d '{"task":"sleep","payload":{"ms":20000}}'
docker compose -f docker/compose.yaml --env-file .env exec postgres psql -U hopper -c "select id, lease_owner from jobs where status = 'running'"
docker kill <container id>         # the part of lease_owner before the first dash, e.g. 6a07c0d4678c
curl localhost:8000/v1/jobs/<id>   # attempt_history: lease_expired on the dead worker, then succeeded elsewhere

# graceful shutdown: running jobs get up to 25 s, the rest go back to the queue with no attempt used
docker compose -f docker/compose.yaml --env-file .env stop worker
```

Built-in tasks so far: `sleep` (`{"ms": 100}`), `flaky` (`{"p": 0.3}` fails with that probability) and
`fail_always` (`{}` retries until it is dead; `{"permanent": true}` goes to the DLQ at once).
Until API keys arrive in Week 5 every request runs as one configured default tenant, so do not expose
the API publicly yet. Scale workers with `docker compose -f docker/compose.yaml --env-file .env up -d --scale worker=N`.

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
```

Tests use real Postgres and Redis, because mocks cannot prove `SKIP LOCKED` behaviour. Integration tests
create a throwaway database per test, so they never touch your dev data. Host-side URLs use `127.0.0.1`,
not `localhost`: on Windows `localhost` resolves to `::1` first and the published ports are IPv4 only.

## Layout

| Path | Contents |
|---|---|
| `src/hopper/` | `api/` (routers), `auth/`, `ratelimit/`, `queue/` (Broker interface and all queue SQL), `worker/` (run loop, heartbeats, shutdown), `scheduler/` (reaper; cron in Week 5), `tasks/`, `config.py`, `logging.py`, `db.py` |
| `migrations/` | Alembic revisions. Hand-written SQL, backward compatible with the previous release |
| `docker/` | `Dockerfile` (one image, every role) and `compose.yaml` |
| `tests/` | `unit/`, `integration/`, `e2e/` |
| `deploy/`, `loadtest/`, `chaos/` | Reserved for Weeks 6 to 8 |
| `docs/` | `adr/` decision records, `ai-usage.md` |

## Roadmap and changelog

| Week | Milestone | State |
|---|---|---|
| 1 | Skeleton: repo, uv, ruff and mypy, Dockerfile, Compose, Alembic schema, `/healthz`, CI | done |
| 2 | Queue core: enqueue, `SKIP LOCKED` claim, fenced ack, worker loop | done |
| 3 | Failure handling: retries, DLQ and replay, idempotency keys | done |
| 4 | Reliability: leases, heartbeats, reaper, graceful shutdown | done |
| 5 | Scheduling and tenancy: delayed and cron jobs, API keys, JWT, `http` task | |
| 6 | Limits and observability: token bucket, backpressure, metrics, Grafana | |
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

## Docs

- [ADR-0001: PostgreSQL as the job queue](docs/adr/0001-postgres-as-queue.md)
- [ADR-0003: Leases with heartbeats and fencing tokens](docs/adr/0003-leases-heartbeats-fencing.md)
- [AI usage notes](docs/ai-usage.md)
