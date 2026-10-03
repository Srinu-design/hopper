# Hopper

[![ci](https://github.com/Srinu-design/hopper/actions/workflows/ci.yml/badge.svg)](https://github.com/Srinu-design/hopper/actions/workflows/ci.yml)

A multi-tenant job queue and scheduler on PostgreSQL: send a job over HTTP; it runs at least once,
retries with backoff, parks in a dead-letter queue if it keeps failing, and can be replayed.

> **Status: Week 2 of 8 (queue core).** Jobs can be enqueued over HTTP, claimed by concurrent workers with
> `FOR UPDATE SKIP LOCKED`, run, and acked. Retries, the dead-letter queue, leases, scheduling, auth and the
> rest arrive in the milestones below. Nothing here claims behaviour that is not built and tested yet.

The demo video, headline numbers, architecture diagram and live links will go at the top of this file in
Week 8, once there is something measured to show.

## Quickstart

Requirements: Docker with the Compose plugin. For development also [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env
make up                      # Postgres + Redis, migrations, then the API and 3 workers
curl localhost:8000/healthz  # {"status":"ok"}
curl localhost:8000/readyz   # {"status":"ok","checks":{"postgres":"ok","redis":"ok"}}

# enqueue a job, then check on it (use the id from the first response)
curl -X POST localhost:8000/v1/jobs -H 'content-type: application/json' -d '{"task":"sleep","payload":{"ms":100}}'
curl localhost:8000/v1/jobs/<id>   # status, result and attempt history
```

Built-in tasks so far: `sleep` (`{"ms": 100}`) and `flaky` (`{"p": 0.3}` fails with that probability).
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
| `src/hopper/` | `api/` (routers), `auth/`, `ratelimit/`, `queue/`, `worker/`, `scheduler/`, `tasks/`, `config.py`, `logging.py`, `db.py` |
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
| 3 | Failure handling: retries, DLQ and replay, idempotency keys | |
| 4 | Reliability: leases, heartbeats, reaper, graceful shutdown | |
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

## Docs

- [ADR-0001: PostgreSQL as the job queue](docs/adr/0001-postgres-as-queue.md)
- [AI usage notes](docs/ai-usage.md)
