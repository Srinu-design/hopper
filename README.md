# Hopper

[![ci](https://github.com/Srinu-design/hopper/actions/workflows/ci.yml/badge.svg)](https://github.com/Srinu-design/hopper/actions/workflows/ci.yml)

A multi-tenant job queue and scheduler on PostgreSQL: send a job over HTTP; it runs at least once,
retries with backoff, parks in a dead-letter queue if it keeps failing, and can be replayed.

> **Status: Week 1 of 8 (skeleton).** The service boots, migrates and reports health. Enqueue, workers,
> retries, scheduling and the rest arrive in the milestones below. Nothing here claims behaviour that is not
> built and tested yet.

The demo video, headline numbers, architecture diagram and live links will go at the top of this file in
Week 8, once there is something measured to show.

## Quickstart

Requirements: Docker with the Compose plugin. For development also [uv](https://docs.astral.sh/uv/).

```bash
cp .env.example .env
make up                      # Postgres + Redis, migrations, then the API
curl localhost:8000/healthz  # {"status":"ok"}
curl localhost:8000/readyz   # {"status":"ok","checks":{"postgres":"ok","redis":"ok"}}
```

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
| 2 | Queue core: enqueue, `SKIP LOCKED` claim, fenced ack, worker loop | |
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

## Docs

- [ADR-0001: PostgreSQL as the job queue](docs/adr/0001-postgres-as-queue.md)
- [AI usage notes](docs/ai-usage.md)
