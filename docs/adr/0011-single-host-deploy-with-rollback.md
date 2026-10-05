# ADR-0011: Single-host Compose deploy with health-check rollback; backward-compatible migrations

Status: Accepted · Date: 2026-10-04

## Context

Hopper runs on one EC2 host: Postgres, Redis, two API replicas, workers, schedulers, Caddy, Prometheus and Grafana,
all in Compose. Every merge to main should reach that host already tested, without a person typing commands. A bad
release must not stay up, and finding out must not depend on someone watching. Migrations only go forward. API
clients retry, but a deploy that drops their requests every time is a deploy people stop doing often.

## Decision

**The image is the release.** CI builds one image per commit to main, tagged with the git sha, and pushes it to GHCR.
The image also carries that release's deploy bundle: `compose.prod.yaml`, the Caddy, Prometheus and Grafana config,
the smoke test and `deploy.sh`. Config therefore always matches its code, and a rollback restores both. The host runs
its own installed copy of `deploy.sh`, so a deploy never depends on the script of the release it judges; the log says
when a release ships a different one.

**`deploy/deploy.sh <sha>` on the host** does, logging each step:

1. Pull the image and unpack its bundle into `releases/<sha>`.
2. Run the migrations with the new image.
3. Install the bundle's config into `config/` and reload Caddy and Prometheus in place (no restart). Grafana rereads
   its dashboards by itself, but reads data sources only at start, so it restarts when those changed.
4. **Drain** `api-1` (its `/readyz` answers 503 `draining` while it keeps serving), wait 5 s for Caddy's 2 s probe to
   take it out of rotation, replace it, wait until healthy; then the same for `api-2`.
5. Replace the workers, schedulers and the rest.
6. **Smoke test** through Caddy: `/readyz`, then a real `sleep` job must reach `succeeded`.

If any of steps 3 to 6 fail, the previous release comes back the same way, **without migrations**, and is smoke tested
too. The script exits 1 (rolled back) or 3 (the rollback failed as well).

**Migrations are expand and contract.** Every migration must work with the previous release's code, because a rollback
never downgrades the schema. CI enforces this. On every pull request the rehearsal first deploys main's image (the
release in production), then upgrades to the new image, which runs its migrations. It ends with a manual rollback that
runs main's image on the new schema, and the smoke test must pass there.

**CI deploys over SSH** after a green CI run on main (`.github/workflows/deploy.yml`, one deploy at a time). On the
host its key is a forced command, `deploy.sh --from-ssh`, so it can only send one word: a tag, `--rollback` or
`--status`. The deploy runs detached on the host and logs to `/opt/hopper/logs`, which the SSH session follows, so a
dropped connection cannot stop a deploy halfway. The `rollback-drill` workflow ships a release broken on purpose and
passes only if the host rolls it back by itself. Its log is the recording of a broken release being rolled back.

## Alternatives considered

- **`docker compose up -d` for everything at once** (the build guide's first version). Simpler, but every deploy takes
  the API down for a few seconds. A release with a broken `/readyz` would take the whole API down until the health
  check gives up and the rollback finishes, about a minute.
- **Blue-green: two full stacks, Caddy switches between them.** Also no downtime, but twice the memory on a 4 GB host,
  and the stacks must share one Postgres and Redis anyway. Rolling the two API replicas gives the same result for
  the API.
- **Kubernetes or ECS.** Rolling updates and health checks come built in, but that is a cluster to run, pay for and
  explain, for one host's worth of load.
- **`git pull` on the host.** Config and code can drift, the host needs the source tree and build tools, and a
  rollback means checking out and building again.
- **Rolling back with `alembic downgrade`.** Downgrades can lose data and are rarely exercised. Old code on a
  compatible schema is simpler and is tested on every pull request.
- **GitHub OIDC into an AWS role plus SSM Run Command.** No SSH key stored in GitHub, which is stronger. It is the next
  step once the SSH path is proven.
- **Pull-based updates (Watchtower and similar).** No health gate and no rollback.

## Consequences

- Rehearsed with `deploy/rehearse.sh` in an isolated Docker host: a first deploy, an upgrade, a release with
  `/readyz` broken, a release whose workers exit at start, and a manual rollback.
  - Ten runs passed. In each, requests sent through Caddy every 100 ms during steps 2 to 5 got **0 failures** out
    of about 3,100 (the last run's raw log is in `docs/delivery/`).
  - An upgrade took 42 to 45 s.
  - The broken `/readyz` was caught 40 to 42 s in, and the old release was serving again at 77 to 79 s, with the
    other API replica serving throughout.
  - The whole stack, 12 containers, used 817 to 1,085 MiB of memory, so a 4 GB instance has room for load.
- The rehearsal found two real problems before any server existed:
  - Caddy's passive health check: a request refused during one replica's restart marked it down for 10 s. If the
    other replica's turn came inside that window, Caddy saw neither as up.
  - Stopping a replica without draining left Caddy sending it requests until its next probe.

  Together they cost 2 to 7 failed requests per run. Draining plus the active probe alone took it to 0.
- A release whose workers die passes every container health check. Only the smoke test catches it, after three
  30 s tries, so jobs wait about 2.5 minutes before the rollback. Nothing is lost (they stay queued), but the wait is
  real.
- Workers are replaced together. Running jobs finish or are released (ADR-0003), so jobs pause for seconds during a
  deploy; none is lost.
- Caddy is the one piece that is not rolled: a deploy that changes Caddy's image or settings (a version bump in a
  release, a new `SITE_ADDRESS` in `.env`) restarts it, and connections are refused while it does. A changed
  Caddyfile alone reloads in place.
- One host means no high availability: if the instance dies, everything is down until it is replaced, and backups are
  not automated yet (`pg_dump` to S3 is the next step).
- The SSH key in GitHub is a credential. The forced command limits what it can do, and the host key is pinned.
- Port 22 is open to the internet, because GitHub's hosted runners have no fixed addresses. Only keys get in
  (`host-setup.sh` turns password and root logins off). OIDC plus SSM (see Alternatives) would close the port.
- The first deploy has nothing to roll back to. A migration that fails stops the deploy before anything else
  changes.
- Migrations run against the live database, so each one has a 5 s `lock_timeout` (`migrations/env.py`). An `ALTER`
  waiting for a lock would queue every later query on its table behind it and stall the API and the workers;
  instead the migration fails, the deploy stops with exit 2, and it can be retried when the database is quieter.
- An older release cannot be deployed afresh once a later one has added a migration: its image cannot migrate a
  schema newer than its own, so `deploy.sh` stops before changing anything. `--rollback`, which skips migrations, is
  the way back.

## When we would revisit

If the queue cannot afford 2.5 minutes without workers on a bad release (then check worker health directly, or roll
workers one at a time); when there is more than one host (then ECS or Kubernetes); when the AWS account allows OIDC
roles (then SSM instead of SSH).
