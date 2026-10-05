# Deploying Hopper to one EC2 host

Every merge to `main` is tested, built into an image on GHCR, and deployed to one EC2 instance by
`.github/workflows/deploy.yml`. On the server, `deploy/deploy.sh` migrates, replaces the two API replicas one at a time
behind Caddy, smoke-tests the release with a real job, and rolls back by itself if anything fails (ADR-0011). This page
covers setting it up once, and running it.

```
GitHub Actions ──ssh (forced command)──▶ /opt/hopper/deploy.sh <sha>
                                              │ pulls ghcr.io/srinu-design/hopper:<sha>
Internet ──80/443──▶ Caddy ─┬─▶ api-1 ┐      │ (public image, carries its own deploy bundle)
                            ├─▶ api-2 ┼─▶ Postgres, Redis
                            └─▶ /grafana/ ─▶ Grafana ─▶ Prometheus ◀── every process :9100
                         workers ×3, schedulers ×2 ─▶ Postgres
```

Only Caddy publishes ports. Postgres, Redis, Prometheus and the metrics ports stay on the Docker network.

## 1. Launch the instance (AWS console)

| Setting | Value |
|---|---|
| AMI | Ubuntu Server 24.04 LTS, **64-bit (x86)**: the image is built for `linux/amd64` |
| Instance type | `t3.medium` (2 vCPU, 4 GB). The whole stack used about 1 GB in the rehearsals, which leaves room for load tests |
| Storage | 30 GB gp3 (images, Postgres, Prometheus' 15 days) |
| Key pair | your own SSH key, for you to log in |
| Security group | SSH 22, HTTP 80 and HTTPS 443 from anywhere; nothing else. SSH must be open to all: GitHub's hosted runners deploy over it and have no fixed addresses (see Security notes) |
| Elastic IP | recommended: the address then survives stop and start (DNS and `known_hosts` keep working) |

Check your account's free-tier or credit terms before you launch, and stop the instance when you are not demoing.

## 2. Set up the host (once)

On your machine, make a new key for CI to deploy with: never your own login key, which `host-setup.sh` refuses
because its unrestricted line would win over the forced command. It gets no passphrase, because a workflow uses
it:

```bash
ssh-keygen -t ed25519 -f hopper-deploy -N "" -C github-deploy
```

Copy the setup files over and run the setup. It installs Docker from Docker's apt repository, adds 2 GB of swap,
turns SSH password and root logins off, creates `/opt/hopper` with `deploy.sh` and an `.env` of generated secrets
(mode 600), and lets the deploy key run `deploy.sh` and nothing else. For HTTPS, set `SITE_ADDRESS` to a domain
whose DNS A record points at the instance, instead of `:80`.

```bash
scp deploy/host-setup.sh deploy/deploy.sh ubuntu@<ip>:
```

```bash
ssh ubuntu@<ip> "sudo DEPLOY_PUBKEY='$(cat hopper-deploy.pub)' SITE_ADDRESS=:80 bash host-setup.sh"
```

Running it again is safe: existing secrets are left alone. Running it with a new `DEPLOY_PUBKEY` replaces the old
deploy key, which is how to rotate it. On the host, the deploy key's line in `~/.ssh/authorized_keys` reads
`restrict,command="/opt/hopper/deploy.sh --from-ssh" ssh-ed25519 ...`.

## 3. Tell GitHub about the host

Repository **Settings → Secrets and variables → Actions**:

| Kind | Name | Value |
|---|---|---|
| Secret | `EC2_SSH_KEY` | the whole private key file `hopper-deploy` |
| Secret | `EC2_KNOWN_HOSTS` | the output of `ssh-keyscan -t ed25519 <host>`, with the same `<host>` as `EC2_HOST` |
| Variable | `EC2_HOST` | the Elastic IP (or DNS name) |
| Variable | `PUBLIC_URL` | `http://<ip>`, or `https://<domain>` |
| Variable | `EC2_USER` | only if not `ubuntu` |

Check the scanned host key before you trust it: `ssh-keygen -lf` of the `ssh-keyscan` output must match the
ED25519 fingerprint the instance printed at first boot (EC2 console: **Actions → Monitor and troubleshoot → Get
system log**, between the `BEGIN SSH HOST KEY FINGERPRINTS` lines). The deploy workflows stay skipped until
`EC2_HOST` is set. The first run creates the `production` environment, where you can add required reviewers if
deploys should wait for approval.

## 4. First deploy

**Actions → deploy → Run workflow**, with the tag set to the full commit sha of main's latest green CI run
(`git rev-parse origin/main`). Images from before Week 7 carry no deploy bundle and cannot be deployed. The log
shows each step with a time stamp. The first deploy also creates the smoke tenant's API key and stores it in
`/opt/hopper/.env`.

Then create the first platform admin (it asks for a password):

```bash
ssh -t ubuntu@<ip> docker exec -it hopper-api-1-1 python -m hopper.bootstrap --email you@example.com
```

Live links: `<PUBLIC_URL>/docs` (API docs) and `<PUBLIC_URL>/grafana/` (dashboard, view only). The Grafana admin
password is `GRAFANA_ADMIN_PASSWORD` in `/opt/hopper/.env`.

From now on, every merge to main deploys by itself once CI is green.

## 5. Record a broken release being rolled back

**Actions → rollback-drill → Run workflow**, breakage `readyz`.

1. The drill builds a release from the image that is live, with `/readyz` broken on purpose
   (`deploy/broken/readyz.Dockerfile`), and pushes it as `drill-readyz-<run id>`.
2. It deploys that release, and the job **passes only if the host rolls it back by itself**, the same release is
   live afterwards, and the public URL still answers.

The job's log is the recording: link it in the README, or screen-record the log page for the demo video. Breakage
`worker` does the same with workers that exit at start, where `/readyz` stays green and only the smoke test catches it.

## Day to day

```bash
ssh ubuntu@<ip> /opt/hopper/deploy.sh --status         # current=<sha> previous=<sha>
ssh ubuntu@<ip> cat /opt/hopper/deploy-history.log     # every deploy, failure and rollback
ssh ubuntu@<ip> ls -t /opt/hopper/logs                 # the full log of each CI deploy (newest 100)
ssh ubuntu@<ip> docker compose -p hopper ps            # containers and health
ssh ubuntu@<ip> docker compose -p hopper logs -f --tail 100 api-1 worker
```

- **Roll back by hand:** **Actions → deploy → Run workflow** with the tag `--rollback`, or
  `ssh ubuntu@<ip> /opt/hopper/deploy.sh --rollback`. This goes back to the previous good release, without
  migrations. It works even while GHCR is down: the previous release's image is still on the host.
- **Deploy an older sha:** the same workflow, with that sha, works only while no later release has added a
  migration: an older image cannot migrate a schema newer than its own, so `deploy.sh` stops with exit 2 before
  changing anything. To go back one release, use `--rollback`, which skips migrations.
- **Change a setting:** edit `/opt/hopper/.env` (keep it mode 600), then redeploy the live sha. `--status` shows it.
- **Switch to HTTPS later:** point a domain's DNS A record at the instance, set `SITE_ADDRESS=<domain>` and
  `PUBLIC_URL=https://<domain>` in `/opt/hopper/.env`, redeploy the live sha, and update the `PUBLIC_URL` variable in
  GitHub. Rerunning `host-setup.sh` does not change an existing `.env`.
- **If the SSH connection drops during a deploy,** the workflow fails, but the deploy carries on to the end on the
  host, rolling back if it must. Its log is in `/opt/hopper/logs`.
- **Update `deploy.sh` itself:** it is installed once by `host-setup.sh`, not with each release. When a release
  ships a different one, the deploy log says so and gives the `install` command that puts it in place.
- **Smoke test says `enqueue answered 401`:** the key in `.env` no longer matches the database (for example after
  a restore). Delete the `SMOKE_API_KEY` line from `/opt/hopper/.env`; the next deploy creates a new key.
- **Back up Postgres:**

  ```bash
  ssh ubuntu@<ip> "docker exec hopper-postgres-1 pg_dump -U hopper hopper | gzip" > hopper-$(date +%F).sql.gz
  ```

  A nightly copy to S3 is the next step.
- **Stop to save money:** stop the instance. On start, Docker brings every container back. Without an Elastic IP the
  address changes, so update `EC2_HOST`, `PUBLIC_URL` and `EC2_KNOWN_HOSTS`.

## Exit codes of deploy.sh

| Code | Meaning |
|---|---|
| 0 | deployed and smoke-tested |
| 1 | the release failed; the previous one is back and smoke-tested (or there was none to go back to) |
| 2 | bad usage, or a failure before the running release was touched (pull, migrations or the smoke key) |
| 3 | the release failed **and** the rollback failed: look at `docker compose -p hopper ps` now |
| 255 | (in the workflow) `ssh` could not reach the host: check the security group, `EC2_HOST` and `EC2_KNOWN_HOSTS` |

## Rehearse without a server

`deploy/rehearse.sh` plays all of this on a throwaway Docker host:

1. a first deploy;
2. an upgrade, sent through the deploy key's forced command as CI sends it;
3. a release with `/readyz` broken, which must roll back;
4. a release whose workers die, which must roll back;
5. a manual rollback.

Throughout, it counts failed requests through Caddy. CI runs it on every pull request, with main's image as the
first release, so the final rollback also proves the new migrations work with the code in production. Locally:

```bash
bash deploy/rehearse-local.sh            # in a docker:dind container; never touches your dev stack
```

## Security notes

- Secrets live only in `/opt/hopper/.env` (mode 600, generated on the host) and in GitHub's secret store. The image is
  public and holds none.
- SSH is open to the internet because GitHub's hosted runners have no fixed addresses. `host-setup.sh` turns
  password and root logins off, so only keys get in, and the internet's background scanning gets nowhere. The deploy
  key can only send one word to `deploy.sh`, which must be a tag, `--rollback` or `--status`; anything else is
  refused before it runs. To close port 22 altogether, deploy through AWS Systems Manager with GitHub's OIDC
  instead (ADR-0011).
- Grafana is view-only without its admin login. Prometheus is not reachable from outside.
