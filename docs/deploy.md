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

Everything in this section happens in the browser. Your AWS project has one **selected Region**; every resource
below must be created in it. Check it in **AWS Settings → View all projects → Overview → Additional Info →
Region** (for this project it is **Asia Pacific (Sydney), `ap-southeast-2`**).

**a. Open EC2 in the right Region**

1. Open the AWS Management Console for your project.
2. In the Region menu at the top right, choose **Asia Pacific (Sydney) ap-southeast-2**.
3. Type **EC2** in the search bar at the top and open **EC2**.

**b. Launch the instance**: on the EC2 dashboard, click **Launch instance**, then fill the form from the top:

| Field | Choose |
|---|---|
| **Name and tags → Name** | `hopper` |
| **Application and OS Images** | **Quick Start → Ubuntu**, then **Ubuntu Server 24.04 LTS (HVM), SSD Volume Type**, architecture **64-bit (x86)**. The Hopper image is built for `linux/amd64`, so not Arm |
| **Instance type** | **`m7i-flex.large`** (2 vCPU, 8 GiB). On the free plan only types marked *Free tier eligible* can be launched, so `t3.medium` is refused; `c7i-flex.large` (2 vCPU, 4 GiB) also fits the stack. About $0.12 an hour, taken from your credits while it runs |
| **Key pair (login)** | **Create new key pair**: name `hopper-login`, type **ED25519**, format **.pem**, then **Create key pair**. The browser downloads `hopper-login.pem`: this is *your* login key, never the deploy key of step 2 |
| **Network settings** | Keep the default VPC and subnet, **Auto-assign public IP: Enable**, **Create security group**, and tick all three: **Allow SSH traffic from Anywhere (0.0.0.0/0)**, **Allow HTTPS traffic from the internet**, **Allow HTTP traffic from the internet**. The console warns about SSH from anywhere: expected, GitHub's runners deploy over SSH and have no fixed addresses; keys are the only way in (see Security notes) |
| **Configure storage** | **30** GiB, **gp3** (images, Postgres, 15 days of Prometheus) |
| **Advanced details** | Leave as it is |

Click **Launch instance**, then **View all instances**. Wait until **Instance state** says *Running* and **Status
check** says *2/2 checks passed* (two or three minutes).

**c. Give it a fixed address (Elastic IP)**: without one, the address changes every time the instance stops and
starts, and GitHub's settings would have to change with it.

1. Left menu: **Network & Security → Elastic IPs → Allocate Elastic IP address → Allocate**.
2. Select the new address, then **Actions → Associate Elastic IP address**: resource type **Instance**, instance
   **hopper**, then **Associate**.
3. Write the address down. Below, `<ip>` means this address.

**d. Note the host key fingerprint**, to check in step 3 that GitHub talks to *your* server: **Instances** → select
**hopper** → **Actions → Monitor and troubleshoot → Get system log**. Between `BEGIN SSH HOST KEY FINGERPRINTS` and
`END SSH HOST KEY FINGERPRINTS`, copy the line that ends in `(ED25519)`. (If the log is still empty, wait a minute
and reload.)

**e. Log in once** from your machine, to check the key and the security group:

```bash
mv ~/Downloads/hopper-login.pem ~/.ssh/ && chmod 400 ~/.ssh/hopper-login.pem
```

```bash
ssh -i ~/.ssh/hopper-login.pem ubuntu@<ip> 'uname -a'
```

What it costs on the free plan: the instance about $0.12 per running hour, the disk about $2.90 a month and the
Elastic IP about $3.65 a month even while the instance is stopped, all taken from your credits. **Stop the
instance when you are not using it**: **Instances** → select **hopper** → **Instance state → Stop instance**
(**Start instance** brings everything back, Docker restarts every container).

## 2. Set up the host (once)

On your machine, make a new key for CI to deploy with: never your own login key, which `host-setup.sh` refuses
because its unrestricted line would win over the forced command. It gets no passphrase, because a workflow uses
it, and it goes in `~/.ssh`, outside the repository, so it can never be committed:

```bash
ssh-keygen -t ed25519 -f ~/.ssh/hopper-deploy -N "" -C github-deploy
```

Copy the setup files over and run the setup. It installs Docker from Docker's apt repository, adds 2 GB of swap,
turns SSH password and root logins off, creates `/opt/hopper` with `deploy.sh` and an `.env` of generated secrets
(mode 600), and lets the deploy key run `deploy.sh` and nothing else. For HTTPS, set `SITE_ADDRESS` to a domain
whose DNS A record points at the instance, instead of `:80`.

```bash
scp -i ~/.ssh/hopper-login.pem deploy/host-setup.sh deploy/deploy.sh ubuntu@<ip>:
```

```bash
ssh -i ~/.ssh/hopper-login.pem ubuntu@<ip> "sudo DEPLOY_PUBKEY='$(cat ~/.ssh/hopper-deploy.pub)' SITE_ADDRESS=:80 bash host-setup.sh"
```

Running it again is safe: existing secrets are left alone. Running it with a new `DEPLOY_PUBKEY` replaces the old
deploy key, which is how to rotate it. On the host, the deploy key's line in `~/.ssh/authorized_keys` reads
`restrict,command="/opt/hopper/deploy.sh --from-ssh" ssh-ed25519 ...`.

## 3. Tell GitHub about the host

First, on your machine, read the server's host key and check it against the fingerprint from step 1d:

```bash
ssh-keyscan -t ed25519 <ip> > ~/.ssh/hopper-known-hosts
```

```bash
ssh-keygen -lf ~/.ssh/hopper-known-hosts
```

The `SHA256:...` part must be the same as the `(ED25519)` line in the system log. If it is not, stop: something
else answered on that address.

Then on github.com, in the repository: **Settings** tab → left menu **Secrets and variables → Actions**.

- On the **Secrets** tab, **New repository secret**, twice:

  | Name | Secret |
  |---|---|
  | `EC2_SSH_KEY` | the whole private key file (`cat ~/.ssh/hopper-deploy`), from `-----BEGIN` to `-----END ...-----` |
  | `EC2_KNOWN_HOSTS` | the whole file `~/.ssh/hopper-known-hosts` (one line, starting with the address) |

- On the **Variables** tab, **New repository variable**, twice (three if you log in as someone other than
  `ubuntu`):

  | Name | Value |
  |---|---|
  | `EC2_HOST` | `<ip>`, the Elastic IP (or a DNS name for it) |
  | `PUBLIC_URL` | `http://<ip>`, or `https://<domain>` |
  | `EC2_USER` | only if not `ubuntu` |

The deploy workflows stay skipped until `EC2_HOST` is set. The first run creates the `production` environment
(**Settings → Environments**), where you can add required reviewers if deploys should wait for approval.

Once `EC2_HOST` is set, every merge to main deploys. While the instance is stopped those deploys fail (the
workflow cannot reach the host); start the instance before merging, or rerun the failed deploy afterwards.

## 4. First deploy

1. On your machine, `git rev-parse origin/main` prints the full commit sha of main's latest green CI run (images
   from before Week 7 carry no deploy bundle and cannot be deployed).
2. On github.com: **Actions** tab → **deploy** in the left list → **Run workflow** → paste the sha into **Image
   tag** → **Run workflow**.
3. Open the run. The log shows each step with a time stamp; it ends green after a smoke test pushed a real job
   through the new release. The first deploy also creates the smoke tenant's API key and stores it in
   `/opt/hopper/.env`.

Then create the first platform admin (it asks for a password):

```bash
ssh -t -i ~/.ssh/hopper-login.pem ubuntu@<ip> docker exec -it hopper-api-1-1 python -m hopper.bootstrap --email you@example.com
```

Live links: `<PUBLIC_URL>/docs` (API docs) and `<PUBLIC_URL>/grafana/` (dashboard, view only). The Grafana admin
password is `GRAFANA_ADMIN_PASSWORD` in `/opt/hopper/.env`.

From now on, every merge to main deploys by itself once CI is green.

## 5. Record a broken release being rolled back

On github.com: **Actions** tab → **rollback-drill** in the left list → **Run workflow** → breakage **readyz** →
**Run workflow**.

1. The drill builds a release from the image that is live, with `/readyz` broken on purpose
   (`deploy/broken/readyz.Dockerfile`), and pushes it as `drill-readyz-<run id>`.
2. It deploys that release, and the job **passes only if the host rolls it back by itself**, the same release is
   live afterwards, and the public URL still answers.

The job's log is the recording: link it in the README, or screen-record the log page for the demo video. Breakage
`worker` does the same with workers that exit at start, where `/readyz` stays green and only the smoke test catches it.

## Day to day

The commands below log in as `ubuntu@<ip>`. Add this to `~/.ssh/config` once, so that ssh uses your login key for
the server without `-i` every time. `IdentitiesOnly` matters if an ssh agent holds the deploy key: ssh would offer
that key first, the server would take it, and its forced command would refuse every command below.

```
Host <ip>
  User ubuntu
  IdentityFile ~/.ssh/hopper-login.pem
  IdentitiesOnly yes
```

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
