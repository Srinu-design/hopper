# Hopper in diagrams

Twelve UML diagrams, from who uses Hopper down to single requests. Each is Mermaid text, so GitHub draws it here,
and `make diagrams` renders the same text to the SVG files in [images/diagrams/](images/diagrams/) that the web page
shows. The words behind each diagram are in [design.md](design.md) and the [ADRs](adr/).

| # | Diagram | Kind | Shows |
|---|---|---|---|
| 1 | [Use cases](#1-use-cases) | Use case | Who uses Hopper, and for what |
| 2 | [Components](#2-components) | Component | The processes, their parts, and what talks to what |
| 3 | [Deployment](#3-deployment) | Deployment | Where each part runs, from GitHub to the EC2 host |
| 4 | [Classes](#4-classes) | Class | The core code: the Broker interface, the worker, tasks, limits, scheduler loops |
| 5 | [Database](#5-database) | Entity–relationship | Every table, column, key and relationship |
| 6 | [Job states](#6-job-states) | State machine | Every status a job can have, and every move between them |
| 7 | [Enqueue a job](#7-enqueue-a-job) | Sequence | One `POST /v1/jobs`: key, rate limit, backpressure, idempotency |
| 8 | [Run a job](#8-run-a-job) | Sequence | Claim, heartbeat, then ack or nack |
| 9 | [A worker dies](#9-a-worker-dies) | Sequence | Lease expiry, the reaper, and the fencing token refusing the late worker |
| 10 | [Cron with two schedulers](#10-cron-with-two-schedulers) | Sequence | Why a tick fires exactly once with no leader |
| 11 | [When a run fails](#11-when-a-run-fails) | Activity | Retry with backoff, or the dead-letter queue |
| 12 | [Deploy and roll back](#12-deploy-and-roll-back) | Sequence | From a merge to a smoke-tested release, or back to the old one |

## 1. Use cases

Tenants' programs call the API with an API key. People log in for a 15-minute token and manage keys. The operator
deploys and watches. Tenant endpoints receive the signed calls that `http` jobs make.

<!-- diagram: use-cases -->
```mermaid
flowchart LR
  client(["Tenant client<br/>a program with an API key"])
  owner(["Tenant owner"])
  admin(["Platform admin"])
  operator(["Operator"])
  endpoint(["Tenant endpoint<br/>an HTTPS URL"])

  subgraph hopper["Hopper"]
    enqueue("Enqueue a job: now, after a delay or at a set time")
    idem("Retry an enqueue safely with an Idempotency-Key")
    status("Check a job, its result and every attempt")
    list("List jobs by status or queue")
    cancel("Cancel a job that is still waiting")
    dlq("List dead jobs and replay them")
    cron("Create, pause and delete cron schedules")
    http("Run an http job: a signed call with retries")
    login("Log in for a 15-minute token")
    keys("Create, list and revoke API keys")
    tenants("Create tenants with their limits and owner")
    deploy("Deploy a release, or roll back")
    watch("Watch the dashboard and alerts")
  end

  client --- enqueue & idem & status & list & cancel & dlq & cron
  enqueue -. "task = http" .-> http
  http --- endpoint
  owner --- login & keys
  admin --- login & keys & tenants
  operator --- deploy & watch
```

## 2. Components

One image runs every role. Caddy is the only public entry. The API, workers and schedulers keep no state of their
own: PostgreSQL holds every job, and Redis holds only rate-limit buckets and a cached queue depth.

<!-- diagram: components -->
```mermaid
flowchart TB
  clients(["Tenant clients and browsers"]) -->|"HTTP + API key"| caddy["Caddy<br/>reverse proxy, TLS, health checks"]
  caddy --> api

  subgraph api["API process x2: FastAPI on uvicorn"]
    direction LR
    mw["Middleware<br/>request id, 256 KB body limit,<br/>HTTP metrics"]
    auth["ApiKeyAuthenticator<br/>HMAC-SHA256 + pepper,<br/>60 s cache"]
    limits["RateLimiter + Backpressure<br/>token bucket, queue quotas"]
    routes["Routers<br/>/v1/jobs, /v1/dlq, /v1/schedules,<br/>/auth, /admin, /healthz, /readyz"]
    web["Web page<br/>/ and /static"]
    mw --> auth --> limits --> routes
    mw --> web
  end

  subgraph worker["Worker process x N"]
    direction LR
    loop["Worker loop<br/>claim, heartbeat,<br/>ack or nack"]
    pgb["PostgresBroker<br/>implements Broker"]
    tasks["Task registry<br/>sleep, flaky, fail_always,<br/>cpu, http"]
    loop --> pgb
    loop --> tasks
  end

  subgraph scheduler["Scheduler process x2"]
    direction LR
    cronl["CronLoop<br/>every 1 s"]
    reaper["Reaper<br/>every 5 s"]
    depth["DepthLoop<br/>every 1 s"]
    retention["Retention<br/>every hour"]
  end

  api -->|"insert, read, replay"| pg[("PostgreSQL 16<br/>the queue and the source of truth")]
  api <-->|"Lua token bucket,<br/>cached depth"| redis[("Redis 7")]
  worker -->|"SKIP LOCKED claim,<br/>fenced writes"| pg
  worker -->|"signed HTTPS,<br/>SSRF guard"| endpoints(["Tenant endpoints"])
  scheduler -->|"fire cron, reap leases,<br/>count depth, delete old jobs"| pg
  scheduler -->|"publish depth"| redis
  obs["Prometheus + Grafana<br/>scrape /metrics on every process,<br/>5 alert rules, read-only dashboard"]
  caddy -->|"/grafana"| obs
  obs -.->|"scrape"| worker
```

## 3. Deployment

A merge to `main` runs CI, pushes an image to GHCR, and asks the EC2 host to deploy it over SSH with a key that can
only run `deploy.sh`. Only Caddy has public ports.

<!-- diagram: deployment -->
```mermaid
flowchart LR
  subgraph github["GitHub"]
    repo["Repository<br/>Srinu-design/hopper"]
    ci["Actions: ci.yml<br/>lint, types, tests,<br/>delivery rehearsal, image"]
    deploywf["Actions: deploy.yml<br/>after green CI on main"]
    ghcr[("GHCR<br/>image per commit sha")]
    repo --> ci --> ghcr
    ci --> deploywf
  end

  internet(["Internet<br/>clients and browsers"])

  subgraph ec2["AWS EC2 instance (Ubuntu, Docker Compose)"]
    direction TB
    script["/opt/hopper/deploy.sh<br/>forced SSH command"]
    files["/opt/hopper<br/>.env (mode 600), releases/, config/"]
    subgraph net["Docker network: only Caddy publishes ports"]
      direction TB
      caddy["caddy<br/>:80 and :443"]
      api1["api-1"]
      api2["api-2"]
      workers["worker x3"]
      schedulers["scheduler x2"]
      postgres[("postgres 16<br/>volume pgdata")]
      redis[("redis 7<br/>volume redisdata")]
      prom["prometheus"]
      graf["grafana"]
    end
    script --> files
    caddy --> api1 & api2
    caddy --> graf
    api1 & api2 & workers & schedulers --> postgres
    api1 & api2 & schedulers --> redis
    prom --> api1 & api2 & workers & schedulers
    graf --> prom
  end

  deploywf -->|"SSH: one word, the tag"| script
  script -->|"docker pull"| ghcr
  internet -->|"HTTP :80"| caddy
```

## 4. Classes

The worker depends only on the `Broker` interface, so the same `Worker` runs on PostgreSQL in production and on the
broker written from scratch (the stretch goal).

<!-- diagram: classes -->
```mermaid
classDiagram
  direction LR

  class Broker {
    <<interface>>
    +claim(queue, worker_id, limit, lease_seconds) list~ClaimedJob~
    +heartbeat(jobs, lease_seconds) set~UUID~
    +ack(job, worker_id, result) bool
    +nack(job, worker_id, error, outcome, delay_seconds, permanent) str
    +release(jobs, worker_id) set~UUID~
  }
  class PostgresBroker {
    -engine AsyncEngine
    +reap(limit) list~ReapedJob~
  }
  class MiniBroker {
    -client Client
    +push(queue, task, payload, tenant_id) UUID
  }
  class ClaimedJob {
    <<frozen dataclass>>
    +id UUID
    +tenant_id UUID
    +queue str
    +task str
    +payload dict
    +attempt int
    +max_attempts int
    +timeout_seconds int
    +lease_token UUID
    +request_id str
  }
  class Worker {
    +worker_id str
    -slots int
    -lease_seconds float
    -heartbeat_interval float
    -shutdown_grace float
    +run()
    +stop()
    +heartbeat()
    +process(job)
  }
  class TaskSpec {
    <<frozen dataclass>>
    +name str
    +handler Handler
    +payload_model type~TaskPayload~
    +max_attempts int
    +timeout_seconds int
    +backoff_base float
    +backoff_cap float
  }
  class TaskPayload {
    <<pydantic model>>
    extra fields forbidden
  }
  class PermanentError
  class RetryableError {
    +retry_after float
  }
  class RateLimiter {
    +take(key, capacity, rate) Decision
  }
  class LocalBuckets {
    +take(key, capacity, rate) Decision
  }
  class Decision {
    +allowed bool
    +remaining int
    +retry_after_ms int
  }
  class Backpressure {
    +check(tenant_id, tenant_limit) Verdict
  }
  class RedisHealth {
    +usable() bool
    +failed(what, exc)
  }
  class ApiKeyAuthenticator {
    +authenticate(presented) Caller
    +evict(prefix)
  }
  class Caller {
    +tenant_id UUID
    +rate_per_sec float
    +burst int
    +max_queue_depth int
  }
  class CronLoop {
    +run()
    +tick() int
  }
  class Reaper {
    +run()
    +reap_once() int
  }
  class DepthLoop {
    +run()
    +tick() Depth
  }
  class Retention {
    +run()
    +delete_once() int
  }

  Broker <|.. PostgresBroker
  Broker <|.. MiniBroker
  Worker --> Broker : claims and acks through
  Broker ..> ClaimedJob : returns
  Worker ..> TaskSpec : looks up by task name
  TaskSpec --> TaskPayload : validates with
  Worker ..> PermanentError : dead at once
  Worker ..> RetryableError : retry after
  RateLimiter --> LocalBuckets : falls back to
  RateLimiter --> RedisHealth
  Backpressure --> RedisHealth
  RateLimiter ..> Decision : returns
  ApiKeyAuthenticator ..> Caller : returns
  Reaper --> PostgresBroker : reap
```

## 5. Database

Six tables hold everything; two more exist only for the chaos test. A job is one row in `jobs`, and every change
to it is one guarded `UPDATE`. A dead job is a row with `status = 'dead'`, not a row in another table, so it keeps
its id and history (ADR-0005).

<!-- diagram: database -->
```mermaid
erDiagram
  tenants ||--o{ api_keys : "authenticates with"
  tenants |o--o{ users : "is owned by"
  tenants ||--o{ schedules : has
  tenants ||--o{ jobs : owns
  schedules |o--o{ jobs : "fires (SET NULL on delete)"
  jobs ||--o{ job_attempts : "records each run in"
  jobs ||--o{ job_executions : "chaos test only"
  jobs ||--o| job_effects : "chaos test only"

  tenants {
    uuid id PK
    text name UK
    numeric rate_per_sec "token bucket refill, default 50"
    integer burst "bucket size, default 100"
    integer max_queue_depth "quota, default 100000"
    bytea signing_secret "signs http task calls"
    timestamptz created_at
  }
  users {
    uuid id PK
    text email UK "lowercase"
    text password_hash "Argon2id"
    text role "owner or platform_admin"
    uuid tenant_id FK "NULL for a platform admin"
    timestamptz created_at
  }
  api_keys {
    uuid id PK
    uuid tenant_id FK
    text prefix UK "finds the key"
    bytea key_hash "HMAC-SHA256 with a pepper"
    text name
    timestamptz created_at
    timestamptz last_used_at
    timestamptz revoked_at
  }
  schedules {
    uuid id PK
    uuid tenant_id FK
    text name "unique per tenant"
    text cron "five fields"
    text timezone "default UTC"
    text queue
    text task
    jsonb payload
    boolean enabled
    timestamptz next_run_at
    timestamptz last_run_at
  }
  jobs {
    uuid id PK
    uuid tenant_id FK
    text queue "default"
    text task
    jsonb payload
    smallint priority "higher runs first"
    text status "queued running succeeded dead cancelled"
    timestamptz run_at "ready from"
    integer attempts
    integer max_attempts "default 5"
    integer timeout_seconds "default 30"
    text lease_owner "worker id"
    uuid lease_token "fencing token, new per claim"
    timestamptz lease_expires_at
    text idempotency_key "unique per tenant"
    bytea request_hash "SHA-256 of the request"
    uuid schedule_id FK
    text request_id
    text last_error
    jsonb result
    integer replay_count
    timestamptz created_at
    timestamptz started_at
    timestamptz finished_at
    timestamptz dead_at
  }
  job_attempts {
    bigserial id PK
    uuid job_id FK
    integer attempt
    text worker_id
    timestamptz started_at
    timestamptz finished_at
    text outcome "succeeded failed timed_out lease_expired"
    text error
  }
  job_executions {
    bigserial id PK
    uuid job_id FK
    integer attempt
    text worker_id
    timestamptz executed_at
  }
  job_effects {
    uuid job_id PK, FK
    timestamptz created_at
  }
```

Indexes, each partial where it can be, so it stays small as finished jobs pile up:

| Index | On | Serves |
|---|---|---|
| `jobs_ready_idx` | `(queue, priority DESC, run_at) WHERE status = 'queued'` | the claim query |
| `jobs_lease_idx` | `(lease_expires_at) WHERE status = 'running'` | the reaper |
| `jobs_dead_idx` | `(tenant_id, queue, dead_at DESC) WHERE status = 'dead'` | listing the DLQ |
| `jobs_tenant_idx` | `(tenant_id, created_at DESC)` | listing a tenant's jobs |
| `jobs_idem_uidx` | unique `(tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL` | idempotency keys |
| `jobs_schedule_idx` | `(schedule_id) WHERE schedule_id IS NOT NULL` | deleting a schedule |
| `schedules_due_idx` | `(next_run_at) WHERE enabled` | the cron loop |
| `job_attempts_job_idx`, `api_keys_tenant_idx` | `(job_id)`, `(tenant_id)` | a job's history, a tenant's keys |

`jobs` is vacuumed after 2% of its rows change instead of the default 20%, because the queue updates rows all the
time. Finished jobs are deleted after 7 days.

## 6. Job states

<!-- diagram: job-states -->
```mermaid
stateDiagram-v2
  [*] --> queued: enqueue (now, delayed, at a time, or cron)
  queued --> running: claim with a lease
  running --> succeeded: ack
  running --> queued: retry after backoff<br/>lease expired (reaper)<br/>released on shutdown
  running --> dead: permanent error<br/>or no attempts left
  dead --> queued: replay (fresh attempts)
  queued --> cancelled: cancel
  succeeded --> [*]: deleted after 7 days
  cancelled --> [*]: deleted after 7 days

  note right of dead
    The dead-letter queue is this status.
    The row keeps its id, payload and history.
  end note
```

While a job is `running`, its worker renews the lease every 10 s (the heartbeat); the job stays `running`.

## 7. Enqueue a job

<!-- diagram: seq-enqueue -->
```mermaid
sequenceDiagram
  autonumber
  participant C as Tenant client
  participant K as Caddy
  participant A as API
  participant R as Redis
  participant P as PostgreSQL

  C->>K: POST /v1/jobs, Authorization: Bearer hop_live_...
  K->>A: forward to api-1 or api-2 (round robin)
  A->>A: request id, body at most 256 KB
  A->>P: look up the key by prefix (cached 60 s)
  A->>A: compare HMAC-SHA256(pepper, secret) in constant time
  alt missing, wrong or revoked key
    A-->>C: 401 unauthorized
  end
  A->>R: take one token from the tenant's enqueue bucket (Lua)
  alt bucket empty
    A-->>C: 429 rate_limited, Retry-After
  end
  A->>R: read the cached queue depth
  alt the tenant is over its quota
    A-->>C: 429 queue_quota_exceeded
  else all tenants together are over the global limit
    A-->>C: 503 overloaded, Retry-After
  end
  A->>A: check the task exists and its payload is valid (else 422)
  A->>P: INSERT job ... ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
  alt a new row
    A-->>C: 201 Created, the job
  else the key exists with the same request hash
    A-->>C: 200, Idempotent-Replayed: true, the existing job
  else the key exists with a different body
    A-->>C: 422 idempotency_key_reused
  end
  Note over A,R: Redis down: limits fall back to buckets inside each API process
  Note over A,P: PostgreSQL down: 503 database_unavailable, Retry-After 5
```

## 8. Run a job

<!-- diagram: seq-run -->
```mermaid
sequenceDiagram
  autonumber
  participant W as Worker
  participant P as PostgreSQL
  participant H as Task handler

  loop while it has free slots
    W->>P: claim up to free-slots jobs: FOR UPDATE SKIP LOCKED
    P-->>W: rows now running, each with a new lease_token, lease 30 s
  end
  W->>H: run(payload) with the task's timeout
  par while the handler runs
    loop every 10 s
      W->>P: extend the lease WHERE lease_token = mine
      P-->>W: the jobs still held
    end
  end
  alt the handler returned a result
    H-->>W: result
    W->>P: status = succeeded WHERE lease_token = mine, add an attempt row
  else it raised, and attempts are left
    H-->>W: error
    W->>P: status = queued, run_at = now + full-jitter backoff
  else permanent error, or the last attempt
    H-->>W: error
    W->>P: status = dead, dead_at = now
  end
  Note over W,P: Every write names the lease token. If the job was taken back, it changes nothing.
```

## 9. A worker dies

At-least-once in one picture: the job runs again on another worker, and the first worker's late answer is
refused because its token is stale.

<!-- diagram: seq-crash -->
```mermaid
sequenceDiagram
  autonumber
  participant W1 as Worker 1
  participant P as PostgreSQL
  participant S as Scheduler (reaper)
  participant W2 as Worker 2

  W1->>P: claim job J
  P-->>W1: J running, token A, lease until t + 30 s
  Note over W1: killed, or paused longer than its lease
  loop every 5 s
    S->>P: expired leases FOR UPDATE SKIP LOCKED
  end
  S->>P: J back to queued (attempt counted, outcome lease_expired)
  W2->>P: claim J
  P-->>W2: J running, token B
  W1->>P: ack J WHERE lease_token = A
  P-->>W1: 0 rows: refused, the run is stale
  W2->>P: ack J WHERE lease_token = B
  P-->>W2: succeeded
  Note over S,P: A job that kills its worker every time dies after max_attempts instead of looping forever
```

## 10. Cron with two schedulers

<!-- diagram: seq-cron -->
```mermaid
sequenceDiagram
  autonumber
  participant S1 as Scheduler 1
  participant S2 as Scheduler 2
  participant P as PostgreSQL

  par every second, both at once
    S1->>P: due schedules FOR UPDATE SKIP LOCKED
    S2->>P: due schedules FOR UPDATE SKIP LOCKED
  end
  P-->>S1: schedule X (locked for S1)
  P-->>S2: nothing: X is locked, so skipped
  S1->>P: insert a job with Idempotency-Key cron:X:tick time
  S1->>P: next_run_at = the next tick after now
  S1->>P: commit both together
  Note over S1,P: Down for an hour: a schedule fires once on return, then jumps to the next future tick
  Note over S1,P: The key is the same for the same tick, so even a retried insert creates one job
```

## 11. When a run fails

<!-- diagram: failure-activity -->
```mermaid
flowchart TD
  start([A handler raises, times out,<br/>or its worker dies]) --> kind{What happened?}
  kind -->|"PermanentError, or<br/>a payload the task rejects"| dead
  kind -->|"timeout"| timed["outcome timed_out"]
  kind -->|"any other error"| failed["outcome failed"]
  kind -->|"lease expired<br/>(the reaper sees it)"| expired["outcome lease_expired"]
  timed --> last{attempts at<br/>max_attempts?}
  failed --> last
  expired --> last
  last -->|yes| dead["status dead<br/>in the dead-letter queue"]
  last -->|no| delay["delay = uniform(0, min(cap, base x 2^(n-1)))<br/>defaults: base 2 s, cap 600 s<br/>at least the handler's Retry-After"]
  delay --> queued["status queued<br/>run_at = now + delay"]
  queued --> claim([claimed again when run_at comes])
  dead --> replay{replayed?}
  replay -->|"POST /v1/jobs/id/replay<br/>or /v1/dlq/replay"| fresh["status queued<br/>attempts reset, replay_count + 1"]
  fresh --> claim
  replay -->|no| keep([kept with its full history])
```

## 12. Deploy and roll back

<!-- diagram: seq-deploy -->
```mermaid
sequenceDiagram
  autonumber
  participant D as Developer
  participant G as GitHub Actions
  participant R as GHCR
  participant H as deploy.sh on EC2
  participant K as Caddy

  D->>G: merge a pull request into main
  G->>G: lint, types, tests, delivery rehearsal
  G->>R: push the image, tagged with the commit sha
  G->>H: SSH with the deploy key: the tag, nothing else
  H->>R: pull the image, unpack its deploy bundle
  H->>H: run migrations (backward compatible)
  H->>H: install the config, reload Caddy and Prometheus
  H->>K: drain api-1 (readyz 503), replace it, wait until healthy
  H->>K: then the same for api-2
  H->>H: replace workers and schedulers
  H->>K: smoke test: /readyz, then a real job must succeed
  alt the smoke test passes
    H-->>G: deployed
  else anything fails from the config step on
    H->>H: start the previous release the same way, no migrations
    H->>K: smoke test the previous release
    H-->>G: rolled back (the drill: caught at 42 s, serving at 79 s)
  end
```
