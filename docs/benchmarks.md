# Benchmarks

Every number on this page was measured. Each one links to the raw files it came from, and the bad numbers are
here too. There are three kinds of test:

- **Load test:** how fast Hopper takes in and runs jobs, and where it breaks.
- **Chaos test:** workers are killed while jobs run; did any job get lost?
- **Rollback drill:** a broken release is deployed on purpose; did the server undo it by itself?

## Headline numbers

| What | Result |
|---|---|
| Enqueue throughput | **400 req/s held** with p95 under 90 ms; the API tops out at about **545 req/s** |
| Drain throughput | **2,773 jobs/s** with 8 workers (100,000 jobs in 36 s) |
| Steady load | 300 jobs/s for 10 minutes: API p95 **20 ms**, enqueue to done p95 **254 ms**, 0 errors |
| Chaos test on the EC2 server | 10,000 jobs, **38 workers killed, 0 lost**, 9 duplicate runs, all harmless |
| Negative control | with ack-before-run (at-most-once) the same test **lost 69 jobs**, and the checks caught all 69 |
| Rollback drill on the EC2 server | broken release caught after **42 s**, old release serving again after **79 s** |
| What breaks first | **the API's CPU**, while Postgres still had room |

## Load test

### Environment

| | |
|---|---|
| Host | Laptop: Intel Core i7-13700H (20 threads), 14.7 GiB RAM, on mains power |
| Docker | Docker Desktop, engine 29.8.2, 20 CPUs and 3.5 GiB memory for containers |
| Versions | Postgres 16.15, Redis 7.4.11, k6 2.3.0 |
| Stack | 2 API replicas (one Python process each, as in production), 3 workers × 20 slots (scenario B: 1, 2, 4 or 8 workers), 2 schedulers |
| Settings | lease 30 s, heartbeat 10 s, poll 250 ms (backs off to 500 ms when idle), DB pool 5 per process, Postgres defaults (`max_connections` 100, `shared_buffers` 128 MB) |
| Load generator | k6 in a container **on the same laptop** as the stack, so the two share the CPU |
| Method | 1-minute warm-up, then every scenario **3 times**. Each cell is the median, with the spread (min to max) in the raw tables |
| Code | commit 385242f, merged to main as d28d7e6 |

The load test ran on the laptop and not on EC2. The server has 2 vCPUs, so a load generator running there would
take CPU away from the stack it measures. The chaos test and the rollback drill did run on the EC2 server.

### Results (median of 3 runs)

| Scenario | Config | Throughput | p50 | p95 | p99 | Errors | First bottleneck |
|---|---|---|---|---|---|---|---|
| A enqueue | 500 req/s target | 498.8 req/s | 13.7 ms | 375.7 ms | 618.8 ms | 0.00% | API CPU (174% of its 200%) |
| B drain | 4 workers | 2,137 jobs/s | n/a | n/a | n/a | 0 | worker CPU (each worker process near 100%) |
| C steady | 300 jobs/s, 5% `flaky` | 300 jobs/s | 5.8 ms | 20.0 ms | 38.7 ms | 0.00% | none reached |
| D break | step 200 → 500 req/s | 400 req/s held | 5.5 ms | 40.1 ms | 105.7 ms | 0.00% | API CPU |

p50, p95 and p99 are the API's response times as k6 saw them. Scenario B has no per-request latency, because it
measures how fast the workers empty a full queue. Scenario D's latencies are at 400 req/s, the highest rate it
held.

### A. Enqueue throughput

k6 sends `POST /v1/jobs` (`sleep` 50 ms) at a fixed rate for 2 minutes.

| Target req/s | Achieved req/s | p50 ms | p95 ms | p99 ms | Errors | API CPU | Postgres CPU |
|---|---|---|---|---|---|---|---|
| 100 | 100.0 | 5.3 | 6.9 | 7.7 | 0.00% | 36% | 12% |
| 250 | 250.0 | 5.7 | 16.9 | 25.6 | 0.00% | 92% | 30% |
| 500 | 498.8 | 13.7 | 375.7 | 618.8 | 0.00% | 174% | 73% |
| 1,000 | **544.7** | 3,521 | 9,882 | 13,989 | 0.01% | **202%** | 88% |

How to read it: CPU is in "% of one core", so 200% is two full cores. The API has two replicas, and each is one
Python process that can use one core at most, so 200% is the API's ceiling. At 1,000 req/s the API was at that
ceiling and took only about 545 req/s, while Postgres used less than one core.

### B. Drain throughput

100,000 `sleep 0 ms` jobs wait in the queue, then N workers start. The timer runs from the first claim to the last
ack, on Postgres's clock.

| Workers | Jobs/s | Drain time | Postgres CPU | Workers CPU (all) |
|---|---|---|---|---|
| 1 | 956 | 104.6 s | 62% | 94% |
| 2 | 1,600 | 62.5 s | 112% | 195% |
| 4 | 2,137 | 46.8 s | 198% | 394% |
| 8 | **2,773** | 36.1 s | 316% | 789% |

How to read it: every worker is one Python process near 100% CPU, so more workers drain faster. Each doubling
gains less than the one before (1.7×, 1.3×, 1.3×), because Postgres has more to do (316% at 8 workers) and every
container shares the same 20 CPU threads. There were 0 lock waits in every run, so `SKIP LOCKED` kept workers out of
each other's way, and connections peaked at 60 of 100.

### C. Steady mix

300 jobs/s for 10 minutes: `sleep` 50 ms, plus 5% `flaky` jobs that fail half the time and retry with backoff.

| Measure | p50 | p95 | p99 |
|---|---|---|---|
| API response time | 5.8 ms | 20.0 ms | 38.7 ms |
| Queue wait (ready → claimed) | 67.1 ms | 189.0 ms | 239.6 ms |
| Run time (`sleep` 50 ms) | 62.9 ms | 79.1 ms | 112.0 ms |
| Enqueue → done | 133.0 ms | 253.8 ms | 306.6 ms |

0 API errors. About 4,500 `flaky` runs failed and were retried; about 280 jobs used up all 5 attempts and went to
the dead-letter queue, as they should. CPU during the run: API 100%, Postgres 43%, workers 40%.

### D. Breaking point

The rate steps up (200, 300, 400, 500 req/s) until the p95 queue wait passes 5 s, errors pass 1%, or the API cannot
accept the rate.

| Run | Highest rate held | At 500 req/s the API accepted | API CPU at 500 | Postgres CPU at 500 | Queue wait p95 at 500 |
|---|---|---|---|---|---|
| 1 | 400 | 491 | 222% | 78% | 0.24 s |
| 2 | 400 | 463 | 202% | 62% | 0.29 s |
| 3 | 400 | 487 | 191% | 74% | 0.29 s |

**First resource to saturate: API CPU.** In all three runs the API was at its two-core ceiling while Postgres used
under one core and the queue wait stayed under 0.3 s. Postgres connections stayed at 35, and there were 0 lock waits.
The workers kept up: there was never a backlog.

### What breaks first at 10x

The build guide predicted Postgres write load would break first. **The measurements say the API breaks first**:

1. **API CPU** (measured). Two Python processes, about 545 req/s in total. The fix is cheap, because the API keeps
   no state: add replicas (or more uvicorn workers per container) on a bigger host.
2. **Postgres CPU and writes** (next, by the numbers). Each job costs about five writes: insert, claim, ack, attempt
   row, and a heartbeat for longer jobs. Postgres used 0.7 cores at 500 req/s and 3.2 cores while 8 workers
   drained, and dead tuples on `jobs` reached about 97,000 during a drain. At 10× load, autovacuum and the claim
   query come next.
3. **Connections.** 60 of 100 with 8 workers. More workers or API replicas need PgBouncer.

The full argument, with fixes for each step, is in the [design doc](design.md#5-scaling-what-breaks-first-at-10x).

### Numbers that look bad

- At 500 req/s the enqueue p95 jumps to 376 ms, from 17 ms at 250. The API is near its CPU limit, so requests
  queue inside it.
- At 1,000 req/s the API accepted only 545 req/s, with a p95 near 10 s. That is a laptop with two API processes,
  not a cluster.
- k6 shared the laptop with the stack, so it took CPU the API could have used. The real ceiling on a dedicated host
  is probably a little higher.
- A first full run on battery power was thrown away. Now `bench.py` refuses to start on battery or in power-saver
  mode.

Full tables, every run: [loadtest/results/2026-10-07-Zenbook-Q420VA/summary.md](../loadtest/results/2026-10-07-Zenbook-Q420VA/summary.md).
k6's raw output: [raw/](../loadtest/results/2026-10-07-Zenbook-Q420VA/raw/).

## Chaos test: kill workers, lose nothing

`chaos/kill_workers.py` enqueues 10,000 `effect` jobs through the API over 5 minutes. Meanwhile, every 3 to 10 s it
kills a random worker with SIGKILL and starts a new one. It also sends one SIGTERM, freezes one worker for 40 s
(longer than its 30 s lease), and restarts Redis once. The `effect` task sleeps 50 to 500 ms, so kills land in the
middle of jobs. It then writes two rows: one per run (`job_executions`) and one per job (`job_effects`, with
`ON CONFLICT DO NOTHING`, an idempotent side effect). Afterwards, SQL checks count lost jobs, duplicate runs and
repeated effects.

| Run | Workers killed | Jobs cut off mid-run | **Lost** | Duplicate runs | Repeated effects | Acks refused by fencing | Recovery (median / max) |
|---|---|---|---|---|---|---|---|
| Laptop | 36 | 69 | **0** | 8 | 0 | 5 | 32.3 s / 64.6 s |
| EC2 server (c7i-flex.large, 2 vCPU, 4 GiB) | 38 | 102 | **0** | 9 | 0 | 1 | 31.7 s / 59.6 s |
| Negative control (ack before run), laptop | 33 | 10 | **69** | 0 | 0 | 0 | 31.3 s / 31.5 s |

- **Normal mode lost nothing.** Every killed job finished on another worker, in about 32 s: the 30 s lease, plus
  up to 5 s for the reaper.
- **Duplicates happen, and they are harmless.** A worker killed after its side effect but before its ack, or the
  frozen worker waking up late, runs a job again. The effect still happened exactly once per job.
- **The negative control proves the test works.** With workers that mark a job done before running it, the same
  test lost 69 jobs, and the checks found every one. A check that can never fail would prove nothing.
- The test also runs every night on GitHub Actions (`nightly-chaos.yml`).

Reports: [EC2 run](../chaos/results/chaos-20261007T174116Z-normal.md),
[laptop run](../chaos/results/chaos-20261007T133538Z-normal.md),
[negative control](../chaos/results/chaos-20261007T134122Z-ack-before-run.md).

## Rollback drill

The `rollback-drill` workflow builds a copy of the live release with `/readyz` broken on purpose and deploys it to
the EC2 server. It passes only if the server rolls back by itself.
([run 37630529905](https://github.com/Srinu-design/hopper/actions/runs/37630529905), 2026-10-07):

| Time | What happened |
|---|---|
| 0 s | deploy of `drill-readyz-37630529905` starts |
| 11 s | migrations done; `api-1` starts on the broken release |
| 42 s | the health check fails: "FAILED its health checks", rolling back (no migrations) |
| 79 s | the old release passes the smoke test (a real job succeeded 0.54 s after enqueue) and is serving again |

The second API replica kept serving through the whole drill. Before the server existed, the same steps were
rehearsed ten times in an isolated Docker host, with 0 failed requests out of about 3,100 per run
([docs/delivery](delivery/rehearsal-2026-10-04.log)).

## Run them yourself

```bash
make load            # scenario A only, about 30 minutes
make bench           # scenarios A to D, three runs each, about two hours
make chaos           # the chaos test, about 7 minutes
make chaos-control   # the negative control: it must lose jobs
```

Each run writes a new folder or report under `loadtest/results/` or `chaos/results/`.
