# Load test results

Measured, not estimated: every number below comes from the raw files in this directory. Each cell is the median of the runs, with the spread (min-max) in brackets when the runs differ.

## Environment

- Host: 13th Gen Intel(R) Core(TM) i7-13700H, 20 threads, 14.7 GiB RAM
- Os: Linux 7.0.0-38-generic
- Docker: Ubuntu 26.04.1 LTS, engine 29.8.2, 20 CPUs and 14.7 GiB memory for containers
- Stack: docker/compose.yaml plus loadtest/compose.bench.yaml: 2 API replicas (one uvicorn process each, as in production; k6 spreads over both through Docker DNS), 3 workers x 20 slots (B: 1, 2, 4, 8 workers), 2 schedulers, Postgres 16.15, Redis 7.4.11
- Settings: lease 30 s, heartbeat 10 s, poll 0.25 s (idle backoff to 0.5 s), DB pool 5 per process, Postgres defaults (max_connections 100, shared_buffers 128 MB)
- Load generator: grafana/k6:2.3.0, in a container on the stack's network (hopper_default), sharing the host with the stack
- Power: on mains power, balanced
- Code: git 17c37b4 + uncommitted changes, which were the mini broker (`src/hopper/minibroker`, its tests and scripts, and a data folder for it in the Dockerfile), committed as 8365363; the API never imports any of it; the API ran on asyncio and h11 through `compose.asyncio-h11.yaml` in this directory (passed to bench.py with --compose), the only difference from the uvloop run; started 2026-10-08 14:00:48 UTC

## A. Enqueue throughput

POST /v1/jobs (`sleep` 50 ms) at a constant arrival rate for 2m per run; latency is the API's, as k6 saw it.

| Target req/s | Achieved req/s | p50 ms | p95 ms | p99 ms | Errors | API CPU % | Postgres CPU % |
|---|---|---|---|---|---|---|---|
| 500 | 499.8 (499.8-499.9) | 22.3 (16.8-22.6) | 154.5 (86.1-164.7) | 239.8 (145.7-250.7) | 0.00% | 178 (165-196) | 69 (65-73) |
| 1,000 | 275.4 (252.6-314.1) | 2,488.8 (2,402.5-3,267.2) | 32,136.0 (31,588.4-33,640.1) | 32,858.2 (32,433.3-34,535.9) | 7.29 (5.84-15.14)% | 214 (204-240) | 10 (1-28) |

## D. Breaking point

The rate steps up each step until the p95 queue wait passes 5 s, errors pass 1%, or the API cannot accept the rate. CPU % is per role, summed over its containers (100% = one core); each API replica is one Python process, so about 100% per replica, 200% for both, is the API's ceiling.

### Run 1

| Target req/s | Achieved | API p95 ms | Queue wait p95 s | Backlog | Errors | API CPU | Postgres CPU | Workers CPU (busiest) | PG connections | jobs dead tuples | Result |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 200 | 200 | 29.0 | 0.18 | 0 | 0.00% | 105% | 32% | 32% (14%) | 35 | 13,610 | held |
| 300 | 300 | 36.1 | 0.20 | 0 | 0.00% | 130% | 48% | 60% (28%) | 35 | 21,797 | held |
| 400 | 400 | 51.1 | 0.25 | 0 | 0.00% | 126% | 43% | 49% (19%) | 35 | 30,448 | held |
| 500 | 499 | 98.1 | 0.26 | 0 | 0.00% | 148% | 64% | 68% (28%) | 35 | 43,179 | held |
| 600 | 600 | 184.3 | 0.36 | 0 | 0.00% | 167% | 79% | 100% (37%) | 35 | 51,899 | held |
| 700 | 355 | 31352.4 | 0.41 | 0 | 7.34% | 249% | 1% | 1% (1%) | 35 | 51,612 | errors 7.3% > 1% |

### Run 2

| Target req/s | Achieved | API p95 ms | Queue wait p95 s | Backlog | Errors | API CPU | Postgres CPU | Workers CPU (busiest) | PG connections | jobs dead tuples | Result |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 200 | 200 | 31.8 | 0.18 | 0 | 0.00% | 108% | 36% | 33% (14%) | 35 | 2,271 | held |
| 300 | 300 | 37.5 | 0.21 | 0 | 0.00% | 133% | 36% | 41% (17%) | 35 | 4,683 | held |
| 400 | 400 | 53.4 | 0.23 | 0 | 0.00% | 149% | 46% | 53% (20%) | 35 | 8,256 | held |
| 500 | 500 | 92.3 | 0.29 | 0 | 0.00% | 144% | 54% | 57% (26%) | 35 | 12,597 | held |
| 600 | 600 | 146.4 | 0.30 | 0 | 0.00% | 173% | 84% | 99% (34%) | 35 | 18,316 | held |
| 700 | 435 | 5609.3 | 0.28 | 0 | 2.72% | 189% | 56% | 57% (23%) | 35 | 21,014 | errors 2.7% > 1% |

### Run 3

| Target req/s | Achieved | API p95 ms | Queue wait p95 s | Backlog | Errors | API CPU | Postgres CPU | Workers CPU (busiest) | PG connections | jobs dead tuples | Result |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 200 | 200 | 31.5 | 0.18 | 0 | 0.00% | 120% | 44% | 42% (17%) | 35 | 16,574 | held |
| 300 | 300 | 38.1 | 0.21 | 0 | 0.00% | 132% | 42% | 48% (22%) | 35 | 25,689 | held |
| 400 | 400 | 52.1 | 0.22 | 0 | 0.00% | 151% | 56% | 61% (22%) | 35 | 35,211 | held |
| 500 | 500 | 76.3 | 0.25 | 0 | 0.00% | 187% | 62% | 76% (30%) | 35 | 45,051 | held |
| 600 | 600 | 62.6 | 0.29 | 0 | 0.00% | 165% | 72% | 84% (31%) | 35 | 56,844 | held |
| 700 | 700 | 82.5 | 0.29 | 0 | 0.00% | 168% | 71% | 93% (37%) | 35 | 69,075 | held |
| 800 | 772 | 2095.8 | 0.36 | 0 | 0.00% | 204% | 86% | 127% (45%) | 35 | 78,892 | API kept up with only 772 of 800 req/s |

Highest rate held: 600 (600-700) req/s.

## Files

- `results.json`: every run's numbers; `environment.json`: the setup
- `raw/`: k6's own end-of-test summary for every run and step
