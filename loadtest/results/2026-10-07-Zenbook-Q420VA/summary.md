# Load test results

Measured, not estimated: every number below comes from the raw files in this directory. Each cell is the median of the runs, with the spread (min-max) in brackets when the runs differ.

## Environment

- Host: 13th Gen Intel(R) Core(TM) i7-13700H, 20 threads, 14.7 GiB RAM
- Os: Linux 7.0.0-38-generic
- Docker: Docker Desktop, engine 29.8.2, 20 CPUs and 3.5 GiB memory for containers
- Stack: docker/compose.yaml plus loadtest/compose.bench.yaml: 2 API replicas (one uvicorn process each, as in production; k6 spreads over both through Docker DNS), 3 workers x 20 slots (B: 1, 2, 4, 8 workers), 2 schedulers, Postgres 16.15, Redis 7.4.11
- Settings: lease 30 s, heartbeat 10 s, poll 0.25 s (idle backoff to 0.5 s), DB pool 5 per process, Postgres defaults (max_connections 100, shared_buffers 128 MB)
- Load generator: grafana/k6:2.3.0, in a container on the stack's network (hopper_default), sharing the host with the stack
- Power: on mains power, performance
- Code: git d8d69d8 + uncommitted changes, which became commit 385242f (merged to main as d28d7e6): the source in the image measured here matches it file for file; started 2026-10-07 16:08:09 UTC

## A. Enqueue throughput

POST /v1/jobs (`sleep` 50 ms) at a constant arrival rate for 2m per run; latency is the API's, as k6 saw it.

| Target req/s | Achieved req/s | p50 ms | p95 ms | p99 ms | Errors | API CPU % | Postgres CPU % |
|---|---|---|---|---|---|---|---|
| 100 | 100.0 | 5.3 (5.1-5.3) | 6.9 (6.5-11.8) | 7.7 (7.3-20.2) | 0.00% | 36 (35-62) | 12 (12-13) |
| 250 | 250.0 | 5.7 (5.6-5.8) | 16.9 (8.1-17.2) | 25.6 (12.7-25.8) | 0.00% | 92 (81-118) | 30 (28-31) |
| 500 | 498.8 (497.0-499.2) | 13.7 (12.3-13.8) | 375.7 (261.7-828.0) | 618.8 (541.6-1,142.7) | 0.00% | 174 (161-193) | 73 (66-78) |
| 1,000 | 544.7 (537.2-548.9) | 3,521.2 (3,477.9-3,536.1) | 9,882.4 (8,929.1-9,944.6) | 13,988.6 (13,501.7-14,268.5) | 0.01 (0.00-0.01)% | 202 (201-204) | 88 (71-117) |

## B. Drain throughput

100,000 `sleep ms=0` jobs inserted while no worker runs, then N workers (20 slots each) start; drain time is from the first claim to the last ack, on Postgres's clock.

| Workers | Jobs/s | Drain time s | Postgres CPU % | Workers CPU % (all) |
|---|---|---|---|---|
| 1 | 956 (912-961) | 104.6 (104.0-109.7) | 62 (26-66) | 94 (31-95) |
| 2 | 1,600 (1,473-1,616) | 62.5 (61.9-67.9) | 112 (101-125) | 195 (192-198) |
| 4 | 2,137 (2,004-2,181) | 46.8 (45.9-49.9) | 198 (69-235) | 394 (117-394) |
| 8 | 2,773 (2,760-3,063) | 36.1 (32.6-36.2) | 316 (118-316) | 789 (246-792) |

## C. Steady mix

300 jobs/s for 10m per run: `sleep` 50 ms, plus 5% `flaky` (fails half the time, then retries with backoff).

| Measure | p50 | p95 | p99 |
|---|---|---|---|
| API latency, ms | 5.8 (5.5-6.1) | 20.0 (19.0-21.5) | 38.7 (37.1-49.3) |
| Queue wait (first attempt), ms | 67.1 (66.5-67.9) | 189.0 (186.7-189.3) | 239.6 (237.9-244.2) |
| Run time (`sleep` 50 ms), ms | 62.9 (62.2-63.0) | 79.1 (77.9-79.3) | 112.0 (110.0-113.0) |
| Enqueue to done (`sleep`), ms | 133.0 (131.8-134.0) | 253.8 (251.3-255.7) | 306.6 (304.9-310.3) |

- Jobs created per second: 300.0; API errors: 0.00%
- Jobs retried (the flaky ones that failed): 4,481 (4,471-4,512); dead after all attempts: 276 (268-303)
- CPU % during the run: API 100 (90-134), Postgres 43 (30-52), workers 40 (35-48)

## D. Breaking point

The rate steps up each step until the p95 queue wait passes 5 s, errors pass 1%, or the API cannot accept the rate. CPU % is per role, summed over its containers (100% = one core); each API replica is one Python process, so about 100% per replica, 200% for both, is the API's ceiling.

### Run 1

| Target req/s | Achieved | API p95 ms | Queue wait p95 s | Backlog | Errors | API CPU | Postgres CPU | Workers CPU (busiest) | PG connections | jobs dead tuples | Result |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 200 | 200 | 16.2 | 0.18 | 0 | 0.00% | 67% | 34% | 21% (11%) | 35 | 17,471 | held |
| 300 | 300 | 8.4 | 0.19 | 0 | 0.00% | 81% | 29% | 27% (15%) | 35 | 30,752 | held |
| 400 | 400 | 40.1 | 0.20 | 0 | 0.00% | 173% | 41% | 52% (21%) | 35 | 42,950 | held |
| 500 | 491 | 1444.7 | 0.24 | 0 | 0.00% | 222% | 78% | 104% (37%) | 35 | 54,603 | API kept up with only 491 of 500 req/s |

### Run 2

| Target req/s | Achieved | API p95 ms | Queue wait p95 s | Backlog | Errors | API CPU | Postgres CPU | Workers CPU (busiest) | PG connections | jobs dead tuples | Result |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 200 | 200 | 6.2 | 0.17 | 0 | 0.00% | 52% | 21% | 20% (7%) | 35 | 1,131 | held |
| 300 | 300 | 20.0 | 0.18 | 0 | 0.00% | 99% | 28% | 32% (13%) | 35 | 2,085 | held |
| 400 | 400 | 86.0 | 0.18 | 0 | 0.00% | 198% | 52% | 52% (19%) | 35 | 3,808 | held |
| 500 | 463 | 3288.6 | 0.29 | 0 | 0.00% | 202% | 62% | 75% (35%) | 35 | 6,040 | API kept up with only 463 of 500 req/s |

### Run 3

| Target req/s | Achieved | API p95 ms | Queue wait p95 s | Backlog | Errors | API CPU | Postgres CPU | Workers CPU (busiest) | PG connections | jobs dead tuples | Result |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 200 | 200 | 23.7 | 0.18 | 0 | 0.00% | 94% | 31% | 25% (9%) | 35 | 4,785 | held |
| 300 | 300 | 12.9 | 0.19 | 0 | 0.00% | 114% | 32% | 38% (16%) | 35 | 8,172 | held |
| 400 | 400 | 20.7 | 0.19 | 0 | 0.00% | 131% | 37% | 46% (17%) | 35 | 11,767 | held |
| 500 | 487 | 1131.1 | 0.29 | 0 | 0.00% | 191% | 74% | 98% (47%) | 35 | 16,381 | API kept up with only 487 of 500 req/s |

Highest rate held: 400 req/s.

## Files

- `results.json`: every run's numbers; `environment.json`: the setup
- `raw/`: k6's own end-of-test summary for every run and step
