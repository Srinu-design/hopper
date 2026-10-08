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
- Code: git 17c37b4 + uncommitted changes, which were the mini broker (`src/hopper/minibroker`, its tests and scripts, and a data folder for it in the Dockerfile), committed as 8365363; the API never imports any of it; the API ran uvicorn with uvloop and httptools; started 2026-10-08 13:13:59 UTC

## A. Enqueue throughput

POST /v1/jobs (`sleep` 50 ms) at a constant arrival rate for 2m per run; latency is the API's, as k6 saw it.

| Target req/s | Achieved req/s | p50 ms | p95 ms | p99 ms | Errors | API CPU % | Postgres CPU % |
|---|---|---|---|---|---|---|---|
| 100 | 100.0 | 7.2 (7.1-11.5) | 13.7 (9.5-17.7) | 20.1 (13.4-21.5) | 0.00% | 114 (54-138) | 21 (17-28) |
| 250 | 250.0 | 6.2 (6.2-6.4) | 14.2 (11.3-14.7) | 26.1 (17.6-78.6) | 0.00% | 87 (86-110) | 28 (27-29) |
| 500 | 500.0 (499.9-500.0) | 7.8 (7.7-9.6) | 54.9 (50.7-76.9) | 118.8 (100.1-164.6) | 0.00% | 166 (153-214) | 66 (57-74) |
| 1,000 | 701.1 (640.6-731.0) | 2,472.7 (2,298.8-2,674.9) | 7,900.2 (7,803.0-8,317.7) | 12,792.3 (12,388.7-14,375.2) | 0.40 (0.26-1.57)% | 215 (214-216) | 80 (76-90) |

## D. Breaking point

The rate steps up each step until the p95 queue wait passes 5 s, errors pass 1%, or the API cannot accept the rate. CPU % is per role, summed over its containers (100% = one core); each API replica is one Python process, so about 100% per replica, 200% for both, is the API's ceiling.

### Run 1

| Target req/s | Achieved | API p95 ms | Queue wait p95 s | Backlog | Errors | API CPU | Postgres CPU | Workers CPU (busiest) | PG connections | jobs dead tuples | Result |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 200 | 200 | 31.9 | 0.19 | 0 | 0.00% | 127% | 34% | 34% (17%) | 35 | 17,193 | held |
| 300 | 300 | 16.5 | 0.19 | 0 | 0.00% | 94% | 36% | 38% (14%) | 35 | 30,583 | held |
| 400 | 400 | 23.5 | 0.20 | 0 | 0.00% | 110% | 43% | 33% (14%) | 35 | 42,746 | held |
| 500 | 500 | 22.3 | 0.18 | 0 | 0.00% | 125% | 58% | 56% (29%) | 36 | 27 | held |
| 600 | 600 | 41.8 | 0.24 | 0 | 0.00% | 206% | 71% | 87% (34%) | 35 | 2,106 | held |
| 700 | 686 | 804.3 | 0.63 | 0 | 0.00% | 183% | 96% | 119% (41%) | 35 | 4,669 | API kept up with only 686 of 700 req/s |

### Run 2

| Target req/s | Achieved | API p95 ms | Queue wait p95 s | Backlog | Errors | API CPU | Postgres CPU | Workers CPU (busiest) | PG connections | jobs dead tuples | Result |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 200 | 200 | 38.1 | 0.19 | 0 | 0.00% | 110% | 44% | 41% (16%) | 35 | 3,995 | held |
| 300 | 300 | 46.7 | 0.21 | 0 | 0.00% | 118% | 46% | 48% (17%) | 35 | 7,419 | held |
| 400 | 400 | 72.3 | 0.21 | 0 | 0.00% | 223% | 76% | 93% (38%) | 35 | 11,795 | held |
| 500 | 500 | 85.6 | 0.25 | 0 | 0.00% | 147% | 47% | 45% (16%) | 35 | 16,713 | held |
| 600 | 600 | 130.5 | 0.33 | 0 | 0.00% | 147% | 65% | 77% (30%) | 35 | 23,437 | held |
| 700 | 668 | 1463.8 | 0.47 | 0 | 0.00% | 230% | 99% | 132% (49%) | 35 | 29,348 | API kept up with only 668 of 700 req/s |

### Run 3

| Target req/s | Achieved | API p95 ms | Queue wait p95 s | Backlog | Errors | API CPU | Postgres CPU | Workers CPU (busiest) | PG connections | jobs dead tuples | Result |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 200 | 200 | 29.1 | 0.19 | 0 | 0.00% | 83% | 28% | 25% (11%) | 35 | 11,514 | held |
| 300 | 300 | 36.3 | 0.19 | 0 | 0.00% | 167% | 42% | 47% (18%) | 35 | 18,951 | held |
| 400 | 400 | 49.2 | 0.23 | 0 | 0.00% | 132% | 63% | 76% (30%) | 35 | 26,570 | held |
| 500 | 500 | 84.5 | 0.26 | 0 | 0.00% | 147% | 77% | 85% (36%) | 35 | 35,900 | held |
| 600 | 599 | 126.4 | 0.38 | 0 | 0.00% | 166% | 77% | 95% (35%) | 35 | 45,502 | held |
| 700 | 674 | 1604.7 | 0.51 | 0 | 0.00% | 188% | 87% | 109% (45%) | 35 | 55,046 | API kept up with only 674 of 700 req/s |

Highest rate held: 600 req/s.

## Files

- `results.json`: every run's numbers; `environment.json`: the setup
- `raw/`: k6's own end-of-test summary for every run and step
