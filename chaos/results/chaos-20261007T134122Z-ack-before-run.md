# Chaos run chaos-20261007T134122Z-ack-before-run

**PASS: the negative control lost 69 jobs, and the checks caught every one**

## Setup

- Mode: negative control: ack before the handler runs (at-most-once)
- Host: 13th Gen Intel(R) Core(TM) i7-13700H, 20 threads, 14.7 GiB RAM; the stack and this script ran on the same machine
- Code: git d8d69d8 + uncommitted changes; Compose stack (docker compose -f docker/compose.yaml --env-file .env -f chaos/compose.ack-before-run.yaml)
- Workers: 4 x 20 slots, lease 30 s, heartbeat 10 s, reaper every 5 s
- Jobs: 10,000 `effect` jobs (sleep 50-500 ms, then record the run and the effect), enqueued over 300 s through the API
- Chaos: for 300 s, SIGKILL a random worker every 3-10 s and scale back to 4; 1 SIGTERM round, 1 Redis restart, 1 worker frozen (SIGSTOP) for 40 s, past its 30 s lease
- Started 2026-10-07 13:41:22 UTC, finished 2026-10-07 13:46:40 UTC

## Results

| Measure | Value |
|---|---|
| Jobs enqueued (API accepted) | 10,000 |
| Workers killed with SIGKILL | 33 |
| Jobs cut off mid-run by those kills | 10 |
| **Lost** (not succeeded or dead, or succeeded without its effect) | **69** |
| Succeeded | 10,000 |
| Dead (in the DLQ: visible, so not lost) | 0 |
| Effects recorded (one per job at most) | 9,931 |
| Runs recorded | 9,931 |
| Duplicate runs (runs minus distinct jobs) | 0 |
| Jobs reclaimed after a lease expired | 10 |
| Jobs in flight on the worker sent SIGTERM | 0 |
| Runs released on SIGTERM (no attempt used) | 0 |
| Jobs frozen mid-run on the frozen worker | 0 |
| Acks refused by the fencing token (worker logs) | 0 |
| Failure reports (nacks) refused by the fencing token | 0 |
| Runs cancelled by a heartbeat after the lease was lost | 0 |
| Longest time for a killed job to finish elsewhere | 31.5 s |
| Median time for a killed job to finish elsewhere | 31.3 s |
| Enqueue retries (any answer but 200/201) | 0 |

## The guide's checks

1. Lost jobs, `status NOT IN ('succeeded', 'dead')`: **0**.
2. The idempotent effect happened exactly once per succeeded job: succeeded jobs without an effect **69** (job_effects has job_id as its primary key, so never more than one). Dead jobs whose effect had already happened before their last failure: 0.
3. Re-executions, `count(*) - count(DISTINCT job_id)` over job_executions: **0**. Above 0 is at-least-once delivery at work; check 2 shows the duplicates were harmless.

Where duplicates come from here: a worker killed after its effect but before its ack (a window of milliseconds, so rare), and the frozen worker, which wakes up after its jobs were reclaimed and finished elsewhere, finishes them again, and has its acks refused because the lease token changed. A worker that ends a SIGTERM drain with jobs still running releases them instead; these jobs are short, so they finish within the 25 s grace period.

In this mode a worker marks each job succeeded as soon as it claims it, so a job shows as running only between its claim and that ack: a kill in that moment is counted above as cutting it off, and the reaper gives it to another worker as usual. A kill while a handler runs, after the ack, is invisible to the queue: that job is counted as lost instead, succeeded with no effect.

## Timeline

| t (s) | Event | Worker | This run's jobs in flight when the signal landed |
|---|---|---|---|
| 6.3 | sigkill | 56e338f31e78 | 0 |
| 18.0 | sigkill | 56e338f31e78 | 0 |
| 29.7 | sigkill | f7a71b53fd93 | 0 |
| 37.4 | sigkill | 350c5cdca110 | 0 |
| 48.3 | sigkill | 56e338f31e78 | 2 |
| 53.4 | sigkill | f74244bd98b0 | 0 |
| 62.9 | sigkill | f74244bd98b0 | 0 |
| 70.9 | sigkill | f74244bd98b0 | 0 |
| 79.0 | sigkill | 350c5cdca110 | 0 |
| 88.2 | sigkill | f7a71b53fd93 | 0 |
| 94.4 | sigkill | 350c5cdca110 | 0 |
| 103.5 | sigkill | f7a71b53fd93 | 3 |
| 112.4 | sigkill | 350c5cdca110 | 0 |
| 118.8 | sigkill | 350c5cdca110 | 0 |
| 124.5 | sigterm | f7a71b53fd93 | 0 |
| 131.5 | sigkill | f74244bd98b0 | 0 |
| 139.5 | sigkill | f74244bd98b0 | 0 |
| 150.8 | sigkill | 350c5cdca110 | 0 |
| 157.5 | sigkill | f74244bd98b0 | 0 |
| 163.4 | sigkill | f74244bd98b0 | 0 |
| 173.5 | freeze | f7a71b53fd93 | 0 |
| 176.1 | sigkill | 56e338f31e78 | 0 |
| 183.7 | sigkill | 56e338f31e78 | 5 |
| 193.7 | sigkill | f74244bd98b0 | 0 |
| 203.3 | sigkill | 350c5cdca110 | 0 |
| 213.6 | redis-restart |  |  |
| 213.9 | thaw | f7a71b53fd93 |  |
| 221.6 | sigkill | 56e338f31e78 | 0 |
| 232.2 | sigkill | f7a71b53fd93 | 0 |
| 239.7 | sigkill | f74244bd98b0 | 0 |
| 247.5 | sigkill | f7a71b53fd93 | 0 |
| 255.9 | sigkill | f7a71b53fd93 | 0 |
| 265.9 | sigkill | f7a71b53fd93 | 0 |
| 272.7 | sigkill | f7a71b53fd93 | 0 |
| 279.9 | sigkill | f7a71b53fd93 | 0 |
| 291.6 | sigkill | f74244bd98b0 | 0 |
| 297.6 | sigkill | 56e338f31e78 | 0 |

Raw data: `chaos-20261007T134122Z-ack-before-run.json`. Grafana shows each kill as an annotation on the Hopper dashboard for this time range.
