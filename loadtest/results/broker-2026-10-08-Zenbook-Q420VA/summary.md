# Broker benchmark: mini broker versus Postgres

Measured, not estimated: every number below comes from results.json in this directory. Each cell is the median of the runs, with the spread (min-max) in brackets when they differ. A round trip is push, pull and ack of one message, timed from the push to the ack, by each connection in a loop.

## Environment

- Host: 13th Gen Intel(R) Core(TM) i7-13700H, 20 threads, 14.7 GiB RAM
- Docker: Ubuntu 26.04.1 LTS, 20 CPUs and 14.7 GiB memory for containers
- Everything in containers on one Docker network: the client, the mini broker (its log on a Docker volume) and the stack's Postgres 16.15 (default settings: synchronous_commit on, so each commit waits for its WAL fsync)
- Postgres: the production broker's own SQL, one transaction per step (insert, claim with SKIP LOCKED, fenced ack), on a pool with one connection per client connection
- Mini broker: one TCP connection per client connection, three commands per round trip
- Runs: 3, each 10 s measured after 2 s of warm-up, per connection count
- Power: on mains power, balanced
- Code: git 17c37b4 + uncommitted changes, which are commit 8365363 (the mini broker as measured here); started 2026-10-08 14:36:45 UTC

## Results

| Broker | Connections | Round trips/s | p50 ms | p99 ms |
|---|---|---|---|---|
| Postgres (SKIP LOCKED) | 1 | 117 (100-131) | 8.95 (7.85-10.05) | 16.10 (15.55-17.97) |
| Postgres (SKIP LOCKED) | 8 | 747 (680-798) | 9.66 (8.94-10.61) | 23.43 (23.03-24.76) |
| Postgres (SKIP LOCKED) | 32 | 1,125 (1,105-1,182) | 26.73 (26.61-27.98) | 40.31 (37.00-81.73) |
| Postgres (SKIP LOCKED) | 64 | 825 (767-835) | 75.42 (73.68-82.22) | 129.56 (125.07-143.08) |
| mini, fsync always | 1 | 207 (204-210) | 4.80 (4.79-4.86) | 7.31 (7.24-7.32) |
| mini, fsync always | 8 | 1,058 (968-1,060) | 7.03 (7.00-7.88) | 15.26 (14.73-15.33) |
| mini, fsync always | 32 | 2,602 (2,118-2,651) | 10.67 (10.51-14.59) | 28.02 (26.39-28.44) |
| mini, fsync always | 64 | 3,917 (3,546-4,608) | 15.90 (13.67-16.85) | 31.67 (27.35-37.16) |
| mini, fsync everysec | 1 | 2,254 (1,880-2,342) | 0.25 (0.24-0.29) | 2.58 (2.50-2.71) |
| mini, fsync everysec | 8 | 10,497 (9,341-10,652) | 0.75 (0.74-0.76) | 0.85 (0.84-2.13) |
| mini, fsync everysec | 32 | 10,906 (10,298-11,346) | 2.80 (2.80-2.85) | 6.71 (3.20-7.66) |
| mini, fsync everysec | 64 | 11,130 (10,903-11,264) | 5.59 (5.56-5.74) | 9.19 (7.27-14.02) |
| mini, fsync no | 1 | 2,584 (2,005-2,785) | 0.23 (0.21-0.29) | 2.46 (2.11-2.93) |
| mini, fsync no | 8 | 10,378 (10,368-10,403) | 0.75 (0.75-0.77) | 0.98 (0.85-1.51) |
| mini, fsync no | 32 | 11,136 (10,266-11,233) | 2.84 (2.82-2.86) | 3.33 (3.28-7.63) |
| mini, fsync no | 64 | 11,242 (11,008-11,309) | 5.64 (5.61-5.73) | 6.63 (6.53-7.06) |

Group commit: commands answered per fsync, median over the runs: always 11, everysec 25,870.
