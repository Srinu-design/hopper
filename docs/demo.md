# The one-minute demo

How to record the demo video for the top of the README. `chaos/demo.py` runs each step when you press Enter,
so you only need to talk and switch windows.

## Before you record

1. Start the stack and wait about a minute for it to settle:

   ```bash
   make up
   ```

2. Open two windows side by side:
   - **Left:** a terminal with a large font (at least 18 pt), in the repository folder.
   - **Right:** the dashboard at <http://localhost:3000>, time range **Last 5 minutes**, refresh **5s**. It needs
     no login.
3. Have these ready in browser tabs:
   - the README on GitHub (the first screen);
   - a chaos report, for example [chaos/results/chaos-20261007T174116Z-normal.md](../chaos/results/chaos-20261007T174116Z-normal.md);
   - the [rollback drill run](https://github.com/Srinu-design/hopper/actions/runs/37630529905) on GitHub Actions.
4. Do one practice run with `python3 chaos/demo.py --no-pause`, so the images are pulled and nothing is slow the
   first time. Then wait a minute for the dashboard to go quiet.
5. Record at 1080p, with no notifications showing. Add burned-in captions afterwards, because many people watch
   without sound.

## The script

Start the recording, then run `python3 chaos/demo.py` in the terminal.

| Time | On screen | What you say |
|---|---|---|
| 0:00 to 0:08 | The README on GitHub | "Hopper is a multi-tenant job queue with retries, a dead-letter queue and cron, and it survives worker crashes." |
| 0:08 to 0:20 | Press Enter (step 1). The terminal enqueues 5,000 jobs; on the dashboard, **Queue depth** rises | "Five thousand jobs go in through the API, and three workers start draining them. Throughput and latency are live on the dashboard." |
| 0:20 to 0:35 | Press Enter (step 2). Two workers are killed; red lines appear on the dashboard; press Enter (step 3) | "I kill two workers in the middle of their jobs. Their leases expire after 30 seconds, and the other workers finish those jobs. The queue keeps draining." |
| 0:35 to 0:45 | The terminal prints "5,000 jobs, 2 workers killed, 0 lost". Then show the chaos report tab | "Zero lost. The full chaos test on the EC2 server killed 38 workers during 10,000 jobs and lost none." |
| 0:45 to 0:53 | Press Enter (step 4), then Enter (step 5): the DLQ list and replay, then 201 five times and 429 with `Retry-After` | "Jobs that keep failing land in the dead-letter queue and replay in one call. A noisy tenant gets a 429 with Retry-After, not a timeout." |
| 0:53 to 1:00 | Press Enter (step 6). Show the rollback drill tab | "Every merge deploys to EC2 by itself, and a release broken on purpose rolled itself back in 79 seconds." |

Step 3 waits until the queue has drained (about 30 to 40 s after the kills). Cut that wait out of the video, or
talk over the dashboard while it happens.

## After recording

1. Upload the video to YouTube as **Unlisted** (or to Loom).
2. Put the link at the top of the README, under the first paragraph:

   ```markdown
   **[▶ Watch the one-minute demo](https://youtu.be/<your video id>)**
   ```

The GIF at the top of the README (`docs/images/crash-recovery.gif`) was made from the dashboard during this same
demo: two workers killed during 5,000 jobs, and 0 lost.
