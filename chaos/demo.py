#!/usr/bin/env python3
"""The one-minute demo (docs/demo.md), one step per Enter, on the local Compose stack.

1. 5,000 jobs go in through the API (`sleep` 500 ms each); the dashboard shows the queue
   fill up and drain.
2. The two busiest workers are killed with SIGKILL in the middle of their jobs, then started
   again. Their leases run out within 30 s, the reaper requeues the jobs, and other workers
   finish them. Each kill is marked on the Grafana dashboard.
3. The tally, from Postgres: every job succeeded, none was lost, and how many were reclaimed.
4. Three jobs that always fail land in the dead-letter queue, and one call replays them.
5. A tenant allowed 1 request/s (burst 5) sends 10 at once: the extra ones get 429 with
   Retry-After.
6. The rollback drill on the EC2 server: a link to its GitHub Actions log.

Start the stack (`make up`), open the dashboard at http://localhost:3000 next to this
terminal, then:

    python3 chaos/demo.py              # waits for Enter before each step
    python3 chaos/demo.py --no-pause   # straight through, about 2 minutes

It kills containers, so it refuses any API that is not on this machine. Standard library only.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import shlex
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from email.message import Message
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from chaos.kill_workers import (  # shared with the chaos test
    DEFAULT_COMPOSE,
    NORMAL_WORKERS,
    ROOT,
    Annotator,
    ChaosError,
    Stack,
    log,
    post_job,
    read_env_file,
)

DRILL_RUN = "https://github.com/Srinu-design/hopper/actions/runs/37630529905"
LOCAL_HOSTS = {"localhost", "127.0.0.1", "::1"}

# A tenant with a small rate limit, for the 429 step: created on first use, and given a
# fresh key on every run (bootstrap's own helper, run inside the API container).
SLOW_TENANT_KEY = (
    "import asyncio; from decimal import Decimal; "
    "from hopper.bootstrap import rotate_tenant_key; "
    "print(asyncio.run(rotate_tenant_key('hopper-demo-slow', rate_per_sec=Decimal(1), "
    "burst=5, max_queue_depth=100, key_name='demo')))"
)


def call(
    base_url: str, key: str, method: str, path: str, body: dict[str, Any] | None = None
) -> tuple[int, Message, Any]:
    """One API call: (status, headers, decoded JSON body). Header names are case-insensitive."""
    request = urllib.request.Request(
        f"{base_url}{path}",
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, response.headers, json.loads(response.read() or b"null")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.headers, json.loads(exc.read() or b"null")


class Demo:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        env = {**read_env_file(ROOT / args.env_file), **os.environ}
        self.compose = shlex.split(args.compose)
        self.stack = Stack(
            self.compose, env.get("POSTGRES_USER", "hopper"), env.get("POSTGRES_DB", "hopper")
        )
        self.annotator = Annotator(args.grafana_url, env.get("GRAFANA_ADMIN_PASSWORD"))
        self.run = f"demo-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
        self.key = ""
        self.killed = 0

    def step(self, title: str) -> None:
        print(f"\n=== {title}", flush=True)
        if self.args.pause:
            input("    press Enter to go ")

    def count(self, where: str) -> int:
        return int(
            self.stack.scalar(
                f"SELECT count(*) FROM jobs WHERE idempotency_key LIKE '{self.run}-%' AND {where}"
            )
        )

    def enqueue(self) -> None:
        self.step(f"1. Enqueue {self.args.jobs:,} jobs across {NORMAL_WORKERS} workers")
        body = json.dumps({"task": "sleep", "payload": {"ms": self.args.job_ms}}).encode()
        started = time.monotonic()
        with concurrent.futures.ThreadPoolExecutor(max_workers=64) as pool:
            statuses = list(
                pool.map(
                    lambda i: post_job(self.args.base_url, self.key, body, f"{self.run}-{i}"),
                    range(self.args.jobs),
                )
            )
        accepted = sum(s in (200, 201) for s in statuses)
        log(f"{accepted:,} accepted in {time.monotonic() - started:.1f} s; watch Queue depth drain")
        if accepted != self.args.jobs:
            raise ChaosError(f"only {accepted} of {self.args.jobs} jobs were accepted")
        time.sleep(3)  # let every worker fill its slots

    def kill_two(self) -> None:
        self.step("2. Kill two workers in the middle of their jobs")
        held = {
            row[0]: int(row[1])
            for row in self.stack.psql(
                "SELECT split_part(lease_owner, '-', 1), count(*) FROM jobs "
                f"WHERE status = 'running' AND idempotency_key LIKE '{self.run}-%' GROUP BY 1"
            )
        }
        workers = sorted(self.stack.running_workers(), key=lambda w: -held.get(w[:12], 0))
        for container in workers[:2]:
            self.stack.kill(container, "KILL")
            self.killed += 1
            jobs = held.get(container[:12], 0)
            self.annotator.mark(
                f"demo: SIGKILL {container[:12]} ({jobs} jobs running)", ["demo", "kill"]
            )
            log(f"killed worker {container[:12]} with {jobs} jobs running on it")
        self.stack.scale_workers(NORMAL_WORKERS, self.compose)
        log("workers started again; the dead ones' leases run out within 30 s, then the reaper")
        log("requeues their jobs (Leases reclaimed panel) and other workers finish them")

    def tally(self) -> None:
        self.step("3. Wait for the queue to drain, then count")
        deadline = time.monotonic() + self.args.drain_timeout
        while left := self.count("status IN ('queued', 'running')"):
            if time.monotonic() > deadline:
                break
            log(f"{left:,} jobs still queued or running")
            time.sleep(5)
        total = self.count("true")
        succeeded = self.count("status = 'succeeded'")
        dead = self.count("status = 'dead'")
        reclaimed = int(
            self.stack.scalar(
                "SELECT count(DISTINCT a.job_id) FROM job_attempts a "
                "JOIN jobs j ON j.id = a.job_id "
                f"WHERE j.idempotency_key LIKE '{self.run}-%' AND a.outcome = 'lease_expired'"
            )
        )
        lost = total - succeeded - dead
        log(
            f"{total:,} jobs, {self.killed} workers killed, {lost} lost; "
            f"{reclaimed} jobs reclaimed from the dead workers and finished elsewhere"
        )
        if lost:
            raise ChaosError(f"{lost} jobs did not finish within {self.args.drain_timeout:.0f} s")

    def dead_letters(self) -> None:
        self.step("4. Jobs that keep failing go to the dead-letter queue; replay them in one call")
        base, key = self.args.base_url, self.key
        body = {"task": "fail_always", "payload": {"permanent": True}}
        ids = [call(base, key, "POST", "/v1/jobs", body)[2]["id"] for _ in range(3)]
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            states = [call(base, key, "GET", f"/v1/jobs/{i}")[2]["status"] for i in ids]
            if all(s == "dead" for s in states):
                break
            time.sleep(0.5)
        query = urllib.parse.urlencode({"task": "fail_always", "limit": 3})
        for job in call(base, key, "GET", f"/v1/dlq?{query}")[2]["jobs"]:
            log(f"GET /v1/dlq: {job['id']}  {job['last_error']}")
        status, _, replayed = call(base, key, "POST", "/v1/dlq/replay", {"ids": ids})
        log(f"POST /v1/dlq/replay: {status}, {len(replayed['job_ids'])} jobs queued again")

    def rate_limit(self) -> None:
        self.step("5. A noisy tenant gets 429 with Retry-After, not a timeout")
        slow = (
            self.stack.run([*self.compose, "exec", "-T", "api", "python", "-c", SLOW_TENANT_KEY])
            .strip()
            .splitlines()[-1]
        )
        body = {"task": "sleep", "payload": {"ms": 1}}
        answers = [call(self.args.base_url, slow, "POST", "/v1/jobs", body) for _ in range(10)]
        for status, headers, _ in answers:
            extra = f"  Retry-After: {headers.get('Retry-After')}" if status == 429 else ""
            log(f"POST /v1/jobs -> {status}{extra}")

    def rollback(self) -> None:
        self.step("6. Every merge deploys to EC2, and a broken release rolled itself back")
        log(f"rollback drill on the EC2 server: {DRILL_RUN}")
        log("caught after 42 s, the old release serving again after 79 s")

    def go(self) -> int:
        try:
            self.key = self.stack.bench_key(self.args.api_service)
            self.stack.scale_workers(NORMAL_WORKERS, self.compose)
            self.annotator.mark(f"{self.run} starts", ["demo"])
            for part in (
                self.enqueue,
                self.kill_two,
                self.tally,
                self.dead_letters,
                self.rate_limit,
                self.rollback,
            ):
                part()
        except ChaosError as exc:
            log(f"demo stopped: {exc}")
            return 1
        finally:
            try:
                self.stack.scale_workers(NORMAL_WORKERS, self.compose)
            except ChaosError as exc:
                log(f"could not restore the workers: {exc}")
        print("\ndone", flush=True)
        return 0


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--no-pause", dest="pause", action="store_false", help="do not wait for Enter")
    p.add_argument("--jobs", type=int, default=5_000)
    p.add_argument("--job-ms", type=int, default=500, help="how long each demo job sleeps")
    p.add_argument("--drain-timeout", type=float, default=300.0)
    p.add_argument("--base-url", default="http://127.0.0.1:8000")
    p.add_argument("--compose", default=DEFAULT_COMPOSE, help="the Compose command prefix")
    p.add_argument("--env-file", default=".env", help="for the Postgres and Grafana logins")
    p.add_argument("--api-service", default="api", help="where to run hopper.bootstrap")
    p.add_argument("--grafana-url", default="http://127.0.0.1:3000")
    args = p.parse_args(argv)
    host = urllib.parse.urlsplit(args.base_url).hostname
    if host not in LOCAL_HOSTS:
        p.error(f"--base-url must be this machine (the demo kills containers), not {host!r}")
    if args.jobs < 1:
        p.error("--jobs must be at least 1")
    return args


def main(argv: list[str] | None = None) -> int:
    return Demo(parse_args(argv)).go()


if __name__ == "__main__":
    sys.exit(main())
