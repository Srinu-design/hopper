#!/usr/bin/env python3
"""Chaos test: kill workers in the middle of jobs for minutes, then prove no job was lost.

What it does (the build guide's chaos test):

1. Runs the stack's workers at --workers (4).
2. Enqueues --jobs (10,000) `effect` jobs through the API, spread over the kill window so
   that kills keep landing mid-job. Each one sleeps 50 to 500 ms, then records its run in
   job_executions and its side effect in job_effects (once per job).
3. For --duration (300 s), every 3 to 10 s: `docker kill -s KILL` a random worker, then
   scales back to --workers. Once in the run a worker gets SIGTERM instead, and once Redis
   restarts. Each kill is also an annotation on the Grafana dashboard.
   Once, the worker with the most jobs in flight is frozen (SIGSTOP, like a long GC pause)
   for longer than its lease, then thawed: its jobs are reclaimed and finished elsewhere
   meanwhile, and when it wakes up it finishes them again. That is the duplicate run the
   design doc describes; the fencing token refuses its acks, and the effect stays single.
4. Waits until none of the run's jobs is queued or running (--drain-timeout, 600 s).
5. Checks, in SQL: no job lost, every succeeded job's effect happened exactly once, and
   counts the duplicate runs (at-least-once at work). Writes chaos/results/<run>.md and .json.

--mode ack-before-run is the negative control: the same test with workers that ack before
running (chaos/compose.ack-before-run.yaml). It must lose jobs, which proves the checks can
see a loss. The normal workers are put back when the test ends, however it ends.

Standard library only, so it runs wherever docker and python3 do:

    make up                                          # the stack, from this checkout
    python3 chaos/kill_workers.py                    # expects 0 lost; exit 1 otherwise
    python3 chaos/kill_workers.py --mode ack-before-run   # expects loss; exit 1 if none

Exit codes: 0 the run proved what it should, 1 it did not, 2 it could not run.
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import json
import os
import random
import shlex
import statistics
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_COMPOSE = "docker compose -f docker/compose.yaml --env-file .env"
CONTROL_OVERRIDE = "chaos/compose.ack-before-run.yaml"
NORMAL_WORKERS = 3  # docker/compose.yaml's replica count, restored at the end
LEASE_SECONDS = 30  # the workers' lease (Settings.lease_seconds); a freeze must outlast it

# Run inside a worker container: sends a signal to the worker process (python -m
# hopper.worker), not to the container's init, so the container stays "running" and Compose
# leaves it alone. SIGSTOP freezes the process the way a long GC pause would; SIGCONT thaws it.
SIGNAL_WORKER = """
import os, signal, sys
sig = getattr(signal, sys.argv[1])
for pid in filter(str.isdigit, os.listdir("/proc")):
    try:
        argv = open(f"/proc/{pid}/cmdline", "rb").read().split(b"\\0")
    except OSError:
        continue
    if argv[1:3] == [b"-m", b"hopper.worker"]:
        os.kill(int(pid), sig)
"""


class ChaosError(Exception):
    """The test could not run (as opposed to running and failing its checks)."""


def log(message: str) -> None:
    print(f"[{datetime.now(UTC):%H:%M:%S}] {message}", flush=True)


def read_env_file(path: Path) -> dict[str, str]:
    """KEY=VALUE lines of a Compose .env file; comments and blank lines ignored."""
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.split(" #", 1)[0].strip()
    return values


# --- the stack ------------------------------------------------------------------------------


class Stack:
    """Drives the Compose stack: workers, Redis, and SQL through `psql` in the postgres
    container, so no database port has to be published (the server publishes none)."""

    def __init__(self, compose: list[str], db_user: str, db_name: str) -> None:
        self.compose = compose
        self.db_user = db_user
        self.db_name = db_name

    def run(self, args: list[str], *, capture: bool = True, timeout: float = 120) -> str:
        result = subprocess.run(
            args, cwd=ROOT, capture_output=capture, text=True, timeout=timeout, check=False
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip()[-500:]
            raise ChaosError(f"`{shlex.join(args)}` failed ({result.returncode}): {detail}")
        return result.stdout if capture else ""

    def psql(self, sql: str) -> list[list[str]]:
        out = self.run(
            [
                *self.compose,
                "exec",
                "-T",
                "postgres",
                "psql",
                "-U",
                self.db_user,
                "-d",
                self.db_name,
                "-v",
                "ON_ERROR_STOP=1",
                "-At",
                "-F",
                "\t",
                "-c",
                sql,
            ]
        )
        return [line.split("\t") for line in out.splitlines() if line]

    def scalar(self, sql: str) -> str:
        rows = self.psql(sql)
        return rows[0][0] if rows else ""

    def running_workers(self) -> list[str]:
        """Full container ids of the worker containers that are running now."""
        out = self.run([*self.compose, "ps", "-q", "--status", "running", "worker"])
        return [line.strip() for line in out.splitlines() if line.strip()]

    def scale_workers(self, n: int, compose: list[str], *, recreate: bool = False) -> None:
        """Brings the worker count back to n. Killed workers are started again; with
        recreate, workers whose configuration changed are replaced (switching modes)."""
        args = [*compose, "up", "-d", "--no-deps", "--scale", f"worker={n}"]
        if not recreate:
            args.append("--no-recreate")
        self.run([*args, "worker"], timeout=300)

    def kill(self, container: str, signal: str) -> None:
        self.run(["docker", "kill", "-s", signal, container])

    def signal_worker_process(self, container: str, signal: str) -> None:
        self.run(["docker", "exec", container, "python", "-c", SIGNAL_WORKER, signal])

    def worker_log_lines(self, since: datetime, needle: str) -> int:
        """How many worker log lines since `since` contain `needle` (every worker container,
        including ones that were killed and started again)."""
        out = self.run(
            [*self.compose, "logs", "--no-color", "--since", since.isoformat(), "worker"],
            timeout=300,
        )
        return sum(needle in line for line in out.splitlines())

    def restart_redis(self) -> None:
        self.run([*self.compose, "restart", "redis"], timeout=120)

    def bench_key(self, api_service: str) -> str:
        bootstrap = ["python", "-m", "hopper.bootstrap", "--bench-key"]
        out = self.run([*self.compose, "exec", "-T", api_service, *bootstrap])
        return out.strip().splitlines()[-1]


# --- enqueueing -----------------------------------------------------------------------------


@dataclass
class EnqueueStats:
    accepted: int = 0
    retried: int = 0
    failed: int = 0
    statuses: dict[str, int] = field(default_factory=dict)


def post_job(base_url: str, key: str, body: bytes, idempotency_key: str) -> int:
    request = urllib.request.Request(
        f"{base_url}/v1/jobs",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return exc.code
    except (urllib.error.URLError, TimeoutError, ConnectionError):
        return 0


def enqueue_all(
    base_url: str, key: str, run: str, jobs: int, seconds: float, stats: EnqueueStats
) -> None:
    """Enqueues `jobs` effect jobs evenly over `seconds`. Each has an Idempotency-Key, so a
    retry after a timeout or a blip can never create a second job."""
    body = json.dumps({"task": "effect", "payload": {"run": run}}).encode()
    lock = threading.Lock()

    def one(i: int) -> None:
        for attempt in range(6):
            status = post_job(base_url, key, body, f"{run}-{i}")
            with lock:
                stats.statuses[str(status)] = stats.statuses.get(str(status), 0) + 1
                if status in (200, 201):
                    stats.accepted += 1
                    return
                stats.retried += 1
            time.sleep(min(5.0, 0.2 * 2**attempt))
        with lock:
            stats.failed += 1

    rate = jobs / seconds if seconds > 0 else float("inf")
    started = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        for i in range(jobs):
            delay = started + i / rate - time.monotonic()
            if delay > 0:
                time.sleep(delay)
            pool.submit(one, i)


# --- Grafana annotations --------------------------------------------------------------------


class Annotator:
    """Marks each kill on the Hopper dashboard. Best effort: the test never fails over it."""

    def __init__(self, url: str | None, password: str | None) -> None:
        self.url = url.rstrip("/") if url else None
        self.auth = base64.b64encode(f"admin:{password}".encode()).decode() if password else ""
        self.warned = False

    def mark(self, text: str, tags: list[str]) -> None:
        if not self.url or not self.auth:
            return
        body = json.dumps(
            {"dashboardUID": "hopper", "time": int(time.time() * 1000), "tags": tags, "text": text}
        ).encode()
        request = urllib.request.Request(
            f"{self.url}/api/annotations",
            data=body,
            method="POST",
            headers={"Authorization": f"Basic {self.auth}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=3):
                pass
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            if not self.warned:
                log(f"note: Grafana annotations are off ({exc}); the test goes on without them")
                self.warned = True


# --- the run --------------------------------------------------------------------------------


@dataclass
class Event:
    t: float  # seconds since the kill window opened
    db_time: str  # Postgres now() at the event, for comparing with job timestamps
    kind: str  # sigkill | sigterm | redis-restart
    container: str = ""
    in_flight: list[str] = field(default_factory=list)


def run_label(mode: str, now: datetime) -> str:
    return f"chaos-{now:%Y%m%dT%H%M%SZ}-{mode}"


def held_jobs(stack: Stack, run: str, container: str, since: str) -> tuple[str, list[str]]:
    """Postgres's clock now, and this run's jobs still leased to the worker in `container`
    that it claimed after `since`.

    Asked right after the signal has landed: a job still running under a dead (or frozen)
    process's lease is exactly a job cut off mid-run. Asking before the signal would count
    jobs that finish while the signal is on its way, and miss ones claimed meanwhile. A
    restarted container keeps its id, so `since` (its previous kill) leaves out the leases of
    the process killed last time, which are still waiting for the reaper.
    """
    rows = stack.psql(
        "SELECT now(), coalesce(string_agg(id::text, ','), '') FROM jobs "
        f"WHERE status = 'running' AND lease_owner LIKE '{container[:12]}-%' "
        f"AND started_at >= '{since}'::timestamptz "
        f"AND task = 'effect' AND payload->>'run' = '{run}'"
    )
    db_now, ids = rows[0][0], rows[0][1] if len(rows[0]) > 1 else ""
    return db_now, [i for i in ids.split(",") if i]


def busiest_worker(stack: Stack, run: str, workers: list[str]) -> str:
    """The running worker that holds the most of this run's leases right now."""
    rows = stack.psql(
        "SELECT split_part(lease_owner, '-', 1), count(*) FROM jobs "
        f"WHERE status = 'running' AND task = 'effect' AND payload->>'run' = '{run}' "
        "GROUP BY 1"
    )
    held = {row[0]: int(row[1]) for row in rows}
    return max(workers, key=lambda w: held.get(w[:12], 0))


class Window:
    """The kill window: SIGKILLs, one SIGTERM, one freeze and one Redis restart."""

    def __init__(
        self, stack: Stack, args: argparse.Namespace, compose: list[str], run: str, ann: Annotator
    ) -> None:
        self.stack, self.args, self.compose, self.run, self.ann = stack, args, compose, run, ann
        self.events: list[Event] = []
        self.lock = threading.Lock()
        self.frozen: set[str] = set()
        self.errors: list[BaseException] = []
        self.started = time.monotonic()
        self.opened = stack.scalar("SELECT now()")  # the window's start, on Postgres's clock
        self.last_signal: dict[str, str] = {}  # container -> Postgres time of its last kill

    def held(self, container: str) -> tuple[str, list[str]]:
        since = self.last_signal.get(container, self.opened)
        db_now, jobs = held_jobs(self.stack, self.run, container, since)
        self.last_signal[container] = db_now
        return db_now, jobs

    def now(self) -> float:
        return round(time.monotonic() - self.started, 1)

    def record(self, event: Event, note: str, tags: list[str]) -> None:
        with self.lock:
            self.events.append(event)
        self.ann.mark(f"chaos: {note}", ["chaos", *tags])
        log(note)

    def freeze(self, container: str, seconds: float) -> None:
        """SIGSTOP the worker process for `seconds`, then SIGCONT: a pause longer than the
        lease, after which the worker finishes jobs that were already reclaimed."""
        try:
            self.stack.signal_worker_process(container, "SIGSTOP")
            db_now, jobs = self.held(container)
            self.record(
                Event(self.now(), db_now, "freeze", container[:12], jobs),
                f"froze worker {container[:12]}: {len(jobs)} jobs frozen mid-run",
                ["freeze"],
            )
            time.sleep(seconds)
        except BaseException as exc:
            self.errors.append(exc)
        finally:
            try:
                self.stack.signal_worker_process(container, "SIGCONT")
                self.record(
                    Event(self.now(), self.stack.scalar("SELECT now()"), "thaw", container[:12]),
                    f"thawed worker {container[:12]}",
                    ["freeze"],
                )
            except BaseException as exc:
                self.errors.append(exc)
            with self.lock:
                self.frozen.discard(container)

    def go(self) -> list[Event]:
        args, stack, rng = self.args, self.stack, random.Random()
        sigterm_at = args.duration * 0.4 if args.sigterm else None
        freeze_at = args.duration * 0.55 if args.freeze else None
        redis_at = args.duration * 0.7 if args.redis_restart else None
        freezer: threading.Thread | None = None
        while not self.errors:
            time.sleep(rng.uniform(args.min_gap, args.max_gap))
            t = self.now()
            if t >= args.duration:
                break
            if redis_at is not None and t >= redis_at:
                redis_at = None
                db_now = stack.scalar("SELECT now()")
                stack.restart_redis()
                self.record(Event(t, db_now, "redis-restart"), "restarted Redis", ["redis"])
                continue
            with self.lock:
                workers = [w for w in stack.running_workers() if w not in self.frozen]
            if not workers:
                stack.scale_workers(args.workers, self.compose)
                continue
            if freeze_at is not None and t >= freeze_at:
                freeze_at = None
                victim = busiest_worker(stack, self.run, workers)
                with self.lock:
                    self.frozen.add(victim)
                freezer = threading.Thread(
                    target=self.freeze, args=(victim, args.freeze_seconds), daemon=True
                )
                freezer.start()
                continue
            victim = rng.choice(workers)
            kind = "sigkill"
            if sigterm_at is not None and t >= sigterm_at:
                sigterm_at, kind = None, "sigterm"
            stack.kill(victim, "KILL" if kind == "sigkill" else "TERM")
            db_now, jobs = self.held(victim)
            self.record(
                Event(t, db_now, kind, victim[:12], jobs),
                f"{kind} worker {victim[:12]}: {len(jobs)} of this run's jobs "
                + ("cut off mid-run" if kind == "sigkill" else "in flight, left to finish"),
                [kind],
            )
            stack.scale_workers(args.workers, self.compose)
        if freezer is not None:
            freezer.join()
        if self.errors:
            raise ChaosError(f"the kill window stopped: {self.errors[0]}")
        return sorted(self.events, key=lambda e: e.t)


def wait_for_drain(stack: Stack, run: str, expected: int, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        counts = dict(
            (row[0], int(row[1]))
            for row in stack.psql(
                "SELECT status, count(*) FROM jobs "
                f"WHERE task = 'effect' AND payload->>'run' = '{run}' GROUP BY status"
            )
        )
        busy = counts.get("queued", 0) + counts.get("running", 0)
        summary = ", ".join(f"{k} {v}" for k, v in sorted(counts.items()))
        if summary != last:
            log(f"waiting for the queue to drain: {summary}")
            last = summary
        if busy == 0 and sum(counts.values()) >= expected:
            return True
        time.sleep(2)
    return False


CHECKS = """
WITH r AS (SELECT id, status FROM jobs WHERE task = 'effect' AND payload->>'run' = '{run}')
SELECT
  (SELECT count(*) FROM r),
  (SELECT count(*) FROM r WHERE status = 'succeeded'),
  (SELECT count(*) FROM r WHERE status = 'dead'),
  (SELECT count(*) FROM r WHERE status NOT IN ('succeeded', 'dead')),
  (SELECT count(*) FROM job_effects e JOIN r ON r.id = e.job_id),
  (SELECT count(*) FROM r WHERE status = 'succeeded'
     AND NOT EXISTS (SELECT 1 FROM job_effects e WHERE e.job_id = r.id)),
  (SELECT count(*) FROM r WHERE status = 'dead'
     AND EXISTS (SELECT 1 FROM job_effects e WHERE e.job_id = r.id)),
  (SELECT count(*) FROM job_executions x JOIN r ON r.id = x.job_id),
  (SELECT count(DISTINCT x.job_id) FROM job_executions x JOIN r ON r.id = x.job_id),
  (SELECT count(*) FROM job_attempts a JOIN r ON r.id = a.job_id
     WHERE a.outcome = 'lease_expired'),
  (SELECT count(DISTINCT a.job_id) FROM job_attempts a JOIN r ON r.id = a.job_id
     WHERE a.outcome = 'lease_expired'),
  (SELECT count(*) FROM job_attempts a JOIN r ON r.id = a.job_id WHERE a.outcome = 'released')
"""
CHECK_NAMES = [
    "jobs",
    "succeeded",
    "dead",
    "unfinished",
    "effects",
    "succeeded_without_effect",
    "dead_with_effect",
    "executions",
    "executed_jobs",
    "lease_expired_attempts",
    "jobs_reclaimed",
    "released_attempts",
]


def recovery_seconds(stack: Stack, events: list[Event]) -> list[float]:
    """For every job in flight on a SIGKILLed worker: how long until it succeeded elsewhere."""
    seconds: list[float] = []
    for event in events:
        if event.kind != "sigkill" or not event.in_flight:
            continue
        ids = ",".join(f"'{i}'" for i in event.in_flight)
        rows = stack.psql(
            "SELECT extract(epoch FROM finished_at - "
            f"'{event.db_time}'::timestamptz) FROM jobs "
            f"WHERE id IN ({ids}) AND status = 'succeeded'"
        )
        seconds += [float(row[0]) for row in rows]
    return seconds


def summarize(
    checks: dict[str, int],
    events: list[Event],
    recovery: list[float],
    mode: str,
    fenced: dict[str, int],
) -> dict[str, Any]:
    lost = checks["unfinished"] + checks["succeeded_without_effect"]
    kills = [e for e in events if e.kind == "sigkill"]
    summary: dict[str, Any] = {
        "lost": lost,
        "duplicate_runs": checks["executions"] - checks["executed_jobs"],
        "sigkills": len(kills),
        "sigterms": sum(e.kind == "sigterm" for e in events),
        "redis_restarts": sum(e.kind == "redis-restart" for e in events),
        "freezes": sum(e.kind == "freeze" for e in events),
        "jobs_in_flight_at_sigkill": sum(len(e.in_flight) for e in kills),
        "jobs_in_flight_at_sigterm": sum(len(e.in_flight) for e in events if e.kind == "sigterm"),
        "jobs_in_flight_at_freeze": sum(len(e.in_flight) for e in events if e.kind == "freeze"),
        **fenced,
        "recovery_max_s": round(max(recovery), 1) if recovery else None,
        "recovery_median_s": round(statistics.median(recovery), 1) if recovery else None,
    }
    if mode == "normal":
        summary["passed"] = lost == 0 and checks["jobs"] > 0
        summary["verdict"] = "PASS: no job lost" if summary["passed"] else f"FAIL: {lost} jobs lost"
    else:
        summary["passed"] = lost > 0
        summary["verdict"] = (
            f"PASS: the negative control lost {lost} jobs, and the checks caught every one"
            if summary["passed"]
            else "FAIL: acking before running lost nothing, so this run proves nothing"
        )
    return summary


def host_description() -> str:
    cpu = ""
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
        mem_kb = int(Path("/proc/meminfo").read_text().split()[1])
        return f"{cpu}, {os.cpu_count()} threads, {mem_kb / 1024 / 1024:.1f} GiB RAM"
    except (OSError, ValueError, IndexError):
        return f"{os.cpu_count()} threads"


def git_sha() -> str:
    """The commit, marked "+ uncommitted changes" when the working tree differs from it."""
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            cwd=ROOT,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except OSError:
        return "unknown"
    return f"{sha} + uncommitted changes" if dirty else sha


def render_report(data: dict[str, Any]) -> str:
    c, s, a = data["checks"], data["summary"], data["args"]
    mode = (
        "normal: ack after the handler returns (at-least-once)"
        if a["mode"] == "normal"
        else "negative control: ack before the handler runs (at-most-once)"
    )
    lines = [
        f"# Chaos run {data['run']}",
        "",
        f"**{s['verdict']}**",
        "",
        "## Setup",
        "",
        f"- Mode: {mode}",
        f"- Host: {data['host']}; the stack and this script ran on the same machine",
        f"- Code: git {data['git']}; Compose stack ({a['compose']})",
        f"- Workers: {a['workers']} x 20 slots, lease 30 s, heartbeat 10 s, reaper every 5 s",
        f"- Jobs: {a['jobs']:,} `effect` jobs (sleep 50-500 ms, then record the run and the "
        f"effect), enqueued over {a['enqueue_seconds']:.0f} s through the API",
        f"- Chaos: for {a['duration']:.0f} s, SIGKILL a random worker every "
        f"{a['min_gap']:g}-{a['max_gap']:g} s and scale back to {a['workers']}; "
        f"{s['sigterms']} SIGTERM round, {s['redis_restarts']} Redis restart, {s['freezes']} "
        f"worker frozen (SIGSTOP) for {a['freeze_seconds']:.0f} s, past its {LEASE_SECONDS} s "
        "lease",
        f"- Started {data['started']}, finished {data['finished']}",
        "",
        "## Results",
        "",
        "| Measure | Value |",
        "|---|---|",
        f"| Jobs enqueued (API accepted) | {data['enqueue']['accepted']:,} |",
        f"| Workers killed with SIGKILL | {s['sigkills']} |",
        f"| Jobs cut off mid-run by those kills | {s['jobs_in_flight_at_sigkill']} |",
        f"| **Lost** (not succeeded or dead, or succeeded without its effect) | **{s['lost']}** |",
        f"| Succeeded | {c['succeeded']:,} |",
        f"| Dead (in the DLQ: visible, so not lost) | {c['dead']} |",
        f"| Effects recorded (one per job at most) | {c['effects']:,} |",
        f"| Runs recorded | {c['executions']:,} |",
        f"| Duplicate runs (runs minus distinct jobs) | {s['duplicate_runs']} |",
        f"| Jobs reclaimed after a lease expired | {c['jobs_reclaimed']} |",
        f"| Jobs in flight on the worker sent SIGTERM | {s['jobs_in_flight_at_sigterm']} |",
        f"| Runs released on SIGTERM (no attempt used) | {c['released_attempts']} |",
        f"| Jobs frozen mid-run on the frozen worker | {s['jobs_in_flight_at_freeze']} |",
        f"| Acks refused by the fencing token (worker logs) | {s['acks_refused']} |",
        f"| Failure reports (nacks) refused by the fencing token | {s['nacks_refused']} |",
        f"| Runs cancelled by a heartbeat after the lease was lost | {s['cancelled_lost_lease']} |",
        f"| Longest time for a killed job to finish elsewhere | {fmt_s(s['recovery_max_s'])} |",
        f"| Median time for a killed job to finish elsewhere | {fmt_s(s['recovery_median_s'])} |",
        f"| Enqueue retries (any answer but 200/201) | {data['enqueue']['retried']} |",
        "",
        "## The guide's checks",
        "",
        f"1. Lost jobs, `status NOT IN ('succeeded', 'dead')`: **{c['unfinished']}**.",
        "2. The idempotent effect happened exactly once per succeeded job: succeeded jobs "
        f"without an effect **{c['succeeded_without_effect']}** (job_effects has job_id as "
        f"its primary key, so never more than one). Dead jobs whose effect had already "
        f"happened before their last failure: {c['dead_with_effect']}.",
        f"3. Re-executions, `count(*) - count(DISTINCT job_id)` over job_executions: "
        f"**{s['duplicate_runs']}**. Above 0 is at-least-once delivery at work; check 2 shows "
        "the duplicates were harmless.",
        "",
        "Where duplicates come from here: a worker killed after its effect but before its ack "
        "(a window of milliseconds, so rare), and the frozen worker, which wakes up after its "
        "jobs were reclaimed and finished elsewhere, finishes them again, and has its acks "
        "refused because the lease token changed. A worker that ends a SIGTERM drain with "
        "jobs still running releases them instead; these jobs are short, so they finish within "
        "the 25 s grace period.",
        "",
        *(
            [
                "In this mode a worker marks each job succeeded as soon as it claims it, so the "
                "queue never shows a job running and the kills above find none cut off mid-run. "
                "Every job a kill did cut off is counted as lost instead: succeeded, with no "
                "effect.",
                "",
            ]
            if a["mode"] != "normal"
            else []
        ),
        "## Timeline",
        "",
        "| t (s) | Event | Worker | This run's jobs in flight when the signal landed |",
        "|---|---|---|---|",
    ]
    for e in data["events"]:
        flight = str(len(e["in_flight"])) if e["kind"] in ("sigkill", "sigterm", "freeze") else ""
        lines.append(f"| {e['t']:.1f} | {e['kind']} | {e['container']} | {flight} |")
    lines += [
        "",
        f"Raw data: `{data['run']}.json`. Grafana shows each kill as an annotation on the "
        "Hopper dashboard for this time range.",
        "",
    ]
    return "\n".join(lines)


def fmt_s(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.1f} s"


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--mode", choices=["normal", "ack-before-run"], default="normal")
    p.add_argument("--jobs", type=int, default=10_000)
    p.add_argument("--duration", type=float, default=300.0, help="kill window, seconds")
    p.add_argument(
        "--enqueue-seconds", type=float, help="spread the enqueues over this (default: duration)"
    )
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--min-gap", type=float, default=3.0)
    p.add_argument("--max-gap", type=float, default=10.0)
    p.add_argument("--drain-timeout", type=float, default=600.0)
    p.add_argument("--no-sigterm", dest="sigterm", action="store_false")
    p.add_argument("--no-redis-restart", dest="redis_restart", action="store_false")
    p.add_argument("--no-freeze", dest="freeze", action="store_false")
    p.add_argument(
        "--freeze-seconds", type=float, default=LEASE_SECONDS + 10, help="must outlast the lease"
    )
    p.add_argument("--base-url", default="http://127.0.0.1:8000")
    p.add_argument("--compose", default=DEFAULT_COMPOSE, help="the Compose command prefix")
    p.add_argument("--env-file", default=".env", help="for the Postgres and Grafana logins")
    p.add_argument("--api-service", default="api", help="where to run hopper.bootstrap")
    p.add_argument("--grafana-url", default="http://127.0.0.1:3000")
    p.add_argument("--out", default="chaos/results")
    args = p.parse_args(argv)
    if args.enqueue_seconds is None:
        args.enqueue_seconds = args.duration
    if args.jobs < 1 or args.workers < 1 or not 0 < args.min_gap <= args.max_gap:
        p.error("need --jobs >= 1, --workers >= 1 and 0 < --min-gap <= --max-gap")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    env = {**read_env_file(ROOT / args.env_file), **os.environ}
    base = shlex.split(args.compose)
    compose = base + (["-f", CONTROL_OVERRIDE] if args.mode == "ack-before-run" else [])
    stack = Stack(base, env.get("POSTGRES_USER", "hopper"), env.get("POSTGRES_DB", "hopper"))
    annotator = Annotator(args.grafana_url, env.get("GRAFANA_ADMIN_PASSWORD"))
    started_at = datetime.now(UTC)
    run = run_label(args.mode, started_at)
    stats = EnqueueStats()
    try:
        if stack.scalar("SELECT to_regclass('job_executions') IS NOT NULL") != "t":
            raise ChaosError("the database has no job_executions table: run `make up` first")
        key = stack.bench_key(args.api_service)
        log(f"run {run}: {args.workers} workers, mode {args.mode}")
        stack.scale_workers(args.workers, compose, recreate=True)
        annotator.mark(f"chaos run {run} starts", ["chaos"])
        enqueuer = threading.Thread(
            target=enqueue_all,
            args=(args.base_url, key, run, args.jobs, args.enqueue_seconds, stats),
            daemon=True,
        )
        enqueuer.start()
        events = Window(stack, args, compose, run, annotator).go()
        log("kill window closed; finishing the enqueues")
        enqueuer.join()
        stack.scale_workers(args.workers, compose)
        drained = wait_for_drain(stack, run, stats.accepted, args.drain_timeout)
        if not drained:
            log(f"the queue did not drain within {args.drain_timeout:.0f} s")
        row = stack.psql(CHECKS.format(run=run))[0]
        checks = dict(zip(CHECK_NAMES, map(int, row), strict=True))
        fenced = {
            name: stack.worker_log_lines(started_at, f'"event": "{event}"')
            for name, event in (
                ("acks_refused", "ack_rejected_lease_lost"),
                ("nacks_refused", "nack_rejected_lease_lost"),
                ("cancelled_lost_lease", "lease_lost_job_cancelled"),
            )
        }
        recovery = recovery_seconds(stack, events)
        summary = summarize(checks, events, recovery, args.mode, fenced)
        annotator.mark(f"chaos run {run} ends: {summary['verdict']}", ["chaos"])
    except ChaosError as exc:
        log(f"cannot run: {exc}")
        return 2
    finally:
        # Never leave at-most-once workers behind, or the wrong number of them.
        try:
            stack.scale_workers(NORMAL_WORKERS, base, recreate=True)
        except ChaosError as exc:
            log(f"could not restore the normal workers: {exc}")

    data = {
        "run": run,
        "started": f"{started_at:%Y-%m-%d %H:%M:%S} UTC",
        "finished": f"{datetime.now(UTC):%Y-%m-%d %H:%M:%S} UTC",
        "host": host_description(),
        "git": git_sha(),
        "args": {**vars(args), "compose": shlex.join(compose)},
        "drained": drained,
        "enqueue": asdict(stats),
        "checks": checks,
        "summary": summary,
        "events": [asdict(e) for e in events],
    }
    out = ROOT / args.out
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{run}.json").write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
    (out / f"{run}.md").write_text(render_report(data), encoding="utf-8")
    log(f"{summary['verdict']}; report: {args.out}/{run}.md")
    return 0 if summary["passed"] and drained else 1


if __name__ == "__main__":
    sys.exit(main())
