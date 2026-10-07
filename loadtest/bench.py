#!/usr/bin/env python3
"""Load test: the build guide's scenarios A-D against the running Compose stack.

A  Enqueue throughput: k6 at a constant 100, 250, 500 and 1,000 requests/s, 2 minutes each.
B  Drain throughput: 100,000 `sleep ms=0` jobs preloaded, drained by 1, 2, 4 and 8 workers.
C  Steady mix: 300 jobs/s of `sleep 50 ms` plus 5% `flaky`, for 10 minutes.
D  Breaking point: the rate steps up a minute at a time until the p95 queue wait passes 5 s,
   errors pass 1%, or the API stops keeping up; each step records CPU per container and
   Postgres's connections, lock waits and dead tuples, to name what saturated first.

The stack is the development one (docker/compose.yaml) with a second API replica added by
loadtest/compose.bench.yaml, as in production (two API replicas): one Python process tops
out well below the guide's rates. Each scenario runs --runs times (3) after a 1-minute warm-up
and reports the median and the spread (min-max). Between runs the load-test tenant's finished
jobs are deleted and the tables vacuumed, so every run starts from the same table.

Where the numbers come from:
- the API's latency and errors: k6, which runs in a container on the stack's own network
  (grafana/k6, no install needed), so it shares the host with the stack;
- queue wait, run time and end to end: Postgres, exact per job (percentile_cont over the
  job rows of the run), not interpolated from histogram buckets;
- CPU: `docker stats` during each run or step; 100% is one core.

Standard library only (Docker does the rest). Results go to loadtest/results/<date>-<host>/:
summary.md (the tables), results.json (every run), environment.json, raw/ (k6 summaries).

    make up && python3 loadtest/bench.py --scenario all     # about two hours
    python3 loadtest/bench.py --scenario A --runs 1 --quick  # a smoke run, a few minutes
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import platform
import re
import shlex
import statistics
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from chaos.kill_workers import (  # noqa: E402  (shared with the chaos test)
    ChaosError,
    Stack,
    git_sha,
    host_description,
    log,
    read_env_file,
)

K6_IMAGE = "grafana/k6:2.3.0"
K6_SCRIPT = "k6/enqueue.js"
NORMAL_WORKERS = 3  # docker/compose.yaml's replica count; also the count for A, C and D
BENCH_COMPOSE = (
    "docker compose -f docker/compose.yaml -f loadtest/compose.bench.yaml --env-file .env"
)
BENCH_TENANT = "hopper-bench"  # created by python -m hopper.bootstrap --bench-key


# --- small helpers ----------------------------------------------------------------------------


def power_state() -> dict[str, Any]:
    """Whether the host runs on mains power, and its power profile, where Linux exposes them
    (on a server neither exists, and both stay None)."""
    on_mains: bool | None = None
    for supply in sorted(Path("/sys/class/power_supply").glob("*")):
        try:
            if (supply / "type").read_text().strip() == "Mains":
                on_mains = bool(on_mains) or (supply / "online").read_text().strip() == "1"
        except OSError:
            continue
    profile: str | None = None
    with contextlib.suppress(OSError, subprocess.SubprocessError):
        out = subprocess.run(
            ["powerprofilesctl", "get"], capture_output=True, text=True, timeout=5
        ).stdout.strip()
        profile = out or None
    return {"on_mains": on_mains, "power_profile": profile}


def throttled(state: dict[str, Any]) -> str:
    """Why this host would throttle its CPU during a run, or "" if nothing says it will.

    A laptop on battery, or in power-saver mode, runs its CPU at a fraction of its speed and
    changes speed as the battery drains: the numbers would measure the battery, not Hopper.
    (It happened: a two-hour run on battery slowed down three times over as it went.)
    """
    if state.get("on_mains") is False:
        return "the machine is running on battery"
    if state.get("power_profile") == "power-saver":
        return "the power profile is power-saver"
    return ""


def median_spread(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"median": None, "min": None, "max": None}
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def fmt(cell: dict[str, float | None], digits: int = 0, scale: float = 1.0) -> str:
    """'median (min-max)', or just the value when the runs agree to the shown precision."""
    if cell["median"] is None:
        return "n/a"
    med, lo, hi = (float(cell[k] or 0) * scale for k in ("median", "min", "max"))
    one = f"{med:,.{digits}f}"
    low, high = f"{lo:,.{digits}f}", f"{hi:,.{digits}f}"
    return one if low == high else f"{one} ({low}-{high})"


def parse_pg_array(text: str) -> list[float | None]:
    """'{0.1,0.2,NULL}' (psql's text form of a float array) -> [0.1, 0.2, None]."""
    text = text.strip()
    if not text or text in ("{}", "NULL"):
        return []
    return [None if v == "NULL" else float(v) for v in text.strip("{}").split(",")]


def parse_docker_stats(text: str) -> dict[str, float]:
    """`docker stats --format '{{.Name}}\\t{{.CPUPerc}}'` -> {name: cpu percent}."""
    cpu: dict[str, float] = {}
    for line in text.splitlines():
        name, _, value = line.partition("\t")
        match = re.match(r"([\d.]+)%", value.strip())
        if name.strip() and match:
            cpu[name.strip()] = float(match.group(1))
    return cpu


def group_cpu(cpu: dict[str, float]) -> dict[str, float]:
    """Per role: api, postgres, redis, workers (all replicas together), worker_max (the
    busiest one), scheduler, k6."""

    def total(prefix: str) -> float:
        return round(sum(v for k, v in cpu.items() if k.startswith(prefix)), 1)

    workers = [v for k, v in cpu.items() if k.startswith("hopper-worker-")]
    return {
        "api": total("hopper-api-"),
        "postgres": total("hopper-postgres-1"),
        "redis": total("hopper-redis-"),
        "workers": round(sum(workers), 1),
        "worker_max": max(workers, default=0.0),
        "scheduler": total("hopper-scheduler-"),
        "k6": total("hopper-k6"),
    }


def k6_numbers(summary: dict[str, Any]) -> dict[str, float]:
    m = summary["metrics"]
    d = m["http_req_duration"]["values"]
    return {
        "requests": float(m["http_reqs"]["values"]["count"]),
        "achieved_rps": float(m["http_reqs"]["values"]["rate"]),
        "p50_ms": float(d["med"]),
        "p95_ms": float(d["p(95)"]),
        "p99_ms": float(d["p(99)"]),
        "max_ms": float(d["max"]),
        "failed_rate": float(m["http_req_failed"]["values"]["rate"]),
        "not_201_rate": 1.0 - float(m["checks"]["values"]["rate"]),
        "dropped": float(m.get("dropped_iterations", {}).get("values", {}).get("count", 0)),
    }


# --- the bench ------------------------------------------------------------------------------


class Bench:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        env = {**read_env_file(ROOT / args.env_file), **os.environ}
        self.compose = shlex.split(args.compose)
        self.stack = Stack(
            self.compose, env.get("POSTGRES_USER", "hopper"), env.get("POSTGRES_DB", "hopper")
        )
        stamp = f"{datetime.now(UTC):%Y-%m-%d}-{platform.node() or 'host'}"
        self.out = ROOT / args.out / (args.tag or stamp)
        self.results: dict[str, Any] = {"runs": []}
        self.key = ""
        self.tenant = ""

    # stack and database

    def check_power(self) -> None:
        """Before setup and before every run: never measure a throttled CPU."""
        reason = throttled(power_state())
        if reason and not self.args.allow_throttled:
            raise ChaosError(
                f"{reason}: plug in and use the balanced or performance power profile "
                "(or pass --allow-throttled to measure anyway)"
            )

    def setup(self) -> None:
        self.check_power()
        (self.out / "raw").mkdir(parents=True, exist_ok=True)
        self.key = self.stack.bench_key(self.args.api_service)
        self.tenant = self.stack.scalar(f"SELECT id FROM tenants WHERE name = '{BENCH_TENANT}'")
        if not self.tenant:
            raise ChaosError("no load-test tenant after hopper.bootstrap --bench-key")
        self.stack.run([*self.compose, "up", "-d", "--no-deps", "--wait", "api-2"], timeout=300)
        self.scale(NORMAL_WORKERS)
        time.sleep(self.args.api_key_settle)  # the API caches keys; let the new one settle

    def teardown(self) -> None:
        """Back to the development stack: 3 workers, one API."""
        self.scale(NORMAL_WORKERS)
        self.stack.run([*self.compose, "rm", "--stop", "--force", "api-2"], timeout=120)

    def scale(self, workers: int) -> None:
        self.stack.scale_workers(workers, self.compose)

    def db_now(self) -> str:
        return self.stack.scalar("SELECT now()")

    def busy(self) -> int:
        return int(
            self.stack.scalar(
                f"SELECT count(*) FROM jobs WHERE tenant_id = '{self.tenant}' "
                "AND status IN ('queued', 'running')"
            )
        )

    def wait_idle(self, timeout: float = 900) -> float:
        """Waits until the tenant has nothing queued or running; returns how long it took."""
        started = time.monotonic()
        while (left := self.busy()) > 0:
            if time.monotonic() - started > timeout:
                raise ChaosError(f"{left} jobs still queued or running after {timeout:.0f} s")
            time.sleep(2)
        return time.monotonic() - started

    def clean(self) -> None:
        """Deletes the tenant's finished jobs (attempts go with them) and vacuums, so the next
        run starts from the same table instead of one bloated by the last."""
        self.stack.psql(
            f"DELETE FROM jobs WHERE tenant_id = '{self.tenant}' "
            "AND status IN ('succeeded', 'cancelled', 'dead')"
        )
        for table in ("jobs", "job_attempts"):
            self.stack.psql(f"VACUUM (ANALYZE) {table}")

    def pg_snapshot(self) -> dict[str, int]:
        row = self.stack.psql(
            "SELECT (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()),"
            " (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
            "    AND wait_event_type = 'Lock'),"
            " (SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'jobs'),"
            " (SELECT n_live_tup FROM pg_stat_user_tables WHERE relname = 'jobs')"
        )[0]
        names = ("connections", "lock_waits", "jobs_dead_tuples", "jobs_live_tuples")
        return {k: int(v or 0) for k, v in zip(names, row, strict=True)}

    def cpu_sample(self) -> dict[str, float]:
        out = self.stack.run(
            ["docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.CPUPerc}}"], timeout=60
        )
        return group_cpu(parse_docker_stats(out))

    def sample_later(self, delay: float, into: dict[str, Any]) -> threading.Thread:
        """Takes the CPU and Postgres snapshot `delay` seconds from now, in the background."""

        def take() -> None:
            time.sleep(delay)
            try:
                into["cpu"] = self.cpu_sample()
                into["pg"] = self.pg_snapshot()
            except ChaosError as exc:
                into["sample_error"] = str(exc)

        thread = threading.Thread(target=take, daemon=True)
        thread.start()
        return thread

    def job_stats(self, since: str, until: str) -> dict[str, Any]:
        """Exact percentiles over the jobs the run created, from their own timestamps.

        queue wait: first attempts only (a retry waits for its backoff on purpose), and jobs
        not started yet count with the time they have waited so far, a lower bound.
        """
        window = (
            f"tenant_id = '{self.tenant}' AND created_at >= '{since}'::timestamptz "
            f"AND created_at < '{until}'::timestamptz"
        )
        pct = "percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY {})"
        row = self.stack.psql(
            "SELECT count(*),"
            " count(*) FILTER (WHERE status = 'succeeded'),"
            " count(*) FILTER (WHERE status = 'dead'),"
            " count(*) FILTER (WHERE status IN ('queued', 'running')),"
            " count(*) FILTER (WHERE attempts > 1),"
            f" {pct.format('extract(epoch FROM coalesce(started_at, now()) - created_at)')}"
            "   FILTER (WHERE attempts <= 1),"
            f" {pct.format('extract(epoch FROM finished_at - started_at)')}"
            "   FILTER (WHERE status = 'succeeded' AND attempts = 1 AND task = 'sleep'),"
            f" {pct.format('extract(epoch FROM finished_at - created_at)')}"
            "   FILTER (WHERE status = 'succeeded' AND task = 'sleep')"
            f" FROM jobs WHERE {window}"
        )[0]
        wait, run, e2e = (parse_pg_array(v) for v in row[5:8])
        return {
            "jobs": int(row[0]),
            "succeeded": int(row[1]),
            "dead": int(row[2]),
            "unfinished": int(row[3]),
            "retried": int(row[4]),
            "wait_s": wait,
            "run_s": run,
            "end_to_end_s": e2e,
        }

    # k6

    def k6(self, name: str, rate: int, duration: str, mix: str = "sleep") -> dict[str, float]:
        raw = f"raw/{name}.json"
        out_dir = self.out.relative_to(ROOT / "loadtest")
        env = {**os.environ, "KEY": self.key}
        args = [
            "docker", "run", "--rm", "--name", "hopper-k6",
            "--network", self.args.network, "--user", f"{os.getuid()}:{os.getgid()}",
            "-v", f"{ROOT / 'loadtest'}:/work", "-w", "/work",
            "-e", f"BASE={self.args.base}", "-e", "KEY", "-e", f"RATE={rate}",
            "-e", f"DURATION={duration}", "-e", f"MIX={mix}",
            "-e", f"SUMMARY=/work/{out_dir}/{raw}",
            K6_IMAGE, "run", "--quiet", K6_SCRIPT,
        ]  # fmt: skip
        result = subprocess.run(args, cwd=ROOT, env=env, capture_output=True, text=True)
        # 99: a threshold was crossed, which is a finding, not a failure to run.
        if result.returncode not in (0, 99):
            raise ChaosError(f"k6 failed ({result.returncode}): {result.stderr.strip()[-800:]}")
        log("  k6 " + result.stdout.strip())
        summary = json.loads((self.out / raw).read_text(encoding="utf-8"))
        return k6_numbers(summary)

    def warm_up(self, rate: int) -> None:
        log(f"warm-up: {self.args.warmup} at {rate} req/s (not measured)")
        self.k6("warmup", rate, self.args.warmup)
        self.wait_idle()
        self.clean()

    def record(self, entry: dict[str, Any]) -> None:
        self.results["runs"].append(entry)
        (self.out / "results.json").write_text(
            json.dumps(self.results, indent=2) + "\n", encoding="utf-8"
        )

    def measured_run(
        self, scenario: str, name: str, rate: int, duration: str, mix: str = "sleep"
    ) -> dict[str, Any]:
        """One k6 run, its jobs drained, and everything measured about it."""
        self.check_power()
        sample: dict[str, Any] = {}
        sampler = self.sample_later(seconds(duration) * 0.6, sample)
        since = self.db_now()
        api = self.k6(name, rate, duration, mix)
        until = self.db_now()
        sampler.join()
        drain_s = self.wait_idle()
        entry = {
            "scenario": scenario,
            "name": name,
            "rate": rate,
            "duration": duration,
            "api": api,
            "jobs": self.job_stats(since, until),
            "drain_after_s": round(drain_s, 1),
            **sample,
        }
        self.clean()
        return entry

    # scenarios

    def scenario_a(self) -> None:
        a = self.args
        self.warm_up(a.a_rates[0])
        for rate in a.a_rates:
            for run in range(1, a.runs + 1):
                log(f"A: {rate} req/s, run {run}/{a.runs}")
                entry = self.measured_run("A", f"A-{rate}rps-run{run}", rate, a.a_duration)
                self.record({**entry, "run": run})

    def scenario_b(self) -> None:
        a = self.args
        try:
            for workers in a.b_workers:
                for run in range(1, a.runs + 1):
                    log(f"B: {a.b_jobs:,} jobs, {workers} workers, run {run}/{a.runs}")
                    self.record({**self.drain_run(workers, a.b_jobs), "run": run})
        finally:
            self.scale(NORMAL_WORKERS)

    def drain_run(self, workers: int, jobs: int) -> dict[str, Any]:
        self.check_power()
        self.scale(0)
        self.wait_idle()
        self.clean()
        self.stack.psql(
            "INSERT INTO jobs (tenant_id, queue, task, payload) "
            f"SELECT '{self.tenant}', 'default', 'sleep', '{{\"ms\": 0}}'::jsonb "
            f"FROM generate_series(1, {jobs})"
        )
        self.stack.psql("VACUUM (ANALYZE) jobs")
        sample: dict[str, Any] = {}
        started = time.monotonic()
        self.scale(workers)
        # CPU and Postgres are sampled mid-drain: once 40% of the jobs are done.
        while (left := self.busy()) > 0:
            if not sample and left <= 0.6 * jobs:
                sample = {"cpu": self.cpu_sample(), "pg": self.pg_snapshot()}
            if time.monotonic() - started > 1800:
                raise ChaosError(f"{left} jobs still waiting after 30 minutes")
            time.sleep(0.5)
        wall_s = time.monotonic() - started
        row = self.stack.psql(
            "SELECT count(*), extract(epoch FROM max(finished_at) - min(started_at)) "
            f"FROM jobs WHERE tenant_id = '{self.tenant}' AND status = 'succeeded'"
        )[0]
        done, seconds_db = int(row[0]), float(row[1] or 0)
        entry = {
            "scenario": "B",
            "name": f"B-{workers}workers",
            "workers": workers,
            "jobs": jobs,
            "succeeded": done,
            "drain_s": round(seconds_db, 2),
            "jobs_per_s": round(done / seconds_db, 1) if seconds_db else None,
            "wall_s_including_worker_start": round(wall_s, 1),
            **sample,
        }
        log(f"  drained {done:,} jobs in {seconds_db:.1f} s: {entry['jobs_per_s']} jobs/s")
        self.clean()
        return entry

    def scenario_c(self) -> None:
        a = self.args
        self.warm_up(a.c_rate)
        for run in range(1, a.runs + 1):
            log(f"C: {a.c_rate} jobs/s steady mix for {a.c_duration}, run {run}/{a.runs}")
            entry = self.measured_run("C", f"C-run{run}", a.c_rate, a.c_duration, mix="steady")
            self.record({**entry, "run": run})

    def scenario_d(self) -> None:
        a = self.args
        self.warm_up(a.d_start)
        for run in range(1, a.runs + 1):
            log(f"D: from {a.d_start} req/s, +{a.d_step} every {a.d_step_duration}, run {run}")
            steps: list[dict[str, Any]] = []
            rate = a.d_start
            while rate <= a.d_max:
                self.check_power()
                sample: dict[str, Any] = {}
                sampler = self.sample_later(seconds(a.d_step_duration) * 0.7, sample)
                since = self.db_now()
                api = self.k6(f"D-run{run}-{rate}rps", rate, a.d_step_duration)
                until = self.db_now()
                sampler.join()
                step = {
                    "rate": rate,
                    "api": api,
                    "jobs": self.job_stats(since, until),
                    "backlog": self.busy(),
                    **sample,
                }
                step["broke"] = breaking_reason(step, a.d_wait_limit, a.d_error_limit)
                steps.append(step)
                log(f"  step {rate} req/s: {step['broke'] or 'held'}")
                if step["broke"]:
                    break
                rate += a.d_step
            self.wait_idle(timeout=1800)
            self.clean()
            self.record({"scenario": "D", "name": f"D-run{run}", "run": run, "steps": steps})

    def environment(self) -> dict[str, Any]:
        info = self.stack.run(
            ["docker", "info", "--format", "{{.NCPU}}\t{{.MemTotal}}\t{{.OperatingSystem}}"]
        ).split("\t")
        versions = self.stack.run(["docker", "version", "--format", "{{.Server.Version}}"])
        return {
            "host": host_description(),
            "os": f"{platform.system()} {platform.release()}",
            "docker": f"{info[2].strip()}, engine {versions.strip()}, {info[0]} CPUs and "
            f"{int(info[1]) / 1024**3:.1f} GiB memory for containers",
            "git": git_sha(),
            "k6": f"{K6_IMAGE}, in a container on the stack's network ({self.args.network}), "
            "sharing the host with the stack",
            "target": self.args.base,
            "stack": "docker/compose.yaml plus loadtest/compose.bench.yaml: 2 API replicas "
            "(one uvicorn process each, as in production; k6 spreads over both through Docker "
            "DNS), 3 workers x 20 slots (B: 1, 2, 4, 8 workers), 2 schedulers, Postgres 16.15, "
            "Redis 7.4.11",
            "settings": "lease 30 s, heartbeat 10 s, poll 0.25 s (idle backoff to 0.5 s), "
            "DB pool 5 per process, Postgres defaults (max_connections 100, shared_buffers 128 MB)",
            "power": power_state(),
            "args": {k: v for k, v in vars(self.args).items() if k != "compose"},
            "started": f"{datetime.now(UTC):%Y-%m-%d %H:%M:%S} UTC",
        }


def seconds(duration: str) -> float:
    """k6 durations: '90s', '2m', '10m', '1m30s'."""
    total = 0.0
    for value, unit in re.findall(r"(\d+(?:\.\d+)?)(ms|s|m|h)", duration):
        total += float(value) * {"ms": 0.001, "s": 1, "m": 60, "h": 3600}[unit]
    return total


def breaking_reason(step: dict[str, Any], wait_limit: float, error_limit: float) -> str:
    """Why a scenario D step counts as broken, or "" if it held (the build guide's rule: p95
    queue wait over 5 s or errors over 1%; also the API falling behind the target rate)."""
    api, jobs = step["api"], step["jobs"]
    wait = jobs["wait_s"]
    if len(wait) > 1 and wait[1] is not None and wait[1] > wait_limit:
        return f"p95 queue wait {wait[1]:.1f} s > {wait_limit:g} s"
    errors = max(api["failed_rate"], api["not_201_rate"])
    if errors > error_limit:
        return f"errors {errors:.1%} > {error_limit:.0%}"
    if api["achieved_rps"] < 0.95 * step["rate"] or api["dropped"] > 0.01 * api["requests"]:
        return f"API kept up with only {api['achieved_rps']:.0f} of {step['rate']} req/s"
    return ""


# --- the summary ----------------------------------------------------------------------------


def ms(values: list[float | None], i: int) -> float | None:
    return values[i] * 1000 if len(values) > i and values[i] is not None else None


def collect(runs: list[dict[str, Any]], pick: Callable[[dict[str, Any]], Any]) -> list[float]:
    return [float(v) for r in runs if (v := pick(r)) is not None]


def render_summary(env: dict[str, Any], results: dict[str, Any]) -> str:
    runs = results["runs"]
    lines = [
        "# Load test results",
        "",
        "Measured, not estimated: every number below comes from the raw files in this "
        "directory. Each cell is the median of the runs, with the spread (min-max) in "
        "brackets when the runs differ.",
        "",
        "## Environment",
        "",
        *(f"- {k.capitalize()}: {env[k]}" for k in ("host", "os", "docker", "stack", "settings")),
        f"- Load generator: {env['k6']}",
        f"- Power: {power_line(env.get('power', {}))}",
        f"- Code: git {env['git']}; started {env['started']}",
        "",
    ]
    a_runs = [r for r in runs if r["scenario"] == "A"]
    if a_runs:
        lines += [
            "## A. Enqueue throughput",
            "",
            "POST /v1/jobs (`sleep` 50 ms) at a constant arrival rate for "
            f"{a_runs[0]['duration']} per run; latency is the API's, as k6 saw it.",
            "",
            "| Target req/s | Achieved req/s | p50 ms | p95 ms | p99 ms | Errors | "
            "API CPU % | Postgres CPU % |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for rate in sorted({r["rate"] for r in a_runs}):
            rr = [r for r in a_runs if r["rate"] == rate]
            cells = [
                fmt(median_spread(collect(rr, lambda r: r["api"]["achieved_rps"])), 1),
                fmt(median_spread(collect(rr, lambda r: r["api"]["p50_ms"])), 1),
                fmt(median_spread(collect(rr, lambda r: r["api"]["p95_ms"])), 1),
                fmt(median_spread(collect(rr, lambda r: r["api"]["p99_ms"])), 1),
                fmt(median_spread(collect(rr, lambda r: errors(r["api"]))), 2, 100) + "%",
                fmt(median_spread(collect(rr, lambda r: r.get("cpu", {}).get("api")))),
                fmt(median_spread(collect(rr, lambda r: r.get("cpu", {}).get("postgres")))),
            ]
            lines.append(f"| {rate:,} | " + " | ".join(cells) + " |")
        lines.append("")
    b_runs = [r for r in runs if r["scenario"] == "B"]
    if b_runs:
        lines += [
            "## B. Drain throughput",
            "",
            f"{b_runs[0]['jobs']:,} `sleep ms=0` jobs inserted while no worker runs, then N "
            "workers (20 slots each) start; drain time is from the first claim to the last "
            "ack, on Postgres's clock.",
            "",
            "| Workers | Jobs/s | Drain time s | Postgres CPU % | Workers CPU % (all) |",
            "|---|---|---|---|---|",
        ]
        for workers in sorted({r["workers"] for r in b_runs}):
            rr = [r for r in b_runs if r["workers"] == workers]
            cells = [
                fmt(median_spread(collect(rr, lambda r: r["jobs_per_s"]))),
                fmt(median_spread(collect(rr, lambda r: r["drain_s"])), 1),
                fmt(median_spread(collect(rr, lambda r: r.get("cpu", {}).get("postgres")))),
                fmt(median_spread(collect(rr, lambda r: r.get("cpu", {}).get("workers")))),
            ]
            lines.append(f"| {workers} | " + " | ".join(cells) + " |")
        lines.append("")
    c_runs = [r for r in runs if r["scenario"] == "C"]
    if c_runs:
        lines += [
            "## C. Steady mix",
            "",
            f"{c_runs[0]['rate']} jobs/s for {c_runs[0]['duration']} per run: `sleep` 50 ms, "
            "plus 5% `flaky` (fails half the time, then retries with backoff).",
            "",
            "| Measure | p50 | p95 | p99 |",
            "|---|---|---|---|",
        ]
        rows: list[tuple[str, Callable[[dict[str, Any], int], float | None]]] = [
            ("API latency, ms", lambda r, i: r["api"][("p50_ms", "p95_ms", "p99_ms")[i]]),
            ("Queue wait (first attempt), ms", lambda r, i: ms(r["jobs"]["wait_s"], i)),
            ("Run time (`sleep` 50 ms), ms", lambda r, i: ms(r["jobs"]["run_s"], i)),
            ("Enqueue to done (`sleep`), ms", lambda r, i: ms(r["jobs"]["end_to_end_s"], i)),
        ]
        for label, get in rows:
            cells = [
                fmt(median_spread(collect(c_runs, lambda r, i=i, get=get: get(r, i))), 1)
                for i in range(3)
            ]
            lines.append(f"| {label} | " + " | ".join(cells) + " |")
        jobs_s = [r["jobs"]["jobs"] / seconds(r["duration"]) for r in c_runs]
        cpu = {
            role: stat(c_runs, lambda r, role=role: r.get("cpu", {}).get(role))
            for role in ("api", "postgres", "workers")
        }
        lines += [
            "",
            f"- Jobs created per second: {fmt(median_spread(jobs_s), 1)}; API errors: "
            f"{stat(c_runs, lambda r: errors(r['api']), 2, 100)}%",
            "- Jobs retried (the flaky ones that failed): "
            f"{stat(c_runs, lambda r: r['jobs']['retried'])}; dead after all attempts: "
            f"{stat(c_runs, lambda r: r['jobs']['dead'])}",
            f"- CPU % during the run: API {cpu['api']}, Postgres {cpu['postgres']}, "
            f"workers {cpu['workers']}",
            "",
        ]
    d_runs = [r for r in runs if r["scenario"] == "D"]
    if d_runs:
        lines += [
            "## D. Breaking point",
            "",
            "The rate steps up each step until the p95 queue wait passes "
            "5 s, errors pass 1%, or the API cannot accept the rate. CPU % is per role, summed "
            "over its containers (100% = one core); each API replica is one Python process, so "
            "about 100% per replica, 200% for both, is the API's ceiling.",
            "",
        ]
        for d in d_runs:
            lines += [
                f"### Run {d['run']}",
                "",
                "| Target req/s | Achieved | API p95 ms | Queue wait p95 s | Backlog | Errors | "
                "API CPU | Postgres CPU | Workers CPU (busiest) | PG connections | "
                "jobs dead tuples | Result |",
                "|---|---|---|---|---|---|---|---|---|---|---|---|",
            ]
            for s in d["steps"]:
                cpu, pg, w = s.get("cpu", {}), s.get("pg", {}), s["jobs"]["wait_s"]
                wait95 = f"{w[1]:.2f}" if len(w) > 1 and w[1] is not None else "n/a"
                dead = pg.get("jobs_dead_tuples")
                dead_tuples = f"{dead:,}" if isinstance(dead, int) else "n/a"
                lines.append(
                    f"| {s['rate']:,} | {s['api']['achieved_rps']:.0f} | "
                    f"{s['api']['p95_ms']:.1f} | {wait95} | {s['backlog']:,} | "
                    f"{errors(s['api']):.2%} | {cpu.get('api', 0):.0f}% | "
                    f"{cpu.get('postgres', 0):.0f}% | {cpu.get('workers', 0):.0f}% "
                    f"({cpu.get('worker_max', 0):.0f}%) | {pg.get('connections', 'n/a')} | "
                    f"{dead_tuples} | {s['broke'] or 'held'} |"
                )
            lines.append("")
        held = [max((s["rate"] for s in d["steps"] if not s["broke"]), default=0) for d in d_runs]
        lines += [
            f"Highest rate held: {fmt(median_spread([float(h) for h in held]))} req/s.",
            "",
        ]
    lines += [
        "## Files",
        "",
        "- `results.json`: every run's numbers; `environment.json`: the setup",
        "- `raw/`: k6's own end-of-test summary for every run and step",
        "",
    ]
    return "\n".join(lines)


def stat(
    runs: list[dict[str, Any]],
    pick: Callable[[dict[str, Any]], Any],
    digits: int = 0,
    scale: float = 1.0,
) -> str:
    return fmt(median_spread(collect(runs, pick)), digits, scale)


def power_line(state: dict[str, Any]) -> str:
    mains = {True: "on mains power", False: "ON BATTERY", None: "power source not reported"}
    profile = state.get("power_profile") or "no power profile reported"
    return f"{mains[state.get('on_mains')]}, {profile}"


def errors(api: dict[str, float]) -> float:
    return max(api["failed_rate"], api["not_201_rate"])


# --- entry point ----------------------------------------------------------------------------


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--scenario", default="all", help="A, B, C, D, a list like A,C, or all")
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--quick", action="store_true", help="short runs, to try the bench out")
    p.add_argument("--compose", default=BENCH_COMPOSE, help="must define the api-2 service")
    p.add_argument("--env-file", default=".env")
    p.add_argument("--api-service", default="api", help="where to run hopper.bootstrap")
    p.add_argument("--network", default="hopper_default", help="the stack's Docker network")
    p.add_argument("--base", default="http://api:8000", help="the API, as k6 reaches it")
    p.add_argument("--out", default="loadtest/results")
    p.add_argument("--tag", help="results subdirectory (default: <date>-<host>)")
    p.add_argument("--warmup", default="1m")
    p.add_argument("--api-key-settle", type=float, default=2.0)
    p.add_argument(
        "--allow-throttled",
        action="store_true",
        help="run even on battery or in power-saver mode (the numbers will say so)",
    )
    p.add_argument("--a-rates", default="100,250,500,1000")
    p.add_argument("--a-duration", default="2m")
    p.add_argument("--b-workers", default="1,2,4,8")
    p.add_argument("--b-jobs", type=int, default=100_000)
    p.add_argument("--c-rate", type=int, default=300)
    p.add_argument("--c-duration", default="10m")
    p.add_argument("--d-start", type=int, default=200)
    p.add_argument("--d-step", type=int, default=100)
    p.add_argument("--d-step-duration", default="1m")
    p.add_argument("--d-max", type=int, default=6000)
    p.add_argument("--d-wait-limit", type=float, default=5.0, help="p95 queue wait, seconds")
    p.add_argument("--d-error-limit", type=float, default=0.01)
    args = p.parse_args(argv)
    if args.quick:
        args.warmup, args.a_duration, args.c_duration, args.d_step_duration = (
            "10s",
            "20s",
            "30s",
            "20s",
        )
        args.b_jobs = min(args.b_jobs, 10_000)
    args.a_rates = [int(v) for v in str(args.a_rates).split(",")]
    args.b_workers = [int(v) for v in str(args.b_workers).split(",")]
    wanted = {s.strip().upper() for s in args.scenario.split(",")}
    args.scenarios = ["A", "B", "C", "D"] if "ALL" in wanted else sorted(wanted)
    if not set(args.scenarios) <= {"A", "B", "C", "D"} or args.runs < 1:
        p.error("--scenario takes A, B, C, D or all; --runs at least 1")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    bench = Bench(args)
    try:
        bench.setup()
        env = bench.environment()
        (bench.out / "environment.json").write_text(json.dumps(env, indent=2) + "\n")
        log(f"results: {bench.out.relative_to(ROOT)}")
        for name in args.scenarios:
            getattr(bench, f"scenario_{name.lower()}")()
            summary = render_summary(env, bench.results)
            (bench.out / "summary.md").write_text(summary, encoding="utf-8")
    except ChaosError as exc:
        log(f"cannot go on: {exc}")
        return 2
    finally:
        try:
            bench.teardown()
        except ChaosError as exc:
            log(f"could not put the development stack back (3 workers, one API): {exc}")
    log(f"done: {bench.out.relative_to(ROOT)}/summary.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
