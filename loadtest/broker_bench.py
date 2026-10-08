#!/usr/bin/env python3
"""Broker benchmark: the mini broker, in each fsync mode, against the Postgres broker.

The build guide's stretch matrix: connections (1, 8, 32, 64) x fsync mode (always, everysec,
no), measuring the full push -> pull -> ack round trip at p50 and p99, with the Postgres broker
measured the same way. Everything runs in containers on the stack's Docker network: the mini
broker (from the Hopper image, its log on a Docker volume), the client
(loadtest/broker_client.py, in the Hopper image), and the stack's own Postgres (16, default
settings: synchronous_commit on, so every commit waits for its WAL fsync). Each run starts the
broker on an empty log. Runs are interleaved (run 1 of every target, then run 2, ...) so that a
slow moment of the machine does not land on one target only.

    make up && python3 loadtest/broker_bench.py      # 3 runs, about 12 minutes
    python3 loadtest/broker_bench.py --quick         # a try-out, about 2 minutes

Results: loadtest/results/broker-<date>-<host>/summary.md and results.json. Like bench.py it
refuses to run on battery or in power-saver mode, and uses only the standard library.
"""

from __future__ import annotations

import argparse
import json
import platform
import shlex
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from chaos.kill_workers import git_sha, host_description, read_env_file  # noqa: E402
from loadtest.bench import (  # noqa: E402  (the same power check and table cells)
    fmt,
    median_spread,
    power_line,
    power_state,
    throttled,
)

BROKER = "hopper-minibroker-bench"
VOLUME = "hopper-minibroker-bench"
MODES = ("always", "everysec", "no")


def log(message: str) -> None:
    print(f"[{datetime.now(UTC):%H:%M:%S}] {message}", flush=True)


def run(args: list[str], timeout: float = 600) -> str:
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=False)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[-400:]
        raise RuntimeError(f"`{shlex.join(args[:6])} ...` failed: {detail}")
    return result.stdout


# Run inside the broker's container: its STATS, as JSON.
STATS = (
    "import asyncio, json; from hopper.minibroker.client import Client\n"
    "async def m():\n"
    "    c = Client('tcp://127.0.0.1:6390')\n"
    "    r = await c.call('STATS'); await c.close()\n"
    "    print(json.dumps(dict(zip([k.decode() for k in r[::2]], r[1::2]))))\n"
    "asyncio.run(m())"
)


class Bench:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        env = read_env_file(ROOT / args.env_file)
        user, db = env.get("POSTGRES_USER", "hopper"), env.get("POSTGRES_DB", "hopper")
        password = env.get("POSTGRES_PASSWORD", "hopper")
        self.dsn = f"postgresql+asyncpg://{user}:{password}@postgres:5432/{db}"

    def client(self, *target: str) -> list[dict[str, Any]]:
        out = run(
            [
                "docker", "run", "--rm", "--network", self.args.network,
                "-v", f"{ROOT / 'loadtest'}:/bench:ro", self.args.image,
                "python", "/bench/broker_client.py", *target,
                "--connections", self.args.connections,
                "--seconds", str(self.args.seconds), "--warmup", str(self.args.warmup),
            ]
        )  # fmt: skip
        results: list[dict[str, Any]] = json.loads(out.strip().splitlines()[-1])
        return results

    def start_broker(self, mode: str) -> None:
        self.stop_broker()
        run(["docker", "volume", "create", VOLUME])
        run(
            [
                "docker", "run", "-d", "--name", BROKER, "--network", self.args.network,
                "--network-alias", "minibroker", "-v", f"{VOLUME}:/var/lib/minibroker",
                self.args.image, "python", "-m", "hopper.minibroker", "--host", "0.0.0.0",
                "--data", "/var/lib/minibroker",
                "--fsync", mode, "--log-level", "WARNING",
            ]
        )  # fmt: skip
        for _ in range(100):
            probe = subprocess.run(
                ["docker", "exec", BROKER, "python", "-c", STATS],
                capture_output=True, text=True, check=False,
            )  # fmt: skip
            if probe.returncode == 0:
                return
            time.sleep(0.2)
        raise RuntimeError("the mini broker did not start")

    def broker_stats(self) -> dict[str, int]:
        out = run(["docker", "exec", BROKER, "python", "-c", STATS])
        stats: dict[str, int] = json.loads(out.strip().splitlines()[-1])
        return stats

    def stop_broker(self) -> None:
        subprocess.run(["docker", "rm", "-f", BROKER], capture_output=True, check=False)
        subprocess.run(["docker", "volume", "rm", "-f", VOLUME], capture_output=True, check=False)

    def go(self) -> dict[str, Any]:
        results: dict[str, list[list[dict[str, Any]]]] = {"postgres": []}
        for mode in self.args.modes.split(","):
            results[f"mini-{mode}"] = []
        fsyncs: dict[str, list[float]] = {}
        try:
            for i in range(1, self.args.runs + 1):
                log(f"run {i}/{self.args.runs}: postgres")
                results["postgres"].append(self.client("--target", "postgres", "--dsn", self.dsn))
                for mode in self.args.modes.split(","):
                    log(f"run {i}/{self.args.runs}: mini broker, fsync {mode}")
                    self.start_broker(mode)
                    results[f"mini-{mode}"].append(
                        self.client("--target", "mini", "--url", "tcp://minibroker:6390")
                    )
                    stats = self.broker_stats()
                    if stats.get("fsyncs"):
                        per = stats["commands"] / stats["fsyncs"]
                        fsyncs.setdefault(mode, []).append(per)
                    self.stop_broker()
        finally:
            self.stop_broker()
        return {"results": results, "commands_per_fsync": fsyncs}


def table(results: dict[str, list[list[dict[str, Any]]]]) -> list[str]:
    lines = [
        "| Broker | Connections | Round trips/s | p50 ms | p99 ms |",
        "|---|---|---|---|---|",
    ]
    names = {
        "postgres": "Postgres (SKIP LOCKED)",
        "mini-always": "mini, fsync always",
        "mini-everysec": "mini, fsync everysec",
        "mini-no": "mini, fsync no",
    }
    for target, runs in results.items():
        for i, first in enumerate(runs[0]):
            cells = [r[i] for r in runs]
            lines.append(
                f"| {names.get(target, target)} | {first['connections']} "
                f"| {fmt(median_spread([c['round_trips_per_s'] for c in cells]))} "
                f"| {fmt(median_spread([c['p50_ms'] for c in cells]), 2)} "
                f"| {fmt(median_spread([c['p99_ms'] for c in cells]), 2)} |"
            )
    return lines


def render(env: dict[str, Any], data: dict[str, Any]) -> str:
    per_fsync = data["commands_per_fsync"]
    lines = [
        "# Broker benchmark: mini broker versus Postgres",
        "",
        "Measured, not estimated: every number below comes from results.json in this directory. "
        "Each cell is the median of the runs, with the spread (min-max) in brackets when they "
        "differ. A round trip is push, pull and ack of one message, timed from the push to the "
        "ack, by each connection in a loop.",
        "",
        "## Environment",
        "",
        f"- Host: {env['host']}",
        f"- Docker: {env['docker']}",
        "- Everything in containers on one Docker network: the client, the mini broker (its log "
        "on a Docker volume) and the stack's Postgres 16.15 (default settings: "
        "synchronous_commit on, so each commit waits for its WAL fsync)",
        "- Postgres: the production broker's own SQL, one transaction per step (insert, claim "
        "with SKIP LOCKED, fenced ack), on a pool with one connection per client connection",
        "- Mini broker: one TCP connection per client connection, three commands per round trip",
        f"- Runs: {env['args']['runs']}, each {env['args']['seconds']:g} s measured after "
        f"{env['args']['warmup']:g} s of warm-up, per connection count",
        f"- Power: {power_line(env['power'])}",
        f"- Code: git {env['git']}; started {env['started']}",
        "",
        "## Results",
        "",
        *table(data["results"]),
        "",
    ]
    if per_fsync:
        lines += [
            "Group commit: commands answered per fsync, median over the runs: "
            + ", ".join(
                f"{mode} {median_spread(v)['median']:,.0f}" for mode, v in sorted(per_fsync.items())
            )
            + ".",
            "",
        ]
    return "\n".join(lines)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--seconds", type=float, default=10)
    p.add_argument("--warmup", type=float, default=2)
    p.add_argument("--connections", default="1,8,32,64")
    p.add_argument("--modes", default=",".join(MODES))
    p.add_argument("--quick", action="store_true", help="1 run of 3 s: a try-out")
    p.add_argument("--image", default="hopper:dev")
    p.add_argument("--network", default="hopper_default")
    p.add_argument("--env-file", default=".env")
    p.add_argument("--out", default="loadtest/results")
    p.add_argument("--tag", help="results subdirectory (default: broker-<date>-<host>)")
    p.add_argument("--allow-throttled", action="store_true")
    args = p.parse_args(argv)
    if args.quick:
        args.runs, args.seconds, args.warmup = 1, 3.0, 1.0
    if any(m not in MODES for m in args.modes.split(",")):
        p.error(f"--modes takes {', '.join(MODES)}")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    reason = throttled(power_state())
    if reason and not args.allow_throttled:
        log(f"cannot go on: {reason} (or pass --allow-throttled to measure anyway)")
        return 2
    probe = subprocess.run(
        ["docker", "run", "--rm", args.image, "python", "-c", "import hopper.minibroker"],
        capture_output=True, check=False,
    )  # fmt: skip
    if probe.returncode != 0:
        log(f"{args.image} has no hopper.minibroker: rebuild it with `make up` first")
        return 2
    info = run(["docker", "info", "--format", "{{.NCPU}}\t{{.MemTotal}}\t{{.OperatingSystem}}"])
    ncpu, mem, system = info.strip().split("\t")
    env = {
        "host": host_description(),
        "docker": f"{system}, {ncpu} CPUs and {int(mem) / 1024**3:.1f} GiB memory for containers",
        "git": git_sha(),
        "power": power_state(),
        "args": vars(args),
        "started": f"{datetime.now(UTC):%Y-%m-%d %H:%M:%S} UTC",
    }
    data = Bench(args).go()
    tag = args.tag or f"broker-{datetime.now(UTC):%Y-%m-%d}-{platform.node() or 'host'}"
    out = ROOT / args.out / tag
    out.mkdir(parents=True, exist_ok=True)
    (out / "results.json").write_text(json.dumps({"environment": env, **data}, indent=2) + "\n")
    (out / "summary.md").write_text(render(env, data))
    log(f"done: {out / 'summary.md'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
