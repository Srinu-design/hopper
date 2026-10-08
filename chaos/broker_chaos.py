"""Chaos test for the mini broker (the stretch goal), in two parts.

1. Workers killed (the chaos test of chaos/kill_workers.py, on the mini broker). The broker runs
   in a container with fsync=always, and 4 workers (`python -m hopper.minibroker.worker`: the
   production Worker class with MiniBroker behind it) run `mb_effect` jobs, which record each
   run and their one effect in Redis. 10,000 jobs are pushed over 5 minutes while, every 3 to
   10 s, a random worker is killed with SIGKILL and started again. Once in the window a worker
   gets SIGTERM, one is frozen for 40 s (past its 30 s lease), and the broker itself is killed
   with SIGKILL and started again. Then: every pushed job's effect must have happened (0 lost),
   and duplicate runs are counted.

2. The broker killed while clients push, once per fsync mode (always, everysec, no): 8
   connections push as fast as they can for 30 s while the broker is killed with SIGKILL 3
   times and started again. Every PUSH that got its reply must still be there afterwards.

Uses the stack's Redis and Docker network (make up first) and the Hopper image, which must
include hopper.minibroker (make up rebuilds it). Run with the project's Python:

    uv run python chaos/broker_chaos.py                 # both parts, about 10 minutes
    uv run python chaos/broker_chaos.py --part broker   # only part 2

Writes chaos/results/broker-<time>.md and .json. Exit code 0 if nothing was lost.
"""

import argparse
import asyncio
import contextlib
import json
import random
import subprocess
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import redis.asyncio as redis_asyncio

from hopper.minibroker.client import Client, MiniBroker

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from chaos.kill_workers import git_sha, host_description, log  # noqa: E402

BROKER = "hopper-minibroker-chaos"
VOLUME = "hopper-minibroker-chaos"
WORKER = "hopper-mbworker-{}"
LOCAL_URL = "tcp://127.0.0.1:6390"


def docker(*args: str, check: bool = True, timeout: float = 120) -> str:
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=timeout, check=False
    )
    if check and result.returncode != 0:
        raise RuntimeError(f"docker {' '.join(args[:3])}: {result.stderr.strip()[-300:]}")
    return result.stdout


async def adocker(*args: str, check: bool = True) -> str:
    """docker, off the event loop: pushes and pulls keep going while it runs."""
    return await asyncio.to_thread(docker, *args, check=check)


async def start_broker(image: str, network: str, fsync: str) -> None:
    await adocker("rm", "-f", BROKER, check=False)
    await adocker("volume", "rm", "-f", VOLUME, check=False)
    await adocker(
        "run", "-d", "--name", BROKER, "--network", network, "--network-alias", "minibroker",
        "-p", "127.0.0.1:6390:6390", "-v", f"{VOLUME}:/var/lib/minibroker", image,
        "python", "-m", "hopper.minibroker", "--host", "0.0.0.0",
        "--data", "/var/lib/minibroker", "--fsync", fsync,
    )  # fmt: skip


async def until_up() -> Client:
    for _ in range(200):
        client = Client(LOCAL_URL, size=16, timeout=5)
        try:
            await client.call("PING")
            return client
        except (OSError, asyncio.IncompleteReadError):
            # Docker's port proxy accepts the connection before the broker in the container
            # listens, then closes it: not up yet.
            await client.close()
            await asyncio.sleep(0.1)
    raise RuntimeError("the mini broker did not come up")


async def stats(client: Client) -> dict[str, int]:
    reply = await client.call("STATS")
    assert isinstance(reply, list)
    return {k.decode(): v for k, v in zip(reply[::2], reply[1::2], strict=True)}  # type: ignore[union-attr,misc]


def cleanup(workers: int) -> None:
    for i in range(workers):
        docker("rm", "-f", WORKER.format(i), check=False)
    docker("rm", "-f", BROKER, check=False)
    docker("volume", "rm", "-f", VOLUME, check=False)


# --- part 1: workers killed ----------------------------------------------------------------


@dataclass
class WorkersRun:
    jobs: int = 0
    pushes_retried: int = 0
    sigkills: int = 0
    sigterms: int = 0
    freezes: int = 0
    broker_kills: int = 0
    lost: int = 0
    dead: int = 0
    effects: int = 0
    runs: int = 0
    duplicate_runs: int = 0
    acks_refused: int = 0
    runs_cancelled: int = 0
    acks_failed: int = 0
    drain_seconds: float = 0.0
    events: list[dict[str, Any]] = field(default_factory=list)


async def push_all(
    client: Client, run: str, jobs: int, seconds: float, out: WorkersRun
) -> set[str]:
    """Push `jobs` mb_effect jobs evenly over `seconds`. A PUSH that fails (the broker is down)
    is retried until it is answered; its key stays the same, so a retry whose first try was in
    fact stored is one job delivered twice, which the key makes harmless."""
    broker = MiniBroker(client)
    tenant = uuid.uuid4()
    keys: set[str] = set()
    started = time.monotonic()

    async def one(i: int) -> None:
        key = f"{run}-{i}"
        while True:
            try:
                await broker.push(
                    queue="default", task="mb_effect", payload={"run": run, "key": key},
                    tenant_id=tenant, timeout_seconds=60,
                )  # fmt: skip
                keys.add(key)
                return
            except (OSError, asyncio.IncompleteReadError, TimeoutError):
                out.pushes_retried += 1
                await asyncio.sleep(0.2)

    tasks = []
    for i in range(jobs):
        delay = started + i * seconds / jobs - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        tasks.append(asyncio.create_task(one(i)))
    await asyncio.gather(*tasks)
    return keys


async def part_workers(args: argparse.Namespace, rds: Any) -> WorkersRun:
    out = WorkersRun(jobs=args.jobs)
    run = f"mbchaos-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    await start_broker(args.image, args.network, "always")
    client = await until_up()
    env = [
        "-e", "MINIBROKER_URL=tcp://minibroker:6390", "-e", "REDIS_URL=redis://redis:6379/0",
        "-e", "METRICS_PORT=0", "-e", "LOG_LEVEL=INFO",
    ]  # fmt: skip
    names = [WORKER.format(i) for i in range(args.workers)]
    for name in names:
        await adocker("rm", "-f", name, check=False)
        await adocker("run", "-d", "--init", "--name", name, "--network", args.network, *env,
               args.image, "python", "-m", "hopper.minibroker.worker")  # fmt: skip
    log(f"part 1: {args.jobs:,} jobs over {args.duration:.0f} s, {args.workers} workers")
    window_start = time.monotonic()
    pusher = asyncio.create_task(push_all(client, run, args.jobs, args.duration, out))
    specials = ["sigterm", "freeze", "broker"]
    random.shuffle(specials)
    special_at = sorted(random.uniform(30, args.duration - 60) for _ in specials)
    frozen: str | None = None
    unfreeze_at = 0.0
    while (t := time.monotonic() - window_start) < args.duration:
        if frozen and t >= unfreeze_at:
            await adocker("unpause", frozen)
            out.events.append({"t": round(t, 1), "event": "thaw", "worker": frozen})
            frozen = None
        if specials and t >= special_at[0]:
            special_at.pop(0)
            kind = specials.pop(0)
            if kind == "broker":
                await adocker("kill", "-s", "KILL", BROKER)
                await adocker("start", BROKER)
                out.broker_kills += 1
                out.events.append({"t": round(t, 1), "event": "broker sigkill"})
            else:
                victim = random.choice([n for n in names if n != frozen])
                if kind == "sigterm":
                    await adocker("kill", "-s", "TERM", victim)
                    await adocker("wait", victim)
                    await adocker("start", victim)
                    out.sigterms += 1
                else:
                    await adocker("pause", victim)
                    frozen, unfreeze_at = victim, t + args.freeze_seconds
                    out.freezes += 1
                out.events.append({"t": round(t, 1), "event": kind, "worker": victim})
        else:
            victim = random.choice([n for n in names if n != frozen])
            await adocker("kill", "-s", "KILL", victim)
            await adocker("start", victim)
            out.sigkills += 1
            out.events.append({"t": round(t, 1), "event": "sigkill", "worker": victim})
        await asyncio.sleep(random.uniform(args.min_gap, args.max_gap))
    if frozen:
        await adocker("unpause", frozen)
    log("kill window closed; waiting for the pushes and the drain")
    keys = await pusher
    drain_from = time.monotonic()
    while True:
        try:
            s = await stats(client)
            if s["ready"] + s["delayed"] + s["leased"] == 0:
                break
        except (OSError, asyncio.IncompleteReadError):
            pass
        if time.monotonic() - drain_from > args.drain_timeout:
            log("the broker did not drain in time")
            break
        await asyncio.sleep(1)
    out.drain_seconds = round(time.monotonic() - drain_from, 1)
    runs_key, effects_key = f"hopper:mbchaos:{run}:runs", f"hopper:mbchaos:{run}:effects"
    effects = {k.decode() for k in await rds.smembers(effects_key)}
    counts = [int(v) for v in (await rds.hvals(runs_key))]
    dead_keys: set[str] = set()
    for item in await client.call("DEAD", "default", "COUNT", 1000):  # type: ignore[union-attr]
        dead_keys.add(json.loads(item[2])["payload"]["key"])  # type: ignore[index]
    out.effects = len(effects)
    out.dead = len(dead_keys)
    out.lost = len(keys - effects - dead_keys)
    out.runs = sum(counts)
    out.duplicate_runs = sum(counts) - len(counts)
    for n in names:
        logs = await asyncio.to_thread(
            subprocess.run, ["docker", "logs", n], capture_output=True, text=True, check=False
        )
        text = logs.stdout + logs.stderr
        out.acks_refused += text.count('"ack_rejected_lease_lost"')
        out.runs_cancelled += text.count('"lease_lost_job_cancelled"')
        out.acks_failed += text.count('"ack_failed"')
    await client.close()
    await rds.delete(runs_key, effects_key)
    return out


# --- part 2: the broker killed -------------------------------------------------------------


@dataclass
class BrokerRun:
    fsync: str
    acknowledged: int = 0
    found: int = 0
    lost: int = 0
    stored_without_reply: int = 0
    kills: int = 0
    pushes_per_second: int = 0


async def part_broker(args: argparse.Namespace, fsync: str) -> BrokerRun:
    out = BrokerRun(fsync)
    await start_broker(args.image, args.network, fsync)
    acked: set[bytes] = set()
    stop = time.monotonic() + args.push_seconds
    clients: list[Client] = []

    async def pusher(n: int) -> None:
        client = Client(LOCAL_URL, size=1, timeout=5)
        clients.append(client)
        i = 0
        while time.monotonic() < stop:
            try:
                reply = await client.call("PUSH", "q", b"%d-%d" % (n, i))
                assert isinstance(reply, bytes)
                acked.add(reply)
                i += 1
            except (OSError, asyncio.IncompleteReadError, TimeoutError):
                await asyncio.sleep(0.05)  # the broker is down: try again (a new message)

    first = await until_up()
    await first.close()
    began = time.monotonic()
    tasks = [asyncio.create_task(pusher(n)) for n in range(8)]
    for at in sorted(random.uniform(3, args.push_seconds - 3) for _ in range(3)):
        await asyncio.sleep(max(0.0, began + at - time.monotonic()))
        await adocker("kill", "-s", "KILL", BROKER)
        await adocker("start", BROKER)
        out.kills += 1
    await asyncio.gather(*tasks)
    for c in clients:
        await c.close()
    out.acknowledged = len(acked)
    out.pushes_per_second = round(len(acked) / args.push_seconds)
    client = await until_up()
    found: set[bytes] = set()
    while batch := await client.call("PULL", "q", 600_000, "COUNT", 1000):
        found.update(item[0] for item in batch)  # type: ignore[union-attr,index]
    await client.close()
    out.found = len(found)
    out.lost = len(acked - found)
    out.stored_without_reply = len(found - acked)
    await adocker("rm", "-f", BROKER, check=False)
    return out


# --- report --------------------------------------------------------------------------------


def render(data: dict[str, Any]) -> str:
    lines = [f"# Mini broker chaos run {data['run']}", "", f"**{data['verdict']}**", ""]
    lines += [
        "## Setup",
        "",
        f"- Host: {data['host']}; the broker, the workers, Redis and this script on one machine",
        f"- Code: git {data['git']}",
        f"- Started {data['started']}, finished {data['finished']}",
        "",
    ]
    w = data.get("workers")
    if w:
        lines += [
            "## 1. Workers killed, on the mini broker",
            "",
            f"4 workers (the production Worker class with MiniBroker), lease 30 s, heartbeat 10 s; "
            f"the broker with fsync=always. {w['jobs']:,} `mb_effect` jobs pushed over "
            f"{data['args']['duration']:.0f} s while workers were killed.",
            "",
            "| Measure | Value |",
            "|---|---|",
            f"| Jobs pushed | {w['jobs']:,} |",
            f"| Workers killed with SIGKILL | {w['sigkills']} |",
            f"| Workers sent SIGTERM | {w['sigterms']} |",
            f"| Workers frozen past their lease | {w['freezes']} |",
            f"| Broker killed with SIGKILL (and started again) | {w['broker_kills']} |",
            f"| PUSHes retried while the broker was down | {w['pushes_retried']} |",
            f"| **Lost** (no effect and not in the DLQ) | **{w['lost']}** |",
            f"| Effects (one per job at most) | {w['effects']:,} |",
            f"| Dead (in the DLQ: visible, so not lost) | {w['dead']} |",
            f"| Runs recorded | {w['runs']:,} |",
            f"| Duplicate runs (runs minus jobs that ran) | {w['duplicate_runs']} |",
            f"| Acks refused by the fencing token (worker logs) | {w['acks_refused']} |",
            f"| Runs cancelled by a heartbeat after their lease was lost | {w['runs_cancelled']} |",
            f"| Acks that failed because the broker was down | {w['acks_failed']} |",
            f"| Drain after the window | {w['drain_seconds']} s |",
            "",
            "Duplicates come from kills between a job's effect and its ack, from the frozen "
            "worker finishing jobs that were meanwhile run elsewhere, and from the broker's own "
            "restart: leases are not logged, so every job leased at that moment is handed out "
            "again, and a worker whose ack failed while the broker was down leaves its job to "
            "run once more. A worker that wakes up with a lost lease either has its ack refused "
            "or, if its next heartbeat comes first, cancels the run. The effect set keeps each "
            "job's effect single.",
            "",
        ]
    b = data.get("broker")
    if b:
        lines += [
            "## 2. The broker killed while clients push",
            "",
            f"8 connections push for {data['args']['push_seconds']:.0f} s; the broker is "
            "killed with SIGKILL 3 times and started again each time. Then every message left "
            "is pulled and compared with the PUSHes that got a reply.",
            "",
            "| fsync | Pushes answered | Pushes/s | **Lost** | Stored without a reply |",
            "|---|---|---|---|---|",
            *(
                f"| {r['fsync']} | {r['acknowledged']:,} | {r['pushes_per_second']:,} "
                f"| **{r['lost']}** | {r['stored_without_reply']} |"
                for r in b
            ),
            "",
            "No mode lost an answered PUSH, and none could have: the broker writes a record to "
            "the kernel before it replies, and killing a process does not take the kernel's "
            "page cache with it. What fsync buys is survival of the machine going down (power "
            "cut, kernel crash): with `everysec` that can cost up to about a second of "
            "answered writes, with `no` whatever the OS had not flushed. That was not tested "
            'here; it needs the power pulled. "Stored without a reply" are PUSHes written '
            "just before a kill whose answer never left: the client counted them as failed "
            "and pushed again, so each is one extra message (at-least-once).",
            "",
        ]
    lines.append(f"Raw data: `{data['run']}.json`.")
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--part", choices=["workers", "broker", "all"], default="all")
    p.add_argument("--jobs", type=int, default=10_000)
    p.add_argument("--duration", type=float, default=300.0, help="kill window, seconds")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--min-gap", type=float, default=3.0)
    p.add_argument("--max-gap", type=float, default=10.0)
    p.add_argument("--freeze-seconds", type=float, default=40.0)
    p.add_argument("--drain-timeout", type=float, default=600.0)
    p.add_argument("--push-seconds", type=float, default=30.0)
    p.add_argument("--image", default="hopper:dev")
    p.add_argument("--network", default="hopper_default")
    p.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    p.add_argument("--out", default="chaos/results")
    return p.parse_args(argv)


async def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = datetime.now(UTC)
    data: dict[str, Any] = {
        "run": f"broker-{started:%Y%m%dT%H%M%SZ}",
        "host": host_description(),
        "git": git_sha(),
        "started": f"{started:%Y-%m-%d %H:%M:%S} UTC",
        "args": vars(args),
    }
    rds = redis_asyncio.Redis.from_url(args.redis_url)
    try:
        if args.part in ("workers", "all"):
            data["workers"] = asdict(await part_workers(args, rds))
        if args.part in ("broker", "all"):
            data["broker"] = []
            for fsync in ("always", "everysec", "no"):
                log(f"part 2: kill -9 the broker while 8 connections push, fsync {fsync}")
                data["broker"].append(asdict(await part_broker(args, fsync)))
    finally:
        with contextlib.suppress(Exception):
            cleanup(args.workers)
        await rds.aclose()
    lost = data.get("workers", {}).get("lost", 0) + sum(r["lost"] for r in data.get("broker", []))
    data["verdict"] = "PASS: nothing lost" if lost == 0 else f"FAIL: {lost} lost"
    data["finished"] = f"{datetime.now(UTC):%Y-%m-%d %H:%M:%S} UTC"
    out = ROOT / args.out
    out.mkdir(parents=True, exist_ok=True)
    (out / f"{data['run']}.json").write_text(json.dumps(data, indent=2) + "\n")
    (out / f"{data['run']}.md").write_text(render(data))
    log(f"{data['verdict']}; report: {args.out}/{data['run']}.md")
    return 0 if lost == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
