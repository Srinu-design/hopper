"""What each process reports to Prometheus, the labels it uses, the metrics endpoint, and that the
dashboard and alert rules only query metrics that exist."""

import asyncio
import json
import re
import socket
import urllib.request
import uuid
from pathlib import Path

import httpx
import pytest
import structlog
from fastapi import FastAPI
from prometheus_client import REGISTRY
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from structlog.testing import capture_logs

from hopper import metrics
from hopper.queue.postgres import PostgresBroker
from hopper.scheduler.cron import CronLoop
from hopper.scheduler.reaper import Reaper
from hopper.worker import loop as worker_loop
from tests.helpers import (
    ROOT,
    TenantCreds,
    insert_job,
    make_worker,
    run_until,
    seed_tenant,
    status_in,
    wait_for,
)

UUID_RE = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


def value(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


class Delta:
    """Counter and histogram values before and after, since the registry is process-wide."""

    def __init__(self, *samples: tuple[str, dict[str, str]]) -> None:
        self.samples = samples
        self.before = [value(name, **labels) for name, labels in samples]

    def __call__(self) -> list[float]:
        return [
            value(name, **labels) - b
            for (name, labels), b in zip(self.samples, self.before, strict=True)
        ]


def sleep_labels(outcome: str, task: str = "sleep") -> tuple[str, dict[str, str]]:
    return (
        "hopper_job_attempts_total",
        {"queue": "default", "task": task, "outcome": outcome},
    )


# --- worker -----------------------------------------------------------------------------------


async def test_worker_counts_every_outcome_and_times_runs(migrated_engine: AsyncEngine) -> None:
    tenant = await seed_tenant(migrated_engine)
    for _ in range(3):
        await insert_job(migrated_engine, tenant, task="sleep", payload='{"ms": 20}')
    await insert_job(migrated_engine, tenant, task="fail_always", payload='{"permanent": true}')
    # A retryable failure on its last attempt: failed, then dead.
    await insert_job(migrated_engine, tenant, task="flaky", payload='{"p": 1}', max_attempts=1)
    delta = Delta(
        sleep_labels("succeeded"),
        sleep_labels("failed", "fail_always"),
        ("hopper_jobs_dead_total", {"queue": "default", "task": "fail_always"}),
        sleep_labels("failed", "flaky"),
        ("hopper_jobs_dead_total", {"queue": "default", "task": "flaky"}),
        ("hopper_job_run_seconds_count", {"queue": "default", "task": "sleep"}),
        ("hopper_job_run_seconds_sum", {"queue": "default", "task": "sleep"}),
        ("hopper_job_wait_seconds_count", {"queue": "default"}),
    )
    await run_until(
        [make_worker(migrated_engine)],
        migrated_engine,
        lambda c: c.get("succeeded") == 3 and c.get("dead") == 2,
    )
    assert delta()[:5] == [3, 1, 1, 1, 1]
    run_count, run_sum, waits = delta()[5:]
    # Three 20 ms handlers (Windows timers tick every ~16 ms, so no tighter bound than this).
    assert run_count == 3 and 0 < run_sum < 1
    assert waits == 5  # one per claim
    assert value("hopper_worker_inflight") == 0


async def test_a_lost_lease_ack_is_counted(migrated_engine: AsyncEngine) -> None:
    tenant = await seed_tenant(migrated_engine)
    await insert_job(migrated_engine, tenant, task="sleep", payload='{"ms": 1}')
    broker = PostgresBroker(migrated_engine)
    [job] = await broker.claim("default", "w1", 1, 30)
    async with migrated_engine.begin() as conn:  # the reaper gave it to someone else
        await conn.execute(text("UPDATE jobs SET lease_token = gen_random_uuid()"))
    delta = Delta(("hopper_acks_rejected_total", {}), sleep_labels("succeeded"))
    await make_worker(migrated_engine).process(job)
    assert delta() == [1, 0]


async def test_released_runs_are_counted_as_released(migrated_engine: AsyncEngine) -> None:
    tenant = await seed_tenant(migrated_engine)
    job_id = await insert_job(migrated_engine, tenant, task="sleep", payload='{"ms": 30000}')
    worker = make_worker(migrated_engine, shutdown_grace=0.05)
    delta = Delta(sleep_labels("released"), sleep_labels("failed"))
    runner = asyncio.create_task(worker.run())
    await wait_for(status_in(migrated_engine, job_id, "running"))
    worker.stop()
    await runner
    assert delta() == [1, 0]


async def test_queue_wait_is_measured_from_run_at(migrated_engine: AsyncEngine) -> None:
    tenant = await seed_tenant(migrated_engine)
    await insert_job(migrated_engine, tenant, task="sleep", payload='{"ms": 1}')
    async with migrated_engine.begin() as conn:
        await conn.execute(text("UPDATE jobs SET run_at = now() - interval '42 seconds'"))
    [job] = await PostgresBroker(migrated_engine).claim("default", "w1", 1, 30)
    assert 42 <= job.wait_seconds < 45


# --- scheduler --------------------------------------------------------------------------------


async def test_reaper_counts_reclaimed_leases_and_poison_pills(
    migrated_engine: AsyncEngine,
) -> None:
    tenant = await seed_tenant(migrated_engine)
    for attempts in (1, 3):  # the second has no attempts left: it goes to the DLQ
        await insert_job(
            migrated_engine,
            tenant,
            task="sleep",
            status="running",
            attempts=attempts,
            max_attempts=3,
            lease_owner="gone",
            lease_token=uuid.uuid4(),
        )
    async with migrated_engine.begin() as conn:
        await conn.execute(
            text("UPDATE jobs SET lease_expires_at = now() - interval '1 s', started_at = now()")
        )
    delta = Delta(
        sleep_labels("lease_expired"),
        ("hopper_leases_reclaimed_total", {"queue": "default"}),
        ("hopper_jobs_dead_total", {"queue": "default", "task": "sleep"}),
    )
    assert await Reaper(PostgresBroker(migrated_engine)).reap_once() == 2
    assert delta() == [2, 2, 1]


async def test_cron_counts_the_jobs_it_creates(migrated_engine: AsyncEngine) -> None:
    tenant = await seed_tenant(migrated_engine)
    async with migrated_engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO schedules (tenant_id, name, cron, task, payload, next_run_at) "
                "VALUES (:t, 'every-minute', '* * * * *', 'sleep', '{\"ms\": 1}', now())"
            ),
            {"t": tenant},
        )
    delta = Delta(("hopper_jobs_enqueued_total", {"queue": "default", "task": "sleep"}))
    assert await CronLoop(migrated_engine).tick() == 1
    assert delta() == [1]


# --- API --------------------------------------------------------------------------------------


async def test_enqueues_are_counted_with_bounded_queue_labels(
    client: httpx.AsyncClient,
) -> None:
    delta = Delta(
        ("hopper_jobs_enqueued_total", {"queue": "default", "task": "sleep"}),
        ("hopper_jobs_enqueued_total", {"queue": "other", "task": "sleep"}),
    )
    job = {"task": "sleep", "payload": {"ms": 1}}
    await client.post("/v1/jobs", json=job)
    await client.post("/v1/jobs", json={**job, "queue": f"mine-{uuid.uuid4().hex}"})
    await client.post("/v1/jobs", json={**job, "queue": "another-of-mine"})
    assert delta() == [1, 2]


async def test_http_metrics_use_the_route_template_not_the_path(
    client: httpx.AsyncClient,
) -> None:
    by_id = {"route": "/v1/jobs/{job_id}", "method": "GET"}
    delta = Delta(
        ("hopper_http_requests_total", {**by_id, "status": "404"}),
        ("hopper_http_request_seconds_count", by_id),
        ("hopper_http_requests_total", {"route": "unmatched", "method": "GET", "status": "404"}),
        ("hopper_http_requests_total", {"route": "/healthz", "method": "other", "status": "405"}),
    )
    for _ in range(3):
        await client.get(f"/v1/jobs/{uuid.uuid4()}")
    await client.get(f"/no/such/path/{uuid.uuid4()}")
    await client.request("BREW", "/healthz")
    assert delta() == [3, 3, 1, 1]


async def test_the_api_does_not_serve_metrics_on_its_public_port(
    anon_client: httpx.AsyncClient,
) -> None:
    assert (await anon_client.get("/metrics")).status_code == 404


async def test_no_metric_label_carries_an_id(
    client: httpx.AsyncClient, tenant: TenantCreds
) -> None:
    """tenant_id, job_id and raw paths would each make a time series per value."""
    job = (await client.post("/v1/jobs", json={"task": "sleep", "payload": {"ms": 1}})).json()
    await client.get(f"/v1/jobs/{job['id']}")
    await client.post(f"/v1/jobs/{job['id']}/cancel")
    leaks = [
        (sample.name, sample.labels)
        for family in REGISTRY.collect()
        if family.name.startswith("hopper_")
        for sample in family.samples
        if any(UUID_RE.search(v) or str(tenant.id) in v for v in sample.labels.values())
    ]
    assert leaks == []


# --- the metrics endpoint ---------------------------------------------------------------------


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
        return port


def test_the_metrics_endpoint_serves_the_catalogue() -> None:
    port = _free_port()
    stop = metrics.serve(port)
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as resp:
            body = resp.read().decode()
    finally:
        stop()
    for name in ("hopper_job_attempts_total", "hopper_queue_depth", "hopper_http_requests_total"):
        assert f"# TYPE {name.removesuffix('_total')}" in body


def test_port_zero_serves_nothing() -> None:
    metrics.serve(0)()  # returns a no-op stop


# --- logs -------------------------------------------------------------------------------------


async def test_the_enqueue_request_id_follows_the_job_into_worker_logs(
    client: httpx.AsyncClient, api_app: FastAPI, monkeypatch: pytest.MonkeyPatch
) -> None:
    # structlog caches a module's logger on first use, so one used by an earlier test would
    # bypass capture_logs; a fresh one is created inside the capture.
    monkeypatch.setattr(worker_loop, "log", structlog.get_logger())
    job = (
        await client.post(
            "/v1/jobs",
            json={"task": "sleep", "payload": {"ms": 1}},
            headers={"X-Request-ID": "trace-me-42"},
        )
    ).json()
    engine = api_app.state.engine
    with capture_logs() as logs:
        await run_until([make_worker(engine)], engine, lambda c: c.get("succeeded") == 1)
    lines = [e for e in logs if e.get("job_id") == job["id"]]
    assert {e["event"] for e in lines} >= {"job_claimed", "job_succeeded"}
    for line in lines:
        assert line["request_id"] == "trace-me-42"
        assert {"tenant_id", "attempt", "worker_id"} <= line.keys()


# --- the dashboard and alerts query real metrics ----------------------------------------------


def _known_series() -> set[str]:
    """Every series name the registry can produce, from the metric types, even for families
    that have not recorded anything yet."""
    names: set[str] = {"up"}
    for family in REGISTRY.collect():
        if not family.name.startswith("hopper_"):
            continue
        suffixes = {
            "counter": ["_total"],
            "gauge": [""],
            "histogram": ["_bucket", "_sum", "_count"],
        }[family.type]
        names |= {family.name + s for s in suffixes}
    return names


def _queried(text: str) -> set[str]:
    return set(re.findall(r"\b(hopper_[a-z_]+|up)\b(?=\s*[{\[)])", text))


def test_dashboard_panels_query_only_real_metrics() -> None:
    path = ROOT / "deploy/grafana/provisioning/dashboards/hopper.json"
    dashboard = json.loads(path.read_text(encoding="utf-8"))
    panels = [p for p in dashboard["panels"] if p["type"] != "row"]
    exprs = [t["expr"] for p in panels for t in p["targets"]]
    assert _queried("\n".join(exprs)) <= _known_series()
    assert all(p["datasource"]["uid"] == "prometheus" for p in panels)
    # The build guide's definition of done names these five.
    titles = " | ".join(p["title"] for p in panels)
    for required in ("Queue depth", "Failure rate", "p95", "Oldest ready job", "DLQ depth"):
        assert required in titles


def test_alert_rules_query_only_real_metrics() -> None:
    rules = (ROOT / "deploy/prometheus/alerts.yml").read_text(encoding="utf-8")
    queried = _queried(rules)
    assert queried <= _known_series()
    assert {
        "hopper_oldest_ready_job_age_seconds",
        "hopper_job_attempts_total",
        "hopper_jobs_dead_total",
        "up",
    } <= queried


def test_prometheus_scrapes_every_process_on_the_metrics_port() -> None:
    config = (ROOT / "deploy/prometheus/prometheus.yml").read_text(encoding="utf-8")
    for service in ("api", "worker", "scheduler"):
        assert f"job_name: {service}" in config and f"names: [{service}]" in config
    assert config.count("port: 9100") == 3


def test_paths_are_where_compose_mounts_them() -> None:
    compose = (ROOT / "docker/compose.yaml").read_text(encoding="utf-8")
    assert "../deploy/prometheus:/etc/prometheus:ro" in compose
    assert "../deploy/grafana/provisioning:/etc/grafana/provisioning:ro" in compose
    assert Path(ROOT / "deploy/grafana/provisioning/datasources/prometheus.yml").exists()
