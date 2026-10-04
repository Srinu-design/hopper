"""Prometheus metrics. Every process serves its own on an internal port (METRICS_PORT, 9100).

Labels never carry tenant_id or job_id: unbounded label values blow up Prometheus memory.
For the same reason queue names, which tenants choose freely, are labelled as themselves only
when listed in WORKER_QUEUES (the queues workers serve); any other name is reported as
"other". Task names come from the registry, and unknown ones are reported as "unknown".
"""

from collections.abc import Callable

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from hopper.config import get_settings
from hopper.tasks import registry

OTHER_QUEUE = "other"
UNKNOWN_TASK = "unknown"
DEPTH_STATES = ("ready", "delayed", "running", "dead")

# Buckets are chosen on purpose: histogram_quantile interpolates inside a bucket, so coarse
# buckets give a vague p95. Run time spans 5 ms to 60 s (the longest default task timeout is
# 30 s); queue wait goes further out, because a backlog is exactly what it has to show.
RUN_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60)
WAIT_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600)
HTTP_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5)

# API
JOBS_ENQUEUED = Counter(
    "hopper_jobs_enqueued_total", "Jobs created, by the API and by cron.", ["queue", "task"]
)
HTTP_REQUESTS = Counter(
    "hopper_http_requests_total", "API responses, by route template.", ["route", "method", "status"]
)
HTTP_SECONDS = Histogram(
    "hopper_http_request_seconds",
    "API response time, by route template.",
    ["route", "method"],
    buckets=HTTP_BUCKETS,
)
RATELIMIT_REJECTIONS = Counter(
    "hopper_ratelimit_rejections_total",
    "Requests refused by a token bucket (429).",
    ["route_class"],
)
RATELIMIT_FALLBACK = Counter(
    "hopper_ratelimit_fallback_total",
    "Rate-limit decisions made by an in-process bucket because Redis was unavailable.",
)
BACKPRESSURE_REJECTIONS = Counter(
    "hopper_backpressure_rejections_total",
    "Enqueues refused for queue depth: quota (429, the tenant's) or overloaded (503, global).",
    ["reason"],
)

# Worker
JOB_ATTEMPTS = Counter(
    "hopper_job_attempts_total",
    "Finished runs: succeeded, failed, timed_out, lease_expired or released.",
    ["queue", "task", "outcome"],
)
JOBS_DEAD = Counter(
    "hopper_jobs_dead_total", "Jobs moved to the dead-letter queue.", ["queue", "task"]
)
JOB_RUN_SECONDS = Histogram(
    "hopper_job_run_seconds", "Handler run time.", ["queue", "task"], buckets=RUN_BUCKETS
)
JOB_WAIT_SECONDS = Histogram(
    "hopper_job_wait_seconds",
    "Time from run_at (the job became ready) to its claim.",
    ["queue"],
    buckets=WAIT_BUCKETS,
)
WORKER_INFLIGHT = Gauge("hopper_worker_inflight", "Jobs this worker is running now.")
ACKS_REJECTED = Counter(
    "hopper_acks_rejected_total",
    "Acks and nacks refused because the lease was lost (the job was reclaimed).",
)

# Scheduler
QUEUE_DEPTH = Gauge(
    "hopper_queue_depth", "Jobs by state: ready, delayed, running or dead.", ["queue", "state"]
)
OLDEST_READY_AGE = Gauge(
    "hopper_oldest_ready_job_age_seconds",
    "How long the oldest ready job has been waiting; 0 when none is ready.",
    ["queue"],
)
LEASES_RECLAIMED = Counter(
    "hopper_leases_reclaimed_total", "Expired leases taken back by the reaper.", ["queue"]
)


def known_queues() -> list[str]:
    """Queue names that get their own label value, then "other" for everything else."""
    return [*get_settings().queues, OTHER_QUEUE]


def queue_label(queue: str) -> str:
    return queue if queue in get_settings().queues else OTHER_QUEUE


def task_label(task: str) -> str:
    return task if registry.get_task(task) is not None else UNKNOWN_TASK


def serve(port: int) -> Callable[[], None]:
    """Serve this process's metrics on 0.0.0.0:port from a daemon thread.

    Returns a function that stops the server. Port 0 serves nothing. The port is internal:
    Compose never publishes it, and Prometheus reaches it over the Compose network.
    """
    if port == 0:
        return lambda: None
    server, thread = start_http_server(port)

    def stop() -> None:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    return stop
