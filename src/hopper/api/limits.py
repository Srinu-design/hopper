"""Rate limits and backpressure for /v1, applied right after the API key is verified.

Every /v1 route depends on exactly one of `EnqueueTenant` and `ReadTenant`, which give it the
tenant id. Each tenant has one token bucket per route class: capacity = burst, refill =
rate_per_sec (ADR-0008).

- enqueue: routes that put jobs in the queue (enqueue, replay, bulk replay). They also pass
  the backpressure gate: 429 over the tenant's queue quota, 503 over the global one (ADR-0009).
- read: every other /v1 call. Most are reads; the rest (cancel, schedule changes) are
  single-row writes that add no work to the queue.
"""

from typing import Annotated, Literal
from uuid import UUID

from fastapi import Depends, Request

from hopper.api.errors import ApiError
from hopper.auth.api_keys import Caller
from hopper.auth.tenant import current_tenant
from hopper.config import get_settings
from hopper.metrics import BACKPRESSURE_REJECTIONS, RATELIMIT_REJECTIONS
from hopper.ratelimit.backpressure import Verdict
from hopper.ratelimit.limiter import Decision

RouteClass = Literal["enqueue", "read", "login"]


async def take_token(
    request: Request, route_class: RouteClass, bucket: str, *, capacity: int, rate: float
) -> None:
    """Spend one token or raise 429. The decision is kept on the request, so the response
    gets X-RateLimit-Limit and X-RateLimit-Remaining whatever happens next."""
    decision: Decision = await request.app.state.limiter.take(bucket, capacity=capacity, rate=rate)
    request.state.rate_limit = decision
    if not decision.allowed:
        RATELIMIT_REJECTIONS.labels(route_class).inc()
        raise ApiError(
            429,
            "rate_limited",
            f"rate limit exceeded; retry in {decision.retry_after_ms} ms",
            headers={"Retry-After": str(decision.retry_after_seconds)},
            details={"retry_after_ms": decision.retry_after_ms},
        )


async def _limited(request: Request, caller: Caller, route_class: RouteClass) -> None:
    await take_token(
        request,
        route_class,
        f"{caller.tenant_id}:{route_class}",
        capacity=caller.burst,
        rate=caller.rate_per_sec,
    )


async def read_tenant(request: Request, caller: Annotated[Caller, Depends(current_tenant)]) -> UUID:
    await _limited(request, caller, "read")
    return caller.tenant_id


async def enqueue_tenant(
    request: Request, caller: Annotated[Caller, Depends(current_tenant)]
) -> UUID:
    await _limited(request, caller, "enqueue")
    verdict = await request.app.state.backpressure.check(caller.tenant_id, caller.max_queue_depth)
    if verdict is None:
        return caller.tenant_id
    BACKPRESSURE_REJECTIONS.labels(verdict.value).inc()
    retry = {"Retry-After": str(get_settings().backpressure_retry_after_seconds)}
    if verdict is Verdict.QUOTA:
        raise ApiError(
            429,
            "queue_quota_exceeded",
            f"this tenant already has {caller.max_queue_depth} or more jobs queued; "
            "retry once some have run",
            headers=retry,
        )
    raise ApiError(503, "overloaded", "the service is overloaded; retry later", headers=retry)


ReadTenant = Annotated[UUID, Depends(read_tenant)]
EnqueueTenant = Annotated[UUID, Depends(enqueue_tenant)]
