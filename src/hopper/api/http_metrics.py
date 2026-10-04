import time

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from hopper.metrics import HTTP_REQUESTS, HTTP_SECONDS

_METHODS = frozenset({"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"})


class HttpMetricsMiddleware:
    """Counts and times every response, labelled by route template, method and status.

    The route label is the template (/v1/jobs/{job_id}), never the raw path, and a path that
    matches no route is "unmatched": raw paths and odd methods would give every scanner's
    request its own time series. Plain ASGI, outermost, so the time covers all other
    middleware; a crash is counted as the 500 that the server error handler sends.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = time.perf_counter()
        status = 500

        async def send_and_record(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_and_record)
        finally:
            route = getattr(scope.get("route"), "path", None) or "unmatched"
            method = scope["method"] if scope["method"] in _METHODS else "other"
            HTTP_REQUESTS.labels(route, method, str(status)).inc()
            HTTP_SECONDS.labels(route, method).observe(time.perf_counter() - started)
