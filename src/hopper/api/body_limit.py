from starlette.exceptions import HTTPException
from starlette.types import ASGIApp, Message, Receive, Scope, Send

MAX_BODY_BYTES = 256 * 1024


class BodySizeLimitMiddleware:
    """Reject request bodies over the limit with 413 without reading the rest into memory.

    A FastAPI dependency cannot do this: FastAPI reads the whole body before it runs
    dependencies. This wraps `receive` instead, failing on the declared Content-Length before
    any body is read, or as soon as a streamed (chunked) body passes the limit.
    """

    def __init__(self, app: ASGIApp, max_bytes: int = MAX_BODY_BYTES) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        declared = dict(scope["headers"]).get(b"content-length")
        too_large = HTTPException(413, f"request body exceeds {self.max_bytes} bytes")
        received = 0

        async def limited_receive() -> Message:
            nonlocal received
            # Raised as an HTTPException so FastAPI passes it through to the error handlers.
            if declared is not None and declared.isdigit() and int(declared) > self.max_bytes:
                raise too_large
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise too_large
            return message

        await self.app(scope, limited_receive, send)
