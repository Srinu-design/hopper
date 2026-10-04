class PermanentError(Exception):
    """Raise from a handler when retrying cannot help: the job goes straight to the DLQ."""


class RetryableError(Exception):
    """Raise from a handler to retry, optionally no sooner than `retry_after` seconds.

    The worker waits for the larger of retry_after and its own backoff delay, so a target
    that answers with Retry-After is never hit earlier than it asked.
    """

    def __init__(self, message: str = "", *, retry_after: float | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after
