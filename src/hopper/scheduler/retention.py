import asyncio
import contextlib

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from hopper.queue import sql

log = structlog.get_logger()


async def delete_finished(engine: AsyncEngine, *, days: int, limit: int) -> int:
    """Delete up to `limit` succeeded or cancelled jobs that finished more than `days` ago.
    Returns how many."""
    async with engine.begin() as conn:
        deleted: int = (
            await conn.execute(text(sql.DELETE_FINISHED), {"days": days, "limit": limit})
        ).scalar_one()
    return deleted


class Retention:
    """Deletes finished jobs once they are `days` old, which also frees their idempotency keys.

    Runs every `interval` seconds (hourly by default). Each batch is one short transaction
    with SKIP LOCKED, so two schedulers can run it at once: each job is deleted once. Dead
    jobs are never deleted; they stay in the DLQ until replayed.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        days: int = 7,
        interval: float = 3600.0,
        batch_size: int = 1000,
    ) -> None:
        self._engine = engine
        self._days = days
        self._interval = interval
        self._batch_size = batch_size
        self._stopping = asyncio.Event()

    def stop(self) -> None:
        self._stopping.set()

    async def run(self) -> None:
        log.info(
            "retention_started",
            days=self._days,
            interval=self._interval,
            batch_size=self._batch_size,
        )
        while not self._stopping.is_set():
            try:
                deleted = await self.delete_once()
            except Exception:
                log.exception("retention_failed")
            else:
                if deleted:
                    log.info("retention_deleted", jobs=deleted, days=self._days)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopping.wait(), timeout=self._interval)
        log.info("retention_stopped")

    async def delete_once(self) -> int:
        """Delete every job past retention by now, in batches. Returns how many.

        A large backlog (the first pass on an old database) is deleted batch after batch,
        but a stop request ends the pass after the current batch.
        """
        total = 0
        while True:
            deleted = await delete_finished(self._engine, days=self._days, limit=self._batch_size)
            total += deleted
            if deleted < self._batch_size or self._stopping.is_set():
                return total
