"""Background retry worker (DESIGN.md §19.4, §19.5).

The reference dispatcher retries failed deliveries inline (sleep, re-send) so a
single ``publish`` call blocks until the event is delivered or dead-lettered.
That is simple and correct, but a crash mid-retry loses the in-flight retries.
:class:`RetryWorker` is the production alternative: it drains a durable
:class:`~a2a_events.runtime.contracts.RetryQueue` by re-attempting delivery,
independently of which queue backend is plugged in.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from .contracts import RetryablePublisher, RetryQueue


def _now() -> datetime:
    return datetime.now(UTC)


class RetryWorker:
    """Drains a :class:`RetryQueue` by re-attempting delivery (DESIGN.md §19.4).

    Each due item is re-sent through the publisher; on success it is completed,
    on a retryable failure it is rescheduled with exponential backoff, and at
    the attempt ceiling it is dead-lettered. Run :meth:`run_once` from a timer or
    :meth:`run_forever` as a background task.
    """

    def __init__(
        self,
        publisher: RetryablePublisher,
        queue: RetryQueue,
        *,
        poll_interval: float = 1.0,
        batch: int = 100,
        lease_seconds: int = 60,
        clock: Callable[[], datetime] = _now,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._publisher = publisher
        self._queue = queue
        self._poll_interval = poll_interval
        self._batch = batch
        self._lease_seconds = lease_seconds
        self._clock = clock
        self._sleep = sleep
        self._stopped = asyncio.Event()

    async def run_once(self) -> int:
        """Process all currently-due retries; return how many were attempted."""
        items = await self._publisher.run_offloaded(
            self._queue.claim_due, self._clock(), self._batch, self._lease_seconds
        )
        for item in items:
            await self._publisher.retry_delivery(item, self._queue)
        return len(items)

    async def run_forever(self) -> None:
        self._stopped.clear()
        while not self._stopped.is_set():
            await self.run_once()
            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=self._poll_interval
                )
            except TimeoutError:
                pass

    def stop(self) -> None:
        self._stopped.set()
