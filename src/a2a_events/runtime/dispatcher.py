"""Background dispatch worker (spec §15, §19).

The durable-dispatch counterpart to the inline reference mode: with
``PublisherConfig(deferred_dispatch=True)``, ``publish`` only persists the
event, and this worker drives all network delivery from the durable
per-(subscription, topic) high-water positions via
``A2AEventsPublisher.dispatch_pending``.

Because those positions (not an in-memory work list) are the source of truth,
an event is recoverable from the moment ``publish`` persists it: a process
crash before, during, or after the first delivery attempt leaves the position
behind the log head, and the next ``run_once`` — in this process or a
restarted one — finishes the work (at-least-once; duplicates are possible,
loss is not).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Protocol


class DispatchingPublisher(Protocol):
    """The slice of the publisher the dispatch worker depends on."""

    async def dispatch_pending(self) -> int: ...


class DispatchWorker:
    """Periodically delivers every event still owed to a subscription.

    Run :meth:`run_once` after a restart (crash recovery) or from a timer, or
    :meth:`run_forever` as a background task alongside the publisher.
    """

    def __init__(
        self,
        publisher: DispatchingPublisher,
        *,
        poll_interval: float = 1.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._publisher = publisher
        self._poll_interval = poll_interval
        self._sleep = sleep
        self._stopped = asyncio.Event()

    async def run_once(self) -> int:
        """Catch every subscription up to the head; return events processed."""
        return await self._publisher.dispatch_pending()

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
