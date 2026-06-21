"""Background retention compaction (DESIGN.md §31).

The stores filter expired events on read, which keeps replay correct, but the
rows live forever. :class:`RetentionCompactor` periodically calls
``EventStore.compact()`` to physically delete events outside each topic's
retention window. Offsets are tracked by a monotonic per-topic counter (not the
row count), so deleting old rows never causes a cursor to be reused.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Protocol


class Compactable(Protocol):
    def compact(self, topic: str | None = None) -> int: ...


class RetentionCompactor:
    """Periodically compacts an event store (DESIGN.md §31)."""

    def __init__(
        self,
        store: Compactable,
        *,
        interval_seconds: float = 3600.0,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._store = store
        self._interval = interval_seconds
        self._sleep = sleep
        self._stopped = asyncio.Event()

    async def run_once(self) -> int:
        """Compact every topic once; return how many events were removed."""
        return await asyncio.to_thread(self._store.compact)

    async def run_forever(self) -> None:
        self._stopped.clear()
        while not self._stopped.is_set():
            await self.run_once()
            try:
                await asyncio.wait_for(self._stopped.wait(), timeout=self._interval)
            except TimeoutError:
                pass

    def stop(self) -> None:
        self._stopped.set()
