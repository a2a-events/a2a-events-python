"""In-memory durable-style retry queue (spec §19.4, §19.5).

The zero-dependency default :class:`~a2a_events.runtime.contracts.RetryQueue`
backend. It is thread-safe so the publisher (enqueue) and the retry worker
(claim/complete/reschedule) may run on different threads under
``offload_store``, but unlike the Postgres reference its backlog does not
survive a process crash.

Claiming uses a *visibility lease*: :meth:`claim_due` pushes a claimed item's
``next_retry_at`` forward by ``lease_seconds`` and returns it, so a worker that
crashes before completing the retry simply lets the item become due again —
at-least-once retry semantics.
"""

from __future__ import annotations

import threading
from dataclasses import replace
from datetime import datetime, timedelta

from ..contracts import RetryItem


class InMemoryRetryQueue:
    """Thread-safe in-process :class:`RetryQueue`."""

    def __init__(self) -> None:
        self._items: dict[str, RetryItem] = {}
        self._lock = threading.Lock()

    def enqueue(self, item: RetryItem) -> None:
        with self._lock:
            self._items[item.retry_id] = replace(item)

    def claim_due(
        self, now: datetime, limit: int = 100, lease_seconds: int = 60
    ) -> list[RetryItem]:
        with self._lock:
            due = sorted(
                (i for i in self._items.values() if i.next_retry_at <= now),
                key=lambda i: i.next_retry_at,
            )[:limit]
            leased = now + timedelta(seconds=lease_seconds)
            for item in due:
                item.next_retry_at = leased  # visibility lease
            return [replace(i) for i in due]

    def complete(self, retry_id: str) -> None:
        with self._lock:
            self._items.pop(retry_id, None)

    def reschedule(
        self,
        retry_id: str,
        next_retry_at: datetime,
        attempt: int,
        last_error: str | None,
    ) -> None:
        with self._lock:
            item = self._items.get(retry_id)
            if item is not None:
                item.next_retry_at = next_retry_at
                item.attempt = attempt
                item.last_error = last_error

    def pending(self) -> list[RetryItem]:
        with self._lock:
            return [replace(i) for i in self._items.values()]
