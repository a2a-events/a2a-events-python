"""Runtime contracts: the backend SPI for A2A Events publishers.

This module is the *seam* between the publisher runtime and its storage and
transport backends. It defines only contracts — typed :class:`Protocol`
interfaces plus the plain data records exchanged across them — and deliberately
holds **no** implementation and **no** third-party imports. The publisher
depends on these names alone, so any backend that satisfies the protocols can be
plugged in: the zero-dependency in-memory reference (``runtime.memory``), the
optional batteries-included Postgres reference (``runtime.postgres``), or your
own adapter against Redis, Kafka, DynamoDB, a message bus, etc.

Contracts here:

- :class:`EventStore` — the per-topic append-only event log with opaque,
  ordered cursors (DESIGN.md §7.3, §10.9, §20, §31).
- :class:`SubscriptionStore` — durable subscriptions, per-topic high-water
  positions, acks, and delivery attempts (§14, §25).
- :class:`RetryQueue` — durable backlog of pending delivery retries (§19.4).
- :class:`Transport` — the delivery transport for webhook and A2A-message
  delivery (§18).
- :class:`RetryablePublisher` — the slice of the publisher the retry worker
  depends on.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, TypeVar

from ..models import Subscription, Topic

_T = TypeVar("_T")


# --- id helpers (shared by every backend so records are minted uniformly) ---
def new_event_id() -> str:
    return "evt_" + uuid.uuid4().hex


def new_attempt_id() -> str:
    return "da_" + uuid.uuid4().hex


def new_retry_id() -> str:
    return "rty_" + uuid.uuid4().hex


# --- event store contract (DESIGN.md §7.3, §10.9) ---
@dataclass
class EventRecord:
    event_id: str
    topic: str
    cursor: str
    event_type: str
    source: str
    data: dict[str, Any]
    subject: str | None
    created_at: datetime
    content_hash: str


class EventStore(Protocol):
    """Publisher event-store interface (DESIGN.md §7.3, §10.9, §20, §31).

    The publisher depends only on these methods, so any backend (in-memory,
    Postgres, ...) can be plugged in. Cursors are opaque, per-topic, and
    totally ordered by byte-wise lexicographic comparison.
    """

    def declare_topic(self, topic: Topic) -> None: ...

    def get_topic(self, name: str) -> Topic: ...

    def topics(self) -> list[Topic]: ...

    def append(
        self,
        topic: str,
        event_type: str,
        source: str,
        data: dict[str, Any],
        subject: str | None = None,
    ) -> EventRecord: ...

    def count(self, topic: str) -> int:
        """Total events ever appended to ``topic`` (ignores retention)."""
        ...

    def oldest_available_cursor(self, topic: str) -> str | None: ...

    def latest_cursor(self, topic: str) -> str: ...

    def read(
        self,
        topic: str,
        from_cursor: str | None = None,
        to_cursor: str | None = None,
        limit: int = 100,
    ) -> tuple[list[EventRecord], str | None]: ...

    def compact(self, topic: str | None = None) -> int:
        """Physically delete events outside the retention window (§31).

        Returns the number of events removed. A non-positive ``retentionSeconds``
        means "retain forever" and is skipped.
        """
        ...


# --- subscription store contract (DESIGN.md §14, §25) ---
@dataclass
class DeadLetter:
    """An event permanently undeliverable to a subscription (§19)."""

    subscription_id: str
    event_id: str
    cursor: str
    reason: str


@dataclass
class DeliveryAttempt:
    """One delivery attempt of an event to a subscription (§25).

    ``status`` is one of ``"delivered"`` (acked), ``"retry"`` (failed, will be
    retried), or ``"dead_letter"`` (terminal failure). The dead-letter queue is
    just the set of ``"dead_letter"`` attempts.
    """

    delivery_attempt_id: str
    subscription_id: str
    event_id: str
    cursor: str
    attempt: int
    status: str
    last_status_code: int | None = None
    last_error: str | None = None


class SubscriptionStore(Protocol):
    """Publisher subscription-store interface (DESIGN.md §14, §25).

    The publisher depends only on these methods, so any backend (in-memory,
    Postgres, ...) can be plugged in independently of the event-store backend.
    """

    def add(self, sub: Subscription, high_water: dict[str, int]) -> None:
        """Persist a new subscription with its initial per-topic high-water."""
        ...

    def get(self, subscription_id: str) -> Subscription | None: ...

    def list_all(self) -> list[Subscription]: ...

    def update(self, sub: Subscription) -> None:
        """Persist mutable subscription fields (status, lease, acked cursors)."""
        ...

    def high_water(self, subscription_id: str) -> dict[str, int]: ...

    def set_high_water(self, subscription_id: str, topic: str, offset: int) -> None: ...

    def record_ack(self, subscription_id: str, event_id: str, cursor: str) -> None: ...

    def record_attempt(self, attempt: DeliveryAttempt) -> None: ...

    def delivery_attempts(self, subscription_id: str) -> list[DeliveryAttempt]: ...

    def dead_letters(self) -> list[DeadLetter]: ...


# --- retry queue contract (DESIGN.md §19.4, §19.5) ---
@dataclass
class RetryItem:
    """One event pending re-delivery to one subscription (DESIGN.md §19.4)."""

    retry_id: str
    subscription_id: str
    topic: str
    cursor: str
    event_id: str
    attempt: int  # number of delivery attempts already made
    next_retry_at: datetime
    last_error: str | None = None


class RetryQueue(Protocol):
    """Durable backlog of pending delivery retries."""

    def enqueue(self, item: RetryItem) -> None: ...

    def claim_due(
        self, now: datetime, limit: int = 100, lease_seconds: int = 60
    ) -> list[RetryItem]:
        """Atomically claim due items, leasing them for ``lease_seconds``."""
        ...

    def complete(self, retry_id: str) -> None:
        """Remove a retry that succeeded or terminally failed."""
        ...

    def reschedule(
        self,
        retry_id: str,
        next_retry_at: datetime,
        attempt: int,
        last_error: str | None,
    ) -> None: ...

    def pending(self) -> list[RetryItem]:
        """All queued items (for inspection/tests)."""
        ...


class RetryablePublisher(Protocol):
    """The slice of the publisher the retry worker depends on."""

    async def run_offloaded(self, fn: Callable[..., _T], *args: object) -> _T:
        """Run a (possibly blocking) callable, offloading to a thread if configured."""
        ...

    async def retry_delivery(self, item: RetryItem, queue: RetryQueue) -> None: ...


# --- delivery transport contract (DESIGN.md §18) ---
@dataclass
class DeliveryResult:
    """Outcome of one delivery attempt (DESIGN.md §10.10, §19)."""

    ack: bool
    retry: bool = True
    status_code: int | None = None
    reason: str | None = None


class Transport(Protocol):
    async def send_webhook(
        self, url: str, headers: dict[str, str], body: dict[str, Any]
    ) -> DeliveryResult: ...

    async def send_a2a_message(
        self, endpoint: str, message: dict[str, Any]
    ) -> DeliveryResult: ...
