"""In-memory durable subscription state (DESIGN.md §14, §25).

The zero-dependency default
:class:`~a2a_events.runtime.contracts.SubscriptionStore` backend. It keeps
identical semantics to the Postgres reference for tests and the default runtime;
the only difference is durability across a restart.

High-water marks are integer per-topic dispatch offsets (``-1`` = "before the
first event"): the highest offset already *handed off* to delivery, whether it
was acked or dead-lettered. They are distinct from the public per-topic acked
cursors on the :class:`~a2a_events.models.Subscription` — a dead-lettered event
advances the high-water without advancing the acked cursor.
"""

from __future__ import annotations

from ...models import Subscription
from ..contracts import DeadLetter, DeliveryAttempt


class InMemorySubscriptionStore:
    """Reference in-process subscription state.

    Subscriptions are held by live reference (not copied), so a caller that
    mutates a returned :class:`Subscription` sees the change reflected on the
    next read — matching the publisher's original in-memory semantics.
    """

    def __init__(self) -> None:
        self._subs: dict[str, Subscription] = {}
        # Per-(subscription, topic) high-water offset (-1 = before first event).
        self._hw: dict[str, dict[str, int]] = {}
        self._attempts: list[DeliveryAttempt] = []
        # (subscription_id, event_id) -> acked cursor.
        self._acks: dict[tuple[str, str], str] = {}

    def add(self, sub: Subscription, high_water: dict[str, int]) -> None:
        self._subs[sub.subscription_id] = sub
        self._hw[sub.subscription_id] = dict(high_water)

    def get(self, subscription_id: str) -> Subscription | None:
        return self._subs.get(subscription_id)

    def list_all(self) -> list[Subscription]:
        return list(self._subs.values())

    def update(self, sub: Subscription) -> None:
        # Live references are already current; keep the write explicit so the
        # seam matches the Postgres backend.
        self._subs[sub.subscription_id] = sub

    def high_water(self, subscription_id: str) -> dict[str, int]:
        return dict(self._hw.get(subscription_id, {}))

    def set_high_water(self, subscription_id: str, topic: str, offset: int) -> None:
        self._hw.setdefault(subscription_id, {})[topic] = offset

    def record_ack(self, subscription_id: str, event_id: str, cursor: str) -> None:
        self._acks[(subscription_id, event_id)] = cursor

    def record_attempt(self, attempt: DeliveryAttempt) -> None:
        self._attempts.append(attempt)

    def delivery_attempts(self, subscription_id: str) -> list[DeliveryAttempt]:
        return [a for a in self._attempts if a.subscription_id == subscription_id]

    def dead_letters(self) -> list[DeadLetter]:
        return [
            DeadLetter(
                a.subscription_id, a.event_id, a.cursor, a.last_error or "failed"
            )
            for a in self._attempts
            if a.status == "dead_letter"
        ]
