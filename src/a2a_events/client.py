"""Subscriber-side helpers (spec §26.2).

``InMemorySubscriber`` is the receiver half of the in-memory vertical slice:
it registers webhook and A2A-message handlers on an ``InMemoryTransport`` and
delegates verify/dedupe/ack to a shared :class:`EventReceiver`. HTTP-backed
subscribers (see :mod:`a2a_events.server`) reuse the same receiver core.
"""

from __future__ import annotations

from typing import Any

from .models import DeliveryMode
from .receiver import EventHandler, EventReceiver, KeyResolver
from .runtime import DeliveryResult, InMemoryTransport
from .runtime.publisher import SubscriberCard


class InMemorySubscriber:
    def __init__(
        self,
        card_url: str,
        transport: InMemoryTransport,
        key_resolver: KeyResolver,
        *,
        a2a_endpoint: str | None = None,
        receive_url: str | None = None,
        accepted_delivery_modes: list[DeliveryMode] | None = None,
    ) -> None:
        self.card_url = card_url
        self.a2a_endpoint = a2a_endpoint or f"{card_url.rstrip('/')}/a2a/v1"
        self.receive_url = receive_url or f"{card_url.rstrip('/')}/a2a-events/receive"
        self.accepted_delivery_modes = accepted_delivery_modes or [
            DeliveryMode.A2A_MESSAGE,
            DeliveryMode.WEBHOOK,
        ]
        self.receiver = EventReceiver(key_resolver)

        transport.register_webhook(self.receive_url, self.receiver.accept_webhook)
        transport.register_a2a(self.a2a_endpoint, self.receiver.accept_a2a_message)

    # Convenience proxies to the shared receiver state.
    @property
    def received(self) -> list[dict[str, Any]]:
        return self.receiver.received

    @property
    def on_event(self) -> EventHandler | None:
        return self.receiver.on_event

    @on_event.setter
    def on_event(self, handler: EventHandler | None) -> None:
        self.receiver.on_event = handler

    def card(self) -> SubscriberCard:
        return SubscriberCard(
            card_url=self.card_url,
            a2a_endpoint=self.a2a_endpoint,
            receive_url=self.receive_url,
            accepted_delivery_modes=self.accepted_delivery_modes,
        )


__all__ = ["DeliveryResult", "InMemorySubscriber"]
