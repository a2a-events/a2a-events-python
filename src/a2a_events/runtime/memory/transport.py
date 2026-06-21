"""In-memory delivery transport (DESIGN.md §18).

The zero-dependency default :class:`~a2a_events.runtime.contracts.Transport`:
routes deliveries to in-process subscriber receivers by target key, so
publishers and subscribers can be wired together without a network for tests and
local dev. HTTP-backed transports (``runtime.http_delivery``) implement the same
protocol.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from ..contracts import DeliveryResult

WebhookHandler = Callable[[dict[str, str], dict[str, Any]], Awaitable[DeliveryResult]]
A2AMessageHandler = Callable[[dict[str, Any]], Awaitable[DeliveryResult]]


class InMemoryTransport:
    """Routes deliveries to in-process subscriber receivers by target key."""

    def __init__(self) -> None:
        self._webhooks: dict[str, WebhookHandler] = {}
        self._a2a: dict[str, A2AMessageHandler] = {}

    def register_webhook(self, url: str, handler: WebhookHandler) -> None:
        self._webhooks[url] = handler

    def register_a2a(self, endpoint: str, handler: A2AMessageHandler) -> None:
        self._a2a[endpoint] = handler

    async def send_webhook(
        self, url: str, headers: dict[str, str], body: dict[str, Any]
    ) -> DeliveryResult:
        handler = self._webhooks.get(url)
        if handler is None:
            return DeliveryResult(
                ack=False, retry=True, status_code=502, reason="no receiver"
            )
        return await handler(headers, body)

    async def send_a2a_message(
        self, endpoint: str, message: dict[str, Any]
    ) -> DeliveryResult:
        handler = self._a2a.get(endpoint)
        if handler is None:
            return DeliveryResult(
                ack=False, retry=True, status_code=502, reason="no receiver"
            )
        return await handler(message)
