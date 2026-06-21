"""HTTP delivery transport (spec §18, §19.4).

Implements the :class:`Transport` protocol over httpx:

- A2A-message delivery (canonical): a JSON-RPC ``a2a.SendMessage`` POST to the
  subscriber's A2A endpoint (reusing A2A core messaging).
- Webhook delivery: a signed CloudEvent POST to the subscriber's receive URL.

Status mapping (§19.4): 2xx -> ack; 408/429/5xx -> retriable; other 4xx ->
permanent failure (no retry, dead-letter).
"""

from __future__ import annotations

import uuid
from typing import Any

import httpx

from .contracts import DeliveryResult


def _result_from_status(status: int, reason: str | None = None) -> DeliveryResult:
    if 200 <= status < 300:
        return DeliveryResult(ack=True, status_code=status)
    retry = status in (408, 429) or status >= 500
    return DeliveryResult(ack=False, retry=retry, status_code=status, reason=reason)


class HttpxTransport:
    """Delivers events over HTTP. Inject a client (e.g. ASGI-bound) for tests."""

    def __init__(self, client: httpx.AsyncClient, timeout: float = 10.0) -> None:
        self._client = client
        self._timeout = timeout

    async def send_webhook(
        self, url: str, headers: dict[str, str], body: dict[str, Any]
    ) -> DeliveryResult:
        try:
            resp = await self._client.post(
                url,
                json=body,
                headers={"Content-Type": "application/cloudevents+json", **headers},
                timeout=self._timeout,
            )
        except httpx.RequestError as exc:
            return DeliveryResult(ack=False, retry=True, reason=str(exc))
        return _result_from_status(resp.status_code)

    async def send_a2a_message(
        self, endpoint: str, message: dict[str, Any]
    ) -> DeliveryResult:
        envelope = {
            "jsonrpc": "2.0",
            "id": uuid.uuid4().hex,
            "method": "a2a.SendMessage",
            "params": message,
        }
        try:
            resp = await self._client.post(
                endpoint, json=envelope, timeout=self._timeout
            )
        except httpx.RequestError as exc:
            return DeliveryResult(ack=False, retry=True, reason=str(exc))
        if not (200 <= resp.status_code < 300):
            return _result_from_status(resp.status_code)

        # A2A returns a JSON-RPC envelope. The subscriber conveys ack/nack in
        # the result (or a JSON-RPC error) per spec §18.1.
        payload = resp.json()
        if "error" in payload:
            return DeliveryResult(
                ack=False, retry=True, reason=payload["error"].get("message")
            )
        result = payload.get("result") or {}
        if result.get("ack") is False:
            return DeliveryResult(ack=False, retry=bool(result.get("retry", False)))
        return DeliveryResult(ack=True, status_code=resp.status_code)
