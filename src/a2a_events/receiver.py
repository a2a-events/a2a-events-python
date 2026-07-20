"""Subscriber-side event receiver core (spec §21.3, §19.2, §10.10).

Shared by the in-memory subscriber and the HTTP subscriber app: verify the
signature, deduplicate by event id, run the user handler, and implicitly ack
by returning a successful :class:`DeliveryResult`.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from .models import EXTENSION_URI
from .runtime import DeliveryResult
from .signing import verify

KeyResolver = Callable[[str], Ed25519PublicKey | Awaitable[Ed25519PublicKey]]
EventHandler = Callable[[dict[str, Any]], DeliveryResult | None]

# A2A-message deliveries carry the signature/timestamp/keyId under the extension
# URI key in the message metadata (spec §18.1). Must match the publisher.
EXTENSION_METADATA_KEY = EXTENSION_URI


class EventReceiver:
    def __init__(
        self, key_resolver: KeyResolver, max_skew_seconds: float | None = 300.0
    ) -> None:
        self._key_resolver = key_resolver
        # Reject events whose signed timestamp is further than this from now,
        # in either direction (replay / badly-skewed clocks, §21). None disables.
        self._max_skew_seconds = max_skew_seconds
        self.received: list[dict[str, Any]] = []
        self._seen: set[str] = set()
        # Per-subscription expected delivery bearer token (§21.1, §21.5). The
        # publisher hands this back at subscribe time; deliveries that do not
        # present the matching token for a registered subscription are rejected.
        self._delivery_tokens: dict[str, str] = {}
        # Test/extension hook: return a DeliveryResult to override, or None to
        # fall through to the normal accept path.
        self.on_event: EventHandler | None = None

    def register_delivery_token(self, subscription_id: str, token: str) -> None:
        """Record the bearer token the publisher issued for a subscription."""
        self._delivery_tokens[subscription_id] = token

    def _delivery_token_ok(
        self, subscription_id: str | None, presented: str | None
    ) -> bool:
        if subscription_id is None or subscription_id not in self._delivery_tokens:
            return True  # no expectation registered for this subscription
        return presented == self._delivery_tokens[subscription_id]

    async def _resolve_key(self, key_id: str) -> Ed25519PublicKey:
        result = self._key_resolver(key_id)
        if inspect.isawaitable(result):
            return await result
        return result

    def _within_skew(self, timestamp: str) -> bool:
        if self._max_skew_seconds is None:
            return True
        try:
            ts = datetime.fromisoformat(timestamp)
        except ValueError:
            return False
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        delta = abs((datetime.now(UTC) - ts).total_seconds())
        return delta <= self._max_skew_seconds

    async def accept(
        self,
        event: dict[str, Any],
        key_id: str,
        timestamp: str,
        signature: str,
        delivery_token: str | None = None,
    ) -> DeliveryResult:
        # The per-subscription delivery credential authenticates the caller as
        # this subscription's publisher (§21.1). A mismatch is permanent.
        subscription_id = event.get("a2asubscription")
        if not self._delivery_token_ok(subscription_id, delivery_token):
            return DeliveryResult(
                ack=False, retry=False, status_code=401, reason="bad delivery token"
            )

        try:
            public_key = await self._resolve_key(key_id)
        except KeyError:
            # The resolver has no key for this kid, so the event can never be
            # verified — a permanent failure, do not retry (§21.3). Transient
            # key-fetch errors (e.g. a JWKS endpoint blip) raise other exception
            # types and propagate, surfacing as a retryable failure instead.
            return DeliveryResult(
                ack=False, retry=False, status_code=401, reason="unknown signing key"
            )
        if not verify(public_key, timestamp, event, signature):
            # Bad signature is a permanent failure: do not retry (§21.3).
            return DeliveryResult(
                ack=False, retry=False, status_code=401, reason="bad signature"
            )

        # The timestamp is signed, so it is now trustworthy: reject stale or
        # far-future events (replay / clock skew, §21). Permanent: a retry from
        # the same publisher carries the same timestamp.
        if not self._within_skew(timestamp):
            return DeliveryResult(
                ack=False, retry=False, status_code=403, reason="timestamp out of skew"
            )

        event_id = event.get("id")
        if not event_id:
            return DeliveryResult(
                ack=False, retry=False, status_code=400, reason="event has no id"
            )
        if event_id in self._seen:
            # Duplicate: idempotent ack, no reprocessing (§19.2).
            return DeliveryResult(ack=True, status_code=200)

        if self.on_event is not None:
            override = self.on_event(event)
            if override is not None:
                if override.ack:
                    self._seen.add(event_id)
                    self.received.append(event)
                return override

        self._seen.add(event_id)
        self.received.append(event)
        return DeliveryResult(ack=True, status_code=204)

    # --- framing helpers (the two delivery modes, §18) ---
    async def accept_webhook(
        self, headers: dict[str, str], body: dict[str, Any]
    ) -> DeliveryResult:
        # HTTP header names are case-insensitive; normalize before lookup.
        h = {k.lower(): v for k, v in headers.items()}
        auth = h.get("authorization", "")
        scheme, _, token = auth.partition(" ")
        return await self.accept(
            event=body,
            key_id=h.get("a2a-event-key-id", ""),
            timestamp=h.get("a2a-event-timestamp", ""),
            signature=h.get("a2a-event-signature", ""),
            delivery_token=token.strip() if scheme.lower() == "bearer" else None,
        )

    async def accept_a2a_message(self, message: dict[str, Any]) -> DeliveryResult:
        try:
            msg = message["message"]
            event = msg["parts"][0]["data"]
            meta = msg["metadata"][EXTENSION_METADATA_KEY]
        except (KeyError, IndexError, TypeError):
            # Not an A2A Events delivery envelope (§18.1) — permanent, since a
            # retry would resend the same malformed message.
            return DeliveryResult(
                ack=False, retry=False, status_code=400, reason="malformed envelope"
            )
        return await self.accept(
            event=event,
            key_id=meta.get("keyId", ""),
            timestamp=meta.get("timestamp", ""),
            signature=meta.get("signature", ""),
            delivery_token=meta.get("deliveryToken"),
        )
