"""Control-plane authentication, topic authorization, and delivery tokens
(DESIGN.md §21.1, §21.4, §21.5).

A2A Events reuses A2A's existing security machinery rather than inventing a new
login handshake. This module provides the three seams the publisher needs:

- :class:`AuthIdentity` — the authenticated caller behind a control-plane
  request (``subscribe``/``renew``/``delete``/``list``/``replay``/``:ack``),
  produced by a :data:`CallerAuthenticator` from the transport's credentials.
- :class:`TopicAuthorizer` — decides whether an identity may subscribe to a set
  of topics, and (re-)checks at delivery time so a revoked grant stops future
  deliveries (§21.4: "evaluated both at subscription creation and delivery
  time").
- :class:`DeliveryTokenIssuer` — mints the per-subscription bearer credential
  the publisher returns at creation and presents on every delivery, so the
  subscriber can authenticate incoming events as belonging to a subscription
  (§21.1, §21.5 least-privilege tokens).

All three are optional: a publisher constructed without them behaves exactly as
before (all topics public, no delivery credential).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import secrets
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from .errors import A2AEventsError, ErrorCode
from .models import Subscription


@dataclass(frozen=True)
class AuthIdentity:
    """An authenticated control-plane caller (DESIGN.md §21.1).

    ``subject`` is the stable principal (e.g. the subscriber's AgentCard URL or
    an OAuth2 ``sub``); ``scopes`` and ``claims`` carry whatever the
    authenticator extracted from the credential.
    """

    subject: str
    scopes: frozenset[str] = field(default_factory=frozenset)
    claims: Mapping[str, Any] = field(default_factory=dict)


@runtime_checkable
class CallerAuthenticator(Protocol):
    """Maps a transport request's credentials to an :class:`AuthIdentity`.

    Returns ``None`` when no/invalid credentials are present; the caller decides
    whether anonymous access is allowed for the requested operation.
    """

    def __call__(self, headers: Mapping[str, str]) -> AuthIdentity | None: ...


def bearer_token(headers: Mapping[str, str]) -> str | None:
    """Extract a ``Authorization: Bearer <token>`` value (case-insensitive)."""
    for key, value in headers.items():
        if key.lower() == "authorization":
            scheme, _, token = value.partition(" ")
            if scheme.lower() == "bearer" and token:
                return token.strip()
    return None


@runtime_checkable
class TopicAuthorizer(Protocol):
    """Decides topic access at subscribe and delivery time (DESIGN.md §21.4)."""

    def authorize_subscribe(
        self, identity: AuthIdentity | None, topics: list[str]
    ) -> None:
        """Raise :class:`A2AEventsError` (``TOPIC_NOT_AUTHORIZED``) on denial."""
        ...

    def authorize_delivery(self, subscription: Subscription) -> bool:
        """Return whether an active subscription may still receive deliveries."""
        ...


class AllowlistAuthorizer:
    """A simple grant-table :class:`TopicAuthorizer` (DESIGN.md §21.4).

    ``public_topics`` are open to everyone (even anonymous callers).
    ``grants`` maps a principal (``AuthIdentity.subject``, which for the
    reference is the subscriber's AgentCard URL) to the topics it may subscribe
    to; the sentinel ``"*"`` grants all topics. Delivery-time checks re-evaluate
    the same table against ``subscription.subscriberCardUrl``, so removing a
    grant stops future deliveries without touching the subscription.
    """

    def __init__(
        self,
        grants: Mapping[str, set[str]] | None = None,
        *,
        public_topics: set[str] | None = None,
    ) -> None:
        self._grants: dict[str, set[str]] = {
            k: set(v) for k, v in (grants or {}).items()
        }
        self._public = set(public_topics or set())

    def grant(self, subject: str, topics: set[str]) -> None:
        self._grants.setdefault(subject, set()).update(topics)

    def revoke(self, subject: str, topics: set[str] | None = None) -> None:
        if topics is None:
            self._grants.pop(subject, None)
        elif subject in self._grants:
            self._grants[subject] -= topics

    def _allowed(self, subject: str | None, topic: str) -> bool:
        if topic in self._public:
            return True
        if subject is None:
            return False
        allowed = self._grants.get(subject, set())
        return "*" in allowed or topic in allowed

    def authorize_subscribe(
        self, identity: AuthIdentity | None, topics: list[str]
    ) -> None:
        subject = identity.subject if identity else None
        denied = [t for t in topics if not self._allowed(subject, t)]
        if denied:
            raise A2AEventsError(
                ErrorCode.TOPIC_NOT_AUTHORIZED,
                f"Caller is not authorized for topic(s): {', '.join(denied)}.",
                {"topics": denied, "subject": subject},
            )

    def authorize_delivery(self, subscription: Subscription) -> bool:
        subject = subscription.subscriber_card_url
        return all(self._allowed(subject, t) for t in subscription.topics)


class DeliveryTokenIssuer:
    """Mints per-subscription bearer delivery credentials (DESIGN.md §21.5).

    The token is a keyed HMAC over the subscription id, so it is unique per
    subscription, deterministic (the publisher recomputes it on every delivery
    without storing it), and revocable/rotatable by rotating ``secret``. It is
    scoped to event receipt only and is never a normal A2A task credential.
    """

    SCHEME = "bearer"
    PREFIX = "dtok_"

    def __init__(self, secret: bytes | None = None) -> None:
        self._secret = secret or secrets.token_bytes(32)

    def issue(self, subscription_id: str) -> str:
        mac = hmac.new(
            self._secret, subscription_id.encode("utf-8"), hashlib.sha256
        ).digest()
        raw = base64.urlsafe_b64encode(mac).rstrip(b"=").decode("ascii")
        return self.PREFIX + raw

    def verify(self, subscription_id: str, token: str | None) -> bool:
        if not token:
            return False
        return hmac.compare_digest(token, self.issue(subscription_id))
