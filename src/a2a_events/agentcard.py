"""AgentCard discovery and trust (spec §12.2, §21.2).

The publisher must not deliver to arbitrary URLs a subscriber hands it. Instead
it fetches the subscriber's A2A AgentCard, parses the A2A Events extension
declaration (``role: subscriber``), and resolves delivery endpoints *only* from
the card — then applies trust checks before trusting them:

- HTTPS-only endpoints,
- same-origin between the card URL and the resolved delivery endpoints,
- a domain allowlist,
- A2A ``AgentCardSignature`` (JWS over the JCS-canonicalized card) verification,
- an out-of-band domain-ownership challenge.

:func:`parse_subscriber_card` is a pure function (easy to test); the HTTP
:class:`AgentCardResolver` wires it to a fetch and plugs straight into the
publisher's ``card_resolver`` seam.
"""

from __future__ import annotations

import base64
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from .errors import A2AEventsError, ErrorCode
from .models import EXTENSION_URI, DeliveryMode
from .runtime.publisher import SubscriberCard
from .signing import canonicalize

if TYPE_CHECKING:
    import httpx

SignatureVerifier = Callable[[dict[str, Any]], bool]
DomainChallenge = Callable[[str], bool]


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _b64url(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


@dataclass
class CardTrustPolicy:
    """Trust checks applied to a fetched AgentCard (spec §21.2)."""

    require_https: bool = False
    require_same_origin: bool = False
    allowed_domains: set[str] | None = None
    signature_verifier: SignatureVerifier | None = None
    domain_challenge: DomainChallenge | None = None


class Ed25519CardSignatureVerifier:
    """Verifies an A2A ``AgentCardSignature`` (detached JWS over JCS, §21.2).

    A2A cards may carry a ``signatures`` array of JWS objects whose payload is
    the JCS-canonicalized card with ``signatures`` removed. This verifies any
    one signature against the publisher-supplied Ed25519 public key.
    """

    def __init__(self, public_key: Ed25519PublicKey) -> None:
        self._public_key = public_key

    def __call__(self, card: dict[str, Any]) -> bool:
        signatures = card.get("signatures") or []
        if not signatures:
            return False
        unsigned = {k: v for k, v in card.items() if k != "signatures"}
        payload_b64 = _b64url(canonicalize(unsigned))
        for sig in signatures:
            protected = sig.get("protected")
            signature = sig.get("signature")
            if not protected or not signature:
                continue
            signing_input = f"{protected}.{payload_b64}".encode("ascii")
            try:
                self._public_key.verify(_b64url_decode(signature), signing_input)
                return True
            except (InvalidSignature, ValueError):
                continue
        return False


def sign_card(
    card: dict[str, Any], private_key: Any, kid: str = "card-1"
) -> dict[str, Any]:
    """Attach an Ed25519 ``AgentCardSignature`` to ``card`` (test/helper, §21.2)."""
    import json

    unsigned = {k: v for k, v in card.items() if k != "signatures"}
    payload_b64 = _b64url(canonicalize(unsigned))
    protected = _b64url(
        json.dumps({"alg": "EdDSA", "kid": kid}, separators=(",", ":")).encode("utf-8")
    )
    signing_input = f"{protected}.{payload_b64}".encode("ascii")
    signature = _b64url(private_key.sign(signing_input))
    return {
        **unsigned,
        "signatures": [{"protected": protected, "signature": signature}],
    }


def _a2a_endpoint_of(card: dict[str, Any]) -> str | None:
    """The subscriber's A2A JSON-RPC endpoint URL from its AgentCard.

    A2A v1.0 declares endpoints via ``supportedInterfaces`` (AgentInterface
    objects with ``url`` + ``protocolBinding``); A2A-message delivery uses
    JSON-RPC, so only a ``JSONRPC`` interface qualifies. A top-level ``url``
    is accepted as a fallback for older/abbreviated card shapes.
    """
    for iface in card.get("supportedInterfaces") or []:
        if iface.get("url") and iface.get("protocolBinding", "JSONRPC") == "JSONRPC":
            return str(iface["url"])
    url = card.get("url")
    return str(url) if url else None


def _find_subscriber_extension(card: dict[str, Any]) -> dict[str, Any]:
    extensions = (card.get("capabilities") or {}).get("extensions") or []
    for ext in extensions:
        if ext.get("uri") == EXTENSION_URI:
            params = ext.get("params") or {}
            if params.get("role") == "subscriber":
                return params
    raise A2AEventsError(
        ErrorCode.SUBSCRIBER_CARD_INVALID,
        "AgentCard does not declare the A2A Events subscriber extension.",
        {"extension": EXTENSION_URI},
    )


def parse_subscriber_card(
    card_url: str,
    card: dict[str, Any],
    *,
    policy: CardTrustPolicy | None = None,
) -> SubscriberCard:
    """Build a trusted :class:`SubscriberCard` from a fetched AgentCard (§12.2)."""
    policy = policy or CardTrustPolicy()
    host = urlparse(card_url).hostname or ""

    if policy.allowed_domains is not None and host not in policy.allowed_domains:
        raise A2AEventsError(
            ErrorCode.SUBSCRIBER_CARD_INVALID,
            f"Subscriber domain {host} is not allowlisted.",
            {"host": host},
        )
    if policy.signature_verifier is not None and not policy.signature_verifier(card):
        raise A2AEventsError(
            ErrorCode.SUBSCRIBER_CARD_INVALID,
            "AgentCard signature is missing or invalid.",
            {"cardUrl": card_url},
        )
    if policy.domain_challenge is not None and not policy.domain_challenge(host):
        raise A2AEventsError(
            ErrorCode.SUBSCRIBER_CARD_INVALID,
            f"Domain ownership challenge failed for {host}.",
            {"host": host},
        )

    params = _find_subscriber_extension(card)
    receive_url = params.get("receiveUrl")
    a2a_endpoint = _a2a_endpoint_of(card)
    modes = [DeliveryMode(m) for m in params.get("acceptedDeliveryModes", [])] or [
        DeliveryMode.A2A_MESSAGE,
        DeliveryMode.WEBHOOK,
    ]

    for endpoint in (receive_url, a2a_endpoint):
        _check_endpoint_trust(card_url, endpoint, policy)

    return SubscriberCard(
        card_url=card_url,
        a2a_endpoint=a2a_endpoint,
        receive_url=receive_url,
        accepted_delivery_modes=modes,
    )


def _check_endpoint_trust(
    card_url: str, endpoint: str | None, policy: CardTrustPolicy
) -> None:
    if endpoint is None:
        return
    parsed = urlparse(endpoint)
    if policy.require_https and parsed.scheme != "https":
        raise A2AEventsError(
            ErrorCode.SUBSCRIBER_CARD_INVALID,
            f"Delivery endpoint {endpoint} must use HTTPS.",
            {"endpoint": endpoint},
        )
    if policy.require_same_origin:
        card = urlparse(card_url)
        if (parsed.scheme, parsed.hostname, parsed.port) != (
            card.scheme,
            card.hostname,
            card.port,
        ):
            raise A2AEventsError(
                ErrorCode.SUBSCRIBER_CARD_INVALID,
                f"Delivery endpoint {endpoint} is not same-origin with the card.",
                {"endpoint": endpoint, "cardUrl": card_url},
            )


@dataclass
class AgentCardResolver:
    """Fetches and parses a subscriber AgentCard over HTTP (spec §12.2).

    Plugs into the publisher's ``card_resolver`` seam. A fetch failure surfaces
    as ``SUBSCRIBER_CARD_UNREACHABLE``; a card that fails parsing/trust surfaces
    as ``SUBSCRIBER_CARD_INVALID``.
    """

    client: httpx.Client
    policy: CardTrustPolicy = field(default_factory=CardTrustPolicy)

    def __call__(self, card_url: str) -> SubscriberCard:
        try:
            resp = self.client.get(card_url)
            resp.raise_for_status()
            card = resp.json()
        except A2AEventsError:
            raise
        except Exception as exc:
            raise A2AEventsError(
                ErrorCode.SUBSCRIBER_CARD_UNREACHABLE,
                f"Could not fetch subscriber card {card_url}.",
                {"cardUrl": card_url},
            ) from exc
        return parse_subscriber_card(card_url, card, policy=self.policy)
