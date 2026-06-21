"""AgentCard discovery and trust tests (spec §12.2, §21.2)."""

from __future__ import annotations

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from a2a_events import (
    A2AEventsError,
    CardTrustPolicy,
    Ed25519CardSignatureVerifier,
    parse_subscriber_card,
)
from a2a_events.agentcard import sign_card
from a2a_events.models import EXTENSION_URI, DeliveryMode

CARD_URL = "https://agent-a.example.com/.well-known/agent-card.json"


def _card(**param_overrides):
    params = {
        "role": "subscriber",
        "receiveUrl": "https://agent-a.example.com/a2a-events/receive",
        "acceptedDeliveryModes": ["webhook", "a2a-message"],
    }
    params.update(param_overrides)
    return {
        "name": "Enrichment Agent",
        "url": "https://agent-a.example.com/a2a/v1",
        "capabilities": {"extensions": [{"uri": EXTENSION_URI, "params": params}]},
    }


def test_parse_minimal_subscriber_card() -> None:
    card = parse_subscriber_card(CARD_URL, _card())
    assert card.receive_url == "https://agent-a.example.com/a2a-events/receive"
    assert card.a2a_endpoint == "https://agent-a.example.com/a2a/v1"
    assert DeliveryMode.WEBHOOK in card.accepted_delivery_modes


def test_missing_extension_rejected() -> None:
    card = {"name": "x", "url": "https://x", "capabilities": {"extensions": []}}
    with pytest.raises(A2AEventsError) as exc:
        parse_subscriber_card(CARD_URL, card)
    assert exc.value.code.value == "SUBSCRIBER_CARD_INVALID"


def test_wrong_role_rejected() -> None:
    with pytest.raises(A2AEventsError):
        parse_subscriber_card(CARD_URL, _card(role="publisher"))


def test_require_https_rejects_http_endpoint() -> None:
    card = _card(receiveUrl="http://agent-a.example.com/recv")
    with pytest.raises(A2AEventsError) as exc:
        parse_subscriber_card(
            CARD_URL, card, policy=CardTrustPolicy(require_https=True)
        )
    assert exc.value.code.value == "SUBSCRIBER_CARD_INVALID"


def test_require_same_origin_rejects_cross_origin() -> None:
    card = _card(receiveUrl="https://evil.example.com/recv")
    policy = CardTrustPolicy(require_same_origin=True)
    with pytest.raises(A2AEventsError):
        parse_subscriber_card(CARD_URL, card, policy=policy)


def test_same_origin_allows_matching_origin() -> None:
    parse_subscriber_card(
        CARD_URL, _card(), policy=CardTrustPolicy(require_same_origin=True)
    )


def test_allowed_domains() -> None:
    policy = CardTrustPolicy(allowed_domains={"trusted.example.com"})
    with pytest.raises(A2AEventsError):
        parse_subscriber_card(CARD_URL, _card(), policy=policy)


def test_domain_challenge() -> None:
    calls: list[str] = []

    def challenge(host: str) -> bool:
        calls.append(host)
        return host == "agent-a.example.com"

    parse_subscriber_card(
        CARD_URL, _card(), policy=CardTrustPolicy(domain_challenge=challenge)
    )
    assert calls == ["agent-a.example.com"]

    with pytest.raises(A2AEventsError):
        parse_subscriber_card(
            "https://other.example.com/card",
            _card(),
            policy=CardTrustPolicy(domain_challenge=challenge),
        )


def test_agentcard_signature_roundtrip() -> None:
    key = Ed25519PrivateKey.generate()
    signed = sign_card(_card(), key)
    verifier = Ed25519CardSignatureVerifier(key.public_key())

    # Valid signature passes the trust policy.
    parse_subscriber_card(
        CARD_URL, signed, policy=CardTrustPolicy(signature_verifier=verifier)
    )

    # A tampered card fails verification.
    tampered = {**signed, "url": "https://attacker.example.com/a2a/v1"}
    with pytest.raises(A2AEventsError) as exc:
        parse_subscriber_card(
            CARD_URL, tampered, policy=CardTrustPolicy(signature_verifier=verifier)
        )
    assert exc.value.code.value == "SUBSCRIBER_CARD_INVALID"


def test_unsigned_card_fails_when_signature_required() -> None:
    key = Ed25519PrivateKey.generate()
    verifier = Ed25519CardSignatureVerifier(key.public_key())
    with pytest.raises(A2AEventsError):
        parse_subscriber_card(
            CARD_URL, _card(), policy=CardTrustPolicy(signature_verifier=verifier)
        )


def test_resolver_over_httpx() -> None:
    import httpx

    signed = _card()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("agent-card.json"):
            return httpx.Response(200, json=signed)
        return httpx.Response(404)

    from a2a_events import AgentCardResolver

    client = httpx.Client(transport=httpx.MockTransport(handler))
    resolver = AgentCardResolver(client=client)
    card = resolver(CARD_URL)
    assert card.receive_url == "https://agent-a.example.com/a2a-events/receive"

    # Unreachable card surfaces as SUBSCRIBER_CARD_UNREACHABLE.
    with pytest.raises(A2AEventsError) as exc:
        resolver("https://agent-a.example.com/missing.json")
    assert exc.value.code.value == "SUBSCRIBER_CARD_UNREACHABLE"
