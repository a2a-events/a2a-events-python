"""Publisher signing-key rotation and JWKS discovery (DESIGN.md §21.3)."""

from __future__ import annotations

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from a2a_events import (
    A2AEventsPublisher,
    DeliveryMode,
    DeliveryPreference,
    InMemorySubscriber,
    InMemoryTransport,
    PublisherConfig,
    SigningKey,
    SigningKeySet,
    Topic,
)
from a2a_events.server import JwksKeyResolver

PUB = "https://agent-b.example.com/.well-known/agent-card.json"
SUB = "https://agent-a.example.com/.well-known/agent-card.json"
TOPIC = "agent_card.discovered"


def _raw(pk: Ed25519PublicKey) -> bytes:
    return pk.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)


# --- SigningKeySet unit ------------------------------------------------------


def test_key_set_active_add_activate_retire():
    k1, k2 = SigningKey.generate("k1"), SigningKey.generate("k2")
    ks = SigningKeySet(k1)
    assert ks.active.kid == "k1"

    ks.add(k2)  # pre-publish without activating
    assert ks.active.kid == "k1"
    assert {j["kid"] for j in ks.jwks()} == {"k1", "k2"}

    ks.activate("k2")
    assert ks.active.kid == "k2"

    with pytest.raises(ValueError):
        ks.retire("k2")  # cannot retire the active key
    ks.retire("k1")
    assert {j["kid"] for j in ks.jwks()} == {"k2"}

    with pytest.raises(KeyError):
        ks.activate("unknown")


# --- end-to-end rotation over in-memory delivery -----------------------------


def _stack() -> tuple[A2AEventsPublisher, InMemorySubscriber]:
    transport = InMemoryTransport()
    publisher = A2AEventsPublisher(
        agent_card_url=PUB,
        transport=transport,
        signing_key=SigningKey.generate("k1"),
        config=PublisherConfig(card_resolver=lambda _url: subscriber.card()),
    )

    def resolve(kid: str) -> Ed25519PublicKey:
        key = publisher.keys.get(kid)
        assert key is not None, f"unknown kid {kid}"
        return key.public_key

    subscriber = InMemorySubscriber(
        card_url=SUB, transport=transport, key_resolver=resolve
    )
    publisher.declare_topic(Topic(name=TOPIC, filterableFields=["data.cardUrl"]))
    return publisher, subscriber


async def test_delivery_continues_across_rotation():
    publisher, subscriber = _stack()
    await publisher.subscribe(
        subscriber_card_url=SUB,
        topics=[TOPIC],
        delivery=DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
        lease_seconds=3600,
    )

    await publisher.publish(TOPIC, "t", {"cardUrl": "https://a"})
    assert publisher.signing_key.kid == "k1"
    assert len(subscriber.received) == 1

    # Pre-publish then activate a new key, then publish again.
    publisher.add_signing_key(SigningKey.generate("k2"))
    publisher.rotate_signing_key("k2")
    assert publisher.signing_key.kid == "k2"

    await publisher.publish(TOPIC, "t", {"cardUrl": "https://b"})
    # The second event is signed with k2 and still verifies at the subscriber.
    assert [e["data"]["cardUrl"] for e in subscriber.received] == [
        "https://a",
        "https://b",
    ]


# --- JWKS resolver refetches on an unknown kid -------------------------------


class _FakeResp:
    def __init__(self, keys: list[dict[str, str]]) -> None:
        self._keys = keys

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, list[dict[str, str]]]:
        return {"keys": self._keys}


class _FakeJwksClient:
    """Serves whatever JWKS the publisher currently exposes."""

    def __init__(self, publisher: A2AEventsPublisher) -> None:
        self._publisher = publisher
        self.calls = 0

    async def get(self, _url: str) -> _FakeResp:  # noqa: S7503
        self.calls += 1
        return _FakeResp(self._publisher.signing_jwks())


async def test_resolver_refetches_on_unknown_kid_after_rotation():
    publisher, _ = _stack()
    client = _FakeJwksClient(publisher)
    resolver = JwksKeyResolver("http://pub/keys", client)  # type: ignore[arg-type]

    k1 = await resolver("k1")
    assert _raw(k1) == _raw(publisher.keys.active.public_key)
    assert client.calls == 1

    publisher.add_signing_key(SigningKey.generate("k2"), activate=True)

    # New kid -> cache miss -> one more fetch, and it resolves to the new key.
    k2 = await resolver("k2")
    assert client.calls == 2
    assert _raw(k2) == _raw(publisher.keys.active.public_key)

    # Already-cached kid -> no extra fetch.
    await resolver("k1")
    assert client.calls == 2
