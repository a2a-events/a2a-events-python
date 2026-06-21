"""Control-plane authorization + delivery-token tests (spec §21.1, §21.4)."""

from __future__ import annotations

import pytest

from a2a_events import (
    A2AEventsError,
    A2AEventsPublisher,
    AllowlistAuthorizer,
    AuthIdentity,
    DeliveryTokenIssuer,
    InMemorySubscriber,
    InMemoryTransport,
    PublisherConfig,
    SigningKey,
    Topic,
)
from a2a_events.auth import bearer_token
from a2a_events.models import DeliveryMode, DeliveryPreference

PUBLISHER_CARD = "https://agent-b.example.com/.well-known/agent-card.json"
SUBSCRIBER_CARD = "https://agent-a.example.com/.well-known/agent-card.json"
TOPIC = "agent_card.discovered"


def _build(authorizer=None, issuer=None):
    transport = InMemoryTransport()
    key = SigningKey.generate("key_2026_06")
    subscriber = InMemorySubscriber(
        card_url=SUBSCRIBER_CARD,
        transport=transport,
        key_resolver=lambda _kid: key.public_key,
    )
    publisher = A2AEventsPublisher(
        agent_card_url=PUBLISHER_CARD,
        transport=transport,
        signing_key=key,
        config=PublisherConfig(
            card_resolver=lambda _url: subscriber.card(),
            authorizer=authorizer,
            delivery_token_issuer=issuer,
        ),
    )
    publisher.declare_topic(Topic(name=TOPIC, filterableFields=["data.cardUrl"]))
    return publisher, subscriber


def _delivery() -> DeliveryPreference:
    return DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE)


async def _subscribe(publisher, caller=None):
    return await publisher.subscribe(
        subscriber_card_url=SUBSCRIBER_CARD,
        topics=[TOPIC],
        delivery=_delivery(),
        caller=caller,
    )


def test_bearer_token_extraction() -> None:
    assert bearer_token({"Authorization": "Bearer abc"}) == "abc"
    assert bearer_token({"authorization": "bearer xyz"}) == "xyz"
    assert bearer_token({"Authorization": "Basic abc"}) is None
    assert bearer_token({}) is None


async def test_subscribe_denied_when_not_authorized() -> None:
    authorizer = AllowlistAuthorizer(grants={"someone-else": {TOPIC}})
    publisher, _ = _build(authorizer=authorizer)
    with pytest.raises(A2AEventsError) as exc:
        await _subscribe(publisher, caller=AuthIdentity(subject=SUBSCRIBER_CARD))
    assert exc.value.code.value == "TOPIC_NOT_AUTHORIZED"


async def test_subscribe_allowed_for_granted_topic() -> None:
    authorizer = AllowlistAuthorizer(grants={SUBSCRIBER_CARD: {TOPIC}})
    publisher, _ = _build(authorizer=authorizer)
    sub = await _subscribe(publisher, caller=AuthIdentity(subject=SUBSCRIBER_CARD))
    assert sub.status.value == "active"


async def test_public_topic_allows_anonymous() -> None:
    authorizer = AllowlistAuthorizer(public_topics={TOPIC})
    publisher, _ = _build(authorizer=authorizer)
    sub = await _subscribe(publisher, caller=None)
    assert sub.status.value == "active"


async def test_delivery_revocation_stops_future_deliveries() -> None:
    authorizer = AllowlistAuthorizer(grants={SUBSCRIBER_CARD: {TOPIC}})
    publisher, subscriber = _build(authorizer=authorizer)
    await _subscribe(publisher, caller=AuthIdentity(subject=SUBSCRIBER_CARD))

    await publisher.publish(TOPIC, "discovered.v1", {"cardUrl": "https://x"})
    assert len(subscriber.received) == 1

    # Revoke the grant; the next publish must not be delivered (§21.4).
    authorizer.revoke(SUBSCRIBER_CARD)
    await publisher.publish(TOPIC, "discovered.v1", {"cardUrl": "https://y"})
    assert len(subscriber.received) == 1


async def test_delivery_token_issued_and_accepted() -> None:
    issuer = DeliveryTokenIssuer(secret=b"s" * 32)
    publisher, subscriber = _build(issuer=issuer)
    sub = await _subscribe(publisher)

    auth = publisher.delivery_auth(sub)
    assert auth is not None and auth["scheme"] == "bearer"
    expected = issuer.issue(sub.subscription_id)
    assert auth["token"] == expected

    # The subscriber records the issued token and accepts matching deliveries.
    subscriber.receiver.register_delivery_token(sub.subscription_id, expected)
    await publisher.publish(TOPIC, "discovered.v1", {"cardUrl": "https://x"})
    assert len(subscriber.received) == 1


async def test_delivery_token_mismatch_rejected() -> None:
    issuer = DeliveryTokenIssuer(secret=b"s" * 32)
    publisher, subscriber = _build(issuer=issuer)
    sub = await _subscribe(publisher)

    # Register a token that does NOT match what the publisher will present.
    subscriber.receiver.register_delivery_token(sub.subscription_id, "dtok_wrong")
    await publisher.publish(TOPIC, "discovered.v1", {"cardUrl": "https://x"})
    assert subscriber.received == []
    # The mismatch is terminal (no retry), so it dead-letters.
    assert any(d.subscription_id == sub.subscription_id for d in publisher.dead_letters)


def test_delivery_token_issuer_verify() -> None:
    issuer = DeliveryTokenIssuer(secret=b"k" * 32)
    token = issuer.issue("sub_1")
    assert issuer.verify("sub_1", token)
    assert not issuer.verify("sub_2", token)
    assert not issuer.verify("sub_1", None)
