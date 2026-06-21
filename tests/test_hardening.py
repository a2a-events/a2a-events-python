"""HTTP-hardening behaviours: timestamp skew, retry backoff, SSRF-at-subscribe.

(DESIGN.md §19.4 retry, §21 / §21.2 / §21.3.)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from conftest import SUBSCRIBER_CARD, TOPIC, Harness

from a2a_events import (
    A2AEventsPublisher,
    DeliveryMode,
    DeliveryPreference,
    DeliveryResult,
    InMemoryTransport,
    PublisherConfig,
    SigningKey,
    SubscriberCard,
    Topic,
)
from a2a_events.errors import ErrorCode
from a2a_events.models import EXTENSION_URI
from a2a_events.receiver import EventReceiver
from a2a_events.runtime.contracts import Transport

# --- timestamp skew (subscriber side, §21) ----------------------------------


def _signed_event(key: SigningKey, timestamp: str) -> tuple[dict, str]:
    event = {"id": "evt_1", "type": "t", "data": {"x": 1}, "time": timestamp}
    return event, key.sign(timestamp, event)


async def test_fresh_timestamp_is_accepted():
    key = SigningKey.generate("k1")
    receiver = EventReceiver(lambda _kid: key.public_key, max_skew_seconds=300)
    ts = datetime.now(UTC).isoformat()
    event, sig = _signed_event(key, ts)
    result = await receiver.accept(event, "k1", ts, sig)
    assert result.ack is True


async def test_stale_timestamp_is_rejected_permanently():
    key = SigningKey.generate("k1")
    receiver = EventReceiver(lambda _kid: key.public_key, max_skew_seconds=300)
    ts = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    event, sig = _signed_event(key, ts)
    result = await receiver.accept(event, "k1", ts, sig)
    assert result.ack is False
    assert result.retry is False  # permanent: a retry carries the same timestamp
    assert receiver.received == []


async def test_future_timestamp_is_rejected():
    key = SigningKey.generate("k1")
    receiver = EventReceiver(lambda _kid: key.public_key, max_skew_seconds=300)
    ts = (datetime.now(UTC) + timedelta(hours=1)).isoformat()
    event, sig = _signed_event(key, ts)
    result = await receiver.accept(event, "k1", ts, sig)
    assert result.ack is False


async def test_skew_check_can_be_disabled():
    key = SigningKey.generate("k1")
    receiver = EventReceiver(lambda _kid: key.public_key, max_skew_seconds=None)
    ts = (datetime.now(UTC) - timedelta(days=365)).isoformat()
    event, sig = _signed_event(key, ts)
    result = await receiver.accept(event, "k1", ts, sig)
    assert result.ack is True


async def test_unknown_signing_key_is_rejected_permanently():
    # A resolver that has no key for the kid raises KeyError (as JwksKeyResolver
    # does). The event can never be verified, so it must be a permanent failure
    # (no retry), not a transient error that burns the retry budget (§21.3).
    key = SigningKey.generate("k1")

    def resolve(kid: str):
        raise KeyError(kid)

    receiver = EventReceiver(resolve, max_skew_seconds=300)
    ts = datetime.now(UTC).isoformat()
    event, sig = _signed_event(key, ts)
    result = await receiver.accept(event, "unknown", ts, sig)
    assert result.ack is False
    assert result.retry is False
    assert receiver.received == []


# --- extension URI is canonical on every surface (§6.1, §13, §18.1) ---------


async def test_extension_uri_is_canonical_on_every_surface():
    """The A2A-message metadata key and the discovery surface must use the
    canonical AgentCard extension URI, not a divergent bare domain."""
    captured: list[dict] = []

    class CapturingTransport(Transport):
        async def send_a2a_message(self, endpoint, message):
            captured.append(message)
            return DeliveryResult(ack=True, status_code=204)

        async def send_webhook(self, url, headers, body):
            return DeliveryResult(ack=True, status_code=204)

    pub = A2AEventsPublisher(
        agent_card_url="https://pub.example/.well-known/agent-card.json",
        transport=CapturingTransport(),
        signing_key=SigningKey.generate("k1"),
        config=PublisherConfig(
            card_resolver=lambda _u: SubscriberCard(
                card_url="https://sub.example/c",
                a2a_endpoint="https://sub.example/a2a",
            )
        ),
    )
    pub.declare_topic(Topic(name=TOPIC))
    await pub.subscribe(
        subscriber_card_url="https://sub.example/c",
        topics=[TOPIC],
        delivery=DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
    )
    await pub.publish(TOPIC, "t", {"x": 1})

    assert [*captured[0]["message"]["metadata"]] == [EXTENSION_URI]
    assert (await pub.list_topics())["extension"] == EXTENSION_URI


# --- retry backoff (publisher side, §19.4) ----------------------------------


async def _subscribe(harness: Harness):
    return await harness.publisher.subscribe(
        subscriber_card_url=SUBSCRIBER_CARD,
        topics=[TOPIC],
        delivery=DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
        lease_seconds=3600,
    )


async def test_no_backoff_when_first_attempt_succeeds(harness: Harness):
    await _subscribe(harness)
    await harness.publisher.publish(TOPIC, "t", {"cardUrl": "https://x"})
    assert harness.sleeps == []


async def test_exponential_backoff_between_retries(harness: Harness):
    await _subscribe(harness)
    attempts = {"n": 0}

    def fail_twice(_event):
        attempts["n"] += 1
        if attempts["n"] <= 2:
            return DeliveryResult(ack=False, retry=True, status_code=503)
        return None  # third attempt succeeds

    harness.subscriber.on_event = fail_twice
    await harness.publisher.publish(TOPIC, "t", {"cardUrl": "https://x"})

    assert len(harness.subscriber.received) == 1
    # initial=1s, doubling: sleep after attempt 1 then after attempt 2.
    assert harness.sleeps == [1.0, 2.0]


async def test_no_backoff_after_final_failed_attempt(harness: Harness):
    await _subscribe(harness)
    harness.subscriber.on_event = lambda _e: DeliveryResult(ack=False, retry=True)
    await harness.publisher.publish(TOPIC, "t", {"cardUrl": "https://x"})
    # 3 attempts -> only 2 inter-attempt sleeps, none after the last.
    assert harness.sleeps == [1.0, 2.0]
    assert len(harness.publisher.dead_letters) == 1


# --- SSRF guard at subscribe time (publisher side, §21.2) -------------------


def _publisher_for_card(card: SubscriberCard) -> A2AEventsPublisher:
    transport = InMemoryTransport()
    pub = A2AEventsPublisher(
        agent_card_url="https://pub.example.com/.well-known/agent-card.json",
        transport=transport,
        signing_key=SigningKey.generate("k1"),
        config=PublisherConfig(card_resolver=lambda _url: card),
    )
    pub.declare_topic(Topic(name=TOPIC, filterableFields=["data.cardUrl"]))
    return pub


async def test_subscribe_rejects_loopback_endpoint():
    pub = _publisher_for_card(
        SubscriberCard(card_url=SUBSCRIBER_CARD, a2a_endpoint="http://127.0.0.1/a2a")
    )
    with pytest.raises(Exception) as exc:
        await pub.subscribe(
            subscriber_card_url=SUBSCRIBER_CARD,
            topics=[TOPIC],
            delivery=DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
            lease_seconds=3600,
        )
    assert getattr(exc.value, "code", None) == ErrorCode.DELIVERY_ENDPOINT_BLOCKED


async def test_subscribe_allows_public_endpoint():
    pub = _publisher_for_card(
        SubscriberCard(
            card_url=SUBSCRIBER_CARD,
            a2a_endpoint="https://agent-a.example.com/a2a",
        )
    )
    sub = await pub.subscribe(
        subscriber_card_url=SUBSCRIBER_CARD,
        topics=[TOPIC],
        delivery=DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
        lease_seconds=3600,
    )
    assert sub.status == "active"
