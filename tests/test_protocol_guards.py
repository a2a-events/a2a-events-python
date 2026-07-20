"""Control-plane guard rails: malformed cursors, replay-disabled topics,
expired subscriptions, and cross-topic acks surface as protocol errors
(spec §10.9, §20.2, §30, §31) — never as transport 500s.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from conftest import SUBSCRIBER_CARD, TOPIC, Harness

from a2a_events import DeliveryMode, DeliveryPreference, Topic
from a2a_events.errors import A2AEventsError, ErrorCode
from a2a_events.jsonrpc import handle

NO_REPLAY_TOPIC = "audit.log"


async def _subscribe(harness: Harness, **overrides) -> dict:
    params = {
        "subscriberCardUrl": SUBSCRIBER_CARD,
        "topics": [TOPIC],
        "delivery": {"mode": "a2a-message"},
        "leaseSeconds": 3600,
        **overrides,
    }
    return await handle(
        harness.publisher,
        {
            "jsonrpc": "2.0",
            "id": "1",
            "method": "a2a.events.Subscribe",
            "params": params,
        },
    )


async def _rpc(harness: Harness, method: str, params: dict) -> dict:
    return await handle(
        harness.publisher,
        {"jsonrpc": "2.0", "id": "x", "method": method, "params": params},
    )


def _expire(harness: Harness, subscription_id: str) -> None:
    sub = harness.publisher.subs.get(subscription_id)
    assert sub is not None
    sub.lease_until = datetime.now(UTC) - timedelta(seconds=1)


# --- malformed cursors are INVALID_CURSOR, not a crash -----------------------


async def test_ack_with_malformed_cursor(harness: Harness):
    sub = (await _subscribe(harness))["result"]
    resp = await _rpc(
        harness,
        "a2a.events.Ack",
        {"subscriptionId": sub["subscriptionId"], "cursor": "not-a-cursor"},
    )
    assert resp["error"]["data"]["code"] == "INVALID_CURSOR"
    assert resp["error"]["code"] == -32014


async def test_replay_with_malformed_from_cursor(harness: Harness):
    sub = (await _subscribe(harness))["result"]
    resp = await _rpc(
        harness,
        "a2a.events.Replay",
        {"subscriptionId": sub["subscriptionId"], "fromCursor": "###"},
    )
    assert resp["error"]["data"]["code"] == "INVALID_CURSOR"


async def test_subscribe_with_malformed_from_cursor(harness: Harness):
    resp = await _subscribe(harness, fromCursor="bogus")
    assert resp["error"]["data"]["code"] == "INVALID_CURSOR"


# --- replay-disabled topics (§20.2, §31) -------------------------------------


def _declare_no_replay(harness: Harness) -> None:
    harness.publisher.declare_topic(Topic(name=NO_REPLAY_TOPIC, replay=False))


async def test_replay_disabled_topic_rejects_replay(harness: Harness):
    _declare_no_replay(harness)
    sub = (await _subscribe(harness, topics=[NO_REPLAY_TOPIC]))["result"]
    resp = await _rpc(
        harness,
        "a2a.events.Replay",
        {"subscriptionId": sub["subscriptionId"], "fromCursor": "earliest"},
    )
    assert resp["error"]["data"]["code"] == "REPLAY_NOT_SUPPORTED"
    assert resp["error"]["code"] == -32050


async def test_replay_disabled_topic_rejects_backfill_subscribe(harness: Harness):
    _declare_no_replay(harness)
    resp = await _subscribe(harness, topics=[NO_REPLAY_TOPIC], fromCursor="earliest")
    assert resp["error"]["data"]["code"] == "REPLAY_NOT_SUPPORTED"
    # Subscribing at the head is still allowed.
    ok = await _subscribe(harness, topics=[NO_REPLAY_TOPIC], fromCursor="latest")
    assert ok["result"]["status"] == "active"


# --- expired subscriptions (§30 SUBSCRIPTION_EXPIRED) ------------------------


async def test_replay_on_expired_subscription(harness: Harness):
    sub = (await _subscribe(harness))["result"]
    _expire(harness, sub["subscriptionId"])
    resp = await _rpc(
        harness,
        "a2a.events.Replay",
        {"subscriptionId": sub["subscriptionId"], "fromCursor": "earliest"},
    )
    assert resp["error"]["data"]["code"] == "SUBSCRIPTION_EXPIRED"
    assert resp["error"]["code"] == -32021


async def test_ack_on_expired_subscription(harness: Harness):
    sub = (await _subscribe(harness))["result"]
    await harness.publisher.publish(TOPIC, "t", {"cardUrl": "https://x"})
    cursor = harness.subscriber.received[0]["a2acursor"]
    _expire(harness, sub["subscriptionId"])
    resp = await _rpc(
        harness,
        "a2a.events.Ack",
        {"subscriptionId": sub["subscriptionId"], "cursor": cursor},
    )
    assert resp["error"]["data"]["code"] == "SUBSCRIPTION_EXPIRED"


async def test_renew_revives_expired_subscription(harness: Harness):
    sub = (await _subscribe(harness))["result"]
    _expire(harness, sub["subscriptionId"])
    resp = await _rpc(
        harness,
        "a2a.events.RenewSubscription",
        {"subscriptionId": sub["subscriptionId"], "leaseSeconds": 3600},
    )
    assert resp["result"]["status"] == "active"


# --- cross-topic acks --------------------------------------------------------


async def test_ack_for_unsubscribed_topic_is_rejected(harness: Harness):
    sub = (await _subscribe(harness))["result"]
    with pytest.raises(A2AEventsError) as exc:
        await harness.publisher.ack(
            sub["subscriptionId"], "some.other.topic:0000000000000000"
        )
    assert exc.value.code == ErrorCode.TOPIC_NOT_AUTHORIZED
    current = await harness.publisher.get_subscription(sub["subscriptionId"])
    assert "some.other.topic" not in current.cursors


# --- subscribe hygiene -------------------------------------------------------


async def test_duplicate_topics_are_deduplicated(harness: Harness):
    sub = (await _subscribe(harness, topics=[TOPIC, TOPIC]))["result"]
    assert sub["topics"] == [TOPIC]


async def test_specific_from_cursor_seeds_only_its_topic(harness: Harness):
    _declare_no_replay(harness)
    other = Topic(name="metrics.rollup")
    harness.publisher.declare_topic(other)
    # Events exist on both topics before subscribing.
    first = await harness.publisher.publish(TOPIC, "t", {"cardUrl": "https://a"})
    await harness.publisher.publish(TOPIC, "t", {"cardUrl": "https://b"})
    await harness.publisher.publish("metrics.rollup", "t", {"n": 1})

    sub = (
        await _subscribe(
            harness, topics=[TOPIC, "metrics.rollup"], fromCursor=first.cursor
        )
    )["result"]
    # Backfill resumed after `first` on its topic; the other topic started at
    # the head, so its pre-existing event was not replayed.
    delivered = [e["a2atopic"] for e in harness.subscriber.received]
    assert delivered == [TOPIC]
    assert harness.subscriber.received[0]["data"]["cardUrl"] == "https://b"
    assert sub["status"] == "active"


# --- delivered envelope carries the topic's schemaUrl (§16) ------------------


async def test_delivered_event_carries_schema_url(harness: Harness):
    url = "https://agent-b.example.com/schemas/audit.v1.json"
    harness.publisher.declare_topic(Topic(name="schema.topic", schemaUrl=url))
    await _subscribe(harness, topics=["schema.topic"])
    await harness.publisher.publish("schema.topic", "t", {"k": "v"})
    assert harness.subscriber.received[0]["a2aschemaurl"] == url


# --- malformed delivery envelopes are permanent failures, not crashes --------


async def test_malformed_a2a_envelope_is_permanent_400(harness: Harness):
    res = await harness.subscriber.receiver.accept_a2a_message({"nope": True})
    assert res.ack is False
    assert res.retry is False
    assert res.status_code == 400


async def test_publisher_delivery_preference_model():
    # DeliveryPreference defaults to the safe AgentCard-relative endpoint ref.
    pref = DeliveryPreference(mode=DeliveryMode.WEBHOOK)
    assert pref.endpoint_ref == "agent-card:events.receive"
