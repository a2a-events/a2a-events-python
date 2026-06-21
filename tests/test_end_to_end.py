"""End-to-end vertical slice over the JSON-RPC surface (spec §35).

subscribe -> deliver -> implicit ack -> replay, plus selector filtering,
delivery modes, dedupe, dead-letter, and lease expiry.
"""

from __future__ import annotations

from datetime import UTC

from conftest import SUBSCRIBER_CARD, TOPIC, Harness

from a2a_events import DeliveryResult
from a2a_events.jsonrpc import handle


def _discovered(card: str, capabilities: list[str]) -> dict:
    return {
        "cardUrl": card,
        "domain": card,
        "capabilities": capabilities,
        "skills": {"tags": ["coding", "search"]},
        "title": "Some Agent",
    }


async def _subscribe(harness: Harness, mode: str = "a2a-message", **overrides) -> dict:
    req = {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "a2a.events.Subscribe",
        "params": {
            "subscriberCardUrl": SUBSCRIBER_CARD,
            "topics": [TOPIC],
            "selector": {
                "type": "field_filter",
                "where": {"data.capabilities": ["streaming"]},
            },
            "delivery": {"mode": mode, "endpointRef": "agent-card:events.receive"},
            "fromCursor": "latest",
            "leaseSeconds": 3600,
            **overrides,
        },
    }
    resp = await handle(harness.publisher, req)
    assert "result" in resp, resp
    return resp["result"]


async def test_subscribe_deliver_ack(harness: Harness):
    sub = await _subscribe(harness)
    assert sub["status"] == "active"
    assert sub["delivery"]["mode"] == "a2a-message"

    rec = await harness.publisher.publish(
        TOPIC,
        "org.example.a2a.agent_card.discovered.v1",
        _discovered("https://x", ["streaming"]),
    )

    assert len(harness.subscriber.received) == 1
    delivered = harness.subscriber.received[0]
    assert delivered["a2aevents"]["topic"] == TOPIC
    assert delivered["a2aevents"]["cursor"] == rec.cursor

    # Implicit ack advanced the per-topic cursor.
    got = await handle(
        harness.publisher,
        {
            "jsonrpc": "2.0",
            "id": "2",
            "method": "a2a.events.GetSubscription",
            "params": {"subscriptionId": sub["subscriptionId"]},
        },
    )
    assert got["result"]["cursors"][TOPIC] == rec.cursor


async def test_selector_filters_non_matching(harness: Harness):
    await _subscribe(harness)
    await harness.publisher.publish(TOPIC, "t", _discovered("https://x", ["batch"]))
    assert harness.subscriber.received == []
    await harness.publisher.publish(TOPIC, "t", _discovered("https://y", ["streaming"]))
    assert len(harness.subscriber.received) == 1


async def test_webhook_delivery_mode(harness: Harness):
    await _subscribe(harness, mode="webhook")
    await harness.publisher.publish(TOPIC, "t", _discovered("https://x", ["streaming"]))
    assert len(harness.subscriber.received) == 1


async def test_replay_returns_matching_events(harness: Harness):
    sub = await _subscribe(harness)
    await harness.publisher.publish(TOPIC, "t", _discovered("https://x", ["streaming"]))
    await harness.publisher.publish(TOPIC, "t", _discovered("https://y", ["batch"]))
    await harness.publisher.publish(TOPIC, "t", _discovered("https://z", ["streaming"]))

    resp = await handle(
        harness.publisher,
        {
            "jsonrpc": "2.0",
            "id": "9",
            "method": "a2a.events.Replay",
            "params": {
                "subscriptionId": sub["subscriptionId"],
                "fromCursor": "earliest",
            },
        },
    )
    events = resp["result"]["events"]
    # Only the two streaming events match the selector.
    assert [e["data"]["cardUrl"] for e in events] == ["https://x", "https://z"]


async def test_duplicate_delivery_is_deduplicated(harness: Harness):
    await _subscribe(harness)
    # Force the subscriber to nack-retry once, so the publisher re-delivers the
    # same event id; the subscriber must dedupe it.
    state = {"first": True}

    def maybe_fail(_event):
        if state["first"]:
            state["first"] = False
            return DeliveryResult(ack=False, retry=True, status_code=503)
        return None  # fall through to normal accept

    harness.subscriber.on_event = maybe_fail
    await harness.publisher.publish(TOPIC, "t", _discovered("https://x", ["streaming"]))
    assert len(harness.subscriber.received) == 1


async def test_permanent_failure_dead_letters(harness: Harness):
    await _subscribe(harness)
    harness.subscriber.on_event = lambda _e: DeliveryResult(
        ack=False, retry=False, status_code=422
    )
    await harness.publisher.publish(TOPIC, "t", _discovered("https://x", ["streaming"]))
    assert harness.subscriber.received == []
    assert len(harness.publisher.dead_letters) == 1


async def test_list_delivery_attempts(harness: Harness):
    sub = await _subscribe(harness)
    sid = sub["subscriptionId"]
    harness.subscriber.on_event = lambda _e: DeliveryResult(
        ack=False, retry=False, status_code=422
    )
    await harness.publisher.publish(TOPIC, "t", _discovered("https://x", ["streaming"]))

    resp = await handle(
        harness.publisher,
        {
            "jsonrpc": "2.0",
            "id": "da",
            "method": "a2a.events.ListDeliveryAttempts",
            "params": {"subscriptionId": sid},
        },
    )
    result = resp["result"]
    assert result["subscriptionId"] == sid
    attempts = result["deliveryAttempts"]
    assert len(attempts) == 1
    assert attempts[0]["status"] == "dead_letter"
    assert attempts[0]["lastStatusCode"] == 422


async def test_lease_expiry_stops_delivery(harness: Harness):
    sub = await _subscribe(harness, leaseSeconds=3600)
    # Force expiry.
    from datetime import datetime, timedelta

    internal = await harness.publisher.get_subscription(sub["subscriptionId"])
    internal.lease_until = datetime.now(UTC) - timedelta(seconds=1)

    await harness.publisher.publish(TOPIC, "t", _discovered("https://x", ["streaming"]))
    assert harness.subscriber.received == []
    expired = await harness.publisher.get_subscription(sub["subscriptionId"])
    assert expired.status == "expired"


async def test_unknown_topic_rejected(harness: Harness):
    resp = await _subscribe_raw(harness, topics=["nope"])
    assert resp["error"]["data"]["code"] == "TOPIC_NOT_FOUND"


async def test_renew_and_delete(harness: Harness):
    sub = await _subscribe(harness)
    sid = sub["subscriptionId"]
    renew = await handle(
        harness.publisher,
        {
            "jsonrpc": "2.0",
            "id": "r",
            "method": "a2a.events.RenewSubscription",
            "params": {"subscriptionId": sid, "leaseSeconds": 7200},
        },
    )
    assert renew["result"]["status"] == "active"

    deleted = await handle(
        harness.publisher,
        {
            "jsonrpc": "2.0",
            "id": "d",
            "method": "a2a.events.DeleteSubscription",
            "params": {"subscriptionId": sid},
        },
    )
    assert deleted["result"]["status"] == "deleted"
    # Idempotent: deleting again still succeeds.
    again = await handle(
        harness.publisher,
        {
            "jsonrpc": "2.0",
            "id": "d2",
            "method": "a2a.events.DeleteSubscription",
            "params": {"subscriptionId": sid},
        },
    )
    assert again["result"]["status"] == "deleted"


async def test_unknown_method(harness: Harness):
    resp = await handle(
        harness.publisher,
        {"jsonrpc": "2.0", "id": "x", "method": "a2a.events.Nope", "params": {}},
    )
    assert resp["error"]["code"] == -32601


async def _subscribe_raw(harness: Harness, **param_overrides) -> dict:
    params = {
        "subscriberCardUrl": SUBSCRIBER_CARD,
        "topics": [TOPIC],
        "delivery": {"mode": "a2a-message"},
        "leaseSeconds": 3600,
    }
    params.update(param_overrides)
    return await handle(
        harness.publisher,
        {
            "jsonrpc": "2.0",
            "id": "1",
            "method": "a2a.events.Subscribe",
            "params": params,
        },
    )
