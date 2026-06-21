"""Durable subscriptions survive a publisher restart (DESIGN.md §14, §25).

A "restart" is modelled by constructing a fresh ``A2AEventsPublisher`` over the
*same* event + subscription stores. Because the publisher keeps no subscription
state of its own, the new instance must see the existing subscription and keep
delivering to it. Runs on the in-memory stores; the cross-connection Postgres
equivalent lives in the subscription-store contract suite.
"""

from __future__ import annotations

from conftest import PUBLISHER_CARD, SUBSCRIBER_CARD, TOPIC, Harness

from a2a_events import (
    A2AEventsPublisher,
    DeliveryMode,
    DeliveryPreference,
    PublisherConfig,
)


def _discovered(card: str, capabilities: list[str]) -> dict:
    return {"cardUrl": card, "capabilities": capabilities}


def _restart(harness: Harness) -> A2AEventsPublisher:
    """A new publisher process sharing the same durable stores."""
    return A2AEventsPublisher(
        agent_card_url=PUBLISHER_CARD,
        transport=harness.transport,
        signing_key=harness.key,
        config=PublisherConfig(
            store=harness.publisher.store,
            subscription_store=harness.publisher.subs,
            card_resolver=lambda _url: harness.subscriber.card(),
        ),
    )


async def test_subscription_survives_restart(harness: Harness):
    sub = await harness.publisher.subscribe(
        subscriber_card_url=SUBSCRIBER_CARD,
        topics=[TOPIC],
        delivery=DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
        lease_seconds=3600,
    )

    restarted = _restart(harness)

    # The subscription is visible to the new publisher...
    reloaded = await restarted.get_subscription(sub.subscription_id)
    assert reloaded.status == "active"
    listed = await restarted.list_subscriptions()
    assert [s.subscription_id for s in listed] == [sub.subscription_id]

    # ...and it keeps delivering to it.
    await restarted.publish(TOPIC, "t", _discovered("https://x", ["streaming"]))
    assert len(harness.subscriber.received) == 1


async def test_acked_cursor_survives_restart(harness: Harness):
    sub = await harness.publisher.subscribe(
        subscriber_card_url=SUBSCRIBER_CARD,
        topics=[TOPIC],
        delivery=DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
        lease_seconds=3600,
    )
    await harness.publisher.publish(TOPIC, "t", _discovered("https://x", ["streaming"]))

    # The implicit ack advanced the per-topic cursor; it must be durable.
    current = await harness.publisher.get_subscription(sub.subscription_id)
    acked = current.cursors[TOPIC]
    restarted = _restart(harness)
    reloaded = await restarted.get_subscription(sub.subscription_id)
    assert reloaded.cursors[TOPIC] == acked

    # A restart must not redeliver the already-acked event.
    harness.subscriber.received.clear()
    await restarted.publish(TOPIC, "t", _discovered("https://y", ["streaming"]))
    assert [e["data"]["cardUrl"] for e in harness.subscriber.received] == ["https://y"]
