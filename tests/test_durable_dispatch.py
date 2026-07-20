"""Durable dispatch: crash recovery, subscription cutover, fan-out isolation.

Covers the four runtime-correctness guarantees:

- an event is recoverable from the moment ``publish`` persists it — a crash
  before the first delivery attempt loses nothing (§19);
- subscription creation has a defined cutover: the position captured at
  creation is the linearization point, and neither the backlog nor the
  "latest" boundary has a loss window (§14.1);
- one subscription's slowness or failure never blocks another's deliveries;
- with ``deferred_dispatch=True``, ``publish`` only persists and returns —
  delivery (and its retries/backoff) runs off the publish path.
"""

from __future__ import annotations

import asyncio
import os

import pytest
from conftest import PUBLISHER_CARD, SUBSCRIBER_CARD, TOPIC, Harness

from a2a_events import (
    A2AEventsPublisher,
    DeliveryMode,
    DeliveryPreference,
    DeliveryResult,
    InMemorySubscriber,
    InMemoryTransport,
    PublisherConfig,
    SigningKey,
    Topic,
)
from a2a_events.runtime.dispatcher import DispatchWorker

PG_DSN = os.environ.get("A2A_EVENTS_TEST_DATABASE_URL")

DELIVERY = DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE)


def _restart(harness: Harness) -> A2AEventsPublisher:
    """A new publisher process over the same durable stores."""
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


# --- crash window: persisted before first delivery ---------------------------


async def test_event_persisted_before_crash_is_recovered(harness: Harness):
    await harness.publisher.subscribe(
        SUBSCRIBER_CARD, [TOPIC], DELIVERY, lease_seconds=3600
    )
    # Simulate: append succeeded, process died before any dispatch or enqueue.
    harness.publisher.store.append(TOPIC, "t", "a2a://pub", {"cardUrl": "https://x"})
    assert harness.subscriber.received == []

    restarted = _restart(harness)
    processed = await restarted.dispatch_pending()
    assert processed == 1
    assert [e["data"]["cardUrl"] for e in harness.subscriber.received] == ["https://x"]

    # Recovery is idempotent: nothing left to do.
    assert await restarted.dispatch_pending() == 0
    assert len(harness.subscriber.received) == 1


async def test_dispatch_worker_drains_pending(harness: Harness):
    await harness.publisher.subscribe(
        SUBSCRIBER_CARD, [TOPIC], DELIVERY, lease_seconds=3600
    )
    harness.publisher.store.append(TOPIC, "t", "a2a://pub", {"cardUrl": "https://w"})
    worker = DispatchWorker(_restart(harness))
    assert await worker.run_once() == 1
    assert len(harness.subscriber.received) == 1


# --- subscription cutover races (§14.1) --------------------------------------


async def test_latest_boundary_has_no_loss_window(harness: Harness):
    """An event appended between reading the topic head and persisting the
    subscription must still be delivered (the LATEST linearization point is
    the captured position, not the save time)."""
    store, subs = harness.publisher.store, harness.publisher.subs
    original_add = subs.add

    def racing_add(sub, high_water):  # type: ignore[no-untyped-def]
        # A concurrent publish lands after start-offset capture, before the
        # subscription becomes visible: it sees no subscription to deliver to.
        store.append(TOPIC, "t", "a2a://pub", {"cardUrl": "https://raced"})
        original_add(sub, high_water)

    subs.add = racing_add  # type: ignore[method-assign]
    try:
        await harness.publisher.subscribe(
            SUBSCRIBER_CARD, [TOPIC], DELIVERY, from_cursor="latest", lease_seconds=3600
        )
    finally:
        subs.add = original_add  # type: ignore[method-assign]

    assert [e["data"]["cardUrl"] for e in harness.subscriber.received] == [
        "https://raced"
    ]


async def test_earliest_backlog_not_skipped_by_concurrent_publish(harness: Harness):
    """A publish that races the subscribe-time backlog must not let the new
    event jump the queue and drag the position past undelivered backlog."""
    for i in range(3):
        await harness.publisher.publish(TOPIC, "t", {"cardUrl": f"https://old{i}"})

    gate = asyncio.Event()
    released = False
    original = harness.transport.send_a2a_message

    async def slow_first(endpoint: str, message: dict):  # type: ignore[no-untyped-def]
        nonlocal released
        if not released:
            await gate.wait()
            released = True
        return await original(endpoint, message)

    harness.transport.send_a2a_message = slow_first  # type: ignore[method-assign]

    subscribe_task = asyncio.create_task(
        harness.publisher.subscribe(
            SUBSCRIBER_CARD,
            [TOPIC],
            DELIVERY,
            from_cursor="earliest",
            lease_seconds=3600,
        )
    )
    await asyncio.sleep(0)  # backlog is now blocked on the first delivery
    publish_task = asyncio.create_task(
        harness.publisher.publish(TOPIC, "t", {"cardUrl": "https://new"})
    )
    await asyncio.sleep(0)
    assert harness.subscriber.received == []
    gate.set()
    await asyncio.gather(subscribe_task, publish_task)

    delivered = [e["data"]["cardUrl"] for e in harness.subscriber.received]
    assert delivered == [
        "https://old0",
        "https://old1",
        "https://old2",
        "https://new",
    ]


async def test_specific_cursor_backlog_with_concurrent_publish(harness: Harness):
    first = await harness.publisher.publish(TOPIC, "t", {"cardUrl": "https://a"})
    await harness.publisher.publish(TOPIC, "t", {"cardUrl": "https://b"})

    gate = asyncio.Event()
    released = False
    original = harness.transport.send_a2a_message

    async def slow_first(endpoint: str, message: dict):  # type: ignore[no-untyped-def]
        nonlocal released
        if not released:
            await gate.wait()
            released = True
        return await original(endpoint, message)

    harness.transport.send_a2a_message = slow_first  # type: ignore[method-assign]
    subscribe_task = asyncio.create_task(
        harness.publisher.subscribe(
            SUBSCRIBER_CARD,
            [TOPIC],
            DELIVERY,
            from_cursor=first.cursor,
            lease_seconds=3600,
        )
    )
    await asyncio.sleep(0)
    publish_task = asyncio.create_task(
        harness.publisher.publish(TOPIC, "t", {"cardUrl": "https://c"})
    )
    await asyncio.sleep(0)
    gate.set()
    await asyncio.gather(subscribe_task, publish_task)
    delivered = [e["data"]["cardUrl"] for e in harness.subscriber.received]
    assert delivered == ["https://b", "https://c"]


# --- fan-out isolation -------------------------------------------------------


def _two_subscriber_setup():
    transport = InMemoryTransport()
    key = SigningKey.generate("k1")
    sub_a = InMemorySubscriber(
        "https://a.example.com/card.json", transport, lambda _k: key.public_key
    )
    sub_b = InMemorySubscriber(
        "https://b.example.com/card.json", transport, lambda _k: key.public_key
    )
    cards = {sub_a.card_url: sub_a.card(), sub_b.card_url: sub_b.card()}

    async def no_sleep(_delay: float) -> None:
        return None

    publisher = A2AEventsPublisher(
        agent_card_url=PUBLISHER_CARD,
        transport=transport,
        signing_key=key,
        config=PublisherConfig(card_resolver=cards.__getitem__, sleep=no_sleep),
    )
    publisher.declare_topic(Topic(name=TOPIC))
    return publisher, transport, sub_a, sub_b


async def test_slow_subscriber_does_not_block_fast_one():
    publisher, transport, slow_sub, fast_sub = _two_subscriber_setup()
    await publisher.subscribe(slow_sub.card_url, [TOPIC], DELIVERY, lease_seconds=3600)
    await publisher.subscribe(fast_sub.card_url, [TOPIC], DELIVERY, lease_seconds=3600)

    gate = asyncio.Event()
    original = slow_sub.receiver.accept_a2a_message

    async def slow(message: dict):  # type: ignore[no-untyped-def]
        await gate.wait()
        return await original(message)

    transport.register_a2a(slow_sub.a2a_endpoint, slow)

    publish_task = asyncio.create_task(publisher.publish(TOPIC, "t", {"n": 1}))
    # The fast subscriber must receive while the slow one is still hanging.
    for _ in range(50):
        if fast_sub.received:
            break
        await asyncio.sleep(0)
    assert fast_sub.received, "fast subscriber was blocked behind the slow one"
    assert slow_sub.received == []
    gate.set()
    await publish_task
    assert len(slow_sub.received) == 1


async def test_failing_subscriber_does_not_break_others():
    publisher, _transport, failing_sub, healthy_sub = _two_subscriber_setup()
    await publisher.subscribe(
        failing_sub.card_url, [TOPIC], DELIVERY, lease_seconds=3600
    )
    await publisher.subscribe(
        healthy_sub.card_url, [TOPIC], DELIVERY, lease_seconds=3600
    )
    failing_sub.on_event = lambda _e: DeliveryResult(
        ack=False, retry=True, status_code=503
    )

    for i in range(3):
        await publisher.publish(TOPIC, "t", {"n": i})

    assert len(healthy_sub.received) == 3
    assert failing_sub.received == []
    assert len(publisher.dead_letters) == 3  # failing sub's events dead-lettered


# --- deferred dispatch mode --------------------------------------------------


async def test_deferred_publish_does_not_deliver_inline(harness: Harness):
    publisher = A2AEventsPublisher(
        agent_card_url=PUBLISHER_CARD,
        transport=harness.transport,
        signing_key=harness.key,
        config=PublisherConfig(
            store=harness.publisher.store,
            subscription_store=harness.publisher.subs,
            card_resolver=lambda _url: harness.subscriber.card(),
            deferred_dispatch=True,
        ),
    )
    await publisher.subscribe(SUBSCRIBER_CARD, [TOPIC], DELIVERY, lease_seconds=3600)
    record = await publisher.publish(TOPIC, "t", {"cardUrl": "https://d"})
    # Persisted, but no delivery attempt happened on the publish path.
    assert record.event_id
    assert harness.subscriber.received == []
    worker = DispatchWorker(publisher)
    assert await worker.run_once() == 1
    assert [e["data"]["cardUrl"] for e in harness.subscriber.received] == ["https://d"]


async def test_deferred_publish_latency_excludes_retries(harness: Harness):
    """publish() must not absorb the retry/backoff cycle of a failing
    subscriber when dispatch is deferred."""
    publisher = A2AEventsPublisher(
        agent_card_url=PUBLISHER_CARD,
        transport=harness.transport,
        signing_key=harness.key,
        config=PublisherConfig(
            store=harness.publisher.store,
            subscription_store=harness.publisher.subs,
            card_resolver=lambda _url: harness.subscriber.card(),
            deferred_dispatch=True,
            sleep=harness.publisher._sleep,
        ),
    )
    sub = await publisher.subscribe(
        SUBSCRIBER_CARD, [TOPIC], DELIVERY, lease_seconds=3600
    )
    harness.subscriber.on_event = lambda _e: DeliveryResult(
        ack=False, retry=True, status_code=503
    )
    await publisher.publish(TOPIC, "t", {"n": 1})
    assert harness.sleeps == []  # no backoff on the publish path
    assert publisher.subs.delivery_attempts(sub.subscription_id) == []


# --- Postgres: durable delivery across a real restart ------------------------


@pytest.mark.skipif(not PG_DSN, reason="set A2A_EVENTS_TEST_DATABASE_URL")
async def test_postgres_crash_before_first_delivery_is_recovered():
    from a2a_events.runtime.postgres import (
        PostgresEventStore,
        PostgresSubscriptionStore,
    )

    assert PG_DSN is not None
    event_store = PostgresEventStore(PG_DSN)
    event_store.create_schema()
    sub_store = PostgresSubscriptionStore(PG_DSN)
    sub_store.create_schema()
    with event_store._pg.cursor() as cur:
        cur.execute(
            "TRUNCATE a2a_events, a2a_event_topics, a2a_subscriptions, "
            "a2a_delivery_attempts, a2a_event_acks RESTART IDENTITY CASCADE"
        )

    transport = InMemoryTransport()
    key = SigningKey.generate("kpg")
    subscriber = InMemorySubscriber(
        SUBSCRIBER_CARD, transport, lambda _k: key.public_key
    )

    def make_publisher() -> A2AEventsPublisher:
        return A2AEventsPublisher(
            agent_card_url=PUBLISHER_CARD,
            transport=transport,
            signing_key=key,
            config=PublisherConfig(
                store=event_store,
                subscription_store=sub_store,
                card_resolver=lambda _url: subscriber.card(),
            ),
        )

    p1 = make_publisher()
    p1.declare_topic(Topic(name=TOPIC))
    await p1.subscribe(SUBSCRIBER_CARD, [TOPIC], DELIVERY, lease_seconds=3600)
    # Crash after persistence, before any delivery.
    event_store.append(TOPIC, "t", "a2a://pub", {"cardUrl": "https://pg"})
    assert subscriber.received == []

    p2 = make_publisher()  # the restarted process
    try:
        assert await p2.dispatch_pending() == 1
        assert [e["data"]["cardUrl"] for e in subscriber.received] == ["https://pg"]
        assert await p2.dispatch_pending() == 0
    finally:
        event_store.close()
        sub_store.close()
