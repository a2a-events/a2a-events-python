"""Non-blocking store access via offload_store (DESIGN.md §25, async deploys).

The publisher can run blocking (e.g. sync Postgres) store calls in a worker
thread so they never stall the event loop. The first test proves the loop keeps
running during a slow store call; the second exercises the offload path against
real Postgres when A2A_EVENTS_TEST_DATABASE_URL is set.
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import pytest

from a2a_events import (
    A2AEventsPublisher,
    DeliveryMode,
    DeliveryPreference,
    InMemoryEventStore,
    InMemoryTransport,
    PublisherConfig,
    SigningKey,
    SubscriberCard,
    Topic,
)

PUB = "https://agent-b.example.com/.well-known/agent-card.json"
SUB = "https://agent-a.example.com/.well-known/agent-card.json"
TOPIC = "agent_card.discovered"
PG_DSN = os.environ.get("A2A_EVENTS_TEST_DATABASE_URL")


async def _no_sleep(_delay: float) -> None:  # noqa: S1186
    pass


class _SlowEventStore(InMemoryEventStore):
    """In-memory store whose append blocks the calling thread for ``delay`` s."""

    def __init__(self, delay: float) -> None:
        super().__init__()
        self._delay = delay

    def append(self, *args: Any, **kwargs: Any) -> Any:
        time.sleep(self._delay)  # blocking I/O stand-in
        return super().append(*args, **kwargs)


def _publisher(store: Any, **kwargs: Any) -> A2AEventsPublisher:
    return A2AEventsPublisher(
        agent_card_url=PUB,
        transport=InMemoryTransport(),
        signing_key=SigningKey.generate("k1"),
        config=PublisherConfig(
            store=store,
            card_resolver=lambda _url: SubscriberCard(
                card_url=SUB, a2a_endpoint="https://agent-a.example.com/a2a"
            ),
            sleep=_no_sleep,
            **kwargs,
        ),
    )


async def test_offload_keeps_event_loop_responsive():
    publisher = _publisher(_SlowEventStore(delay=0.2), offload_store=True)
    publisher.declare_topic(Topic(name=TOPIC, filterableFields=["data.cardUrl"]))

    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    task = asyncio.create_task(ticker())
    await asyncio.sleep(0)  # let the ticker start
    await publisher.publish(TOPIC, "t", {"cardUrl": "https://x"})
    ticks_during_publish = ticks
    task.cancel()

    # The 0.2s blocking append ran in a thread, so the loop kept ticking.
    assert ticks_during_publish > 3


async def test_offload_disabled_blocks_the_loop():
    publisher = _publisher(_SlowEventStore(delay=0.2), offload_store=False)
    publisher.declare_topic(Topic(name=TOPIC, filterableFields=["data.cardUrl"]))

    ticks = 0

    async def ticker() -> None:
        nonlocal ticks
        while True:
            await asyncio.sleep(0.01)
            ticks += 1

    task = asyncio.create_task(ticker())
    await asyncio.sleep(0)
    await publisher.publish(TOPIC, "t", {"cardUrl": "https://x"})
    ticks_during_publish = ticks
    task.cancel()

    # Without offload, the synchronous sleep stalled the loop entirely.
    assert ticks_during_publish <= 1


@pytest.mark.skipif(not PG_DSN, reason="set A2A_EVENTS_TEST_DATABASE_URL")
async def test_offloaded_postgres_round_trip():
    from a2a_events.runtime.postgres import (
        PostgresEventStore,
        PostgresSubscriptionStore,
    )

    assert PG_DSN is not None
    event_store = PostgresEventStore(PG_DSN)
    sub_store = PostgresSubscriptionStore(PG_DSN)
    event_store.create_schema()
    sub_store.create_schema()
    with event_store._pg.cursor() as cur:
        cur.execute(
            "TRUNCATE a2a_events, a2a_event_topics, a2a_subscriptions, "
            "a2a_delivery_attempts, a2a_event_acks RESTART IDENTITY CASCADE"
        )

    try:
        publisher = _publisher(
            event_store, subscription_store=sub_store, offload_store=True
        )
        publisher.declare_topic(Topic(name=TOPIC, filterableFields=["data.cardUrl"]))
        sub = await publisher.subscribe(
            subscriber_card_url=SUB,
            topics=[TOPIC],
            delivery=DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
            from_cursor="earliest",
            lease_seconds=3600,
        )
        # No receiver wired -> the event dead-letters, recording an attempt.
        await publisher.publish(TOPIC, "t", {"cardUrl": "https://x"})

        reloaded = await publisher.get_subscription(sub.subscription_id)
        assert reloaded.subscription_id == sub.subscription_id
        attempts = await publisher.list_delivery_attempts(sub.subscription_id)
        assert attempts["deliveryAttempts"]
    finally:
        event_store.close()
        sub_store.close()
