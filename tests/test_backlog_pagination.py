"""Backlog catch-up pagination (§14.1, §20): no cap, no gaps, no duplicates.

The old backlog path did a single ``read(limit=10_000)`` and dropped the rest;
catch-up now pages from the durable position until it reaches the head. These
tests pin: exact page boundaries, backlogs beyond the old 10,000 cap,
selector-sparse streams (scan position advances past non-matching events),
mid-backlog crash + resume, and multi-topic subscriptions.
"""

from __future__ import annotations

import pytest
from conftest import PUBLISHER_CARD, SUBSCRIBER_CARD, TOPIC, Harness

from a2a_events import (
    A2AEventsPublisher,
    DeliveryMode,
    DeliveryPreference,
    FieldFilterSelector,
    PublisherConfig,
    Topic,
)

DELIVERY = DeliveryPreference(mode=DeliveryMode.WEBHOOK)


def _publisher(harness: Harness, page_size: int) -> A2AEventsPublisher:
    return A2AEventsPublisher(
        agent_card_url=PUBLISHER_CARD,
        transport=harness.transport,
        signing_key=harness.key,
        config=PublisherConfig(
            store=harness.publisher.store,
            subscription_store=harness.publisher.subs,
            card_resolver=lambda _url: harness.subscriber.card(),
            page_size=page_size,
        ),
    )


async def _seed(harness: Harness, n: int, topic: str = TOPIC) -> None:
    for i in range(n):
        await harness.publisher.publish(topic, "t", {"cardUrl": f"https://{i}"})


def _delivered_indices(harness: Harness) -> list[int]:
    return [
        int(e["data"]["cardUrl"].removeprefix("https://"))
        for e in harness.subscriber.received
        if e["a2atopic"] == TOPIC
    ]


@pytest.mark.parametrize("count", [20, 21, 22], ids=["under", "exact", "over"])
async def test_backlog_page_boundaries(harness: Harness, count: int):
    """Backlogs just under / exactly at / just over a page multiple all
    deliver completely, in order, without duplicates (page_size=7)."""
    await _seed(harness, count)
    publisher = _publisher(harness, page_size=7)
    await publisher.subscribe(
        SUBSCRIBER_CARD, [TOPIC], DELIVERY, from_cursor="earliest", lease_seconds=3600
    )
    assert _delivered_indices(harness) == list(range(count))


@pytest.mark.parametrize("count", [10_000, 10_001], ids=["10k", "10k+1"])
async def test_backlog_beyond_former_hard_cap(harness: Harness, count: int):
    """The old one-shot read stopped at 10,000 events; paging must not."""
    store = harness.publisher.store
    for i in range(count):  # append directly: seeding must not dominate runtime
        store.append(TOPIC, "t", "a2a://pub", {"cardUrl": f"https://{i}"})
    await harness.publisher.subscribe(
        SUBSCRIBER_CARD, [TOPIC], DELIVERY, from_cursor="earliest", lease_seconds=3600
    )
    delivered = _delivered_indices(harness)
    assert len(delivered) == count
    assert delivered == list(range(count))


async def test_sparse_selector_advances_scan_position(harness: Harness):
    """Non-matching events advance the durable scan position: they are not
    rescanned by later dispatch passes and never delivered."""
    await _seed(harness, 30)
    publisher = _publisher(harness, page_size=7)
    sub = await publisher.subscribe(
        SUBSCRIBER_CARD,
        [TOPIC],
        DELIVERY,
        selector=FieldFilterSelector(
            where={"data.cardUrl": ["https://5", "https://17", "https://29"]}
        ),
        from_cursor="earliest",
        lease_seconds=3600,
    )
    assert _delivered_indices(harness) == [5, 17, 29]
    # Scan position sits at the head even though most events were filtered.
    high_water = publisher.subs.high_water(sub.subscription_id)
    assert high_water[TOPIC] == 29
    # Re-running dispatch finds nothing: filtered events are not rescanned.
    assert await publisher.dispatch_pending() == 0
    assert _delivered_indices(harness) == [5, 17, 29]


async def test_crash_mid_backlog_resumes_without_loss(harness: Harness):
    """A crash partway through catch-up resumes from the durable position:
    duplicates are allowed (at-least-once), loss is not."""
    await _seed(harness, 10)
    publisher = _publisher(harness, page_size=3)

    fail_after = 4
    count = {"n": 0}

    def bomb(_event):  # type: ignore[no-untyped-def]
        count["n"] += 1
        if count["n"] > fail_after:
            raise RuntimeError("simulated crash")
        return None

    harness.subscriber.on_event = bomb
    with pytest.raises(RuntimeError):
        await publisher.subscribe(
            SUBSCRIBER_CARD,
            [TOPIC],
            DELIVERY,
            from_cursor="earliest",
            lease_seconds=3600,
        )
    assert _delivered_indices(harness) == list(range(fail_after))

    # "Restart": a fresh publisher over the same stores finishes the job.
    harness.subscriber.on_event = None
    restarted = _publisher(harness, page_size=3)
    await restarted.dispatch_pending()
    delivered = _delivered_indices(harness)
    assert sorted(set(delivered)) == list(range(10))  # nothing lost
    assert delivered == sorted(delivered)  # order preserved


async def test_multi_topic_backlog(harness: Harness):
    other = "metrics.rollup"
    harness.publisher.declare_topic(Topic(name=other))
    await _seed(harness, 9)
    for i in range(4):
        await harness.publisher.publish(other, "t", {"n": i})

    publisher = _publisher(harness, page_size=4)
    await publisher.subscribe(
        SUBSCRIBER_CARD,
        [TOPIC, other],
        DELIVERY,
        from_cursor="earliest",
        lease_seconds=3600,
    )
    assert _delivered_indices(harness) == list(range(9))
    other_events = [
        e["data"]["n"] for e in harness.subscriber.received if e["a2atopic"] == other
    ]
    assert other_events == list(range(4))
