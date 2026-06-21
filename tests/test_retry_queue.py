"""Durable retry queue + worker tests (spec §19.4, §19.5)."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest

from a2a_events import (
    A2AEventsPublisher,
    DeliveryResult,
    InMemoryEventStore,
    InMemoryRetryQueue,
    InMemorySubscriptionStore,
    PublisherConfig,
    RetryItem,
    RetryWorker,
    SigningKey,
    SubscriberCard,
    Topic,
)
from a2a_events.models import DeliveryMode, DeliveryPreference
from a2a_events.runtime.contracts import Transport

TOPIC = "agent_card.discovered"
PUB_CARD = "https://pub.example/.well-known/agent-card.json"
SUB_CARD = "https://sub.example/.well-known/agent-card.json"
FUTURE = datetime(2099, 1, 1, tzinfo=UTC)


class ScriptedTransport(Transport):
    """Returns queued results in order, then defaults to a successful ack."""

    def __init__(self, results: list[DeliveryResult]) -> None:
        self._results = list(results)
        self.calls = 0

    def _next(self) -> DeliveryResult:
        self.calls += 1
        if self._results:
            return self._results.pop(0)
        return DeliveryResult(ack=True, status_code=204)

    async def send_a2a_message(self, endpoint, message):
        return self._next()

    async def send_webhook(self, url, headers, body):
        return self._next()


def _resolver(_url: str) -> SubscriberCard:
    return SubscriberCard(card_url=SUB_CARD, a2a_endpoint="https://sub.example/a2a")


def _build(transport, queue, *, store=None, subs=None, max_attempts=3):
    pub = A2AEventsPublisher(
        agent_card_url=PUB_CARD,
        transport=transport,
        signing_key=SigningKey.generate("k1"),
        config=PublisherConfig(
            store=store or InMemoryEventStore(),
            subscription_store=subs or InMemorySubscriptionStore(),
            card_resolver=_resolver,
            retry_queue=queue,
            max_attempts=max_attempts,
            retry_initial_delay=1.0,
        ),
    )
    pub.declare_topic(Topic(name=TOPIC, filterableFields=["data.cardUrl"]))
    return pub


async def _subscribe(pub):
    return await pub.subscribe(
        subscriber_card_url=SUB_CARD,
        topics=[TOPIC],
        delivery=DeliveryPreference(mode=DeliveryMode.A2A_MESSAGE),
    )


# --- queue unit behavior ---------------------------------------------------
def _item(retry_id: str, when: datetime) -> RetryItem:
    return RetryItem(
        retry_id=retry_id,
        subscription_id="sub_1",
        topic=TOPIC,
        cursor=f"{TOPIC}:0000000000000000",
        event_id="evt_1",
        attempt=1,
        next_retry_at=when,
    )


def test_inmemory_queue_claim_lease_and_complete() -> None:
    queue = InMemoryRetryQueue()
    now = datetime(2026, 6, 19, tzinfo=UTC)
    queue.enqueue(_item("a", now - timedelta(seconds=5)))
    queue.enqueue(_item("b", now + timedelta(hours=1)))  # not yet due

    claimed = queue.claim_due(now, lease_seconds=60)
    assert [c.retry_id for c in claimed] == ["a"]
    # The lease pushes it out, so an immediate re-claim sees nothing due.
    assert queue.claim_due(now) == []
    # ...but it is due again after the lease (crash-recovery path).
    assert [c.retry_id for c in queue.claim_due(now + timedelta(seconds=61))] == ["a"]

    queue.complete("a")
    assert [i.retry_id for i in queue.pending()] == ["b"]


# --- publisher enqueues failed deliveries ----------------------------------
async def test_failed_delivery_is_enqueued_not_inline_retried() -> None:
    transport = ScriptedTransport(
        [DeliveryResult(ack=False, retry=True, status_code=503)]
    )
    queue = InMemoryRetryQueue()
    pub = _build(transport, queue)
    sub = await _subscribe(pub)

    await pub.publish(TOPIC, "v1", {"cardUrl": "https://x"})
    # Exactly one attempt was made inline; the rest is deferred to the queue.
    assert transport.calls == 1
    pending = queue.pending()
    assert len(pending) == 1 and pending[0].subscription_id == sub.subscription_id
    attempts = pub.subs.delivery_attempts(sub.subscription_id)
    assert [a.status for a in attempts] == ["retry"]


async def test_worker_delivers_due_retry() -> None:
    transport = ScriptedTransport(
        [DeliveryResult(ack=False, retry=True, status_code=503)]
    )
    queue = InMemoryRetryQueue()
    pub = _build(transport, queue)
    sub = await _subscribe(pub)
    await pub.publish(TOPIC, "v1", {"cardUrl": "https://x"})

    worker = RetryWorker(pub, queue, clock=lambda: FUTURE)
    assert await worker.run_once() == 1

    assert queue.pending() == []  # delivered on the retry, removed from the queue
    statuses = [a.status for a in pub.subs.delivery_attempts(sub.subscription_id)]
    assert statuses == ["retry", "delivered"]


async def test_worker_dead_letters_at_attempt_ceiling() -> None:
    # Always fails (retryable); max_attempts=2 -> attempt 1 inline, attempt 2 in worker.
    transport = ScriptedTransport(
        [DeliveryResult(ack=False, retry=True, status_code=503)] * 5
    )
    queue = InMemoryRetryQueue()
    pub = _build(transport, queue, max_attempts=2)
    sub = await _subscribe(pub)
    await pub.publish(TOPIC, "v1", {"cardUrl": "https://x"})

    worker = RetryWorker(pub, queue, clock=lambda: FUTURE)
    await worker.run_once()

    assert queue.pending() == []
    assert any(d.subscription_id == sub.subscription_id for d in pub.dead_letters)


async def test_retry_survives_publisher_restart() -> None:
    """A retry enqueued by one publisher is drained by a fresh instance (§19.4)."""
    store = InMemoryEventStore()
    subs = InMemorySubscriptionStore()
    queue = InMemoryRetryQueue()

    failing = ScriptedTransport(
        [DeliveryResult(ack=False, retry=True, status_code=503)]
    )
    pub_a = _build(failing, queue, store=store, subs=subs)
    sub = await _subscribe(pub_a)
    await pub_a.publish(TOPIC, "v1", {"cardUrl": "https://x"})
    assert len(queue.pending()) == 1

    # "Restart": a new publisher over the SAME durable queue + stores.
    healthy = ScriptedTransport([])  # default-acks
    pub_b = _build(healthy, queue, store=store, subs=subs)
    worker = RetryWorker(pub_b, queue, clock=lambda: FUTURE)
    assert await worker.run_once() == 1

    assert queue.pending() == []
    assert any(
        a.status == "delivered"
        for a in pub_b.subs.delivery_attempts(sub.subscription_id)
    )


async def test_retry_delivers_event_whose_predecessor_expired() -> None:
    """A queued retry for what is now the oldest live event still delivers after
    the preceding offset is compacted out of retention.

    Regression: ``_record_at`` read from the predecessor cursor (exclusive), so
    once that predecessor aged out the read raised ``CURSOR_EXPIRED`` and the
    still-live event was wrongly dropped as "aged out", losing the event.
    """
    store = InMemoryEventStore()
    queue = InMemoryRetryQueue()
    # Event 0 acks; event 1's first attempt fails (enqueued); the worker retry acks.
    transport = ScriptedTransport(
        [
            DeliveryResult(ack=True, status_code=204),
            DeliveryResult(ack=False, retry=True, status_code=503),
        ]
    )
    pub = _build(transport, queue, store=store)
    sub = await _subscribe(pub)

    await pub.publish(TOPIC, "v1", {"cardUrl": "https://x0"})  # offset 0, delivered
    rec1 = await pub.publish(
        TOPIC, "v1", {"cardUrl": "https://x1"}
    )  # offset 1, enqueued
    assert len(queue.pending()) == 1

    # Age offset 0 out of retention so offset 1 is now the oldest live event and
    # reading from its predecessor cursor would raise CURSOR_EXPIRED.
    store._logs[TOPIC].events[0].created_at = datetime(2000, 1, 1, tzinfo=UTC)

    worker = RetryWorker(pub, queue, clock=lambda: FUTURE)
    assert await worker.run_once() == 1

    assert queue.pending() == []  # completed, not silently dropped
    delivered = [
        a
        for a in pub.subs.delivery_attempts(sub.subscription_id)
        if a.cursor == rec1.cursor and a.status == "delivered"
    ]
    assert delivered, "the retried event must be delivered, not dropped as expired"


# --- Postgres backend ------------------------------------------------------
PG_DSN = os.environ.get("A2A_EVENTS_TEST_DATABASE_URL")


@pytest.mark.skipif(not PG_DSN, reason="set A2A_EVENTS_TEST_DATABASE_URL")
def test_postgres_retry_queue_roundtrip() -> None:
    from a2a_events.runtime.postgres import PostgresRetryQueue

    queue = PostgresRetryQueue(PG_DSN)  # type: ignore[arg-type]
    queue.create_schema()
    with queue._pg.cursor() as cur:
        cur.execute("DELETE FROM a2a_retry_queue")

    now = datetime(2026, 6, 19, tzinfo=UTC)
    queue.enqueue(_item("pg-a", now - timedelta(seconds=5)))
    queue.enqueue(_item("pg-b", now + timedelta(hours=1)))

    claimed = queue.claim_due(now, lease_seconds=60)
    assert [c.retry_id for c in claimed] == ["pg-a"]
    # Leased forward: not due again immediately, but due after the lease.
    assert queue.claim_due(now) == []
    assert [c.retry_id for c in queue.claim_due(now + timedelta(seconds=61))] == [
        "pg-a"
    ]

    queue.reschedule("pg-a", now + timedelta(days=1), 2, "boom")
    rescheduled = next(i for i in queue.pending() if i.retry_id == "pg-a")
    assert rescheduled.attempt == 2 and rescheduled.last_error == "boom"

    queue.complete("pg-a")
    assert {i.retry_id for i in queue.pending()} == {"pg-b"}
    queue.complete("pg-b")
    queue.close()
