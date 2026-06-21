"""Postgres connection-pool concurrency (DESIGN.md §25, async deployments).

These prove the pooled backends run store calls concurrently instead of behind
a single connection — the bottleneck the pool replaces. Skipped unless
``A2A_EVENTS_TEST_DATABASE_URL`` points at a real Postgres.
"""

from __future__ import annotations

import asyncio
import os
import threading
import time

import pytest

from a2a_events import (
    A2AEventsPublisher,
    DeliveryResult,
    PublisherConfig,
    SigningKey,
    SubscriberCard,
    Topic,
)
from a2a_events.runtime.contracts import Transport

PG_DSN = os.environ.get("A2A_EVENTS_TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not PG_DSN, reason="set A2A_EVENTS_TEST_DATABASE_URL")

TOPIC = "pgconc.topic"


def test_pool_runs_queries_concurrently() -> None:
    from a2a_events.runtime.postgres import PgPool

    pool = PgPool(PG_DSN, max_size=4)  # type: ignore[arg-type]

    def slow() -> None:
        with pool.cursor() as cur:
            cur.execute("SELECT pg_sleep(0.5)")

    start = time.monotonic()
    threads = [threading.Thread(target=slow) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - start
    pool.close()

    # Four 0.5s sleeps would take ~2s if serialized on one connection; with a
    # pool of 4 they overlap and finish in well under 1.2s.
    assert elapsed < 1.2, f"queries did not run concurrently (took {elapsed:.2f}s)"


class _AckTransport(Transport):

    async def send_a2a_message(self, endpoint, message):
        return DeliveryResult(ack=True, status_code=204)

    async def send_webhook(self, url, headers, body):
        return DeliveryResult(ack=True, status_code=204)


async def test_concurrent_appends_get_distinct_cursors() -> None:
    """Concurrent publishes must not collide on the UNIQUE per-topic cursor."""
    from a2a_events.runtime.postgres import (
        PostgresEventStore,
        PostgresSubscriptionStore,
    )

    store = PostgresEventStore(PG_DSN, max_size=8)  # type: ignore[arg-type]
    subs = PostgresSubscriptionStore(PG_DSN, max_size=8)  # type: ignore[arg-type]
    store.create_schema()
    subs.create_schema()
    with store._pg.cursor() as cur:
        cur.execute("DELETE FROM a2a_events WHERE topic = %s", (TOPIC,))

    pub = A2AEventsPublisher(
        agent_card_url="https://pub.example/card",
        transport=_AckTransport(),
        signing_key=SigningKey.generate("k1"),
        config=PublisherConfig(
            store=store,
            subscription_store=subs,
            card_resolver=lambda _u: SubscriberCard(
                card_url="https://s.example", a2a_endpoint="https://s.example/a2a"
            ),
            offload_store=True,
            store_thread_safe=True,
        ),
    )
    pub.declare_topic(
        Topic(name=TOPIC, retentionSeconds=0, filterableFields=["data.n"])
    )

    # Fire many concurrent publishes; the per-topic advisory lock must keep
    # offset allocation race-free so every cursor is distinct.
    await asyncio.gather(*(pub.publish(TOPIC, "v1", {"n": i}) for i in range(25)))

    with store._pg.cursor() as cur:
        cur.execute(
            "SELECT count(*), count(DISTINCT cursor) FROM a2a_events WHERE topic = %s",
            (TOPIC,),
        )
        row = cur.fetchone()
    assert row is not None
    total, distinct = row
    store.close()
    subs.close()
    assert total == 25 and distinct == 25
