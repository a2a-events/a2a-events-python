"""Retention compaction tests (spec §31).

Verifies that compaction physically removes expired events while keeping cursor
offsets monotonic (never reused), across both store backends.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta

import pytest

from a2a_events import RetentionCompactor, Topic
from a2a_events.cursor import offset_of
from a2a_events.runtime.memory import InMemoryEventStore

TOPIC = "agent_card.discovered"


def _backdate(record, seconds: int) -> None:
    record.created_at = datetime.now(UTC) - timedelta(seconds=seconds)


def test_inmemory_compaction_removes_expired_and_keeps_offsets() -> None:
    store = InMemoryEventStore()
    store.declare_topic(Topic(name=TOPIC, retentionSeconds=3600))

    old = store.append(TOPIC, "v1", "a2a://p", {"n": 0})
    _backdate(old, 7200)  # 2h old -> outside the 1h window
    fresh = store.append(TOPIC, "v1", "a2a://p", {"n": 1})

    removed = store.compact()
    assert removed == 1

    records, _ = store.read(TOPIC, "earliest")
    assert [r.event_id for r in records] == [fresh.event_id]

    # The next append must continue past the deleted offset, never reuse it.
    nxt = store.append(TOPIC, "v1", "a2a://p", {"n": 2})
    assert offset_of(nxt.cursor) == offset_of(fresh.cursor) + 1
    assert offset_of(nxt.cursor) == 2  # old(0), fresh(1), nxt(2)


def test_inmemory_compaction_skips_infinite_retention() -> None:
    store = InMemoryEventStore()
    store.declare_topic(Topic(name=TOPIC, retentionSeconds=0))  # retain forever
    rec = store.append(TOPIC, "v1", "a2a://p", {"n": 0})
    _backdate(rec, 10**6)
    assert store.compact() == 0


async def test_compactor_run_once() -> None:
    store = InMemoryEventStore()
    store.declare_topic(Topic(name=TOPIC, retentionSeconds=3600))
    rec = store.append(TOPIC, "v1", "a2a://p", {"n": 0})
    _backdate(rec, 7200)
    compactor = RetentionCompactor(store, interval_seconds=0.01)
    assert await compactor.run_once() == 1


# --- Postgres backend ------------------------------------------------------
PG_DSN = os.environ.get("A2A_EVENTS_TEST_DATABASE_URL")


@pytest.mark.skipif(not PG_DSN, reason="set A2A_EVENTS_TEST_DATABASE_URL")
def test_postgres_compaction_removes_expired_and_keeps_offsets() -> None:
    from a2a_events.runtime.postgres import PostgresEventStore

    store = PostgresEventStore(PG_DSN)  # type: ignore[arg-type]
    store.create_schema()
    topic = "retention.pg"
    with store._pg.cursor() as cur:
        cur.execute("DELETE FROM a2a_events WHERE topic = %s", (topic,))
        cur.execute("DELETE FROM a2a_event_topics WHERE topic = %s", (topic,))
    store.declare_topic(Topic(name=topic, retentionSeconds=3600))

    old = store.append(topic, "v1", "a2a://p", {"n": 0})
    fresh = store.append(topic, "v1", "a2a://p", {"n": 1})
    # Backdate the first event beyond the retention window.
    with store._pg.cursor() as cur:
        cur.execute(
            "UPDATE a2a_events SET created_at = now() - interval '2 hours' "
            "WHERE event_id = %s",
            (old.event_id,),
        )

    assert store.compact() == 1
    records, _ = store.read(topic, "earliest")
    assert [r.event_id for r in records] == [fresh.event_id]

    # Offset stays monotonic across compaction (count == total ever appended).
    nxt = store.append(topic, "v1", "a2a://p", {"n": 2})
    assert offset_of(nxt.cursor) == 2
    assert store.count(topic) == 3
    store.close()
