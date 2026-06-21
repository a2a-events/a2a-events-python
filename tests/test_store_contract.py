"""Store-contract suite: every EventStore backend must behave identically.

Runs against InMemoryEventStore always, and PostgresEventStore when
``A2A_EVENTS_TEST_DATABASE_URL`` points at a Postgres instance (otherwise the
postgres params are skipped). This is what guarantees the Postgres adapter
preserves the external semantics (spec §6, §10.9, §31).
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import timedelta

import pytest

from a2a_events import cursor
from a2a_events.errors import A2AEventsError, ErrorCode
from a2a_events.models import Topic
from a2a_events.runtime.contracts import EventStore
from a2a_events.runtime.memory import InMemoryEventStore

PG_DSN = os.environ.get("A2A_EVENTS_TEST_DATABASE_URL")


@dataclass
class Backend:
    store: EventStore
    backdate: Callable[[str, str, int], None]
    close: Callable[[], None]


def _memory_backend() -> Backend:
    store = InMemoryEventStore()

    def backdate(topic: str, target: str, seconds: int) -> None:
        for event in store._logs[topic].events:
            if event.cursor == target:
                event.created_at -= timedelta(seconds=seconds)

    return Backend(store=store, backdate=backdate, close=lambda: None)


def _postgres_backend() -> Backend:
    from a2a_events.runtime.postgres import PostgresEventStore

    assert PG_DSN is not None
    store = PostgresEventStore(PG_DSN)
    store.create_schema()
    with store._pg.cursor() as cur:
        cur.execute("TRUNCATE a2a_events, a2a_event_topics RESTART IDENTITY CASCADE")

    def backdate(_topic: str, target: str, seconds: int) -> None:
        with store._pg.cursor() as cur:
            cur.execute(
                "UPDATE a2a_events SET created_at = created_at - make_interval(secs => %s) "
                "WHERE cursor = %s",
                (seconds, target),
            )

    return Backend(store=store, backdate=backdate, close=store.close)


_PARAMS = [pytest.param(_memory_backend, id="memory")]
if PG_DSN:
    _PARAMS.append(pytest.param(_postgres_backend, id="postgres"))
else:
    _PARAMS.append(
        pytest.param(
            _postgres_backend,
            id="postgres",
            marks=pytest.mark.skip(reason="set A2A_EVENTS_TEST_DATABASE_URL"),
        )
    )

TOPIC = "agent_card.discovered"


@pytest.fixture(params=_PARAMS)
def backend(request: pytest.FixtureRequest) -> Iterator[Backend]:
    b: Backend = request.param()
    yield b
    b.close()


def _topic(retention: int = 604800) -> Topic:
    return Topic(
        name=TOPIC,
        retentionSeconds=retention,
        filterableFields=["data.cardUrl"],
    )


def _seed(store: EventStore, n: int) -> list[str]:
    store.declare_topic(_topic())
    cursors = []
    for i in range(n):
        rec = store.append(
            TOPIC, "discovered.v1", "a2a://pub", {"cardUrl": f"https://x{i}"}
        )
        cursors.append(rec.cursor)
    return cursors


def test_topic_registry(backend: Backend):
    store = backend.store
    store.declare_topic(_topic())
    got = store.get_topic(TOPIC)
    assert got.name == TOPIC
    assert got.retention_seconds == 604800
    assert got.filterable_fields == ["data.cardUrl"]
    assert [t.name for t in store.topics()] == [TOPIC]
    with pytest.raises(A2AEventsError) as exc:
        store.get_topic("nope")
    assert exc.value.code == ErrorCode.TOPIC_NOT_FOUND


def test_append_and_read_ordering(backend: Backend):
    store = backend.store
    cursors = _seed(store, 3)
    assert cursors == sorted(cursors)  # monotonic, lexicographically ordered
    events, nxt = store.read(TOPIC, cursor.EARLIEST)
    assert [e.cursor for e in events] == cursors
    assert nxt is None
    assert store.count(TOPIC) == 3


def test_read_from_cursor_is_exclusive(backend: Backend):
    store = backend.store
    cursors = _seed(store, 3)
    events, _ = store.read(TOPIC, cursors[0])
    assert [e.cursor for e in events] == cursors[1:]


def test_read_limit_paginates(backend: Backend):
    store = backend.store
    cursors = _seed(store, 3)
    page1, nxt = store.read(TOPIC, cursor.EARLIEST, limit=2)
    assert [e.cursor for e in page1] == cursors[:2]
    assert nxt == cursors[1]
    page2, nxt2 = store.read(TOPIC, nxt, limit=2)
    assert [e.cursor for e in page2] == cursors[2:]
    assert nxt2 is None


def test_latest_and_oldest_cursor(backend: Backend):
    store = backend.store
    cursors = _seed(store, 3)
    assert store.latest_cursor(TOPIC) == cursors[-1]
    assert store.oldest_available_cursor(TOPIC) == cursors[0]


def test_latest_sentinel_returns_empty(backend: Backend):
    store = backend.store
    _seed(store, 2)
    assert store.read(TOPIC, cursor.LATEST) == ([], None)


def test_expired_cursor_is_rejected(backend: Backend):
    store = backend.store
    store.declare_topic(_topic(retention=3600))
    c0 = store.append(TOPIC, "d", "a2a://pub", {"cardUrl": "https://a"}).cursor
    c1 = store.append(TOPIC, "d", "a2a://pub", {"cardUrl": "https://b"}).cursor

    backend.backdate(TOPIC, c0, 7200)  # push c0 outside the 1h retention window

    assert store.oldest_available_cursor(TOPIC) == c1
    events, _ = store.read(TOPIC, cursor.EARLIEST)
    assert [e.cursor for e in events] == [c1]
    with pytest.raises(A2AEventsError) as exc:
        store.read(TOPIC, c0)
    assert exc.value.code == ErrorCode.CURSOR_EXPIRED
