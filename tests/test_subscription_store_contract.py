"""Store-contract suite: every SubscriptionStore backend behaves identically.

Runs against InMemorySubscriptionStore always, and PostgresSubscriptionStore
when ``A2A_EVENTS_TEST_DATABASE_URL`` points at a Postgres instance (otherwise
the postgres params are skipped). ``reopen()`` returns a store reading the same
durable state — for Postgres a fresh connection, which is what proves a
subscription survives a publisher restart (DESIGN.md §14, §25).
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from a2a_events.models import (
    DeliveryMode,
    FieldFilterSelector,
    ResolvedDelivery,
    Selector,
    Subscription,
    SubscriptionStatus,
)
from a2a_events.runtime.contracts import (
    DeliveryAttempt,
    SubscriptionStore,
    new_attempt_id,
)
from a2a_events.runtime.memory import InMemorySubscriptionStore

PG_DSN = os.environ.get("A2A_EVENTS_TEST_DATABASE_URL")
TOPIC = "agent_card.discovered"


@dataclass
class Backend:
    store: SubscriptionStore
    reopen: Callable[[], SubscriptionStore]
    close: Callable[[], None]


def _memory_backend() -> Backend:
    store = InMemorySubscriptionStore()
    return Backend(store=store, reopen=lambda: store, close=lambda: None)


def _postgres_backend() -> Backend:
    from a2a_events.runtime.postgres import PostgresSubscriptionStore

    assert PG_DSN is not None
    dsn = PG_DSN  # narrowed to str; captured by the reopen() closure below
    store = PostgresSubscriptionStore(dsn)
    store.create_schema()
    with store._pg.cursor() as cur:
        cur.execute(
            "TRUNCATE a2a_subscriptions, a2a_delivery_attempts, a2a_event_acks "
            "RESTART IDENTITY CASCADE"
        )

    conns = [store]

    def reopen() -> SubscriptionStore:
        fresh = PostgresSubscriptionStore(dsn)
        conns.append(fresh)
        return fresh

    def close() -> None:
        for c in conns:
            c.close()

    return Backend(store=store, reopen=reopen, close=close)


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


@pytest.fixture(params=_PARAMS)
def backend(request: pytest.FixtureRequest) -> Iterator[Backend]:
    b: Backend = request.param()
    yield b
    b.close()


def _sub(
    sub_id: str = "sub_1",
    *,
    selector: Selector | None = None,
    status: SubscriptionStatus = SubscriptionStatus.ACTIVE,
) -> Subscription:
    now = datetime(2026, 6, 19, 12, 0, 0, tzinfo=UTC)
    return Subscription(
        subscriptionId=sub_id,
        status=status,
        publisherCardUrl="https://pub.example/.well-known/agent-card.json",
        subscriberCardUrl="https://sub.example/.well-known/agent-card.json",
        topics=[TOPIC],
        selector=selector,
        delivery=ResolvedDelivery(
            mode=DeliveryMode.A2A_MESSAGE, resolvedEndpoint="https://sub.example/a2a"
        ),
        createdAt=now,
        leaseUntil=now + timedelta(hours=1),
        cursors={},
        metadata={"team": "discovery"},
    )


def test_add_get_list_roundtrip(backend: Backend):
    store = backend.store
    store.add(_sub("sub_a"), {TOPIC: -1})
    store.add(_sub("sub_b"), {TOPIC: 4})

    got = store.get("sub_a")
    assert got is not None
    assert got.subscription_id == "sub_a"
    assert got.topics == [TOPIC]
    assert got.delivery.resolved_endpoint == "https://sub.example/a2a"
    assert got.metadata == {"team": "discovery"}
    assert store.get("missing") is None
    assert {s.subscription_id for s in store.list_all()} == {"sub_a", "sub_b"}


def test_high_water_is_per_topic(backend: Backend):
    store = backend.store
    store.add(_sub("sub_a"), {TOPIC: -1})
    assert store.high_water("sub_a") == {TOPIC: -1}
    store.set_high_water("sub_a", TOPIC, 7)
    assert store.high_water("sub_a") == {TOPIC: 7}
    # An unknown subscription has no high-water marks.
    assert store.high_water("nope") == {}


def test_update_persists_status_lease_and_cursors(backend: Backend):
    store = backend.store
    store.add(_sub("sub_a"), {TOPIC: -1})

    sub = store.get("sub_a")
    assert sub is not None
    sub.status = SubscriptionStatus.EXPIRED
    sub.lease_until = datetime(2030, 1, 1, tzinfo=UTC)
    sub.cursors[TOPIC] = f"{TOPIC}:0000000003"
    store.update(sub)

    reloaded = backend.reopen().get("sub_a")
    assert reloaded is not None
    assert reloaded.status == SubscriptionStatus.EXPIRED
    assert reloaded.lease_until == datetime(2030, 1, 1, tzinfo=UTC)
    assert reloaded.cursors == {TOPIC: f"{TOPIC}:0000000003"}


def test_selector_roundtrips(backend: Backend):
    store = backend.store
    selector = FieldFilterSelector(where={"data.capabilities": ["streaming"]})
    store.add(_sub("sub_a", selector=selector), {TOPIC: -1})
    store.add(_sub("sub_b", selector=None), {TOPIC: -1})

    with_sel = backend.reopen().get("sub_a")
    assert with_sel is not None
    assert isinstance(with_sel.selector, FieldFilterSelector)
    assert with_sel.selector.where == {"data.capabilities": ["streaming"]}

    without = backend.reopen().get("sub_b")
    assert without is not None
    assert without.selector is None


def test_record_ack_is_idempotent(backend: Backend):
    store = backend.store
    store.add(_sub("sub_a"), {TOPIC: -1})
    # Re-acking the same event must not raise (PK upsert).
    store.record_ack("sub_a", "evt_1", f"{TOPIC}:0000000000")
    store.record_ack("sub_a", "evt_1", f"{TOPIC}:0000000000")


def test_delivery_attempts_feed_dead_letters(backend: Backend):
    store = backend.store
    store.add(_sub("sub_a"), {TOPIC: -1})

    def attempt(event_id: str, status: str, error: str | None) -> DeliveryAttempt:
        return DeliveryAttempt(
            delivery_attempt_id=new_attempt_id(),
            subscription_id="sub_a",
            event_id=event_id,
            cursor=f"{TOPIC}:0000000000",
            attempt=1,
            status=status,
            last_status_code=422 if status == "dead_letter" else 200,
            last_error=error,
        )

    store.record_attempt(attempt("evt_1", "delivered", None))
    store.record_attempt(attempt("evt_2", "retry", "503"))
    store.record_attempt(attempt("evt_3", "dead_letter", "rejected"))

    dead = backend.reopen().dead_letters()
    assert [(d.event_id, d.reason) for d in dead] == [("evt_3", "rejected")]


def test_delivery_attempts_are_listed_per_subscription(backend: Backend):
    store = backend.store
    store.add(_sub("sub_a"), {TOPIC: -1})
    store.add(_sub("sub_b"), {TOPIC: -1})

    def attempt(sub_id: str, event_id: str) -> DeliveryAttempt:
        return DeliveryAttempt(
            delivery_attempt_id=new_attempt_id(),
            subscription_id=sub_id,
            event_id=event_id,
            cursor=f"{TOPIC}:0000000000",
            attempt=1,
            status="delivered",
        )

    store.record_attempt(attempt("sub_a", "evt_1"))
    store.record_attempt(attempt("sub_a", "evt_2"))
    store.record_attempt(attempt("sub_b", "evt_9"))

    reopened = backend.reopen()
    assert [a.event_id for a in reopened.delivery_attempts("sub_a")] == [
        "evt_1",
        "evt_2",
    ]
    assert [a.event_id for a in reopened.delivery_attempts("sub_b")] == ["evt_9"]


def test_subscription_survives_reopen(backend: Backend):
    """The core durability property: a subscription outlives the connection."""
    backend.store.add(_sub("sub_a"), {TOPIC: 2})
    reopened = backend.reopen()
    got = reopened.get("sub_a")
    assert got is not None
    assert got.subscription_id == "sub_a"
    assert reopened.high_water("sub_a") == {TOPIC: 2}
