"""Postgres subscription store (spec §25) implementing ``SubscriptionStore``.

The durable counterpart to :class:`InMemorySubscriptionStore`: subscriptions,
their per-topic high-water dispatch positions, acks, and delivery attempts
survive a publisher restart. The publisher observes identical external
semantics whichever backend it uses.

The schema follows §25 with three reference-only deviations, noted inline:
``a2a_subscriptions.high_water`` (per-topic dispatch offset) and ``.metadata``
round-trip the full publisher/model state, and the ``event_id`` foreign keys to
``a2a_events`` are dropped so the subscription store is usable independently of
the event-store backend. Requires the ``postgres`` extra (psycopg 3).
"""

from __future__ import annotations

from typing import Any

from psycopg.types.json import Jsonb

from ...models import ResolvedDelivery, Subscription
from ..contracts import DeadLetter, DeliveryAttempt
from .pool import PgPool

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS a2a_subscriptions (
    subscription_id TEXT PRIMARY KEY,
    publisher_card_url TEXT NOT NULL,
    subscriber_card_url TEXT NOT NULL,
    topics TEXT[] NOT NULL,
    selector JSONB NOT NULL DEFAULT '{}',
    delivery JSONB NOT NULL,
    status TEXT NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL,
    -- per-topic last-acked cursor map: { "<topic>": "<cursor>", ... }
    cursors JSONB NOT NULL DEFAULT '{}',
    -- per-topic high-water dispatch offset (reference detail, see module docs)
    high_water JSONB NOT NULL DEFAULT '{}',
    metadata JSONB NOT NULL DEFAULT '{}',  -- subscription metadata (reference detail)
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS a2a_delivery_attempts (
    delivery_attempt_id TEXT PRIMARY KEY,
    subscription_id TEXT NOT NULL
        REFERENCES a2a_subscriptions(subscription_id) ON DELETE CASCADE,
    event_id TEXT NOT NULL,  -- no FK to a2a_events (event store may differ)
    cursor TEXT NOT NULL,    -- event cursor (reference detail; avoids a join for dead-letters)
    attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    last_status_code INTEGER,
    last_error TEXT,
    next_retry_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS a2a_event_acks (
    subscription_id TEXT NOT NULL
        REFERENCES a2a_subscriptions(subscription_id) ON DELETE CASCADE,
    event_id TEXT NOT NULL,  -- no FK to a2a_events (event store may differ)
    cursor TEXT NOT NULL,
    acked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (subscription_id, event_id)
);

CREATE INDEX IF NOT EXISTS a2a_delivery_attempts_deadletter_idx
    ON a2a_delivery_attempts (created_at) WHERE status = 'dead_letter';
"""

_SUB_COLUMNS = (
    "subscription_id, publisher_card_url, subscriber_card_url, topics, "
    "selector, delivery, status, lease_until, cursors, metadata, created_at"
)


class PostgresSubscriptionStore:
    """``SubscriptionStore`` backed by a Postgres connection pool (psycopg 3)."""

    def __init__(self, conninfo: str, *, max_size: int = 10) -> None:
        self._pg = PgPool(conninfo, max_size=max_size)

    def create_schema(self) -> None:
        with self._pg.cursor() as cur:
            cur.execute(SCHEMA_SQL)

    def close(self) -> None:
        self._pg.close()

    # --- subscription CRUD --------------------------------------------------
    def add(self, sub: Subscription, high_water: dict[str, int]) -> None:
        with self._pg.cursor() as cur:
            cur.execute(
                """
                INSERT INTO a2a_subscriptions
                    (subscription_id, publisher_card_url, subscriber_card_url,
                     topics, selector, delivery, status, lease_until, cursors,
                     high_water, metadata, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    sub.subscription_id,
                    sub.publisher_card_url,
                    sub.subscriber_card_url,
                    list(sub.topics),
                    Jsonb(self._selector_json(sub)),
                    Jsonb(sub.delivery.model_dump(by_alias=True, exclude_none=True)),
                    sub.status.value,
                    sub.lease_until,
                    Jsonb(sub.cursors),
                    Jsonb(high_water),
                    Jsonb(sub.metadata),
                    sub.created_at,
                ),
            )

    def get(self, subscription_id: str) -> Subscription | None:
        with self._pg.cursor() as cur:
            cur.execute(
                f"SELECT {_SUB_COLUMNS} FROM a2a_subscriptions "
                "WHERE subscription_id = %s",
                (subscription_id,),
            )
            row = cur.fetchone()
        return self._row_to_sub(row) if row is not None else None

    def list_all(self) -> list[Subscription]:
        with self._pg.cursor() as cur:
            # subscription_id tiebreaker gives a stable total order so keyset
            # pagination (which indexes by id) is deterministic even when
            # several subscriptions share a created_at timestamp.
            cur.execute(
                f"SELECT {_SUB_COLUMNS} FROM a2a_subscriptions "
                "ORDER BY created_at, subscription_id"
            )
            return [self._row_to_sub(r) for r in cur.fetchall()]

    def update(self, sub: Subscription) -> None:
        # Only the mutable fields change after creation (§14).
        with self._pg.cursor() as cur:
            cur.execute(
                """
                UPDATE a2a_subscriptions
                SET status = %s, lease_until = %s, cursors = %s, updated_at = now()
                WHERE subscription_id = %s
                """,
                (
                    sub.status.value,
                    sub.lease_until,
                    Jsonb(sub.cursors),
                    sub.subscription_id,
                ),
            )

    # --- high-water ---------------------------------------------------------
    def high_water(self, subscription_id: str) -> dict[str, int]:
        with self._pg.cursor() as cur:
            cur.execute(
                "SELECT high_water FROM a2a_subscriptions WHERE subscription_id = %s",
                (subscription_id,),
            )
            row = cur.fetchone()
        if row is None or row[0] is None:
            return {}
        return {k: int(v) for k, v in row[0].items()}

    def set_high_water(self, subscription_id: str, topic: str, offset: int) -> None:
        with self._pg.cursor() as cur:
            cur.execute(
                """
                UPDATE a2a_subscriptions
                SET high_water = jsonb_set(high_water, %s::text[], to_jsonb(%s::int)),
                    updated_at = now()
                WHERE subscription_id = %s
                """,
                ([topic], offset, subscription_id),
            )

    # --- acks + delivery attempts -------------------------------------------
    def record_ack(self, subscription_id: str, event_id: str, cursor: str) -> None:
        with self._pg.cursor() as cur:
            cur.execute(
                """
                INSERT INTO a2a_event_acks (subscription_id, event_id, cursor)
                VALUES (%s, %s, %s)
                ON CONFLICT (subscription_id, event_id)
                DO UPDATE SET cursor = EXCLUDED.cursor, acked_at = now()
                """,
                (subscription_id, event_id, cursor),
            )

    def record_attempt(self, attempt: DeliveryAttempt) -> None:
        with self._pg.cursor() as cur:
            cur.execute(
                """
                INSERT INTO a2a_delivery_attempts
                    (delivery_attempt_id, subscription_id, event_id, cursor,
                     attempt, status, last_status_code, last_error)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    attempt.delivery_attempt_id,
                    attempt.subscription_id,
                    attempt.event_id,
                    attempt.cursor,
                    attempt.attempt,
                    attempt.status,
                    attempt.last_status_code,
                    attempt.last_error,
                ),
            )

    def delivery_attempts(self, subscription_id: str) -> list[DeliveryAttempt]:
        with self._pg.cursor() as cur:
            cur.execute(
                "SELECT delivery_attempt_id, subscription_id, event_id, cursor, "
                "attempt, status, last_status_code, last_error "
                "FROM a2a_delivery_attempts WHERE subscription_id = %s "
                "ORDER BY created_at",
                (subscription_id,),
            )
            return [
                DeliveryAttempt(
                    delivery_attempt_id=r[0],
                    subscription_id=r[1],
                    event_id=r[2],
                    cursor=r[3],
                    attempt=r[4],
                    status=r[5],
                    last_status_code=r[6],
                    last_error=r[7],
                )
                for r in cur.fetchall()
            ]

    def dead_letters(self) -> list[DeadLetter]:
        with self._pg.cursor() as cur:
            cur.execute(
                "SELECT subscription_id, event_id, cursor, last_error "
                "FROM a2a_delivery_attempts WHERE status = 'dead_letter' "
                "ORDER BY created_at"
            )
            return [
                DeadLetter(r[0], r[1], r[2], r[3] or "failed") for r in cur.fetchall()
            ]

    # --- helpers ------------------------------------------------------------
    @staticmethod
    def _selector_json(sub: Subscription) -> dict[str, Any]:
        if sub.selector is None:
            return {}
        return sub.selector.model_dump(by_alias=True, exclude_none=True)

    @staticmethod
    def _row_to_sub(row: tuple[Any, ...]) -> Subscription:
        selector = row[4] or None  # stored '{}' for "no selector"
        return Subscription.model_validate(
            {
                "subscriptionId": row[0],
                "publisherCardUrl": row[1],
                "subscriberCardUrl": row[2],
                "topics": list(row[3]),
                "selector": selector,
                "delivery": ResolvedDelivery.model_validate(row[5]),
                "status": row[6],
                "leaseUntil": row[7],
                "cursors": row[8],
                "metadata": row[9],
                "createdAt": row[10],
            }
        )
