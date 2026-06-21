"""Postgres-backed durable retry queue (DESIGN.md §19.4, §25).

The crash-surviving counterpart to :class:`InMemoryRetryQueue`: pending retries
live in ``a2a_retry_queue`` and are claimed with ``FOR UPDATE SKIP LOCKED`` plus
a visibility lease, so a publisher crash mid-retry simply lets the item become
due again and another worker picks it up. Requires the ``postgres`` extra.

A connection pool backs the queue, so the publisher (enqueue) and the worker
(claim/complete/reschedule) can call concurrently from different threads under
``offload_store`` — each checks out its own connection.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from ..contracts import RetryItem
from .pool import PgPool

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS a2a_retry_queue (
    retry_id TEXT PRIMARY KEY,
    subscription_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    cursor TEXT NOT NULL,
    event_id TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    next_retry_at TIMESTAMPTZ NOT NULL,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS a2a_retry_queue_due_idx
    ON a2a_retry_queue (next_retry_at);
"""

_COLUMNS = (
    "retry_id, subscription_id, topic, cursor, event_id, attempt, "
    "next_retry_at, last_error"
)
# Same columns, table-qualified, for the claim_due RETURNING (the CTE also
# exposes a retry_id, so unqualified names are ambiguous).
_q_columns = ", ".join(f"q.{c.strip()}" for c in _COLUMNS.split(","))


class PostgresRetryQueue:
    """``RetryQueue`` backed by a Postgres connection pool (psycopg 3)."""

    def __init__(self, conninfo: str, *, max_size: int = 10) -> None:
        self._pg = PgPool(conninfo, max_size=max_size)

    def create_schema(self) -> None:
        with self._pg.cursor() as cur:
            cur.execute(SCHEMA_SQL)

    def close(self) -> None:
        self._pg.close()

    def enqueue(self, item: RetryItem) -> None:
        with self._pg.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO a2a_retry_queue ({_COLUMNS})
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (retry_id) DO NOTHING
                """,
                (
                    item.retry_id,
                    item.subscription_id,
                    item.topic,
                    item.cursor,
                    item.event_id,
                    item.attempt,
                    item.next_retry_at,
                    item.last_error,
                ),
            )

    def claim_due(
        self, now: datetime, limit: int = 100, lease_seconds: int = 60
    ) -> list[RetryItem]:
        leased = now + timedelta(seconds=lease_seconds)
        # _q_columns is a constant column list, not user input, so this
        # non-literal query text is safe; values stay parameterized.
        query = f"""
                WITH due AS (
                    SELECT retry_id FROM a2a_retry_queue
                    WHERE next_retry_at <= %s
                    ORDER BY next_retry_at
                    FOR UPDATE SKIP LOCKED
                    LIMIT %s
                )
                UPDATE a2a_retry_queue q
                SET next_retry_at = %s
                FROM due
                WHERE q.retry_id = due.retry_id
                RETURNING {_q_columns}
                """
        args = (now, limit, leased)
        with self._pg.cursor() as cur:
            cur.execute(query, args)  # pyright: ignore[reportArgumentType]
            return [_row_to_item(r) for r in cur.fetchall()]

    def complete(self, retry_id: str) -> None:
        with self._pg.cursor() as cur:
            cur.execute("DELETE FROM a2a_retry_queue WHERE retry_id = %s", (retry_id,))

    def reschedule(
        self,
        retry_id: str,
        next_retry_at: datetime,
        attempt: int,
        last_error: str | None,
    ) -> None:
        with self._pg.cursor() as cur:
            cur.execute(
                """
                UPDATE a2a_retry_queue
                SET next_retry_at = %s, attempt = %s, last_error = %s
                WHERE retry_id = %s
                """,
                (next_retry_at, attempt, last_error, retry_id),
            )

    def pending(self) -> list[RetryItem]:
        with self._pg.cursor() as cur:
            cur.execute(
                f"SELECT {_COLUMNS} FROM a2a_retry_queue ORDER BY next_retry_at"
            )
            return [_row_to_item(r) for r in cur.fetchall()]


def _row_to_item(row: tuple[Any, ...]) -> RetryItem:
    return RetryItem(
        retry_id=row[0],
        subscription_id=row[1],
        topic=row[2],
        cursor=row[3],
        event_id=row[4],
        attempt=row[5],
        next_retry_at=row[6],
        last_error=row[7],
    )
