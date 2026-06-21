"""Postgres event store (spec §25) implementing the ``EventStore`` protocol.

A reference backend adapter: the publisher observes identical external
semantics whether it uses :class:`InMemoryEventStore` or this. Cursors keep
the reference ``<topic>:<zero-padded-offset>`` encoding, so their byte-wise
lexicographic order equals event order in SQL too — ``cursor`` comparisons do
all ordering, replay, and retention bounds.

The schema follows §25 with two reference-only additions noted inline
(``a2a_event_topics.config`` to round-trip the full Topic model, and
``a2a_events.source`` for the CloudEvents ``source``). Requires the
``postgres`` extra (psycopg 3).
"""

from __future__ import annotations

import hashlib
from typing import Any

from psycopg.types.json import Jsonb

from ... import cursor as cursor_mod
from ...errors import A2AEventsError, ErrorCode
from ...models import Topic
from ...signing import canonicalize
from ..contracts import EventRecord, new_event_id
from .pool import PgPool

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS a2a_event_topics (
    topic TEXT PRIMARY KEY,
    schema_url TEXT,
    retention_seconds INTEGER NOT NULL,
    replay_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    config JSONB NOT NULL DEFAULT '{}',  -- full Topic model (reference detail)
    -- monotonic next dispatch offset; never reused, so retention compaction can
    -- delete old rows without offsets/cursors ever colliding (reference detail)
    next_offset BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS a2a_events (
    event_id TEXT PRIMARY KEY,
    topic TEXT NOT NULL REFERENCES a2a_event_topics(topic),
    cursor TEXT NOT NULL UNIQUE,
    subject TEXT,
    event_type TEXT NOT NULL,
    source TEXT NOT NULL,  -- CloudEvents source (reference detail)
    payload JSONB NOT NULL,
    content_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS a2a_events_topic_cursor_idx
    ON a2a_events (topic, cursor);

-- Migration for tables created before retention compaction: the monotonic
-- offset counter. append() self-heals its value as max(next_offset, count).
ALTER TABLE a2a_event_topics
    ADD COLUMN IF NOT EXISTS next_offset BIGINT NOT NULL DEFAULT 0;
"""


class PostgresEventStore:
    """``EventStore`` backed by a Postgres connection pool (psycopg 3)."""

    def __init__(self, conninfo: str, *, max_size: int = 10) -> None:
        self._pg = PgPool(conninfo, max_size=max_size)

    def create_schema(self) -> None:
        with self._pg.cursor() as cur:
            cur.execute(SCHEMA_SQL)

    def close(self) -> None:
        self._pg.close()

    # --- topic registry -----------------------------------------------------
    def declare_topic(self, topic: Topic) -> None:
        config = topic.model_dump(by_alias=True, mode="json")
        with self._pg.cursor() as cur:
            # setdefault semantics: first declaration wins (idempotent).
            cur.execute(
                """
                INSERT INTO a2a_event_topics
                    (topic, schema_url, retention_seconds, replay_enabled, config)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (topic) DO NOTHING
                """,
                (
                    topic.name,
                    topic.schema_url,
                    topic.retention_seconds,
                    topic.replay,
                    Jsonb(config),
                ),
            )

    def get_topic(self, name: str) -> Topic:
        with self._pg.cursor() as cur:
            cur.execute("SELECT config FROM a2a_event_topics WHERE topic = %s", (name,))
            row = cur.fetchone()
        if row is None:
            raise A2AEventsError(
                ErrorCode.TOPIC_NOT_FOUND,
                f"Topic {name} does not exist.",
                {"topic": name},
            )
        return Topic.model_validate(row[0])

    def topics(self) -> list[Topic]:
        with self._pg.cursor() as cur:
            cur.execute("SELECT config FROM a2a_event_topics ORDER BY created_at")
            return [Topic.model_validate(r[0]) for r in cur.fetchall()]

    # --- append -------------------------------------------------------------
    def append(
        self,
        topic: str,
        event_type: str,
        source: str,
        data: dict[str, Any],
        subject: str | None = None,
    ) -> EventRecord:
        self._require_topic(topic)
        event_id = new_event_id()
        content_hash = "sha256:" + hashlib.sha256(canonicalize(data)).hexdigest()
        # Offset allocation must be atomic: with a connection pool, concurrent
        # appends to the same topic would otherwise compute the same offset and
        # collide on the UNIQUE cursor. A transaction-scoped per-topic advisory
        # lock serializes just the appends to this topic (reads stay concurrent).
        with self._pg.transaction() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (topic,))
            # Monotonic offset: the larger of the stored counter and the live row
            # count (the count term self-heals a pre-counter table; after the
            # first compaction the stored counter always wins, so offsets never
            # repeat even though old rows are gone).
            cur.execute(
                "SELECT next_offset FROM a2a_event_topics WHERE topic = %s", (topic,)
            )
            stored_row = cur.fetchone()
            stored = int(stored_row[0]) if stored_row else 0
            cur.execute("SELECT count(*) FROM a2a_events WHERE topic = %s", (topic,))
            count_row = cur.fetchone()
            offset = max(stored, int(count_row[0]) if count_row else 0)
            event_cursor = cursor_mod.encode(topic, offset)
            cur.execute(
                """
                INSERT INTO a2a_events
                    (event_id, topic, cursor, subject, event_type, source,
                     payload, content_hash, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
                RETURNING created_at
                """,
                (
                    event_id,
                    topic,
                    event_cursor,
                    subject,
                    event_type,
                    source,
                    Jsonb(data),
                    content_hash,
                ),
            )
            created_row = cur.fetchone()
            cur.execute(
                "UPDATE a2a_event_topics SET next_offset = %s WHERE topic = %s",
                (offset + 1, topic),
            )
        assert created_row is not None
        return EventRecord(
            event_id=event_id,
            topic=topic,
            cursor=event_cursor,
            event_type=event_type,
            source=source,
            data=data,
            subject=subject,
            created_at=created_row[0],
            content_hash=content_hash,
        )

    # --- reads / replay -----------------------------------------------------
    def count(self, topic: str) -> int:
        # Total ever appended (the monotonic offset), so it survives compaction.
        with self._pg.cursor() as cur:
            cur.execute(
                "SELECT next_offset FROM a2a_event_topics WHERE topic = %s", (topic,)
            )
            row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    def compact(self, topic: str | None = None) -> int:
        """Delete events outside each topic's retention window (§31)."""
        names = [topic] if topic is not None else [t.name for t in self.topics()]
        removed = 0
        for name in names:
            retention = self.get_topic(name).retention_seconds
            if retention <= 0:
                continue
            with self._pg.cursor() as cur:
                cur.execute(
                    "DELETE FROM a2a_events WHERE topic = %s "
                    "AND created_at < now() - make_interval(secs => %s)",
                    (name, retention),
                )
                removed += cur.rowcount
        return removed

    def oldest_available_cursor(self, topic: str) -> str | None:
        retention = self.get_topic(topic).retention_seconds
        with self._pg.cursor() as cur:
            if retention <= 0:
                cur.execute(
                    "SELECT min(cursor) FROM a2a_events WHERE topic = %s", (topic,)
                )
            else:
                cur.execute(
                    "SELECT min(cursor) FROM a2a_events "
                    "WHERE topic = %s AND created_at >= now() - make_interval(secs => %s)",
                    (topic, retention),
                )
            row = cur.fetchone()
        return row[0] if row and row[0] is not None else None

    def latest_cursor(self, topic: str) -> str:
        with self._pg.cursor() as cur:
            cur.execute("SELECT max(cursor) FROM a2a_events WHERE topic = %s", (topic,))
            row = cur.fetchone()
        if row and row[0] is not None:
            return str(row[0])
        return cursor_mod.encode(topic, 0)

    def read(
        self,
        topic: str,
        from_cursor: str | None = None,
        to_cursor: str | None = None,
        limit: int = 100,
    ) -> tuple[list[EventRecord], str | None]:
        self._require_topic(topic)
        if from_cursor == cursor_mod.LATEST:
            return [], None

        clauses = ["topic = %s"]
        params: list[Any] = [topic]

        if from_cursor not in (None, cursor_mod.EARLIEST):
            assert from_cursor is not None
            oldest = self.oldest_available_cursor(topic)
            if oldest is not None and from_cursor < oldest:
                raise A2AEventsError(
                    ErrorCode.CURSOR_EXPIRED,
                    "The requested cursor is outside the topic retention window.",
                    {"fromCursor": from_cursor, "oldestAvailableCursor": oldest},
                )
            clauses.append("cursor > %s")
            params.append(from_cursor)

        if to_cursor is not None:
            clauses.append("cursor <= %s")
            params.append(to_cursor)

        retention = self.get_topic(topic).retention_seconds
        if retention > 0:
            clauses.append("created_at >= now() - make_interval(secs => %s)")
            params.append(retention)

        sql = (
            "SELECT event_id, topic, cursor, subject, event_type, source, payload, "
            "content_hash, created_at FROM a2a_events WHERE "
            + " AND ".join(clauses)
            + " ORDER BY cursor ASC LIMIT %s"
        )
        params.append(limit + 1)  # fetch one extra to detect a next page

        with self._pg.cursor() as cur:
            # sql is assembled from constant fragments (no user input in the
            # text; values are parameterized), so the non-literal type is safe.
            cur.execute(sql, params)  # pyright: ignore[reportArgumentType]
            rows = cur.fetchall()

        records = [self._row_to_record(r) for r in rows[:limit]]
        next_cursor = records[-1].cursor if len(rows) > limit else None
        return records, next_cursor

    # --- helpers ------------------------------------------------------------
    def _require_topic(self, topic: str) -> None:
        self.get_topic(topic)

    @staticmethod
    def _row_to_record(row: tuple[Any, ...]) -> EventRecord:
        return EventRecord(
            event_id=row[0],
            topic=row[1],
            cursor=row[2],
            subject=row[3],
            event_type=row[4],
            source=row[5],
            data=row[6],
            content_hash=row[7],
            created_at=row[8],
        )
