"""In-memory event store with opaque, ordered, per-topic cursors (§7.3, §10.9).

The zero-dependency default :class:`~a2a_events.runtime.contracts.EventStore`
backend: append-only per-topic logs held in process memory. It is the reference
against which other backends (e.g. ``runtime.postgres``) are contract-tested.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from ... import cursor as cursor_mod
from ...errors import A2AEventsError, ErrorCode
from ...models import Topic
from ...signing import canonicalize
from ..contracts import EventRecord, new_event_id


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass
class _TopicLog:
    topic: Topic
    events: list[EventRecord] = field(default_factory=list)
    # Monotonic next dispatch offset. Tracked separately from len(events) so
    # retention compaction can physically drop old events without offsets (and
    # therefore cursors) ever being reused.
    next_offset: int = 0


class InMemoryEventStore:
    """Append-only per-topic logs. Cursors are monotonic within a topic."""

    def __init__(self) -> None:
        self._logs: dict[str, _TopicLog] = {}

    # --- topic registry ---
    def declare_topic(self, topic: Topic) -> None:
        self._logs.setdefault(topic.name, _TopicLog(topic=topic))

    def get_topic(self, name: str) -> Topic:
        log = self._logs.get(name)
        if log is None:
            raise A2AEventsError(
                ErrorCode.TOPIC_NOT_FOUND,
                f"Topic {name} does not exist.",
                {"topic": name},
            )
        return log.topic

    def topics(self) -> list[Topic]:
        return [log.topic for log in self._logs.values()]

    def count(self, topic: str) -> int:
        # Total ever appended (ignores retention), so it stays monotonic across
        # compaction — this is the next dispatch offset, not the live row count.
        log = self._logs.get(topic)
        return log.next_offset if log else 0

    # --- append ---
    def append(
        self,
        topic: str,
        event_type: str,
        source: str,
        data: dict[str, Any],
        subject: str | None = None,
    ) -> EventRecord:
        log = self._logs.get(topic)
        if log is None:
            raise A2AEventsError(
                ErrorCode.TOPIC_NOT_FOUND,
                f"Topic {topic} does not exist.",
                {"topic": topic},
            )
        offset = log.next_offset
        log.next_offset += 1
        record = EventRecord(
            event_id=new_event_id(),
            topic=topic,
            cursor=cursor_mod.encode(topic, offset),
            event_type=event_type,
            source=source,
            data=data,
            subject=subject,
            created_at=_now(),
            content_hash="sha256:" + hashlib.sha256(canonicalize(data)).hexdigest(),
        )
        log.events.append(record)
        return record

    def compact(self, topic: str | None = None) -> int:
        """Drop events outside each topic's retention window (§31)."""
        names = [topic] if topic is not None else list(self._logs)
        removed = 0
        now = _now()
        for name in names:
            log = self._logs.get(name)
            if log is None or log.topic.retention_seconds <= 0:
                continue
            horizon = now - timedelta(seconds=log.topic.retention_seconds)
            kept = [e for e in log.events if e.created_at >= horizon]
            removed += len(log.events) - len(kept)
            log.events = kept
        return removed

    # --- reads / replay (spec §20, §31) ---
    def _live_events(self, topic: str) -> list[EventRecord]:
        """Events still inside the topic's retention window."""
        log = self._logs[topic]
        if log.topic.retention_seconds <= 0:
            return list(log.events)
        horizon = _now() - timedelta(seconds=log.topic.retention_seconds)
        return [e for e in log.events if e.created_at >= horizon]

    def oldest_available_cursor(self, topic: str) -> str | None:
        live = self._live_events(topic)
        return live[0].cursor if live else None

    def latest_cursor(self, topic: str) -> str:
        log = self._logs[topic]
        return log.events[-1].cursor if log.events else cursor_mod.encode(topic, 0)

    def read(
        self,
        topic: str,
        from_cursor: str | None = None,
        to_cursor: str | None = None,
        limit: int = 100,
    ) -> tuple[list[EventRecord], str | None]:
        """Return ``(events, next_cursor)`` after ``from_cursor`` (exclusive).

        ``from_cursor`` of None or ``"earliest"`` starts at the oldest live
        event; ``"latest"`` returns nothing. Raises ``CURSOR_EXPIRED`` if the
        cursor predates the retention window (§31).
        """
        if topic not in self._logs:
            raise A2AEventsError(
                ErrorCode.TOPIC_NOT_FOUND,
                f"Topic {topic} does not exist.",
                {"topic": topic},
            )
        live = self._live_events(topic)

        if from_cursor in (None, cursor_mod.EARLIEST):
            start_offset = -1
        elif from_cursor == cursor_mod.LATEST:
            return [], None
        else:
            assert from_cursor is not None
            start_offset = cursor_mod.offset_of(from_cursor)
            oldest = self.oldest_available_cursor(topic)
            if oldest is not None and start_offset < cursor_mod.offset_of(oldest):
                raise A2AEventsError(
                    ErrorCode.CURSOR_EXPIRED,
                    "The requested cursor is outside the topic retention window.",
                    {"fromCursor": from_cursor, "oldestAvailableCursor": oldest},
                )

        to_offset = cursor_mod.offset_of(to_cursor) if to_cursor else None
        out: list[EventRecord] = []
        for record in live:
            offset = cursor_mod.offset_of(record.cursor)
            if offset <= start_offset:
                continue
            if to_offset is not None and offset > to_offset:
                break
            out.append(record)
            if len(out) >= limit:
                nxt = out[-1].cursor
                # Is there anything after this within range?
                has_more = any(
                    cursor_mod.offset_of(r.cursor) > offset
                    and (
                        to_offset is None or cursor_mod.offset_of(r.cursor) <= to_offset
                    )
                    for r in live
                )
                return out, (nxt if has_more else None)
        return out, None
