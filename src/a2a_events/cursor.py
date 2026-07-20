"""Opaque, per-topic cursors (spec §10.9).

Cursors are opaque to subscribers: they store and return them but must not
parse or construct them. Within a topic, cursors are totally ordered by
byte-wise lexicographic comparison.

The reference encoding is ``"<topic>:<offset>"`` with a fixed-width,
zero-padded offset so that lexicographic order equals numeric order. This
encoding is an implementation detail, never part of the protocol contract.
"""

from __future__ import annotations

from .errors import A2AEventsError, ErrorCode

# Sentinels accepted by ``fromCursor`` (spec §14.1). Never returned as an
# event cursor.
EARLIEST = "earliest"
LATEST = "latest"

_OFFSET_WIDTH = 16


def encode(topic: str, offset: int) -> str:
    """Encode a reference cursor for ``topic`` at ``offset`` (>= 0)."""
    if offset < 0:
        raise ValueError("offset must be non-negative")
    return f"{topic}:{offset:0{_OFFSET_WIDTH}d}"


def _split(cursor: str) -> tuple[str, int]:
    """Internal: decode a reference cursor. Publisher-side only.

    Raises ``INVALID_CURSOR`` (not ``ValueError``) so a malformed cursor from a
    subscriber surfaces as a proper protocol error, never a transport 500.
    """
    topic, _, raw = cursor.rpartition(":")
    if not topic or not raw.isdigit():
        raise A2AEventsError(
            ErrorCode.INVALID_CURSOR,
            f"Not a valid cursor: {cursor!r}.",
            {"cursor": cursor},
        )
    return topic, int(raw)


def offset_of(cursor: str) -> int:
    """Publisher-side helper: extract the numeric offset of a reference cursor."""
    return _split(cursor)[1]


def offset_for(topic: str, cursor: str) -> int:
    """Decode ``cursor``, requiring it to belong to ``topic``.

    Raises ``INVALID_CURSOR`` for a malformed cursor or one scoped to a
    different topic (cursors are per-topic, spec §10.9).
    """
    cursor_topic, offset = _split(cursor)
    if cursor_topic != topic:
        raise A2AEventsError(
            ErrorCode.INVALID_CURSOR,
            f"Cursor {cursor!r} does not belong to topic {topic!r}.",
            {"cursor": cursor, "topic": topic},
        )
    return offset


def topic_of(cursor: str) -> str:
    """Publisher-side helper: extract the topic of a reference cursor."""
    return _split(cursor)[0]
