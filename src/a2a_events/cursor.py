"""Opaque, per-topic cursors (DESIGN.md §10.9).

Cursors are opaque to subscribers: they store and return them but must not
parse or construct them. Within a topic, cursors are totally ordered by
byte-wise lexicographic comparison.

The reference encoding is ``"<topic>:<offset>"`` with a fixed-width,
zero-padded offset so that lexicographic order equals numeric order. This
encoding is an implementation detail, never part of the protocol contract.
"""

from __future__ import annotations

# Sentinels accepted by ``fromCursor`` (DESIGN.md §14.1). Never returned as an
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
    """Internal: decode a reference cursor. Publisher-side only."""
    topic, _, raw = cursor.rpartition(":")
    if not topic or not raw.isdigit():
        raise ValueError(f"not a reference cursor: {cursor!r}")
    return topic, int(raw)


def offset_of(cursor: str) -> int:
    """Publisher-side helper: extract the numeric offset of a reference cursor."""
    return _split(cursor)[1]


def topic_of(cursor: str) -> str:
    """Publisher-side helper: extract the topic of a reference cursor."""
    return _split(cursor)[0]
