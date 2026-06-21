"""Shared Postgres connection pooling (spec §25, async deployments).

The original Postgres backends held a single connection and relied on the
publisher serializing every store call behind one global ``asyncio.Lock`` — safe
but a hard concurrency ceiling of one in-flight query at a time. :class:`PgPool`
replaces that with a thread-safe ``psycopg_pool`` connection pool: each store
operation checks out its own connection, so many offloaded calls run truly
concurrently (bounded by ``max_size``). The publisher can then drop its global
lock for these backends via ``store_thread_safe=True``.

Requires the ``postgres`` extra (psycopg 3 + psycopg-pool).
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg_pool import ConnectionPool


class PgPool:
    """A small thread-safe Postgres connection pool wrapper."""

    def __init__(self, conninfo: str, *, min_size: int = 1, max_size: int = 10) -> None:
        self._pool = ConnectionPool(
            conninfo,
            min_size=min_size,
            max_size=max_size,
            kwargs={"autocommit": True},
            open=False,
        )
        self._pool.open()

    @contextmanager
    def cursor(self) -> Iterator[psycopg.Cursor[Any]]:
        """A cursor on a pooled autocommit connection (single-statement work)."""
        with self._pool.connection() as conn, conn.cursor() as cur:
            yield cur

    @contextmanager
    def transaction(self) -> Iterator[psycopg.Cursor[Any]]:
        """A cursor inside a transaction (multi-statement atomic work).

        ``Connection.transaction()`` opens a real transaction even on an
        autocommit connection, so transaction-scoped advisory locks held inside
        survive until commit — used to serialize per-topic offset allocation.
        """
        with self._pool.connection() as conn, conn.transaction(), conn.cursor() as cur:
            yield cur

    def close(self) -> None:
        self._pool.close()
