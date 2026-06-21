"""Optional Postgres reference backends — the batteries-included durable adapter.

Each class here implements a contract from
:mod:`a2a_events.runtime.contracts`, observing semantics identical to the
in-memory reference but surviving a publisher restart. Importing this package
requires the ``postgres`` extra (``pip install a2a-events[postgres]``), so the
core package stays dependency-free; the publisher never imports it implicitly.

This is *a* reference durable backend, not *the* one — the same contracts admit
your own Redis/Kafka/DynamoDB/etc. adapter without touching the publisher.
"""

from .event_store import PostgresEventStore
from .pool import PgPool
from .retry_queue import PostgresRetryQueue
from .subscription_store import PostgresSubscriptionStore

__all__ = [
    "PgPool",
    "PostgresEventStore",
    "PostgresRetryQueue",
    "PostgresSubscriptionStore",
]
