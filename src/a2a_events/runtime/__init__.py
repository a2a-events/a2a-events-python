"""Reference runtime for A2A Events publishers (DESIGN.md §7.3, §7.4).

The runtime is split along a contract/implementation seam:

- :mod:`a2a_events.runtime.contracts` — the backend SPI (Protocols + the data
  records exchanged across them). The publisher depends only on these.
- :mod:`a2a_events.runtime.memory` — the zero-dependency in-memory reference
  backends, used by default.
- :mod:`a2a_events.runtime.postgres` — optional batteries-included durable
  backends (requires the ``postgres`` extra), imported explicitly.

The Postgres package is intentionally *not* re-exported here so importing the
runtime never requires the ``postgres`` extra. Import it as
``from a2a_events.runtime.postgres import PostgresEventStore``.
"""

from .contracts import (
    DeadLetter,
    DeliveryAttempt,
    DeliveryResult,
    EventRecord,
    EventStore,
    RetryablePublisher,
    RetryItem,
    RetryQueue,
    SubscriptionStore,
    Transport,
)
from .memory import (
    InMemoryEventStore,
    InMemoryRetryQueue,
    InMemorySubscriptionStore,
    InMemoryTransport,
)
from .publisher import A2AEventsPublisher, PublisherConfig
from .retention import RetentionCompactor
from .retry_worker import RetryWorker

__all__ = [
    "A2AEventsPublisher",
    "PublisherConfig",
    "DeadLetter",
    "DeliveryAttempt",
    "DeliveryResult",
    "EventRecord",
    "EventStore",
    "InMemoryEventStore",
    "InMemoryRetryQueue",
    "InMemorySubscriptionStore",
    "InMemoryTransport",
    "RetentionCompactor",
    "RetryItem",
    "RetryQueue",
    "RetryWorker",
    "RetryablePublisher",
    "SubscriptionStore",
    "Transport",
]
