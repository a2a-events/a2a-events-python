"""In-memory reference backends — the zero-dependency default batteries.

Every class here implements a contract from
:mod:`a2a_events.runtime.contracts`. They are the reference against which other
backends are contract-tested and the default the publisher uses when no backend
is supplied.
"""

from .event_store import InMemoryEventStore
from .retry_queue import InMemoryRetryQueue
from .subscription_store import InMemorySubscriptionStore
from .transport import A2AMessageHandler, InMemoryTransport, WebhookHandler

__all__ = [
    "A2AMessageHandler",
    "InMemoryEventStore",
    "InMemoryRetryQueue",
    "InMemorySubscriptionStore",
    "InMemoryTransport",
    "WebhookHandler",
]
