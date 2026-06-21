"""A2A Events — durable event subscriptions for AgentCard-native A2A systems.

This package is a v0.1 vertical slice: an in-memory publisher runtime, the
canonical ``a2a.events.*`` JSON-RPC surface, A2A-message + webhook delivery,
selectors, leases, opaque cursors, signed delivery, and replay. See
``DESIGN.md`` for the full specification.
"""

from __future__ import annotations

from .agentcard import (
    AgentCardResolver,
    CardTrustPolicy,
    Ed25519CardSignatureVerifier,
    parse_subscriber_card,
)
from .auth import (
    AllowlistAuthorizer,
    AuthIdentity,
    CallerAuthenticator,
    DeliveryTokenIssuer,
    TopicAuthorizer,
)
from .client import InMemorySubscriber
from .errors import A2AEventsError, ErrorCode
from .lease import AutoLeaseRenewer
from .limits import RateLimiter, SelectorLimits, TokenBucketRateLimiter
from .models import (
    EXTENSION_URI,
    CloudEvent,
    DeliveryMode,
    DeliveryPreference,
    FieldFilterSelector,
    KeywordSearchSelector,
    Subscription,
    Topic,
)
from .observability import InMemoryMetrics, Metrics, NullMetrics, trace_id_for
from .runtime import (
    A2AEventsPublisher,
    DeliveryResult,
    InMemoryEventStore,
    InMemoryRetryQueue,
    InMemorySubscriptionStore,
    InMemoryTransport,
    PublisherConfig,
    RetentionCompactor,
    RetryItem,
    RetryQueue,
    RetryWorker,
)
from .runtime.publisher import SubscriberCard
from .signing import SigningKey, SigningKeySet

__version__ = "0.1.0"

__all__ = [
    "EXTENSION_URI",
    "A2AEventsError",
    "A2AEventsPublisher",
    "AgentCardResolver",
    "AllowlistAuthorizer",
    "AuthIdentity",
    "AutoLeaseRenewer",
    "CallerAuthenticator",
    "CardTrustPolicy",
    "CloudEvent",
    "DeliveryTokenIssuer",
    "Ed25519CardSignatureVerifier",
    "DeliveryMode",
    "DeliveryPreference",
    "DeliveryResult",
    "ErrorCode",
    "FieldFilterSelector",
    "InMemoryEventStore",
    "InMemoryMetrics",
    "InMemoryRetryQueue",
    "InMemorySubscriber",
    "InMemorySubscriptionStore",
    "InMemoryTransport",
    "KeywordSearchSelector",
    "Metrics",
    "NullMetrics",
    "PublisherConfig",
    "RateLimiter",
    "RetentionCompactor",
    "RetryItem",
    "RetryQueue",
    "RetryWorker",
    "SelectorLimits",
    "TokenBucketRateLimiter",
    "SigningKey",
    "SigningKeySet",
    "SubscriberCard",
    "Subscription",
    "Topic",
    "TopicAuthorizer",
    "__version__",
    "parse_subscriber_card",
    "trace_id_for",
]
