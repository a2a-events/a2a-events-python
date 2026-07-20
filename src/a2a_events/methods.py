"""Canonical method names — the single definition point (spec §29).

Every transport binding (JSON-RPC dispatch, HTTP+JSON routes, gRPC service)
imports these names instead of hardcoding strings, so the surface cannot
drift between bindings.

Naming note (spec §12.3): A2A core's JSON-RPC methods are unprefixed
PascalCase strings (``SendMessage``, ``GetTask``, ... — verified against the
official ``a2a-sdk``). The ``a2a.events.`` dotted prefix below is **this
extension's own namespace convention**, chosen so extension methods can never
collide with A2A core's unprefixed namespace; it is not an A2A core
convention.
"""

from __future__ import annotations

# --- A2A core methods this extension invokes (A2A v1.0) ---------------------
A2A_SEND_MESSAGE = "SendMessage"

# --- a2a.events.* extension surface (spec §29) ------------------------------
METHOD_PREFIX = "a2a.events."

LIST_TOPICS = METHOD_PREFIX + "ListTopics"
SUBSCRIBE = METHOD_PREFIX + "Subscribe"
GET_SUBSCRIPTION = METHOD_PREFIX + "GetSubscription"
LIST_SUBSCRIPTIONS = METHOD_PREFIX + "ListSubscriptions"
RENEW_SUBSCRIPTION = METHOD_PREFIX + "RenewSubscription"
DELETE_SUBSCRIPTION = METHOD_PREFIX + "DeleteSubscription"
REPLAY = METHOD_PREFIX + "Replay"
ACK = METHOD_PREFIX + "Ack"
LIST_DELIVERY_ATTEMPTS = METHOD_PREFIX + "ListDeliveryAttempts"

#: Every canonical method, in spec §29 order.
CANONICAL_METHODS: tuple[str, ...] = (
    LIST_TOPICS,
    SUBSCRIBE,
    GET_SUBSCRIPTION,
    LIST_SUBSCRIPTIONS,
    RENEW_SUBSCRIPTION,
    DELETE_SUBSCRIPTION,
    REPLAY,
    ACK,
    LIST_DELIVERY_ATTEMPTS,
)


def grpc_rpc_name(method: str) -> str:
    """The gRPC RPC name for a canonical method (strip the dotted prefix)."""
    return method.removeprefix(METHOD_PREFIX)
