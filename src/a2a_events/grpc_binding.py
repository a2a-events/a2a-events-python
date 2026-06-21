"""gRPC transport binding for the ``a2a.events.*`` surface (spec §12.3).

A2A core offers a gRPC binding alongside JSON-RPC; this provides the equivalent
for A2A Events. Like the optional HTTP+JSON binding, every gRPC method maps 1:1
to a canonical ``a2a.events.*`` method and runs through the shared
:func:`a2a_events.jsonrpc.handle`, so semantics and error mapping stay identical
across transports.

Messages are JSON-serialized request/response payloads (params in, result out),
which keeps the binding dependency-light — no ``.proto`` compilation step — while
still being a real ``grpc.aio`` service with one unary RPC per method. Protocol
errors map to gRPC status codes; the symbolic A2A Events code travels in trailing
metadata so the client can reconstruct the :class:`A2AEventsError`.

Requires the ``grpc`` extra (grpcio).
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

import grpc

from .auth import AuthIdentity, CallerAuthenticator
from .errors import A2AEventsError, ErrorCode, http_status_for_error
from .jsonrpc import handle

if TYPE_CHECKING:
    from .runtime.publisher import A2AEventsPublisher

SERVICE = "a2a.events.v1.A2AEvents"

# gRPC method name -> canonical JSON-RPC method.
METHODS = {
    "ListTopics": "a2a.events.ListTopics",
    "Subscribe": "a2a.events.Subscribe",
    "GetSubscription": "a2a.events.GetSubscription",
    "ListSubscriptions": "a2a.events.ListSubscriptions",
    "RenewSubscription": "a2a.events.RenewSubscription",
    "DeleteSubscription": "a2a.events.DeleteSubscription",
    "Replay": "a2a.events.Replay",
    "Ack": "a2a.events.Ack",
    "ListDeliveryAttempts": "a2a.events.ListDeliveryAttempts",
}

# HTTP status (from the §30 error table) -> gRPC status code.
_HTTP_TO_GRPC = {
    400: grpc.StatusCode.INVALID_ARGUMENT,
    401: grpc.StatusCode.UNAUTHENTICATED,
    403: grpc.StatusCode.PERMISSION_DENIED,
    404: grpc.StatusCode.NOT_FOUND,
    409: grpc.StatusCode.FAILED_PRECONDITION,
    429: grpc.StatusCode.RESOURCE_EXHAUSTED,
    502: grpc.StatusCode.UNAVAILABLE,
}


def _serialize(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8")


def _deserialize(data: bytes) -> dict[str, Any]:
    return json.loads(data) if data else {}


def _grpc_status(error: Mapping[str, Any]) -> grpc.StatusCode:
    return _HTTP_TO_GRPC.get(
        http_status_for_error(dict(error)), grpc.StatusCode.INTERNAL
    )


def _make_handler(
    publisher: A2AEventsPublisher,
    method: str,
    authenticator: CallerAuthenticator | None,
) -> Callable[[dict[str, Any], grpc.aio.ServicerContext], Awaitable[dict[str, Any]]]:
    async def _handler(
        params: dict[str, Any], context: grpc.aio.ServicerContext
    ) -> dict[str, Any]:
        caller: AuthIdentity | None = None
        if authenticator is not None:
            # grpc's aio metadata typing is imperfect; at runtime iteration yields
            # (key, value) pairs. cast keeps pyright honest without a suppression.
            raw_md = cast(list[tuple[str, str]], context.invocation_metadata() or ())
            md = {k.lower(): v for k, v in raw_md}
            caller = authenticator(md)
        response = await handle(
            publisher,
            {"jsonrpc": "2.0", "id": "grpc", "method": method, "params": params},
            caller=caller,
        )
        if "error" in response:
            error = response["error"]
            symbolic = (error.get("data") or {}).get("code", "")
            await context.send_initial_metadata([])
            context.set_trailing_metadata((("a2a-error-code", str(symbolic)),))
            await context.abort(_grpc_status(error), error.get("message", ""))
        result: dict[str, Any] = response["result"]
        return result

    return _handler


def add_a2a_events_servicer(
    server: grpc.aio.Server,
    publisher: A2AEventsPublisher,
    *,
    authenticator: CallerAuthenticator | None = None,
) -> None:
    """Register the A2A Events gRPC service on ``server`` (spec §12.3)."""
    handlers = {
        rpc_name: grpc.unary_unary_rpc_method_handler(
            _make_handler(publisher, method, authenticator),
            request_deserializer=_deserialize,
            response_serializer=_serialize,
        )
        for rpc_name, method in METHODS.items()
    }
    generic = grpc.method_handlers_generic_handler(SERVICE, handlers)
    server.add_generic_rpc_handlers((generic,))


class A2AEventsGrpcClient:
    """Typed-ish client for the A2A Events gRPC binding (spec §12.3)."""

    def __init__(self, channel: grpc.aio.Channel) -> None:
        self._channel = channel

    async def _call(self, rpc_name: str, params: dict[str, Any]) -> dict[str, Any]:
        callable_ = self._channel.unary_unary(
            f"/{SERVICE}/{rpc_name}",
            request_serializer=_serialize,
            response_deserializer=_deserialize,
        )
        try:
            result: dict[str, Any] = await callable_(params)
            return result
        except grpc.aio.AioRpcError as exc:
            raise _to_a2a_error(exc) from exc

    async def list_topics(self) -> dict[str, Any]:
        return await self._call("ListTopics", {})

    async def subscribe(self, **params: Any) -> dict[str, Any]:
        return await self._call("Subscribe", params)

    async def get_subscription(self, subscription_id: str) -> dict[str, Any]:
        return await self._call("GetSubscription", {"subscriptionId": subscription_id})

    async def list_subscriptions(self, page_token: str | None = None) -> dict[str, Any]:
        params = {"pageToken": page_token} if page_token else {}
        return await self._call("ListSubscriptions", params)

    async def renew_subscription(
        self, subscription_id: str, lease_seconds: int
    ) -> dict[str, Any]:
        return await self._call(
            "RenewSubscription",
            {"subscriptionId": subscription_id, "leaseSeconds": lease_seconds},
        )

    async def delete_subscription(self, subscription_id: str) -> dict[str, Any]:
        return await self._call(
            "DeleteSubscription", {"subscriptionId": subscription_id}
        )

    async def replay(self, subscription_id: str, **params: Any) -> dict[str, Any]:
        return await self._call("Replay", {"subscriptionId": subscription_id, **params})

    async def ack(self, subscription_id: str, cursor: str) -> dict[str, Any]:
        return await self._call(
            "Ack", {"subscriptionId": subscription_id, "cursor": cursor}
        )

    async def list_delivery_attempts(
        self, subscription_id: str, page_token: str | None = None
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"subscriptionId": subscription_id}
        if page_token:
            params["pageToken"] = page_token
        return await self._call("ListDeliveryAttempts", params)


def _to_a2a_error(exc: grpc.aio.AioRpcError) -> A2AEventsError:
    """Reconstruct an :class:`A2AEventsError` from a gRPC error response."""
    symbolic = ""
    # grpc metadata iteration typing is imperfect (see _make_handler).
    trailing = cast(list[tuple[str, str]], exc.trailing_metadata() or ())
    for key, value in trailing:
        if key == "a2a-error-code":
            symbolic = value
    try:
        code = ErrorCode(symbolic)
    except ValueError:
        code = ErrorCode.EXTENSION_NOT_SUPPORTED
    return A2AEventsError(code, exc.details() or "gRPC call failed")
