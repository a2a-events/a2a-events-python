"""JSON-RPC dispatch for the ``a2a.events.*`` method surface (spec §29).

This is the canonical transport. ``handle`` takes a parsed JSON-RPC request
object and returns a JSON-RPC response object. The HTTP/activation-handshake
concerns live in :mod:`a2a_events.server`.
"""

from __future__ import annotations

from typing import Any

from pydantic import TypeAdapter, ValidationError

from .auth import AuthIdentity
from .errors import A2AEventsError, ErrorCode
from .models import DeliveryPreference, Selector, Subscription
from .runtime.publisher import A2AEventsPublisher

PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

_selector_adapter: TypeAdapter[Selector] = TypeAdapter(Selector)

METHODS = {
    "a2a.events.ListTopics",
    "a2a.events.Subscribe",
    "a2a.events.GetSubscription",
    "a2a.events.ListSubscriptions",
    "a2a.events.RenewSubscription",
    "a2a.events.DeleteSubscription",
    "a2a.events.Replay",
    "a2a.events.Ack",
    "a2a.events.ListDeliveryAttempts",
}


def _result(req_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": req_id, "result": result}


def _error(
    req_id: Any, code: int, message: str, data: dict[str, Any] | None = None
) -> dict[str, Any]:
    err: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        err["data"] = data
    return {"jsonrpc": "2.0", "id": req_id, "error": err}


def _dump(sub: Subscription) -> dict[str, Any]:
    return sub.model_dump(by_alias=True, mode="json", exclude_none=True)


async def handle(
    publisher: A2AEventsPublisher,
    request: dict[str, Any],
    caller: AuthIdentity | None = None,
) -> dict[str, Any]:
    req_id = request.get("id")
    if request.get("jsonrpc") != "2.0" or "method" not in request:
        return _error(req_id, INVALID_REQUEST, "Invalid JSON-RPC request.")

    method = request["method"]
    params = request.get("params") or {}
    if method not in METHODS:
        return _error(req_id, METHOD_NOT_FOUND, f"Unknown method {method}.")

    try:
        result = await _dispatch(publisher, method, params, caller)
        return _result(req_id, result)
    except A2AEventsError as exc:
        return _error(
            req_id, exc.jsonrpc_code, exc.message, exc.to_error_object()["data"]
        )
    except (ValidationError, KeyError, TypeError) as exc:
        return _error(req_id, INVALID_PARAMS, f"Invalid params: {exc}")


def _dump_with_auth(publisher: A2AEventsPublisher, sub: Subscription) -> dict[str, Any]:
    """Dump a subscription, attaching the delivery credential (§21.1) if any."""
    dumped = _dump(sub)
    auth = publisher.delivery_auth(sub)
    if auth is not None:
        dumped.setdefault("delivery", {})["auth"] = auth
    return dumped


async def _dispatch(
    publisher: A2AEventsPublisher,
    method: str,
    params: dict[str, Any],
    caller: AuthIdentity | None = None,
) -> Any:
    if method == "a2a.events.ListTopics":
        return await publisher.list_topics()

    if method == "a2a.events.Subscribe":
        selector = _parse_selector(params.get("selector"))
        delivery = DeliveryPreference.model_validate(params["delivery"])
        sub = await publisher.subscribe(
            subscriber_card_url=params["subscriberCardUrl"],
            topics=params["topics"],
            delivery=delivery,
            selector=selector,
            from_cursor=params.get("fromCursor", "latest"),
            lease_seconds=params.get("leaseSeconds", 86400),
            metadata=params.get("metadata"),
            caller=caller,
        )
        return _dump_with_auth(publisher, sub)

    if method == "a2a.events.GetSubscription":
        return _dump_with_auth(
            publisher, await publisher.get_subscription(params["subscriptionId"])
        )

    if method == "a2a.events.ListSubscriptions":
        subs, next_token = await publisher.paginate_subscriptions(
            params.get("pageToken"), params.get("pageSize")
        )
        return {
            "subscriptions": [_dump(s) for s in subs],
            "nextPageToken": next_token,
        }

    if method == "a2a.events.RenewSubscription":
        sub = await publisher.renew(params["subscriptionId"], params["leaseSeconds"])
        return _dump_with_auth(publisher, sub)

    if method == "a2a.events.DeleteSubscription":
        await publisher.delete(params["subscriptionId"])
        return {"subscriptionId": params["subscriptionId"], "status": "deleted"}

    if method == "a2a.events.Replay":
        return await publisher.replay(
            params["subscriptionId"],
            from_cursor=params.get("fromCursor"),
            to_cursor=params.get("toCursor"),
            limit=params.get("limit", 100),
        )

    if method == "a2a.events.Ack":
        return _dump(await publisher.ack(params["subscriptionId"], params["cursor"]))

    if method == "a2a.events.ListDeliveryAttempts":
        return await publisher.list_delivery_attempts(
            params["subscriptionId"],
            params.get("pageToken"),
            params.get("pageSize"),
        )

    raise A2AEventsError(
        ErrorCode.EXTENSION_NOT_SUPPORTED, f"Unhandled method {method}."
    )


def _parse_selector(raw: dict[str, Any] | None) -> Selector | None:
    if raw is None:
        return None
    try:
        return _selector_adapter.validate_python(raw)
    except ValidationError as exc:
        raise A2AEventsError(
            ErrorCode.INVALID_SELECTOR, f"Invalid selector: {exc}", {"selector": raw}
        ) from exc
