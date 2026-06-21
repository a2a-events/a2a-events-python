"""HTTP servers and key resolution (DESIGN.md §12.3, §18, §21.3).

- :func:`create_publisher_app` exposes the canonical JSON-RPC surface plus the
  JWKS ``signingKeysUrl`` endpoint, and performs the ``A2A-Extensions``
  activation handshake (echoing activated URIs on the response).
- :func:`create_subscriber_app` exposes the subscriber's A2A endpoint
  (``a2a.SendMessage``) and webhook receive endpoint.
- :class:`JwksKeyResolver` fetches and caches publisher signing keys.

Requires the ``server``/``client`` extras (fastapi, httpx).
"""

from __future__ import annotations

from typing import Any

import httpx
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse

from .auth import AuthIdentity, CallerAuthenticator
from .errors import http_status_for_error
from .jsonrpc import handle
from .models import EXTENSION_URI
from .receiver import EventReceiver
from .runtime import DeliveryResult
from .runtime.publisher import A2AEventsPublisher
from .signing import public_key_from_jwk

EXTENSIONS_HEADER = "A2A-Extensions"


async def _json_body(request: Request) -> dict[str, Any]:
    """Parse a JSON request body, tolerating an empty one."""
    if not await request.body():
        return {}
    body: dict[str, Any] = await request.json()
    return body


async def _rpc_over_http(
    publisher: A2AEventsPublisher,
    method: str,
    params: dict[str, Any],
    *,
    ok_status: int = 200,
    caller: AuthIdentity | None = None,
) -> JSONResponse:
    """Run a canonical JSON-RPC method and render an HTTP+JSON response (§29)."""
    response = await handle(
        publisher,
        {"jsonrpc": "2.0", "id": "http", "method": method, "params": params},
        caller=caller,
    )
    if "error" in response:
        error = response["error"]
        return JSONResponse(error, status_code=http_status_for_error(error))
    return JSONResponse(response["result"], status_code=ok_status)


def _activated(header_value: str | None) -> list[str]:
    """Return the activated extension URIs for the handshake (DESIGN.md §12.3)."""
    if not header_value:
        return []
    requested = {u.strip() for u in header_value.split(",") if u.strip()}
    return [EXTENSION_URI] if EXTENSION_URI in requested else []


def _receive_status(res: DeliveryResult) -> int:
    """Map a delivery result to the webhook response status (DESIGN.md §10.10)."""
    if res.status_code is not None:
        return res.status_code
    if res.ack:
        return 204
    if res.retry:
        return 503
    return 422


def create_publisher_app(
    publisher: A2AEventsPublisher,
    *,
    authenticator: CallerAuthenticator | None = None,
) -> FastAPI:
    app = FastAPI(title="A2A Events Publisher")

    def caller_of(request: Request) -> AuthIdentity | None:
        """Authenticate the control-plane caller from request headers (§21.1)."""
        if authenticator is None:
            return None
        return authenticator(request.headers)

    @app.post("/a2a-events/jsonrpc")
    async def jsonrpc(request: Request) -> JSONResponse:
        activated = _activated(request.headers.get(EXTENSIONS_HEADER))
        body = await request.json()
        result = await handle(publisher, body, caller=caller_of(request))
        headers = {EXTENSIONS_HEADER: ", ".join(activated)} if activated else {}
        return JSONResponse(result, headers=headers)

    @app.get("/a2a-events/keys")
    async def keys() -> dict[str, list[dict[str, str]]]:
        return {"keys": publisher.signing_jwks()}

    # --- optional HTTP+JSON binding (§29 table) ---------------------------
    # Each route maps 1:1 to a canonical a2a.events.* method via the shared
    # JSON-RPC handler, so semantics and error mapping stay identical.

    @app.get("/a2a-events/topics")
    async def http_list_topics() -> JSONResponse:
        return await _rpc_over_http(publisher, "a2a.events.ListTopics", {})

    @app.post("/a2a-events/subscriptions")
    async def http_subscribe(request: Request) -> JSONResponse:
        params = await _json_body(request)
        return await _rpc_over_http(
            publisher,
            "a2a.events.Subscribe",
            params,
            ok_status=201,
            caller=caller_of(request),
        )

    @app.get("/a2a-events/subscriptions")
    async def http_list_subscriptions(
        request: Request,
        page_token: str | None = Query(default=None, alias="pageToken"),
    ) -> JSONResponse:
        params: dict[str, Any] = {"pageToken": page_token} if page_token else {}
        return await _rpc_over_http(
            publisher,
            "a2a.events.ListSubscriptions",
            params,
            caller=caller_of(request),
        )

    @app.get("/a2a-events/subscriptions/{subscription_id}")
    async def http_get_subscription(
        subscription_id: str, request: Request
    ) -> JSONResponse:
        return await _rpc_over_http(
            publisher,
            "a2a.events.GetSubscription",
            {"subscriptionId": subscription_id},
            caller=caller_of(request),
        )

    @app.delete("/a2a-events/subscriptions/{subscription_id}")
    async def http_delete_subscription(
        subscription_id: str, request: Request
    ) -> JSONResponse:
        return await _rpc_over_http(
            publisher,
            "a2a.events.DeleteSubscription",
            {"subscriptionId": subscription_id},
            caller=caller_of(request),
        )

    @app.get("/a2a-events/subscriptions/{subscription_id}/deliveries")
    async def http_list_deliveries(
        subscription_id: str,
        request: Request,
        page_token: str | None = Query(default=None, alias="pageToken"),
    ) -> JSONResponse:
        params: dict[str, Any] = {"subscriptionId": subscription_id}
        if page_token:
            params["pageToken"] = page_token
        return await _rpc_over_http(
            publisher,
            "a2a.events.ListDeliveryAttempts",
            params,
            caller=caller_of(request),
        )

    @app.post("/a2a-events/subscriptions/{subscription_id}:renew")
    async def http_renew(subscription_id: str, request: Request) -> JSONResponse:
        params = {"subscriptionId": subscription_id, **await _json_body(request)}
        return await _rpc_over_http(
            publisher,
            "a2a.events.RenewSubscription",
            params,
            caller=caller_of(request),
        )

    @app.post("/a2a-events/subscriptions/{subscription_id}:replay")
    async def http_replay(subscription_id: str, request: Request) -> JSONResponse:
        params = {"subscriptionId": subscription_id, **await _json_body(request)}
        return await _rpc_over_http(
            publisher, "a2a.events.Replay", params, caller=caller_of(request)
        )

    @app.post("/a2a-events/subscriptions/{subscription_id}:ack")
    async def http_ack(subscription_id: str, request: Request) -> JSONResponse:
        params = {"subscriptionId": subscription_id, **await _json_body(request)}
        return await _rpc_over_http(
            publisher, "a2a.events.Ack", params, caller=caller_of(request)
        )

    return app


def create_subscriber_app(receiver: EventReceiver) -> FastAPI:
    app = FastAPI(title="A2A Events Subscriber")

    @app.post("/a2a/v1")
    async def a2a(request: Request) -> JSONResponse:
        activated = _activated(request.headers.get(EXTENSIONS_HEADER))
        body = await request.json()
        headers = {EXTENSIONS_HEADER: ", ".join(activated)} if activated else {}
        if body.get("method") != "a2a.SendMessage":
            return JSONResponse(
                {
                    "jsonrpc": "2.0",
                    "id": body.get("id"),
                    "error": {"code": -32601, "message": "Unknown method."},
                },
                headers=headers,
            )
        res = await receiver.accept_a2a_message(body["params"])
        result: dict[str, object] = {"ack": res.ack}
        if not res.ack:
            result["retry"] = res.retry
        return JSONResponse(
            {"jsonrpc": "2.0", "id": body.get("id"), "result": result}, headers=headers
        )

    @app.post("/a2a-events/receive")
    async def receive(request: Request) -> Response:
        activated = _activated(request.headers.get(EXTENSIONS_HEADER))
        body = await request.json()
        res = await receiver.accept_webhook(dict(request.headers), body)
        status = _receive_status(res)
        headers = {EXTENSIONS_HEADER: ", ".join(activated)} if activated else {}
        return Response(status_code=status, headers=headers)

    return app


class JwksKeyResolver:
    """Resolves publisher signing keys from a JWKS endpoint (DESIGN.md §21.3)."""

    def __init__(self, jwks_url: str, client: httpx.AsyncClient) -> None:
        self._jwks_url = jwks_url
        self._client = client
        self._cache: dict[str, Ed25519PublicKey] = {}

    async def __call__(self, key_id: str) -> Ed25519PublicKey:
        if key_id not in self._cache:
            await self._refresh()
        return self._cache[key_id]

    async def _refresh(self) -> None:
        resp = await self._client.get(self._jwks_url)
        resp.raise_for_status()
        for jwk in resp.json().get("keys", []):
            if jwk.get("kty") == "OKP" and jwk.get("crv") == "Ed25519":
                self._cache[jwk["kid"]] = public_key_from_jwk(jwk)
