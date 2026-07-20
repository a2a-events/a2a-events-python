"""Subscriber service for the multi-container E2E (spec §12.2, §18, §21).

Receives A2A-message and webhook deliveries, verifies signatures by fetching the
publisher's JWKS over HTTP (RFC 8785 canonicalization + key rotation across
processes), rejects timestamp-skewed events, and authenticates per-subscription
delivery tokens. It also serves a real A2A AgentCard (so the publisher resolves
delivery endpoints via discovery) and a deliberately-malicious loopback card (to
exercise the SSRF guard). /admin routes let the driver inspect and fault-inject.
"""

from __future__ import annotations

import os
from typing import Any

import httpx
from fastapi import Request

from a2a_events.models import EXTENSION_URI
from a2a_events.receiver import EventReceiver
from a2a_events.runtime import DeliveryResult
from a2a_events.server import JwksKeyResolver, create_subscriber_app

PUBLISHER_JWKS_URL = os.environ["PUBLISHER_JWKS_URL"]
SELF_BASE = os.environ.get("SELF_BASE", "http://a2a-e2e-sub:8000")

_jwks_client = httpx.AsyncClient()
resolver = JwksKeyResolver(PUBLISHER_JWKS_URL, _jwks_client)
receiver = EventReceiver(resolver, max_skew_seconds=300)

# Fault injection: fail the next N deliveries with a retryable 503 so the driver
# can exercise the durable retry queue + worker.
_fail_next = {"count": 0}


def _on_event(_event: dict[str, Any]) -> DeliveryResult | None:
    if _fail_next["count"] > 0:
        _fail_next["count"] -= 1
        return DeliveryResult(ack=False, retry=True, status_code=503, reason="injected")
    return None


receiver.on_event = _on_event

app = create_subscriber_app(receiver)


def _agent_card(receive_url: str, a2a_url: str) -> dict[str, Any]:
    # A2A v1.0 card shape: endpoints are declared via supportedInterfaces.
    return {
        "name": "Enrichment Agent",
        "description": "Enriches discovered AgentCards.",
        "version": "1.0.0",
        "supportedInterfaces": [{"url": a2a_url, "protocolBinding": "JSONRPC"}],
        "capabilities": {
            "extensions": [
                {
                    "uri": EXTENSION_URI,
                    "description": "A2A Events subscriber",
                    "params": {
                        "role": "subscriber",
                        "receiveUrl": receive_url,
                        "acceptedDeliveryModes": ["webhook", "a2a-message"],
                    },
                }
            ]
        },
        "defaultInputModes": ["application/json"],
        "defaultOutputModes": ["application/json"],
        "skills": [],
    }


@app.get("/.well-known/agent-card.json")
async def agent_card() -> dict[str, Any]:
    return _agent_card(f"{SELF_BASE}/a2a-events/receive", f"{SELF_BASE}/a2a/v1")


@app.get("/.well-known/loopback-card.json")
async def loopback_card() -> dict[str, Any]:
    # Endpoints resolve to loopback; the publisher's SSRF guard must reject them.
    return _agent_card("http://127.0.0.1:9/recv", "http://127.0.0.1:9/a2a")


@app.get("/admin/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/admin/received")
async def received() -> dict[str, Any]:
    return {
        "count": len(receiver.received),
        "events": [
            {
                "id": e["id"],
                "cardUrl": e["data"].get("cardUrl"),
                "cursor": e["a2acursor"],
                "traceId": e.get("a2atraceid"),
            }
            for e in receiver.received
        ],
    }


@app.post("/admin/clear")
async def clear() -> dict[str, str]:
    receiver.received.clear()
    receiver._seen.clear()
    return {"status": "cleared"}


@app.post("/admin/register-token")
async def register_token(request: Request) -> dict[str, str]:
    body = await request.json()
    receiver.register_delivery_token(body["subscriptionId"], body["token"])
    return {"status": "registered"}


@app.post("/admin/fail-next")
async def fail_next(request: Request) -> dict[str, int]:
    body = await request.json()
    _fail_next["count"] = int(body.get("count", 1))
    return {"failNext": _fail_next["count"]}
