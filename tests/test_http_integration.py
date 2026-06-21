"""HTTP integration: two FastAPI apps over ASGI, real signed delivery.

Exercises the JSON-RPC surface, the A2A-Extensions activation handshake,
JWKS-based signature key discovery, and both delivery modes end to end.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import cast

import httpx
import pytest

from a2a_events import (
    A2AEventsPublisher,
    InMemoryTransport,
    PublisherConfig,
    SigningKey,
    Topic,
)
from a2a_events.models import EXTENSION_URI
from a2a_events.receiver import EventReceiver
from a2a_events.runtime.contracts import Transport
from a2a_events.runtime.http_delivery import HttpxTransport
from a2a_events.runtime.publisher import SubscriberCard
from a2a_events.server import (
    JwksKeyResolver,
    create_publisher_app,
    create_subscriber_app,
)

PUB = "https://agent-b.example.com/.well-known/agent-card.json"
SUB = "https://agent-a.example.com/.well-known/agent-card.json"
TOPIC = "agent_card.discovered"


async def _no_sleep(_delay: float) -> None:
    """Skip backoff waits in tests."""


class Stack:
    def __init__(self) -> None:
        self.key = SigningKey.generate("key_2026_06")
        self.publisher = A2AEventsPublisher(
            agent_card_url=PUB,
            transport=cast(Transport, InMemoryTransport()),  # replaced below
            signing_key=self.key,
            config=PublisherConfig(
                card_resolver=lambda _url: SubscriberCard(
                    card_url=SUB,
                    a2a_endpoint="http://sub/a2a/v1",
                    receive_url="http://sub/a2a-events/receive",
                ),
                sleep=_no_sleep,
            ),
        )
        self.publisher.declare_topic(
            Topic(name=TOPIC, filterableFields=["data.cardUrl", "data.capabilities"])
        )

        pub_app = create_publisher_app(self.publisher)
        self.pub_client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=pub_app), base_url="http://pub"
        )
        resolver = JwksKeyResolver("http://pub/a2a-events/keys", self.pub_client)
        self.receiver = EventReceiver(resolver)
        sub_app = create_subscriber_app(self.receiver)
        self.sub_client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=sub_app), base_url="http://sub"
        )
        self.publisher.transport = HttpxTransport(self.sub_client)

    async def aclose(self) -> None:
        await self.pub_client.aclose()
        await self.sub_client.aclose()


@pytest.fixture
async def stack() -> AsyncIterator[Stack]:
    s = Stack()
    yield s
    await s.aclose()


def _subscribe_req(mode: str) -> dict:
    return {
        "jsonrpc": "2.0",
        "id": "1",
        "method": "a2a.events.Subscribe",
        "params": {
            "subscriberCardUrl": SUB,
            "topics": [TOPIC],
            "selector": {
                "type": "field_filter",
                "where": {"data.capabilities": ["streaming"]},
            },
            "delivery": {"mode": mode},
            "fromCursor": "latest",
            "leaseSeconds": 3600,
        },
    }


async def test_activation_handshake_echoes_extension(stack: Stack):
    resp = await stack.pub_client.post(
        "/a2a-events/jsonrpc",
        json=_subscribe_req("a2a-message"),
        headers={"A2A-Extensions": EXTENSION_URI},
    )
    assert resp.status_code == 200
    assert resp.headers.get("A2A-Extensions") == EXTENSION_URI
    assert resp.json()["result"]["status"] == "active"


async def test_jwks_endpoint_serves_key(stack: Stack):
    resp = await stack.pub_client.get("/a2a-events/keys")
    keys = resp.json()["keys"]
    assert keys and keys[0]["kid"] == "key_2026_06"
    assert keys[0]["kty"] == "OKP"


async def test_a2a_message_delivery_over_http(stack: Stack):
    await stack.pub_client.post(
        "/a2a-events/jsonrpc",
        json=_subscribe_req("a2a-message"),
        headers={"A2A-Extensions": EXTENSION_URI},
    )
    await stack.publisher.publish(
        TOPIC, "discovered.v1", {"cardUrl": "https://x", "capabilities": ["streaming"]}
    )
    assert len(stack.receiver.received) == 1
    assert stack.receiver.received[0]["data"]["cardUrl"] == "https://x"


async def test_webhook_delivery_over_http(stack: Stack):
    await stack.pub_client.post(
        "/a2a-events/jsonrpc",
        json=_subscribe_req("webhook"),
        headers={"A2A-Extensions": EXTENSION_URI},
    )
    await stack.publisher.publish(
        TOPIC, "discovered.v1", {"cardUrl": "https://y", "capabilities": ["streaming"]}
    )
    assert len(stack.receiver.received) == 1


async def test_selector_filters_over_http(stack: Stack):
    await stack.pub_client.post(
        "/a2a-events/jsonrpc",
        json=_subscribe_req("a2a-message"),
        headers={"A2A-Extensions": EXTENSION_URI},
    )
    await stack.publisher.publish(
        TOPIC, "discovered.v1", {"cardUrl": "https://z", "capabilities": ["batch"]}
    )
    assert stack.receiver.received == []
