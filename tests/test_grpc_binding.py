"""gRPC transport binding tests (spec §12.3).

Runs a real ``grpc.aio`` server + client against an in-memory publisher and
exercises the surface plus error mapping.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

grpc = pytest.importorskip("grpc")

from a2a_events import (  # noqa: E402
    A2AEventsError,
    A2AEventsPublisher,
    DeliveryResult,
    PublisherConfig,
    SigningKey,
    SubscriberCard,
    Topic,
)
from a2a_events.grpc_binding import (  # noqa: E402
    A2AEventsGrpcClient,
    add_a2a_events_servicer,
)
from a2a_events.runtime.contracts import Transport  # noqa: E402

TOPIC = "agent_card.discovered"
SUB_CARD = "https://sub.example/card"


class _AckTransport(Transport):

    async def send_a2a_message(self, endpoint, message):
        return DeliveryResult(ack=True, status_code=204)

    async def send_webhook(self, url, headers, body):
        return DeliveryResult(ack=True, status_code=204)


def _publisher() -> A2AEventsPublisher:
    pub = A2AEventsPublisher(
        agent_card_url="https://pub.example/card",
        transport=_AckTransport(),
        signing_key=SigningKey.generate("k1"),
        config=PublisherConfig(
            card_resolver=lambda u: SubscriberCard(
                card_url=u, a2a_endpoint="https://sub.example/a2a"
            )
        ),
    )
    pub.declare_topic(Topic(name=TOPIC, filterableFields=["data.n"]))
    return pub


@pytest.fixture
async def client() -> AsyncIterator[A2AEventsGrpcClient]:
    server = grpc.aio.server()
    add_a2a_events_servicer(server, _publisher())
    port = server.add_insecure_port("127.0.0.1:0")
    await server.start()
    channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
    try:
        yield A2AEventsGrpcClient(channel)
    finally:
        await channel.close()
        await server.stop(None)


async def test_list_topics(client: A2AEventsGrpcClient) -> None:
    result = await client.list_topics()
    assert any(t["name"] == TOPIC for t in result["topics"])


async def test_subscribe_get_delete_roundtrip(client: A2AEventsGrpcClient) -> None:
    sub = await client.subscribe(
        subscriberCardUrl=SUB_CARD,
        topics=[TOPIC],
        delivery={"mode": "a2a-message"},
    )
    sid = sub["subscriptionId"]
    assert sub["status"] == "active"

    got = await client.get_subscription(sid)
    assert got["subscriptionId"] == sid

    listed = await client.list_subscriptions()
    assert any(s["subscriptionId"] == sid for s in listed["subscriptions"])

    deleted = await client.delete_subscription(sid)
    assert deleted["status"] == "deleted"


async def test_unknown_subscription_maps_to_a2a_error(
    client: A2AEventsGrpcClient,
) -> None:
    with pytest.raises(A2AEventsError) as exc:
        await client.get_subscription("sub_does_not_exist")
    assert exc.value.code.value == "SUBSCRIPTION_NOT_FOUND"


async def test_renew_extends_lease(client: A2AEventsGrpcClient) -> None:
    sub = await client.subscribe(
        subscriberCardUrl=SUB_CARD,
        topics=[TOPIC],
        delivery={"mode": "a2a-message"},
        leaseSeconds=120,
    )
    renewed = await client.renew_subscription(sub["subscriptionId"], 3600)
    assert renewed["status"] == "active"
    assert renewed["leaseUntil"] > sub["leaseUntil"]
